import asyncio
import json
import math
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

import discord
from aiohttp import web
from discord import app_commands

import appsettings
import db

# Fire-and-forget tasks are kept here until they finish: asyncio only holds weak references to
# tasks, so an unreferenced task can be garbage-collected mid-run (Python docs, create_task).
_BG_TASKS: set = set()

def _spawn(coro):
    task = asyncio.ensure_future(coro)
    _BG_TASKS.add(task)
    task.add_done_callback(_BG_TASKS.discard)
    return task

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

SERVER_MODE = "--server" in sys.argv

EPH_KEYS = {
    "P_EPH":  "/play",
    "PL_EPH": "/playlist",
    "AP_EPH": "/autoplay",
    "HC_EPH": "/hctest",
    "CC_EPH": "/control panel",
    "S_EPH":  "/stop",
}


SCRIPT_DIR = Path(__file__).parent
TOKENS     = appsettings.require_startup()["tokens"]
PYTHON     = sys.executable

SOURCES = {"sp": "Spotify", "yt": "YouTube Music", "sc": "SoundCloud"}
DEFAULT_SOURCE = "sp"

WORKER_SCRIPT = SCRIPT_DIR / "worker.py"
MAIN_SOCKET   = SCRIPT_DIR / ".main.sock"
if not WORKER_SCRIPT.exists():
    sys.exit("worker.py not found.")


# ── per-server config (database/server/servers.db) ──────────────────────────

_gcfg_cache: dict[int, dict] = {}

def _gcfg(guild_id: int) -> dict:
    if guild_id not in _gcfg_cache:
        _gcfg_cache[guild_id] = db.get_guild(guild_id)
    return _gcfg_cache[guild_id]

def _save_gcfg(guild_id: int) -> None:
    db.save_guild(guild_id, _gcfg(guild_id))

def _cfg_cc(guild_id: int) -> int | None:
    return _gcfg(guild_id).get("cc_id") or None

def _cfg_vcw(guild_id: int, index: int) -> int | None:
    return _gcfg(guild_id)["vcw"].get(str(index)) or None

def _cfg_src(guild_id: int) -> str:
    src = _gcfg(guild_id).get("src")
    return src if src in SOURCES else DEFAULT_SOURCE

# Keys added later default to an older key's value, so turning on private /play also covers /autoplay.
_EPH_FALLBACK = {"AP_EPH": "P_EPH"}

def _eph(guild_id: int | None, key: str) -> bool:
    if not guild_id:
        return False
    eph = _gcfg(guild_id)["eph"]
    if key not in eph and key in _EPH_FALLBACK:
        return bool(eph.get(_EPH_FALLBACK[key], False))
    return bool(eph.get(key, False))

def _migrate_legacy_settings() -> None:
    """One-time import of the old single-server settings.json into the server DB."""
    legacy = SCRIPT_DIR / "settings.json"
    if not legacy.exists():
        return
    try:
        with open(legacy) as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return
    gid = data.get("G_ID")
    if gid and not db.guild_exists(int(gid)):
        eph = {k: v for k, v in (data.get("EPH") or {}).items() if k in EPH_KEYS}
        db.save_guild(int(gid), {"cc_id": data.get("CC_ID"), "vcw": data.get("VCW") or {}, "eph": eph})
        print(f"[Main] Migrated settings.json into the server DB for guild {gid}", flush=True)
    legacy.rename(legacy.with_name("settings.json.migrated"))


# ── worker processes ────────────────────────────────────────────────────────

PENDING_TTL = 90.0   # how long a reservation made by /play survives without the worker confirming it
IPC_LINE_LIMIT = 8 << 20   # longest reply line accepted from a worker (bytes)
HUNG_AFTER     = 4         # failed 30-second health checks in a row before a running worker is restarted

# Graceful restart (systemctl restart/stop): finish current songs, play shutdown.mp3, then exit.
SHUTDOWN_MP3     = SCRIPT_DIR / "shutdown.mp3"
DRAIN_TIMEOUT    = 300.0   # after this, songs are cut and the message plays right away
DRAIN_MESSAGE_MAX = 130.0  # extra time allowed for the message itself to play out
PID_FILE = SCRIPT_DIR / ".bot.pid"   # lets launcher.py wait for a previous bot that's still finishing
_stop_event: asyncio.Event | None = None
_draining = False
_force_drain: asyncio.Event | None = None

class WorkerProcess:
    def __init__(self, index: int):
        self.index       = index
        self.socket_path = SCRIPT_DIR / f".worker{index}.sock"
        self.proc: subprocess.Popen | None = None
        self._pgid: int | None = None
        self._output_task: asyncio.Task | None = None
        self._shutdown_requested = False
        # Per-guild session snapshots reported by the worker:
        # {channel_id, controller_id, mode, autoplay, text_channel_id}
        self.sessions: dict[int, dict] = {}
        # Reservations made while a /play is in flight, before the worker reports the session.
        self.pending: dict[int, tuple[dict, float]] = {}
        self.guild_ids: set[int] = set()
        self.user_id: int | None = None
        self.lavalink_ok = False
        self.available = True      # False while the worker doesn't answer (see _resync_worker)
        # Serialises start/restart: the watchdog and a dev ``restart`` must never both start a
        # process (that left two copies of one worker logged in, one of them untracked).
        self.lock = asyncio.Lock()
        self.watch_task: asyncio.Task | None = None

    def ensure_watched(self) -> None:
        if self.watch_task is None or self.watch_task.done():
            self.watch_task = asyncio.create_task(_watch_worker(self))

    def session(self, guild_id: int) -> dict | None:
        s = self.sessions.get(guild_id)
        if s:
            return s
        p = self.pending.get(guild_id)
        if p and p[1] > time.monotonic():
            return p[0]
        return None

    def busy_in(self, guild_id: int) -> bool:
        return self.session(guild_id) is not None

    def reserve(self, guild_id: int, channel_id: int, user_id: int) -> None:
        snap = {"channel_id": channel_id, "controller_id": user_id, "mode": "me",
                "autoplay": False, "text_channel_id": None}
        self.pending[guild_id] = (snap, time.monotonic() + PENDING_TTL)

    def apply(self, guild_id: int, snap: dict | None) -> None:
        if snap:
            self.sessions[guild_id] = snap
        else:
            self.sessions.pop(guild_id, None)

    def clear_state(self) -> None:
        self.sessions.clear()
        self.pending.clear()

    def start(self):
        if self.socket_path.exists():
            self.socket_path.unlink(missing_ok=True)
        self.proc = subprocess.Popen(
            [PYTHON, "-u", str(WORKER_SCRIPT), "--index", str(self.index)],
            cwd=str(SCRIPT_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            self._pgid = os.getpgid(self.proc.pid)
        except Exception:
            self._pgid = None
        print(f"[Main] Started Worker {self.index} (PID {self.proc.pid} PGID {self._pgid})", flush=True)

        if self._output_task and not self._output_task.done():
            self._output_task.cancel()
        self._output_task = asyncio.ensure_future(self._stream_output())

        _spawn(self._reap())

    async def _stream_output(self):
        """Copy the worker's output into our log. This must never stop reading: if the pipe fills
        up, the worker blocks on its next print and freezes. Very long lines are split, not fatal."""
        if not self.proc or not self.proc.stdout:
            return
        loop = asyncio.get_event_loop()
        reader = asyncio.StreamReader(limit=1 << 20)
        protocol = asyncio.StreamReaderProtocol(reader)
        await loop.connect_read_pipe(lambda: protocol, self.proc.stdout)
        while True:
            try:
                line = await reader.readline()
            except (ValueError, asyncio.LimitOverrunError):
                line = await reader.read(1 << 16)   # over-long line: take a chunk and keep going
            if not line:
                return
            print(f"[Worker {self.index}] {line.decode(errors='replace').rstrip()}", flush=True)

    async def _reap(self):
        if not self.proc:
            return
        try:
            await asyncio.get_event_loop().run_in_executor(None, self.proc.wait)
        except Exception:
            pass

    def is_alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    async def terminate(self, timeout: float = 8.0):
        if self.proc is None:
            # Adopted from a previous controller (no process handle): ask it to exit over IPC
            # and wait for its socket to go away, so restarting it can't create a second copy.
            if self.socket_path.exists():
                await self.send({"op": "shutdown_graceful"}, timeout=5.0)
                for _ in range(int(timeout * 2)):
                    if not self.socket_path.exists():
                        break
                    await asyncio.sleep(0.5)
            return
        if not self.is_alive():
            return

        try:
            await asyncio.wait_for(
                self.send({"op": "shutdown_graceful"}, timeout=5.0), timeout=6.0
            )
        except Exception:
            pass
        if not self.is_alive():
            return

        try:
            if self._pgid:
                os.killpg(self._pgid, signal.SIGTERM)
            else:
                self.proc.terminate()
        except ProcessLookupError:
            pass

        try:
            await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(None, self.proc.wait),
                timeout=timeout,
            )
        except asyncio.TimeoutError:

            try:
                if self._pgid:
                    os.killpg(self._pgid, signal.SIGKILL)
                else:
                    self.proc.kill()
            except ProcessLookupError:
                pass

    async def restart(self):
        async with self.lock:
            print(f"[Main] Restarting Worker {self.index} ...", flush=True)
            self.clear_state()
            self._shutdown_requested = False
            await self.terminate()
            await asyncio.sleep(1)
            self.start()
        self.ensure_watched()   # a worker shut down earlier lost its watcher

    async def send(self, cmd: dict, timeout: float = 15.0) -> dict:
        for attempt in range(3):
            try:
                # Large limit: a get_queue reply for a few-hundred-song playlist is bigger than the
                # default 64 KB line limit, which made /control fail on long queues.
                reader, writer = await asyncio.wait_for(
                    asyncio.open_unix_connection(str(self.socket_path), limit=IPC_LINE_LIMIT),
                    timeout=5.0,
                )
                writer.write((json.dumps(cmd) + "\n").encode())
                await writer.drain()
                data = await asyncio.wait_for(reader.readline(), timeout=timeout)
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass
                if not data:
                    return {"status": "error", "message": "Worker closed the connection without replying (crashed or restarting?)"}
                return json.loads(data.decode())
            except (FileNotFoundError, ConnectionRefusedError):
                if attempt < 2:
                    await asyncio.sleep(1)
            except asyncio.TimeoutError:
                return {"status": "error", "message": "Worker timed out"}
            except Exception as exc:
                return {"status": "error", "message": str(exc)}
        return {"status": "error", "message": "Worker socket not available"}

_workers: dict[int, WorkerProcess] = {}
_shutting_down = False
_pick_lock = asyncio.Lock()

def _ordered_workers() -> list[WorkerProcess]:
    return sorted(_workers.values(), key=lambda x: x.index)

def _worker_for_channel(guild_id: int, channel_id: int) -> WorkerProcess | None:
    for w in _ordered_workers():
        s = w.session(guild_id)
        if s and s["channel_id"] == channel_id:
            return w
    return None

async def _resync_worker(w: WorkerProcess, timeout: float = 4.0) -> bool:
    """Ask the worker what it is really doing. The worker is the source of truth for busy/free."""
    resp = await w.send({"op": "sync"}, timeout=timeout)
    if resp.get("status") != "ok":
        # Down (shut down, crashed, restarting): it has no sessions and can't take new songs.
        # Keeping its last-known state made /play pick a dead worker instead of a working one.
        w.available = False
        w.sessions = {}
        return False
    w.available   = True
    w.user_id     = resp.get("user_id")
    w.guild_ids   = {int(g) for g in resp.get("guilds") or []}
    w.lavalink_ok = bool(resp.get("lavalink_ok"))
    w.sessions    = {int(g): s for g, s in (resp.get("sessions") or {}).items()}
    return True

async def _resync_all() -> None:
    await asyncio.gather(*[_resync_worker(w, timeout=3.0) for w in _workers.values()],
                         return_exceptions=True)


# ── worker → main state events ──────────────────────────────────────────────

def _request_stop() -> None:
    """Start the graceful shutdown (finish songs, play shutdown.mp3, exit).
    A second request skips the wait: songs are cut and the message plays now."""
    if _stop_event is None:
        return
    if _stop_event.is_set():
        print("[Main] Second stop request — cutting songs now", flush=True)
        if _force_drain:
            _force_drain.set()
        return
    _stop_event.set()

async def _handle_event(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    try:
        data = await asyncio.wait_for(reader.readline(), timeout=5.0)
        msg  = json.loads(data.decode())
        if msg.get("op") == "shutdown":
            # From launcher.py on `systemctl restart/stop`: acknowledge, then shut down gracefully
            # in the background so the launcher (and systemctl) can return immediately.
            print("[Main] Shutdown requested by the launcher — finishing current songs in the background", flush=True)
            writer.write((json.dumps({"status": "ok", "pid": os.getpid()}) + "\n").encode())
            await writer.drain()
            _request_stop()
        elif msg.get("op") == "state":
            w = _workers.get(int(msg["index"]))
            if w:
                w.apply(int(msg["guild_id"]), msg.get("session"))
        elif msg.get("op") == "panel":
            for st in _open_panels.get((int(msg["index"]), int(msg["guild_id"])), ()):
                st.wake.set()
        elif msg.get("op") == "lyrics":
            st = _private_lyrics.get((int(msg["index"]), int(msg["guild_id"])))
            if st:
                _spawn(st.apply(msg))
    except Exception:
        pass
    finally:
        writer.close()

async def _event_server():
    MAIN_SOCKET.unlink(missing_ok=True)
    server = await asyncio.start_unix_server(_handle_event, path=str(MAIN_SOCKET))
    async with server:
        await server.serve_forever()


COLOUR = discord.Colour.from_str("#5865F2")

def _embed_from_response(resp: dict) -> discord.Embed | None:
    et = resp.get("embed_type")
    if not et:
        return None

    if et in ("playing", "queued"):
        title   = resp.get("title", "Unknown")
        author  = resp.get("author", "")
        uri     = resp.get("uri", "")
        artwork = resp.get("artwork", "")
        dur_ms  = resp.get("duration", 0)
        lbl     = resp.get("source_label", "")
        qpos    = resp.get("queue_pos", 0)
        link    = (title or "").replace("[", "\\[").replace("]", "\\]")   # "[Official Video]" would end the link
        desc    = f"**[{link}]({uri})**\n{author}" if uri else f"**{title}**\n{author}"
        embed   = discord.Embed(
            title="▶️  Now Playing" if et == "playing" else "➕  Added to Queue",
            description=desc,
            colour=COLOUR if et == "playing" else discord.Colour.green(),
        )
        if et == "queued":
            embed.set_footer(text=f"Position in queue: #{qpos}")
        if artwork:
            embed.set_thumbnail(url=artwork)
        if dur_ms:
            m, s = divmod(dur_ms // 1000, 60)
            embed.add_field(name="Duration", value=f"{m}:{s:02d}", inline=True)
        if lbl:
            embed.add_field(name="Source", value=lbl, inline=True)
        return embed

    if et in ("playing_playlist", "queued_playlist"):
        pl_name = resp.get("pl_name", "Playlist")
        count   = resp.get("count", 0)
        artwork = resp.get("artwork", "")
        lbl     = resp.get("source_label", "")
        skipped = resp.get("skipped", 0)
        embed   = discord.Embed(
            title="▶️  Playing Playlist" if et == "playing_playlist" else "➕  Queued Playlist",
            description=f"**{pl_name}**",
            colour=COLOUR if et == "playing_playlist" else discord.Colour.green(),
        )
        embed.add_field(name="Tracks", value=str(count), inline=True)
        if lbl:
            embed.add_field(name="Source", value=lbl, inline=True)
        if skipped:
            embed.add_field(name="⚠️ Skipped", value=f"{skipped} local file(s)", inline=False)
        if artwork:
            embed.set_thumbnail(url=artwork)
        return embed

    return None

def _simple_embed(title: str, colour: discord.Colour) -> discord.Embed:
    return discord.Embed(title=title, colour=colour)

def _err_embed(msg: str) -> discord.Embed:
    return discord.Embed(title=f"❌  {msg}", colour=discord.Colour.red())

def _all_busy_embed(guild_id: int) -> discord.Embed:
    present = [w for w in _workers.values() if guild_id in w.guild_ids]
    busy = sum(1 for w in present if w.busy_in(guild_id))
    return discord.Embed(
        title="❌  No bots available",
        description=(f"{busy} of {len(present)} music bot(s) in this server are busy and the rest are reserved "
                     f"for other channels. Wait, use `/stop`, or pick one with the `worker` option."),
        colour=discord.Colour.red(),
    )

def _is_dev(user) -> bool:
    return appsettings.is_dev(user.id)

def _has_perm(user, perm: str) -> bool:
    perms = getattr(user, "guild_permissions", None)
    return bool(perms and getattr(perms, perm, False))

def _allowed(session: dict | None, user) -> bool:
    """Whoever started playback controls it, unless they opened it to everyone. Devs and members
    with Manage Server can always control (Me/All doesn't apply to them)."""
    if not session:
        return False
    return (_is_dev(user) or _has_perm(user, "manage_guild") or session.get("controller_id") == user.id
            or session.get("mode") == "all")

def _session_public(interaction: discord.Interaction, worker_arg: int = 0) -> bool:
    """True when the session this command goes to was made public with /control no_eph:true.
    Decided from the cached state (no worker call), since Discord needs the reply's privacy
    within 3 s. Only that session is affected; sessions in other channels keep their settings."""
    w = _workers.get(worker_arg) if worker_arg else None
    if w is None:
        vc = _user_vc_id(interaction)
        w = _worker_for_channel(interaction.guild_id, vc) if vc else None
    s = w.session(interaction.guild_id) if w else None
    return bool(s and s.get("no_eph"))

def _reply_eph(interaction: discord.Interaction, key: str, worker_arg: int = 0) -> bool:
    """Private reply? The server's setting for this command, unless the session is public."""
    return False if _session_public(interaction, worker_arg) else _eph(interaction.guild_id, key)

def _locked_msg(w: WorkerProcess, session: dict) -> str:
    return (f"🔒 Worker {w.index} is controlled by <@{session.get('controller_id')}>. "
            "Join another voice channel to get your own worker.")


class MainBot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self._cmds_synced = False

    async def setup_hook(self):
        # Commands are registered per server in on_ready (instant), not globally (can take a
        # while to show up in Discord apps).


        print("[Main] Clearing worker bot commands...", flush=True)
        for token in TOKENS[1:]:
            client = discord.Client(intents=discord.Intents.none())
            tree   = app_commands.CommandTree(client)
            try:
                await client.login(token)
                tree.clear_commands(guild=None)
                await tree.sync()
            except Exception as exc:
                print(f"[Main] Worker clear failed: {exc}", flush=True)
            finally:
                try:
                    await client.close()
                except Exception:
                    pass

main_bot = MainBot()


async def _enforce_whitelist(guild: discord.Guild) -> bool:
    if appsettings.is_whitelisted(guild.id):
        return True
    print(f"[Main] Leaving non-whitelisted guild {guild.id} ({guild.name})", flush=True)
    me = guild.me
    channel = guild.system_channel
    if not (channel and me and channel.permissions_for(me).send_messages):
        channel = next((c for c in guild.text_channels if me and c.permissions_for(me).send_messages), None)
    if channel:
        try:
            await channel.send(
                f"<@{guild.owner_id}> {appsettings.get()['leave_message']}",
                allowed_mentions=discord.AllowedMentions(users=True, everyone=False, roles=False),
            )
        except Exception as exc:
            print(f"[Main] Could not post leave notice in {guild.id}: {exc}", flush=True)
    try:
        await guild.leave()
    except Exception as exc:
        print(f"[Main] Could not leave {guild.id}: {exc}", flush=True)
    return False

@main_bot.event
async def on_ready():
    print(f"[Main] Logged in as {main_bot.user} — {len(_workers)} worker(s), {len(main_bot.guilds)} server(s)", flush=True)
    allowed = [g for g in list(main_bot.guilds) if await _enforce_whitelist(g)]
    if main_bot._cmds_synced:
        return   # on_ready also fires after reconnects
    main_bot._cmds_synced = True
    await asyncio.gather(*[_sync_guild(g) for g in allowed])
    try:
        # Remove the global copies so commands don't show up twice. Done over HTTP so the tree
        # keeps its global commands for copying to servers joined later.
        await main_bot.http.bulk_upsert_global_commands(main_bot.application_id, [])
    except Exception as exc:
        print(f"[Main] Could not clear global commands: {exc}", flush=True)

async def _sync_guild(guild: discord.Guild) -> None:
    """Register the slash commands on one server. Per-server commands update instantly."""
    try:
        main_bot.tree.copy_global_to(guild=guild)
        cmds = await main_bot.tree.sync(guild=guild)
        print(f"[Main] {len(cmds)} commands synced to {guild.name} ({guild.id})", flush=True)
    except Exception as exc:
        print(f"[Main] Command sync failed for {guild.id}: {exc}", flush=True)

@main_bot.event
async def on_guild_join(guild: discord.Guild):
    if await _enforce_whitelist(guild):
        print(f"[Main] Joined whitelisted guild {guild.id} ({guild.name})", flush=True)
        await _sync_guild(guild)


# ── dev prefix commands (``restart``, ``shutdown``, ``get-env``) ─────────────

_DEV_RE = re.compile(r"^(restart|shutdown|get-env|show-env)(?:\s+(\S+))?\s*$", re.IGNORECASE)

def _env_summary(guild: discord.Guild) -> str:
    gid   = guild.id
    lines = [f"**{guild.name}** (`{gid}`) — whitelisted: `{str(appsettings.is_whitelisted(gid)).lower()}`"]
    cc  = _cfg_cc(gid)
    cch = guild.get_channel(cc) if cc else None
    lines.append("`CC_ID` = " + (f"`{cc}` (#{cch.name if cch else 'unknown'})" if cc else "not set (any channel)"))
    lines.append(f"`SRC` = `{_cfg_src(gid)}` ({SOURCES[_cfg_src(gid)]})")
    for w in _ordered_workers():
        vc  = _cfg_vcw(gid, w.index)
        vch = guild.get_channel(vc) if vc else None
        s   = w.session(gid)
        state = ("🔴 busy" if s else "🟢 free") if gid in w.guild_ids else "⚫ not in server"
        lines.append(
            f"`VCW{w.index}` = "
            + (f"`{vc}` (🔊 {vch.name if vch else 'unknown'})" if vc else "not set (joins where the user is)")
            + f"  — {state}"
        )
    for k, label in EPH_KEYS.items():
        lines.append(f"`{k}` = `{str(_eph(gid, k)).lower()}`  — {label} replies {'private' if _eph(gid, k) else 'public'}")
    return "\n".join(lines)

@main_bot.event
async def on_message(message: discord.Message):
    if message.author.bot or not message.guild:
        return
    text = message.content.strip()
    if len(text) < 5 or not (text.startswith("``") and text.endswith("``")):
        return
    m = _DEV_RE.match(text.strip("`").strip())
    if not m or not _is_dev(message.author):
        return
    cmd, arg = m.group(1).lower(), (m.group(2) or "").lower()

    if cmd in ("get-env", "show-env"):
        await message.reply(_env_summary(message.guild), mention_author=False)
        return

    await _dev_power(message, cmd, arg)


SERVICE_NAME = "SERVER_musicbots"

async def _systemctl(action: str):
    # --no-block: queue the job and return. A blocking call would wait for this very process
    # to finish its graceful shutdown.
    proc = await asyncio.create_subprocess_exec(
        "systemctl", "--no-block", action, SERVICE_NAME,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    await proc.wait()

async def _delayed_systemctl(action: str, delay: float = 2.0):
    await asyncio.sleep(delay)
    await _systemctl(action)

async def _dev_power(message: discord.Message, cmd: str, arg: str) -> None:
    """``restart [n|full]`` / ``shutdown [n|full]`` — no argument means all workers."""
    restart = cmd == "restart"

    if arg == "full":
        action = "restart" if restart else "stop"
        embed = discord.Embed(
            title="🔄  Restarting entire service…" if restart else "🔴  Shutting down entire service…",
            description=(f"Running `systemctl {action} {SERVICE_NAME}`.\n"
                         + ("Current songs finish (max 5 min), the shutdown message plays, then everything restarts."
                            if restart else
                            f"Everything stops until `systemctl start {SERVICE_NAME}` is run on the server.")),
            colour=COLOUR if restart else discord.Colour.red(),
        )
        embed.set_footer(text="This message will not update.")
        await message.reply(embed=embed, mention_author=False)
        _spawn(_delayed_systemctl(action, delay=2.0))
        return

    if arg in ("", "all", "0"):
        targets = _ordered_workers()
    elif arg.isdigit() and int(arg) in _workers:
        targets = [_workers[int(arg)]]
    else:
        await message.reply(
            embed=_err_embed(f"Unknown target `{arg}`. Use a worker number ({', '.join(str(i) for i in sorted(_workers))}), "
                             "`full`, or nothing for all workers"),
            mention_author=False)
        return

    label = f"all {len(targets)} worker(s)" if len(targets) > 1 else f"Worker {targets[0].index}"
    status = await message.reply(
        embed=discord.Embed(
            title=f"{'🔄  Restarting' if restart else '🔴  Shutting down'} {label}…",
            description=("Stopping music in every server and reconnecting." if restart
                         else "Stopping music in every server. Workers will **not** restart."),
            colour=COLOUR if restart else discord.Colour.red(),
        ),
        mention_author=False,
    )

    lines: list[str] = []
    for w in targets:
        try:
            if restart:
                await w.restart()
                for _ in range(15):
                    await asyncio.sleep(1)
                    resp = await w.send({"op": "ping"}, timeout=3.0)
                    if resp.get("status") == "ok":
                        lines.append(f"✅ Worker {w.index} — online (Lavalink: {'✅' if resp.get('lavalink_ok') else '❌'})")
                        break
                else:
                    lines.append(f"⚠️ Worker {w.index} — started but socket not yet ready")
            else:
                w._shutdown_requested = True
                await w.terminate()
                w.clear_state()
                lines.append(f"🔴 Worker {w.index} — shut down")
        except Exception as exc:
            lines.append(f"❌ Worker {w.index} — error: {exc}")

    if restart:
        colour = discord.Colour.green() if all("✅" in l for l in lines) else discord.Colour.orange()
    else:
        colour = discord.Colour.red()
    result = discord.Embed(title=f"{'🔄  Restart' if restart else '🔴  Shutdown'} complete — {label}",
                           description="\n".join(lines), colour=colour)
    result.set_footer(text="``restart <n>`` brings workers back. ``restart full`` / ``shutdown full`` affect the whole service.")
    try:
        await status.edit(embed=result)
    except Exception:
        await message.channel.send(embed=result)


# ── shared slash-command helpers ─────────────────────────────────────────────

async def _guard(interaction: discord.Interaction, during_restart: bool = False) -> bool:
    """during_restart: the command still works while a restart lets songs finish (/stop,
    /control). Only commands that start playback are blocked then."""
    if not interaction.guild_id:
        await interaction.response.send_message(embed=_err_embed("Server only"), ephemeral=True)
        return False
    if _draining and not during_restart:
        await interaction.response.send_message(
            embed=_err_embed("The music bots are restarting — try again in a few minutes"), ephemeral=True)
        return False
    if not appsettings.is_whitelisted(interaction.guild_id):
        await interaction.response.send_message(embed=_err_embed("This server is not whitelisted"), ephemeral=True)
        return False
    cc = _cfg_cc(interaction.guild_id)
    if cc and interaction.channel_id != cc:
        await interaction.response.send_message(embed=_err_embed(f"Use the control channel: <#{cc}>"), ephemeral=True)
        return False
    return True

def _text_channel_id(interaction: discord.Interaction) -> int | None:
    return _cfg_cc(interaction.guild_id) or interaction.channel_id

def _user_vc_id(interaction: discord.Interaction) -> int | None:
    m = interaction.user
    if isinstance(m, discord.Member) and m.voice and m.voice.channel:
        return m.voice.channel.id
    return None

async def _fail(interaction: discord.Interaction, msg: str, embed: discord.Embed | None = None) -> None:
    await interaction.followup.send(embed=embed or _err_embed(msg), ephemeral=True)

async def _pick_for_play(interaction: discord.Interaction, worker_arg: int):
    """Pick (and reserve) the worker for a /play-style command. Returns (worker, voice channel id)."""
    guild_id = interaction.guild_id
    user     = interaction.user
    user_vc  = _user_vc_id(interaction)

    async with _pick_lock:
        # Fresh state from every worker, so a worker that went idle on its own is seen as free.
        await _resync_all()
        present = [w for w in _ordered_workers() if guild_id in w.guild_ids and w.available]
        if not present:
            if any(guild_id in w.guild_ids for w in _workers.values()):
                await _fail(interaction, "The music workers are restarting — try again in a moment")
            else:
                await _fail(interaction, "No music workers are in this server — invite the worker bots first")
            return None

        if worker_arg:
            w = _workers.get(worker_arg)
            if not w:
                await _fail(interaction, f"No worker #{worker_arg}. Valid: {', '.join(str(i) for i in sorted(_workers))}")
                return None
            if guild_id not in w.guild_ids:
                await _fail(interaction, f"Worker {w.index} isn't in this server")
                return None
            if not w.available:
                await _fail(interaction, f"Worker {w.index} is down right now — try another one or wait a moment")
                return None
            s = w.session(guild_id)
            if s:
                if not _allowed(s, user):
                    await _fail(interaction, _locked_msg(w, s))
                    return None
                return w, s["channel_id"]
            target = _cfg_vcw(guild_id, w.index) or user_vc
            if target is None:
                await _fail(interaction, f"Worker {w.index} has no fixed channel — join a voice channel first")
                return None
            other = _worker_for_channel(guild_id, target)
            if other:
                await _fail(interaction, f"Worker {other.index} is already playing in that channel")
                return None
            w.reserve(guild_id, target, user.id)
            return w, target

        if user_vc:
            w = _worker_for_channel(guild_id, user_vc)
            if w:
                s = w.session(guild_id)
                if not _allowed(s, user):
                    await _fail(interaction, _locked_msg(w, s))
                    return None
                return w, user_vc
            free = [w for w in present if not w.busy_in(guild_id)]
            for w in free:
                if _cfg_vcw(guild_id, w.index) == user_vc:
                    w.reserve(guild_id, user_vc, user.id)
                    return w, user_vc
            for w in free:
                if _cfg_vcw(guild_id, w.index) is None:
                    w.reserve(guild_id, user_vc, user.id)
                    return w, user_vc
            await interaction.followup.send(embed=_all_busy_embed(guild_id), ephemeral=True)
            return None

        for w in present:
            vcw = _cfg_vcw(guild_id, w.index)
            if not w.busy_in(guild_id) and vcw:
                w.reserve(guild_id, vcw, user.id)
                return w, vcw
        await _fail(interaction, "Join a voice channel first")
        return None

async def _pick_for_control(interaction: discord.Interaction, worker_arg: int, for_stop: bool = False):
    """Find the session the user means and check they may control it. Returns (worker, session)."""
    guild_id = interaction.guild_id
    user     = interaction.user
    is_mod   = for_stop and _has_perm(user, "manage_guild")
    await _resync_all()

    if worker_arg:
        w = _workers.get(worker_arg)
        if not w:
            await _fail(interaction, f"No worker #{worker_arg}. Valid: {', '.join(str(i) for i in sorted(_workers))}")
            return None
        s = w.session(guild_id)
        if not s:
            await _fail(interaction, f"Worker {w.index} isn't playing anything here")
            return None
    else:
        user_vc = _user_vc_id(interaction)
        w = _worker_for_channel(guild_id, user_vc) if user_vc else None
        if w:
            s = w.session(guild_id)
        else:
            busy = [(x, x.session(guild_id)) for x in _ordered_workers() if x.busy_in(guild_id)]
            usable = [(x, s) for x, s in busy if _allowed(s, user)] or (busy if is_mod else [])
            if not busy:
                await _fail(interaction, "Nothing is playing")
                return None
            if not usable:
                await _fail(interaction, "Nothing you control is playing")
                return None
            if len(usable) > 1:
                await _fail(interaction, "Several bots are playing — pick one with the `worker` option")
                return None
            w, s = usable[0]

    if not (_allowed(s, user) or is_mod):
        await _fail(interaction, _locked_msg(w, s))
        return None
    return w, s

_MESSAGE_EMBEDS = {
    "stopped":      ("⏹️  Stopped", discord.Colour.red()),
    "paused":       ("⏸️  Paused", COLOUR),
    "resumed":      ("▶️  Resumed", COLOUR),
    "skipped":      ("⏭️  Skipped", COLOUR),
    "restarted":    ("⏮️  Restarted track", COLOUR),
    "muted":        ("🔇  Muted — bot will play silently", COLOUR),
    "unmuted":      ("🔊  Unmuted", COLOUR),
    "loop_on":      ("🔁  Loop ON — current track will repeat", COLOUR),
    "loop_off":     ("➡️  Loop OFF", COLOUR),
    "autoplay_on":  ("🎲  Autoplay ON — I'll keep picking songs when the queue runs out", COLOUR),
    "autoplay_off": ("🎲  Autoplay OFF", COLOUR),
    "mode_all":     ("🔓  Control: everyone in the voice channel", COLOUR),
    "mode_me":      ("🔒  Control: only the person who started playback", COLOUR),
    "normalize_on":  ("🎚️  Normalize ON — loud and quiet songs are evened out", COLOUR),
    "normalize_off": ("🎚️  Normalize OFF", COLOUR),
}

def _message_embed(resp: dict) -> discord.Embed | None:
    msg = resp.get("message", "")
    if msg in _MESSAGE_EMBEDS:
        title, colour = _MESSAGE_EMBEDS[msg]
        return _simple_embed(title, colour)
    if msg == "seeked":
        delta = resp.get("delta_ms", 0)
        sign  = "+" if delta > 0 else ""
        icon  = "⏩" if delta > 0 else "⏪"
        return _simple_embed(f"{icon}  {sign}{delta//1000}s", COLOUR)
    return None

async def _dispatch(interaction: discord.Interaction, cmd: dict, worker: WorkerProcess,
                    eph: bool = False, timeout: float = 30.0) -> dict:
    resp = await worker.send(cmd, timeout=timeout)
    if resp.get("status") == "error":
        await interaction.followup.send(embed=_err_embed(resp.get("message", "Unknown error")), ephemeral=eph)
        return resp
    embed = _embed_from_response(resp) or _message_embed(resp)
    if embed:
        await interaction.followup.send(embed=embed, ephemeral=eph)
    return resp

async def _start_playback(interaction: discord.Interaction, op: str, eph_key: str,
                          worker_arg: int, timeout: float = 30.0, **extra) -> None:
    """Shared body of /play, /playlist and /autoplay."""
    eph = _reply_eph(interaction, eph_key, worker_arg)
    await interaction.response.defer(thinking=True, ephemeral=eph)
    pick = await _pick_for_play(interaction, worker_arg)
    if pick is None:
        return
    w, channel_id = pick
    guild_id = interaction.guild_id
    try:
        resp = await _dispatch(interaction, {
            "op": op, "guild_id": guild_id, "channel_id": channel_id,
            "text_channel_id": _text_channel_id(interaction),
            "user_id": interaction.user.id,
            # Private command -> the worker won't post public "Now Playing" messages either.
            "announce": not eph, **extra,
        }, w, eph, timeout=timeout)
    finally:
        w.pending.pop(guild_id, None)
    if resp.get("session"):
        w.apply(guild_id, resp["session"])

_source_choices = [app_commands.Choice(name=label, value=key) for key, label in SOURCES.items()]
# Search (Spotify + YouTube Music + AI) plus joining voice and fetching the stream can exceed 30 s
# on a slow moment; the interaction itself stays valid for 15 minutes.
PLAY_TIMEOUT = 90.0


@main_bot.tree.command(name="play", description="Play a song — name or URL (YouTube / SoundCloud / Spotify)")
@app_commands.describe(query="Song name or URL", source="Search source (ignored for URLs; default set in /settings)",
                       worker="Force a specific worker bot (0 = automatic)")
@app_commands.choices(source=_source_choices)
async def slash_play(interaction: discord.Interaction, query: str,
                     source: app_commands.Choice[str] | None = None, worker: int = 0):
    if not await _guard(interaction): return
    await _start_playback(interaction, "search_and_play", "P_EPH", worker, timeout=PLAY_TIMEOUT,
                          query=query, source=source.value if source else _cfg_src(interaction.guild_id))


@main_bot.tree.command(name="playlist", description="Queue a full playlist — YouTube, SoundCloud, or Spotify URL")
@app_commands.describe(query="Playlist URL or search term", source="Search source (ignored for URLs; default set in /settings)",
                       worker="Force a specific worker bot (0 = automatic)")
@app_commands.choices(source=_source_choices)
async def slash_playlist(interaction: discord.Interaction, query: str,
                         source: app_commands.Choice[str] | None = None, worker: int = 0):
    if not await _guard(interaction): return
    await _start_playback(interaction, "search_and_playlist", "PL_EPH", worker, timeout=PLAY_TIMEOUT,
                          query=query, source=source.value if source else _cfg_src(interaction.guild_id))


@main_bot.tree.command(name="autoplay", description="Autoplay music picked from your saved listening history")
@app_commands.describe(worker="Force a specific worker bot (0 = automatic)")
async def slash_autoplay(interaction: discord.Interaction, worker: int = 0):
    if not await _guard(interaction): return
    await _start_playback(interaction, "autoplay_start", "AP_EPH", worker, timeout=PLAY_TIMEOUT)


@main_bot.tree.command(name="stop", description="Stop music and disconnect")
@app_commands.describe(worker="Which worker to stop (0 = the one in your channel)")
async def slash_stop(interaction: discord.Interaction, worker: int = 0):
    if not await _guard(interaction, during_restart=True): return
    eph = _reply_eph(interaction, "S_EPH", worker)
    await interaction.response.defer(thinking=True, ephemeral=eph)
    pick = await _pick_for_control(interaction, worker, for_stop=True)
    if pick is None: return
    w, _s = pick
    resp = await w.send({"op": "stop", "guild_id": interaction.guild_id})
    if resp.get("status") == "ok":
        w.apply(interaction.guild_id, None)
        await interaction.followup.send(embed=_simple_embed("⏹️  Stopped", discord.Colour.red()), ephemeral=eph)
    else:
        await interaction.followup.send(embed=_err_embed(resp.get("message", "Error")), ephemeral=eph)


PAGE_SIZE = 25

def _truncate(text: str, n: int = 80) -> str:
    return text if len(text) <= n else text[:n - 1] + "…"


class QueueSelect(discord.ui.Select):

    def __init__(self, view: "ControlView", queue: list[dict],
                 page: int, total_pages: int, current_title: str,
                 selected_qid: int | None = None):
        self._ctrl = view
        self._page = page

        start  = page * PAGE_SIZE
        slice_ = queue[start : start + PAGE_SIZE]

        options: list[discord.SelectOption] = []
        if not queue:
            options.append(discord.SelectOption(
                label="Queue is empty",
                value="__empty__",
                description="No upcoming tracks",
                emoji="🎵",
            ))
        else:
            for item in slice_:
                idx      = item["index"]
                label    = _truncate(f"#{idx + 1}  {item['title']}", 100)
                desc     = _truncate(item["author"], 100) if item.get("author") else None
                selected = (item["qid"] == selected_qid)
                options.append(discord.SelectOption(
                    label=label, value=str(item["qid"]), description=desc,
                    emoji="▶️" if selected else ("🎲" if item.get("autoplay") else "🎶"),
                    default=selected,
                ))

        page_info = f"  [pg {page + 1}/{total_pages}]" if total_pages > 1 else ""
        sel_item = next((q for q in queue if q["qid"] == selected_qid), None) if selected_qid is not None else None
        if sel_item:
            sel_title = _truncate(sel_item["title"], 40)
            placeholder = f"✅ #{sel_item['index'] + 1} {sel_title}{page_info} — choose action below"
        else:
            placeholder = f"▶ {_truncate(current_title, 45)}{page_info} — select a track…"

        super().__init__(
            placeholder=placeholder,
            min_values=0, max_values=1,
            options=options,
            row=2,
        )

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=self._ctrl.eph)
        if self.values and self.values[0] == "__empty__":
            return
        # Nothing chosen = the user cleared the selection: Jump To / Remove must not act on the old one.
        self._ctrl._selected_qid = int(self.values[0]) if self.values else None
        await _refresh_control_panel(interaction, self._ctrl,
                                     page=self._ctrl._queue_page,
                                     selected_qid=self._ctrl._selected_qid)


class JumpToButton(discord.ui.Button):
    def __init__(self, view: "ControlView"):
        self._ctrl = view
        has_sel    = view._selected_qid is not None
        super().__init__(
            label="⏭ Jump To", style=discord.ButtonStyle.primary,
            disabled=not has_sel, row=3,
        )

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=self._ctrl.eph)
        resp = await self._ctrl.worker.send({
            "op": "jump_to", "guild_id": self._ctrl.guild_id, "qid": self._ctrl._selected_qid,
        }, timeout=30.0)
        if resp.get("status") == "error":
            await interaction.followup.send(embed=_err_embed(resp.get("message", "Error")), ephemeral=self._ctrl.eph)
        else:
            title = resp.get("title", "track")
            rem   = resp.get("queue_remaining", 0)
            await interaction.followup.send(
                embed=_simple_embed(f"⏭  Jumped to **{_truncate(title, 60)}** — {rem} remaining", COLOUR),
                ephemeral=self._ctrl.eph,
            )
        await _refresh_control_panel(interaction, self._ctrl, page=0, selected_qid=None)


class RemoveButton(discord.ui.Button):
    def __init__(self, view: "ControlView"):
        self._ctrl = view
        has_sel    = view._selected_qid is not None
        super().__init__(
            label="🗑 Remove", style=discord.ButtonStyle.danger,
            disabled=not has_sel, row=3,
        )

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=self._ctrl.eph)
        resp = await self._ctrl.worker.send({
            "op": "remove_from_queue", "guild_id": self._ctrl.guild_id, "qid": self._ctrl._selected_qid,
        }, timeout=15.0)
        if resp.get("status") == "error":
            await interaction.followup.send(embed=_err_embed(resp.get("message", "Error")), ephemeral=self._ctrl.eph)
        else:
            title = resp.get("title", "track")
            rem   = resp.get("queue_remaining", 0)
            await interaction.followup.send(
                embed=_simple_embed(f"🗑  Removed **{_truncate(title, 60)}** — {rem} remaining", discord.Colour.orange()),
                ephemeral=self._ctrl.eph,
            )

        await _refresh_control_panel(interaction, self._ctrl,
                                     page=self._ctrl._queue_page, selected_qid=None)


class ClearSelectionButton(discord.ui.Button):
    def __init__(self, view: "ControlView"):
        self._ctrl = view
        super().__init__(
            label="✖ Clear", style=discord.ButtonStyle.secondary,
            disabled=(view._selected_qid is None), row=3,
        )

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=self._ctrl.eph)
        await _refresh_control_panel(interaction, self._ctrl,
                                     page=self._ctrl._queue_page, selected_qid=None)


class AutoplayButton(discord.ui.Button):
    def __init__(self, view: "ControlView"):
        self._ctrl = view
        on = view._autoplay
        super().__init__(label="🎲 Autoplay ON" if on else "🎲 Autoplay",
                         style=discord.ButtonStyle.success if on else discord.ButtonStyle.secondary,
                         row=3, custom_id=f"ctl_autoplay:{view.tag}")

    async def callback(self, interaction: discord.Interaction):
        await self._ctrl._dispatch(interaction, "toggle_autoplay", timeout=45.0)


class ModeButton(discord.ui.Button):
    def __init__(self, view: "ControlView"):
        self._ctrl = view
        everyone = view._mode == "all"
        super().__init__(label="🔓 Control: All" if everyone else "🔒 Control: Me",
                         style=discord.ButtonStyle.success if everyone else discord.ButtonStyle.secondary,
                         row=3, custom_id=f"ctl_mode:{view.tag}")

    async def callback(self, interaction: discord.Interaction):
        new_mode = "me" if self._ctrl._mode == "all" else "all"
        await self._ctrl._dispatch(interaction, "set_mode", mode=new_mode)


class NormalizeButton(discord.ui.Button):
    def __init__(self, view: "ControlView"):
        self._ctrl = view
        on = view._normalize
        super().__init__(label="🎚️ Normalize ON" if on else "🎚️ Normalize",
                         style=discord.ButtonStyle.success if on else discord.ButtonStyle.secondary, row=4)

    async def callback(self, interaction: discord.Interaction):
        await self._ctrl._dispatch(interaction, "toggle_normalize")


class LyricsButton(discord.ui.Button):
    def __init__(self, view: "ControlView"):
        self._ctrl = view
        on = view._lyrics
        super().__init__(label="🎤 Lyrics ON" if on else "🎤 Lyrics",
                         style=discord.ButtonStyle.success if on else discord.ButtonStyle.secondary, row=4)

    async def callback(self, interaction: discord.Interaction):
        ctrl = self._ctrl
        turn_on = not ctrl._lyrics
        if turn_on:
            # Ready before the worker is asked, so no update of private lyrics can arrive first.
            key = (ctrl.worker.index, ctrl.guild_id)
            old = _private_lyrics.get(key)
            _private_lyrics[key] = PrivateLyrics(interaction, ctrl.worker, ctrl.guild_id)
            if old:
                _spawn(old.retire())
        await ctrl._dispatch(interaction, "toggle_lyrics", on=turn_on)


class PrevPageButton(discord.ui.Button):
    def __init__(self, view: "ControlView"):
        self._ctrl = view
        super().__init__(label="◀ Prev", style=discord.ButtonStyle.secondary,
                         disabled=(view._queue_page == 0), row=4)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=self._ctrl.eph)
        await _refresh_control_panel(interaction, self._ctrl,
                                     page=self._ctrl._queue_page - 1, selected_qid=None)


class PageLabelButton(discord.ui.Button):
    def __init__(self, page: int, total: int):
        super().__init__(label=f"Page {page + 1} / {total}",
                         style=discord.ButtonStyle.secondary, disabled=True, row=4)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer()


class NextPageButton(discord.ui.Button):
    def __init__(self, view: "ControlView", total_pages: int):
        self._ctrl  = view
        self._total = total_pages
        super().__init__(label="Next ▶", style=discord.ButtonStyle.secondary,
                         disabled=(view._queue_page >= total_pages - 1), row=4)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=self._ctrl.eph)
        await _refresh_control_panel(interaction, self._ctrl,
                                     page=self._ctrl._queue_page + 1, selected_qid=None)


class ControlView(discord.ui.View):
    def __init__(self, worker: WorkerProcess, guild_id: int,
                 queue: list[dict], current_title: str, page: int = 0,
                 muted: bool = False, loop: bool = False, autoplay: bool = False,
                 mode: str = "me", selected_qid: int | None = None, normalize: bool = False,
                 lyrics: bool = False):
        super().__init__(timeout=900)
        # Every refresh puts a new view on the same message and retires the old one. discord.py
        # forgets the old view's buttons by custom_id for that message, so buttons with the same
        # id in every version (Stop, Autoplay, Control) were unhooked from the new view too and
        # stopped working after the first refresh. Ids are unique per version instead.
        self.tag             = os.urandom(6).hex()
        self.btn_stop.custom_id = f"ctl_stop:{self.tag}"
        self.worker          = worker
        self.guild_id        = guild_id
        self.eph             = False if (worker.session(guild_id) or {}).get("no_eph") else _eph(guild_id, "CC_EPH")
        self._stopped        = False
        self._queue_page     = page
        self._queue          = queue
        self._current_title  = current_title
        self._muted          = muted
        self._loop           = loop
        self._autoplay       = autoplay
        self._mode           = mode
        self._selected_qid   = selected_qid
        self._normalize      = normalize
        self._lyrics         = lyrics
        self.state: PanelState | None = None
        self.sig = None

        total_pages = max(1, -(-len(queue) // PAGE_SIZE))


        self.add_item(QueueSelect(self, queue, page, total_pages, current_title, selected_qid))


        self.add_item(JumpToButton(self))
        self.add_item(RemoveButton(self))
        self.add_item(ClearSelectionButton(self))
        self.add_item(AutoplayButton(self))
        self.add_item(ModeButton(self))


        self.add_item(NormalizeButton(self))
        self.add_item(LyricsButton(self))
        if total_pages > 1:
            self.add_item(PrevPageButton(self))
            self.add_item(PageLabelButton(page, total_pages))
            self.add_item(NextPageButton(self, total_pages))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        s = self.state.session() if self.state else self.worker.session(self.guild_id)
        if not s:
            await interaction.response.send_message(
                embed=_err_embed("This session has ended — use /play or /control again"), ephemeral=True)
            return False
        custom_id = (interaction.data or {}).get("custom_id", "")
        user = interaction.user
        if custom_id.startswith("ctl_mode:"):
            # Only the person who started playback can hand control to everyone (or take it back).
            if s.get("controller_id") == user.id or _is_dev(user) or _has_perm(user, "manage_guild"):
                return True
            await interaction.response.send_message(
                embed=_err_embed("Only the person who started playback can change who controls it"), ephemeral=True)
            return False
        if _allowed(s, user):
            return True
        if custom_id.startswith("ctl_stop:") and _has_perm(user, "manage_guild"):
            return True
        await interaction.response.send_message(embed=_err_embed(_locked_msg(self.worker, s)), ephemeral=True)
        return False

    async def _dispatch(self, interaction: discord.Interaction, op: str,
                        timeout: float = 15.0, **extra) -> dict:
        await interaction.response.defer(ephemeral=self.eph)
        resp = await self.worker.send({"op": op, "guild_id": self.guild_id, **extra}, timeout=timeout)
        if resp.get("status") == "error":
            await interaction.followup.send(
                embed=_err_embed(resp.get("message", "Error")), ephemeral=self.eph)
            return resp
        if resp.get("session"):
            self.worker.apply(self.guild_id, resp["session"])
        embed = _message_embed(resp)
        if embed:
            await interaction.followup.send(embed=embed, ephemeral=self.eph)
        if resp.get("message") in ("skipped", "restarted", "muted", "unmuted", "loop_on", "loop_off",
                                   "autoplay_on", "autoplay_off", "mode_all", "mode_me",
                                   "normalize_on", "normalize_off", "paused", "resumed",
                                   "lyrics_on", "lyrics_off"):
            await _refresh_control_panel(interaction, self, page=self._queue_page, selected_qid=None)
        return resp

    @discord.ui.button(label="⏪ 10s", style=discord.ButtonStyle.secondary, row=0)
    async def rw10(self, i, _): await self._dispatch(i, "seek", delta_ms=-10_000)

    @discord.ui.button(label="⏪ 5s", style=discord.ButtonStyle.secondary, row=0)
    async def rw5(self, i, _): await self._dispatch(i, "seek", delta_ms=-5_000)

    @discord.ui.button(label="⏩ 5s", style=discord.ButtonStyle.secondary, row=0)
    async def ff5(self, i, _): await self._dispatch(i, "seek", delta_ms=5_000)

    @discord.ui.button(label="⏩ 10s", style=discord.ButtonStyle.secondary, row=0)
    async def ff10(self, i, _): await self._dispatch(i, "seek", delta_ms=10_000)

    @discord.ui.button(label="🔁 Loop", style=discord.ButtonStyle.secondary, row=0)
    async def btn_loop(self, i, _): await self._dispatch(i, "toggle_loop")

    @discord.ui.button(label="⏮ Backward", style=discord.ButtonStyle.primary, row=1)
    async def btn_backward(self, i, _): await self._dispatch(i, "backward")

    @discord.ui.button(label="⏸ Pause", style=discord.ButtonStyle.primary, row=1)
    async def btn_pause(self, i, _): await self._dispatch(i, "pause_resume")

    @discord.ui.button(label="⏹ Stop", style=discord.ButtonStyle.danger, row=1, custom_id="ctl_stop:")
    async def btn_stop(self, i, _):
        resp = await self._dispatch(i, "stop")
        if resp.get("status") == "ok" and not self._stopped:
            self._stopped = True
            self.worker.apply(self.guild_id, None)
            if self.state:
                # Shows "Stopped" for a moment, then the panel is deleted (it can't do anything now).
                _spawn(self.state.retire("⏹️  Stopped — use /play or /control again"))
                return
            for child in self.children: child.disabled = True
            try:
                await i.edit_original_response(
                    embed=_simple_embed("⏹️  Stopped — use /play or /control again", discord.Colour.dark_grey()),
                    view=self)
            except Exception:
                pass
            self.stop()

    @discord.ui.button(label="⏭ Skip", style=discord.ButtonStyle.primary, row=1)
    async def btn_skip(self, i, _): await self._dispatch(i, "skip", timeout=45.0)

    @discord.ui.button(label="🔇 Mute", style=discord.ButtonStyle.secondary, row=1)
    async def btn_mute(self, i, _): await self._dispatch(i, "toggle_mute")


PANEL_REFRESH  = 4.0       # seconds between /control panel checks (the worker also pushes changes right away)
PANEL_END_NOTE = 5.0       # seconds an ended panel shows "Playback ended" before it's deleted
PANEL_LIFETIME = 14 * 60   # Discord allows editing the panel for 15 min; it marks itself expired at 14

class PanelState:
    """One open /control panel: the message, its newest view and what it last showed, so it can
    redraw itself when the song or queue changes (not only when a button is pressed)."""
    def __init__(self, worker: "WorkerProcess", guild_id: int):
        self.worker   = worker
        self.guild_id = guild_id
        self.message  = None
        self.view: "ControlView | None" = None
        self.sig      = None
        self.created  = time.monotonic()
        self.wake     = asyncio.Event()   # set by the worker's "panel" event: something changed
        self.public   = False             # not ephemeral: deleted when it expires
        # The session this panel belongs to. A later session on the same worker gets a new id, so
        # an old panel can't end up controlling it.
        self.sid      = (worker.session(guild_id) or {}).get("sid")

    def session(self) -> dict | None:
        """This panel's session, or None once it ended (or another session replaced it)."""
        s = self.worker.session(self.guild_id)
        if not s or (self.sid is not None and s.get("sid") not in (None, self.sid)):
            return None
        return s

    async def ended(self) -> bool:
        """Confirmed with the worker: the cached state can be empty after a missed sync, and a
        panel must never be deleted while its session is still playing."""
        if self.session():
            return False
        resp = await self.worker.send({"op": "get_queue", "guild_id": self.guild_id}, timeout=5.0)
        if resp.get("status") != "ok":
            return False   # worker not answering right now: decide on a later check
        s = resp.get("session")
        if s:
            self.worker.apply(self.guild_id, s)
        return not s or (self.sid is not None and s.get("sid") != self.sid)

    async def retire(self, note: str | None, delay: float | None = None) -> None:
        """Panel is done: show the note for a moment, then delete the message."""
        _open_panels.get((self.worker.index, self.guild_id), set()).discard(self)
        if self.view is not None:
            self.view.stop()
        try:
            if note:
                await self.message.edit(embed=_simple_embed(note, discord.Colour.dark_grey()), view=None)
                await asyncio.sleep(PANEL_END_NOTE if delay is None else delay)
            await self.message.delete()
        except Exception:
            pass

    def minutes_left(self) -> int:
        return max(1, math.ceil((self.created + PANEL_LIFETIME - time.monotonic()) / 60))

    def show(self, view: "ControlView") -> None:
        if self.view is not None and self.view is not view:
            self.view.stop()
        self.view, self.sig = view, view.sig

class PrivateLyrics:
    """Lyrics for privately started playback. The worker sends each update as an event; they're shown
    as an ephemeral follow-up to the 🎤 click (one per song, deleted when the song ends). Discord
    only allows follow-ups for 15 minutes after a click, so private lyrics end at 14 with a notice."""

    def __init__(self, interaction: discord.Interaction, worker: "WorkerProcess", guild_id: int):
        self.interaction, self.worker, self.guild_id = interaction, worker, guild_id
        self.started = time.monotonic()
        self.msg = None
        self.song = None
        self.seq = 0
        self.lock = asyncio.Lock()
        self.expired = False

    async def apply(self, ev: dict) -> None:
        async with self.lock:
            if ev.get("seq", 0) <= self.seq:
                return   # an older update that arrived late
            self.seq = ev["seq"]
            try:
                if ev.get("delete"):
                    if self.song == ev.get("song"):
                        await self._drop()
                    return
                if self.expired:
                    return
                if time.monotonic() - self.started > PANEL_LIFETIME:
                    await self._expire()
                    return
                # Plain text (lyric_safe) or an embed, as the worker built it.
                fields = ({"embed": discord.Embed.from_dict(ev["embed"])} if ev.get("embed")
                          else {"content": ev.get("content") or ""})
                if self.msg is not None and self.song == ev.get("song"):
                    await self.msg.edit(**fields)
                else:
                    await self._drop()
                    self.msg = await self.interaction.followup.send(**fields, ephemeral=True, wait=True)
                    self.song = ev.get("song")
            except Exception as exc:
                print(f"[Main] Private lyrics update failed: {exc}", flush=True)

    async def _drop(self) -> None:
        if self.msg is not None:
            try:
                await self.msg.delete()
            except Exception:
                pass
            self.msg = None

    async def _expire(self) -> None:
        self.expired = True
        notice = "🎤 **Private lyrics expired** — press 🎤 Lyrics in /control again."
        try:
            if self.msg is not None:
                await self.msg.edit(content=notice, embed=None)
            else:
                await self.interaction.followup.send(content=notice, ephemeral=True)
        except Exception:
            pass
        self.msg = None   # the notice stays; it isn't deleted with the song
        _spawn(self.worker.send({"op": "toggle_lyrics", "guild_id": self.guild_id, "on": False}))

    async def retire(self) -> None:
        """Replaced by a newer 🎤 click."""
        async with self.lock:
            self.expired = True
            await self._drop()

_private_lyrics: dict[tuple[int, int], PrivateLyrics] = {}
_open_panels: dict[tuple[int, int], set[PanelState]] = {}   # (worker, guild) -> open /control panels


async def _build_control_view(worker: WorkerProcess, guild_id: int,
                               page: int = 0,
                               selected_qid: int | None = None,
                               state: PanelState | None = None) -> tuple[discord.Embed, ControlView]:
    resp = await worker.send({"op": "get_queue", "guild_id": guild_id}, timeout=10.0)
    ok             = resp.get("status") == "ok"
    queue: list[dict] = resp.get("queue", []) if ok else []
    current_title: str = (resp.get("current") or {}).get("title", "Unknown") if ok else "Unknown"
    muted: bool    = resp.get("muted", False) if ok else False
    loop:  bool    = resp.get("loop",  False) if ok else False
    autoplay: bool = resp.get("autoplay", False) if ok else False
    normalize: bool = resp.get("normalize", False) if ok else False
    paused: bool   = resp.get("paused", False) if ok else False
    lyrics_on: bool = resp.get("lyrics", False) if ok else False
    session        = (resp.get("session") if ok else None) or worker.session(guild_id) or {}
    if ok:
        worker.apply(guild_id, resp.get("session"))
    mode           = session.get("mode", "me")
    controller     = session.get("controller_id")

    total_pages = max(1, -(-len(queue) // PAGE_SIZE))
    page        = max(0, min(page, total_pages - 1))


    # The selected song may have played or been removed since it was picked.
    sel_item = next((q for q in queue if q["qid"] == selected_qid), None) if selected_qid is not None else None
    if sel_item is None:
        selected_qid = None

    view  = ControlView(worker, guild_id, queue, current_title,
                        page, muted, loop, autoplay, mode, selected_qid, normalize, lyrics_on)
    view.state = state
    view.sig   = (current_title, tuple(q["qid"] for q in queue), muted, loop, autoplay, normalize,
                  mode, controller, paused, lyrics_on, state.minutes_left() if state else None)


    for child in view.children:
        if not hasattr(child, "label"):
            continue
        if child.label in ("🔇 Mute", "🔇 Muted"):
            child.style = discord.ButtonStyle.danger if muted else discord.ButtonStyle.secondary
            child.label = "🔇 Muted" if muted else "🔇 Mute"
        elif child.label in ("🔁 Loop", "🔁 Loop ON"):
            child.style = discord.ButtonStyle.success if loop else discord.ButtonStyle.secondary
            child.label = "🔁 Loop ON" if loop else "🔁 Loop"
        elif child.label in ("⏸ Pause", "▶ Resume"):
            child.style = discord.ButtonStyle.success if paused else discord.ButtonStyle.primary
            child.label = "▶ Resume" if paused else "⏸ Pause"

    start     = page * PAGE_SIZE
    end       = min(start + PAGE_SIZE, len(queue))
    page_info = f"  (page {page + 1}/{total_pages})" if total_pages > 1 else ""
    flags     = (("  ⏸️ paused" if paused else "") + ("  🔇 muted" if muted else "") + ("  🔁 loop" if loop else "") + ("  🎲 autoplay" if autoplay else "")
                 + ("  🎚️ normalized" if normalize else "") + ("  🎤 lyrics" if lyrics_on else ""))
    sel_info  = (f"\n✅ **Selected:** #{sel_item['index'] + 1} {_truncate(sel_item['title'], 50)}"
                 if sel_item else "")
    who       = (f"🔒 Controlled by <@{controller}>" if mode == "me" else
                 f"🔓 Anyone can control (started by <@{controller}>)") if controller else ""
    embed = discord.Embed(
        title=f"🎛️  Playback Controls — Worker {worker.index}",
        description=(
            f"▶ **{_truncate(current_title, 80)}**{flags}\n"
            f"{'📋 **' + str(len(queue)) + '** track(s) in queue' + page_info + f' — showing #{start+1}–#{end}' if queue else '📭 No tracks queued'}"
            f"{sel_info}\n{who}\n\n"
            + (f"⏳ Panel expires in: {state.minutes_left()} min" if state else "")
        ),
        colour=COLOUR,
    )
    return embed, view


async def _refresh_control_panel(interaction: discord.Interaction, ctrl: ControlView,
                                 page: int = 0, selected_qid: int | None = None):
    try:
        embed, view = await _build_control_view(ctrl.worker, ctrl.guild_id, page, selected_qid, ctrl.state)
        await interaction.edit_original_response(embed=embed, view=view)
        if ctrl.state:
            ctrl.state.show(view)
    except Exception as exc:
        print(f"[Main] Control panel refresh failed: {exc}", flush=True)

async def _panel_autorefresh(state: PanelState) -> None:
    """Keep an open panel in sync with playback. Interaction messages can only be edited for
    15 minutes, which is also how long the panel's buttons work."""
    key = (state.worker.index, state.guild_id)
    _open_panels.setdefault(key, set()).add(state)
    try:
        await _panel_loop(state)
    finally:
        _open_panels.get(key, set()).discard(state)

async def _panel_loop(state: PanelState) -> None:
    while time.monotonic() < (deadline := state.created + PANEL_LIFETIME):
        try:
            # Wake up right away when the worker reports a change (song picked or started, queue
            # changed), otherwise check every PANEL_REFRESH.
            await asyncio.wait_for(state.wake.wait(), timeout=min(PANEL_REFRESH, max(0.0, deadline - time.monotonic())))
            await asyncio.sleep(0.3)   # let a burst of changes settle into one edit
        except asyncio.TimeoutError:
            pass
        state.wake.clear()
        view = state.view
        if view is None or view._stopped or view.is_finished():
            return
        if time.monotonic() >= deadline:
            break
        try:
            if not state.session():
                if await state.ended():
                    # The session ended (or a new one replaced it): this panel can't do anything now.
                    await state.retire("⏹️  Playback ended — use /play or /control again")
                    return
                continue
            embed, new = await _build_control_view(state.worker, state.guild_id, view._queue_page,
                                                   view._selected_qid, state)
            if new.sig == state.sig:
                new.stop()
                continue
            await state.message.edit(embed=embed, view=new)
            state.show(new)
        except Exception as exc:
            print(f"[Main] Control panel auto-refresh stopped: {exc}", flush=True)
            return
    # Last moment Discord accepts edits: a public panel is deleted, a private one marked expired.
    view = state.view
    if view is not None and not view._stopped and state.public:
        await state.retire(None)
        return
    if view is not None and not view._stopped:
        try:
            await state.message.edit(embed=discord.Embed(
                title="⌛  Panel expired", description="Run `/control` again to get a new one.",
                colour=discord.Colour.dark_grey()), view=None)
        except Exception as exc:
            print(f"[Main] Could not mark control panel expired: {exc}", flush=True)
        view.stop()


@main_bot.tree.command(name="control", description="Open playback control panel")
@app_commands.describe(worker="Which worker to control (0 = the one in your channel)",
                       no_eph="Make this session public: panel and everyone's replies visible (control stays Me/All)")
async def slash_control(interaction: discord.Interaction, worker: int = 0, no_eph: bool | None = None):
    if not await _guard(interaction, during_restart=True): return
    if no_eph is True:
        eph = False
    elif no_eph is False:
        eph = _eph(interaction.guild_id, "CC_EPH")
    else:
        eph = _reply_eph(interaction, "CC_EPH", worker)
    await interaction.response.defer(thinking=True, ephemeral=eph)
    pick = await _pick_for_control(interaction, worker)
    if pick is None: return
    w, _s = pick
    if no_eph is not None:
        # Public session: its panel and everyone's replies about it (play, stop, panel buttons)
        # are visible to the channel. Control is unchanged (Me mode still locks the buttons).
        resp = await w.send({"op": "set_no_eph", "guild_id": interaction.guild_id, "on": no_eph})
        if resp.get("session"):
            w.apply(interaction.guild_id, resp["session"])
    state = PanelState(w, interaction.guild_id)
    state.public = not eph
    embed, view = await _build_control_view(w, interaction.guild_id, page=0, state=state)
    state.message = await interaction.followup.send(embed=embed, view=view, ephemeral=eph, wait=True)
    state.show(view)
    _spawn(_panel_autorefresh(state))


@main_bot.tree.command(name="hctest", description="Health check — tests all systems")
async def slash_hctest(interaction: discord.Interaction):
    if not await _guard(interaction): return
    gid = interaction.guild_id
    eph = _eph(gid, "HC_EPH")
    await interaction.response.defer(thinking=True, ephemeral=eph)
    start = time.monotonic()
    await _resync_all()
    results = []
    for w in _ordered_workers():
        resp = await w.send({"op": "ping", "guild_id": gid}, timeout=10.0)
        results.append((w, resp))
    elapsed = round((time.monotonic() - start) * 1000)
    all_ok  = all(r.get("status") == "ok" and r.get("lavalink_ok") for _, r in results)
    busy_here = sum(1 for w in _workers.values() if w.busy_in(gid))
    embed   = discord.Embed(
        title="🏥  Health Check",
        description="**✅ All systems operational**" if all_ok else "**⚠️ Some systems degraded**",
        colour=discord.Colour.green() if all_ok else discord.Colour.orange(),
    )
    embed.add_field(
        name="🤖  Main Controller",
        value=(
            f"Status: ✅ Online\n"
            f"Discord latency: `{round(main_bot.latency * 1000)}ms`\n"
            f"Workers: `{len(_workers)}` total, `{busy_here}` busy in this server\n"
            f"Servers: `{len(main_bot.guilds)}`"
        ),
        inline=False,
    )
    for w, r in results:
        vcw = _cfg_vcw(gid, w.index)
        fixed = f"\nFixed VC: <#{vcw}>" if vcw else ""
        if r.get("status") == "ok":
            disc  = "✅" if r.get("discord_ok") else "❌"
            lava  = "✅" if r.get("lavalink_ok") else "❌"
            dms   = r.get("discord_ms", -1)
            s     = r.get("session")
            if gid not in w.guild_ids:
                state = "⚫ Not in this server"
            elif s:
                state = f"🔴 Busy in <#{s['channel_id']}> for <@{s['controller_id']}>  |  Queue: `{r.get('queue_len', 0)}`"
            else:
                state = "🟢 Free"
            val   = (
                f"Discord: {disc} `{dms}ms`\n"
                f"Lavalink: {lava} `{r.get('lavalink_uri','?')}`\n"
                f"Status: {state}\n"
                f"Active in `{r.get('sessions', 0)}` server(s)" + fixed
            )
        else:
            val = f"❌ {r.get('message', 'Not responding')}{fixed}"
        embed.add_field(name=f"🎵  Worker {w.index}", value=val, inline=True)
    embed.set_footer(text=f"Check completed in {elapsed}ms")
    await interaction.followup.send(embed=embed, ephemeral=eph)


# ── /settings GUI ────────────────────────────────────────────────────────────

def _settings_embed(guild: discord.Guild, worker_idx: int, note: str | None = None) -> discord.Embed:
    gid = guild.id
    cc  = _cfg_cc(gid)
    lines = []
    if note:
        lines += [note, ""]
    lines.append("**Control channel:** " + (f"<#{cc}>" if cc else "any channel"))
    lines.append("**Fixed voice channels:**")
    for w in _ordered_workers():
        vc = _cfg_vcw(gid, w.index)
        here = "" if gid in w.guild_ids else "  *(not in this server)*"
        marker = "▸ " if w.index == worker_idx else ""
        lines.append(f"{marker}Worker {w.index}: " + (f"<#{vc}>" if vc else "joins where the user is") + here)
    lines.append(f"**Default search source:** {SOURCES[_cfg_src(gid)]}")
    private = [label for k, label in EPH_KEYS.items() if _eph(gid, k)]
    lines.append("**Private replies:** " + (", ".join(private) if private else "none (all public)"))
    embed = discord.Embed(title=f"⚙️  Settings — {guild.name}", description="\n".join(lines), colour=COLOUR)
    embed.set_footer(text="Only you can use this panel. Changes apply immediately.")
    return embed

def _channel_default(channel_id: int | None) -> list:
    if not channel_id:
        return []
    return [discord.SelectDefaultValue(id=channel_id, type=discord.SelectDefaultValueType.channel)]


class SettingsView(discord.ui.View):
    def __init__(self, invoker_id: int, guild: discord.Guild, worker_idx: int):
        super().__init__(timeout=600)
        self.invoker_id = invoker_id
        self.guild      = guild
        self.worker_idx = worker_idx
        gid = guild.id
        cfg = _gcfg(gid)

        cc_select = discord.ui.ChannelSelect(
            channel_types=[discord.ChannelType.text], min_values=0, max_values=1, row=0,
            placeholder="Control channel — where commands work (empty = any)",
            default_values=_channel_default(cfg.get("cc_id")))
        cc_select.callback = self._on_cc
        self.cc_select = cc_select
        self.add_item(cc_select)

        if _workers:   # empty for a few seconds right after a restart; Discord rejects empty dropdowns
            worker_select = discord.ui.Select(
                row=1, min_values=1, max_values=1,
                options=[discord.SelectOption(label=f"Worker {w.index}", value=str(w.index),
                                              description="Choose which worker's fixed channel to edit",
                                              default=(w.index == worker_idx))
                         for w in _ordered_workers()])
            worker_select.callback = self._on_worker
            self.worker_select = worker_select
            self.add_item(worker_select)

            vc_select = discord.ui.ChannelSelect(
                channel_types=[discord.ChannelType.voice], min_values=0, max_values=1, row=2,
                placeholder=f"Worker {worker_idx} fixed voice channel (empty = follow the user)",
                default_values=_channel_default(_cfg_vcw(gid, worker_idx)))
            vc_select.callback = self._on_vc
            self.vc_select = vc_select
            self.add_item(vc_select)

        eph_select = discord.ui.Select(
            row=3, min_values=0, max_values=len(EPH_KEYS),
            placeholder="Private replies (only the person who ran it sees them)",
            options=[discord.SelectOption(label=f"{label} replies private", value=k,
                                          default=_eph(gid, k))
                     for k, label in EPH_KEYS.items()])
        eph_select.callback = self._on_eph
        self.eph_select = eph_select
        self.add_item(eph_select)

        self.btn_source.label = f"🔎 Source: {SOURCES[_cfg_src(gid)]}"

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id == self.invoker_id:
            return True
        await interaction.response.send_message(
            embed=_err_embed("Only the person who ran /settings can use this panel"), ephemeral=True)
        return False

    async def _redraw(self, interaction: discord.Interaction, note: str | None = None,
                      worker_idx: int | None = None) -> None:
        idx = worker_idx or self.worker_idx
        self.stop()
        await interaction.response.edit_message(
            embed=_settings_embed(self.guild, idx, note),
            view=SettingsView(self.invoker_id, self.guild, idx))

    async def _on_cc(self, interaction: discord.Interaction):
        gid = self.guild.id
        cfg = _gcfg(gid)
        ch  = self.cc_select.values[0] if self.cc_select.values else None
        cfg["cc_id"] = ch.id if ch else None
        _save_gcfg(gid)
        await self._redraw(interaction, f"✅ Control channel {'set to <#' + str(ch.id) + '>' if ch else 'cleared'}.")

    async def _on_worker(self, interaction: discord.Interaction):
        await self._redraw(interaction, worker_idx=int(self.worker_select.values[0]))

    async def _on_vc(self, interaction: discord.Interaction):
        gid = self.guild.id
        cfg = _gcfg(gid)
        idx = self.worker_idx
        ch  = self.vc_select.values[0] if self.vc_select.values else None
        if ch is None:
            cfg["vcw"].pop(str(idx), None)
            _save_gcfg(gid)
            await self._redraw(interaction, f"✅ Worker {idx} now joins whichever voice channel the user is in.")
            return
        for other, cid in cfg["vcw"].items():
            if cid == ch.id and other != str(idx):
                await self._redraw(interaction, f"❌ Worker {other} is already fixed to <#{ch.id}>.")
                return
        cfg["vcw"][str(idx)] = ch.id
        _save_gcfg(gid)
        note = f"✅ Worker {idx} always joins <#{ch.id}>."
        s = _workers[idx].session(gid) if idx in _workers else None
        if s and s["channel_id"] != ch.id:
            note += " It is playing elsewhere right now — applies from its next play."
        await self._redraw(interaction, note)

    async def _on_eph(self, interaction: discord.Interaction):
        gid = self.guild.id
        cfg = _gcfg(gid)
        cfg["eph"] = {k: (k in self.eph_select.values) for k in EPH_KEYS}
        _save_gcfg(gid)
        await self._redraw(interaction, "✅ Private reply settings saved.")

    @discord.ui.button(label="🔎 Source", style=discord.ButtonStyle.primary, row=4)
    async def btn_source(self, interaction: discord.Interaction, _):
        keys = list(SOURCES)
        nxt  = keys[(keys.index(_cfg_src(self.guild.id)) + 1) % len(keys)]
        _gcfg(self.guild.id)["src"] = nxt
        _save_gcfg(self.guild.id)
        await self._redraw(interaction, f"✅ Default search source: {SOURCES[nxt]}.")

    @discord.ui.button(label="Clear control channel", style=discord.ButtonStyle.secondary, row=4)
    async def btn_clear_cc(self, interaction: discord.Interaction, _):
        _gcfg(self.guild.id)["cc_id"] = None
        _save_gcfg(self.guild.id)
        await self._redraw(interaction, "✅ Control channel cleared — commands work in any channel.")

    @discord.ui.button(label="Clear worker channel", style=discord.ButtonStyle.secondary, row=4)
    async def btn_clear_vc(self, interaction: discord.Interaction, _):
        _gcfg(self.guild.id)["vcw"].pop(str(self.worker_idx), None)
        _save_gcfg(self.guild.id)
        await self._redraw(interaction, f"✅ Worker {self.worker_idx} now follows the user.")

    @discord.ui.button(label="Done", style=discord.ButtonStyle.primary, row=4)
    async def btn_done(self, interaction: discord.Interaction, _):
        await interaction.response.edit_message(
            embed=_settings_embed(self.guild, self.worker_idx, "✅ Settings saved."), view=None)
        self.stop()


@main_bot.tree.command(name="settings", description="Server settings for the music bots (Manage Server)")
async def slash_settings(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message(embed=_err_embed("Server only"), ephemeral=True)
        return
    if not appsettings.is_whitelisted(interaction.guild_id):
        await interaction.response.send_message(embed=_err_embed("This server is not whitelisted"), ephemeral=True)
        return
    if not (_has_perm(interaction.user, "manage_guild") or _is_dev(interaction.user)):
        await interaction.response.send_message(
            embed=_err_embed("You need the Manage Server permission to change settings"), ephemeral=True)
        return
    # Not routed through _guard: the control-channel lock must never lock admins out of fixing it.
    idx = min(_workers) if _workers else 1
    await interaction.response.send_message(
        embed=_settings_embed(interaction.guild, idx),
        view=SettingsView(interaction.user.id, interaction.guild, idx),
        ephemeral=True)


ALGO_PAGE = 25

async def _forget_on_workers(guild_id: int, user_id: int) -> None:
    await asyncio.gather(*[w.send({"op": "forget_user", "guild_id": guild_id, "user_id": user_id}, timeout=5.0)
                           for w in _workers.values()], return_exceptions=True)

def _algo_embed(rows: list[dict], page: int, selected: dict[str, str], queries: int,
                note: str | None = None) -> discord.Embed:
    pages = max(1, -(-len(rows) // ALGO_PAGE))
    start = page * ALGO_PAGE
    lines = [note, ""] if note else []
    lines.append(f"**{len(rows)}** saved song(s) and **{queries}** search memor{'y' if queries == 1 else 'ies'} "
                 "on this server. Only you can see and change this.")
    lines.append("Pick songs in the menu to remove them, or reset everything.\n")
    for i, r in enumerate(rows[start:start + ALGO_PAGE], start=start + 1):
        mark = "🟠 " if r["track_key"] in selected else ""
        skips = f", {r['skips']} skip(s)" if r["skips"] else ""
        lines.append(f"{mark}`#{i}` **{_truncate(r['title'] or '?', 50)}** — {_truncate(r['author'] or '?', 30)}"
                     f" · {r['plays']} play(s){skips}")
    if selected:
        lines.append(f"\n🟠 **{len(selected)}** selected")
    embed = discord.Embed(title="🎵  Your algo", description="\n".join(lines)[:4000], colour=COLOUR)
    if pages > 1:
        embed.set_footer(text=f"Page {page + 1} / {pages}")
    return embed


class AlgoMenuView(discord.ui.View):
    """/reset-algo: your saved songs, like /control's queue list. Select songs to remove them
    (by song key, never by position), or reset everything. Every delete asks for confirmation."""

    def __init__(self, user_id: int, guild_id: int, rows: list[dict], queries: int,
                 page: int = 0, selected: dict[str, str] | None = None):
        super().__init__(timeout=600)
        self.user_id, self.guild_id = user_id, guild_id
        self.rows, self.queries     = rows, queries
        self.pages    = max(1, -(-len(rows) // ALGO_PAGE))
        self.page     = max(0, min(page, self.pages - 1))
        self.selected = dict(selected or {})   # track_key -> "title — artist"

        page_rows = rows[self.page * ALGO_PAGE:(self.page + 1) * ALGO_PAGE]
        if page_rows:
            pick = discord.ui.Select(
                placeholder="Select songs to remove…", min_values=0, max_values=len(page_rows), row=0,
                # Values are positions in this menu's own list (Discord caps values at 100 chars;
                # song keys can be long URLs). The list never changes while this menu is open.
                options=[discord.SelectOption(
                    label=_truncate(f"#{self.page * ALGO_PAGE + i + 1}  {r['title'] or '?'}", 100),
                    description=_truncate(f"{r['author'] or '?'} · {r['plays']} play(s)", 100),
                    value=str(self.page * ALGO_PAGE + i), default=r["track_key"] in self.selected)
                    for i, r in enumerate(page_rows)])
            pick.callback = self._on_pick
            self.pick = pick
            self.add_item(pick)
        if self.pages > 1:
            prev = discord.ui.Button(label="◀ Prev", style=discord.ButtonStyle.secondary, row=1,
                                     disabled=self.page == 0)
            nxt  = discord.ui.Button(label="Next ▶", style=discord.ButtonStyle.secondary, row=1,
                                     disabled=self.page >= self.pages - 1)
            prev.callback = lambda i: self._goto(i, self.page - 1)
            nxt.callback  = lambda i: self._goto(i, self.page + 1)
            self.add_item(prev)
            self.add_item(discord.ui.Button(label=f"Page {self.page + 1} / {self.pages}", disabled=True,
                                            style=discord.ButtonStyle.secondary, row=1))
            self.add_item(nxt)
        # Discord buttons can't be orange: "remove" is blurple with an orange marker.
        reset  = discord.ui.Button(label="Reset all", emoji="🗑️", style=discord.ButtonStyle.danger, row=2)
        remove = discord.ui.Button(label=f"Remove selected ({len(self.selected)})", emoji="🟠",
                                   style=discord.ButtonStyle.primary, row=2, disabled=not self.selected)
        cancel = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.secondary, row=2)
        reset.callback, remove.callback, cancel.callback = self._on_reset, self._on_remove, self._on_cancel
        for b in (reset, remove, cancel):
            self.add_item(b)

    def embed(self, note: str | None = None) -> discord.Embed:
        return _algo_embed(self.rows, self.page, self.selected, self.queries, note)

    def again(self, page: int | None = None) -> "AlgoMenuView":
        return AlgoMenuView(self.user_id, self.guild_id, self.rows, self.queries,
                            self.page if page is None else page, self.selected)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id == self.user_id:
            return True
        await interaction.response.send_message(embed=_err_embed("This isn't your algo"), ephemeral=True)
        return False

    async def _goto(self, interaction: discord.Interaction, page: int):
        view = self.again(page)
        self.stop()
        await interaction.response.edit_message(embed=view.embed(), view=view)

    async def _on_pick(self, interaction: discord.Interaction):
        on_page = {r["track_key"]: r for r in self.rows[self.page * ALGO_PAGE:(self.page + 1) * ALGO_PAGE]}
        for k in on_page:
            self.selected.pop(k, None)
        for v in self.pick.values:
            r = self.rows[int(v)]
            self.selected[r["track_key"]] = f"{r['title'] or '?'} — {r['author'] or '?'}"
        await self._goto(interaction, self.page)

    async def _on_reset(self, interaction: discord.Interaction):
        confirm = AlgoConfirmView(self, "all")
        self.stop()
        await interaction.response.edit_message(embed=discord.Embed(
            title="Reset your whole algo?",
            description=f"This deletes **all {len(self.rows)}** of your saved songs and your search memory on "
                        "this server. Searches and autoplay stop being personalised until you listen again. "
                        "This can't be undone.", colour=discord.Colour.red()), view=confirm)

    async def _on_remove(self, interaction: discord.Interaction):
        names = list(self.selected.values())
        shown = "\n".join(f"• {_truncate(n, 80)}" for n in names[:20])
        more  = f"\n…and {len(names) - 20} more" if len(names) > 20 else ""
        confirm = AlgoConfirmView(self, "remove")
        self.stop()
        await interaction.response.edit_message(embed=discord.Embed(
            title=f"Remove {len(names)} song(s) from your algo?",
            description=f"{shown}{more}\n\nOnly these songs are removed. This can't be undone.",
            colour=discord.Colour.orange()), view=confirm)

    async def _on_cancel(self, interaction: discord.Interaction):
        self.stop()
        await interaction.response.edit_message(embed=_simple_embed("No changes made", COLOUR), view=None)


class AlgoConfirmView(discord.ui.View):
    """Second step for every delete in the algo menu."""

    def __init__(self, menu: AlgoMenuView, action: str):
        super().__init__(timeout=120)
        self.menu, self.action = menu, action
        label = "Yes, delete everything" if action == "all" else f"Yes, remove {len(menu.selected)}"
        yes  = discord.ui.Button(label=label, style=discord.ButtonStyle.danger)
        back = discord.ui.Button(label="Back", style=discord.ButtonStyle.secondary)
        yes.callback, back.callback = self._on_yes, self._on_back
        self.add_item(yes)
        self.add_item(back)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return await self.menu.interaction_check(interaction)

    async def _on_back(self, interaction: discord.Interaction):
        view = self.menu.again()
        self.stop()
        await interaction.response.edit_message(embed=view.embed(), view=view)

    async def _on_yes(self, interaction: discord.Interaction):
        m = self.menu
        self.stop()
        # Acknowledge first: Discord fails the click after 3 s, and telling every worker to forget
        # can take longer than that (up to 5 s per unresponsive worker).
        await interaction.response.defer()
        if self.action == "all":
            songs, queries = await asyncio.to_thread(db.reset_user, m.guild_id, m.user_id)
            await _forget_on_workers(m.guild_id, m.user_id)
            await interaction.edit_original_response(embed=discord.Embed(
                title="🗑️  Your algo was reset",
                description=f"Deleted {songs} saved song(s) and {queries} search memor{'y' if queries == 1 else 'ies'} "
                            "on this server. Searches and autoplay start fresh from your next listens.",
                colour=discord.Colour.red()), view=None)
            return
        removed = await asyncio.to_thread(db.remove_user_tracks, m.guild_id, m.user_id, list(m.selected))
        await _forget_on_workers(m.guild_id, m.user_id)
        rows    = await asyncio.to_thread(db.user_tracks, m.guild_id, m.user_id)
        queries = await asyncio.to_thread(db.user_query_count, m.guild_id, m.user_id)
        view = AlgoMenuView(m.user_id, m.guild_id, rows, queries, m.page)
        await interaction.edit_original_response(embed=view.embed(f"🟠 Removed {removed} song(s) from your algo."),
                                                 view=view)


@main_bot.tree.command(name="reset-algo", description="See your saved songs (algo) and remove some or all of them")
async def slash_reset_algo(interaction: discord.Interaction):
    # Personal and private: works in any channel, and there is deliberately no option to target someone else.
    if not interaction.guild_id:
        await interaction.response.send_message(embed=_err_embed("Server only"), ephemeral=True)
        return
    if not appsettings.is_whitelisted(interaction.guild_id):
        await interaction.response.send_message(embed=_err_embed("This server is not whitelisted"), ephemeral=True)
        return
    gid, uid = interaction.guild_id, interaction.user.id
    rows    = await asyncio.to_thread(db.user_tracks, gid, uid)
    queries = await asyncio.to_thread(db.user_query_count, gid, uid)
    if not rows and not queries:
        await interaction.response.send_message(
            embed=_simple_embed("You have no saved algo on this server", COLOUR), ephemeral=True)
        return
    view = AlgoMenuView(uid, gid, rows, queries)
    await interaction.response.send_message(embed=view.embed(), view=view, ephemeral=True)


@main_bot.tree.command(name="purge", description="Delete the last N bot messages in this channel (default: 20)")
@app_commands.describe(limit="Number of messages to scan (1–100, default 20)")
async def slash_purge(interaction: discord.Interaction, limit: int = 20):
    if not (_has_perm(interaction.user, "manage_messages") or _is_dev(interaction.user)):
        await interaction.response.send_message(
            embed=_err_embed("You need the Manage Messages permission"), ephemeral=True)
        return
    if not await _guard(interaction): return
    await interaction.response.defer(thinking=True, ephemeral=True)
    if not interaction.guild or not isinstance(interaction.channel, discord.TextChannel):
        await interaction.followup.send(embed=_err_embed("Use this in a server text channel"), ephemeral=True)
        return
    limit = max(1, min(limit, 100))


    deleted = 0
    try:
        messages = [m async for m in interaction.channel.history(limit=limit) if m.author.bot]
        for msg in messages:
            try:
                await msg.delete()
                deleted += 1
            except Exception:
                pass
    except discord.Forbidden:
        await interaction.followup.send(embed=_err_embed("Missing permission to delete messages"), ephemeral=True)
        return
    embed = discord.Embed(
        title=f"🧹  Purged {deleted} bot message(s)",
        colour=COLOUR,
    )
    await interaction.followup.send(embed=embed, ephemeral=False)


async def _watch_worker(w: WorkerProcess):
    ticks = 0
    failed_checks = 0
    while True:
        await asyncio.sleep(5)
        if _shutting_down:
            return
        ticks += 1

        if w.proc is not None:
            dead = not w.is_alive()
        else:
            probe = await w.send({"op": "ping"}, timeout=4.0)
            dead  = probe.get("status") != "ok"
        if dead:
            if w._shutdown_requested:
                print(f"[Main] Worker {w.index} is shut down (requested) — not restarting", flush=True)
                return
            if w.lock.locked():
                continue   # a dev ``restart`` is already bringing it back
            async with w.lock:
                if w.proc is not None and w.is_alive():
                    continue   # restarted by someone else meanwhile
                print(f"[Main] Worker {w.index} unresponsive — restarting in 3s", flush=True)
                w.clear_state()
                await asyncio.sleep(3)
                if not _shutting_down and not w._shutdown_requested:
                    if w.proc is None:
                        await w.terminate()   # adopted worker that stopped answering
                    w.start()
        elif ticks % 6 == 0:
            # Backstop in case a state event was lost, and a health check: a worker whose process
            # is alive but that stops answering (frozen event loop) is killed and restarted.
            if await _resync_worker(w):
                failed_checks = 0
            elif w.proc is not None and not w.lock.locked():
                failed_checks += 1
                if failed_checks >= HUNG_AFTER:
                    print(f"[Main] Worker {w.index} hasn't answered for {failed_checks * 30}s — killing it", flush=True)
                    failed_checks = 0
                    try:
                        os.killpg(w._pgid, signal.SIGKILL) if w._pgid else w.proc.kill()
                    except ProcessLookupError:
                        pass


async def _serve_shutdown_message():
    """Serve shutdown.mp3 on a private localhost port so Lavalink (same machine) can stream it.
    Returns (url, runner), or (None, None) when the file doesn't exist."""
    if not SHUTDOWN_MP3.exists():
        print("[Main] shutdown.mp3 not found — workers will leave without a message", flush=True)
        return None, None
    app = web.Application()
    app.router.add_get("/shutdown.mp3", lambda request: web.FileResponse(SHUTDOWN_MP3))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    return f"http://127.0.0.1:{port}/shutdown.mp3", runner

async def _drain_workers() -> None:
    """Scheduled restart: every worker finishes its current song (the rest of the queue/playlist is
    dropped), plays shutdown.mp3 and leaves. Finished workers stay blocked. Waits until all are done,
    or DRAIN_TIMEOUT, after which remaining songs are cut and the message plays immediately."""
    global _draining
    _draining = True
    live = [w for w in _ordered_workers() if not w._shutdown_requested]
    await _resync_all()
    if not any(w.sessions for w in live):
        print("[Main] Nothing playing — restarting right away", flush=True)
        return
    url, runner = await _serve_shutdown_message()
    try:
        await asyncio.gather(*[w.send({"op": "drain", "message_url": url}, timeout=30.0) for w in live],
                             return_exceptions=True)
        print(f"[Main] Restart scheduled — waiting up to {int(DRAIN_TIMEOUT)}s for current songs to finish", flush=True)

        async def still_busy() -> list[WorkerProcess]:
            results = await asyncio.gather(*[_resync_worker(w, timeout=3.0) for w in live],
                                           return_exceptions=True)
            for w, ok in zip(live, results):
                if ok is not True:
                    w.sessions.clear()   # not answering (crashed/exited): nothing left to wait for
            return [w for w in live if w.sessions]

        deadline = time.monotonic() + DRAIN_TIMEOUT
        busy = await still_busy()
        while busy and time.monotonic() < deadline and not _force_drain.is_set():
            await asyncio.sleep(2)
            busy = await still_busy()
        if busy:
            print(f"[Main] Restart timeout — cutting songs on worker(s) {[w.index for w in busy]}", flush=True)
            await asyncio.gather(*[w.send({"op": "drain_force"}, timeout=30.0) for w in busy],
                                 return_exceptions=True)
            end = time.monotonic() + DRAIN_MESSAGE_MAX
            while busy and time.monotonic() < end:
                await asyncio.sleep(2)
                busy = await still_busy()
        print("[Main] All workers finished — restarting", flush=True)
    finally:
        if runner:
            await runner.cleanup()

async def _run(stop_event: asyncio.Event | None = None):
    global _shutting_down, _force_drain

    _force_drain = asyncio.Event()
    PID_FILE.write_text(str(os.getpid()))
    _migrate_legacy_settings()
    _spawn(_event_server())

    main_task = asyncio.create_task(main_bot.start(TOKENS[0]), name="main_bot")
    await asyncio.sleep(5)

    for i in range(1, len(TOKENS)):
        _workers[i] = WorkerProcess(index=i)


    for w in _ordered_workers():
        if w.socket_path.exists():
            print(f"[Main] Socket exists for Worker {w.index} — attempting resync ...", flush=True)
            alive = await _resync_worker(w)
            if alive:
                print(f"[Main] Worker {w.index} already running ({len(w.sessions)} session(s)) — skipping spawn", flush=True)
                w.ensure_watched()
                continue
            print(f"[Main] Worker {w.index} socket stale — spawning fresh", flush=True)
        w.start()
        w.ensure_watched()
        await asyncio.sleep(1)

    print(f"[Main] All {len(_workers)} worker(s) ready", flush=True)

    exit_code = 0
    if stop_event:
        stop_wait = asyncio.create_task(stop_event.wait())
        await asyncio.wait({stop_wait, main_task}, return_when=asyncio.FIRST_COMPLETED)
        if main_task.done() and not stop_event.is_set():
            # discord.py reconnects by itself; ending means a fatal error (bad token, intents…).
            # Exit with an error so the launcher / systemd restart us instead of idling offline.
            exc = main_task.exception() if not main_task.cancelled() else None
            print(f"[Main] Discord connection ended ({exc!r}) — exiting so the service restarts", flush=True)
            exit_code = 1
            stop_wait.cancel()
        else:
            try:
                await _drain_workers()
            except Exception as exc:
                print(f"[Main] Graceful restart failed: {exc} — stopping now", flush=True)
    else:
        try:
            await main_task
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass

    _shutting_down = True
    print("[Main] Shutting down ...", flush=True)
    # Panels stop working once this process exits: delete them instead of leaving dead buttons.
    open_panels = [st for group in _open_panels.values() for st in group]
    if open_panels:
        await asyncio.wait([asyncio.ensure_future(st.retire(None)) for st in open_panels], timeout=10)
    await main_bot.close()
    await asyncio.gather(*[w.terminate() for w in _workers.values()], return_exceptions=True)
    MAIN_SOCKET.unlink(missing_ok=True)
    PID_FILE.unlink(missing_ok=True)
    print("[Main] All workers stopped.", flush=True)
    return exit_code


if __name__ == "__main__":
    if SERVER_MODE:
        loop     = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        stop_evt = asyncio.Event()

        _stop_event = stop_evt

        def _sig():
            print("[Main] Signal — finishing current songs before stopping ...", flush=True)
            _request_stop()

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _sig)

        code = 1
        try:
            code = loop.run_until_complete(_run(stop_event=stop_evt)) or 0
        finally:
            pending = asyncio.all_tasks(loop)
            for t in pending: t.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.close()
        sys.exit(code)

    else:
        try:
            asyncio.run(_run())
        except KeyboardInterrupt:
            pass

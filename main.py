import asyncio
import json
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

def _eph(guild_id: int | None, key: str) -> bool:
    if not guild_id:
        return False
    return bool(_gcfg(guild_id)["eph"].get(key, False))

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

        asyncio.ensure_future(self._reap())

    async def _stream_output(self):
        if not self.proc or not self.proc.stdout:
            return
        loop = asyncio.get_event_loop()
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        await loop.connect_read_pipe(lambda: protocol, self.proc.stdout)
        async for line in reader:
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
        if not self.proc or not self.is_alive():
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
        print(f"[Main] Restarting Worker {self.index} ...", flush=True)
        self.clear_state()
        self._shutdown_requested = False
        await self.terminate()
        await asyncio.sleep(1)
        self.start()

    async def send(self, cmd: dict, timeout: float = 15.0) -> dict:
        for attempt in range(3):
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_unix_connection(str(self.socket_path)),
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
        return False
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
        path    = resp.get("search_path", "")
        qpos    = resp.get("queue_pos", 0)
        desc    = f"**[{title}]({uri})**\n{author}" if uri else f"**{title}**\n{author}"
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
        if path:
            embed.add_field(name="Found via", value=path, inline=True)
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
    """Whoever started playback controls it, unless they opened it to everyone. Devs can always control."""
    if not session:
        return False
    return (_is_dev(user) or session.get("controller_id") == user.id
            or session.get("mode") == "all")

def _locked_msg(w: WorkerProcess, session: dict) -> str:
    return (f"🔒 Worker {w.index} is controlled by <@{session.get('controller_id')}>. "
            "Join another voice channel to get your own worker.")


class MainBot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self._guild_cmds_cleared = False

    async def setup_hook(self):
        await self.tree.sync()
        print("[Main] Commands synced globally", flush=True)


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
    for g in list(main_bot.guilds):
        if not await _enforce_whitelist(g):
            continue
        if not main_bot._guild_cmds_cleared:
            # Older versions synced commands per-guild; remove those so they don't show up twice.
            try:
                main_bot.tree.clear_commands(guild=g)
                await main_bot.tree.sync(guild=g)
            except Exception as exc:
                print(f"[Main] Could not clear guild commands in {g.id}: {exc}", flush=True)
    main_bot._guild_cmds_cleared = True

@main_bot.event
async def on_guild_join(guild: discord.Guild):
    if await _enforce_whitelist(guild):
        print(f"[Main] Joined whitelisted guild {guild.id} ({guild.name})", flush=True)


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
        asyncio.ensure_future(_delayed_systemctl(action, delay=2.0))
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

async def _guard(interaction: discord.Interaction) -> bool:
    if not interaction.guild_id:
        await interaction.response.send_message(embed=_err_embed("Server only"), ephemeral=True)
        return False
    if _draining:
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
        present = [w for w in _ordered_workers() if guild_id in w.guild_ids]
        if not present:
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
    eph = _eph(interaction.guild_id, eph_key)
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


@main_bot.tree.command(name="play", description="Play a song — name or URL (YouTube / SoundCloud / Spotify)")
@app_commands.describe(query="Song name or URL", source="Search source (ignored for URLs; default set in /settings)",
                       worker="Force a specific worker bot (0 = automatic)")
@app_commands.choices(source=_source_choices)
async def slash_play(interaction: discord.Interaction, query: str,
                     source: app_commands.Choice[str] | None = None, worker: int = 0):
    if not await _guard(interaction): return
    await _start_playback(interaction, "search_and_play", "P_EPH", worker,
                          query=query, source=source.value if source else _cfg_src(interaction.guild_id))


@main_bot.tree.command(name="playlist", description="Queue a full playlist — YouTube, SoundCloud, or Spotify URL")
@app_commands.describe(query="Playlist URL or search term", source="Search source (ignored for URLs; default set in /settings)",
                       worker="Force a specific worker bot (0 = automatic)")
@app_commands.choices(source=_source_choices)
async def slash_playlist(interaction: discord.Interaction, query: str,
                         source: app_commands.Choice[str] | None = None, worker: int = 0):
    if not await _guard(interaction): return
    await _start_playback(interaction, "search_and_playlist", "PL_EPH", worker,
                          query=query, source=source.value if source else _cfg_src(interaction.guild_id))


@main_bot.tree.command(name="autoplay", description="Autoplay music picked from your saved listening history")
@app_commands.describe(worker="Force a specific worker bot (0 = automatic)")
async def slash_autoplay(interaction: discord.Interaction, worker: int = 0):
    if not await _guard(interaction): return
    await _start_playback(interaction, "autoplay_start", "AP_EPH", worker, timeout=60.0)


@main_bot.tree.command(name="stop", description="Stop music and disconnect")
@app_commands.describe(worker="Which worker to stop (0 = the one in your channel)")
async def slash_stop(interaction: discord.Interaction, worker: int = 0):
    if not await _guard(interaction): return
    eph = _eph(interaction.guild_id, "S_EPH")
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
        if not self.values or self.values[0] == "__empty__":
            return

        self._ctrl._selected_qid = int(self.values[0])
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
                         row=3, custom_id=f"ctl_autoplay:{view.worker.index}:{view.guild_id}")

    async def callback(self, interaction: discord.Interaction):
        await self._ctrl._dispatch(interaction, "toggle_autoplay", timeout=45.0)


class ModeButton(discord.ui.Button):
    def __init__(self, view: "ControlView"):
        self._ctrl = view
        everyone = view._mode == "all"
        super().__init__(label="🔓 Control: All" if everyone else "🔒 Control: Me",
                         style=discord.ButtonStyle.success if everyone else discord.ButtonStyle.secondary,
                         row=3, custom_id=f"ctl_mode:{view.worker.index}:{view.guild_id}")

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
                 mode: str = "me", selected_qid: int | None = None, normalize: bool = False):
        super().__init__(timeout=900)
        self.worker          = worker
        self.guild_id        = guild_id
        self.eph             = _eph(guild_id, "CC_EPH")
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

        total_pages = max(1, -(-len(queue) // PAGE_SIZE))


        self.add_item(QueueSelect(self, queue, page, total_pages, current_title, selected_qid))


        self.add_item(JumpToButton(self))
        self.add_item(RemoveButton(self))
        self.add_item(ClearSelectionButton(self))
        self.add_item(AutoplayButton(self))
        self.add_item(ModeButton(self))


        self.add_item(NormalizeButton(self))
        if total_pages > 1:
            self.add_item(PrevPageButton(self))
            self.add_item(PageLabelButton(page, total_pages))
            self.add_item(NextPageButton(self, total_pages))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        s = self.worker.session(self.guild_id)
        if not s:
            await interaction.response.send_message(
                embed=_err_embed("This session has ended — use /play or /control again"), ephemeral=True)
            return False
        custom_id = (interaction.data or {}).get("custom_id", "")
        user = interaction.user
        if custom_id.startswith("ctl_mode:"):
            # Only the person who started playback can hand control to everyone (or take it back).
            if s.get("controller_id") == user.id or _is_dev(user):
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
                                   "normalize_on", "normalize_off"):
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

    @discord.ui.button(label="⏸ Pause / ▶ Play", style=discord.ButtonStyle.primary, row=1)
    async def btn_pause(self, i, _): await self._dispatch(i, "pause_resume")

    @discord.ui.button(label="⏹ Stop", style=discord.ButtonStyle.danger, row=1, custom_id="ctl_stop:")
    async def btn_stop(self, i, _):
        resp = await self._dispatch(i, "stop")
        if resp.get("status") == "ok" and not self._stopped:
            self._stopped = True
            self.worker.apply(self.guild_id, None)
            for child in self.children: child.disabled = True
            try: await i.edit_original_response(view=self)
            except Exception: pass

    @discord.ui.button(label="⏭ Skip", style=discord.ButtonStyle.primary, row=1)
    async def btn_skip(self, i, _): await self._dispatch(i, "skip", timeout=45.0)

    @discord.ui.button(label="🔇 Mute", style=discord.ButtonStyle.secondary, row=1)
    async def btn_mute(self, i, _): await self._dispatch(i, "toggle_mute")


async def _build_control_view(worker: WorkerProcess, guild_id: int,
                               page: int = 0,
                               selected_qid: int | None = None) -> tuple[discord.Embed, ControlView]:
    resp = await worker.send({"op": "get_queue", "guild_id": guild_id}, timeout=10.0)
    ok             = resp.get("status") == "ok"
    queue: list[dict] = resp.get("queue", []) if ok else []
    current_title: str = (resp.get("current") or {}).get("title", "Unknown") if ok else "Unknown"
    muted: bool    = resp.get("muted", False) if ok else False
    loop:  bool    = resp.get("loop",  False) if ok else False
    autoplay: bool = resp.get("autoplay", False) if ok else False
    normalize: bool = resp.get("normalize", False) if ok else False
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
                        page, muted, loop, autoplay, mode, selected_qid, normalize)


    for child in view.children:
        if not hasattr(child, "label"):
            continue
        if child.label in ("🔇 Mute", "🔇 Muted"):
            child.style = discord.ButtonStyle.danger if muted else discord.ButtonStyle.secondary
            child.label = "🔇 Muted" if muted else "🔇 Mute"
        elif child.label in ("🔁 Loop", "🔁 Loop ON"):
            child.style = discord.ButtonStyle.success if loop else discord.ButtonStyle.secondary
            child.label = "🔁 Loop ON" if loop else "🔁 Loop"

    start     = page * PAGE_SIZE
    end       = min(start + PAGE_SIZE, len(queue))
    page_info = f"  (page {page + 1}/{total_pages})" if total_pages > 1 else ""
    flags     = (("  🔇 muted" if muted else "") + ("  🔁 loop" if loop else "") + ("  🎲 autoplay" if autoplay else "")
                 + ("  🎚️ normalized" if normalize else ""))
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
            "⚠️ **Panel expires in 15 minutes** — run `/control` again if buttons stop working."
        ),
        colour=COLOUR,
    )
    return embed, view


async def _refresh_control_panel(interaction: discord.Interaction, ctrl: ControlView,
                                 page: int = 0, selected_qid: int | None = None):
    try:
        embed, view = await _build_control_view(ctrl.worker, ctrl.guild_id, page, selected_qid)
        await interaction.edit_original_response(embed=embed, view=view)
    except Exception as exc:
        print(f"[Main] Control panel refresh failed: {exc}", flush=True)


@main_bot.tree.command(name="control", description="Open playback control panel")
@app_commands.describe(worker="Which worker to control (0 = the one in your channel)")
async def slash_control(interaction: discord.Interaction, worker: int = 0):
    if not await _guard(interaction): return
    eph = _eph(interaction.guild_id, "CC_EPH")
    await interaction.response.defer(thinking=True, ephemeral=eph)
    pick = await _pick_for_control(interaction, worker)
    if pick is None: return
    w, _s = pick
    embed, view = await _build_control_view(w, interaction.guild_id, page=0)
    await interaction.followup.send(embed=embed, view=view, ephemeral=eph)


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
                                          default=bool(cfg["eph"].get(k)))
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


class ResetAlgoView(discord.ui.View):
    """Confirm step for /reset-algo. Only the person who ran it can press the buttons,
    and it only ever deletes that person's own data on this server."""
    def __init__(self, user_id: int, guild_id: int):
        super().__init__(timeout=120)
        self.user_id  = user_id
        self.guild_id = guild_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id == self.user_id:
            return True
        await interaction.response.send_message(embed=_err_embed("This isn't your reset"), ephemeral=True)
        return False

    @discord.ui.button(label="Delete my algo", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, _):
        songs, queries = await asyncio.to_thread(db.reset_user, self.guild_id, self.user_id)
        await asyncio.gather(*[w.send({"op": "forget_user", "guild_id": self.guild_id, "user_id": self.user_id},
                                      timeout=5.0) for w in _workers.values()], return_exceptions=True)
        self.stop()
        await interaction.response.edit_message(embed=discord.Embed(
            title="🗑️  Your algo was reset",
            description=f"Deleted {songs} saved song(s) and {queries} search memor{'y' if queries == 1 else 'ies'} "
                        "on this server. Searches and autoplay start fresh from your next listens.",
            colour=discord.Colour.orange()), view=None)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, _):
        self.stop()
        await interaction.response.edit_message(
            embed=_simple_embed("Nothing was deleted", COLOUR), view=None)


@main_bot.tree.command(name="reset-algo", description="Delete your own saved songs and search memory on this server")
async def slash_reset_algo(interaction: discord.Interaction):
    # Personal and private: works in any channel, and there is deliberately no option to target someone else.
    if not interaction.guild_id:
        await interaction.response.send_message(embed=_err_embed("Server only"), ephemeral=True)
        return
    if not appsettings.is_whitelisted(interaction.guild_id):
        await interaction.response.send_message(embed=_err_embed("This server is not whitelisted"), ephemeral=True)
        return
    gid, uid = interaction.guild_id, interaction.user.id
    songs, size = await asyncio.to_thread(db.user_stats, gid, uid)
    queries = await asyncio.to_thread(db.user_query_count, gid, uid)
    if not songs and not queries:
        await interaction.response.send_message(
            embed=_simple_embed("You have no saved algo on this server", COLOUR), ephemeral=True)
        return
    await interaction.response.send_message(embed=discord.Embed(
        title="Reset your algo?",
        description=(f"This deletes **your** {songs} saved song(s) and {queries} search memor{'y' if queries == 1 else 'ies'} "
                     f"(about {max(1, size // 1024)} KB) on this server. Other people's data isn't affected.\n"
                     "Your searches and autoplay will stop being personalised until you listen again. "
                     "This can't be undone."),
        colour=discord.Colour.orange()), view=ResetAlgoView(uid, gid), ephemeral=True)


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
            print(f"[Main] Worker {w.index} unresponsive — restarting in 3s", flush=True)
            w.clear_state()
            await asyncio.sleep(3)
            if not _shutting_down:
                w.start()
        elif ticks % 6 == 0:
            await _resync_worker(w)   # backstop in case a state event was lost


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
            await _resync_all()
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
    asyncio.create_task(_event_server(), name="event_server")

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
                asyncio.create_task(_watch_worker(w))
                continue
            print(f"[Main] Worker {w.index} socket stale — spawning fresh", flush=True)
        w.start()
        asyncio.create_task(_watch_worker(w))
        await asyncio.sleep(1)

    print(f"[Main] All {len(_workers)} worker(s) ready", flush=True)

    if stop_event:
        await stop_event.wait()
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
    await main_bot.close()
    await asyncio.gather(*[w.terminate() for w in _workers.values()], return_exceptions=True)
    MAIN_SOCKET.unlink(missing_ok=True)
    PID_FILE.unlink(missing_ok=True)
    print("[Main] All workers stopped.", flush=True)


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

        try:
            loop.run_until_complete(_run(stop_event=stop_evt))
        finally:
            pending = asyncio.all_tasks(loop)
            for t in pending: t.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.close()

    else:
        try:
            asyncio.run(_run())
        except KeyboardInterrupt:
            pass

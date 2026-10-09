import asyncio
import glob
import json
import os
import re
import signal
import subprocess
import sys
from pathlib import Path

import discord
from discord import app_commands

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

SERVER_MODE = "--server" in sys.argv

OWNER_ID = 844921681449058326

EPH_KEYS = {
    "P_EPH":  "/play",
    "PL_EPH": "/playlist",
    "HC_EPH": "/hctest",
    "CC_EPH": "/control panel",
    "S_EPH":  "/stop",
    "RS_EPH": "/restart",
    "SD_EPH": "/shutdown",
}


CFG_PATH = Path(__file__).parent / "config.json"
with open(CFG_PATH) as _f:
    CONFIG = json.load(_f)

TOKENS         = CONFIG["tokens"]
DEFAULT_SOURCE = CONFIG.get("default_source", "sp")
PYTHON         = sys.executable

if len(TOKENS) < 2:
    sys.exit("config.json needs at least 2 tokens: tokens[0]=main, tokens[1+]=workers")


SCRIPT_DIR      = Path(__file__).parent
_bot_files      = sorted(Path(p) for p in glob.glob(str(SCRIPT_DIR / "musicbot*.py")))
_worker_scripts = _bot_files[: len(TOKENS) - 1]

if not _worker_scripts:
    sys.exit("No musicbot*.py files found.")
if len(_worker_scripts) < len(TOKENS) - 1:
    print(f"[Main] WARNING: {len(TOKENS)-1} worker token(s) but only {len(_worker_scripts)} script(s). Extra tokens ignored.", flush=True)


SETTINGS_PATH = SCRIPT_DIR / "settings.json"

def _load_settings() -> dict:
    try:
        with open(SETTINGS_PATH) as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        data = {}
    if not isinstance(data, dict):
        data = {}
    data.setdefault("G_ID", None)
    data.setdefault("CC_ID", None)
    if not isinstance(data.get("VCW"), dict):
        data["VCW"] = {}
    if not isinstance(data.get("EPH"), dict):
        data["EPH"] = {}
    return data

SETTINGS = _load_settings()

def _save_settings() -> None:
    tmp = SETTINGS_PATH.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(SETTINGS, f, indent=2)
    os.replace(tmp, SETTINGS_PATH)

def _cfg_guild() -> int | None:
    return SETTINGS.get("G_ID") or None

def _cfg_cc() -> int | None:
    return SETTINGS.get("CC_ID") or None

def _cfg_vcw(index: int) -> int | None:
    return SETTINGS["VCW"].get(str(index)) or None

def _eph(key: str) -> bool:
    return bool(SETTINGS["EPH"].get(key, False))


class WorkerProcess:
    def __init__(self, index: int, script: Path):
        self.index       = index
        self.script      = script
        self.socket_path = SCRIPT_DIR / f".worker{index}.sock"
        self.proc: subprocess.Popen | None = None
        self.busy        = False
        self.channel_id: int | None = None
        self._pgid: int | None = None
        self._output_task: asyncio.Task | None = None
        self._shutdown_requested = False

    def start(self):
        if self.socket_path.exists():
            self.socket_path.unlink(missing_ok=True)
        self.proc = subprocess.Popen(
            [PYTHON, "-u", str(self.script)],
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
        self.busy                = False
        self.channel_id          = None
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

def _worker_for_channel(channel_id: int) -> WorkerProcess | None:
    for w in _workers.values():
        if w.busy and w.channel_id == channel_id:
            return w
    return None


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

def _all_busy_embed() -> discord.Embed:
    busy = sum(1 for w in _workers.values() if w.busy)
    return discord.Embed(
        title="❌  No bots available",
        description=(f"{busy} of {len(_workers)} music bot(s) are busy and the rest are reserved "
                     f"for other channels. Wait, use `/stop`, or pick one with the `worker` option."),
        colour=discord.Colour.red(),
    )

def _is_owner(user) -> bool:
    return user.id == OWNER_ID

async def _owner_only(interaction: discord.Interaction) -> bool:
    if _is_owner(interaction.user):
        return True
    await interaction.response.send_message(
        embed=_err_embed("Only the bot owner can use this command"), ephemeral=True
    )
    return False


class MainBot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self):
        guild_id_str = _cfg_guild() or CONFIG.get("guild_id")
        if guild_id_str:
            guild = discord.Object(id=int(guild_id_str))
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)
            print(f"[Main] Commands synced to guild {guild_id_str}", flush=True)
            self.tree.clear_commands(guild=None)
            await self.tree.sync()
            print("[Main] Global commands cleared", flush=True)
        else:
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

@main_bot.event
async def on_ready():
    print(f"[Main] Logged in as {main_bot.user} — {len(_workers)} worker(s)", flush=True)


_ENV_KEY_RE = re.compile(r"^(G_ID|CC_ID|VCW(\d+))$")
_SET_RE     = re.compile(r"^set-env\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(\S*)\s*$", re.IGNORECASE)
_UNSET_RE   = re.compile(r"^unset-env\s+([A-Za-z_][A-Za-z0-9_]*)\s*$", re.IGNORECASE)
_ENV_USAGE  = (
    "**Settings commands** (wrap in double backticks):\n"
    "``set-env G_ID=<server id>``  — lock the bot to one server\n"
    "``set-env CC_ID=<text channel id>``  — control channel\n"
    "``set-env VCW1=<voice channel id>``  — fixed voice channel for worker 1 (VCW2, VCW3, ...)\n"
    "``set-env P_EPH=true``  — private replies for a command (also PL_EPH, HC_EPH, CC_EPH, S_EPH, RS_EPH, SD_EPH); `false` = shared with everyone\n"
    "``unset-env <KEY>``  — clear a setting\n"
    "``get-env``  — show current settings"
)

def _parse_env_value(raw: str) -> int | None:
    raw = raw.strip().strip("<>#@&!")
    if raw.lower() in ("", "none", "null", "off", "0"):
        return None
    if not raw.isdigit():
        raise ValueError("The value must be a numeric ID (or `none` to clear).")
    return int(raw)

def _parse_bool_value(raw: str) -> bool | None:
    raw = raw.strip().lower()
    if raw in ("", "none", "null", "default"):
        return None
    if raw in ("true", "1", "yes", "y", "on", "t"):
        return True
    if raw in ("false", "0", "no", "n", "off", "f"):
        return False
    raise ValueError("The value must be `true` or `false`.")

def _channel_guild_id(ch) -> int | None:
    g = getattr(ch, "guild", None)
    return g.id if g else None

def _apply_env(key: str, value: int | bool | None, home_guild: discord.Guild) -> str:
    key = key.upper()
    if key in EPH_KEYS:
        if value is None:
            SETTINGS["EPH"].pop(key, None)
            _save_settings()
            return f"`{key}` reset to default — {EPH_KEYS[key]} replies are public (shared with everyone)."
        SETTINGS["EPH"][key] = bool(value)
        _save_settings()
        who = "private (only the person who ran it sees them)" if value else "public (shared with everyone)"
        return f"`{key}` = `{str(bool(value)).lower()}` — {EPH_KEYS[key]} replies are {who}."
    m = _ENV_KEY_RE.match(key)
    if not m:
        raise ValueError("Unknown key. Valid keys: `G_ID`, `CC_ID`, `VCW<n>` (e.g. `VCW1`), " + ", ".join(f"`{k}`" for k in EPH_KEYS) + ".")
    target_gid = _cfg_guild() or home_guild.id


    if key == "G_ID":
        if value is None:
            SETTINGS["G_ID"] = None
            _save_settings()
            return "`G_ID` cleared — the bot works in any server it is in."
        g = main_bot.get_guild(value)
        if not g:
            raise ValueError("The main bot isn't in a server with that ID.")
        dropped: list[str] = []
        if SETTINGS.get("CC_ID"):
            ch = main_bot.get_channel(SETTINGS["CC_ID"])
            if not ch or _channel_guild_id(ch) != value:
                SETTINGS["CC_ID"] = None
                dropped.append("CC_ID")
        for idx, cid in list(SETTINGS["VCW"].items()):
            ch = main_bot.get_channel(cid)
            if not ch or _channel_guild_id(ch) != value:
                del SETTINGS["VCW"][idx]
                dropped.append(f"VCW{idx}")
        SETTINGS["G_ID"] = value
        _save_settings()
        msg = f"`G_ID` = `{value}` ({g.name})."
        if dropped:
            msg += " Cleared (belonged to another server): " + ", ".join(f"`{d}`" for d in dropped) + "."
        msg += " Restart the service once so slash commands sync to this server."
        return msg


    if key == "CC_ID":
        if value is None:
            SETTINGS["CC_ID"] = None
            _save_settings()
            return "`CC_ID` cleared — commands work in any channel."
        ch = main_bot.get_channel(value)
        if not isinstance(ch, discord.TextChannel) or ch.guild.id != target_gid:
            raise ValueError("That isn't a text channel in the bot's server.")
        SETTINGS["CC_ID"] = value
        _save_settings()
        return f"`CC_ID` = `{value}` (#{ch.name}). Slash commands now only work there, and workers post status messages there."


    idx = int(m.group(2))
    if idx not in _workers:
        raise ValueError(f"No worker #{idx}. Valid: {', '.join(str(i) for i in sorted(_workers))}.")
    if value is None:
        SETTINGS["VCW"].pop(str(idx), None)
        _save_settings()
        return f"`VCW{idx}` cleared — worker {idx} joins whichever voice channel the user is in."
    ch = main_bot.get_channel(value)
    if not isinstance(ch, discord.VoiceChannel) or ch.guild.id != target_gid:
        raise ValueError("That isn't a (non-stage) voice channel in the bot's server.")
    for other, cid in SETTINGS["VCW"].items():
        if cid == value and other != str(idx):
            raise ValueError(f"Worker {other} is already fixed to that channel.")
    SETTINGS["VCW"][str(idx)] = value
    _save_settings()
    note = ""
    w = _workers[idx]
    if w.busy and w.channel_id != value:
        note = " (it is playing elsewhere right now — applies from its next play)"
    return f"`VCW{idx}` = `{value}` (🔊 {ch.name}). Worker {idx} always joins this channel, with or without people{note}."

def _env_summary() -> str:
    lines = []
    gid = _cfg_guild()
    g = main_bot.get_guild(gid) if gid else None
    guild_summary = f"`{gid}` ({g.name if g else 'unknown server'})" if gid else "not set (any server)"
    lines.append(f"`G_ID`  = {guild_summary}")
    cc = _cfg_cc()
    cch = main_bot.get_channel(cc) if cc else None
    channel_summary = f"`{cc}` (#{cch.name if cch else 'unknown'})" if cc else "not set (any channel)"
    lines.append(f"`CC_ID` = {channel_summary}")
    for idx in sorted(_workers):
        vc = _cfg_vcw(idx)
        vch = main_bot.get_channel(vc) if vc else None
        state = "🔴 busy" if _workers[idx].busy else "🟢 free"
        lines.append(
            f"`VCW{idx}`  = "
            + (f"`{vc}` (🔊 {vch.name if vch else 'unknown'})" if vc else "not set (joins where the user is)")
            + f"  — {state}"
        )
    for k, label in EPH_KEYS.items():
        lines.append(f"`{k}` = `{str(_eph(k)).lower()}`  — {label} replies {'private' if _eph(k) else 'public'}")
    return "\n".join(lines)

@main_bot.event
async def on_message(message: discord.Message):
    if message.author.bot or not message.guild:
        return
    text = message.content.strip()
    if len(text) < 5 or not (text.startswith("``") and text.endswith("``")):
        return
    inner = text.strip("`").strip()
    low = inner.lower()
    if not (low.startswith("set-env") or low.startswith("unset-env") or low in ("get-env", "show-env")):
        return

    g = _cfg_guild()
    if g and message.guild.id != g:
        return

    if not _is_owner(message.author):
        await message.reply("❌ Only the bot owner can change settings.", mention_author=False)
        return

    if low in ("get-env", "show-env"):
        await message.reply(_env_summary(), mention_author=False)
        return

    try:
        m = _SET_RE.match(inner)
        if m:
            key = m.group(1).upper()
            value = _parse_bool_value(m.group(2)) if key in EPH_KEYS else _parse_env_value(m.group(2))
            _apply_env(key, value, message.guild)
            value_text = str(value).lower() if isinstance(value, bool) else "none" if value is None else str(value)
            reply = f"{key}={value_text} successfully set"
        else:
            m = _UNSET_RE.match(inner)
            if not m:
                await message.reply("❌ Couldn't read that.\n" + _ENV_USAGE, mention_author=False)
                return
            result = _apply_env(m.group(1), None, message.guild)
            reply = "✅ " + result
    except ValueError as exc:
        await message.reply(f"❌ {exc}", mention_author=False)
        return
    await message.reply(reply, mention_author=False)


async def _guard(interaction: discord.Interaction) -> bool:
    if not interaction.guild_id:
        await interaction.response.send_message(embed=_err_embed("Server only"), ephemeral=True)
        return False
    g = _cfg_guild()
    if g and interaction.guild_id != g:
        await interaction.response.send_message(embed=_err_embed("This bot is locked to a different server"), ephemeral=True)
        return False
    cc = _cfg_cc()
    if cc and interaction.channel_id != cc:
        await interaction.response.send_message(embed=_err_embed(f"Use the control channel: <#{cc}>"), ephemeral=True)
        return False
    return True

def _text_channel_id(interaction: discord.Interaction) -> int | None:
    return _cfg_cc() or interaction.channel_id

def _user_vc_id(interaction: discord.Interaction) -> int | None:
    m = interaction.user
    if isinstance(m, discord.Member) and m.voice and m.voice.channel:
        return m.voice.channel.id
    return None

async def _fail(interaction: discord.Interaction, msg: str, embed: discord.Embed | None = None) -> None:
    await interaction.followup.send(embed=embed or _err_embed(msg), ephemeral=True)

async def _pick_for_play(interaction: discord.Interaction, worker_arg: int):
    guild_id = interaction.guild_id
    user_vc  = _user_vc_id(interaction)
    ordered  = sorted(_workers.values(), key=lambda x: x.index)

    if worker_arg:
        w = _workers.get(worker_arg)
        if not w:
            await _fail(interaction, f"No worker #{worker_arg}. Valid: {', '.join(str(i) for i in sorted(_workers))}")
            return None
        target = _cfg_vcw(w.index) or user_vc
        if target is None:
            await _fail(interaction, f"Worker {w.index} has no fixed channel — join a voice channel first")
            return None
        if w.busy and w.channel_id != target:
            await _fail(interaction, f"Worker {w.index} is busy in another channel")
            return None
        return guild_id, target, w

    if user_vc:
        w = _worker_for_channel(user_vc)
        if w:
            return guild_id, user_vc, w
        for w in ordered:
            if not w.busy and _cfg_vcw(w.index) == user_vc:
                return guild_id, user_vc, w
        for w in ordered:
            if not w.busy and _cfg_vcw(w.index) is None:
                return guild_id, user_vc, w
        await interaction.followup.send(embed=_all_busy_embed(), ephemeral=True)
        return None

    for w in ordered:
        vcw = _cfg_vcw(w.index)
        if not w.busy and vcw:
            return guild_id, vcw, w
    await _fail(interaction, "Join a voice channel first")
    return None

async def _pick_for_control(interaction: discord.Interaction, worker_arg: int):
    guild_id = interaction.guild_id
    if worker_arg:
        w = _workers.get(worker_arg)
        if not w:
            await _fail(interaction, f"No worker #{worker_arg}. Valid: {', '.join(str(i) for i in sorted(_workers))}")
            return None
        if not w.busy or w.channel_id is None:
            await _fail(interaction, f"Worker {w.index} isn't playing anything")
            return None
        return guild_id, w.channel_id, w
    user_vc = _user_vc_id(interaction)
    if user_vc:
        w = _worker_for_channel(user_vc)
        if w:
            return guild_id, user_vc, w
        await _fail(interaction, "Nothing playing in your channel. Use /play first")
        return None
    busy = [w for w in sorted(_workers.values(), key=lambda x: x.index) if w.busy and w.channel_id]
    if len(busy) == 1:
        return guild_id, busy[0].channel_id, busy[0]
    if not busy:
        await _fail(interaction, "Nothing is playing")
        return None
    await _fail(interaction, "Several bots are playing — pick one with the `worker` option")
    return None

async def _dispatch(interaction: discord.Interaction, cmd: dict, worker: WorkerProcess, eph: bool = False) -> bool:
    resp = await worker.send(cmd, timeout=30.0)
    if resp.get("status") == "error":
        await interaction.followup.send(embed=_err_embed(resp.get("message", "Unknown error")), ephemeral=eph)
        return False
    embed = _embed_from_response(resp)
    if embed:
        await interaction.followup.send(embed=embed, ephemeral=eph)
    elif resp.get("message") == "stopped":
        await interaction.followup.send(embed=_simple_embed("⏹️  Stopped", discord.Colour.red()), ephemeral=eph)
    elif resp.get("message") == "paused":
        await interaction.followup.send(embed=_simple_embed("⏸️  Paused", COLOUR), ephemeral=eph)
    elif resp.get("message") == "resumed":
        await interaction.followup.send(embed=_simple_embed("▶️  Resumed", COLOUR), ephemeral=eph)
    elif resp.get("message") == "skipped":
        await interaction.followup.send(embed=_simple_embed("⏭️  Skipped", COLOUR), ephemeral=eph)
    elif resp.get("message") == "restarted":
        await interaction.followup.send(embed=_simple_embed("⏮️  Restarted track", COLOUR), ephemeral=eph)
    elif resp.get("message") == "seeked":
        delta = resp.get("delta_ms", 0)
        sign  = "+" if delta > 0 else ""
        icon  = "⏩" if delta > 0 else "⏪"
        await interaction.followup.send(embed=_simple_embed(f"{icon}  {sign}{delta//1000}s", COLOUR), ephemeral=eph)
    return True

_source_choices = [
    app_commands.Choice(name="Spotify (default)", value="sp"),
    app_commands.Choice(name="YouTube",           value="yt"),
    app_commands.Choice(name="SoundCloud",        value="sc"),
]


@main_bot.tree.command(name="play", description="Play a song — name or URL (YouTube / SoundCloud / Spotify)")
@app_commands.describe(query="Song name or URL", source="Search source (ignored for URLs)",
                       worker="Force a specific worker bot (0 = automatic)")
@app_commands.choices(source=_source_choices)
async def slash_play(interaction: discord.Interaction, query: str,
                     source: app_commands.Choice[str] | None = None, worker: int = 0):
    if not await _guard(interaction): return
    await interaction.response.defer(thinking=True, ephemeral=_eph("P_EPH"))
    pick = await _pick_for_play(interaction, worker)
    if pick is None: return
    guild_id, channel_id, w = pick
    src      = source.value if source else DEFAULT_SOURCE
    was_busy = w.busy
    op       = "queue_track" if (w.busy and w.channel_id == channel_id) else "search_and_play"
    w.busy       = True
    w.channel_id = channel_id
    ok = await _dispatch(interaction, {
        "op": op, "query": query, "source": src,
        "guild_id": guild_id, "channel_id": channel_id,
        "text_channel_id": _text_channel_id(interaction),
    }, w, _eph("P_EPH"))
    if not ok and not was_busy:
        w.busy       = False
        w.channel_id = None


@main_bot.tree.command(name="playlist", description="Queue a full playlist — YouTube, SoundCloud, or Spotify URL")
@app_commands.describe(query="Playlist URL or search term", source="Search source (ignored for URLs)",
                       worker="Force a specific worker bot (0 = automatic)")
@app_commands.choices(source=_source_choices)
async def slash_playlist(interaction: discord.Interaction, query: str,
                         source: app_commands.Choice[str] | None = None, worker: int = 0):
    if not await _guard(interaction): return
    await interaction.response.defer(thinking=True, ephemeral=_eph("PL_EPH"))
    pick = await _pick_for_play(interaction, worker)
    if pick is None: return
    guild_id, channel_id, w = pick
    src      = source.value if source else DEFAULT_SOURCE
    was_busy = w.busy
    w.busy       = True
    w.channel_id = channel_id
    ok = await _dispatch(interaction, {
        "op": "search_and_playlist", "query": query, "source": src,
        "guild_id": guild_id, "channel_id": channel_id,
        "text_channel_id": _text_channel_id(interaction),
    }, w, _eph("PL_EPH"))
    if not ok and not was_busy:
        w.busy       = False
        w.channel_id = None


@main_bot.tree.command(name="stop", description="Stop music and disconnect")
@app_commands.describe(worker="Which worker to stop (0 = the one in your channel)")
async def slash_stop(interaction: discord.Interaction, worker: int = 0):
    if not await _guard(interaction): return
    await interaction.response.defer(thinking=True, ephemeral=_eph("S_EPH"))
    pick = await _pick_for_control(interaction, worker)
    if pick is None: return
    guild_id, channel_id, w = pick
    resp = await w.send({"op": "stop", "guild_id": guild_id, "channel_id": channel_id})
    w.busy       = False
    w.channel_id = None
    if resp.get("status") == "ok":
        await interaction.followup.send(embed=_simple_embed("⏹️  Stopped", discord.Colour.red()), ephemeral=_eph("S_EPH"))
    else:
        await interaction.followup.send(embed=_err_embed(resp.get("message", "Error")), ephemeral=_eph("S_EPH"))


PAGE_SIZE = 25

def _truncate(text: str, n: int = 80) -> str:
    return text if len(text) <= n else text[:n - 1] + "…"


class QueueSelect(discord.ui.Select):

    def __init__(self, view: "ControlView", queue: list[dict],
                 page: int, total_pages: int, current_title: str,
                 selected_idx: int | None = None):
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
                selected = (idx == selected_idx)
                options.append(discord.SelectOption(
                    label=label, value=str(idx), description=desc,
                    emoji="▶️" if selected else "🎶",
                    default=selected,
                ))

        page_info = f"  [pg {page + 1}/{total_pages}]" if total_pages > 1 else ""
        if selected_idx is not None:

            sel_item  = next((q for q in queue if q["index"] == selected_idx), None)
            sel_title = _truncate(sel_item["title"], 40) if sel_item else "?"
            placeholder = f"✅ #{selected_idx + 1} {sel_title}{page_info} — choose action below"
        else:
            placeholder = f"▶ {_truncate(current_title, 45)}{page_info} — select a track…"

        super().__init__(
            placeholder=placeholder,
            min_values=0, max_values=1,
            options=options,
            row=2,
        )

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=_eph("CC_EPH"))
        if not self.values or self.values[0] == "__empty__":
            return

        self._ctrl._selected_idx = int(self.values[0])
        await _refresh_control_panel(
            interaction, self._ctrl.worker,
            self._ctrl.guild_id, self._ctrl.channel_id,
            page=self._ctrl._queue_page,
            selected_idx=self._ctrl._selected_idx,
        )


class JumpToButton(discord.ui.Button):
    def __init__(self, view: "ControlView"):
        self._ctrl = view
        has_sel    = view._selected_idx is not None
        super().__init__(
            label="⏭ Jump To", style=discord.ButtonStyle.primary,
            disabled=not has_sel, row=3,
        )

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=_eph("CC_EPH"))
        idx  = self._ctrl._selected_idx
        resp = await self._ctrl.worker.send({
            "op": "jump_to", "guild_id": self._ctrl.guild_id,
            "channel_id": self._ctrl.channel_id, "index": idx,
        }, timeout=30.0)
        if resp.get("status") == "error":
            await interaction.followup.send(embed=_err_embed(resp.get("message", "Error")), ephemeral=_eph("CC_EPH"))
        else:
            title = resp.get("title", "track")
            rem   = resp.get("queue_remaining", 0)
            await interaction.followup.send(
                embed=_simple_embed(f"⏭  Jumped to **{_truncate(title, 60)}** — {rem} remaining", COLOUR),
                ephemeral=_eph("CC_EPH"),
            )
        await _refresh_control_panel(interaction, self._ctrl.worker,
                                     self._ctrl.guild_id, self._ctrl.channel_id,
                                     page=0, selected_idx=None)


class RemoveButton(discord.ui.Button):
    def __init__(self, view: "ControlView"):
        self._ctrl = view
        has_sel    = view._selected_idx is not None
        super().__init__(
            label="🗑 Remove", style=discord.ButtonStyle.danger,
            disabled=not has_sel, row=3,
        )

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=_eph("CC_EPH"))
        idx  = self._ctrl._selected_idx
        resp = await self._ctrl.worker.send({
            "op": "remove_from_queue", "guild_id": self._ctrl.guild_id,
            "channel_id": self._ctrl.channel_id, "index": idx,
        }, timeout=15.0)
        if resp.get("status") == "error":
            await interaction.followup.send(embed=_err_embed(resp.get("message", "Error")), ephemeral=_eph("CC_EPH"))
        else:
            title = resp.get("title", "track")
            rem   = resp.get("queue_remaining", 0)
            await interaction.followup.send(
                embed=_simple_embed(f"🗑  Removed **{_truncate(title, 60)}** — {rem} remaining", discord.Colour.orange()),
                ephemeral=_eph("CC_EPH"),
            )

        await _refresh_control_panel(interaction, self._ctrl.worker,
                                     self._ctrl.guild_id, self._ctrl.channel_id,
                                     page=self._ctrl._queue_page, selected_idx=None)


class ClearSelectionButton(discord.ui.Button):
    def __init__(self, view: "ControlView"):
        self._ctrl = view
        super().__init__(
            label="✖ Clear", style=discord.ButtonStyle.secondary,
            disabled=(view._selected_idx is None), row=3,
        )

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=_eph("CC_EPH"))
        await _refresh_control_panel(interaction, self._ctrl.worker,
                                     self._ctrl.guild_id, self._ctrl.channel_id,
                                     page=self._ctrl._queue_page, selected_idx=None)


class PrevPageButton(discord.ui.Button):
    def __init__(self, view: "ControlView"):
        self._ctrl = view
        super().__init__(label="◀ Prev", style=discord.ButtonStyle.secondary,
                         disabled=(view._queue_page == 0), row=4)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=_eph("CC_EPH"))
        await _refresh_control_panel(interaction, self._ctrl.worker,
                                     self._ctrl.guild_id, self._ctrl.channel_id,
                                     page=self._ctrl._queue_page - 1, selected_idx=None)


class PageLabelButton(discord.ui.Button):
    def __init__(self, page: int, total: int):
        super().__init__(label=f"Page {page + 1} / {total}",
                         style=discord.ButtonStyle.secondary, disabled=True, row=4)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=_eph("CC_EPH"))


class NextPageButton(discord.ui.Button):
    def __init__(self, view: "ControlView", total_pages: int):
        self._ctrl  = view
        self._total = total_pages
        super().__init__(label="Next ▶", style=discord.ButtonStyle.secondary,
                         disabled=(view._queue_page >= total_pages - 1), row=4)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=_eph("CC_EPH"))
        await _refresh_control_panel(interaction, self._ctrl.worker,
                                     self._ctrl.guild_id, self._ctrl.channel_id,
                                     page=self._ctrl._queue_page + 1, selected_idx=None)


class ControlView(discord.ui.View):
    def __init__(self, worker: WorkerProcess, guild_id: int, channel_id: int,
                 queue: list[dict], current_title: str, page: int = 0,
                 muted: bool = False, loop: bool = False,
                 selected_idx: int | None = None):
        super().__init__(timeout=900)
        self.worker          = worker
        self.guild_id        = guild_id
        self.channel_id      = channel_id
        self._stopped        = False
        self._queue_page     = page
        self._queue          = queue
        self._current_title  = current_title
        self._muted          = muted
        self._loop           = loop
        self._selected_idx   = selected_idx

        total_pages = max(1, -(-len(queue) // PAGE_SIZE))


        self.add_item(QueueSelect(self, queue, page, total_pages, current_title, selected_idx))


        self.add_item(JumpToButton(self))
        self.add_item(RemoveButton(self))
        self.add_item(ClearSelectionButton(self))


        if total_pages > 1:
            self.add_item(PrevPageButton(self))
            self.add_item(PageLabelButton(page, total_pages))
            self.add_item(NextPageButton(self, total_pages))

    async def _dispatch(self, interaction: discord.Interaction, op: str, **extra):
        await interaction.response.defer(ephemeral=_eph("CC_EPH"))
        resp = await self.worker.send({
            "op": op, "guild_id": self.guild_id,
            "channel_id": self.channel_id, **extra
        })
        if resp.get("status") == "error":
            await interaction.followup.send(
                embed=_err_embed(resp.get("message", "Error")), ephemeral=_eph("CC_EPH"))
            return
        msg = resp.get("message", "")
        labels = {
            "paused":    "⏸️  Paused",   "resumed":   "▶️  Resumed",
            "skipped":   "⏭️  Skipped",  "restarted": "⏮️  Restarted track",
            "stopped":   "⏹️  Stopped",
        }
        if msg in labels:
            await interaction.followup.send(
                embed=_simple_embed(labels[msg], COLOUR), ephemeral=_eph("CC_EPH"))
        elif msg == "seeked":
            delta = resp.get("delta_ms", 0)
            sign  = "+" if delta > 0 else ""
            icon  = "⏩" if delta > 0 else "⏪"
            await interaction.followup.send(
                embed=_simple_embed(f"{icon}  {sign}{delta//1000}s", COLOUR), ephemeral=_eph("CC_EPH"))
        elif msg == "muted":
            await interaction.followup.send(
                embed=_simple_embed("🔇  Muted — bot will play silently", COLOUR), ephemeral=_eph("CC_EPH"))
        elif msg == "unmuted":
            await interaction.followup.send(
                embed=_simple_embed("🔊  Unmuted", COLOUR), ephemeral=_eph("CC_EPH"))
        elif msg == "loop_on":
            await interaction.followup.send(
                embed=_simple_embed("🔁  Loop ON — current track will repeat", COLOUR), ephemeral=_eph("CC_EPH"))
        elif msg == "loop_off":
            await interaction.followup.send(
                embed=_simple_embed("➡️  Loop OFF", COLOUR), ephemeral=_eph("CC_EPH"))
        if msg in ("skipped", "restarted", "muted", "unmuted", "loop_on", "loop_off"):
            await _refresh_control_panel(interaction, self.worker,
                                         self.guild_id, self.channel_id,
                                         page=self._queue_page, selected_idx=None)

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

    @discord.ui.button(label="⏹ Stop", style=discord.ButtonStyle.danger, row=1)
    async def btn_stop(self, i, _):
        await self._dispatch(i, "stop")
        if not self._stopped:
            self._stopped = True
            self.worker.busy = False
            self.worker.channel_id = None
            for child in self.children: child.disabled = True
            try: await i.edit_original_response(view=self)
            except Exception: pass

    @discord.ui.button(label="⏭ Skip", style=discord.ButtonStyle.primary, row=1)
    async def btn_skip(self, i, _): await self._dispatch(i, "skip")

    @discord.ui.button(label="🔇 Mute", style=discord.ButtonStyle.secondary, row=1)
    async def btn_mute(self, i, _): await self._dispatch(i, "toggle_mute")


async def _build_control_view(worker: WorkerProcess, guild_id: int, channel_id: int,
                               page: int = 0,
                               selected_idx: int | None = None) -> tuple[discord.Embed, ControlView]:
    resp = await worker.send({"op": "get_queue", "guild_id": guild_id,
                              "channel_id": channel_id}, timeout=10.0)
    ok             = resp.get("status") == "ok"
    queue: list[dict] = resp.get("queue", []) if ok else []
    current_title: str = (resp.get("current") or {}).get("title", "Unknown") if ok else "Unknown"
    muted: bool    = resp.get("muted", False) if ok else False
    loop:  bool    = resp.get("loop",  False) if ok else False

    total_pages = max(1, -(-len(queue) // PAGE_SIZE))
    page        = max(0, min(page, total_pages - 1))


    if selected_idx is not None:
        if not any(q["index"] == selected_idx for q in queue):
            selected_idx = None

    view  = ControlView(worker, guild_id, channel_id, queue, current_title,
                        page, muted, loop, selected_idx)


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
    flags     = ("  🔇 muted" if muted else "") + ("  🔁 loop" if loop else "")
    sel_info  = f"\n✅ **Selected:** #{selected_idx + 1}" if selected_idx is not None else ""
    embed = discord.Embed(
        title=f"🎛️  Playback Controls — Worker {worker.index}",
        description=(
            f"▶ **{_truncate(current_title, 80)}**{flags}\n"
            f"{'📋 **' + str(len(queue)) + '** track(s) in queue' + page_info + f' — showing #{start+1}–#{end}' if queue else '📭 No tracks queued'}"
            f"{sel_info}\n\n"
            "⚠️ **Panel expires in 15 minutes** — run `/control` again if buttons stop working."
        ),
        colour=COLOUR,
    )
    return embed, view


async def _refresh_control_panel(interaction: discord.Interaction,
                                 worker: WorkerProcess, guild_id: int, channel_id: int,
                                 page: int = 0, selected_idx: int | None = None):
    try:
        embed, view = await _build_control_view(worker, guild_id, channel_id, page, selected_idx)
        await interaction.edit_original_response(embed=embed, view=view)
    except Exception as exc:
        print(f"[Main] Control panel refresh failed: {exc}", flush=True)


@main_bot.tree.command(name="control", description="Open playback control panel")
@app_commands.describe(worker="Which worker to control (0 = the one in your channel)")
async def slash_control(interaction: discord.Interaction, worker: int = 0):
    if not await _guard(interaction): return
    await interaction.response.defer(thinking=True, ephemeral=_eph("CC_EPH"))
    pick = await _pick_for_control(interaction, worker)
    if pick is None: return
    guild_id, channel_id, w = pick
    embed, view = await _build_control_view(w, guild_id, channel_id, page=0)
    await interaction.followup.send(embed=embed, view=view, ephemeral=_eph("CC_EPH"))


@main_bot.tree.command(name="hctest", description="Health check — tests all systems")
async def slash_hctest(interaction: discord.Interaction):
    if not await _guard(interaction): return
    await interaction.response.defer(thinking=True, ephemeral=_eph("HC_EPH"))
    import time
    start = time.monotonic()
    results = []
    for w in sorted(_workers.values(), key=lambda x: x.index):
        resp = await w.send({"op": "ping"}, timeout=10.0)
        results.append((w.index, resp))
    elapsed = round((time.monotonic() - start) * 1000)
    all_ok  = all(r.get("status") == "ok" and r.get("lavalink_ok") for _, r in results)
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
            f"Workers: `{len(_workers)}` total, `{sum(1 for w in _workers.values() if w.busy)}` busy"
        ),
        inline=False,
    )
    for idx, r in results:
        vcw = _cfg_vcw(idx)
        fixed = f"\nFixed VC: <#{vcw}>" if vcw else ""
        if r.get("status") == "ok":
            disc  = "✅" if r.get("discord_ok") else "❌"
            lava  = "✅" if r.get("lavalink_ok") else "❌"
            dms   = r.get("discord_ms", -1)
            state = "🔴 Busy" if r.get("busy") else "🟢 Free"
            qlen  = r.get("queue_len", 0)
            val   = (
                f"Discord: {disc} `{dms}ms`\n"
                f"Lavalink: {lava} `{r.get('lavalink_uri','?')}`\n"
                f"Status: {state}" + (f"  |  Queue: `{qlen}`" if r.get("busy") else "") + fixed
            )
        else:
            val = f"❌ {r.get('message', 'Not responding')}{fixed}"
        embed.add_field(name=f"🎵  Worker {idx}", value=val, inline=True)
    embed.set_footer(text=f"Check completed in {elapsed}ms")
    await interaction.followup.send(embed=embed, ephemeral=_eph("HC_EPH"))


SERVICE_NAME = "SERVER_musicbots"

async def _systemctl(action: str):
    proc = await asyncio.create_subprocess_exec(
        "systemctl", action, SERVICE_NAME,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    await proc.wait()

async def _delayed_systemctl(action: str, delay: float = 2.0):
    await asyncio.sleep(delay)
    await _systemctl(action)


@main_bot.tree.command(name="restart", description="Restart worker(s) or the entire service")
@app_commands.describe(
    worker="Which worker to restart (1, 2, … or 0 = all workers)",
    full="Restart the entire musicbot service including main controller (default: False)",
)
async def slash_restart(interaction: discord.Interaction, worker: int = 0, full: bool = False):
    if not await _owner_only(interaction): return
    if not await _guard(interaction): return
    await interaction.response.defer(thinking=True, ephemeral=_eph("RS_EPH"))


    if full:
        embed = discord.Embed(
            title="🔄  Restarting entire service…",
            description=(
                f"Running `systemctl restart {SERVICE_NAME}`.\n"
                "All workers and the main controller will restart. "
                "Music will stop and resume from scratch."
            ),
            colour=COLOUR,
        )
        embed.set_footer(text="This message will not update — the bot is restarting.")
        await interaction.followup.send(embed=embed, ephemeral=_eph("RS_EPH"))

        asyncio.ensure_future(_delayed_systemctl("restart", delay=2.0))
        return


    targets: list[WorkerProcess] = []
    if worker == 0:
        targets = sorted(_workers.values(), key=lambda x: x.index)
    elif worker in _workers:
        targets = [_workers[worker]]
    else:
        await interaction.followup.send(
            embed=_err_embed(f"No worker #{worker}. Valid: 0 (all), {', '.join(str(i) for i in sorted(_workers))}"),
            ephemeral=_eph("RS_EPH"),
        )
        return

    label = f"all {len(targets)} worker(s)" if len(targets) > 1 else f"Worker {targets[0].index}"
    await interaction.followup.send(
        embed=discord.Embed(
            title=f"🔄  Restarting {label}…",
            description="Stopping music and reconnecting. This takes a few seconds.",
            colour=COLOUR,
        ),
        ephemeral=_eph("RS_EPH"),
    )

    lines: list[str] = []
    for w in targets:
        try:
            await w.restart()
            for _ in range(15):
                await asyncio.sleep(1)
                resp = await w.send({"op": "ping"}, timeout=3.0)
                if resp.get("status") == "ok":
                    lines.append(f"✅ Worker {w.index} — online (Lavalink: {'✅' if resp.get('lavalink_ok') else '❌'})")
                    break
            else:
                lines.append(f"⚠️ Worker {w.index} — started but socket not yet ready")
        except Exception as exc:
            lines.append(f"❌ Worker {w.index} — error: {exc}")

    result_embed = discord.Embed(
        title=f"🔄  Restart complete — {label}",
        description="\n".join(lines),
        colour=discord.Colour.green() if all("✅" in l for l in lines) else discord.Colour.orange(),
    )
    result_embed.set_footer(text="Use full:True to restart the entire service including main controller.")
    try:
        await interaction.edit_original_response(embed=result_embed)
    except Exception:
        await interaction.followup.send(embed=result_embed, ephemeral=_eph("RS_EPH"))


@main_bot.tree.command(name="shutdown", description="Shut down worker(s) or the entire service")
@app_commands.describe(
    worker="Which worker to shut down (1, 2, … or 0 = all workers)",
    full="Shut down the entire musicbot service including main controller (default: False)",
)
async def slash_shutdown(interaction: discord.Interaction, worker: int = 0, full: bool = False):
    if not await _owner_only(interaction): return
    if not await _guard(interaction): return
    await interaction.response.defer(thinking=True, ephemeral=_eph("SD_EPH"))


    if full:
        embed = discord.Embed(
            title="🔴  Shutting down entire service…",
            description=(
                f"Running `systemctl stop {SERVICE_NAME}`.\n"
                "All workers and the main controller will stop permanently. "
                f"Use `systemctl start {SERVICE_NAME}` on the server to bring it back up."
            ),
            colour=discord.Colour.red(),
        )
        embed.set_footer(text="This message will not update — the bot is shutting down.")
        await interaction.followup.send(embed=embed, ephemeral=_eph("SD_EPH"))
        asyncio.ensure_future(_delayed_systemctl("stop", delay=2.0))
        return


    targets: list[WorkerProcess] = []
    if worker == 0:
        targets = sorted(_workers.values(), key=lambda x: x.index)
    elif worker in _workers:
        targets = [_workers[worker]]
    else:
        await interaction.followup.send(
            embed=_err_embed(f"No worker #{worker}. Valid: 0 (all), {', '.join(str(i) for i in sorted(_workers))}"),
            ephemeral=_eph("SD_EPH"),
        )
        return

    label = f"all {len(targets)} worker(s)" if len(targets) > 1 else f"Worker {targets[0].index}"
    await interaction.followup.send(
        embed=discord.Embed(
            title=f"🔴  Shutting down {label}…",
            description="Stopping music and disconnecting. Workers will **not** restart.",
            colour=discord.Colour.red(),
        ),
        ephemeral=_eph("SD_EPH"),
    )

    lines: list[str] = []
    for w in targets:
        w._shutdown_requested = True
        try:
            await w.terminate()
            w.busy       = False
            w.channel_id = None
            lines.append(f"🔴 Worker {w.index} — shut down")
        except Exception as exc:
            lines.append(f"❌ Worker {w.index} — error: {exc}")

    result_embed = discord.Embed(
        title=f"🔴  Shutdown complete — {label}",
        description="\n".join(lines),
        colour=discord.Colour.red(),
    )
    result_embed.set_footer(text="Use /restart to bring worker(s) back up. Use full:True to shut down the entire service.")
    try:
        await interaction.edit_original_response(embed=result_embed)
    except Exception:
        await interaction.followup.send(embed=result_embed, ephemeral=_eph("SD_EPH"))


@main_bot.tree.command(name="purge", description="Delete the last N bot messages in this channel (default: 20)")
@app_commands.describe(limit="Number of messages to scan (1–100, default 20)")
async def slash_purge(interaction: discord.Interaction, limit: int = 20):
    if not await _owner_only(interaction): return
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
    msg = await interaction.followup.send(embed=embed, ephemeral=False)


async def _watch_worker(w: WorkerProcess):
    while True:
        await asyncio.sleep(5)
        if _shutting_down:
            return

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
            w.busy = False
            w.channel_id = None
            await asyncio.sleep(3)
            if not _shutting_down:
                w.start()


async def _resync_worker(w: WorkerProcess) -> bool:
    resp = await w.send({"op": "sync"}, timeout=8.0)
    if resp.get("status") != "ok":
        return False
    if resp.get("busy"):
        w.busy       = True
        w.channel_id = resp.get("channel_id")
        print(
            f"[Main] Resynced Worker {w.index} — busy ch={w.channel_id} "
            f"playing={resp.get('current_title','?')!r} queue={resp.get('queue_len',0)}",
            flush=True,
        )
    else:
        w.busy       = False
        w.channel_id = None
        print(f"[Main] Resynced Worker {w.index} — idle", flush=True)
    return True


async def _run(stop_event: asyncio.Event | None = None):
    global _shutting_down

    main_task = asyncio.create_task(main_bot.start(TOKENS[0]), name="main_bot")
    await asyncio.sleep(5)

    for i, script in enumerate(_worker_scripts, start=1):
        _workers[i] = WorkerProcess(index=i, script=script)


    for w in sorted(_workers.values(), key=lambda x: x.index):
        if w.socket_path.exists():
            print(f"[Main] Socket exists for Worker {w.index} — attempting resync ...", flush=True)
            alive = await _resync_worker(w)
            if alive:
                print(f"[Main] Worker {w.index} already running — skipping spawn", flush=True)
                asyncio.create_task(_watch_worker(w))
                continue
            print(f"[Main] Worker {w.index} socket stale — spawning fresh", flush=True)
        w.start()
        asyncio.create_task(_watch_worker(w))
        await asyncio.sleep(1)

    print(f"[Main] All {len(_workers)} worker(s) ready", flush=True)

    if stop_event:
        await stop_event.wait()
    else:
        try:
            await main_task
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass

    _shutting_down = True
    print("[Main] Shutting down ...", flush=True)
    await main_bot.close()
    await asyncio.gather(*[w.terminate() for w in _workers.values()], return_exceptions=True)
    print("[Main] All workers stopped.", flush=True)


if __name__ == "__main__":
    if SERVER_MODE:
        loop     = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        stop_evt = asyncio.Event()

        def _sig():
            print("[Main] Signal — stopping ...", flush=True)
            loop.call_soon_threadsafe(stop_evt.set)

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
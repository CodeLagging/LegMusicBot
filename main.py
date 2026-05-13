import asyncio
import glob
import importlib.util
import json
import signal
import sys
from pathlib import Path

import discord
from discord import app_commands

# Flush stdout so systemd/journald sees every line immediately
sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]
sys.stderr.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

SERVER_MODE = "--server" in sys.argv

# Config
CFG_PATH = Path(__file__).parent / "config.json"
with open(CFG_PATH) as _f:
    CONFIG = json.load(_f)

TOKENS         = CONFIG["tokens"]
DEFAULT_SOURCE = CONFIG.get("default_source", "sp")

if len(TOKENS) < 2:
    sys.exit(
        "config.json needs at least 2 tokens:\n"
        "  tokens[0] = main controller\n"
        "  tokens[1+] = workers (one per musicbot*.py)\n"
    )

# Discover worker scripts
SCRIPT_DIR = Path(__file__).parent
_bot_files = sorted(Path(p) for p in glob.glob(str(SCRIPT_DIR / "musicbot*.py")))
_worker_tokens = TOKENS[1:]
_worker_scripts = _bot_files[: len(_worker_tokens)]

if not _worker_scripts:
    sys.exit("No musicbot*.py files found. Create at least musicbot1.py.")
if len(_worker_scripts) < len(_worker_tokens):
    print(
        f"[Main] WARNING: {len(_worker_tokens)} worker token(s) but only "
        f"{len(_worker_scripts)} musicbot*.py file(s). Extra tokens ignored.",
        flush=True,
    )


def _load_worker_class(script: Path):
    spec   = importlib.util.spec_from_file_location(script.stem, script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.Worker
_all_workers: dict[int, object] = {}


async def _status_callback(index: int, _ignored, busy: bool):
    worker = _all_workers.get(index)
    if not worker:
        return
    worker.busy = busy
    if not busy:
        print(f"[Main] Worker {index} is now free", flush=True)


def _assign_worker():
    for w in sorted(_all_workers.values(), key=lambda x: x.index):
        if not w.busy:
            print(f"[Main] Assigned Worker {w.index}", flush=True)
            return w
    return None


def _worker_for_channel(channel_id: int):
    for w in _all_workers.values():
        if w.busy and getattr(w, "channel_id", None) == channel_id:
            return w
    return None


# Main bot/Central Controller Bot
class MainBot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self):
        await self.tree.sync()
        print("[Main] Slash commands synced globally", flush=True)


main_bot = MainBot()


# Shared helpers
async def _get_voice_ids(
    interaction: discord.Interaction,
) -> tuple[int, int] | None:
    if not interaction.guild:
        await interaction.followup.send(
            embed=discord.Embed(title="❌ Server only", colour=discord.Colour.red()),
            ephemeral=True,
        )
        return None
    member = interaction.user
    if (
        not isinstance(member, discord.Member)
        or not member.voice
        or not member.voice.channel
    ):
        await interaction.followup.send(
            embed=discord.Embed(
                title="❌ Join a voice channel first",
                colour=discord.Colour.red(),
            ),
            ephemeral=True,
        )
        return None
    return interaction.guild_id, member.voice.channel.id


def _all_busy_embed() -> discord.Embed:
    busy_count = sum(1 for w in _all_workers.values() if w.busy)
    return discord.Embed(
        title="❌ No bots available",
        description=(
            f"All {busy_count} music bot(s) are busy right now.\n"
            "Wait for one to finish or use `/stop` to free one up."
        ),
        colour=discord.Colour.red(),
    )


# Source choices
_source_choices = [
    app_commands.Choice(name="Spotify (Default)", value="sp"),
    app_commands.Choice(name="YouTube",           value="yt"),
    app_commands.Choice(name="SoundCloud",        value="sc"),
]


# /play command
@main_bot.tree.command(
    name="play",
    description="Play a song — name or URL (YouTube / SoundCloud / Spotify)",
)
@app_commands.describe(
    query="Song name, or a URL from YouTube / SoundCloud / Spotify",
    source="Search source for plain-text queries (ignored for URLs)",
)
@app_commands.choices(source=_source_choices)
async def slash_play(
    interaction: discord.Interaction,
    query: str,
    source: app_commands.Choice[str] | None = None,
):
    await interaction.response.defer(thinking=True, ephemeral=True)

    ids = await _get_voice_ids(interaction)
    if ids is None:
        return

    guild_id, channel_id = ids
    src = source.value if source else DEFAULT_SOURCE

    # Reuse worker already in this channel, or assign a free one
    worker = _worker_for_channel(channel_id) or _assign_worker()

    if worker is None:
        await interaction.followup.send(embed=_all_busy_embed(), ephemeral=True)
        return

    op = (
        "queue_track"
        if (worker.busy and getattr(worker, "channel_id", None) == channel_id)
        else "search_and_play"
    )

    await worker.command_queue.put({
        "op": op,
        "query": query,
        "source": src,
        "guild_id": guild_id,
        "channel_id": channel_id,
        "text_channel_id": interaction.channel_id,
        "interaction": interaction,
    })


# /playlist
@main_bot.tree.command(
    name="playlist",
    description="Queue a full playlist — YouTube, SoundCloud, or Spotify URL",
)
@app_commands.describe(
    query="Playlist URL (YouTube / SoundCloud / Spotify) or search term",
    source="Search source for plain-text queries (ignored for URLs)",
)
@app_commands.choices(source=_source_choices)
async def slash_playlist(
    interaction: discord.Interaction,
    query: str,
    source: app_commands.Choice[str] | None = None,
):
    await interaction.response.defer(thinking=True, ephemeral=True)

    ids = await _get_voice_ids(interaction)
    if ids is None:
        return

    guild_id, channel_id = ids
    src = source.value if source else DEFAULT_SOURCE
    worker = _worker_for_channel(channel_id) or _assign_worker()

    if worker is None:
        await interaction.followup.send(embed=_all_busy_embed(), ephemeral=True)
        return

    await worker.command_queue.put({
        "op": "search_and_playlist",
        "query": query,
        "source": src,
        "guild_id": guild_id,
        "channel_id": channel_id,
        "text_channel_id": interaction.channel_id,
        "interaction": interaction,
    })


# /stop command
@main_bot.tree.command(name="stop", description="Stop music and disconnect the bot in your channel")
async def slash_stop(interaction: discord.Interaction):
    await interaction.response.defer(thinking=True, ephemeral=True)

    ids = await _get_voice_ids(interaction)
    if ids is None:
        return

    guild_id, channel_id = ids
    worker = _worker_for_channel(channel_id)

    if worker is None:
        await interaction.followup.send(
            embed=discord.Embed(
                title="❌ Nothing playing",
                description="No music bot is active in your voice channel.",
                colour=discord.Colour.red(),
            ),
            ephemeral=True,
        )
        return

    await worker.command_queue.put({
        "op": "stop",
        "guild_id": guild_id,
        "channel_id": channel_id,
        "interaction": interaction,
    })



class ControlView(discord.ui.View):

    def __init__(self, worker, guild_id: int, channel_id: int):
        super().__init__(timeout=900)   # 15 min — matches Discord's token lifetime
        self.worker     = worker
        self.guild_id   = guild_id
        self.channel_id = channel_id

    async def _send(self, interaction: discord.Interaction, op: str, **extra):
        await interaction.response.defer(ephemeral=True)
        await self.worker.command_queue.put({
            "op": op,
            "guild_id": self.guild_id,
            "channel_id": self.channel_id,
            "interaction": interaction,
            **extra,
        })

    # ── Top row ────────────────────────────────────────────────────────────────

    @discord.ui.button(label="⏪ 10s", style=discord.ButtonStyle.secondary, row=0)
    async def rw10(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self._send(interaction, "seek", delta_ms=-10_000)

    @discord.ui.button(label="⏪ 5s", style=discord.ButtonStyle.secondary, row=0)
    async def rw5(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self._send(interaction, "seek", delta_ms=-5_000)

    @discord.ui.button(label="⏩ 5s", style=discord.ButtonStyle.secondary, row=0)
    async def ff5(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self._send(interaction, "seek", delta_ms=5_000)

    @discord.ui.button(label="⏩ 10s", style=discord.ButtonStyle.secondary, row=0)
    async def ff10(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self._send(interaction, "seek", delta_ms=10_000)

    # ── Bottom row ─────────────────────────────────────────────────────────────

    @discord.ui.button(label="⏮ Backward", style=discord.ButtonStyle.primary, row=1)
    async def backward(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self._send(interaction, "backward")

    @discord.ui.button(label="⏸ Pause / ▶ Play", style=discord.ButtonStyle.primary, row=1)
    async def pause_resume(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self._send(interaction, "pause_resume")

    @discord.ui.button(label="⏹ Stop", style=discord.ButtonStyle.danger, row=1)
    async def stop(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self._send(interaction, "stop")
        self.stop()   # disable the view after stopping

    @discord.ui.button(label="⏭ Skip", style=discord.ButtonStyle.primary, row=1)
    async def skip(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self._send(interaction, "skip")


@main_bot.tree.command(
    name="control",
    description="Open the playback control panel for your voice channel",
)
async def slash_control(interaction: discord.Interaction):
    await interaction.response.defer(thinking=True, ephemeral=True)

    ids = await _get_voice_ids(interaction)
    if ids is None:
        return

    guild_id, channel_id = ids
    worker = _worker_for_channel(channel_id)

    if worker is None:
        await interaction.followup.send(
            embed=discord.Embed(
                title="❌ Nothing playing",
                description=(
                    "No music bot is active in your voice channel.\n"
                    "Use `/play` first."
                ),
                colour=discord.Colour.red(),
            ),
            ephemeral=True,
        )
        return

    embed = discord.Embed(
        title="🎛️  Playback Controls",
        description=(
            "Use the buttons below to control playback.\n"
            "⚠️ **This panel expires after 15 minutes** — run `/control` again if buttons stop working."
        ),
        colour=discord.Colour.from_str("#5865F2"),
    )

    view = ControlView(worker=worker, guild_id=guild_id, channel_id=channel_id)
    await interaction.followup.send(embed=embed, view=view, ephemeral=True)


# Ready event
@main_bot.event
async def on_ready():
    print(
        f"[Main] Logged in as {main_bot.user} — "
        f"controlling {len(_all_workers)} worker(s)",
        flush=True,
    )


# Core Startup and Shutdown
async def _run(stop_event: asyncio.Event | None = None):
    for i, (script, token) in enumerate(zip(_worker_scripts, _worker_tokens), start=1):
        WorkerClass = _load_worker_class(script)
        w = WorkerClass(index=i, token=token, status_callback=_status_callback)
        w.channel_id = None
        _all_workers[i] = w

    tasks = [asyncio.create_task(main_bot.start(TOKENS[0]), name="main_bot")]
    await asyncio.sleep(5)
    for w in _all_workers.values():
        tasks.append(asyncio.create_task(w.start(), name=f"worker_{w.index}"))
        await asyncio.sleep(2)

    print(
        f"[Main] Starting {len(tasks)} bot(s) ... "
        f"(mode: {'server' if SERVER_MODE else 'normal'})",
        flush=True,
    )

    runner = asyncio.gather(*tasks, return_exceptions=True)

    if stop_event is not None:
        await stop_event.wait()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    else:
        try:
            await runner
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass

    print("[Main] Shutting down ...", flush=True)
    await main_bot.close()
    for w in _all_workers.values():
        try:
            await w.bot.close()
        except Exception:
            pass
    print("[Main] All bots stopped.", flush=True)


# Start the main bot
if __name__ == "__main__":
    if SERVER_MODE:
        loop     = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        stop_evt = asyncio.Event()

        def _handle_signal():
            print("[Main] Signal received — stopping ...", flush=True)
            loop.call_soon_threadsafe(stop_evt.set)

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _handle_signal)

        try:
            loop.run_until_complete(_run(stop_event=stop_evt))
        finally:
            pending = asyncio.all_tasks(loop)
            for t in pending:
                t.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.close()
    else:
        try:
            asyncio.run(_run())
        except KeyboardInterrupt:
            pass
"""
musicbot1.py — Music Worker Bot
=================================
Copy as musicbot1.py, musicbot2.py, musicbot3.py ...

Silent worker — receives commands from main.py via command_queue.
Sources: YouTube Music, SoundCloud, Spotify (via LavaSrc).

Clean-version filter applies to ALL sources on plain-text queries.
Filter is skipped only when the user's query explicitly requests
an altered version (sped up, slowed, reverb, etc.).

3-minute idle timeout:
  - Fires when queue is empty and nothing is playing
  - Fires when all members leave the VC (even mid-song)
  Both reset when a new track starts playing.

Requirements:
    pip install "discord.py[voice]" "wavelink>=3.4.0"
    Lavalink: youtube-plugin + lavasrc-plugin (see application.yml)
"""

import asyncio
import json
import re
from pathlib import Path

import discord
import wavelink

# ── Config ─────────────────────────────────────────────────────────────────────
_cfg = Path(__file__).parent / "config.json"
with open(_cfg) as _f:
    _CONFIG = json.load(_f)

LAVALINK_URI  = _CONFIG["lavalink"]["uri"]
LAVALINK_PASS = _CONFIG["lavalink"]["password"]

VC_TIMEOUT    = 60.0   # seconds per connect attempt
VC_RETRIES    = 3
IDLE_TIMEOUT  = 180    # 3 minutes — disconnect if idle or empty

# ── Altered-version filter (applied to ALL sources) ────────────────────────────
_ALTERED_RE = re.compile(
    r"\b("
    r"sped[\s\-]*up|speed[\s\-]*up|fast[\s\-]*version"
    r"|slowed|slow[\s\-]*version"
    r"|reverb|reverbed"
    r"|nightcore|daycore|nightcored"
    r"|lofi|lo[\s\-]*fi"
    r"|pitched[\s\-]*up|pitched[\s\-]*down|pitch[\s\-]*shift"
    r"|bass[\s\-]*boost(?:ed)?"
    r"|8d|8[\s\-]*d[\s\-]*audio"
    r"|chopped[\s&+]*screwed"
    r"|underwater[\s\-]*version"
    r")\b",
    re.IGNORECASE,
)

# Preferred lyric/repost channels — tried when YouTube plain search has no clean result
_PREFERRED_CHANNELS = [
    "Dan Music",
    "7clouds",
    "Unique Vibes",
    "Magic Records",
    "Chill Musik",
    "Konbini",
    "Mood Melody",
    "Sing King",
    "Lyrics Translate",
]

# ── Source helpers ─────────────────────────────────────────────────────────────
_SPOTIFY_RE    = re.compile(r"open\.spotify\.com/")
_SOUNDCLOUD_RE = re.compile(r"soundcloud\.com/")
_YT_RE         = re.compile(r"(youtube\.com|youtu\.be)")


def _source_label(hint: str, query: str) -> str:
    if _SPOTIFY_RE.search(query):    return "Spotify"
    if _SOUNDCLOUD_RE.search(query): return "SoundCloud"
    if _YT_RE.search(query):         return "YouTube"
    return {"yt": "YouTube", "sc": "SoundCloud", "sp": "Spotify"}.get(hint, "Spotify")


def _user_wants_altered(query: str) -> bool:
    """True if the user's own query asks for an altered version."""
    return bool(_ALTERED_RE.search(query))


def _is_clean(track: wavelink.Playable) -> bool:
    """True if the track title has no altered-version keywords."""
    return not bool(_ALTERED_RE.search(track.title or ""))


def _is_local(track: wavelink.Playable) -> bool:
    uri = track.uri or ""
    return uri.startswith("spotify:local:") or (
        bool(uri) and not uri.startswith("http") and not _SPOTIFY_RE.search(uri)
    )


# ── Embeds ─────────────────────────────────────────────────────────────────────
COLOUR = discord.Colour.from_str("#5865F2")


def _track_embed(
    track: wavelink.Playable, action: str, source_label: str, queue_pos: int = 0
) -> discord.Embed:
    title  = track.title  or "Unknown Title"
    author = track.author or "Unknown Artist"
    uri    = track.uri    or ""
    desc   = f"**[{title}]({uri})**\n{author}" if uri else f"**{title}**\n{author}"

    if action == "playing":
        embed = discord.Embed(title="▶️  Now Playing", description=desc, colour=COLOUR)
    else:
        embed = discord.Embed(title="➕  Added to Queue", description=desc, colour=discord.Colour.green())
        embed.set_footer(text=f"Position in queue: #{queue_pos}")

    if track.artwork:
        embed.set_thumbnail(url=track.artwork)
    if track.length:
        m, s = divmod(track.length // 1000, 60)
        embed.add_field(name="Duration", value=f"{m}:{s:02d}", inline=True)
    embed.add_field(name="Source", value=source_label, inline=True)
    return embed


def _playlist_embed(
    name: str, count: int, first: wavelink.Playable,
    source_label: str, action: str, skipped: int = 0,
) -> discord.Embed:
    embed = discord.Embed(
        title="▶️  Playing Playlist" if action == "playing" else "➕  Queued Playlist",
        description=f"**{name}**",
        colour=COLOUR if action == "playing" else discord.Colour.green(),
    )
    embed.add_field(name="Tracks", value=str(count), inline=True)
    embed.add_field(name="Source", value=source_label, inline=True)
    if skipped:
        embed.add_field(
            name="⚠️ Skipped",
            value=f"{skipped} local file(s) can't be played",
            inline=False,
        )
    if first.artwork:
        embed.set_thumbnail(url=first.artwork)
    return embed


def _now_playing_embed(track: wavelink.Playable, source_label: str) -> discord.Embed:
    return _track_embed(track, "playing", source_label)


def _local_warning_embed(name: str) -> discord.Embed:
    return discord.Embed(
        title="⚠️  Local File — Skipped",
        description=(
            f"**{name}** is a local file and can't be streamed.\n"
            "Only songs available on YouTube, SoundCloud, or Spotify's catalogue can be played."
        ),
        colour=discord.Colour.yellow(),
    )


# ── Worker ─────────────────────────────────────────────────────────────────────
class Worker:
    def __init__(self, index: int, token: str, status_callback):
        self.index           = index
        self.token           = token
        self.status_callback = status_callback

        self.busy:       bool      = False
        self.guild_id:   int | None = None
        self.channel_id: int | None = None

        self.command_queue: asyncio.Queue                       = asyncio.Queue()
        self._track_queue:  list[tuple[wavelink.Playable, str]] = []
        self._text_channel: discord.TextChannel | None          = None
        self._idle_task:    asyncio.Task | None                  = None

        intents = discord.Intents.default()
        intents.voice_states = True   # needed to detect empty VC
        self.bot = discord.Client(intents=intents)

        self.bot.event(self.on_ready)
        self.bot.event(self.on_wavelink_node_ready)
        self.bot.event(self.on_wavelink_track_end)
        self.bot.event(self.on_wavelink_track_exception)
        self.bot.event(self.on_voice_state_update)

    # ── Idle timeout ───────────────────────────────────────────────────────────

    def _reset_idle_timer(self):
        """Cancel any running idle timer and start a fresh 3-minute one."""
        if self._idle_task and not self._idle_task.done():
            self._idle_task.cancel()
        self._idle_task = asyncio.ensure_future(
            self._idle_countdown(), loop=self.bot.loop
        )

    def _cancel_idle_timer(self):
        if self._idle_task and not self._idle_task.done():
            self._idle_task.cancel()
        self._idle_task = None

    async def _idle_countdown(self):
        """Wait IDLE_TIMEOUT seconds then disconnect if still idle/empty."""
        try:
            await asyncio.sleep(IDLE_TIMEOUT)
        except asyncio.CancelledError:
            return
        # Timer fired — disconnect
        if self.guild_id:
            player = self._get_player(self.guild_id)
            if player:
                print(f"[Worker {self.index}] Idle timeout — disconnecting")
                self._track_queue.clear()
                try:
                    await player.stop()
                    await player.disconnect()
                except Exception:
                    pass
            await self._set_free()

    # ── Events ─────────────────────────────────────────────────────────────────

    async def on_ready(self):
        node = wavelink.Node(uri=LAVALINK_URI, password=LAVALINK_PASS)
        await wavelink.Pool.connect(client=self.bot, nodes=[node])
        asyncio.ensure_future(self._command_loop(), loop=self.bot.loop)
        print(f"[Worker {self.index}] Ready — {self.bot.user}")

    async def on_wavelink_node_ready(self, payload: wavelink.NodeReadyEventPayload):
        print(f"[Worker {self.index}] Lavalink OK: {payload.node.uri}")

    async def on_wavelink_track_end(self, payload: wavelink.TrackEndEventPayload):
        player: wavelink.Player = payload.player
        if self._track_queue:
            next_track, next_label = self._track_queue.pop(0)
            await player.play(next_track)
            self._reset_idle_timer()   # reset — new track started
            if self._text_channel:
                try:
                    await self._text_channel.send(
                        embed=_now_playing_embed(next_track, next_label)
                    )
                except Exception:
                    pass
        else:
            # Queue empty — start idle timer
            self._reset_idle_timer()

    async def on_wavelink_track_exception(self, payload: wavelink.TrackExceptionEventPayload):
        print(f"[Worker {self.index}] Track exception — "
              f"{payload.track.title if payload.track else '?'}: {payload.exception}")
        player: wavelink.Player = payload.player
        if self._track_queue:
            next_track, next_label = self._track_queue.pop(0)
            await player.play(next_track)
            self._reset_idle_timer()
        else:
            self._reset_idle_timer()

    async def on_voice_state_update(
        self,
        member: discord.Member,
        before: discord.VoiceState,
        after: discord.VoiceState,
    ):
        """Start idle timer if everyone leaves the VC (even mid-song)."""
        if not self.guild_id or not self.channel_id:
            return
        if member.bot:
            return   # ignore other bots

        guild = self.bot.get_guild(self.guild_id)
        if not guild:
            return

        vc = guild.get_channel(self.channel_id)
        if not isinstance(vc, discord.VoiceChannel):
            return

        # Count non-bot members still in the channel
        human_members = [m for m in vc.members if not m.bot]
        if len(human_members) == 0:
            print(f"[Worker {self.index}] VC empty — starting idle timer")
            self._reset_idle_timer()
        else:
            # Someone is still there — if timer was running due to empty VC, cancel it
            # (don't cancel if timer is running due to empty queue — let that run)
            pass

    # ── State ──────────────────────────────────────────────────────────────────

    async def _set_busy(self, guild_id: int, channel_id: int):
        self.busy       = True
        self.guild_id   = guild_id
        self.channel_id = channel_id
        await self.status_callback(self.index, channel_id, busy=True)

    async def _set_free(self):
        self._cancel_idle_timer()
        self.busy          = False
        self.guild_id      = None
        self.channel_id    = None
        self._track_queue.clear()
        self._text_channel = None
        await self.status_callback(self.index, None, busy=False)

    def _get_player(self, guild_id: int) -> wavelink.Player | None:
        guild = self.bot.get_guild(guild_id)
        return guild.voice_client if guild else None  # type: ignore

    def _resolve_vc(self, guild_id: int, channel_id: int) -> discord.VoiceChannel | None:
        guild = self.bot.get_guild(guild_id)
        if not guild:
            return None
        ch = guild.get_channel(channel_id)
        return ch if isinstance(ch, discord.VoiceChannel) else None

    def _resolve_text(self, guild_id: int, channel_id: int) -> discord.TextChannel | None:
        guild = self.bot.get_guild(guild_id)
        if not guild:
            return None
        ch = guild.get_channel(channel_id)
        return ch if isinstance(ch, discord.TextChannel) else None

    # ── VC connect ─────────────────────────────────────────────────────────────

    async def _connect(self, guild_id: int, channel_id: int) -> wavelink.Player:
        vc = self._resolve_vc(guild_id, channel_id)
        if vc is None:
            raise RuntimeError(
                f"Cannot find voice channel {channel_id} in guild {guild_id}. "
                "Is this worker bot a member of the server?"
            )
        guild  = vc.guild
        player: wavelink.Player = guild.voice_client  # type: ignore

        if player is not None and player.channel.id == vc.id:
            return player

        if player is not None:
            for attempt in range(1, VC_RETRIES + 1):
                try:
                    await asyncio.wait_for(player.move_to(vc), timeout=VC_TIMEOUT)
                    print(f"[Worker {self.index}] Moved to {vc.name}")
                    return player
                except asyncio.TimeoutError:
                    print(f"[Worker {self.index}] move_to timeout ({attempt}/{VC_RETRIES})")
            raise RuntimeError(f"Could not move to {vc.name} after {VC_RETRIES} attempts.")

        for attempt in range(1, VC_RETRIES + 1):
            try:
                player = await asyncio.wait_for(
                    vc.connect(cls=wavelink.Player, self_deaf=True),
                    timeout=VC_TIMEOUT,
                )
                return player
            except asyncio.TimeoutError:
                print(f"[Worker {self.index}] connect timeout ({attempt}/{VC_RETRIES})")
                try:
                    if guild.voice_client:
                        await guild.voice_client.disconnect(force=True)
                except Exception:
                    pass
                if attempt < VC_RETRIES:
                    await asyncio.sleep(2)

        raise RuntimeError(f"Could not connect to {vc.name} after {VC_RETRIES} attempts.")

    # ── Search ─────────────────────────────────────────────────────────────────

    async def _raw_search(self, query: str, source: str) -> list[wavelink.Playable]:
        try:
            if source == "sc":
                results = await wavelink.Playable.search(
                    query, source=wavelink.TrackSource.SoundCloud
                )
            elif source == "sp":
                results = await wavelink.Playable.search(f"spsearch:{query}")
            else:
                results = await wavelink.Playable.search(
                    query, source=wavelink.TrackSource.YouTubeMusic
                )
            if isinstance(results, wavelink.Playlist):
                return results.tracks[:5]
            return results[:5] if results else []
        except Exception as exc:
            print(f"[Worker {self.index}] Raw search error: {exc}")
            return []

    async def _search(self, query: str, source: str) -> tuple[wavelink.Playable | None, str]:
        """
        Smart single-track search with clean-version filter on ALL sources.
        Filter skipped only when user's query explicitly requests altered version.
        Returns (track, source_label).
        """
        label  = _source_label(source, query)
        is_url = query.startswith(("http://", "https://"))

        # URLs — straight to Lavalink / LavaSrc, no filter (user pasted exact link)
        if is_url:
            try:
                results = await wavelink.Playable.search(query)
                if isinstance(results, wavelink.Playlist):
                    tracks = [t for t in results.tracks if not _is_local(t)]
                    return (tracks[0] if tracks else None), label
                if isinstance(results, list) and results:
                    track = results[0]
                    return (None if _is_local(track) else track), label
            except Exception as exc:
                print(f"[Worker {self.index}] URL search error: {exc}")
            return None, label

        # Plain-text — apply clean filter to all sources
        skip_filter = _user_wants_altered(query)

        if skip_filter:
            # User wants the altered version — just search normally
            candidates = await self._raw_search(query, source)
            return (candidates[0] if candidates else None), label

        # ── Filtered path ──────────────────────────────────────────────────────

        # Step 1: plain search top 5, pick first clean
        candidates = await self._raw_search(query, source)
        for track in candidates:
            if _is_clean(track):
                print(f"[Worker {self.index}] Clean ({source}): {track.title}")
                return track, label

        # Step 2: if YouTube, also try preferred channels
        if source == "yt":
            for channel in _PREFERRED_CHANNELS:
                ch_results = await self._raw_search(f"{query} {channel}", source)
                for track in ch_results:
                    if _is_clean(track):
                        print(f"[Worker {self.index}] Clean via '{channel}': {track.title}")
                        return track, label

        # Step 3: unfiltered fallback — take first result regardless
        if candidates:
            print(f"[Worker {self.index}] No clean match, fallback: {candidates[0].title}")
            return candidates[0], label

        return None, label

    async def _search_playlist(
        self, query: str, source: str
    ) -> tuple[list[wavelink.Playable], str, str, int]:
        """Returns (playable_tracks, playlist_name, source_label, skipped_local_count)."""
        label      = _source_label(source, query)
        is_url     = query.startswith(("http://", "https://"))
        raw_tracks: list[wavelink.Playable] = []
        name       = query

        try:
            if is_url:
                result = await wavelink.Playable.search(query)
            elif source == "sc":
                result = await wavelink.Playable.search(
                    query, source=wavelink.TrackSource.SoundCloud
                )
            elif source == "sp":
                result = await wavelink.Playable.search(f"spsearch:{query}")
            else:
                result = await wavelink.Playable.search(
                    query, source=wavelink.TrackSource.YouTubeMusic
                )

            if isinstance(result, wavelink.Playlist):
                raw_tracks = result.tracks
                name       = result.name
            elif isinstance(result, list) and result:
                raw_tracks = result
        except Exception as exc:
            print(f"[Worker {self.index}] Playlist search error: {exc}")

        good, skipped = [], 0
        for t in raw_tracks:
            if _is_local(t):
                skipped += 1
                print(f"[Worker {self.index}] Skipping local file: {t.title}")
            else:
                good.append(t)

        return good, name, label, skipped

    # ── Command loop ───────────────────────────────────────────────────────────

    async def _command_loop(self):
        while True:
            cmd = await self.command_queue.get()
            try:
                await self._handle(cmd)
            except Exception as exc:
                print(f"[Worker {self.index}] Error in op={cmd.get('op')}: {exc}")
                itx: discord.Interaction | None = cmd.get("interaction")
                if itx:
                    try:
                        await itx.followup.send(
                            embed=discord.Embed(
                                title="⚠️ Error", description=str(exc),
                                colour=discord.Colour.red(),
                            ),
                            ephemeral=True,
                        )
                    except Exception:
                        pass

    async def _handle(self, cmd: dict):
        op         = cmd["op"]
        guild_id   = cmd.get("guild_id")
        channel_id = cmd.get("channel_id")
        text_ch_id = cmd.get("text_channel_id")
        itx: discord.Interaction | None = cmd.get("interaction")

        if text_ch_id and guild_id:
            resolved = self._resolve_text(guild_id, text_ch_id)
            if resolved:
                self._text_channel = resolved

        # ── search_and_play ────────────────────────────────────────────────────
        if op == "search_and_play":
            track, label = await self._search(cmd["query"], cmd["source"])
            if track is None:
                if itx:
                    # Check if it was a local file
                    if _SPOTIFY_RE.search(cmd["query"]):
                        await itx.followup.send(embed=_local_warning_embed(cmd["query"]), ephemeral=True)
                    else:
                        await itx.followup.send(
                            embed=discord.Embed(title="❌ Nothing found.", colour=discord.Colour.red()),
                            ephemeral=True,
                        )
                return

            await self._set_busy(guild_id, channel_id)
            player = await self._connect(guild_id, channel_id)

            if player.playing:
                self._track_queue.append((track, label))
                if itx:
                    await itx.followup.send(
                        embed=_track_embed(track, "queued", label, len(self._track_queue)),
                        ephemeral=True,
                    )
            else:
                await player.play(track)
                self._reset_idle_timer()
                if itx:
                    await itx.followup.send(
                        embed=_track_embed(track, "playing", label),
                        ephemeral=True,
                    )

        # ── queue_track ────────────────────────────────────────────────────────
        elif op == "queue_track":
            track, label = await self._search(cmd["query"], cmd["source"])
            if track is None:
                if itx:
                    await itx.followup.send(
                        embed=discord.Embed(title="❌ Nothing found.", colour=discord.Colour.red()),
                        ephemeral=True,
                    )
                return

            await self._set_busy(guild_id, channel_id)
            player = await self._connect(guild_id, channel_id)

            if player.playing:
                self._track_queue.append((track, label))
                if itx:
                    await itx.followup.send(
                        embed=_track_embed(track, "queued", label, len(self._track_queue)),
                        ephemeral=True,
                    )
            else:
                await player.play(track)
                self._reset_idle_timer()
                if itx:
                    await itx.followup.send(
                        embed=_track_embed(track, "playing", label),
                        ephemeral=True,
                    )

        # ── search_and_playlist ────────────────────────────────────────────────
        elif op == "search_and_playlist":
            tracks, pl_name, label, skipped = await self._search_playlist(
                cmd["query"], cmd["source"]
            )
            if not tracks:
                msg = "❌ No playable tracks found."
                if skipped:
                    msg += f"\n({skipped} local file(s) were skipped.)"
                if itx:
                    await itx.followup.send(
                        embed=discord.Embed(title="❌ Empty playlist", description=msg, colour=discord.Colour.red()),
                        ephemeral=True,
                    )
                return

            await self._set_busy(guild_id, channel_id)
            player = await self._connect(guild_id, channel_id)

            if player.playing:
                for t in tracks:
                    self._track_queue.append((t, label))
                if itx:
                    await itx.followup.send(
                        embed=_playlist_embed(pl_name, len(tracks), tracks[0], label, "queued", skipped),
                        ephemeral=True,
                    )
            else:
                first, *rest = tracks
                await player.play(first)
                self._reset_idle_timer()
                for t in rest:
                    self._track_queue.append((t, label))
                if itx:
                    await itx.followup.send(
                        embed=_playlist_embed(pl_name, len(tracks), first, label, "playing", skipped),
                        ephemeral=True,
                    )

        # ── stop ──────────────────────────────────────────────────────────────
        elif op == "stop":
            player = self._get_player(guild_id)
            if player:
                self._track_queue.clear()
                await player.stop()
                await player.disconnect()
            await self._set_free()
            if itx:
                await itx.followup.send(
                    embed=discord.Embed(
                        title="⏹️  Stopped", description="Disconnected from voice.",
                        colour=discord.Colour.red(),
                    ),
                    ephemeral=True,
                )

        # ── control ops ───────────────────────────────────────────────────────
        elif op == "pause_resume":
            player = self._get_player(guild_id)
            if not player or not player.playing and not player.paused:
                if itx:
                    await itx.followup.send(
                        embed=discord.Embed(title="❌ Nothing playing", colour=discord.Colour.red()),
                        ephemeral=True,
                    )
                return
            if player.paused:
                await player.pause(False)
                label = "▶️  Resumed"
                self._reset_idle_timer()
            else:
                await player.pause(True)
                label = "⏸️  Paused"
                # Start idle timer while paused (counts as idle)
                self._reset_idle_timer()
            if itx:
                await itx.followup.send(
                    embed=discord.Embed(title=label, colour=COLOUR),
                    ephemeral=True,
                )

        elif op == "skip":
            player = self._get_player(guild_id)
            if not player or not player.playing:
                if itx:
                    await itx.followup.send(
                        embed=discord.Embed(title="❌ Nothing playing", colour=discord.Colour.red()),
                        ephemeral=True,
                    )
                return
            await player.stop()   # triggers on_wavelink_track_end → plays next
            if itx:
                await itx.followup.send(
                    embed=discord.Embed(title="⏭️  Skipped", colour=COLOUR),
                    ephemeral=True,
                )

        elif op == "backward":
            player = self._get_player(guild_id)
            if not player or not player.playing:
                if itx:
                    await itx.followup.send(
                        embed=discord.Embed(title="❌ Nothing playing", colour=discord.Colour.red()),
                        ephemeral=True,
                    )
                return
            await player.seek(0)
            if itx:
                await itx.followup.send(
                    embed=discord.Embed(title="⏮️  Restarted track", colour=COLOUR),
                    ephemeral=True,
                )

        elif op == "seek":
            player = self._get_player(guild_id)
            if not player or not player.playing:
                if itx:
                    await itx.followup.send(
                        embed=discord.Embed(title="❌ Nothing playing", colour=discord.Colour.red()),
                        ephemeral=True,
                    )
                return
            delta_ms = cmd.get("delta_ms", 0)
            new_pos  = max(0, player.position + delta_ms)
            if player.current and new_pos > player.current.length:
                new_pos = max(0, player.current.length - 1000)
            await player.seek(int(new_pos))
            sign  = "+" if delta_ms > 0 else ""
            secs  = delta_ms // 1000
            if itx:
                await itx.followup.send(
                    embed=discord.Embed(
                        title=f"{'⏩' if delta_ms > 0 else '⏪'}  {sign}{secs}s",
                        colour=COLOUR,
                    ),
                    ephemeral=True,
                )

    async def start(self):
        await self.bot.start(self.token)
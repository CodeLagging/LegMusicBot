"""
musicbot1.py — Music Worker Bot
=================================
Copy as musicbot1.py, musicbot2.py, musicbot3.py ...

SEARCH STRATEGY (most accurate first):
  1. Default source is Spotify (sp):
       spsearch: → Spotify finds the canonical track → LavaSrc resolves
       via ISRC on YouTube Music → near-perfect accuracy
  2. YouTube (yt): ytmsearch: (YouTube Music, not ytsearch)
       Top 5 results scored + filtered, prefer "official audio/video"
  3. SoundCloud (sc): direct search, no filtering needed
  4. Any URL: passed straight through (Spotify/YT/SC all work)

Filter is applied to ALL plain-text searches.
Filter is SKIPPED only when the user's query explicitly contains
one of the altered-version keywords (they want that specific version).
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

LAVALINK_URI      = _CONFIG["lavalink"]["uri"]
LAVALINK_PASS     = _CONFIG["lavalink"]["password"]
STATUS_CHANNEL_ID = int(_CONFIG.get("status_channel_id", 0))

VC_TIMEOUT   = 60.0
VC_RETRIES   = 3
IDLE_TIMEOUT = 180  # 3 minutes

# ──────────────────────────────────────────────────────────────────────────────
# LARGE KEYWORD FILTER
# Anything matching here is considered an "altered" or "non-original" version.
# Grouped by category for maintainability.
# ──────────────────────────────────────────────────────────────────────────────
_ALTERED_KEYWORDS = [
    # Speed alterations
    "sped up", "spedup", "speed up", "sped-up", "speed-up",
    "fast version", "faster version", "super fast",
    "slowed", "slowed down", "slow version", "slowed version",
    "slowed reverb", "slowed   reverb",

    # Reverb / audio effects
    "reverb", "reverbed", "with reverb", "heavy reverb",
    "8d", "8d audio", "8-d audio", "8d music",
    "bass boost", "bass boosted", "bassboost", "bassboosted",
    "bass enhanced", "treble boost",
    "underwater", "underwater version", "underwater effect",
    "echo version", "echo effect",

    # Pitch alterations
    "pitched up", "pitched down", "pitch up", "pitch down",
    "pitch shift", "pitch shifted", "higher pitch", "lower pitch",
    "chipmunk", "chipmunk version",

    # Nightcore / daycore variants
    "nightcore", "nightcored", "night core",
    "daycore", "day core",
    "lullaby version", "lullaby",

    # Lo-fi / chill edits
    "lofi", "lo-fi", "lo fi", "lofied",
    "chillhop", "chill hop",
    "study version", "sleep version", "rain version",

    # Acoustic / stripped versions (when labelled as such)
    "acoustic", "acoustic version", "acoustic cover", "acoustic mix",
    "unplugged", "stripped version", "stripped",
    "piano version", "piano cover",
    "guitar version", "guitar cover",
    "violin version", "ukulele version",
    "a cappella", "acapella",

    # Remix / edit variants (generic non-official)
    "extended mix", "extended version", "extended edit",
    "club mix", "club edit", "radio edit",
    "vip mix", "vip edit",
    "chopped and screwed", "chopped & screwed", "chopped screwed",
    "slowed chopped", "chopped slowed",
    "flip", "bootleg", "mashup", "mash up", "mash-up",
    "rework",

    # Genre-specific production styles used as altered versions
    "hardstyle", "hard style", "hardstyle remix", "hardstyle edit",
    "hardcore", "hard core",
    "gabber",
    "rawstyle", "raw style",
    "uptempo",
    "frenchcore", "french core",
    "terrorcore",
    "industrial hardstyle",
    "jumpstyle",
    "trance remix", "trance version", "trance edit",
    "psytrance", "psy trance",
    "goa trance",
    "drum and bass remix", "drum & bass remix", "dnb remix",
    "dubstep remix", "dubstep version",
    "trap remix", "trap version", "trap edit",
    "phonk remix", "phonk version", "phonk edit",
    "drift phonk",
    "jersey club remix",
    "amapiano remix",
    "afrobeats remix",
    "reggaeton remix",
    "metal version", "metal cover", "metal remix",
    "rock version", "rock cover", "rock remix",
    "punk version", "punk cover",
    "jazz version", "jazz cover",
    "r&b remix", "rnb remix",
    "country version", "country cover",
    "classical version", "orchestral version",

    # Cover / live / karaoke
    "cover", "covered by", "cover by", "cover version",
    "live version", "live performance", "live session", "live at",
    "karaoke", "karaoke version", "instrumental karaoke",
    "tribute", "tribute to",
    "in the style of",

    # Parody / joke versions
    "parody", "parody version",
    "comedy version",

    # AI / generated
    "ai version", "ai cover", "ai generated",
    "ai vocals",

    # Lyric video label (we prefer official audio over fan lyric videos)
    # NOTE: "official lyric video" is fine, but fan-made lyric videos are not
    # We don't filter "lyrics" alone since that's too broad

    # Instrumental
    "instrumental", "instrumental version", "no vocals",
    "backing track",

    # Sped reverb combos
    "sped up reverb",
    "sped reverb",
]

# Build a single compiled regex from the keyword list
# Word-boundary aware, case-insensitive
_ALTERED_RE = re.compile(
    r"\b(" + "|".join(re.escape(k) for k in _ALTERED_KEYWORDS) + r")\b",
    re.IGNORECASE,
)

# Official audio markers — boost score
_OFFICIAL_RE = re.compile(
    r"\b(official\s*(?:audio|video|music\s*video|lyric\s*video|visualizer)?)\b",
    re.IGNORECASE,
)

# Preferred lyric/repost channels — tried when plain YT search has no clean result
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
    """True if the user's own query explicitly asks for an altered version."""
    return bool(_ALTERED_RE.search(query))


def _is_clean(track: wavelink.Playable) -> bool:
    return not bool(_ALTERED_RE.search(track.title or ""))


def _track_score(track: wavelink.Playable) -> int:
    """Higher = better match. Used to rank candidates."""
    title = track.title or ""
    score = 0
    if _OFFICIAL_RE.search(title):
        score += 10
    if _ALTERED_RE.search(title):
        score -= 100
    return score


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
        embed = discord.Embed(
            title="➕  Added to Queue", description=desc,
            colour=discord.Colour.green(),
        )
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


# ── Worker ─────────────────────────────────────────────────────────────────────
class Worker:
    def __init__(self, index: int, token: str, status_callback):
        self.index           = index
        self.token           = token
        self.status_callback = status_callback

        self.busy:        bool       = False
        self.guild_id:    int | None = None
        self.channel_id:  int | None = None
        self.lavalink_ok: bool       = False

        self.command_queue: asyncio.Queue                       = asyncio.Queue()
        self._track_queue:  list[tuple[wavelink.Playable, str]] = []
        self._text_channel: discord.TextChannel | None          = None
        self._idle_task:    asyncio.Task | None                  = None

        intents = discord.Intents.default()
        intents.voice_states = True
        self.bot = discord.Client(intents=intents)

        self.bot.event(self.on_ready)
        self.bot.event(self.on_wavelink_node_ready)
        self.bot.event(self.on_wavelink_track_end)
        self.bot.event(self.on_wavelink_track_exception)
        self.bot.event(self.on_voice_state_update)

    # ── Lavalink connect with infinite retry ───────────────────────────────────

    async def _connect_lavalink(self):
        attempt  = 0
        max_wait = 30
        while True:
            attempt += 1
            try:
                node = wavelink.Node(uri=LAVALINK_URI, password=LAVALINK_PASS)
                await wavelink.Pool.connect(client=self.bot, nodes=[node])
                return
            except Exception as exc:
                wait = min(5 * attempt, max_wait)
                print(
                    f"[Worker {self.index}] Lavalink connect failed "
                    f"(attempt {attempt}): {exc} — retrying in {wait}s",
                    flush=True,
                )
                await asyncio.sleep(wait)

    # ── Idle timeout ───────────────────────────────────────────────────────────

    def _reset_idle_timer(self):
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
        try:
            await asyncio.sleep(IDLE_TIMEOUT)
        except asyncio.CancelledError:
            return
        if self.guild_id:
            player = self._get_player(self.guild_id)
            if player:
                print(f"[Worker {self.index}] Idle timeout — disconnecting", flush=True)
                self._track_queue.clear()
                try:
                    await player.stop()
                    await player.disconnect()
                except Exception:
                    pass
            await self._set_free()

    # ── Events ─────────────────────────────────────────────────────────────────

    async def on_ready(self):
        asyncio.ensure_future(self._connect_lavalink(), loop=self.bot.loop)
        asyncio.ensure_future(self._command_loop(),     loop=self.bot.loop)
        print(f"[Worker {self.index}] Discord ready — {self.bot.user}", flush=True)

    async def on_wavelink_node_ready(self, payload: wavelink.NodeReadyEventPayload):
        self.lavalink_ok = True
        print(f"[Worker {self.index}] Lavalink OK: {payload.node.uri}", flush=True)
        if STATUS_CHANNEL_ID:
            try:
                ch = self.bot.get_channel(STATUS_CHANNEL_ID) or \
                     await self.bot.fetch_channel(STATUS_CHANNEL_ID)
                await ch.send(
                    embed=discord.Embed(
                        title="✅  Worker Bot Connected",
                        description=(
                            f"**Worker {self.index}** (`{self.bot.user}`) is ready.\n"
                            f"Lavalink: `{payload.node.uri}`"
                        ),
                        colour=discord.Colour.green(),
                    ).set_footer(
                        text=f"Connected at {discord.utils.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
                    )
                )
            except Exception as exc:
                print(f"[Worker {self.index}] Status post failed: {exc}", flush=True)

    async def on_wavelink_track_end(self, payload: wavelink.TrackEndEventPayload):
        player: wavelink.Player = payload.player
        if self._track_queue:
            next_track, next_label = self._track_queue.pop(0)
            await player.play(next_track)
            self._reset_idle_timer()
            if self._text_channel:
                try:
                    await self._text_channel.send(
                        embed=_now_playing_embed(next_track, next_label)
                    )
                except Exception:
                    pass
        else:
            self._reset_idle_timer()

    async def on_wavelink_track_exception(self, payload: wavelink.TrackExceptionEventPayload):
        print(
            f"[Worker {self.index}] Track exception — "
            f"{payload.track.title if payload.track else '?'}: {payload.exception}",
            flush=True,
        )
        player: wavelink.Player = payload.player
        if self._track_queue:
            next_track, next_label = self._track_queue.pop(0)
            await player.play(next_track)
            self._reset_idle_timer()
        else:
            self._reset_idle_timer()

    async def on_voice_state_update(
        self, member: discord.Member,
        before: discord.VoiceState, after: discord.VoiceState,
    ):
        if not self.guild_id or not self.channel_id or member.bot:
            return
        guild = self.bot.get_guild(self.guild_id)
        if not guild:
            return
        vc = guild.get_channel(self.channel_id)
        if not isinstance(vc, discord.VoiceChannel):
            return
        if not any(not m.bot for m in vc.members):
            print(f"[Worker {self.index}] VC empty — starting idle timer", flush=True)
            self._reset_idle_timer()

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
                    print(f"[Worker {self.index}] Moved to {vc.name}", flush=True)
                    return player
                except asyncio.TimeoutError:
                    print(
                        f"[Worker {self.index}] move_to timeout ({attempt}/{VC_RETRIES})",
                        flush=True,
                    )
            raise RuntimeError(f"Could not move to {vc.name} after {VC_RETRIES} attempts.")

        for attempt in range(1, VC_RETRIES + 1):
            try:
                player = await asyncio.wait_for(
                    vc.connect(cls=wavelink.Player, self_deaf=True),
                    timeout=VC_TIMEOUT,
                )
                return player
            except asyncio.TimeoutError:
                print(
                    f"[Worker {self.index}] connect timeout ({attempt}/{VC_RETRIES})",
                    flush=True,
                )
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
        """
        Return up to 5 raw candidates.
        Always uses ytmsearch (YouTube Music) for yt — far better accuracy than ytsearch.
        Spotify uses spsearch — LavaSrc resolves via ISRC on YouTube Music automatically.
        """
        try:
            if source == "sc":
                results = await wavelink.Playable.search(
                    query, source=wavelink.TrackSource.SoundCloud
                )
            elif source == "sp":
                # spsearch: → Spotify metadata → ISRC lookup on YouTube Music
                # This is the most accurate path for any song name search
                results = await wavelink.Playable.search(f"spsearch:{query}")
            else:
                # ytmsearch: — YouTube Music curated catalogue, much more accurate
                # than ytsearch: which returns all video types
                results = await wavelink.Playable.search(
                    query, source=wavelink.TrackSource.YouTubeMusic
                )
            if isinstance(results, wavelink.Playlist):
                return results.tracks[:5]
            return results[:5] if results else []
        except Exception as exc:
            print(f"[Worker {self.index}] Raw search error ({source}): {exc}", flush=True)
            return []

    async def _search(self, query: str, source: str) -> tuple[wavelink.Playable | None, str]:
        """
        Smart search:
          - URLs: passed straight through (Spotify/YT/SC)
          - Spotify (sp): spsearch → ISRC resolution → near-perfect accuracy
            Still filtered in case Spotify returns a remix/edit variant
          - YouTube (yt): ytmsearch top 5 scored + filtered, preferred channels fallback
          - SoundCloud (sc): direct, no extra filtering
          - User explicitly wants altered version: skip filter, return best scored result
        """
        label  = _source_label(source, query)
        is_url = query.startswith(("http://", "https://"))

        # URLs — straight to Lavalink / LavaSrc, no filter
        if is_url:
            try:
                results = await wavelink.Playable.search(query)
                if isinstance(results, wavelink.Playlist):
                    tracks = [t for t in results.tracks if not _is_local(t)]
                    return (tracks[0] if tracks else None), label
                if isinstance(results, list) and results:
                    t = results[0]
                    return (None if _is_local(t) else t), label
            except Exception as exc:
                print(f"[Worker {self.index}] URL search error: {exc}", flush=True)
            return None, label

        # User explicitly wants an altered version — respect that
        if _user_wants_altered(query):
            candidates = await self._raw_search(query, source)
            if candidates:
                return max(candidates, key=_track_score), label
            return None, label

        # ── Filtered + scored search ───────────────────────────────────────────

        # Step 1: plain search, score all, pick best clean result
        candidates = await self._raw_search(query, source)
        clean = [t for t in candidates if _is_clean(t)]
        if clean:
            best = max(clean, key=_track_score)
            print(
                f"[Worker {self.index}] ✓ Clean ({source}): "
                f"{best.title!r} score={_track_score(best)}",
                flush=True,
            )
            return best, label

        # Step 2: YouTube / Spotify — try preferred lyric channels as fallback
        if source in ("yt", "sp"):
            for channel in _PREFERRED_CHANNELS:
                ch_results = await self._raw_search(f"{query} {channel}", "yt")
                ch_clean = [t for t in ch_results if _is_clean(t)]
                if ch_clean:
                    best = max(ch_clean, key=_track_score)
                    print(
                        f"[Worker {self.index}] ✓ Clean via '{channel}': {best.title!r}",
                        flush=True,
                    )
                    return best, label

        # Step 3: unfiltered fallback — best-scored result regardless
        if candidates:
            best = max(candidates, key=_track_score)
            print(
                f"[Worker {self.index}] ⚠ No clean match, best fallback: {best.title!r}",
                flush=True,
            )
            return best, label

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
            print(f"[Worker {self.index}] Playlist search error: {exc}", flush=True)

        good, skipped = [], 0
        for t in raw_tracks:
            if _is_local(t):
                skipped += 1
                print(f"[Worker {self.index}] Skipping local file: {t.title}", flush=True)
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
                print(
                    f"[Worker {self.index}] Error in op={cmd.get('op')}: {exc}",
                    flush=True,
                )
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

        async def _err(msg: str):
            if itx:
                await itx.followup.send(
                    embed=discord.Embed(title=msg, colour=discord.Colour.red()),
                    ephemeral=True,
                )

        # ── search_and_play / queue_track ──────────────────────────────────────
        if op in ("search_and_play", "queue_track"):
            track, label = await self._search(cmd["query"], cmd["source"])
            if track is None:
                await _err("❌ Nothing found.")
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
                    msg += f"\n({skipped} local file(s) skipped.)"
                await _err(msg)
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

        # ── pause_resume ───────────────────────────────────────────────────────
        elif op == "pause_resume":
            player = self._get_player(guild_id)
            if not player or (not player.playing and not player.paused):
                await _err("❌ Nothing playing")
                return
            if player.paused:
                await player.pause(False)
                title = "▶️  Resumed"
            else:
                await player.pause(True)
                title = "⏸️  Paused"
            self._reset_idle_timer()
            if itx:
                await itx.followup.send(
                    embed=discord.Embed(title=title, colour=COLOUR), ephemeral=True
                )

        # ── skip ──────────────────────────────────────────────────────────────
        elif op == "skip":
            player = self._get_player(guild_id)
            if not player or not player.playing:
                await _err("❌ Nothing playing")
                return
            await player.stop()
            if itx:
                await itx.followup.send(
                    embed=discord.Embed(title="⏭️  Skipped", colour=COLOUR), ephemeral=True
                )

        # ── backward ──────────────────────────────────────────────────────────
        elif op == "backward":
            player = self._get_player(guild_id)
            if not player or not player.playing:
                await _err("❌ Nothing playing")
                return
            await player.seek(0)
            if itx:
                await itx.followup.send(
                    embed=discord.Embed(title="⏮️  Restarted track", colour=COLOUR),
                    ephemeral=True,
                )

        # ── seek ──────────────────────────────────────────────────────────────
        elif op == "seek":
            player = self._get_player(guild_id)
            if not player or not player.playing:
                await _err("❌ Nothing playing")
                return
            delta_ms = cmd.get("delta_ms", 0)
            new_pos  = max(0, player.position + delta_ms)
            if player.current and new_pos > player.current.length:
                new_pos = max(0, player.current.length - 1000)
            await player.seek(int(new_pos))
            sign = "+" if delta_ms > 0 else ""
            secs = delta_ms // 1000
            icon = "⏩" if delta_ms > 0 else "⏪"
            if itx:
                await itx.followup.send(
                    embed=discord.Embed(title=f"{icon}  {sign}{secs}s", colour=COLOUR),
                    ephemeral=True,
                )

        # ── ping (health check) ────────────────────────────────────────────────
        elif op == "ping":
            result_queue: asyncio.Queue = cmd["result_queue"]
            await result_queue.put({
                "worker_index": self.index,
                "discord_ok":   self.bot.is_ready(),
                "discord_ms":   round(self.bot.latency * 1000),
                "lavalink_ok":  self.lavalink_ok,
                "lavalink_uri": LAVALINK_URI,
                "busy":         self.busy,
                "queue_len":    len(self._track_queue),
            })

    async def start(self):
        await self.bot.start(self.token)
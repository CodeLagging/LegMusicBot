import asyncio
import json
import os
import re
import sys
import time
import logging
logging.basicConfig(level=logging.INFO)
logging.getLogger("wavelink").setLevel(logging.INFO)
from pathlib import Path


_stem  = Path(sys.argv[0]).stem
_match = re.search(r"(\d+)$", _stem)
BOT_INDEX = int(_match.group(1)) if _match else 1

SOCKET_PATH = Path(__file__).parent / f".worker{BOT_INDEX}.sock"


from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")

import discord
import wavelink
from groq import AsyncGroq

_cfg = Path(__file__).parent / "config.json"
with open(_cfg) as _f:
    _CONFIG = json.load(_f)

TOKEN             = _CONFIG["tokens"][BOT_INDEX]
LAVALINK_URI      = _CONFIG["lavalink"]["uri"]
LAVALINK_PASS     = _CONFIG["lavalink"]["password"]
STATUS_CHANNEL_ID = int(_CONFIG.get("status_channel_id", 0))

VC_TIMEOUT   = 60.0
VC_RETRIES   = 3
IDLE_TIMEOUT = 180


YTDLP_TIMEOUT        = 20
URL_CACHE_TTL        = 3600
MAX_RESOLVE_ATTEMPTS = 2
MAX_FAIL_STREAK      = 3

_groq         = AsyncGroq(api_key=os.environ["GROQ_API_KEY"])
_GROQ_MODEL   = "openai/gpt-oss-20b"
_GROQ_TIMEOUT = 4.0


_OFFICIAL_RE = re.compile(
    r"\b(official\s*(?:audio|video|music\s*video|lyric\s*video|visualizer)?)\b",
    re.IGNORECASE,
)

_PREFERRED_CHANNELS = [
    "Dan Music", "7clouds", "Unique Vibes", "Magic Records",
    "Chill Musik", "Konbini", "Mood Melody", "Sing King", "Lyrics Translate",
]

_SPOTIFY_RE    = re.compile(r"open\.spotify\.com/")
_SOUNDCLOUD_RE = re.compile(r"soundcloud\.com/")
_YT_RE         = re.compile(r"(youtube\.com|youtu\.be)")
_AM_RE         = re.compile(r"music\.apple\.com/")
_SEARCH_PREFIX_RE = re.compile(r"^(?:ytmsearch|ytsearch|scsearch|spsearch):\s*", re.IGNORECASE)


_ALTERED_SYSTEM = """You are a music metadata classifier.
Decide if a song title refers to an ALTERED or NON-ORIGINAL version of a song.

ALTERED: sped up, slowed, reverb, nightcore, daycore, lofi, bass boost, 8D audio,
hardstyle, trance, dubstep, phonk, trap remix, cover, acoustic version, piano version,
live version, karaoke, instrumental, AI cover, mashup, bootleg, extended mix, etc.

NOT altered: original studio recording, official remix by original artist,
official lyric/audio video, remastered version, radio edit.

Respond with EXACTLY one word: yes or no."""

async def _is_altered(title: str) -> bool:
    try:
        r = await asyncio.wait_for(
            _groq.chat.completions.create(
                model=_GROQ_MODEL,
                messages=[
                    {"role": "system", "content": _ALTERED_SYSTEM},
                    {"role": "user",   "content": f'Song title: "{title}"'},
                ],
                max_tokens=3, temperature=0,
            ),
            timeout=_GROQ_TIMEOUT,
        )
        return r.choices[0].message.content.strip().lower().startswith("yes")
    except Exception as exc:
        print(f"[Worker {BOT_INDEX}] Groq error ({title!r}): {exc} — assuming clean", flush=True)
        return False

async def _user_wants_altered(query: str) -> bool:
    try:
        r = await asyncio.wait_for(
            _groq.chat.completions.create(
                model=_GROQ_MODEL,
                messages=[
                    {"role": "system", "content":
                        "Decide if the user's search query explicitly requests an altered, "
                        "remixed, or non-original version (sped up, slowed, reverb, nightcore, "
                        "cover, remix, lofi, etc.). If it's just a song name or artist, answer no.\n"
                        "Respond with EXACTLY one word: yes or no."},
                    {"role": "user", "content": f'Search query: "{query}"'},
                ],
                max_tokens=3, temperature=0,
            ),
            timeout=_GROQ_TIMEOUT,
        )
        return r.choices[0].message.content.strip().lower().startswith("yes")
    except Exception as exc:
        print(f"[Worker {BOT_INDEX}] Groq query-check error: {exc} — assuming not altered", flush=True)
        return False


def _source_label(hint: str, query: str) -> str:
    if _SPOTIFY_RE.search(query):    return "Spotify"
    if _SOUNDCLOUD_RE.search(query): return "SoundCloud"
    if _YT_RE.search(query):         return "YouTube"
    if _AM_RE.search(query):         return "Apple Music"
    return {"yt": "YouTube", "sc": "SoundCloud", "sp": "Spotify"}.get(hint, "Spotify")

def _is_local(track: wavelink.Playable) -> bool:
    uri = track.uri or ""
    return uri.startswith("spotify:local:") or (
        bool(uri) and not uri.startswith("http") and not _SPOTIFY_RE.search(uri)
    )

def _track_score(track: wavelink.Playable, query: str = "",
                 ref: wavelink.Playable | None = None) -> int:
    score   = 0
    title   = (track.title  or "").lower()
    author  = (track.author or "").lower()
    q_lower = query.lower()
    words   = [w for w in re.split(r"\W+", q_lower) if len(w) > 1]

    for w in words:
        if w in title:  score += 40
        if w in author: score += 20


    if ref is not None:
        ref_words = [w for w in re.split(r"\W+", f"{ref.title or ''} {ref.author or ''}".lower()) if len(w) > 1]
        for w in set(ref_words):
            if w in title:  score += 15
            elif w in author: score += 5
        if ref.length and track.length:
            diff = abs(track.length - ref.length)
            if   diff <= 3000:  score += 60
            elif diff <= 8000:  score += 25
            elif diff > 20000:  score -= 40

    if _OFFICIAL_RE.search(track.title or ""):
        score += 10


    _ALTERED_QUICK = re.compile(
        r"\b(sped[\s_-]*up|slowed|reverb|nightcore|daycore|lofi|lo[\s_-]*fi|"
        r"bass[\s_-]*boost|8d|cover|acoustic|piano|karaoke|instrumental|remix|"
        r"mashup|bootleg|extended|phonk|trap)\b",
        re.IGNORECASE,
    )
    if _ALTERED_QUICK.search(track.title or ""):
        score -= 20

    return score


COLOUR = discord.Colour.from_str("#5865F2")

def _track_embed(track, action, source_label, queue_pos=0, search_path=""):
    title  = track.title  or "Unknown Title"
    author = track.author or "Unknown Artist"
    uri    = track.uri    or ""
    desc   = f"**[{title}]({uri})**\n{author}" if uri else f"**{title}**\n{author}"
    embed  = discord.Embed(
        title="▶️  Now Playing" if action == "playing" else "➕  Added to Queue",
        description=desc,
        colour=COLOUR if action == "playing" else discord.Colour.green(),
    )
    if action == "queued":
        embed.set_footer(text=f"Position in queue: #{queue_pos}")
    if track.artwork:
        embed.set_thumbnail(url=track.artwork)
    if track.length:
        m, s = divmod(track.length // 1000, 60)
        embed.add_field(name="Duration", value=f"{m}:{s:02d}", inline=True)
    embed.add_field(name="Source", value=source_label, inline=True)
    if search_path:
        embed.add_field(name="Found via", value=search_path, inline=True)
    return embed

def _playlist_embed(name, count, first, source_label, action, skipped=0):
    embed = discord.Embed(
        title="▶️  Playing Playlist" if action == "playing" else "➕  Queued Playlist",
        description=f"**{name}**",
        colour=COLOUR if action == "playing" else discord.Colour.green(),
    )
    embed.add_field(name="Tracks", value=str(count), inline=True)
    embed.add_field(name="Source", value=source_label, inline=True)
    if skipped:
        embed.add_field(name="⚠️ Skipped", value=f"{skipped} local file(s)", inline=False)
    if first.artwork:
        embed.set_thumbnail(url=first.artwork)
    return embed

def _now_playing_embed(track, source_label, search_path=""):
    return _track_embed(track, "playing", source_label, search_path=search_path)


_STATUS_DELETE_DELAY = 10

async def _send_status(channel: discord.TextChannel, embed: discord.Embed):
    try:
        msg = await channel.send(embed=embed)
        asyncio.ensure_future(_delete_after(msg, _STATUS_DELETE_DELAY))
    except Exception:
        pass

async def _delete_after(msg: discord.Message, delay: float):
    await asyncio.sleep(delay)
    try:
        await msg.delete()
    except Exception:
        pass


_status_msg_id: int | None = None

def _status_msg_file() -> Path:
    return Path(__file__).parent / f".worker{BOT_INDEX}_status_msg"

async def _get_status_channel(bot):
    if not STATUS_CHANNEL_ID:
        return None
    try:
        ch = bot.get_channel(STATUS_CHANNEL_ID)
        return ch or await bot.fetch_channel(STATUS_CHANNEL_ID)
    except Exception:
        return None

async def _post_status(bot, state: str):
    global _status_msg_id
    ch = await _get_status_channel(bot)
    if not ch:
        return
    colours = {"connected": discord.Colour.green(), "lavalink_down": discord.Colour.orange(), "shutdown": discord.Colour.red()}
    icons   = {"connected": "✅", "lavalink_down": "⚠️", "shutdown": "🔴"}
    user_str = str(bot.user) if bot.user else f"Worker {BOT_INDEX}"
    descs = {
        "connected":     f"**Worker {BOT_INDEX}** (`{user_str}`) is online.\nLavalink: `{LAVALINK_URI}`",
        "lavalink_down": f"**Worker {BOT_INDEX}** (`{user_str}`) lost Lavalink connection. Reconnecting...",
        "shutdown":      f"**Worker {BOT_INDEX}** (`{user_str}`) has shut down.",
    }
    embed = discord.Embed(
        title=f"{icons[state]}  Worker {BOT_INDEX} — {state.replace('_', ' ').title()}",
        description=descs[state], colour=colours[state],
    ).set_footer(text=f"{discord.utils.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    try:
        f = _status_msg_file()
        msg = None
        if f.exists():
            try:
                msg = await ch.fetch_message(int(f.read_text().strip()))
            except Exception:
                f.unlink(missing_ok=True)
        if msg:
            await msg.edit(embed=embed)
        else:
            msg = await ch.send(embed=embed)
            _status_msg_id = msg.id
            f.write_text(str(msg.id))

        if state != "shutdown":
            asyncio.ensure_future(_delete_after(msg, _STATUS_DELETE_DELAY))
    except Exception as exc:
        print(f"[Worker {BOT_INDEX}] Status post failed: {exc}", flush=True)


intents = discord.Intents.default()
intents.voice_states = True
bot = discord.Client(intents=intents)


_track_queue: list[tuple[wavelink.Playable, str, str]] = []
_track_fallbacks: dict[str, list[tuple[wavelink.Playable, str, str]]] = {}
_text_channel: discord.TextChannel | None = None
_guild_id:  int | None = None
_channel_id: int | None = None
_lavalink_ok = False
_idle_task: asyncio.Task | None = None
_busy = False
_skipping = False
_now_playing_msgs: list[discord.Message] = []
_muted = False
_loop  = False
_fail_streak = 0
_current_info: dict | None = None
_origin: dict[str, wavelink.Playable] = {}

def _track_key(track: wavelink.Playable) -> str:
    return track.identifier or track.uri or f"{track.title}:{track.author}"

def _remember_alternates(
    track: wavelink.Playable,
    candidates: list[wavelink.Playable],
    source_label: str,
    search_path: str,
) -> None:
    key = _track_key(track)
    seen = {key}
    alternates = []
    for candidate in candidates:
        candidate_key = _track_key(candidate)
        if candidate_key not in seen:
            seen.add(candidate_key)
            alternates.append((candidate, source_label, search_path))
    if alternates:
        _track_fallbacks[key] = alternates
    else:
        _track_fallbacks.pop(key, None)


_url_cache: dict[str, tuple[float, str]] = {}
_inflight: dict[str, asyncio.Future] = {}

async def _ytdlp_run(video_url: str) -> str | None:
    try:
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "yt_dlp",
            "-g", "-f", "bestaudio/best",
            "--no-playlist", "--no-warnings", "--socket-timeout", "10",
            video_url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except Exception as exc:
        print(f"[Worker {BOT_INDEX}] yt-dlp could not start: {exc}", flush=True)
        return None
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=YTDLP_TIMEOUT)
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        print(f"[Worker {BOT_INDEX}] yt-dlp timed out for {video_url}", flush=True)
        return None
    if proc.returncode != 0:
        msg = err.decode(errors="replace").strip().splitlines()
        print(f"[Worker {BOT_INDEX}] yt-dlp failed: {msg[-1] if msg else 'unknown error'}", flush=True)
        return None
    for line in out.decode(errors="replace").splitlines():
        line = line.strip()
        if line.startswith("http"):
            return line
    return None

async def _ytdlp_url(video_url: str) -> str | None:
    hit = _url_cache.get(video_url)
    if hit and time.time() - hit[0] < URL_CACHE_TTL:
        return hit[1]
    fut = _inflight.get(video_url)
    if fut is None:
        fut = asyncio.ensure_future(_ytdlp_run(video_url))
        _inflight[video_url] = fut
    try:
        url = await fut
    finally:
        if _inflight.get(video_url) is fut:
            _inflight.pop(video_url, None)
    if url:
        _url_cache[video_url] = (time.time(), url)
        if len(_url_cache) > 200:
            for k, _ in sorted(_url_cache.items(), key=lambda kv: kv[1][0])[:50]:
                _url_cache.pop(k, None)
    return url

async def _resolve(track: wavelink.Playable) -> tuple[wavelink.Playable, wavelink.Playable] | None:
    src = track
    uri = track.uri or ""

    if _SPOTIFY_RE.search(uri) or _AM_RE.search(uri):
        q = f"{track.title} {track.author}".strip()
        cands = await _ytm(q)
        if not cands:
            return None
        src = max(cands, key=lambda t: _track_score(t, q, track))
        uri = src.uri or ""

    if not _YT_RE.search(uri):
        return src, track

    direct = await _ytdlp_url(uri)
    if not direct:
        return None
    try:
        res = await wavelink.Pool.fetch_tracks(direct)
        tracks = res.tracks if isinstance(res, wavelink.Playlist) else list(res)
    except Exception as exc:
        print(f"[Worker {BOT_INDEX}] Lavalink could not load resolved URL: {exc}", flush=True)
        return None
    if not tracks:
        return None
    return tracks[0], track

def _prefetch_next() -> None:
    if not _track_queue:
        return
    uri = _track_queue[0][0].uri or ""
    if _YT_RE.search(uri):
        asyncio.ensure_future(_ytdlp_url(uri))

async def _play(player: wavelink.Player, track: wavelink.Playable) -> bool:
    global _current_info
    alternates = [t for t, _, _ in _track_fallbacks.get(_track_key(track), [])]
    candidates = [track] + alternates

    for i, cand in enumerate(candidates[:MAX_RESOLVE_ATTEMPTS]):
        resolved = await _resolve(cand)
        if resolved is None:
            print(f"[Worker {BOT_INDEX}] Could not resolve {cand.title!r} — trying next candidate", flush=True)
            continue
        playable, origin = resolved
        _origin[_track_key(playable)] = origin
        _current_info = {"title": origin.title or "Unknown", "author": origin.author or ""}
        if i > 0:

            rest = _track_fallbacks.get(_track_key(track), [])[i:]
            if rest:
                _track_fallbacks[_track_key(origin)] = rest
        _cancel_idle_timer()
        await player.play(playable)
        _prefetch_next()
        return True


    print(f"[Worker {BOT_INDEX}] Resolve failed for {track.title!r} — passing to Lavalink as-is", flush=True)
    _origin[_track_key(track)] = track
    _current_info = {"title": track.title or "Unknown", "author": track.author or ""}
    _cancel_idle_timer()
    await player.play(track)
    return False


def _start_idle_timer():
    global _idle_task
    if _idle_task and not _idle_task.done():
        return
    _idle_task = asyncio.ensure_future(_idle_countdown())

def _cancel_idle_timer():
    global _idle_task
    if _idle_task and not _idle_task.done():
        _idle_task.cancel()
    _idle_task = None

async def _idle_countdown():
    try:
        await asyncio.sleep(IDLE_TIMEOUT)
    except asyncio.CancelledError:
        return
    if _guild_id:
        player = _get_player(_guild_id)
        if player and (player.playing or player.paused):
            print(f"[Worker {BOT_INDEX}] Idle fired but still playing — skipping", flush=True)
            return
        if player:
            print(f"[Worker {BOT_INDEX}] Idle timeout — disconnecting", flush=True)
            _track_queue.clear()
            try:
                await player.stop()
                await player.disconnect()
            except Exception:
                pass
    await _set_free()

async def _do_shutdown():
    await asyncio.sleep(0.3)
    await _shutdown()

    asyncio.get_event_loop().stop()

def _get_player(guild_id: int) -> wavelink.Player | None:
    guild = bot.get_guild(guild_id)
    return guild.voice_client if guild else None

async def _set_free():
    global _busy, _guild_id, _channel_id, _text_channel, _current_info, _fail_streak
    _cancel_idle_timer()
    _busy       = False
    _guild_id   = None
    _channel_id = None
    _track_queue.clear()
    _track_fallbacks.clear()
    _origin.clear()
    _current_info = None
    _fail_streak  = 0
    _text_channel = None

async def _set_busy(guild_id: int, channel_id: int):
    global _busy, _guild_id, _channel_id
    _cancel_idle_timer()
    _busy       = True
    _guild_id   = guild_id
    _channel_id = channel_id


@bot.event
async def on_ready():
    asyncio.ensure_future(_connect_lavalink())
    asyncio.ensure_future(_ipc_server())
    print(f"[Worker {BOT_INDEX}] Discord ready — {bot.user}", flush=True)

@bot.event
async def on_wavelink_node_ready(payload: wavelink.NodeReadyEventPayload):
    global _lavalink_ok
    _lavalink_ok = True
    print(f"[Worker {BOT_INDEX}] Lavalink OK: {payload.node.uri}", flush=True)
    await _post_status(bot, "connected")

@bot.event
async def on_wavelink_node_disconnected(payload: wavelink.NodeDisconnectedEventPayload):
    global _lavalink_ok
    _lavalink_ok = False
    print(f"[Worker {BOT_INDEX}] Lavalink disconnected", flush=True)
    await _post_status(bot, "lavalink_down")

@bot.event
async def on_wavelink_track_end(payload: wavelink.TrackEndEventPayload):
    global _skipping, _fail_streak
    reason = str(getattr(payload, "reason", "") or "").lower()
    ended  = payload.track
    orig   = _origin.pop(_track_key(ended), ended) if ended else None

    if reason == "finished":
        _fail_streak = 0


    if reason in ("loadfailed", "replaced", "stopped", "cleanup"):
        return

    if _skipping:
        return

    player: wavelink.Player = payload.player
    if orig:
        _track_fallbacks.pop(_track_key(orig), None)


    if _loop and orig:
        _track_queue.insert(0, (orig, "Looping", ""))

    if _track_queue:
        nxt, nxt_label, nxt_path = _track_queue.pop(0)
        await _play(player, nxt)
        if _text_channel and not _muted:
            await _send_status(_text_channel, _now_playing_embed(nxt, nxt_label, nxt_path))
    else:
        _start_idle_timer()

@bot.event
async def on_wavelink_track_exception(payload: wavelink.TrackExceptionEventPayload):
    global _skipping, _fail_streak
    _skipping = False
    player: wavelink.Player = payload.player
    failed_track = payload.track

    origin = _origin.get(_track_key(failed_track), failed_track) if failed_track else None

    try:
        first_line = str((payload.exception or {}).get("message", "")).strip().splitlines()[0]
    except Exception:
        first_line = "unknown error"
    print(f"[Worker {BOT_INDEX}] Track exception: {first_line}", flush=True)

    _fail_streak += 1
    if _fail_streak >= MAX_FAIL_STREAK:
        print(f"[Worker {BOT_INDEX}] {_fail_streak} tracks failed in a row — stopping", flush=True)
        if _text_channel:
            await _send_status(_text_channel, discord.Embed(
                title="❌  Playback failed",
                description=f"{_fail_streak} tracks in a row could not be played, so I stopped.\n"
                            f"Last error: `{first_line[:200]}`",
                colour=discord.Colour.red(),
            ))
        _track_queue.clear()
        _fail_streak = 0
        try:
            await player.stop()
        except Exception:
            pass
        _start_idle_timer()
        return

    alternatives = _track_fallbacks.pop(_track_key(origin), []) if origin else []
    if alternatives:
        nxt, nxt_label, nxt_path = alternatives.pop(0)
        if alternatives:
            _track_fallbacks[_track_key(nxt)] = alternatives
        print(f"[Worker {BOT_INDEX}] Trying alternate track: {nxt.title!r}", flush=True)
        await _play(player, nxt)
        return
    if _track_queue:
        nxt, nxt_label, nxt_path = _track_queue.pop(0)
        await _play(player, nxt)
    else:
        _start_idle_timer()

@bot.event
async def on_voice_state_update(member, before, after):
    if not _guild_id or not _channel_id or member.bot:
        return
    guild = bot.get_guild(_guild_id)
    if not guild:
        return
    vc = guild.get_channel(_channel_id)
    if isinstance(vc, discord.VoiceChannel) and not any(not m.bot for m in vc.members):
        print(f"[Worker {BOT_INDEX}] VC empty — starting idle timer", flush=True)
        _start_idle_timer()


async def _connect_lavalink():
    attempt = 0
    while True:
        attempt += 1
        try:
            node = wavelink.Node(uri=LAVALINK_URI, password=LAVALINK_PASS)
            await wavelink.Pool.connect(client=bot, nodes=[node])
            return
        except Exception as exc:
            wait = min(5 * attempt, 30)
            print(f"[Worker {BOT_INDEX}] Lavalink failed (attempt {attempt}): {exc} — retry in {wait}s", flush=True)
            await asyncio.sleep(wait)


async def _connect_vc(guild_id: int, channel_id: int) -> wavelink.Player:
    guild = bot.get_guild(guild_id)
    if not guild:
        raise RuntimeError(f"Guild {guild_id} not found — is this worker in the server?")
    vc = guild.get_channel(channel_id)
    if not isinstance(vc, discord.VoiceChannel):
        raise RuntimeError(f"Channel {channel_id} not found or not a voice channel")
    player: wavelink.Player = guild.voice_client
    if player and player.channel.id == vc.id:
        return player
    if player:
        for i in range(1, VC_RETRIES + 1):
            try:
                await asyncio.wait_for(player.move_to(vc), timeout=VC_TIMEOUT)
                return player
            except asyncio.TimeoutError:
                print(f"[Worker {BOT_INDEX}] move_to timeout ({i}/{VC_RETRIES})", flush=True)
        raise RuntimeError("Could not move to VC")
    for i in range(1, VC_RETRIES + 1):
        try:
            return await asyncio.wait_for(vc.connect(cls=wavelink.Player, self_deaf=True), timeout=VC_TIMEOUT)
        except asyncio.TimeoutError:
            print(f"[Worker {BOT_INDEX}] connect timeout ({i}/{VC_RETRIES})", flush=True)
            if guild.voice_client:
                await guild.voice_client.disconnect(force=True)
            if i < VC_RETRIES:
                await asyncio.sleep(2)
    raise RuntimeError("Could not connect to VC")


async def _sp_lookup(query: str) -> wavelink.Playable | None:
    try:
        r = await wavelink.Playable.search(f"spsearch:{query}")
        if isinstance(r, list) and r:              return r[0]
        if isinstance(r, wavelink.Playlist) and r.tracks: return r.tracks[0]
    except Exception as exc:
        print(f"[Worker {BOT_INDEX}] Spotify lookup error: {exc}", flush=True)
    return None

async def _am_load(url: str) -> wavelink.Playable | wavelink.Playlist | None:
    try:
        result = await wavelink.Pool.fetch_tracks(url)
        if isinstance(result, wavelink.Playlist):
            return result if result.tracks else None

        tracks = result.tracks if hasattr(result, "tracks") else list(result)
        return tracks[0] if tracks else None
    except Exception as exc:
        print(f"[Worker {BOT_INDEX}] Apple Music load error: {exc}", flush=True)
    return None

async def _ytm(query: str, count: int = 8) -> list[wavelink.Playable]:
    try:
        r = await wavelink.Playable.search(query, source=wavelink.TrackSource.YouTubeMusic)
        if isinstance(r, wavelink.Playlist): return r.tracks[:count]
        return r[:count] if r else []
    except Exception as exc:
        print(f"[Worker {BOT_INDEX}] YTM error: {exc}", flush=True)
        return []

async def _sc(query: str, count: int = 5) -> list[wavelink.Playable]:
    try:
        r = await wavelink.Playable.search(query, source=wavelink.TrackSource.SoundCloud)
        if isinstance(r, wavelink.Playlist): return r.tracks[:count]
        return r[:count] if r else []
    except Exception as exc:
        print(f"[Worker {BOT_INDEX}] SC error: {exc}", flush=True)
        return []

async def _first_clean(candidates: list) -> wavelink.Playable | None:
    for t in candidates:
        if not await _is_altered(t.title or ""):
            return t
    return None

async def _search(query: str, source: str) -> tuple[wavelink.Playable | None, str, str]:
    query = _SEARCH_PREFIX_RE.sub("", query).strip()
    label  = _source_label(source, query)
    is_url = query.startswith(("http://", "https://"))


    if is_url and _AM_RE.search(query):

        is_am_collection = any(seg in query for seg in ("/playlist/", "/album/"))
        if not is_am_collection:
            r = await _am_load(query)
            if isinstance(r, wavelink.Playable):
                return r, label, "Apple Music → YouTube Music"

            if isinstance(r, wavelink.Playlist) and r.tracks:
                _remember_alternates(r.tracks[0], r.tracks[1:], label, "Apple Music → YouTube Music")
                return r.tracks[0], label, "Apple Music → YouTube Music"
        return None, label, "Apple Music"

    if is_url:
        try:
            result = await wavelink.Pool.fetch_tracks(query)
            if isinstance(result, wavelink.Playlist):
                tracks = [t for t in result.tracks if not _is_local(t)]
                if tracks:
                    _remember_alternates(tracks[0], tracks[1:], label, "URL")
                return (tracks[0] if tracks else None), label, "URL"
            tracks = result.tracks if hasattr(result, "tracks") else list(result)
            tracks = [t for t in tracks if not _is_local(t)]
            return (tracks[0] if tracks else None), label, "URL"
        except Exception as exc:
            print(f"[Worker {BOT_INDEX}] URL load error: {exc}", flush=True)
        return None, label, "URL"

    if source == "sc":
        results = await _sc(query)
        if await _user_wants_altered(query):
            return (results[0] if results else None), label, "SoundCloud"
        clean = await _first_clean(results)
        selected = clean or (results[0] if results else None)
        if selected:
            _remember_alternates(selected, results, label, "SoundCloud")
        return selected, label, "SoundCloud"

    user_alt = await _user_wants_altered(query)
    sp = await _sp_lookup(query)
    if sp:
        yt_q  = f"{sp.title} {sp.author}".strip()
        path  = "Spotify → YouTube Music"
        print(f"[Worker {BOT_INDEX}] Spotify: {sp.title!r} by {sp.author!r} ({(sp.length or 0)//1000}s)", flush=True)
    else:
        yt_q  = query
        path  = "YouTube Music"

    if user_alt:
        results = await _ytm(yt_q)
        selected = max(results, key=lambda t: _track_score(t, query, sp)) if results else None
        if selected:
            ranked = sorted(results, key=lambda t: _track_score(t, query, sp), reverse=True)
            _remember_alternates(selected, ranked, label, path)
        return selected, label, path

    candidates = await _ytm(yt_q)

    scored = [(t, _track_score(t, query, sp)) for t in candidates]
    scored.sort(key=lambda x: x[1], reverse=True)
    for t, sc in scored[:3]:
        print(f"[Worker {BOT_INDEX}]   candidate {sc:>4}: {t.title!r} by {t.author!r} ({(t.length or 0)//1000}s)", flush=True)
    for t, sc in scored:
        if not await _is_altered(t.title or ""):
            print(f"[Worker {BOT_INDEX}] ✓ {t.title!r} (score {sc})", flush=True)
            _remember_alternates(t, [candidate for candidate, _ in scored], label, path)
            return t, label, path

    if sp:
        for ch_name in _PREFERRED_CHANNELS:
            ch_results = await _ytm(f"{yt_q} {ch_name}", count=5)
            ch_scored  = sorted([(t, _track_score(t, query, sp)) for t in ch_results],
                                 key=lambda x: x[1], reverse=True)
            for t, sc in ch_scored:
                if not await _is_altered(t.title or ""):
                    print(f"[Worker {BOT_INDEX}] ✓ via {ch_name!r}: {t.title!r} (score {sc})", flush=True)
                    _remember_alternates(t, [candidate for candidate, _ in ch_scored], label, path)
                    return t, label, path

    if candidates:
        ranked = sorted(candidates, key=lambda t: _track_score(t, query, sp), reverse=True)
        best = ranked[0]
        print(f"[Worker {BOT_INDEX}] ⚠ fallback: {best.title!r}", flush=True)
        _remember_alternates(best, ranked, label, path)
        return best, label, path

    return None, label, path

async def _search_playlist(query: str, source: str) -> tuple[list, str, str, int]:
    label  = _source_label(source, query)
    is_url = query.startswith(("http://", "https://"))
    raw: list[wavelink.Playable] = []
    name = query
    try:
        if is_url and _AM_RE.search(query):

            r = await _am_load(query)
            if isinstance(r, wavelink.Playlist):
                raw, name = r.tracks, (r.name or name)
            elif isinstance(r, wavelink.Playable):
                raw = [r]
        elif is_url:
            result = await wavelink.Pool.fetch_tracks(query)
            if isinstance(result, wavelink.Playlist):
                raw, name = result.tracks, (result.name or name)
            else:
                tracks = result.tracks if hasattr(result, "tracks") else list(result)
                raw = tracks
        elif source == "sc":
            r = await wavelink.Playable.search(query, source=wavelink.TrackSource.SoundCloud)
            if isinstance(r, wavelink.Playlist):
                raw, name = r.tracks, r.name
            elif isinstance(r, list) and r:
                raw = r
        else:
            r = await wavelink.Playable.search(f"spsearch:{query}")
            if isinstance(r, wavelink.Playlist):
                raw, name = r.tracks, r.name
            elif isinstance(r, list) and r:
                raw = r
    except Exception as exc:
        print(f"[Worker {BOT_INDEX}] Playlist search error: {exc}", flush=True)
    good, skipped = [], 0
    for t in raw:
        if _is_local(t): skipped += 1
        else:            good.append(t)
    return good, name, label, skipped


async def _handle_connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    global _text_channel, _guild_id, _channel_id
    try:
        data = await reader.readline()
        if not data:
            return
        cmd = json.loads(data.decode())
        op  = cmd.get("op")

        async def reply(ok: bool, msg: str = "", **extra):
            resp = json.dumps({"status": "ok" if ok else "error", "message": msg, **extra})
            writer.write((resp + "\n").encode())
            await writer.drain()


        global _skipping, _muted, _loop, _fail_streak

        gid  = cmd.get("guild_id")
        cid  = cmd.get("channel_id")
        tcid = cmd.get("text_channel_id")

        if tcid and gid:
            g = bot.get_guild(int(gid))
            if g:
                ch = g.get_channel(int(tcid))
                if isinstance(ch, discord.TextChannel):
                    _text_channel = ch

        if op == "ping":
            await reply(True, "pong",
                        discord_ok=bot.is_ready(),
                        discord_ms=round(bot.latency * 1000),
                        lavalink_ok=_lavalink_ok,
                        lavalink_uri=LAVALINK_URI,
                        busy=_busy,
                        queue_len=len(_track_queue))
            return

        if op in ("search_and_play", "queue_track"):
            track, lbl, path = await _search(cmd["query"], cmd["source"])
            if not track:
                await reply(False, "Nothing found.")
                return
            await _set_busy(int(gid), int(cid))
            player = await _connect_vc(int(gid), int(cid))
            if player.playing:
                _track_queue.append((track, lbl, path))
                await reply(True, "queued",
                            embed_type="queued",
                            title=track.title, author=track.author or "",
                            uri=track.uri or "", artwork=track.artwork or "",
                            duration=track.length or 0,
                            source_label=lbl, search_path=path,
                            queue_pos=len(_track_queue))
            else:
                _fail_streak = 0
                await _play(player, track)
                await reply(True, "playing",
                            embed_type="playing",
                            title=track.title, author=track.author or "",
                            uri=track.uri or "", artwork=track.artwork or "",
                            duration=track.length or 0,
                            source_label=lbl, search_path=path)
            return

        if op == "search_and_playlist":
            tracks, pl_name, lbl, skipped = await _search_playlist(cmd["query"], cmd["source"])
            if not tracks:
                await reply(False, f"No playable tracks found.{' (' + str(skipped) + ' local skipped)' if skipped else ''}")
                return
            await _set_busy(int(gid), int(cid))
            player = await _connect_vc(int(gid), int(cid))
            first = tracks[0]
            if player.playing:
                for t in tracks: _track_queue.append((t, lbl, "Playlist"))
                await reply(True, "queued_playlist",
                            embed_type="queued_playlist",
                            pl_name=pl_name, count=len(tracks),
                            title=first.title, artwork=first.artwork or "",
                            source_label=lbl, skipped=skipped)
            else:
                _fail_streak = 0
                for t in tracks[1:]: _track_queue.append((t, lbl, "Playlist"))
                await _play(player, first)
                await reply(True, "playing_playlist",
                            embed_type="playing_playlist",
                            pl_name=pl_name, count=len(tracks),
                            title=first.title, artwork=first.artwork or "",
                            source_label=lbl, skipped=skipped)
            return

        if op == "stop":
            player = _get_player(int(gid))
            if player:
                _track_queue.clear()
                await player.stop()
                await player.disconnect()
            await _set_free()
            await reply(True, "stopped")
            return

        if op == "pause_resume":
            player = _get_player(int(gid))
            if not player or (not player.playing and not player.paused):
                await reply(False, "Nothing playing.")
                return
            if player.paused:
                await player.pause(False)
                _cancel_idle_timer()
                await reply(True, "resumed")
            else:
                await player.pause(True)
                await reply(True, "paused")
            return

        if op == "skip":
            player = _get_player(int(gid))
            if not player or not player.playing:
                await reply(False, "Nothing playing.")
                return
            _skipping = True
            try:
                await player.stop()

                if _track_queue:
                    nxt, nxt_label, nxt_path = _track_queue.pop(0)
                    await _play(player, nxt)
                    if _text_channel and not _muted:
                        await _send_status(_text_channel, _now_playing_embed(nxt, nxt_label, nxt_path))
                else:
                    _start_idle_timer()
            finally:
                _skipping = False
            await reply(True, "skipped")
            return

        if op == "backward":
            player = _get_player(int(gid))
            if not player or not player.playing:
                await reply(False, "Nothing playing.")
                return
            await player.seek(0)
            await reply(True, "restarted")
            return

        if op == "seek":
            player = _get_player(int(gid))
            if not player or not player.playing:
                await reply(False, "Nothing playing.")
                return
            delta = cmd.get("delta_ms", 0)
            pos   = max(0, player.position + delta)
            if player.current and pos > player.current.length:
                pos = max(0, player.current.length - 1000)
            await player.seek(int(pos))
            await reply(True, "seeked", delta_ms=delta)
            return

        if op == "sync":

            player = _get_player(int(gid)) if gid else None

            if not player and _guild_id:
                player = _get_player(_guild_id)
            current_title = ""
            if _current_info:
                current_title = _current_info["title"]
            elif player and player.current:
                current_title = player.current.title or ""
            playing = bool(player and (player.playing or player.paused))
            await reply(
                True, "synced",
                busy=_busy,
                playing=playing,
                guild_id=_guild_id,
                channel_id=_channel_id,
                text_channel_id=_text_channel.id if _text_channel else None,
                current_title=current_title,
                queue_len=len(_track_queue),
                muted=_muted,
                loop=_loop,
            )
            return

        if op == "shutdown_graceful":

            player = _get_player(int(gid)) if gid else (_get_player(_guild_id) if _guild_id else None)
            if player:
                _track_queue.clear()
                _cancel_idle_timer()
                try:
                    await player.stop()
                    await player.disconnect()
                except Exception:
                    pass
            await _set_free()
            await reply(True, "shutdown_graceful")

            asyncio.ensure_future(_do_shutdown())
            return

        if op == "remove_from_queue":
            idx = int(cmd.get("index", -1))
            if idx < 0 or idx >= len(_track_queue):
                await reply(False, "Invalid queue index.")
                return
            removed_title = _track_queue[idx][0].title or "Unknown"
            del _track_queue[idx]
            await reply(True, "removed", title=removed_title, queue_remaining=len(_track_queue))
            return

        if op == "get_queue":

            player = _get_player(int(gid)) if gid else None
            current = None
            if _current_info and player and player.current:
                current = {"title": _current_info["title"], "author": _current_info["author"], "index": -1}
            elif player and player.current:
                t = player.current
                current = {"title": t.title or "Unknown", "author": t.author or "", "index": -1}
            queue_list = [
                {"title": t.title or "Unknown", "author": t.author or "", "index": i}
                for i, (t, _lbl, _path) in enumerate(_track_queue)
            ]
            await reply(True, "queue", current=current, queue=queue_list,
                        muted=_muted, loop=_loop)
            return

        if op == "toggle_mute":
            _muted = not _muted
            await reply(True, "muted" if _muted else "unmuted", muted=_muted)
            return

        if op == "toggle_loop":
            _loop = not _loop
            await reply(True, "loop_on" if _loop else "loop_off", loop=_loop)
            return

        if op == "jump_to":
            player = _get_player(int(gid))
            if not player or not player.playing:
                await reply(False, "Nothing playing.")
                return
            idx = int(cmd.get("index", 0))
            if idx < 0 or idx >= len(_track_queue):
                await reply(False, "Invalid queue index.")
                return

            target_track, target_lbl, target_path = _track_queue[idx]
            del _track_queue[:idx + 1]
            _skipping = True
            try:
                await player.stop()
                _fail_streak = 0
                await _play(player, target_track)
                if _text_channel and not _muted:
                    await _send_status(_text_channel, _now_playing_embed(target_track, target_lbl, target_path))
            finally:
                _skipping = False
            await reply(True, "jumped",
                        title=target_track.title or "Unknown",
                        author=target_track.author or "",
                        queue_remaining=len(_track_queue))
            return

        await reply(False, f"Unknown op: {op}")

    except Exception as exc:
        print(f"[Worker {BOT_INDEX}] IPC handler error: {exc}", flush=True)
        try:
            writer.write((json.dumps({"status": "error", "message": str(exc)}) + "\n").encode())
            await writer.drain()
        except Exception:
            pass
    finally:
        writer.close()

async def _ipc_server():
    if SOCKET_PATH.exists():
        SOCKET_PATH.unlink()
    server = await asyncio.start_unix_server(_handle_connection, path=str(SOCKET_PATH))
    print(f"[Worker {BOT_INDEX}] IPC listening on {SOCKET_PATH}", flush=True)
    async with server:
        await server.serve_forever()


async def _shutdown():
    await _post_status(bot, "shutdown")
    await bot.close()
    if SOCKET_PATH.exists():
        SOCKET_PATH.unlink()


if __name__ == "__main__":
    try:
        bot.run(TOKEN)
    except KeyboardInterrupt:
        pass
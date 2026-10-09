import asyncio
import json
import os
import random
import re
import sys
import time
import logging
from collections import deque
logging.basicConfig(level=logging.INFO)
logging.getLogger("wavelink").setLevel(logging.INFO)
from pathlib import Path


def _index_from_argv() -> int:
    if "--index" in sys.argv:
        return int(sys.argv[sys.argv.index("--index") + 1])
    m = re.search(r"(\d+)$", Path(sys.argv[0]).stem)
    return int(m.group(1)) if m else 1

BOT_INDEX = _index_from_argv()

SCRIPT_DIR  = Path(__file__).parent
SOCKET_PATH = SCRIPT_DIR / f".worker{BOT_INDEX}.sock"
MAIN_SOCKET = SCRIPT_DIR / ".main.sock"


from dotenv import load_dotenv
load_dotenv(SCRIPT_DIR / ".env")

import discord
import wavelink
from groq import AsyncGroq

import appsettings
import db

_SETTINGS     = appsettings.require_startup()
TOKEN         = _SETTINGS["tokens"][BOT_INDEX]
LAVALINK_URI  = _SETTINGS["lavalink"]["uri"]
LAVALINK_PASS = _SETTINGS["lavalink"]["password"]

VC_TIMEOUT   = 60.0
VC_RETRIES   = 3
IDLE_TIMEOUT = 180


YTDLP_TIMEOUT        = 20
URL_CACHE_TTL        = 3600
MAX_RESOLVE_ATTEMPTS = 2
MAX_FAIL_STREAK      = 3

PLAY_COUNT_MS   = 30_000   # a song counts as "listened" for the algo after this long
HISTORY_LEN     = 25
AUTOPLAY_BATCH  = 2
AUTOPLAY_LABEL  = "Autoplay"
ALGO_DIRECT_CHANCE = 0.3   # /autoplay: chance to replay a saved song instead of a recommendation

_groq         = AsyncGroq(api_key=os.environ["GROQ_API_KEY"])
_GROQ_MODEL   = "openai/gpt-oss-20b"
_GROQ_TIMEOUT = 4.0


_OFFICIAL_RE = re.compile(
    r"\b(official\s*(?:audio|video|music\s*video|lyric\s*video|visualizer)?)\b",
    re.IGNORECASE,
)

_ALTERED_QUICK = re.compile(
    r"\b(sped[\s_-]*up|slowed|reverb|nightcore|daycore|lofi|lo[\s_-]*fi|"
    r"bass[\s_-]*boost|8d|cover|acoustic|piano|karaoke|instrumental|remix|"
    r"mashup|bootleg|extended|phonk|trap)\b",
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

async def _groq_similar(seeds: list[dict], count: int = 5) -> list[str]:
    """Autoplay fallback: ask the LLM for songs similar to the seeds, as search queries."""
    listing = "\n".join(f"- {s.get('title', '')} — {s.get('author', '')}" for s in seeds[:5])
    try:
        r = await asyncio.wait_for(
            _groq.chat.completions.create(
                model=_GROQ_MODEL,
                messages=[
                    {"role": "system", "content":
                        f"Recommend {count} real, existing songs a listener of these songs would enjoy next. "
                        "Do not repeat the given songs. Prefer original studio versions. "
                        "Answer with one song per line, formatted exactly as: Title - Artist. No other text."},
                    {"role": "user", "content": listing},
                ],
                max_tokens=800, temperature=0.8,
                extra_body={"reasoning_effort": "low"},
            ),
            timeout=10.0,
        )
        text = r.choices[0].message.content or ""
    except Exception as exc:
        print(f"[Worker {BOT_INDEX}] Groq recommend error: {exc}", flush=True)
        return []
    lines = [re.sub(r"^[\s\-•*\d.)]+", "", l).strip() for l in text.splitlines()]
    return [l for l in lines if l][:count]


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


intents = discord.Intents.default()
intents.voice_states = True
bot = discord.Client(intents=intents)


# ── per-server playback session ─────────────────────────────────────────────
# A Discord bot can be in one voice channel per server, so each worker keeps one
# Session per guild and can play in several servers at once.

# Queue items are (track, source_label, search_path, requester_id).
QueueItem = tuple

class Session:
    def __init__(self, guild_id: int, channel_id: int, controller_id: int):
        self.guild_id      = guild_id
        self.channel_id    = channel_id
        self.controller_id = controller_id
        self.mode          = "me"           # "me" = only the controller, "all" = anyone
        self.text_channel: discord.TextChannel | None = None
        self.queue: list[QueueItem] = []
        self.fallbacks: dict[str, list[tuple[wavelink.Playable, str, str]]] = {}
        self.origin: dict[str, wavelink.Playable] = {}
        self.skipping     = False
        self.muted        = False
        self.loop         = False
        self.autoplay     = False
        self.autoplay_source = "session"    # "session" = seeds from what was played, "algo" = saved songs
        self.autoplay_user: int | None = None
        self.autoplay_task: asyncio.Task | None = None
        self.fail_streak  = 0
        self.current_info: dict | None = None
        self.current_meta: dict | None = None
        self.current_requester: int | None = None
        self.idle_task: asyncio.Task | None = None
        self.auto_paused  = False
        self.connecting   = False
        self.closing      = False
        self.history: deque[dict] = deque(maxlen=HISTORY_LEN)

    def snapshot(self) -> dict:
        return {
            "channel_id":      self.channel_id,
            "controller_id":   self.controller_id,
            "mode":            self.mode,
            "autoplay":        self.autoplay,
            "text_channel_id": self.text_channel.id if self.text_channel else None,
        }

sessions: dict[int, Session] = {}


def _track_key(track: wavelink.Playable) -> str:
    return track.identifier or track.uri or f"{track.title}:{track.author}"

def _remember_alternates(
    sess: "Session | None",
    track: wavelink.Playable,
    candidates: list[wavelink.Playable],
    source_label: str,
    search_path: str,
) -> None:
    if sess is None:
        return
    key = _track_key(track)
    seen = {key}
    alternates = []
    for candidate in candidates:
        candidate_key = _track_key(candidate)
        if candidate_key not in seen:
            seen.add(candidate_key)
            alternates.append((candidate, source_label, search_path))
    if alternates:
        sess.fallbacks[key] = alternates
    else:
        sess.fallbacks.pop(key, None)


# ── state events to the main controller ─────────────────────────────────────

_events: asyncio.Queue | None = None

def _notify(guild_id: int) -> None:
    if _events is None:
        return
    sess = sessions.get(guild_id)
    _events.put_nowait({"op": "state", "index": BOT_INDEX, "guild_id": guild_id,
                        "session": sess.snapshot() if sess else None})

async def _event_sender():
    # One sender keeps events in order, so main never sees "free" before "busy".
    while True:
        msg = await _events.get()
        try:
            _, writer = await asyncio.wait_for(asyncio.open_unix_connection(str(MAIN_SOCKET)), timeout=3.0)
            writer.write((json.dumps(msg) + "\n").encode())
            await writer.drain()
            writer.close()
        except Exception:
            pass   # main resyncs before every pick, so a lost event is harmless


def _bg(fn, *args) -> None:
    async def run():
        try:
            await asyncio.to_thread(fn, *args)
        except Exception as exc:
            print(f"[Worker {BOT_INDEX}] DB error in {fn.__name__}: {exc}", flush=True)
    asyncio.ensure_future(run())


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

async def _resolve(track: wavelink.Playable
                   ) -> tuple[wavelink.Playable, wavelink.Playable, wavelink.Playable | None] | None:
    """Returns (what Lavalink plays, the track the user asked for, the YouTube track it maps to)."""
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
        return src, track, None

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
    return tracks[0], track, src

def _track_meta(origin: wavelink.Playable, yt_src: wavelink.Playable | None) -> dict:
    """What the algo stores for a song. The YouTube id is the stable key when we have one."""
    yt_id = ""
    if yt_src is not None:
        yt_id = yt_src.identifier or ""
    elif _YT_RE.search(origin.uri or ""):
        yt_id = origin.identifier or ""
    key = yt_id or origin.uri or f"{origin.title}:{origin.author}"
    return {"key": key, "title": origin.title or "", "author": origin.author or "",
            "uri": origin.uri or "", "yt_id": yt_id}

def _prefetch_next(sess: Session) -> None:
    if not sess.queue:
        return
    uri = sess.queue[0][0].uri or ""
    if _YT_RE.search(uri):
        asyncio.ensure_future(_ytdlp_url(uri))

def _set_current(sess: Session, origin: wavelink.Playable,
                 yt_src: wavelink.Playable | None, requester: int | None) -> None:
    meta = _track_meta(origin, yt_src)
    sess.current_info      = {"title": origin.title or "Unknown", "author": origin.author or ""}
    sess.current_meta      = meta
    sess.current_requester = requester
    sess.history.append(meta)

async def _set_vc_status(guild_id: int, channel_id: int, title: str | None) -> None:
    """Show the song title as the voice channel status (like Rythm). Needs 'Set Voice Channel Status'."""
    guild = bot.get_guild(guild_id)
    vc = guild.get_channel(channel_id) if guild else None
    if not isinstance(vc, discord.VoiceChannel):
        return
    try:
        await vc.edit(status=(title[:500] if title else None))
    except discord.Forbidden:
        print(f"[Worker {BOT_INDEX}] No permission to set voice status in guild {guild_id}", flush=True)
    except Exception as exc:
        print(f"[Worker {BOT_INDEX}] Voice status update failed: {exc}", flush=True)

def _after_start(sess: Session) -> None:
    if sess.current_info:
        asyncio.ensure_future(_set_vc_status(sess.guild_id, sess.channel_id, sess.current_info["title"]))
    _cancel_idle_timer(sess)
    _prefetch_next(sess)
    if sess.autoplay and not sess.queue:
        _kick_autoplay(sess)

async def _play(sess: Session, player: wavelink.Player, track: wavelink.Playable,
                requester: int | None) -> bool:
    alternates = [t for t, _, _ in sess.fallbacks.get(_track_key(track), [])]
    candidates = [track] + alternates

    for i, cand in enumerate(candidates[:MAX_RESOLVE_ATTEMPTS]):
        resolved = await _resolve(cand)
        if resolved is None:
            print(f"[Worker {BOT_INDEX}] Could not resolve {cand.title!r} — trying next candidate", flush=True)
            continue
        playable, origin, yt_src = resolved
        sess.origin[_track_key(playable)] = origin
        _set_current(sess, origin, yt_src, requester)
        if i > 0:

            rest = sess.fallbacks.get(_track_key(track), [])[i:]
            if rest:
                sess.fallbacks[_track_key(origin)] = rest
        await player.play(playable)
        _after_start(sess)
        return True


    print(f"[Worker {BOT_INDEX}] Resolve failed for {track.title!r} — passing to Lavalink as-is", flush=True)
    sess.origin[_track_key(track)] = track
    _set_current(sess, track, None, requester)
    await player.play(track)
    _after_start(sess)
    return False

def _account(sess: Session, played_ms: int | None = None,
             finished: bool = False, skipped: bool = False) -> None:
    """Feed the outgoing track into the requester's algo: a listen, a quick skip, or nothing."""
    meta, uid = sess.current_meta, sess.current_requester
    sess.current_meta = None
    if not meta or not uid:
        return
    if finished or (played_ms or 0) >= PLAY_COUNT_MS:
        s = appsettings.get()
        _bg(db.record_play, sess.guild_id, uid, meta, int(s["algo_max_songs"]), int(s["algo_max_kb"]))
    elif skipped:
        _bg(db.record_skip, sess.guild_id, uid, meta["key"])

def _enqueue(sess: Session, item: QueueItem) -> int:
    """User songs go ahead of autoplay picks. Returns the 1-based queue position."""
    for i, q in enumerate(sess.queue):
        if q[1] == AUTOPLAY_LABEL:
            sess.queue.insert(i, item)
            return i + 1
    sess.queue.append(item)
    return len(sess.queue)

async def _advance(sess: Session, player: wavelink.Player, announce: bool = True) -> None:
    """Play the next queued track (refilling from autoplay if needed), or go idle."""
    if not sess.queue and sess.autoplay:
        await _autoplay_fill(sess)
    if sessions.get(sess.guild_id) is not sess:
        return
    if sess.queue:
        nxt, nxt_label, nxt_path, req = sess.queue.pop(0)
        await _play(sess, player, nxt, req)
        if announce and sess.text_channel and not sess.muted:
            await _send_status(sess.text_channel, _now_playing_embed(nxt, nxt_label, nxt_path))
    else:
        sess.current_info = None
        asyncio.ensure_future(_set_vc_status(sess.guild_id, sess.channel_id, None))
        _start_idle_timer(sess)


# ── autoplay ─────────────────────────────────────────────────────────────────

def _norm_title(title: str) -> str:
    t = re.sub(r"[\(\[].*?[\)\]]", " ", (title or "").lower())
    t = re.sub(r"\b(official|audio|video|lyrics?|music|hd|hq|4k|mv)\b", " ", t)
    return " ".join(re.findall(r"\w+", t))

def _excluded_sets(sess: Session) -> tuple[set[str], set[str]]:
    ids, titles = set(), set()
    for m in sess.history:
        if m.get("yt_id"): ids.add(m["yt_id"])
        titles.add(_norm_title(m.get("title", "")))
    for t, *_ in sess.queue:
        ids.add(t.identifier or "")
        titles.add(_norm_title(t.title or ""))
    return ids, titles

def _usable_rec(t: wavelink.Playable, ids: set[str], titles: set[str]) -> bool:
    if (t.identifier or "") in ids or _norm_title(t.title or "") in titles:
        return False
    if _ALTERED_QUICK.search(t.title or ""):
        return False
    length = t.length or 0
    return 60_000 <= length <= 10 * 60_000   # skip shorts and hour-long compilations

async def _yt_id_for(seed: dict) -> str:
    if seed.get("yt_id"):
        return seed["yt_id"]
    cands = await _ytm(f"{seed.get('title', '')} {seed.get('author', '')}".strip(), count=3)
    return (cands[0].identifier or "") if cands else ""

async def _mix_for(seed: dict) -> list[wavelink.Playable]:
    """YouTube's own radio mix for a song (watch?v=ID&list=RDID)."""
    yt_id = await _yt_id_for(seed)
    if not yt_id:
        return []
    try:
        res = await wavelink.Pool.fetch_tracks(f"https://www.youtube.com/watch?v={yt_id}&list=RD{yt_id}")
    except Exception as exc:
        print(f"[Worker {BOT_INDEX}] Mix load failed for {yt_id}: {exc}", flush=True)
        return []
    tracks = res.tracks if isinstance(res, wavelink.Playlist) else list(res or [])
    return [t for t in tracks if t.identifier != yt_id]

def _weighted_pick(rows: list[dict], k: int) -> list[dict]:
    pool, out = list(rows), []
    while pool and len(out) < k:
        weights = [max(r["score"], 0.5) for r in pool]
        r = random.choices(pool, weights=weights, k=1)[0]
        out.append(r)
        pool.remove(r)
    return out

async def _autoplay_seeds(sess: Session) -> list[dict]:
    n = max(1, int(appsettings.get()["autoplay_seed_count"]))
    if sess.autoplay_source == "algo" and sess.autoplay_user:
        rows = await asyncio.to_thread(db.user_tracks, sess.guild_id, sess.autoplay_user, 200)
        return _weighted_pick(rows, n)
    recent = list(sess.history)[-n:]
    recent.reverse()   # most recent first
    return recent

async def _recommend(sess: Session, count: int) -> list[tuple[wavelink.Playable, str, str]]:
    seeds = await _autoplay_seeds(sess)
    if not seeds:
        return []
    ids, titles = _excluded_sets(sess)
    out: list[tuple[wavelink.Playable, str, str]] = []

    def take(t: wavelink.Playable, path: str) -> None:
        ids.add(t.identifier or "")
        titles.add(_norm_title(t.title or ""))
        out.append((t, AUTOPLAY_LABEL, path))

    if sess.autoplay_source == "algo" and random.random() < ALGO_DIRECT_CHANCE:
        for seed in seeds:
            if seed.get("yt_id") and seed["yt_id"] not in ids:
                try:
                    res = await wavelink.Pool.fetch_tracks(f"https://www.youtube.com/watch?v={seed['yt_id']}")
                    tracks = res.tracks if isinstance(res, wavelink.Playlist) else list(res or [])
                except Exception:
                    tracks = []
                if tracks:
                    take(tracks[0], "Your saved songs")
                break

    for seed in seeds:
        if len(out) >= count:
            break
        mix = [t for t in await _mix_for(seed) if _usable_rec(t, ids, titles)]
        if mix:
            take(random.choice(mix[:8]), f"Mix · {seed.get('title', '')[:40]}")

    if not out:
        print(f"[Worker {BOT_INDEX}] No mix results — asking Groq for recommendations", flush=True)
        for line in await _groq_similar(seeds):
            if len(out) >= count:
                break
            t, _, _ = await _search(line, "sp", None)
            if t and _usable_rec(t, ids, titles):
                take(t, "Autoplay · AI pick")
    return out

async def _autoplay_refill(sess: Session) -> None:
    if sess.queue:
        return
    try:
        recs = await _recommend(sess, AUTOPLAY_BATCH)
    except Exception as exc:
        print(f"[Worker {BOT_INDEX}] Autoplay refill failed: {exc}", flush=True)
        return
    if sess.closing or sessions.get(sess.guild_id) is not sess or not sess.autoplay:
        return
    requester = sess.autoplay_user or sess.controller_id
    for t, lbl, path in recs:
        sess.queue.append((t, lbl, path, requester))
    if recs:
        print(f"[Worker {BOT_INDEX}] Autoplay queued: {', '.join(repr(t.title) for t, _, _ in recs)}", flush=True)
        _prefetch_next(sess)

def _kick_autoplay(sess: Session) -> None:
    if sess.autoplay_task is None or sess.autoplay_task.done():
        sess.autoplay_task = asyncio.ensure_future(_autoplay_refill(sess))

async def _autoplay_fill(sess: Session) -> None:
    _kick_autoplay(sess)
    try:
        await sess.autoplay_task
    except Exception:
        pass


# ── idle / lifecycle ─────────────────────────────────────────────────────────

def _start_idle_timer(sess: Session):
    if sess.idle_task and not sess.idle_task.done():
        return
    sess.idle_task = asyncio.ensure_future(_idle_countdown(sess))

def _cancel_idle_timer(sess: Session):
    if sess.idle_task and not sess.idle_task.done():
        sess.idle_task.cancel()
    sess.idle_task = None

def _humans_in_vc(sess: Session) -> bool:
    guild = bot.get_guild(sess.guild_id)
    vc = guild.get_channel(sess.channel_id) if guild else None
    return bool(vc) and any(not m.bot for m in vc.members)

async def _idle_countdown(sess: Session):
    try:
        await asyncio.sleep(IDLE_TIMEOUT)
    except asyncio.CancelledError:
        return
    if sessions.get(sess.guild_id) is not sess:
        return
    player = _get_player(sess.guild_id)
    active = player is not None and player.current is not None and not sess.auto_paused
    if active and _humans_in_vc(sess):
        print(f"[Worker {BOT_INDEX}] Idle fired but still in use (guild {sess.guild_id}) — skipping", flush=True)
        return
    print(f"[Worker {BOT_INDEX}] Idle timeout — leaving guild {sess.guild_id}", flush=True)
    await _end_session(sess.guild_id)

async def _end_session(guild_id: int, disconnect: bool = True) -> None:
    sess = sessions.pop(guild_id, None)
    if not sess:
        return
    sess.closing = True
    _cancel_idle_timer(sess)
    if sess.autoplay_task and not sess.autoplay_task.done():
        sess.autoplay_task.cancel()
    player = _get_player(guild_id)
    if player:
        _account(sess, player.position if player.current else 0)
        if disconnect:
            await _set_vc_status(guild_id, sess.channel_id, None)   # clear it while still in the channel
            try:
                await player.stop()
                await player.disconnect()
            except Exception:
                pass
    _notify(guild_id)

def _claim(guild_id: int, channel_id: int, user_id: int) -> tuple[Session, bool]:
    sess = sessions.get(guild_id)
    if sess:
        if sess.channel_id != channel_id:
            raise RuntimeError("This worker is already playing in another voice channel in this server.")
        return sess, False
    sess = Session(guild_id, channel_id, user_id)
    sessions[guild_id] = sess
    _notify(guild_id)
    return sess, True

async def _do_shutdown():
    await asyncio.sleep(0.3)
    await _shutdown()

    asyncio.get_event_loop().stop()

def _get_player(guild_id: int) -> wavelink.Player | None:
    guild = bot.get_guild(guild_id)
    return guild.voice_client if guild else None

def _session_for_player(player) -> Session | None:
    if not player or not getattr(player, "guild", None):
        return None
    return sessions.get(player.guild.id)


# ── whitelist ────────────────────────────────────────────────────────────────

async def _enforce_whitelist(guild: discord.Guild) -> None:
    # Workers leave quietly; the main bot is the one that posts the notice.
    if not appsettings.is_whitelisted(guild.id):
        print(f"[Worker {BOT_INDEX}] Leaving non-whitelisted guild {guild.id} ({guild.name})", flush=True)
        try:
            await guild.leave()
        except Exception as exc:
            print(f"[Worker {BOT_INDEX}] Could not leave {guild.id}: {exc}", flush=True)


_started = False

@bot.event
async def on_ready():
    global _started, _events
    print(f"[Worker {BOT_INDEX}] Discord ready — {bot.user}", flush=True)
    for g in list(bot.guilds):
        await _enforce_whitelist(g)
    # on_ready fires again after every reconnect; the IPC server and Lavalink pool must start only once.
    if _started:
        return
    _started = True
    _events = asyncio.Queue()
    asyncio.ensure_future(_event_sender())
    asyncio.ensure_future(_connect_lavalink())
    asyncio.ensure_future(_ipc_server())

@bot.event
async def on_guild_join(guild: discord.Guild):
    await _enforce_whitelist(guild)

@bot.event
async def on_wavelink_node_ready(payload: wavelink.NodeReadyEventPayload):
    global _lavalink_ok
    _lavalink_ok = True
    print(f"[Worker {BOT_INDEX}] Lavalink OK: {payload.node.uri}", flush=True)

@bot.event
async def on_wavelink_node_disconnected(payload: wavelink.NodeDisconnectedEventPayload):
    global _lavalink_ok
    _lavalink_ok = False
    print(f"[Worker {BOT_INDEX}] Lavalink disconnected", flush=True)

_lavalink_ok = False

@bot.event
async def on_wavelink_track_end(payload: wavelink.TrackEndEventPayload):
    player: wavelink.Player = payload.player
    sess = _session_for_player(player)
    if not sess:
        return
    reason = str(getattr(payload, "reason", "") or "").lower()
    ended  = payload.track
    orig   = sess.origin.pop(_track_key(ended), ended) if ended else None

    if reason == "finished":
        sess.fail_streak = 0
        _account(sess, finished=True)


    if reason in ("loadfailed", "replaced", "stopped", "cleanup"):
        return

    if sess.skipping:
        return

    if orig:
        sess.fallbacks.pop(_track_key(orig), None)


    if sess.loop and orig:
        sess.queue.insert(0, (orig, "Looping", "", sess.current_requester))

    await _advance(sess, player)

@bot.event
async def on_wavelink_track_exception(payload: wavelink.TrackExceptionEventPayload):
    player: wavelink.Player = payload.player
    sess = _session_for_player(player)
    if not sess:
        return
    sess.skipping = False
    sess.current_meta = None   # never played, so nothing to learn from
    failed_track = payload.track

    origin = sess.origin.get(_track_key(failed_track), failed_track) if failed_track else None

    try:
        first_line = str((payload.exception or {}).get("message", "")).strip().splitlines()[0]
    except Exception:
        first_line = "unknown error"
    print(f"[Worker {BOT_INDEX}] Track exception: {first_line}", flush=True)

    sess.fail_streak += 1
    if sess.fail_streak >= MAX_FAIL_STREAK:
        print(f"[Worker {BOT_INDEX}] {sess.fail_streak} tracks failed in a row — stopping", flush=True)
        if sess.text_channel:
            await _send_status(sess.text_channel, discord.Embed(
                title="❌  Playback failed",
                description=f"{sess.fail_streak} tracks in a row could not be played, so I stopped.\n"
                            f"Last error: `{first_line[:200]}`",
                colour=discord.Colour.red(),
            ))
        sess.queue.clear()
        sess.autoplay = False
        sess.fail_streak = 0
        try:
            await player.stop()
        except Exception:
            pass
        _start_idle_timer(sess)
        _notify(sess.guild_id)
        return

    alternatives = sess.fallbacks.pop(_track_key(origin), []) if origin else []
    if alternatives:
        nxt, nxt_label, nxt_path = alternatives.pop(0)
        if alternatives:
            sess.fallbacks[_track_key(nxt)] = alternatives
        print(f"[Worker {BOT_INDEX}] Trying alternate track: {nxt.title!r}", flush=True)
        await _play(sess, player, nxt, sess.current_requester)
        return
    await _advance(sess, player, announce=False)

@bot.event
async def on_voice_state_update(member: discord.Member, before, after):
    sess = sessions.get(member.guild.id)
    if not sess:
        return

    if bot.user and member.id == bot.user.id:
        if after.channel is None and not sess.connecting and not sess.closing:
            # Kicked or disconnected by someone else: free the session so main sees us as available.
            print(f"[Worker {BOT_INDEX}] Disconnected from voice in guild {sess.guild_id} — freeing", flush=True)
            await _end_session(sess.guild_id, disconnect=False)
        elif after.channel is not None and after.channel.id != sess.channel_id:
            sess.channel_id = after.channel.id   # dragged to another channel
            _notify(sess.guild_id)
        return

    if member.bot:
        return
    touched = {getattr(before.channel, "id", None), getattr(after.channel, "id", None)}
    if sess.channel_id not in touched:
        return

    player = _get_player(sess.guild_id)
    if not _humans_in_vc(sess):
        print(f"[Worker {BOT_INDEX}] VC empty in guild {sess.guild_id} — pausing, idle timer started", flush=True)
        if player and player.current and not player.paused:
            try:
                await player.pause(True)
                sess.auto_paused = True
            except Exception:
                pass
        _start_idle_timer(sess)
    else:
        if sess.auto_paused and player and player.paused:
            try:
                await player.pause(False)
            except Exception:
                pass
        sess.auto_paused = False
        if player and player.current:
            _cancel_idle_timer(sess)


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


async def _connect_vc(sess: Session) -> wavelink.Player:
    guild = bot.get_guild(sess.guild_id)
    if not guild:
        raise RuntimeError(f"Worker {BOT_INDEX} isn't in this server — invite it first.")
    vc = guild.get_channel(sess.channel_id)
    if not isinstance(vc, discord.VoiceChannel):
        raise RuntimeError(f"Channel {sess.channel_id} not found or not a voice channel")
    player: wavelink.Player = guild.voice_client
    if player and player.channel.id == vc.id:
        return player
    sess.connecting = True
    try:
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
    finally:
        sess.connecting = False


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

async def _search(query: str, source: str,
                  sess: Session | None) -> tuple[wavelink.Playable | None, str, str]:
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
                _remember_alternates(sess, r.tracks[0], r.tracks[1:], label, "Apple Music → YouTube Music")
                return r.tracks[0], label, "Apple Music → YouTube Music"
        return None, label, "Apple Music"

    if is_url:
        try:
            result = await wavelink.Pool.fetch_tracks(query)
            if isinstance(result, wavelink.Playlist):
                tracks = [t for t in result.tracks if not _is_local(t)]
                if tracks:
                    _remember_alternates(sess, tracks[0], tracks[1:], label, "URL")
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
            _remember_alternates(sess, selected, results, label, "SoundCloud")
        return selected, label, "SoundCloud"

    user_alt = await _user_wants_altered(query)
    sp = await _sp_lookup(query) if source != "yt" else None
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
            _remember_alternates(sess, selected, ranked, label, path)
        return selected, label, path

    candidates = await _ytm(yt_q)

    scored = [(t, _track_score(t, query, sp)) for t in candidates]
    scored.sort(key=lambda x: x[1], reverse=True)
    for t, sc in scored[:3]:
        print(f"[Worker {BOT_INDEX}]   candidate {sc:>4}: {t.title!r} by {t.author!r} ({(t.length or 0)//1000}s)", flush=True)
    for t, sc in scored:
        if not await _is_altered(t.title or ""):
            print(f"[Worker {BOT_INDEX}] ✓ {t.title!r} (score {sc})", flush=True)
            _remember_alternates(sess, t, [candidate for candidate, _ in scored], label, path)
            return t, label, path

    if sp:
        for ch_name in _PREFERRED_CHANNELS:
            ch_results = await _ytm(f"{yt_q} {ch_name}", count=5)
            ch_scored  = sorted([(t, _track_score(t, query, sp)) for t in ch_results],
                                 key=lambda x: x[1], reverse=True)
            for t, sc in ch_scored:
                if not await _is_altered(t.title or ""):
                    print(f"[Worker {BOT_INDEX}] ✓ via {ch_name!r}: {t.title!r} (score {sc})", flush=True)
                    _remember_alternates(sess, t, [candidate for candidate, _ in ch_scored], label, path)
                    return t, label, path

    if candidates:
        ranked = sorted(candidates, key=lambda t: _track_score(t, query, sp), reverse=True)
        best = ranked[0]
        print(f"[Worker {BOT_INDEX}] ⚠ fallback: {best.title!r}", flush=True)
        _remember_alternates(sess, best, ranked, label, path)
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


def _track_reply(track: wavelink.Playable, embed_type: str, lbl: str, path: str, **extra) -> dict:
    return dict(embed_type=embed_type,
                title=track.title, author=track.author or "",
                uri=track.uri or "", artwork=track.artwork or "",
                duration=track.length or 0,
                source_label=lbl, search_path=path, **extra)


async def _handle_connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
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

        gid  = int(cmd["guild_id"]) if cmd.get("guild_id") else None
        cid  = int(cmd["channel_id"]) if cmd.get("channel_id") else None
        tcid = cmd.get("text_channel_id")
        uid  = int(cmd.get("user_id") or 0) or None

        if op == "ping":
            await reply(True, "pong",
                        discord_ok=bot.is_ready(),
                        discord_ms=round(bot.latency * 1000),
                        lavalink_ok=_lavalink_ok,
                        lavalink_uri=LAVALINK_URI,
                        sessions=len(sessions),
                        session=sessions[gid].snapshot() if gid in sessions else None,
                        queue_len=len(sessions[gid].queue) if gid in sessions else 0)
            return

        if op == "sync":
            await reply(True, "synced",
                        user_id=bot.user.id if bot.user else None,
                        guilds=[g.id for g in bot.guilds],
                        lavalink_ok=_lavalink_ok,
                        sessions={str(g): s.snapshot() for g, s in sessions.items()})
            return

        if op == "shutdown_graceful":
            for g in list(sessions):
                await _end_session(g)
            await reply(True, "shutdown_graceful")

            asyncio.ensure_future(_do_shutdown())
            return

        if gid is None:
            await reply(False, "guild_id required")
            return

        if op in ("search_and_play", "queue_track", "search_and_playlist", "autoplay_start"):
            try:
                sess, new = _claim(gid, cid, uid)
            except RuntimeError as exc:
                await reply(False, str(exc))
                return
            if tcid:
                g = bot.get_guild(gid)
                ch = g.get_channel(int(tcid)) if g else None
                if isinstance(ch, discord.TextChannel):
                    sess.text_channel = ch
            try:
                await _start_op(sess, op, cmd, uid, reply)
            except Exception:
                if new and sessions.get(gid) is sess:
                    await _end_session(gid)
                raise
            player = _get_player(gid)
            if new and sessions.get(gid) is sess and not (player and player.current):
                await _end_session(gid)   # nothing ended up playing (search failed etc.)
            return

        sess = sessions.get(gid)
        player = _get_player(gid)

        if op == "stop":
            await _end_session(gid)
            await reply(True, "stopped")
            return

        if op == "get_queue":
            current = None
            if sess and player and player.current:
                info = sess.current_info or {"title": player.current.title or "Unknown",
                                             "author": player.current.author or ""}
                current = {"title": info["title"], "author": info["author"], "index": -1}
            queue_list = [
                {"title": t.title or "Unknown", "author": t.author or "", "index": i,
                 "autoplay": lbl == AUTOPLAY_LABEL}
                for i, (t, lbl, _path, _req) in enumerate(sess.queue if sess else [])
            ]
            await reply(True, "queue", current=current, queue=queue_list,
                        muted=sess.muted if sess else False, loop=sess.loop if sess else False,
                        autoplay=sess.autoplay if sess else False,
                        session=sess.snapshot() if sess else None)
            return

        if not sess:
            await reply(False, "Nothing playing.")
            return

        if op == "pause_resume":
            if not player or (not player.playing and not player.paused):
                await reply(False, "Nothing playing.")
                return
            if player.paused:
                await player.pause(False)
                sess.auto_paused = False
                _cancel_idle_timer(sess)
                await reply(True, "resumed")
            else:
                await player.pause(True)
                await reply(True, "paused")
            return

        if op == "skip":
            if not player or not player.playing:
                await reply(False, "Nothing playing.")
                return
            _account(sess, player.position, skipped=True)
            sess.skipping = True
            try:
                await player.stop()
                await _advance(sess, player)
            finally:
                sess.skipping = False
            await reply(True, "skipped")
            return

        if op == "backward":
            if not player or not player.playing:
                await reply(False, "Nothing playing.")
                return
            await player.seek(0)
            await reply(True, "restarted")
            return

        if op == "seek":
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

        if op == "remove_from_queue":
            idx = int(cmd.get("index", -1))
            if idx < 0 or idx >= len(sess.queue):
                await reply(False, "Invalid queue index.")
                return
            removed_title = sess.queue[idx][0].title or "Unknown"
            del sess.queue[idx]
            await reply(True, "removed", title=removed_title, queue_remaining=len(sess.queue))
            return

        if op == "toggle_mute":
            sess.muted = not sess.muted
            await reply(True, "muted" if sess.muted else "unmuted", muted=sess.muted)
            return

        if op == "toggle_loop":
            sess.loop = not sess.loop
            await reply(True, "loop_on" if sess.loop else "loop_off", loop=sess.loop)
            return

        if op == "toggle_autoplay":
            sess.autoplay = not sess.autoplay
            if sess.autoplay:
                if not sess.history and sess.autoplay_source != "algo":
                    sess.autoplay = False
                    await reply(False, "Play something first so autoplay has songs to base picks on.")
                    return
                if not sess.queue:
                    if player and player.current:
                        _kick_autoplay(sess)
                    elif player:
                        await _advance(sess, player)
            else:
                sess.queue = [q for q in sess.queue if q[1] != AUTOPLAY_LABEL]
            _notify(gid)
            await reply(True, "autoplay_on" if sess.autoplay else "autoplay_off",
                        autoplay=sess.autoplay, session=sess.snapshot())
            return

        if op == "set_mode":
            mode = cmd.get("mode")
            if mode not in ("me", "all"):
                await reply(False, "mode must be 'me' or 'all'")
                return
            sess.mode = mode
            _notify(gid)
            await reply(True, f"mode_{mode}", session=sess.snapshot())
            return

        if op == "jump_to":
            if not player or not player.playing:
                await reply(False, "Nothing playing.")
                return
            idx = int(cmd.get("index", 0))
            if idx < 0 or idx >= len(sess.queue):
                await reply(False, "Invalid queue index.")
                return

            target_track, target_lbl, target_path, target_req = sess.queue[idx]
            del sess.queue[:idx + 1]
            _account(sess, player.position, skipped=True)
            sess.skipping = True
            try:
                await player.stop()
                sess.fail_streak = 0
                await _play(sess, player, target_track, target_req)
                if sess.text_channel and not sess.muted:
                    await _send_status(sess.text_channel, _now_playing_embed(target_track, target_lbl, target_path))
            finally:
                sess.skipping = False
            await reply(True, "jumped",
                        title=target_track.title or "Unknown",
                        author=target_track.author or "",
                        queue_remaining=len(sess.queue))
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


async def _start_op(sess: Session, op: str, cmd: dict, uid: int | None, reply) -> None:
    """Ops that can start playback: /play, /playlist, /autoplay."""
    gid = sess.guild_id

    if op in ("search_and_play", "queue_track"):
        track, lbl, path = await _search(cmd["query"], cmd["source"], sess)
        if not track:
            await reply(False, "Nothing found.")
            return
        player = await _connect_vc(sess)
        if player.playing:
            pos = _enqueue(sess, (track, lbl, path, uid))
            await reply(True, "queued", session=sess.snapshot(),
                        **_track_reply(track, "queued", lbl, path, queue_pos=pos))
        else:
            sess.fail_streak = 0
            await _play(sess, player, track, uid)
            await reply(True, "playing", session=sess.snapshot(),
                        **_track_reply(track, "playing", lbl, path))
        return

    if op == "search_and_playlist":
        tracks, pl_name, lbl, skipped = await _search_playlist(cmd["query"], cmd["source"])
        if not tracks:
            await reply(False, f"No playable tracks found.{' (' + str(skipped) + ' local skipped)' if skipped else ''}")
            return
        player = await _connect_vc(sess)
        first = tracks[0]
        if player.playing:
            for t in tracks: _enqueue(sess, (t, lbl, "Playlist", uid))
            await reply(True, "queued_playlist", session=sess.snapshot(),
                        embed_type="queued_playlist",
                        pl_name=pl_name, count=len(tracks),
                        title=first.title, artwork=first.artwork or "",
                        source_label=lbl, skipped=skipped)
        else:
            sess.fail_streak = 0
            for t in tracks[1:]: _enqueue(sess, (t, lbl, "Playlist", uid))
            await _play(sess, player, first, uid)
            await reply(True, "playing_playlist", session=sess.snapshot(),
                        embed_type="playing_playlist",
                        pl_name=pl_name, count=len(tracks),
                        title=first.title, artwork=first.artwork or "",
                        source_label=lbl, skipped=skipped)
        return

    if op == "autoplay_start":
        rows = await asyncio.to_thread(db.user_tracks, gid, uid, 1)
        if not rows:
            await reply(False, "You have no saved songs in this server yet. Play some music first "
                               f"(a song is saved after {PLAY_COUNT_MS // 1000}s of listening).")
            return
        sess.autoplay        = True
        sess.autoplay_source = "algo"
        sess.autoplay_user   = uid
        _notify(gid)
        player = await _connect_vc(sess)
        if player.playing:
            sess.queue = [q for q in sess.queue if q[1] != AUTOPLAY_LABEL]
            if not sess.queue:
                _kick_autoplay(sess)
            await reply(True, "autoplay_on", autoplay=True, session=sess.snapshot())
            return
        await _autoplay_fill(sess)
        if not sess.queue:
            await reply(False, "Couldn't find anything to autoplay right now — try again in a bit.")
            return
        nxt, lbl, path, req = sess.queue.pop(0)
        sess.fail_streak = 0
        await _play(sess, player, nxt, req)
        await reply(True, "playing", session=sess.snapshot(),
                    **_track_reply(nxt, "playing", "Autoplay", path or "Your saved songs"))
        return


async def _ipc_server():
    if SOCKET_PATH.exists():
        SOCKET_PATH.unlink()
    server = await asyncio.start_unix_server(_handle_connection, path=str(SOCKET_PATH))
    print(f"[Worker {BOT_INDEX}] IPC listening on {SOCKET_PATH}", flush=True)
    async with server:
        await server.serve_forever()


async def _shutdown():
    await bot.close()
    if SOCKET_PATH.exists():
        SOCKET_PATH.unlink()


if __name__ == "__main__":
    try:
        bot.run(TOKEN)
    except KeyboardInterrupt:
        pass

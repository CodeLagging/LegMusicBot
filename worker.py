import asyncio
import json
import math
import os
import random
import re
import sys
import time
import unicodedata
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

# Fire-and-forget tasks are kept here until they finish: asyncio only holds weak references to
# tasks, so an unreferenced task can be garbage-collected mid-run (Python docs, create_task).
_BG_TASKS: set = set()

def _spawn(coro):
    task = asyncio.ensure_future(coro)
    _BG_TASKS.add(task)
    task.add_done_callback(_BG_TASKS.discard)
    return task

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

POP_CHECK       = 3      # top candidates whose YouTube views/likes are looked up (each is a YouTube request)
META_CACHE_TTL  = 24 * 3600   # views/likes barely change; fewer YouTube requests
POP_VIEW_W      = 8      # popularity points per 10x views
POP_LIKE_W      = 4      # popularity points per 10x likes
SP_MIN_COVERAGE = 0.5    # share of the query's words a Spotify match must contain

PLAY_COUNT_MS   = 30_000   # a song counts as "listened" for the algo after this long
HISTORY_LEN     = 25
AUTOPLAY_BATCH  = 5      # autoplay songs queued per refill
AUTOPLAY_QUERY  = "~autoplay"   # search-memory key for how the user reacts to autoplay picks
AUTOPLAY_PER_ARTIST = 2  # variety: at most this many songs by one artist per refill
AUTOPLAY_LABEL  = "Autoplay (Algo)"   # source shown for autoplay picks; also marks them in the queue
AUTOPLAY_NEW_SHARE = 0.7   # chance each autoplay pick is a song the user has never played (still from their taste)
AUTOPLAY_MIX_DEPTH = 30    # songs used from each seed's YouTube Mix (deeper = more new songs to choose from)
# For new songs the only history signal is the artist. At full weight, new songs by artists the
# user already plays won every slot; at this weight about half are (the rest are new artists).
AUTOPLAY_NEW_TASTE_W = 0.15

_groq         = AsyncGroq(api_key=os.environ["GROQ_API_KEY"])
_GROQ_MODEL   = "openai/gpt-oss-20b"
_GROQ_TIMEOUT = 6.0
# gpt-oss is a reasoning model: it thinks before answering, and the thinking counts toward
# max_tokens. With a tiny limit the answer comes back empty, so leave room and keep thinking short.
_GROQ_ARGS = {"max_tokens": 400, "temperature": 0, "extra_body": {"reasoning_effort": "low"}}
AI_PICK_CANDIDATES = 6   # candidates shown to the AI per search
AI_SKIP_MARGIN     = 30  # a top result this far ahead of every rival is picked without the AI
AI_CACHE_TTL       = 6 * 3600


_OFFICIAL_RE = re.compile(
    r"\b(official\s*(?:audio|video|music\s*video|lyric\s*video|visualizer)?)\b",
    re.IGNORECASE,
)

_ALTERED_QUICK = re.compile(
    r"\b(sped[\s_-]*up|slowed|reverb|nightcore|daycore|lofi|lo[\s_-]*fi|"
    r"bass[\s_-]*boost|8d|cover|acoustic|piano|karaoke|instrumental|remix|"
    r"mashup|bootleg|extended|phonk|trap|tiktok|ultra[\s_-]*speed|speed[\s_-]*up)\b",
    re.IGNORECASE,
)

# Words that, in the user's own query, clearly ask for an altered version. Checked before the AI,
# so a Groq timeout can't filter out a version the user asked for by name.
_ALTERED_ASK = re.compile(
    r"\b(sped[\s_-]*up|slowed|reverb|nightcore|daycore|8d|bass[\s_-]*boost(?:ed)?|"
    r"lo[\s_-]*fi|remix|cover|karaoke|instrumental|mashup|tiktok|ultra[\s_-]*speed|speed[\s_-]*up)\b",
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
                **_GROQ_ARGS,
            ),
            timeout=_GROQ_TIMEOUT,
        )
        answer = (r.choices[0].message.content or "").strip().lower()
        if not answer:
            raise RuntimeError("empty answer")
        return answer.startswith("yes")
    except Exception as exc:
        # Fall back to the title rules instead of assuming every title is clean.
        print(f"[Worker {BOT_INDEX}] Groq error ({title!r}): {exc} — using title rules", flush=True)
        return bool(_ALTERED_QUICK.search(title or ""))

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
                **_GROQ_ARGS,
            ),
            timeout=_GROQ_TIMEOUT,
        )
        answer = (r.choices[0].message.content or "").strip().lower()
        if not answer:
            raise RuntimeError("empty answer")
        return answer.startswith("yes")
    except Exception as exc:
        print(f"[Worker {BOT_INDEX}] Groq query-check error: {exc} — using title rules", flush=True)
        return bool(_ALTERED_ASK.search(query or ""))

_PICK_SYSTEM = """Pick which search result a music bot should play. Reply with JSON only:
{"wants_altered": bool, "altered": [numbers], "pick": number}
wants_altered: true only if the search explicitly asks for a non-original version (sped up, slowed,
reverb, nightcore, 8D, bass boosted, lofi, remix, cover, karaoke, instrumental, acoustic, live,
mashup, montagem/edit of the song). Genre, artist, language or mood words don't count.
altered: results that are non-original versions (the above, AI covers, fan edits, compilations,
medleys). Official remixes by the original artist, remasters, radio edits and official audio/lyric
videos are NOT altered.
pick: the result that IS the searched song (same title; never a different song sharing a word).
If not wants_altered, never pick an altered result; prefer the original artist's studio recording,
then more views. If wants_altered, pick that version. Among equal matches prefer ones noted as the
user's. -1 if none fits."""

def _short_views(v: int | None) -> str:
    if v is None:
        return ""
    for div, suf in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if v >= div:
            return f", {v / div:.1f}{suf} views"
    return f", {v} views"

_ai_cache: dict[str, tuple[float, dict]] = {}

def _altered_kinds(text: str) -> set[str]:
    """Which altered-version words appear ('slowed', 'sped up', ...), normalised for comparison."""
    return {re.sub(r"[\s_-]+", " ", m.group(1).lower()) for m in _ALTERED_ASK.finditer(text or "")}

def _obvious_pick(ranked_top: list[tuple[wavelink.Playable, float]], asked: bool,
                  personal: dict[str, tuple[float, str]], query_kinds: set[str] | None = None) -> int | None:
    """0 when the top result is clearly right and the AI call can be skipped, else None.
    Clear = it's the kind of version asked for (original vs altered by title words), and either
    the user's history strongly points at it or it beats every rival of that kind by AI_SKIP_MARGIN."""
    if not ranked_top:
        return None
    t0, s0 = ranked_top[0]
    if bool(_ALTERED_QUICK.search(t0.title or "")) != asked:
        return None
    if asked and query_kinds and not query_kinds <= _altered_kinds(t0.title or ""):
        return None   # asked for "slowed" but the top result is e.g. "sped up": let the AI decide
    if personal.get(_track_key(t0), (0.0, ""))[0] >= PERSONAL_STRONG:
        return 0
    rivals = [sc for t, sc in ranked_top[1:] if bool(_ALTERED_QUICK.search(t.title or "")) == asked]
    if not rivals:
        return 0
    return 0 if s0 - max(rivals) >= AI_SKIP_MARGIN else None

async def _ai_pick_cached(query: str, cands: list[wavelink.Playable],
                          notes: dict[int, str] | None) -> dict | None:
    """_ai_pick, remembered for AI_CACHE_TTL per search text. Stored by song id, not position, so
    it still applies when the result list shifts a little (Spotify/YouTube answers vary)."""
    ids  = [t.identifier or t.uri or t.title for t in cands]
    key  = (_query_norm(query) or query).lower()
    if notes:
        # A decision shaped by one user's history must not be reused for another user.
        key += "|" + "|".join(f"{ids[i]}={n}" for i, n in sorted(notes.items()) if i < len(ids))
    hit  = _ai_cache.get(key)
    if hit and time.time() - hit[0] < AI_CACHE_TTL and hit[1]["pick_id"] in ids:
        d = hit[1]
        print(f"[Worker {BOT_INDEX}] AI decision from cache", flush=True)
        return {"wants_altered": d["wants_altered"], "pick": ids.index(d["pick_id"]),
                "altered": {i for i, x in enumerate(ids) if x in d["altered_ids"]}}
    decision = await _ai_pick(query, cands, notes)
    if decision is not None and 0 <= decision["pick"] < len(ids):
        _ai_cache[key] = (time.time(), {"wants_altered": decision["wants_altered"],
                                        "pick_id": ids[decision["pick"]],
                                        "altered_ids": {ids[i] for i in decision["altered"] if 0 <= i < len(ids)}})
        if len(_ai_cache) > 500:
            for k, _ in sorted(_ai_cache.items(), key=lambda kv: kv[1][0])[:100]:
                _ai_cache.pop(k, None)
    return decision

async def _ai_pick(query: str, cands: list[wavelink.Playable],
                   notes: dict[int, str] | None = None) -> dict | None:
    """One AI call per search: did the user ask for an altered version, which results are altered,
    and which result is the song they mean. None when Groq fails (title rules are used instead)."""
    lines = []
    for i, t in enumerate(cands):
        views = (_meta_cache.get(t.uri or "", (0, None))[1] or {}).get("view_count")
        length = f"{(t.length or 0) // 60000}:{(t.length or 0) // 1000 % 60:02d}"
        lines.append(f"{i}. {(t.title or '')[:70]} — {(t.author or '')[:40]} ({length}"
                     + _short_views(views)
                     + (f"; {notes[i]}" if notes and i in notes else "") + ")")
    for attempt in (1, 2):
        decision = await _ai_pick_once(query, lines, len(cands), last=(attempt == 2))
        if decision is not _RETRY:
            return decision
    return None

_RETRY = object()

async def _ai_pick_once(query: str, lines: list[str], n: int, last: bool):
    try:
        r = await asyncio.wait_for(
            _groq.chat.completions.create(
                model=_GROQ_MODEL,
                messages=[{"role": "system", "content": _PICK_SYSTEM},
                          {"role": "user", "content": f'Search: "{query}"\nResults:\n' + "\n".join(lines)}],
                response_format={"type": "json_object"},
                **{**_GROQ_ARGS, "max_tokens": 700},
            ),
            timeout=8.0,
        )
        data = json.loads(r.choices[0].message.content or "")
        pick = int(data.get("pick", -1))
        altered = {int(i) for i in data.get("altered", []) if 0 <= int(i) < n}
        return {"wants_altered": bool(data.get("wants_altered")), "altered": altered,
                "pick": pick if 0 <= pick < n else -1}
    except Exception as exc:
        if not last and ("json" in str(exc).lower() or isinstance(exc, json.JSONDecodeError)):
            return _RETRY   # Groq occasionally rejects its own JSON; one retry usually works
        print(f"[Worker {BOT_INDEX}] AI pick failed: {str(exc)[:120]} — using title rules", flush=True)
        return None

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
                max_tokens=500, temperature=0.8,
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

def _fold(text: str) -> str:
    """Lower-case and strip accents, so 'Tântrico' matches 'tantrico'."""
    nfkd = unicodedata.normalize("NFKD", text or "")
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower()

def _words(text: str) -> list[str]:
    return [w for w in re.split(r"\W+", _fold(text)) if len(w) > 1]

def _track_score(track: wavelink.Playable, query: str = "",
                 ref: wavelink.Playable | None = None) -> int:
    """Relevance of a search candidate to the query (popularity is added separately)."""
    score   = 0
    title   = _fold(track.title or "")
    author  = _fold(track.author or "")
    q_fold  = _fold(query)
    words   = _words(query)

    for w in words:
        if w in title:  score += 40
        if w in author: score += 20


    if ref is not None:
        ref_words = _words(f"{ref.title or ''} {ref.author or ''}")
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

    # Medleys ("A / B / C / D") and long compilations are rarely what a song search means.
    if (track.title or "").count("/") >= 2:
        score -= 60
    if (track.length or 0) > 10 * 60_000 and not re.search(r"\b(mix|full|album|hour|live)\b", q_fold):
        score -= 30
    # Mashups ("Song A x Song B") and snippets ("best part") are a different thing than the song.
    if re.search(r"\s[x×]\s", title) and not re.search(r"\s[x×]\s", q_fold):
        score -= 50
    if re.search(r"\bbest[\s_-]*part\b", title) and "best part" not in q_fold:
        score -= 30

    return score

_taste_cache: dict[tuple[int, int], tuple[float, dict]] = {}

PERSONAL_HALF_LIFE_DAYS = 30   # personal boosts fade with time since the last listen
PERSONAL_STRONG = 40           # personal score that overrides the AI's generic pick

_CHANNEL_SUFFIX_RE = re.compile(r"(\s*-\s*topic|\s*vevo|\s+official|\s+music|\s+oficial)$")

def _artist_key(author: str) -> str:
    """'Sxilwix - Topic', 'SxilwixVEVO' and 'Sxilwix' are the same artist."""
    a = _fold(author or "").strip()
    for _ in range(2):
        a = _CHANNEL_SUFFIX_RE.sub("", a).strip()
    return a

def _song_id(title: str, author: str) -> tuple[str, str]:
    return _norm_title(_fold(title or "")), _artist_key(author)

def _recency(ts: float) -> float:
    days = max(0.0, (time.time() - (ts or 0)) / 86400)
    return 0.5 + 0.5 * math.exp(-days / PERSONAL_HALF_LIFE_DAYS)

async def _taste(guild_id: int, user_id: int) -> dict | None:
    """The requester's listening summarised for search: saved uploads, songs, titles, artists
    and search memory. Cached for 60 s and dropped whenever they finish or skip a song."""
    key = (guild_id, user_id)
    hit = _taste_cache.get(key)
    if hit and time.monotonic() - hit[0] < 60:
        return hit[1]
    try:
        rows    = await asyncio.to_thread(db.user_tracks, guild_id, user_id)
        queries = await asyncio.to_thread(db.user_queries, guild_id, user_id)
    except Exception as exc:
        print(f"[Worker {BOT_INDEX}] Could not read saved songs: {exc}", flush=True)
        return None
    taste = None
    if rows or queries:
        ids, songs, titles, artists, artist_skips = {}, {}, {}, {}, {}
        for r in rows:
            score = r["plays"] - r["skips"]   # for search, one quick skip cancels one listen
            if r["yt_id"]:
                ids[r["yt_id"]] = (score, r["last_played"])
            sid = _song_id(r["title"], r["author"])
            old = songs.get(sid, (0.0, 0.0))
            songs[sid] = (old[0] + score, max(old[1], r["last_played"]))
            titles.setdefault(sid[0], set()).add(sid[1])
            artists[sid[1]] = artists.get(sid[1], 0) + max(r["plays"], 0)
            artist_skips[sid[1]] = artist_skips.get(sid[1], 0) + r["skips"]
        by_query: dict[str, list[dict]] = {}
        for q in queries:
            by_query.setdefault(q["query"], []).append(q)
        taste = {"ids": ids, "songs": songs, "titles": titles, "artists": artists, "artist_skips": artist_skips,
                 "total": max(1, sum(artists.values())), "queries": by_query, "rows": rows}
    _taste_cache[key] = (time.monotonic(), taste)
    return taste

def _personal(track: wavelink.Playable, taste: dict | None, query_norm: str | None) -> tuple[float, str]:
    """How much this user's own history favours a search result, plus a note for the AI.

    Signals, strongest first:
      - search memory: what they listened to (or skipped) the last times they searched this text
      - the exact upload they've listened to before
      - the same song (title + artist) from another upload
      - they know a song with this title by a different artist -> this one is probably not it
      - how big a share of their listening this artist is
    Listens raise it, quick skips lower it, and it fades over about a month without listening."""
    if not taste:
        return 0.0, ""
    sid = _song_id(track.title, track.author)
    yt  = track.identifier or ""
    bonus, notes = 0.0, []

    for q in taste["queries"].get(query_norm or "", []):
        same = (yt and q["yt_id"] == yt) or _song_id(q["title"], q["author"]) == sid
        if not same:
            continue
        net = q["listens"] - 1.5 * q["skips"]
        if net > 0:
            bonus += (60 + 15 * math.log2(1 + net)) * _recency(q["last_used"])
            notes.append(f"the user picked this for the same search before ({q['listens']} listens)")
        elif q["skips"]:
            bonus -= 40
            notes.append("the user skipped this for the same search before")

    saved = taste["ids"].get(yt)
    song  = taste["songs"].get(sid)
    if saved:
        score, last = saved
        bonus += (35 + 10 * math.log2(1 + score)) * _recency(last) if score > 0 else -20
        notes.append("the user has listened to this exact upload" if score > 0 else "the user often skips this upload")
    elif song and song[0] > 0:
        bonus += (30 + 8 * math.log2(1 + song[0])) * _recency(song[1])
        notes.append("the user listens to this song (another upload)")
    elif sid[0] in taste["titles"] and sid[1] not in taste["titles"][sid[0]]:
        bonus -= 15   # they know a same-titled song by someone else; this is probably a different one

    artist_plays = taste["artists"].get(sid[1], 0)
    if artist_plays:
        share = artist_plays / taste["total"]
        bonus += min(25.0, 60 * share + 4 * math.log2(1 + artist_plays))
        if artist_plays >= 2:
            notes.append(f"an artist the user plays a lot ({artist_plays} plays)")
    return min(bonus, 140.0), "; ".join(notes)

async def _remembered_candidates(taste: dict, query_norm: str | None,
                                 existing: list[wavelink.Playable]) -> list[wavelink.Playable]:
    """Uploads from the user's history that fit this search but aren't among the results:
    what they listened to for this exact search, and saved songs whose title has every query word."""
    have = {t.identifier for t in existing}
    want: dict[str, float] = {}
    for q in taste["queries"].get(query_norm or "", []):
        if q["yt_id"] and q["listens"] - 1.5 * q["skips"] > 0:
            want[q["yt_id"]] = want.get(q["yt_id"], 0) + 100 + q["listens"]
    q_words = (query_norm or "").split()
    if q_words:
        for r in taste["rows"]:
            title = _fold(r["title"])
            if r["yt_id"] and r["plays"] - 0.5 * r["skips"] > 0 and all(w in title for w in q_words):
                want[r["yt_id"]] = want.get(r["yt_id"], 0) + r["plays"]
    picks = [yt for yt, _ in sorted(want.items(), key=lambda kv: -kv[1]) if yt not in have][:3]
    if not picks:
        return []
    async def load(yt: str):
        try:
            res = await wavelink.Pool.fetch_tracks(f"https://www.youtube.com/watch?v={yt}")
            tracks = res.tracks if isinstance(res, wavelink.Playlist) else list(res or [])
            return tracks[0] if tracks else None
        except Exception:
            return None
    found = [t for t in await asyncio.gather(*[load(yt) for yt in picks]) if t]
    for t in found:
        print(f"[Worker {BOT_INDEX}]   + from your history: {t.title!r} by {t.author!r}", flush=True)
    return found

def _popularity(meta: dict | None) -> float:
    if not meta:
        return 0.0
    views = meta.get("view_count") or 0
    likes = meta.get("like_count") or 0
    return POP_VIEW_W * math.log10(views + 1) + POP_LIKE_W * math.log10(likes + 1)


COLOUR = discord.Colour.from_str("#5865F2")

def _track_embed(track, action, source_label, queue_pos=0, search_path=""):
    title  = track.title  or "Unknown Title"
    author = track.author or "Unknown Artist"
    uri    = track.uri    or ""
    desc   = f"**[{_link_text(title)}]({uri})**\n{author}" if uri else f"**{title}**\n{author}"
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
    return embed

def _link_text(text: str) -> str:
    """Square brackets in a title (e.g. "[Official Video]") would end the Markdown link early."""
    return (text or "").replace("[", "\\[").replace("]", "\\]")

def _now_playing_embed(track, source_label, search_path=""):
    return _track_embed(track, "playing", source_label, search_path=search_path)


_STATUS_DELETE_DELAY = 10

async def _send_status(channel: discord.TextChannel, embed: discord.Embed):
    try:
        msg = await channel.send(embed=embed)
        _spawn(_delete_after(msg, _STATUS_DELETE_DELAY))
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

class QueueItem(tuple):
    """(track, source_label, search_path, requester_id) plus a stable `qid`.
    The control panel refers to songs by qid, not position: positions shift whenever a song
    ends or autoplay adds picks, which made Jump To / Remove hit the song after the one chosen."""
    _next_qid = 0

    def __new__(cls, track, label, path, requester, query: str | None = None):
        item = super().__new__(cls, (track, label, path, requester))
        QueueItem._next_qid += 1
        item.qid = QueueItem._next_qid
        item.query = query   # the search that found it, for the requester's search memory
        return item

def _queue_pos(sess: "Session", qid: int) -> int | None:
    return next((i for i, q in enumerate(sess.queue) if q.qid == qid), None)

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
        self.normalize    = False   # LavaDSPX normalization filter (also applies to shutdown.mp3)
        # False when playback was started with a private (ephemeral) command: track-change
        # messages are normal channel posts and can't be private, so they're not posted at all.
        self.announce     = True
        self.loop         = False
        self.autoplay     = False
        self.autoplay_source = "session"    # "session" = seeds from what was played, "algo" = saved songs
        self.autoplay_user: int | None = None
        self.autoplay_task: asyncio.Task | None = None
        self.fail_streak  = 0
        self.current_info: dict | None = None
        self.current_meta: dict | None = None
        self.current_requester: int | None = None
        self.current_query: str | None = None   # normalised search that led to the playing track
        self.idle_task: asyncio.Task | None = None
        self.auto_paused  = False
        self.connecting   = False
        self.closing      = False
        self.drain_state: str | None = None   # None, "finishing" (current song) or "message" (shutdown.mp3)
        self.created      = time.monotonic()
        self.stalled      = 0      # watchdog passes in a row with songs queued but nothing playing
        self.mix_cache: dict[str, list] = {}   # seed YouTube id -> its Mix, reused across refills
        self.ai_reserve: list[str] = []        # unused AI suggestions, used before asking Groq again
        self.history: deque[dict] = deque(maxlen=HISTORY_LEN)
        # One song change at a time. wavelink only reports "playing" once a song is sent to
        # Lavalink, so a /play landing while another song was still resolving or connecting
        # started too, and one replaced the other (that song was lost).
        self.play_lock = asyncio.Lock()

    def snapshot(self) -> dict:
        return {
            "channel_id":      self.channel_id,
            "controller_id":   self.controller_id,
            "mode":            self.mode,
            "autoplay":        self.autoplay,
            "text_channel_id": self.text_channel.id if self.text_channel else None,
        }

sessions: dict[int, Session] = {}

# Graceful restart: set by the controller's "drain" op. While draining no new playback can start.
_draining = False
_shutdown_url: str | None = None
SHUTDOWN_MESSAGE_MAX = 120   # seconds; a session ends even if the message never reports finishing


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
    _spawn(run())


_url_cache: dict[str, tuple[float, str]] = {}
_inflight: dict[str, asyncio.Future] = {}

async def _kill_proc(proc) -> None:
    """Kill a timed-out subprocess and reap it (a killed but never-awaited process lingers)."""
    try:
        proc.kill()
    except ProcessLookupError:
        pass
    try:
        await asyncio.wait_for(proc.wait(), timeout=5)
    except Exception:
        pass

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
        await _kill_proc(proc)
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

_meta_cache: dict[str, tuple[float, dict | None]] = {}
_meta_inflight: dict[str, asyncio.Future] = {}

async def _ytdlp_meta_run(video_url: str) -> dict | None:
    try:
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "yt_dlp",
            "-j", "-f", "bestaudio/best",
            "--no-playlist", "--no-warnings", "--socket-timeout", "10",
            video_url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=YTDLP_TIMEOUT)
    except asyncio.TimeoutError:
        await _kill_proc(proc)
        return None
    except Exception as exc:
        print(f"[Worker {BOT_INDEX}] yt-dlp metadata failed: {exc}", flush=True)
        return None
    if proc.returncode != 0:
        return None
    try:
        info = json.loads(out)
    except json.JSONDecodeError:
        return None
    return {"view_count": info.get("view_count"), "like_count": info.get("like_count"),
            "url": info.get("url")}

async def _yt_meta(video_url: str) -> dict | None:
    """Views, likes and the direct stream URL of a YouTube video in one yt-dlp call.
    The stream URL goes into the resolver cache, so the winning track starts without a second lookup."""
    hit = _meta_cache.get(video_url)
    if hit and time.time() - hit[0] < META_CACHE_TTL:
        return hit[1]
    fut = _meta_inflight.get(video_url)
    if fut is None:
        fut = asyncio.ensure_future(_ytdlp_meta_run(video_url))
        _meta_inflight[video_url] = fut
    try:
        meta = await fut
    finally:
        if _meta_inflight.get(video_url) is fut:
            _meta_inflight.pop(video_url, None)
    if meta:   # a failed lookup (e.g. YouTube's bot check) is retried next time, not cached
        _meta_cache[video_url] = (time.time(), meta)
    if len(_meta_cache) > 2000:
        for k, _ in sorted(_meta_cache.items(), key=lambda kv: kv[1][0])[:500]:
            _meta_cache.pop(k, None)
    if meta and meta.get("url"):
        _url_cache[video_url] = (time.time(), meta["url"])
    return meta

async def _rank_by_popularity(scored: list[tuple[wavelink.Playable, int]],
                              skip_altered: bool = False) -> list[tuple[wavelink.Playable, float]]:
    """Adds YouTube popularity to the most relevant candidates and re-sorts.
    Relevance still dominates: popularity only separates candidates that match about equally well.
    With skip_altered, obviously altered titles don't use up the popularity lookups."""
    scored = sorted(scored, key=lambda x: x[1], reverse=True)
    pool   = [(t, rel) for t, rel in scored
              if _YT_RE.search(t.uri or "") and not (skip_altered and _ALTERED_QUICK.search(t.title or ""))]
    top    = pool[:POP_CHECK]
    metas  = await asyncio.gather(*[_yt_meta(t.uri) for t, _ in top])
    final: dict[str, float] = {}
    for (t, rel), meta in zip(top, metas):
        pop = _popularity(meta)
        final[_track_key(t)] = rel + pop
        views = (meta or {}).get("view_count")
        likes = (meta or {}).get("like_count")
        print(f"[Worker {BOT_INDEX}]   {rel:6.1f} + pop {pop:5.1f} = {rel + pop:6.1f}: "
              f"{t.title!r} by {t.author!r} ({views or 0:,} views, {likes or 0:,} likes)", flush=True)
    ranked = [(t, final.get(_track_key(t), float(rel))) for t, rel in scored]
    ranked.sort(key=lambda x: x[1], reverse=True)
    return ranked

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
        # Don't map an original Spotify/Apple Music song onto a sped-up/remix upload.
        if not _ALTERED_QUICK.search(track.title or ""):
            cands = [t for t in cands if not _ALTERED_QUICK.search(t.title or "")] or cands
        src = max(cands, key=lambda t: _track_score(t, q, track))
        uri = src.uri or ""

    if not _YT_RE.search(uri):
        return src, track, None

    # 1) cached/fresh yt-dlp stream URL, 2) a forced-fresh URL (cached ones can be refused by
    #    YouTube), 3) Lavalink's own YouTube plugin. Only then give up on this video.
    for attempt in ("cached", "fresh", "plugin"):
        if attempt == "fresh":
            _url_cache.pop(uri, None)
            _meta_cache.pop(uri, None)
        target = uri if attempt == "plugin" else await _ytdlp_url(uri)
        if not target:
            continue
        try:
            res = await wavelink.Pool.fetch_tracks(target)
            tracks = res.tracks if isinstance(res, wavelink.Playlist) else list(res)
        except Exception as exc:
            print(f"[Worker {BOT_INDEX}] Lavalink could not load {attempt} stream for {src.title!r}: "
                  f"{str(exc).splitlines()[0][:100]}", flush=True)
            continue
        if tracks:
            if attempt != "cached":
                print(f"[Worker {BOT_INDEX}] Loaded {src.title!r} via {attempt} stream", flush=True)
            return tracks[0], track, src
    return None

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
        _spawn(_ytdlp_url(uri))

def _query_norm(query: str | None) -> str | None:
    """Search text as stored in search memory: no prefix, accents or punctuation. URLs aren't stored."""
    if not query or query.strip().startswith(("http://", "https://")):
        return None
    if query.startswith("~"):
        return query   # internal keys like "~autoplay"
    return " ".join(_words(_SEARCH_PREFIX_RE.sub("", query))) or None

def _set_current(sess: Session, origin: wavelink.Playable,
                 yt_src: wavelink.Playable | None, requester: int | None,
                 query: str | None = None) -> None:
    meta = _track_meta(origin, yt_src)
    sess.current_info      = {"title": origin.title or "Unknown", "author": origin.author or ""}
    sess.current_meta      = meta
    sess.current_requester = requester
    sess.current_query     = _query_norm(query)
    sess.history.append(meta)

async def _set_vc_status(guild_id: int, channel_id: int, title: str | None) -> None:
    """Show the song title as the voice channel status (like Rythm). Needs 'Set Voice Channel Status'."""
    guild = bot.get_guild(guild_id)
    vc = guild.get_channel(channel_id) if guild else None
    if not isinstance(vc, discord.VoiceChannel):
        return
    try:
        await vc.edit(status=(f"🎵Playing - {title}"[:500] if title else None))
    except discord.Forbidden:
        print(f"[Worker {BOT_INDEX}] No permission to set voice status in guild {guild_id}", flush=True)
    except Exception as exc:
        print(f"[Worker {BOT_INDEX}] Voice status update failed: {exc}", flush=True)

def _after_start(sess: Session) -> None:
    if sess.current_info:
        _spawn(_set_vc_status(sess.guild_id, sess.channel_id, sess.current_info["title"]))
    if _humans_in_vc(sess):
        _cancel_idle_timer(sess)
    else:
        # Nobody is listening (everyone left while the song was changing, or a fixed channel
        # nobody is in): keep the idle timer, or the bot plays to an empty channel forever.
        _start_idle_timer(sess)
    _prefetch_next(sess)
    if sess.autoplay and not sess.queue:
        _kick_autoplay(sess)

async def _play(sess: Session, player: wavelink.Player, track: wavelink.Playable,
                requester: int | None, query: str | None = None) -> wavelink.Playable:
    """Plays a track (or, if it can't be loaded, one of its same-song alternates).
    Returns the track that is actually playing, so messages never show a different song."""
    alternates = [t for t, _, _ in sess.fallbacks.get(_track_key(track), [])]
    candidates = [track] + alternates

    for i, cand in enumerate(candidates[:MAX_RESOLVE_ATTEMPTS]):
        resolved = await _resolve(cand)
        if resolved is None:
            print(f"[Worker {BOT_INDEX}] Could not resolve {cand.title!r} — trying next candidate", flush=True)
            continue
        playable, origin, yt_src = resolved
        try:
            await player.play(playable)
        except Exception as exc:
            print(f"[Worker {BOT_INDEX}] Lavalink refused to play {cand.title!r}: {exc} — trying next candidate", flush=True)
            continue
        sess.origin[_track_key(playable)] = origin
        _set_current(sess, origin, yt_src, requester, query)
        if i > 0:

            rest = sess.fallbacks.get(_track_key(track), [])[i:]
            if rest:
                sess.fallbacks[_track_key(origin)] = rest
        _after_start(sess)
        if i > 0:
            print(f"[Worker {BOT_INDEX}] Playing alternate {origin.title!r} by {origin.author!r} "
                  f"instead of {track.title!r}", flush=True)
        return origin


    print(f"[Worker {BOT_INDEX}] Resolve failed for {track.title!r} — passing to Lavalink as-is", flush=True)
    await player.play(track)   # raises if this fails too; callers move on to the next song
    sess.origin[_track_key(track)] = track
    _set_current(sess, track, None, requester, query)
    _after_start(sess)
    return track

def _account(sess: Session, played_ms: int | None = None,
             finished: bool = False, skipped: bool = False) -> None:
    """Feed the outgoing track into the requester's algo: a listen, a quick skip, or nothing."""
    meta, uid, query = sess.current_meta, sess.current_requester, sess.current_query
    sess.current_meta = sess.current_query = None
    if not meta or not uid:
        return
    max_kb = appsettings.get_int("algo_max_kb")
    if finished or (played_ms or 0) >= PLAY_COUNT_MS:
        _bg(db.record_play, sess.guild_id, uid, meta, max_kb)
        if query:
            _bg(db.record_query, sess.guild_id, uid, query, meta, True, max_kb)
    elif skipped:
        _bg(db.record_skip, sess.guild_id, uid, meta["key"])
        if query:   # this search picked something the user didn't want
            _bg(db.record_query, sess.guild_id, uid, query, meta, False, max_kb)
    _taste_cache.pop((sess.guild_id, uid), None)

def _can_post(sess: Session) -> bool:
    return bool(sess.text_channel) and sess.announce and not sess.muted

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
    async with sess.play_lock:   # see Session.play_lock
        await _next_song(sess, player, announce)

async def _next_song(sess: Session, player: wavelink.Player, announce: bool) -> None:
    if sess.drain_state == "finishing":
        await _play_shutdown_message(sess, player)   # the last song is done; nothing else plays
        return
    if not sess.queue and sess.autoplay:
        await _autoplay_fill(sess)
    if sessions.get(sess.guild_id) is not sess:
        return
    while sess.queue and sessions.get(sess.guild_id) is sess:
        item = sess.queue.pop(0)
        nxt, nxt_label, nxt_path, req = item
        try:
            played = await _play(sess, player, nxt, req, item.query)
        except Exception as exc:
            # This song can't be played at all: skip it instead of leaving the session stuck.
            print(f"[Worker {BOT_INDEX}] Skipping {nxt.title!r}: {exc}", flush=True)
            sess.fail_streak += 1
            if sess.fail_streak >= MAX_FAIL_STREAK:
                print(f"[Worker {BOT_INDEX}] {sess.fail_streak} songs in a row failed — stopping", flush=True)
                sess.queue.clear()
                sess.autoplay = False
                break
            if not sess.queue and sess.autoplay:
                await _autoplay_fill(sess)
            continue
        if announce and _can_post(sess):
            await _send_status(sess.text_channel, _now_playing_embed(played, nxt_label, nxt_path))
        return
    if sessions.get(sess.guild_id) is not sess:
        return
    sess.current_info = None
    _spawn(_set_vc_status(sess.guild_id, sess.channel_id, None))
    _start_idle_timer(sess)


async def _advance_safely(sess: Session, player: wavelink.Player, announce: bool = True) -> None:
    """_advance for event handlers: an error is logged and the session goes idle (it will leave
    after the idle timeout) instead of the handler dying and the session hanging."""
    try:
        await _advance(sess, player, announce)
    except Exception as exc:
        print(f"[Worker {BOT_INDEX}] Could not continue playback in guild {sess.guild_id}: {exc}", flush=True)
        if sessions.get(sess.guild_id) is sess:
            _start_idle_timer(sess)


# ── normalization ────────────────────────────────────────────────────────────

# LavaDSPX "normalization": tames peaks above maxAmplitude and adapts quickly to each song,
# so loud and quiet songs end up at a similar level. Needs the LavaDSPX plugin in Lavalink.
NORMALIZE_SETTINGS = {"maxAmplitude": 0.75, "adaptive": True}

async def _apply_normalize(player: wavelink.Player, on: bool) -> None:
    filters: wavelink.Filters = player.filters
    if on:
        filters.plugin_filters.set(normalization=dict(NORMALIZE_SETTINGS))
    else:
        filters.plugin_filters.reset()
    await player.set_filters(filters)


# ── graceful restart ─────────────────────────────────────────────────────────

async def _play_shutdown_message(sess: Session, player: wavelink.Player | None) -> None:
    """Play shutdown.mp3 (served by the controller) and leave when it ends.
    Without the file, or if it can't load, the session just ends."""
    if sess.drain_state == "message":
        return
    sess.drain_state = "message"
    sess.queue.clear()
    sess.autoplay = sess.loop = False
    _cancel_idle_timer(sess)
    if sess.autoplay_task and not sess.autoplay_task.done():
        sess.autoplay_task.cancel()
    if player and player.current:
        _account(sess, player.position)
    track = None
    if _shutdown_url and player:
        try:
            res = await wavelink.Pool.fetch_tracks(_shutdown_url)
            tracks = res.tracks if isinstance(res, wavelink.Playlist) else list(res or [])
            track = tracks[0] if tracks else None
        except Exception as exc:
            print(f"[Worker {BOT_INDEX}] Could not load shutdown message: {str(exc).splitlines()[0][:100]}", flush=True)
    if track is None:
        await _end_session(sess.guild_id)
        return
    try:
        if player.paused:
            await player.pause(False)
        await player.play(track)
        sess.current_info = {"title": "Restarting", "author": ""}
        _spawn(_set_vc_status(sess.guild_id, sess.channel_id, None))
        print(f"[Worker {BOT_INDEX}] Playing shutdown message in guild {sess.guild_id}", flush=True)
        _spawn(_end_later(sess, SHUTDOWN_MESSAGE_MAX))
    except Exception as exc:
        print(f"[Worker {BOT_INDEX}] Shutdown message failed: {exc}", flush=True)
        await _end_session(sess.guild_id)

async def _end_later(sess: Session, delay: float) -> None:
    await asyncio.sleep(delay)
    if sessions.get(sess.guild_id) is sess:
        await _end_session(sess.guild_id)


# ── autoplay ─────────────────────────────────────────────────────────────────

def _same_song(a: wavelink.Playable, b: wavelink.Playable) -> bool:
    ta, tb = _norm_title(_fold(a.title or "")), _norm_title(_fold(b.title or ""))
    if not ta or not tb or (ta not in tb and tb not in ta):
        return False
    same_artist = _artist_key(a.author) == _artist_key(b.author)
    return same_artist or abs((a.length or 0) - (b.length or 0)) <= 8000

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
    """Saved songs as autoplay seeds: favours ones played more and more recently."""
    pool, out = list(rows), []
    while pool and len(out) < k:
        weights = [max(r["score"], 0.5) * _recency(r.get("last_played", 0)) for r in pool]
        r = random.choices(pool, weights=weights, k=1)[0]
        out.append(r)
        pool.remove(r)
    return out

async def _autoplay_seeds(sess: Session) -> list[dict]:
    n = max(1, appsettings.get_int("autoplay_seed_count"))
    if sess.autoplay_source == "algo" and sess.autoplay_user:
        rows = await asyncio.to_thread(db.user_tracks, sess.guild_id, sess.autoplay_user, 200)
        return _weighted_pick(rows, n)
    recent = list(sess.history)[-n:]
    recent.reverse()   # most recent first
    return recent

async def _mix_cached(sess: Session, seed: dict) -> list[wavelink.Playable]:
    yt_id = await _yt_id_for(seed)
    if not yt_id:
        return []
    if yt_id not in sess.mix_cache:
        seed = dict(seed, yt_id=yt_id)
        sess.mix_cache[yt_id] = await _mix_for(seed)
    return sess.mix_cache[yt_id]

def _autoplay_score(t: wavelink.Playable, position: int, seed_weight: float, appearances: int,
                    taste: dict | None, taste_w: float = 0.6) -> float | None:
    """How good an autoplay pick is for this user. None = don't play it.
    taste_w: how much their history (artists, known songs) counts next to how related the song is."""
    score = max(0.0, 25 - position) * seed_weight      # earlier in a Mix = more closely related
    score += 12 * (appearances - 1)                     # related to several seeds = strong signal
    if taste:
        for q in taste["queries"].get(AUTOPLAY_QUERY, []):
            if q["yt_id"] and q["yt_id"] == t.identifier:
                net = q["listens"] - 1.5 * q["skips"]
                if net < 0:
                    return None                         # skipped when autoplay played it before
                score += 15 * min(net, 3)
        bonus, _ = _personal(t, taste, None)            # artist share, known songs, same-title rules
        score += taste_w * bonus
        artist = _artist_key(t.author)
        skips, plays = taste["artist_skips"].get(artist, 0), taste["artists"].get(artist, 0)
        if skips >= 2 and skips > plays:
            score -= 25                                 # an artist this user keeps skipping
    return score + random.uniform(0, 8)                 # a little variety between refills

def _known_song(t: wavelink.Playable, taste: dict | None) -> bool:
    """The user already has this song in their algo: this upload, or another upload of it."""
    if not taste:
        return False
    sid = _song_id(t.title, t.author)
    if (t.identifier or "") in taste["ids"] or sid in taste["songs"]:
        return True
    # Re-uploads put the artist in the title ("Sxilwix - Bella Noche (Lyrics)" by a lyrics channel).
    text = f" {sid[0]} "
    return any(f" {title} " in text and any(a and f" {a} " in text for a in artists)
               for title, artists in taste["titles"].items() if title)

async def _recommend(sess: Session, count: int) -> list[tuple[wavelink.Playable, str, str]]:
    """Autoplay picks. Each one has an AUTOPLAY_NEW_SHARE chance to be a song the user has never
    played, otherwise it's one they already like. Both kinds come from their taste: the Mixes of
    their songs, ranked by how related a song is, how many of their songs lead to it, the artists
    they play, and what they skipped before.
    Before, songs they knew got the full history bonus and won almost every slot, so autoplay
    mostly replayed what was already in the algo."""
    seeds = await _autoplay_seeds(sess)
    if not seeds:
        return []
    ids, titles = _excluded_sets(sess)
    taste = await _taste(sess.guild_id, sess.autoplay_user or sess.controller_id)
    out: list[tuple[wavelink.Playable, str, str]] = []
    per_artist: dict[str, int] = {}

    def take(t: wavelink.Playable, path: str, check: bool = True) -> bool:
        if check and not _usable_rec(t, ids, titles):
            return False
        artist = _artist_key(t.author)
        if per_artist.get(artist, 0) >= AUTOPLAY_PER_ARTIST:
            return False
        per_artist[artist] = per_artist.get(artist, 0) + 1
        ids.add(t.identifier or "")
        titles.add(_norm_title(t.title or ""))
        out.append((t, AUTOPLAY_LABEL, path))
        return True

    # One pool from every seed's Mix (cached per session), ranked for this user, then split into
    # songs they've never played and songs they already know.
    mixes = await asyncio.gather(*[_mix_cached(sess, seed) for seed in seeds])
    pool: dict[str, dict] = {}
    for rank, (seed, mix) in enumerate(zip(seeds, mixes)):
        weight = 1.0 - 0.1 * rank                        # most recent / strongest seed counts most
        for pos, t in enumerate(mix[:AUTOPLAY_MIX_DEPTH]):
            if not _usable_rec(t, ids, titles):
                continue
            entry = pool.setdefault(t.identifier or t.uri, {"t": t, "pos": pos, "w": weight, "n": 0,
                                                            "seed": seed.get("title", "")})
            entry["n"] += 1
            entry["pos"], entry["w"] = min(entry["pos"], pos), max(entry["w"], weight)
    fresh, known = [], []
    for e in pool.values():
        is_known = _known_song(e["t"], taste)
        sc = _autoplay_score(e["t"], e["pos"], e["w"], e["n"], taste,
                             taste_w=0.6 if is_known else AUTOPLAY_NEW_TASTE_W)
        if sc is not None:
            (known if is_known else fresh).append((sc, e))
    ranked = {kind: [(e["t"], f"Mix · {e['seed'][:40]}") for _, e in sorted(lst, key=lambda x: -x[0])]
              for kind, lst in (("new", fresh), ("known", known))}
    # /autoplay: familiar picks start with the seeds themselves (saved songs drawn at random,
    # weighted by plays), so the familiar share isn't always the same few top songs.
    saved = [s for s in seeds if s.get("yt_id")] if sess.autoplay_source == "algo" else []

    async def fill(kind: str) -> bool:
        if kind == "known":
            while saved:
                yt = saved.pop(0)["yt_id"]
                if yt in ids:
                    continue
                try:
                    res = await wavelink.Pool.fetch_tracks(f"https://www.youtube.com/watch?v={yt}")
                    tracks = res.tracks if isinstance(res, wavelink.Playlist) else list(res or [])
                except Exception:
                    tracks = []
                # Their own song as saved: altered versions they like are fine here.
                if tracks and _norm_title(tracks[0].title or "") not in titles \
                        and take(tracks[0], "Your saved songs", check=False):
                    return True
        lst = ranked[kind]
        while lst:
            t, path = lst.pop(0)
            if take(t, path):
                return True
        return False

    kinds = ["new" if random.random() < AUTOPLAY_NEW_SHARE else "known" for _ in range(count)]
    for kind in kinds:
        # Short on one kind (e.g. a new listener knows nothing yet): the other kind fills the slot.
        if not (await fill(kind) or await fill("known" if kind == "new" else "new")):
            break

    if len(out) < count:
        # Fallback: AI suggestions (new songs only). Ask for a batch once and keep the rest for later refills.
        if not sess.ai_reserve:
            print(f"[Worker {BOT_INDEX}] Not enough Mix results — asking Groq for a batch of suggestions", flush=True)
            sess.ai_reserve = await _groq_similar(seeds, count=10)
        while sess.ai_reserve and len(out) < count:
            line = sess.ai_reserve.pop(0)
            t, _, _ = await _search(line, "sp", None, use_ai=False)   # no extra Groq call per song
            if t and not _known_song(t, taste):
                take(t, "AI pick")
    new = sum(1 for t, _, _ in out if not _known_song(t, taste))
    print(f"[Worker {BOT_INDEX}] Autoplay picks: {new} new to the user, {len(out) - new} they know "
          f"(pool: {len(fresh)} new, {len(known)} known)", flush=True)
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
        sess.queue.append(QueueItem(t, lbl, path, requester, AUTOPLAY_QUERY))
    if recs:
        print(f"[Worker {BOT_INDEX}] Autoplay queued: {', '.join(repr(t.title) for t, _, _ in recs)}", flush=True)
        _prefetch_next(sess)

def _kick_autoplay(sess: Session) -> None:
    if sess.autoplay_task is None or sess.autoplay_task.done():
        sess.autoplay_task = asyncio.ensure_future(_autoplay_refill(sess))

async def _autoplay_fill(sess: Session) -> None:
    _kick_autoplay(sess)
    # asyncio.wait never raises: if the refill is cancelled (stop / restart), awaiting it directly
    # would raise CancelledError into the song change or command that is waiting for it.
    await asyncio.wait([sess.autoplay_task])


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
        if player:
            await _set_vc_status(guild_id, sess.channel_id, None)   # clear it while still in the channel
        await _leave_voice(guild_id)
    _notify(guild_id)

async def _leave_voice(guild_id: int) -> None:
    """Really leave the voice channel. Each step is separate: wavelink's disconnect() removes the
    Lavalink player before telling Discord to leave, so if that first part failed the bot used to
    stay in the channel with a dead player that the next session then reused."""
    guild = bot.get_guild(guild_id)
    if not guild:
        return
    player = guild.voice_client
    if player:
        try:
            await player.stop()
        except Exception as exc:
            print(f"[Worker {BOT_INDEX}] stop() failed while leaving guild {guild_id}: {exc}", flush=True)
        try:
            await player.disconnect()
        except Exception as exc:
            print(f"[Worker {BOT_INDEX}] disconnect() failed in guild {guild_id}: {exc} — forcing", flush=True)
    if guild.me and guild.me.voice and guild.me.voice.channel:
        try:
            await guild.change_voice_state(channel=None)
        except Exception as exc:
            print(f"[Worker {BOT_INDEX}] Could not leave voice in guild {guild_id}: {exc}", flush=True)
    if guild.voice_client:
        try:
            guild.voice_client.cleanup()
        except Exception:
            pass

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

async def _session_watchdog():
    """Every 30 s: end sessions whose voice connection is gone, and start the idle timer for
    sessions with nothing to play, so a worker can't stay 'busy' forever."""
    while True:
        await asyncio.sleep(30)
        try:
            await _watchdog_pass()
        except Exception as exc:
            print(f"[Worker {BOT_INDEX}] Watchdog error: {exc}", flush=True)

async def _watchdog_pass() -> None:
    # Ghost voice connections: Discord can keep a worker in a channel after the process that
    # joined it restarted. A worker in voice without a session leaves.
    for guild in list(bot.guilds):
        if guild.me and guild.me.voice and guild.me.voice.channel and guild.id not in sessions:
            print(f"[Worker {BOT_INDEX}] Watchdog: in voice in guild {guild.id} without a session — leaving", flush=True)
            await _leave_voice(guild.id)
    for gid, sess in list(sessions.items()):
        if sess.connecting or sess.closing or sess.drain_state:
            continue
        if time.monotonic() - sess.created < 90:
            continue   # still searching / connecting
        player = _get_player(gid)
        if player is None or not getattr(player, "connected", True):
            print(f"[Worker {BOT_INDEX}] Watchdog: session in guild {gid} has no voice connection — ending", flush=True)
            await _end_session(gid, disconnect=False)
            continue
        if sess.play_lock.locked():
            # A song change is in progress (e.g. a slow YouTube lookup): nothing playing for a
            # moment is expected, and resuming now would start a second song next to it.
            sess.stalled = 0
            continue
        refilling = sess.autoplay_task is not None and not sess.autoplay_task.done()
        if player.current is None and sess.queue and not sess.skipping and not refilling:
            # Songs are waiting but nothing plays (e.g. Lavalink restarted mid-song and dropped
            # the player's track). Two checks in a row (30-60 s) so a normal song change isn't hit.
            sess.stalled += 1
            if sess.stalled >= 2:
                print(f"[Worker {BOT_INDEX}] Watchdog: queue stalled in guild {gid} — resuming playback", flush=True)
                sess.stalled = 0
                await _advance_safely(sess, player)
            continue
        sess.stalled = 0
        if player.current is None and not sess.queue and not (sess.idle_task and not sess.idle_task.done()):
            print(f"[Worker {BOT_INDEX}] Watchdog: nothing playing in guild {gid} — starting idle timer", flush=True)
            _start_idle_timer(sess)

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
    _spawn(_event_sender())
    _spawn(_connect_lavalink())
    _spawn(_ipc_server())
    _spawn(_session_watchdog())
    asyncio.get_event_loop().call_later(10, lambda: _spawn(_watchdog_pass()))   # clean ghosts soon after start

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

    if sess.drain_state == "message":
        if reason != "replaced":   # "replaced" is the song the message just took over from
            await _end_session(sess.guild_id)
        return

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
        sess.queue.insert(0, QueueItem(orig, "Looping", "", sess.current_requester))

    await _advance_safely(sess, player)

@bot.event
async def on_wavelink_track_stuck(payload: wavelink.TrackStuckEventPayload):
    """Lavalink says the stream stopped delivering audio. Without this the song just sat silent."""
    player: wavelink.Player = payload.player
    sess = _session_for_player(player)
    if not sess or sess.drain_state == "message":
        return
    title = payload.track.title if payload.track else "?"
    print(f"[Worker {BOT_INDEX}] Track stuck ({payload.threshold} ms without audio): {title!r} — skipping", flush=True)
    sess.current_meta = None   # a stall isn't the user's skip
    sess.skipping = True
    try:
        try:
            await player.stop()
        except Exception:
            pass
        await _advance_safely(sess, player)
    finally:
        sess.skipping = False

@bot.event
async def on_wavelink_track_exception(payload: wavelink.TrackExceptionEventPayload):
    player: wavelink.Player = payload.player
    sess = _session_for_player(player)
    if not sess:
        return
    sess.skipping = False
    sess.current_meta = None   # never played, so nothing to learn from
    if sess.drain_state == "message":
        await _end_session(sess.guild_id)
        return
    if sess.drain_state == "finishing":
        await _play_shutdown_message(sess, player)
        return
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
        if sess.text_channel and sess.announce:
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
        try:
            async with sess.play_lock:
                await _play(sess, player, nxt, sess.current_requester, sess.current_query)
            return
        except Exception as exc:
            print(f"[Worker {BOT_INDEX}] Alternate failed too: {exc}", flush=True)
    await _advance_safely(sess, player, announce=False)

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
            old = sess.channel_id
            sess.channel_id = after.channel.id   # dragged to another channel
            _notify(sess.guild_id)
            # The "🎵Playing - title" status moves with the bot instead of staying on the old channel.
            _spawn(_set_vc_status(sess.guild_id, old, None))
            if sess.current_info and not sess.drain_state:
                _spawn(_set_vc_status(sess.guild_id, sess.channel_id, sess.current_info["title"]))
        return

    if member.bot:
        return
    touched = {getattr(before.channel, "id", None), getattr(after.channel, "id", None)}
    if sess.channel_id not in touched:
        return

    player = _get_player(sess.guild_id)
    if not _humans_in_vc(sess) and sess.drain_state:
        await _end_session(sess.guild_id)   # restarting and nobody is listening
        return
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
    if player and getattr(player, "channel", None) is not None and player.channel.id == vc.id:
        return player
    # Mark the session as connecting BEFORE any leave/move: the bot's own "left voice" event
    # would otherwise look like a kick and end the session that is connecting.
    sess.connecting = True
    try:
        if player and getattr(player, "channel", None) is None:
            await _leave_voice(sess.guild_id)   # half-disconnected leftover: start clean
            player = None
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


async def _sp_lookup(query: str, taste: dict | None = None) -> wavelink.Playable | None:
    """Clean title/artist/length from Spotify, but only if a top result actually matches the query.
    Spotify's first hit can be unrelated (e.g. 'noite quente' -> 'Spa Noturno Hindu')."""
    try:
        r = await wavelink.Playable.search(f"spsearch:{query}")
    except Exception as exc:
        print(f"[Worker {BOT_INDEX}] Spotify lookup error: {str(exc).splitlines()[0][:120]}", flush=True)
        return None
    items = r.tracks if isinstance(r, wavelink.Playlist) else list(r or [])
    q_words = set(_words(query))
    if not items or not q_words:
        return None
    asked = bool(_ALTERED_ASK.search(query))
    best, best_key = None, (-1.0, False, 0)
    for t in items[:5]:
        t_text = _fold(f"{t.title} {t.author}")
        cov = sum(1 for w in q_words if w in t_text) / len(q_words)
        # e.g. "noite quente phonk" should not lock onto "Noite Quente (Slowed)" by "Adiel phonk"
        clean = asked or not _ALTERED_QUICK.search(t.title or "")
        # Many songs share a title ("Noite Quente" by M22, Flame Runner, ...). Among matching
        # results, prefer the song / artist this user actually listens to.
        known = 0
        if taste:
            sid = _song_id(t.title, t.author)
            if taste["songs"].get(sid, (0, 0))[0] > 0:
                known = 2
            elif taste["artists"].get(sid[1], 0) > 0:
                known = 1
        key = (cov if cov >= SP_MIN_COVERAGE else 0.0, clean, known)
        if key > best_key:
            best, best_key = t, key
    best_cov = best_key[0]
    if best_cov < SP_MIN_COVERAGE:
        print(f"[Worker {BOT_INDEX}] Spotify results don't match the query — ignoring "
              f"(best: {items[0].title!r} by {items[0].author!r})", flush=True)
        return None
    return best

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

async def _search(query: str, source: str, sess: Session | None,
                  user_id: int | None = None, use_ai: bool = True) -> tuple[wavelink.Playable | None, str, str]:
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
        if _ALTERED_ASK.search(query) or await _user_wants_altered(query):
            return (results[0] if results else None), label, "SoundCloud"
        clean = await _first_clean(results)
        selected = clean or (results[0] if results else None)
        if selected:
            _remember_alternates(sess, selected, results, label, "SoundCloud")
        return selected, label, "SoundCloud"

    # Spotify and the plain YouTube Music search run at the same time.
    taste = await _taste(sess.guild_id, user_id) if sess and user_id else None
    sp_task = _sp_lookup(query, taste) if source != "yt" else asyncio.sleep(0, result=None)
    asked = bool(_ALTERED_ASK.search(query))
    sp, raw = await asyncio.gather(sp_task, _ytm(query))
    if sp:
        yt_q  = f"{sp.title} {sp.author}".strip()
        path  = "Spotify → YouTube Music"
        print(f"[Worker {BOT_INDEX}] Spotify: {sp.title!r} by {sp.author!r} ({(sp.length or 0)//1000}s)", flush=True)
        # Search with Spotify's clean metadata too, and merge (Spotify-based results first).
        candidates, seen = [], set()
        for t in (await _ytm(yt_q)) + raw:
            if _track_key(t) not in seen:
                seen.add(_track_key(t))
                candidates.append(t)
    else:
        yt_q, path, candidates = query, "YouTube Music", raw

    qnorm = _query_norm(query)
    if taste:
        # The upload they listened to may not be in this search's results at all: add it.
        candidates += await _remembered_candidates(taste, qnorm, candidates)

    # If the search names an artist, personal history must not pull results by other artists.
    q_words = [w for w in _words(query) if len(w) >= 3]
    named = {_artist_key(t.author) for t in candidates
             if any(w in _fold(t.author or "") and w not in _fold(t.title or "") for w in q_words)}
    personal: dict[str, tuple[float, str]] = {}
    for t in candidates:
        bonus, note = _personal(t, taste, qnorm)
        if named and _artist_key(t.author) not in named:
            bonus = min(bonus, 0.0)
        personal[_track_key(t)] = (bonus, note)
        if bonus:
            print(f"[Worker {BOT_INDEX}]   personal {bonus:+6.1f}: {t.title!r} by {t.author!r}", flush=True)

    scored = [(t, _track_score(t, query, sp) + personal[_track_key(t)][0]) for t in candidates]
    ranked = await _rank_by_popularity(scored, skip_altered=not asked) if scored else []
    top    = [t for t, _ in ranked[:AI_PICK_CANDIDATES]]

    notes    = {i: personal[_track_key(t)][1] for i, t in enumerate(top) if personal[_track_key(t)][1]}
    obvious  = _obvious_pick(ranked[:AI_PICK_CANDIDATES], asked, personal, _altered_kinds(query))
    if obvious is None and not use_ai:
        obvious = 0 if top and not _ALTERED_QUICK.search(top[0].title or "") else None
    if obvious is not None:
        # Clear winner: no AI call needed (saves Groq tokens).
        print(f"[Worker {BOT_INDEX}] Clear winner — AI skipped", flush=True)
        decision = {"wants_altered": asked, "pick": obvious,
                    "altered": set() if asked else {i for i, t in enumerate(top) if _ALTERED_QUICK.search(t.title or "")}}
    elif use_ai:
        decision = await _ai_pick_cached(query, top, notes) if top else None
    else:
        decision = None
    if decision:
        wants_altered = decision["wants_altered"] or asked
        altered = set(decision["altered"])
        if not wants_altered:
            # Belt and braces: obvious title words override the AI calling something "original".
            altered |= {i for i, t in enumerate(top) if _ALTERED_ASK.search(t.title or "")}
        print(f"[Worker {BOT_INDEX}] AI: wants_altered={wants_altered} altered={sorted(altered)} "
              f"pick={decision['pick']}", flush=True)
    else:
        wants_altered = asked
        altered = set() if asked else {i for i, t in enumerate(top) if _ALTERED_QUICK.search(t.title or "")}

    allowed = [i for i in range(len(top)) if wants_altered or i not in altered]
    if wants_altered:
        # An explicit request for an altered version beats history: strong history with the
        # normal upload must not push every slowed/sped-up result out of the running.
        alt_allowed = [i for i in allowed if i in altered or _ALTERED_QUICK.search(top[i].title or "")]
        allowed = alt_allowed or allowed
    if allowed:
        # Relevance guard: the pick must match the search about as well as the best allowed result,
        # so "noite quente slowed" can't land on a slowed version of a different song. Measured on
        # the user's own words plus their history, NOT on Spotify's guess: Spotify picks one of many
        # same-titled songs (e.g. M22's "Noite Quente"), which must not veto a better pick.
        rel = {_track_key(t): _track_score(t, query) + personal[_track_key(t)][0] for t, _ in scored}
        best_rel = max(rel[_track_key(top[i])] for i in allowed)
        relevant = [i for i in allowed if rel[_track_key(top[i])] >= best_rel - 40]
        if wants_altered:
            # Asked for an altered version: prefer relevant results that are altered.
            relevant = [i for i in relevant if i in altered or _ALTERED_QUICK.search(top[i].title or "")] or relevant
        if decision and decision["pick"] in relevant:
            choice = decision["pick"]
        else:
            if decision and decision["pick"] >= 0:
                print(f"[Worker {BOT_INDEX}] AI pick {decision['pick']} doesn't match the search well enough — overriding", flush=True)
            choice = relevant[0]
        # The AI judges generically ("most popular official upload"). Strong personal evidence
        # for a result that matches the search wins over that.
        pers = {i: personal[_track_key(top[i])][0] for i in relevant}
        fav  = max(relevant, key=lambda i: pers[i])
        if pers[fav] >= PERSONAL_STRONG and pers[fav] > pers[choice] + 20:
            print(f"[Worker {BOT_INDEX}] Personal history prefers {top[fav].title!r} by {top[fav].author!r} "
                  f"({pers[fav]:+.0f}) over the AI pick", flush=True)
            choice = fav
        t = top[choice]
        print(f"[Worker {BOT_INDEX}] ✓ {t.title!r} by {t.author!r}", flush=True)
        # Fallbacks (if this upload can't be played) must be another upload of the SAME song:
        # same title and either the same artist or the same length. A same-named song by
        # someone else (e.g. the "just drums" one) is not a fallback.
        alternates = [t] + [top[i] for i in relevant if i != choice and _same_song(top[i], t)]
        _remember_alternates(sess, t, alternates, label, path)
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

    if ranked:
        best = ranked[0][0]
        print(f"[Worker {BOT_INDEX}] ⚠ fallback: {best.title!r}", flush=True)
        _remember_alternates(sess, best, [t for t, _ in ranked], label, path)
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
    global _draining, _shutdown_url
    try:
        data = await reader.readline()
        if not data:
            return
        cmd = json.loads(data.decode())
        op  = cmd.get("op")

        async def reply(ok: bool, msg: str = "", **extra):
            # If the controller already gave up waiting (timeout), the connection is gone. That
            # must not count as a failure: before, it ended the session that had just started.
            resp = json.dumps({"status": "ok" if ok else "error", "message": msg, **extra})
            try:
                writer.write((resp + "\n").encode())
                await writer.drain()
            except (ConnectionError, OSError) as exc:
                print(f"[Worker {BOT_INDEX}] Reply to controller lost ({op}): {exc}", flush=True)

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

            _spawn(_do_shutdown())
            return

        if op == "forget_user":
            # /reset-algo: drop cached history and don't re-save the song playing right now.
            uid_f = int(cmd.get("user_id") or 0)
            _taste_cache.pop((gid, uid_f), None)
            for g, sess in sessions.items():
                if g == gid and sess.current_requester == uid_f:
                    sess.current_meta = sess.current_query = None
            await reply(True, "forgotten")
            return

        if op == "drain":
            # Restart scheduled: finish the current song (dropping the rest of the queue/playlist),
            # then play the shutdown message and leave. Nothing new can start from now on.
            _draining = True
            _shutdown_url = cmd.get("message_url")
            for g, sess in list(sessions.items()):
                player = _get_player(g)
                sess.queue.clear()
                sess.autoplay = sess.loop = False
                if not player or sess.auto_paused or not _humans_in_vc(sess):
                    await _end_session(g)
                elif player.current and not player.paused:
                    sess.drain_state = "finishing"
                else:
                    await _play_shutdown_message(sess, player)   # paused or between songs
            print(f"[Worker {BOT_INDEX}] Draining — {len(sessions)} session(s) finishing", flush=True)
            await reply(True, "draining", remaining=len(sessions))
            return

        if op == "drain_force":
            # Timeout reached: cut the song and play the message now.
            for g, sess in list(sessions.items()):
                if sess.drain_state != "message":
                    await _play_shutdown_message(sess, _get_player(g))
            await reply(True, "forced", remaining=len(sessions))
            return

        if gid is None:
            await reply(False, "guild_id required")
            return

        if _draining and op in ("search_and_play", "queue_track", "search_and_playlist", "autoplay_start",
                                "toggle_autoplay", "toggle_loop", "jump_to"):
            await reply(False, "The music bots are restarting — try again in a few minutes.")
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
            sess.announce = bool(cmd.get("announce", True))   # follows the latest play command
            try:
                await _start_op(sess, op, cmd, uid, reply, new)
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
                {"title": item[0].title or "Unknown", "author": item[0].author or "", "index": i,
                 "qid": item.qid, "autoplay": item[1] == AUTOPLAY_LABEL}
                for i, item in enumerate(sess.queue if sess else [])
            ]
            await reply(True, "queue", current=current, queue=queue_list,
                        muted=sess.muted if sess else False, loop=sess.loop if sess else False,
                        autoplay=sess.autoplay if sess else False,
                        normalize=sess.normalize if sess else False,
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
            delta = int(cmd.get("delta_ms", 0))
            pos   = max(0, player.position + delta)
            length = (player.current.length or 0) if player.current else 0
            if length and pos > length:   # streams have no length: don't clamp them to 0
                pos = max(0, length - 1000)
            await player.seek(int(pos))
            await reply(True, "seeked", delta_ms=delta)
            return

        if op == "remove_from_queue":
            idx = _queue_pos(sess, int(cmd.get("qid", -1)))
            if idx is None:
                await reply(False, "That song is no longer in the queue (it already played or was removed).")
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

        if op == "toggle_normalize":
            if not player:
                await reply(False, "Nothing playing.")
                return
            want = not sess.normalize
            try:
                await _apply_normalize(player, want)
            except Exception as exc:
                await reply(False, f"Couldn't change normalization (is the LavaDSPX plugin installed in Lavalink?): {exc}")
                return
            sess.normalize = want
            await reply(True, "normalize_on" if want else "normalize_off", normalize=want)
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
            idx = _queue_pos(sess, int(cmd.get("qid", -1)))
            if idx is None:
                await reply(False, "That song is no longer in the queue (it already played or was removed).")
                return

            target_item = sess.queue[idx]
            target_track, target_lbl, target_path, target_req = target_item
            del sess.queue[:idx + 1]
            _account(sess, player.position, skipped=True)
            sess.skipping = True
            try:
                await player.stop()
                sess.fail_streak = 0
                try:
                    target_track = await _play(sess, player, target_track, target_req, target_item.query)
                    if _can_post(sess):
                        await _send_status(sess.text_channel, _now_playing_embed(target_track, target_lbl, target_path))
                except Exception as exc:
                    print(f"[Worker {BOT_INDEX}] Jump target failed: {exc} — continuing with the queue", flush=True)
                    await _advance_safely(sess, player)
                    await reply(False, f"Couldn't play {target_track.title or 'that song'} — moved on to the next one.")
                    return
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


async def _fresh_vc(sess: Session, new: bool) -> wavelink.Player:
    """Connect for a play command. A new session never reuses a player left over from an earlier
    one: a dead leftover (still holding an old paused track) made new songs queue up behind it and
    never start."""
    guild = bot.get_guild(sess.guild_id)
    if new and guild and guild.voice_client:
        print(f"[Worker {BOT_INDEX}] Leftover voice connection in guild {sess.guild_id} — reconnecting fresh", flush=True)
        sess.connecting = True
        try:
            await _leave_voice(sess.guild_id)
            await asyncio.sleep(1)
        finally:
            sess.connecting = False
    return await _connect_vc(sess)

def _not_found_msg(query: str, default: str) -> str:
    """Spotify links fail when Spotify refuses Lavalink's lookups (its API now requires the app
    owner to have Premium); "Nothing found" made it look like the song doesn't exist."""
    if _SPOTIFY_RE.search(query or ""):
        return ("Couldn't load that Spotify link — Spotify is refusing lookups right now "
                "(its API requires the Spotify app owner to have Premium). Try searching by name instead.")
    return default

async def _abandoned(sess: Session, reply) -> bool:
    """True if the session was stopped (/stop, or a restart began) while we were searching or
    connecting. Playing anyway would leave music running with no session to control or stop it."""
    if sessions.get(sess.guild_id) is sess:
        return False
    print(f"[Worker {BOT_INDEX}] Session in guild {sess.guild_id} ended before playback started — not playing", flush=True)
    if sess.guild_id not in sessions:
        await _leave_voice(sess.guild_id)
    await reply(False, "Playback was stopped before it started.")
    return True

async def _start_op(sess: Session, op: str, cmd: dict, uid: int | None, reply, new: bool = False) -> None:
    """Ops that can start playback: /play, /playlist, /autoplay."""
    gid = sess.guild_id

    if op in ("search_and_play", "queue_track"):
        track, lbl, path = await _search(cmd["query"], cmd["source"], sess, uid)
        if not track:
            await reply(False, _not_found_msg(cmd["query"], "Nothing found."))
            return
        async with sess.play_lock:   # play-or-queue is decided once the previous change is done
            player = await _fresh_vc(sess, new)
            if await _abandoned(sess, reply):
                return
            if player.playing:
                pos = _enqueue(sess, QueueItem(track, lbl, path, uid, cmd["query"]))
                await reply(True, "queued", session=sess.snapshot(),
                            **_track_reply(track, "queued", lbl, path, queue_pos=pos))
            else:
                sess.fail_streak = 0
                played = await _play(sess, player, track, uid, cmd["query"])
                await reply(True, "playing", session=sess.snapshot(),
                            **_track_reply(played, "playing", lbl, path))
        return

    if op == "search_and_playlist":
        tracks, pl_name, lbl, skipped = await _search_playlist(cmd["query"], cmd["source"])
        if not tracks:
            await reply(False, _not_found_msg(
                cmd["query"], f"No playable tracks found.{' (' + str(skipped) + ' local skipped)' if skipped else ''}"))
            return
        async with sess.play_lock:
            player = await _fresh_vc(sess, new)
            if await _abandoned(sess, reply):
                return
            first = tracks[0]
            if player.playing:
                for t in tracks: _enqueue(sess, QueueItem(t, lbl, "Playlist", uid))
                await reply(True, "queued_playlist", session=sess.snapshot(),
                            embed_type="queued_playlist",
                            pl_name=pl_name, count=len(tracks),
                            title=first.title, artwork=first.artwork or "",
                            source_label=lbl, skipped=skipped)
            else:
                sess.fail_streak = 0
                for t in tracks[1:]: _enqueue(sess, QueueItem(t, lbl, "Playlist", uid))
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
        async with sess.play_lock:
            player = await _fresh_vc(sess, new)
            if await _abandoned(sess, reply):
                return
            if player.playing:
                sess.queue = [q for q in sess.queue if q[1] != AUTOPLAY_LABEL]
                if not sess.queue:
                    _kick_autoplay(sess)
                await reply(True, "autoplay_on", autoplay=True, session=sess.snapshot())
                return
            await _autoplay_fill(sess)
            if await _abandoned(sess, reply):
                return
            if not sess.queue:
                await reply(False, "Couldn't find anything to autoplay right now — try again in a bit.")
                return
            item = sess.queue.pop(0)
            nxt, lbl, path, req = item
            sess.fail_streak = 0
            played = await _play(sess, player, nxt, req, item.query)
            await reply(True, "playing", session=sess.snapshot(),
                        **_track_reply(played, "playing", AUTOPLAY_LABEL, path or "Your saved songs"))
        return


async def _ipc_server():
    if SOCKET_PATH.exists():
        SOCKET_PATH.unlink()
    server = await asyncio.start_unix_server(_handle_connection, path=str(SOCKET_PATH), limit=1 << 20)
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

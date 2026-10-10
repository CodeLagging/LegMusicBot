"""Synced lyrics for the song that's playing.

Providers are asked together; one that fails or times out is simply skipped:
  - YouTube Music (ytmusicapi): the lyrics of the exact video being played (song uploads only)
  - LRCLib: open lyrics database, matched by title, artist and duration
  - NetEase Cloud Music: matched by title, artist and duration
Only synced (timed) lyrics are used. When several providers have them, the best match wins.
Musixmatch (terms forbid its desktop API) and Genius (no synced lyrics) are deliberately not used.
"""
import asyncio
import bisect
import json
import re
import time

import aiohttp

import db
from textnorm import artist_key, fold, norm_title, words

PROVIDER_TIMEOUT   = 8.0      # per provider; most lookups are prefetched while the previous song plays
MAX_LENGTH_DIFF_MS = 3000       # a source further off is another version (slowed, sped up, video intro)
NONE_CACHE_TTL     = 3 * 86400  # "no synced lyrics anywhere" is checked again after this
FOUND_CACHE_TTL    = 30 * 86400
GAP_MS             = 8000       # this far into a line, with the next one still a while away -> ♪
PAIR_MAX_GAP_MS    = 7000       # two lyric lines share a display line only if the second starts this soon
SOURCE_BONUS       = {"YouTube Music": 30, "LRCLib": 15, "NetEase": 0}
MUSIC_NOTE         = "♪"
SEPARATOR          = "─"        # divider between blocks: box-drawing line, continuous in a code block
SEPARATOR_MAX      = 30         # wider would wrap in an embed on a phone
USER_AGENT = "LegMusicBot (https://github.com/CodeLagging/LegMusicBot)"
BROWSER_UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"


class Lyrics:
    __slots__ = ("lines", "starts", "provider", "duration_ms", "layouts", "width")

    def __init__(self, lines: list[tuple[int, str]], provider: str, duration_ms: int = 0):
        self.lines       = sorted((int(t), s) for t, s in lines)
        self.starts      = [t for t, _ in self.lines]
        self.provider    = provider
        self.duration_ms = int(duration_ms or 0)
        # What's displayed, per lyric_safe mode: (display lines, their start times).
        # Each display line is (start, text, start of its last lyric line).
        # Divider length: the song's longest line (+ marker), so it stays the same all song.
        self.width = min(SEPARATOR_MAX, max(12, max((len(t) + 2 for _, t in self.lines), default=12)))
        self.layouts = {}
        for merged in (False, True):
            groups = _group(self.lines, 2 if merged else 1)
            self.layouts[merged] = (groups, [g[0] for g in groups])


def _group(lines: list[tuple[int, str]], per_line: int) -> list[tuple[int, str, int]]:
    """Lyric lines to display lines. With per_line=2 (lyric_safe) two lines share one display line,
    so the message changes about half as often (fewer edits, less rate limiting). Pairs are fixed
    (1+2, 3+4, ...). A music break (♪) stays on its own line, and lines far apart aren't joined
    (the second one would show long before it's sung)."""
    groups: list[list] = []   # [start, [texts], start of its last line]
    for t, text in lines:
        g = groups[-1] if groups else None
        if g and text == MUSIC_NOTE and g[1] == [MUSIC_NOTE]:
            continue   # one ♪ for consecutive breaks
        if (g and text != MUSIC_NOTE and g[1][0] != MUSIC_NOTE and len(g[1]) < per_line
                and t - g[0] <= PAIR_MAX_GAP_MS):
            g[1].append(text)
            g[2] = t
        else:
            groups.append([t, [text], t])
    return [(start, "\n".join(texts), last) for start, texts, last in groups]   # one row per lyric line


# ── parsing ──────────────────────────────────────────────────────────────────

_TIMESTAMP = re.compile(r"\[(\d{1,3}):(\d{1,2})(?:[.:](\d{1,3}))?\]")
_WORD_TIME = re.compile(r"<\d{1,3}:\d{1,2}(?:[.:]\d{1,3})?>")
_OFFSET    = re.compile(r"^\s*\[offset:\s*([+-]?\d+)\s*\]", re.IGNORECASE)
# Credit lines NetEase (and some LRC files) put first: "作词 : …", "Composed by: …".
_CREDIT    = re.compile(r"^\s*(?:[一-鿿]{1,8}|(?:lyrics?|written|composed|arranged|produced|mixed|mastered)"
                        r"(?:\s+by)?|composer|lyricist|producer|arranger)\s*[:：]", re.IGNORECASE)

def parse_lrc(text: str) -> list[tuple[int, str]]:
    """LRC to [(start ms, text)]: several timestamps per line, [offset:], [mm:ss.xx] and [mm:ss:xx].
    Metadata tags and credit lines are dropped; an empty line is a music break."""
    offset, out = 0, []
    for raw in (text or "").splitlines():
        m = _OFFSET.match(raw)
        if m:
            offset = int(m.group(1))   # positive = lyrics come sooner
            continue
        stamps, pos = [], 0
        while (m := _TIMESTAMP.match(raw, pos)):
            mins, secs, frac = m.groups()
            stamps.append(int(mins) * 60_000 + int(secs) * 1000 + int((frac or "0").ljust(3, "0")[:3]))
            pos = m.end()
        if not stamps:
            continue   # [ar:], [ti:], ... or junk
        line = _WORD_TIME.sub("", raw[pos:]).strip()
        if _CREDIT.match(line):
            continue
        for ms in stamps:
            out.append((max(0, ms - offset), line or MUSIC_NOTE))
    return sorted(out)


# ── matching ─────────────────────────────────────────────────────────────────

_JUNK_BRACKET = re.compile(r"[\(\[][^\)\]]*\b(?:official|video|audio|lyrics?|visuali[sz]er|hd|hq|4k|mv|feat|ft|prod|"
                           r"remaster(?:ed)?)\b[^\)\]]*[\)\]]", re.IGNORECASE)

def clean_artist(author: str) -> str:
    """'Flame Runner - Topic' -> 'Flame Runner'; first artist only ('A, B' / 'A & B' / 'A x B')."""
    a = re.sub(r"\s*-\s*topic$", "", author or "", flags=re.IGNORECASE)
    a = re.sub(r"\s*vevo$", "", a, flags=re.IGNORECASE)
    a = re.split(r",|&|\s+x\s+|\s+feat\.?\s+|\s+ft\.?\s+", a, flags=re.IGNORECASE)[0]
    return a.strip()

def clean_title(title: str, artist: str) -> str:
    """The song name as lyrics sites list it: no "(Official Video)", "feat." or "Artist - " prefix.
    Version words like "(Slowed)" stay: they're a different recording with different timing."""
    t = _JUNK_BRACKET.sub(" ", title or "")
    a = clean_artist(artist)
    if a:
        t = re.sub(r"^\s*" + re.escape(a) + r"\s*[-–—]\s*", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\s+(?:feat|ft)\.?\s.*$", "", t, flags=re.IGNORECASE)
    return " ".join(t.split())

def _variants(title: str, artist: str) -> list[tuple[str, str]]:
    """(title, artist) to search for. Re-uploads put the real artist in the title ("Nakama, Mc Staff -
    MENTE MÁ" uploaded by "void"), so that split is tried as well."""
    out = [(title, artist)]
    m = re.match(r"^(.+?)\s+[-–—]\s+(.+)$", title)
    if m:
        alt = (clean_title(m.group(2), ""), clean_artist(m.group(1)))
        if alt[0] and alt[1] and alt not in out:
            out.append(alt)
    return out

def _same_title(a: str, b: str) -> bool:
    na, nb = norm_title(fold(a)), norm_title(fold(b))
    return bool(na and nb) and (na == nb or f" {na} " in f" {nb} " or f" {nb} " in f" {na} ")

def _same_artist(a: str, b: str) -> bool:
    ka, kb = artist_key(a), artist_key(b)
    return bool(ka and kb) and (bool(set(words(ka)) & set(words(kb))) or ka in kb or kb in ka)

def _problem(c: Lyrics, length_ms: int) -> str | None:
    """Why this source can't be used for the playing upload, or None if it can."""
    sung = [s for _, s in c.lines if s and s != MUSIC_NOTE]
    if len(sung) < 3 or len(set(c.starts)) < 3:
        return "not really synced"
    if length_ms and c.duration_ms and abs(c.duration_ms - length_ms) > MAX_LENGTH_DIFF_MS:
        return f"another version ({abs(c.duration_ms - length_ms) / 1000:.0f}s off)"
    if length_ms and c.starts[-1] > length_ms + 2000:
        return "runs past the end of the song"
    return None

def _score(c: Lyrics, length_ms: int) -> float:
    score = SOURCE_BONUS.get(c.provider, 0)
    if length_ms and c.duration_ms:
        score += 40 - min(40.0, abs(c.duration_ms - length_ms) / 100)   # closest duration = same recording
    return score + min(sum(1 for _, s in c.lines if s != MUSIC_NOTE), 60) / 3


# ── providers ────────────────────────────────────────────────────────────────

_http: aiohttp.ClientSession | None = None

def _session() -> aiohttp.ClientSession:
    global _http
    if _http is None or _http.closed:
        _http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=PROVIDER_TIMEOUT))
    return _http

async def close() -> None:
    if _http is not None and not _http.closed:
        await _http.close()

def _length_ms(text: str | None) -> int:
    """'3:22' -> 202000."""
    try:
        parts = [int(p) for p in (text or "").split(":")]
    except ValueError:
        return 0
    secs = 0
    for p in parts:
        secs = secs * 60 + p
    return secs * 1000

_ytm_missing = False

def _ytm_song_lyrics(yt, video_id: str) -> Lyrics | None:
    wp = yt.get_watch_playlist(videoId=video_id, limit=1)
    track = (wp.get("tracks") or [{}])[0]
    # Song uploads only: a music video's intro would put every line early or late.
    if track.get("videoId") != video_id or track.get("videoType") != "MUSIC_VIDEO_TYPE_ATV" or not wp.get("lyrics"):
        return None
    ly = yt.get_lyrics(wp["lyrics"], timestamps=True)
    if not ly or not ly.get("hasTimestamps"):
        return None
    lines = [(int(x.start_time), (x.text or "").strip() or MUSIC_NOTE) for x in ly["lyrics"]]
    return Lyrics(lines, "YouTube Music", _length_ms(track.get("length")))

def _ytm_sync(yt_id: str | None, variants: list[tuple[str, str]], length_ms: int) -> Lyrics | None:
    global _ytm_missing
    try:
        from ytmusicapi import YTMusic
    except ImportError:
        if not _ytm_missing:
            _ytm_missing = True
            print("[Lyrics] ytmusicapi isn't installed — YouTube Music lyrics are skipped", flush=True)
        return None
    yt = YTMusic()
    if yt_id:
        found = _ytm_song_lyrics(yt, yt_id)
        if found:
            return found
    if not length_ms:
        return None
    # The playing upload isn't a song upload (a re-upload, lyric video, ...): find the song upload
    # of the same recording. Only one within 3 s of the same length is trusted, so lines stay in time.
    for title, artist in variants:
        for r in yt.search(f"{title} {artist}", filter="songs", limit=5)[:5]:
            dur = int(r.get("duration_seconds") or 0) * 1000
            names = ", ".join(a.get("name") or "" for a in r.get("artists") or [])
            if (r.get("videoId") and r["videoId"] != yt_id and _same_title(r.get("title") or "", title)
                    and dur and abs(dur - length_ms) <= MAX_LENGTH_DIFF_MS and _same_artist(names, artist)):
                found = _ytm_song_lyrics(yt, r["videoId"])
                if found:
                    return found
    return None

async def _ytm(yt_id: str | None, variants: list[tuple[str, str]], length_ms: int) -> Lyrics | None:
    return await asyncio.to_thread(_ytm_sync, yt_id, variants, length_ms)

async def _lrclib(title: str, artist: str, length_ms: int) -> Lyrics | None:
    s, headers = _session(), {"User-Agent": USER_AGENT}
    found: list[dict] = []
    params = {"track_name": title, "artist_name": artist}
    if length_ms:
        params["duration"] = str(round(length_ms / 1000))   # LRCLib matches within ±2 s
    async with s.get("https://lrclib.net/api/get", params=params, headers=headers) as r:
        if r.status == 200:
            found.append(await r.json())
    if not any(x.get("syncedLyrics") for x in found):
        async with s.get("https://lrclib.net/api/search", params={"track_name": title, "artist_name": artist},
                         headers=headers) as r:
            if r.status == 200:
                found += await r.json()
    best, best_key = None, None
    for x in found:
        if x.get("instrumental") or not x.get("syncedLyrics") or not _same_title(x.get("trackName") or "", title):
            continue
        c = Lyrics(parse_lrc(x["syncedLyrics"]), "LRCLib", int((x.get("duration") or 0) * 1000))
        if _problem(c, length_ms):
            continue
        key = (not _same_artist(x.get("artistName") or "", artist),
               abs(c.duration_ms - length_ms) if c.duration_ms and length_ms else MAX_LENGTH_DIFF_MS)
        if best_key is None or key < best_key:
            best, best_key = c, key
    return best

async def _netease(title: str, artist: str, length_ms: int) -> Lyrics | None:
    s, headers = _session(), {"User-Agent": BROWSER_UA, "Referer": "https://music.163.com/"}
    async with s.get("https://music.163.com/api/search/get",
                     params={"s": f"{title} {artist}", "type": "1", "limit": "6"}, headers=headers) as r:
        data = json.loads(await r.text())   # served as text/plain
    picks = []
    for song in (data.get("result") or {}).get("songs") or []:
        dur = int(song.get("duration") or 0)
        names = ", ".join(a.get("name") or "" for a in song.get("artists") or [])
        if not _same_title(song.get("name") or "", title):
            continue
        if length_ms and dur and abs(dur - length_ms) > MAX_LENGTH_DIFF_MS:
            continue
        picks.append((not _same_artist(names, artist), abs(dur - length_ms) if dur and length_ms else 0,
                      song["id"], dur))
    for *_, song_id, dur in sorted(picks)[:2]:
        async with s.get("https://music.163.com/api/song/lyric",
                         params={"id": str(song_id), "lv": "1", "kv": "1", "tv": "-1"}, headers=headers) as r:
            d = json.loads(await r.text())
        lrc = (d.get("lrc") or {}).get("lyric") or ""
        if not lrc or "纯音乐" in lrc:   # "instrumental, enjoy"
            continue
        c = Lyrics(parse_lrc(lrc), "NetEase", dur)
        if not _problem(c, length_ms):
            return c
    return None


# ── lookup ───────────────────────────────────────────────────────────────────

def cache_key(title: str, artist: str) -> str:
    return " ".join(words(clean_title(title, artist))) + "|" + artist_key(clean_artist(artist))

async def _each_variant(provider, variants: list[tuple[str, str]], length_ms: int) -> Lyrics | None:
    for title, artist in variants:
        found = await provider(title, artist, length_ms)
        if found:
            return found
    return None

async def _lookup(title: str, artist: str, length_ms: int, yt_id: str | None):
    variants = _variants(title, artist)
    jobs = {"YouTube Music": lambda: _ytm(yt_id, variants, length_ms),
            "LRCLib": lambda: _each_variant(_lrclib, variants, length_ms),
            "NetEase": lambda: _each_variant(_netease, variants, length_ms)}
    names = list(jobs)
    results = await asyncio.gather(*[asyncio.wait_for(jobs[n](), PROVIDER_TIMEOUT) for n in names],
                                   return_exceptions=True)
    found, report, failed = [], [], False
    for name, r in zip(names, results):
        if isinstance(r, BaseException):
            failed = True
            report.append(f"{name} failed ({type(r).__name__})")
        elif r is None:
            report.append(f"{name}: none")
        elif (why := _problem(r, length_ms)):
            report.append(f"{name}: rejected, {why}")
        else:
            found.append(r)
            report.append(f"{name}: synced, {len(r.lines)} lines")
    best = max(found, key=lambda c: _score(c, length_ms)) if found else None
    return best, failed, "; ".join(report)

_inflight: dict[str, asyncio.Future] = {}

async def find(title: str, artist: str, length_ms: int, yt_id: str | None = None) -> tuple[Lyrics | None, str]:
    """Synced lyrics for the playing upload, or None. Also returns a short report for the log
    (provider names and counts only, never lyric text)."""
    t, a = clean_title(title, artist), clean_artist(artist)
    key = cache_key(title, artist)
    try:
        hit = await asyncio.to_thread(db.lyrics_get, key)
    except Exception:
        hit = None
    if hit:
        age = time.time() - hit["fetched"]
        if hit["lines"] is None and age < NONE_CACHE_TTL:
            return None, "cached: no synced lyrics"
        if hit["lines"] and age < FOUND_CACHE_TTL:
            c = Lyrics([tuple(x) for x in json.loads(hit["lines"])], hit["provider"], hit["duration_ms"])
            if not _problem(c, length_ms):
                return c, f"cached ({c.provider})"
    fut = _inflight.get(key)
    if fut is None:
        fut = asyncio.ensure_future(_lookup(t, a, length_ms, yt_id))
        _inflight[key] = fut
        fut.add_done_callback(lambda _f: _inflight.pop(key, None))
    best, failed, report = await asyncio.shield(fut)
    # A provider that failed might have had them: only a clean "nobody has synced lyrics" is cached.
    if best or not failed:
        try:
            await asyncio.to_thread(db.lyrics_put, key, best.provider if best else None,
                                    best.duration_ms if best else 0,
                                    json.dumps(best.lines, ensure_ascii=False) if best else None)
        except Exception as exc:
            report += f"; cache write failed ({exc})"
    return best, (f"{report} -> {best.provider}" if best else f"{report} -> no synced lyrics")


# ── display ──────────────────────────────────────────────────────────────────

def state_at(lyr: Lyrics, pos_ms: int, merged: bool = True) -> tuple[str, str, str]:
    """(previous, current, next) display line at this position; with merged, each holds up to two
    lyric lines. Before the first line, during long breaks and after the last line it's ♪."""
    groups, starts = lyr.layouts[merged]
    i = bisect.bisect_right(starts, pos_ms) - 1
    if i < 0:
        return "", MUSIC_NOTE, groups[0][1] if groups else ""
    nxt_start = groups[i + 1][0] if i + 1 < len(groups) else None
    prev, cur, last = (groups[i - 1][1] if i >= 1 else ""), groups[i][1], groups[i][2]
    nxt = groups[i + 1][1] if nxt_start is not None else ""
    # Nothing sung for a while (an instrumental break the file doesn't mark, or the outro).
    if cur != MUSIC_NOTE and pos_ms - last >= GAP_MS and (nxt_start is None or nxt_start - last >= GAP_MS + 4000):
        prev, cur = cur, MUSIC_NOTE
    return prev, cur, nxt

def render(state: tuple[str, str, str], width: int = SEPARATOR_MAX) -> str:
    """Previous, current and next block in a code block, divided by solid lines. A block is one or
    (lyric_safe) two lyric lines, one row each; the current block's rows are marked with ▶ (bold
    doesn't work in code blocks)."""
    def rows(block: str, mark: str) -> list[str]:
        return [mark + (line or "").replace("```", "'''")[:90] for line in (block or "").split("\n")]
    prev, cur, nxt = state
    blocks = [rows(prev, "  "), rows(cur, "▶ "), rows(nxt, "  ")]
    divider = f"\n{SEPARATOR * width}\n"
    return "```\n" + divider.join("\n".join(b) for b in blocks) + "\n```"

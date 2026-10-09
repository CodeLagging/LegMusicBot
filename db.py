"""SQLite storage shared by the main controller and the workers.

database/server/servers.db      per-server config (CC_ID, VCW, EPH, SRC)
database/algo/<guild_id>.db     per-user listening history ("algo") for that server
"""
import json
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path

DB_ROOT   = Path(__file__).parent / "database"
SERVER_DB = DB_ROOT / "server" / "servers.db"
ALGO_DIR  = DB_ROOT / "algo"

# Rough per-row overhead used for the storage cap (keys, counters, sqlite bookkeeping).
_ROW_OVERHEAD = 64
_SIZE_SQL = ("COALESCE(SUM(LENGTH(track_key) + LENGTH(title) + LENGTH(author) + "
             f"LENGTH(uri) + LENGTH(yt_id) + {_ROW_OVERHEAD}), 0)")
# "Most unused" = lowest score, oldest play breaks ties.
_SCORE_SQL = "(plays - 0.5 * skips)"


def _connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path, timeout=10)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    return con


@contextmanager
def _tx(con: sqlite3.Connection):
    """Commit (or roll back) and always close. sqlite3's own `with con:` only commits; the
    connection stays open until garbage-collected."""
    try:
        with con:
            yield con
    finally:
        con.close()


# ── server config ────────────────────────────────────────────────────────────

def _server_con() -> sqlite3.Connection:
    con = _connect(SERVER_DB)
    con.execute("""CREATE TABLE IF NOT EXISTS guild_settings (
        guild_id   INTEGER PRIMARY KEY,
        cc_id      INTEGER,
        vcw        TEXT NOT NULL DEFAULT '{}',
        eph        TEXT NOT NULL DEFAULT '{}',
        src        TEXT,
        updated_at REAL)""")
    cols = {r[1] for r in con.execute("PRAGMA table_info(guild_settings)")}
    if "src" not in cols:
        con.execute("ALTER TABLE guild_settings ADD COLUMN src TEXT")
    return con


def get_guild(guild_id: int) -> dict:
    with _tx(_server_con()) as con:
        row = con.execute("SELECT cc_id, vcw, eph, src FROM guild_settings WHERE guild_id = ?",
                          (guild_id,)).fetchone()
    if not row:
        return {"cc_id": None, "vcw": {}, "eph": {}, "src": None}
    return {"cc_id": row["cc_id"], "vcw": json.loads(row["vcw"] or "{}"),
            "eph": json.loads(row["eph"] or "{}"), "src": row["src"]}


def save_guild(guild_id: int, cfg: dict) -> None:
    with _tx(_server_con()) as con:
        con.execute(
            """INSERT INTO guild_settings (guild_id, cc_id, vcw, eph, src, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(guild_id) DO UPDATE SET
                 cc_id = excluded.cc_id, vcw = excluded.vcw, eph = excluded.eph,
                 src = excluded.src, updated_at = excluded.updated_at""",
            (guild_id, cfg.get("cc_id"), json.dumps(cfg.get("vcw") or {}),
             json.dumps(cfg.get("eph") or {}), cfg.get("src"), time.time()),
        )


def guild_exists(guild_id: int) -> bool:
    with _tx(_server_con()) as con:
        return con.execute("SELECT 1 FROM guild_settings WHERE guild_id = ?",
                           (guild_id,)).fetchone() is not None


# ── per-user algo ────────────────────────────────────────────────────────────

def _algo_con(guild_id: int) -> sqlite3.Connection:
    con = _connect(ALGO_DIR / f"{int(guild_id)}.db")
    con.execute("""CREATE TABLE IF NOT EXISTS user_tracks (
        user_id     INTEGER NOT NULL,
        track_key   TEXT    NOT NULL,
        title       TEXT    NOT NULL DEFAULT '',
        author      TEXT    NOT NULL DEFAULT '',
        uri         TEXT    NOT NULL DEFAULT '',
        yt_id       TEXT    NOT NULL DEFAULT '',
        plays       INTEGER NOT NULL DEFAULT 0,
        skips       INTEGER NOT NULL DEFAULT 0,
        last_played REAL    NOT NULL,
        added_at    REAL    NOT NULL,
        PRIMARY KEY (user_id, track_key))""")
    # Search memory: what the user ended up listening to (or skipped) for a given search text.
    con.execute("""CREATE TABLE IF NOT EXISTS user_queries (
        user_id     INTEGER NOT NULL,
        query       TEXT    NOT NULL,
        track_key   TEXT    NOT NULL,
        title       TEXT    NOT NULL DEFAULT '',
        author      TEXT    NOT NULL DEFAULT '',
        yt_id       TEXT    NOT NULL DEFAULT '',
        listens     INTEGER NOT NULL DEFAULT 0,
        skips       INTEGER NOT NULL DEFAULT 0,
        last_used   REAL    NOT NULL,
        PRIMARY KEY (user_id, query, track_key))""")
    return con


def record_play(guild_id: int, user_id: int, meta: dict, max_kb: int) -> None:
    """Count a listen; if the user's saved songs go over max_kb, evict their most-unused songs.
    There is no song-count limit: storage is the only cap."""
    key = meta.get("key")
    if not key:
        return
    now = time.time()
    with _tx(_algo_con(guild_id)) as con:
        con.execute(
            """INSERT INTO user_tracks (user_id, track_key, title, author, uri, yt_id,
                                        plays, skips, last_played, added_at)
               VALUES (?, ?, ?, ?, ?, ?, 1, 0, ?, ?)
               ON CONFLICT(user_id, track_key) DO UPDATE SET
                 plays = plays + 1, last_played = excluded.last_played,
                 title = excluded.title, author = excluded.author,
                 uri = excluded.uri,
                 yt_id = CASE WHEN excluded.yt_id != '' THEN excluded.yt_id ELSE yt_id END""",
            (user_id, key, meta.get("title") or "", meta.get("author") or "",
             meta.get("uri") or "", meta.get("yt_id") or "", now, now),
        )
        _evict(con, user_id, key, max_kb)


def _evict(con: sqlite3.Connection, user_id: int, keep_key: str, max_kb: int) -> None:
    # The song just played is never the one evicted, otherwise a full list could never take new songs.
    victims_sql = (f"SELECT track_key FROM user_tracks WHERE user_id = ? AND track_key != ? "
                   f"ORDER BY {_SCORE_SQL} ASC, last_played ASC LIMIT ?")
    limit = max_kb * 1024
    while con.execute(f"SELECT {_SIZE_SQL} FROM user_tracks WHERE user_id = ?",
                      (user_id,)).fetchone()[0] > limit:
        victim = con.execute(victims_sql, (user_id, keep_key, 1)).fetchone()
        if not victim:
            break
        con.execute("DELETE FROM user_tracks WHERE user_id = ? AND track_key = ?", (user_id, victim[0]))


_QUERY_SIZE_SQL = ("COALESCE(SUM(LENGTH(query) + LENGTH(track_key) + LENGTH(title) + LENGTH(author) + "
                   f"LENGTH(yt_id) + {_ROW_OVERHEAD}), 0)")

def record_query(guild_id: int, user_id: int, query: str, meta: dict,
                 listened: bool, max_kb: int) -> None:
    """Remember what a search led to: a listen (>= 30 s) or a quick skip.
    Search memory gets a quarter of the user's storage cap; the least recently used entries go first."""
    key = meta.get("key")
    if not key or not query:
        return
    with _tx(_algo_con(guild_id)) as con:
        con.execute(
            """INSERT INTO user_queries (user_id, query, track_key, title, author, yt_id,
                                         listens, skips, last_used)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(user_id, query, track_key) DO UPDATE SET
                 listens = listens + excluded.listens, skips = skips + excluded.skips,
                 last_used = excluded.last_used""",
            (user_id, query, key, meta.get("title") or "", meta.get("author") or "",
             meta.get("yt_id") or "", 1 if listened else 0, 0 if listened else 1, time.time()),
        )
        limit = max_kb * 1024 // 4
        while con.execute(f"SELECT {_QUERY_SIZE_SQL} FROM user_queries WHERE user_id = ?",
                          (user_id,)).fetchone()[0] > limit:
            oldest = con.execute("SELECT query, track_key FROM user_queries WHERE user_id = ? "
                                 "ORDER BY last_used ASC LIMIT 1", (user_id,)).fetchone()
            if not oldest:
                break
            con.execute("DELETE FROM user_queries WHERE user_id = ? AND query = ? AND track_key = ?",
                        (user_id, oldest[0], oldest[1]))

def user_queries(guild_id: int, user_id: int) -> list[dict]:
    if not (ALGO_DIR / f"{int(guild_id)}.db").exists():
        return []
    with _tx(_algo_con(guild_id)) as con:
        rows = con.execute("SELECT query, track_key, title, author, yt_id, listens, skips, last_used "
                           "FROM user_queries WHERE user_id = ?", (user_id,)).fetchall()
    return [dict(r) for r in rows]

def record_skip(guild_id: int, user_id: int, key: str) -> None:
    """A quick skip lowers a saved song's score. Unsaved songs are not added."""
    if not key:
        return
    with _tx(_algo_con(guild_id)) as con:
        con.execute("UPDATE user_tracks SET skips = skips + 1 WHERE user_id = ? AND track_key = ?",
                    (user_id, key))


def user_tracks(guild_id: int, user_id: int, limit: int = 100_000) -> list[dict]:
    """The user's saved songs, best first."""
    if not (ALGO_DIR / f"{int(guild_id)}.db").exists():
        return []
    with _tx(_algo_con(guild_id)) as con:
        rows = con.execute(
            f"""SELECT track_key, title, author, uri, yt_id, plays, skips, last_played,
                       {_SCORE_SQL} AS score
                FROM user_tracks WHERE user_id = ?
                ORDER BY score DESC, last_played DESC LIMIT ?""",
            (user_id, limit),
        ).fetchall()
    return [dict(r) for r in rows]


def reset_user(guild_id: int, user_id: int) -> tuple[int, int]:
    """/reset-algo: delete one user's saved songs and search memory on one server.
    Returns (songs deleted, search-memory entries deleted). Never touches other users."""
    if not (ALGO_DIR / f"{int(guild_id)}.db").exists():
        return 0, 0
    with _tx(_algo_con(guild_id)) as con:
        songs   = con.execute("DELETE FROM user_tracks WHERE user_id = ?", (user_id,)).rowcount
        queries = con.execute("DELETE FROM user_queries WHERE user_id = ?", (user_id,)).rowcount
    return songs, queries

def remove_user_tracks(guild_id: int, user_id: int, keys: list[str]) -> int:
    """Remove chosen songs from one user's algo (and their search memory for those songs)."""
    if not keys or not (ALGO_DIR / f"{int(guild_id)}.db").exists():
        return 0
    with _tx(_algo_con(guild_id)) as con:
        removed = 0
        for k in keys:
            removed += con.execute("DELETE FROM user_tracks WHERE user_id = ? AND track_key = ?",
                                   (user_id, k)).rowcount
            con.execute("DELETE FROM user_queries WHERE user_id = ? AND (track_key = ? OR yt_id = ?)",
                        (user_id, k, k))
    return removed

def user_query_count(guild_id: int, user_id: int) -> int:
    if not (ALGO_DIR / f"{int(guild_id)}.db").exists():
        return 0
    with _tx(_algo_con(guild_id)) as con:
        return con.execute("SELECT COUNT(*) FROM user_queries WHERE user_id = ?", (user_id,)).fetchone()[0]

def user_stats(guild_id: int, user_id: int) -> tuple[int, int]:
    """(song count, approx bytes) for one user."""
    if not (ALGO_DIR / f"{int(guild_id)}.db").exists():
        return 0, 0
    with _tx(_algo_con(guild_id)) as con:
        row = con.execute(f"SELECT COUNT(*), {_SIZE_SQL} FROM user_tracks WHERE user_id = ?",
                          (user_id,)).fetchone()
    return row[0], row[1]

"""Bot-wide settings from server_settings.json: tokens, Lavalink, and behaviour.

Behaviour keys are re-read automatically when the file changes. Tokens and Lavalink
are only read at startup (changing them needs a restart).
"""
import json
import os
import sys
from pathlib import Path

PATH = Path(__file__).parent / "server_settings.json"

DEFAULTS: dict = {
    "tokens": [],
    "lavalink": {"uri": "", "password": ""},
    "dev_ids": [],
    "whitelist_enabled": True,
    "whitelist": [],
    "leave_message": "This server is not currently whitelisted, bot will not function",
    "algo_max_kb": 1024,       # per user per server; about 100 bytes per saved song
    "autoplay_seed_count": 5,
}

_cache: dict = dict(DEFAULTS)
_mtime: float | None = None


def get() -> dict:
    global _cache, _mtime
    try:
        mtime = os.path.getmtime(PATH)
    except OSError:
        return _cache
    if mtime != _mtime:
        try:
            with open(PATH) as f:
                data = json.load(f)
            if isinstance(data, dict):
                _cache = {**DEFAULTS, **data}
                _mtime = mtime
        except (OSError, json.JSONDecodeError) as exc:
            print(f"[Settings] Could not read {PATH.name}: {exc} — keeping previous values", flush=True)
    return _cache


def require_startup() -> dict:
    """Settings needed to run at all; exits with a clear message if they are missing."""
    if not PATH.exists():
        sys.exit(f"{PATH.name} not found — copy {PATH.name}.example and fill in the tokens and Lavalink details.")
    s = get()
    if len(s["tokens"]) < 2:
        sys.exit(f"{PATH.name} needs at least 2 tokens: tokens[0]=main, tokens[1+]=workers")
    if not s["lavalink"].get("uri"):
        sys.exit(f"{PATH.name} is missing lavalink.uri")
    return s


def _id_set(values) -> set[int]:
    """IDs from a settings list; a typo in one entry is skipped (and logged) instead of raising on
    every message the bot sees."""
    out = set()
    for v in values or []:
        try:
            out.add(int(v))
        except (TypeError, ValueError):
            if repr(v) not in _warned:
                _warned.add(repr(v))
                print(f"[Settings] Ignoring invalid id {v!r} in {PATH.name}", flush=True)
    return out

_warned: set[str] = set()


def is_dev(user_id: int) -> bool:
    return int(user_id) in _id_set(get()["dev_ids"])


def is_whitelisted(guild_id: int) -> bool:
    s = get()
    if not s["whitelist_enabled"]:
        return True
    return int(guild_id) in _id_set(s["whitelist"])

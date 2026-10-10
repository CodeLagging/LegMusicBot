"""Text normalisation shared by search (worker.py) and lyrics (lyrics.py)."""
import re
import unicodedata

_CHANNEL_SUFFIX_RE = re.compile(r"(\s*-\s*topic|\s*vevo|\s+official|\s+music|\s+oficial)$")


def fold(text: str) -> str:
    """Lower-case and strip accents, so 'Tântrico' matches 'tantrico'."""
    nfkd = unicodedata.normalize("NFKD", text or "")
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower()


def words(text: str) -> list[str]:
    return [w for w in re.split(r"\W+", fold(text)) if len(w) > 1]


def artist_key(author: str) -> str:
    """'Sxilwix - Topic', 'SxilwixVEVO' and 'Sxilwix' are the same artist."""
    a = fold(author or "").strip()
    for _ in range(2):
        a = _CHANNEL_SUFFIX_RE.sub("", a).strip()
    return a


def norm_title(title: str) -> str:
    """Title without bracketed parts and words like 'official video'."""
    t = re.sub(r"[\(\[].*?[\)\]]", " ", (title or "").lower())
    t = re.sub(r"\b(official|audio|video|lyrics?|music|hd|hq|4k|mv)\b", " ", t)
    return " ".join(re.findall(r"\w+", t))

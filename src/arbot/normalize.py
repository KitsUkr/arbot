from __future__ import annotations

import re
import unicodedata

_PUNCT_RE = re.compile(r"[^\w\s]", flags=re.UNICODE)
_WS_RE = re.compile(r"\s+")
_STOPWORDS: frozenset[str] = frozenset({
    "the", "a", "an", "of", "in", "on", "at", "to", "for", "by",
    "will", "be", "is", "are", "was", "were", "this", "that",
    "as", "with",
})


def normalize_title(title: str) -> str:
    """Lowercase, strip diacritics, drop punctuation/stopwords, collapse whitespace."""
    s = unicodedata.normalize("NFKD", title)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = _PUNCT_RE.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    if not s:
        return ""
    return " ".join(t for t in s.split() if t not in _STOPWORDS)

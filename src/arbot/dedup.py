from __future__ import annotations

import time
from collections import OrderedDict


class TTLDedup:
    """In-memory FIFO TTL set. Returns True when key is newly seen, False otherwise."""

    def __init__(self, ttl_seconds: int) -> None:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        self._ttl = ttl_seconds
        self._seen: OrderedDict[str, float] = OrderedDict()

    def is_new(self, key: str) -> bool:
        now = time.monotonic()
        self._evict(now)
        if key in self._seen:
            return False
        self._seen[key] = now
        return True

    def _evict(self, now: float) -> None:
        cutoff = now - self._ttl
        while self._seen:
            k, t = next(iter(self._seen.items()))
            if t < cutoff:
                self._seen.popitem(last=False)
            else:
                break

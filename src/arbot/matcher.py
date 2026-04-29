from __future__ import annotations

from collections.abc import Iterable
from datetime import timezone

import structlog
from rapidfuzz import fuzz, process

from .models import MarketPair, MarketQuote
from .normalize import normalize_title

log = structlog.get_logger(__name__)


class MarketMatcher:
    """Pairs markets across venues by normalized title similarity + expiry proximity."""

    def __init__(
        self,
        title_similarity_threshold: int = 88,
        max_expiry_delta_hours: int = 72,
    ) -> None:
        if not 0 <= title_similarity_threshold <= 100:
            raise ValueError("title_similarity_threshold must be in [0, 100]")
        if max_expiry_delta_hours < 0:
            raise ValueError("max_expiry_delta_hours must be non-negative")
        self._threshold = title_similarity_threshold
        self._max_expiry = max_expiry_delta_hours

    def match(
        self,
        markets_a: Iterable[MarketQuote],
        markets_b: Iterable[MarketQuote],
    ) -> list[MarketPair]:
        a_list = list(markets_a)
        b_list = list(markets_b)
        if not a_list or not b_list:
            return []

        b_titles: dict[int, str] = {}
        for i, m in enumerate(b_list):
            norm = normalize_title(m.title)
            if norm:
                b_titles[i] = norm
        if not b_titles:
            return []

        pairs: list[MarketPair] = []
        for a in a_list:
            a_norm = normalize_title(a.title)
            if not a_norm:
                continue
            best = process.extractOne(
                a_norm,
                b_titles,
                scorer=fuzz.token_set_ratio,
                score_cutoff=self._threshold,
            )
            if best is None:
                continue
            _matched_norm, score, b_idx = best
            b = b_list[b_idx]

            delta_h = self._expiry_delta_hours(a, b)
            if delta_h is not None and delta_h > self._max_expiry:
                log.debug(
                    "matcher.expiry_too_far",
                    title_a=a.title, title_b=b.title, delta_hours=delta_h,
                )
                continue

            pairs.append(
                MarketPair(
                    a=a, b=b,
                    similarity=int(score),
                    expiry_delta_hours=delta_h,
                )
            )
        return pairs

    @staticmethod
    def _expiry_delta_hours(a: MarketQuote, b: MarketQuote) -> float | None:
        if a.expires_at is None or b.expires_at is None:
            return None
        ax = a.expires_at if a.expires_at.tzinfo else a.expires_at.replace(tzinfo=timezone.utc)
        bx = b.expires_at if b.expires_at.tzinfo else b.expires_at.replace(tzinfo=timezone.utc)
        return abs((ax - bx).total_seconds()) / 3600.0

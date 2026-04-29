from __future__ import annotations

from collections.abc import Iterable
from datetime import timezone

import structlog
from rapidfuzz import fuzz, process

from .models import MarketPair, MarketQuote
from .normalize import normalize_title

log = structlog.get_logger(__name__)


class MarketMatcher:
    """Pairs markets across venues.

    Two-stage strategy:
      1. Direct match via `linked_market_ids` (cross-listing the venue itself
         publishes — e.g. predict.fun's `polymarketConditionIds`). 100% precision.
      2. Fuzzy fallback for the rest: normalized titles + rapidfuzz token_set_ratio
         + expiry-proximity guard.
    """

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

        # Stage 1: direct matches via cross-listed IDs.
        # Index B markets by both venue_market_id and linked_market_ids — either
        # side of the cross-listing can carry the link.
        b_by_id: dict[str, int] = {}
        for i, m in enumerate(b_list):
            for key in (m.venue_market_id, *m.linked_market_ids):
                if key:
                    b_by_id.setdefault(key, i)

        matched_a: set[int] = set()
        matched_b: set[int] = set()
        pairs: list[MarketPair] = []

        for ai, a in enumerate(a_list):
            candidates = (a.venue_market_id, *a.linked_market_ids)
            for key in candidates:
                if not key:
                    continue
                bi = b_by_id.get(key)
                if bi is None or bi in matched_b:
                    continue
                b = b_list[bi]
                pairs.append(
                    MarketPair(
                        a=a,
                        b=b,
                        similarity=100,
                        expiry_delta_hours=self._expiry_delta_hours(a, b),
                        match_method="direct",
                    )
                )
                matched_a.add(ai)
                matched_b.add(bi)
                break

        log.info("matcher.direct_matches", n=len(pairs))

        # Stage 2: fuzzy fallback over what's left.
        a_remaining = [(i, m) for i, m in enumerate(a_list) if i not in matched_a]
        b_remaining_idx = [i for i in range(len(b_list)) if i not in matched_b]

        if a_remaining and b_remaining_idx:
            b_titles: dict[int, str] = {}
            for bi in b_remaining_idx:
                norm = normalize_title(b_list[bi].title)
                if norm:
                    b_titles[bi] = norm

            if b_titles:
                fuzzy_count = 0
                for _ai, a in a_remaining:
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
                    if b_idx in matched_b:
                        continue
                    b = b_list[b_idx]
                    delta_h = self._expiry_delta_hours(a, b)
                    if delta_h is not None and delta_h > self._max_expiry:
                        continue
                    pairs.append(
                        MarketPair(
                            a=a,
                            b=b,
                            similarity=int(score),
                            expiry_delta_hours=delta_h,
                            match_method="fuzzy",
                        )
                    )
                    matched_b.add(b_idx)
                    fuzzy_count += 1
                log.info("matcher.fuzzy_matches", n=fuzzy_count)

        return pairs

    @staticmethod
    def _expiry_delta_hours(a: MarketQuote, b: MarketQuote) -> float | None:
        if a.expires_at is None or b.expires_at is None:
            return None
        ax = a.expires_at if a.expires_at.tzinfo else a.expires_at.replace(tzinfo=timezone.utc)
        bx = b.expires_at if b.expires_at.tzinfo else b.expires_at.replace(tzinfo=timezone.utc)
        return abs((ax - bx).total_seconds()) / 3600.0

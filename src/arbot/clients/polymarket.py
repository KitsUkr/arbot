from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
import structlog

from ..models import MarketQuote, OutcomeSide, Quote, Venue

log = structlog.get_logger(__name__)

_GAMMA_PAGE_LIMIT = 100
_BOOKS_BATCH = 25


class PolymarketClient:
    """Polymarket data client: list active markets via Gamma, fetch L2 books via CLOB."""

    venue_name = "polymarket"

    def __init__(
        self,
        gamma_url: str,
        clob_url: str,
        max_markets: int = 200,
        timeout_seconds: float = 15.0,
    ) -> None:
        self._gamma_url = gamma_url.rstrip("/")
        self._clob_url = clob_url.rstrip("/")
        self._max_markets = max_markets
        self._http = httpx.AsyncClient(
            timeout=timeout_seconds,
            headers={"User-Agent": "arbot/0.1", "Accept": "application/json"},
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def fetch_markets(self) -> list[MarketQuote]:
        raw_markets = await self._fetch_active_markets()
        log.info("polymarket.listed", n=len(raw_markets))

        token_to_meta: dict[str, tuple[str, OutcomeSide, dict[str, Any]]] = {}
        for m in raw_markets:
            try:
                yes_id, no_id = self._extract_token_ids(m)
            except (ValueError, KeyError, json.JSONDecodeError):
                continue
            mid = str(m.get("id"))
            token_to_meta[yes_id] = (mid, OutcomeSide.YES, m)
            token_to_meta[no_id] = (mid, OutcomeSide.NO, m)

        if not token_to_meta:
            return []

        books = await self._fetch_books(list(token_to_meta.keys()))

        buckets: dict[str, dict[str, Any]] = {}
        for token_id, book in books.items():
            meta = token_to_meta.get(token_id)
            if meta is None:
                continue
            mid, side, m = meta
            buckets.setdefault(mid, {"market": m})[side.value] = book

        out: list[MarketQuote] = []
        now = datetime.now(timezone.utc)
        for mid, bucket in buckets.items():
            if "yes" not in bucket or "no" not in bucket:
                continue
            m = bucket["market"]
            try:
                yes_q = self._book_to_quote(bucket["yes"], OutcomeSide.YES)
                no_q = self._book_to_quote(bucket["no"], OutcomeSide.NO)
            except ValueError as e:
                log.debug("polymarket.skip_book", id=mid, reason=str(e))
                continue
            try:
                quote = MarketQuote(
                    venue=Venue.POLYMARKET,
                    venue_market_id=mid,
                    title=str(m.get("question") or "").strip(),
                    expires_at=self._parse_dt(m.get("endDate")),
                    yes=yes_q,
                    no=no_q,
                    fetched_at=now,
                    url=f"https://polymarket.com/event/{m.get('slug', '')}",
                )
            except Exception as e:
                log.debug("polymarket.skip_validate", id=mid, error=str(e))
                continue
            out.append(quote)

        log.info("polymarket.quoted", n=len(out))
        return out

    async def _fetch_active_markets(self) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        offset = 0
        while len(results) < self._max_markets:
            params = {
                "active": "true",
                "closed": "false",
                "archived": "false",
                "limit": _GAMMA_PAGE_LIMIT,
                "offset": offset,
            }
            try:
                r = await self._http.get(f"{self._gamma_url}/markets", params=params)
                r.raise_for_status()
            except httpx.HTTPError as e:
                log.warning("polymarket.gamma_failed", error=str(e), offset=offset)
                break
            page = r.json()
            if not isinstance(page, list) or not page:
                break
            results.extend(page)
            if len(page) < _GAMMA_PAGE_LIMIT:
                break
            offset += _GAMMA_PAGE_LIMIT
        return results[: self._max_markets]

    async def _fetch_books(self, token_ids: list[str]) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        for i in range(0, len(token_ids), _BOOKS_BATCH):
            chunk = token_ids[i : i + _BOOKS_BATCH]
            payload = [{"token_id": tid} for tid in chunk]
            try:
                r = await self._http.post(f"{self._clob_url}/books", json=payload)
                r.raise_for_status()
                data = r.json()
            except httpx.HTTPError as e:
                log.warning("polymarket.books_failed", error=str(e), chunk=len(chunk))
                continue
            if not isinstance(data, list):
                continue
            for book in data:
                tid = book.get("asset_id") or book.get("token_id")
                if tid is not None:
                    out[str(tid)] = book
        return out

    @staticmethod
    def _extract_token_ids(market: dict[str, Any]) -> tuple[str, str]:
        outcomes_raw = market.get("outcomes")
        token_ids_raw = market.get("clobTokenIds")
        if not outcomes_raw or not token_ids_raw:
            raise ValueError("missing outcomes / clobTokenIds")
        outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
        tids = json.loads(token_ids_raw) if isinstance(token_ids_raw, str) else token_ids_raw
        if not isinstance(outcomes, list) or not isinstance(tids, list):
            raise ValueError("outcomes/clobTokenIds wrong type")
        if len(outcomes) != 2 or len(tids) != 2:
            raise ValueError("not a binary market")
        idx_yes = next(
            (i for i, o in enumerate(outcomes) if str(o).strip().lower() == "yes"), None
        )
        idx_no = next(
            (i for i, o in enumerate(outcomes) if str(o).strip().lower() == "no"), None
        )
        if idx_yes is None or idx_no is None:
            raise ValueError(f"non yes/no outcomes: {outcomes}")
        return str(tids[idx_yes]), str(tids[idx_no])

    @staticmethod
    def _book_to_quote(book: dict[str, Any], side: OutcomeSide) -> Quote:
        asks = book.get("asks") or []
        bids = book.get("bids") or []
        if not asks:
            raise ValueError("empty asks")
        try:
            asks_sorted = sorted(asks, key=lambda x: Decimal(str(x["price"])))
            bids_sorted = sorted(
                bids, key=lambda x: Decimal(str(x["price"])), reverse=True
            )
        except (KeyError, InvalidOperation) as e:
            raise ValueError(f"bad book row: {e}") from e

        a0 = asks_sorted[0]
        ask_price = Decimal(str(a0["price"]))
        ask_size = Decimal(str(a0["size"]))
        bid_price: Decimal | None = None
        bid_size: Decimal | None = None
        if bids_sorted:
            b0 = bids_sorted[0]
            bid_price = Decimal(str(b0["price"]))
            bid_size = Decimal(str(b0["size"]))

        return Quote(
            side=side,
            ask_price=ask_price,
            ask_size=ask_size,
            bid_price=bid_price,
            bid_size=bid_size,
        )

    @staticmethod
    def _parse_dt(s: Any) -> datetime | None:
        if not s:
            return None
        try:
            return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
        except ValueError:
            return None

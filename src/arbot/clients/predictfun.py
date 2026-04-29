from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
import structlog

from ..models import MarketQuote, OutcomeSide, Quote, Venue

log = structlog.get_logger(__name__)


class PredictFunClient:
    """Predict.fun REST client matching the real /v1 API.

    Two-stage fetch:
      1. GET /v1/markets — paginated list of markets (metadata only, including
         the cross-listing field `polymarketConditionIds`).
      2. For each open market, GET /v1/markets/{id}/orderbook to derive top-of-book
         for both YES and NO. The orderbook exposes only YES-side levels:
            asks: [[price, qty], ...]  (best ask first)
            bids: [[price, qty], ...]  (best bid first)
         NO-side prices are derived as: NO_ask = 1 - YES_bid, NO_size = YES_bid_size.

    Mainnet requires an API key in `x-api-key`; if PREDICTFUN_API_KEY is empty we
    still send the request and surface 401/403 in the logs so it's obvious.
    """

    venue_name = "predictfun"

    def __init__(
        self,
        api_url: str,
        api_key: str = "",
        max_markets: int = 60,
        concurrency: int = 4,
        timeout_seconds: float = 15.0,
    ) -> None:
        self._api_url = api_url.rstrip("/")
        self._max_markets = max_markets
        self._sem = asyncio.Semaphore(max(1, concurrency))
        headers = {"User-Agent": "arbot/0.1", "Accept": "application/json"}
        if api_key:
            headers["x-api-key"] = api_key
        self._http = httpx.AsyncClient(timeout=timeout_seconds, headers=headers)

    async def aclose(self) -> None:
        await self._http.aclose()

    async def fetch_markets(self) -> list[MarketQuote]:
        markets = await self._list_markets()
        if not markets:
            return []

        log.info("predictfun.listed", n=len(markets))
        now = datetime.now(timezone.utc)

        # Pull orderbooks concurrently, capped by semaphore to respect rate limits.
        tasks = [self._fetch_orderbook_safe(m) for m in markets]
        books = await asyncio.gather(*tasks)

        out: list[MarketQuote] = []
        for m, book in zip(markets, books, strict=True):
            if book is None:
                continue
            try:
                yes_q, no_q = self._book_to_quotes(book)
            except ValueError as e:
                log.debug("predictfun.skip_book", id=m.get("id"), reason=str(e))
                continue

            try:
                quote = self._build_market_quote(m, yes_q, no_q, now)
            except (ValueError, TypeError) as e:
                log.debug("predictfun.skip_validate", id=m.get("id"), error=str(e))
                continue
            out.append(quote)

        log.info("predictfun.quoted", n=len(out))
        return out

    async def _list_markets(self) -> list[dict[str, Any]]:
        all_markets: list[dict[str, Any]] = []
        cursor: str | None = None
        page = 0
        max_pages = 10  # safety net; pagination should normally end well before this

        while len(all_markets) < self._max_markets and page < max_pages:
            params: dict[str, Any] = {
                "first": min(50, self._max_markets - len(all_markets)),
                "status": "REGISTERED",
            }
            if cursor:
                params["after"] = cursor
            try:
                r = await self._http.get(f"{self._api_url}/v1/markets", params=params)
                r.raise_for_status()
                payload = r.json()
            except httpx.HTTPStatusError as e:
                log.warning(
                    "predictfun.list_failed",
                    status=e.response.status_code,
                    body=e.response.text[:200],
                )
                break
            except httpx.HTTPError as e:
                log.warning("predictfun.list_failed", error=str(e))
                break

            if not isinstance(payload, dict):
                break
            data = payload.get("data") or []
            if not isinstance(data, list) or not data:
                break

            # Filter to markets that are actively trading (we want OPEN orderbooks).
            for m in data:
                if not isinstance(m, dict):
                    continue
                trading_status = str(m.get("tradingStatus", "")).upper()
                if trading_status and trading_status != "OPEN":
                    continue
                all_markets.append(m)
                if len(all_markets) >= self._max_markets:
                    break

            cursor = payload.get("cursor")
            if not cursor:
                break
            page += 1

        return all_markets

    async def _fetch_orderbook_safe(self, market: dict[str, Any]) -> dict[str, Any] | None:
        mid = market.get("id")
        if mid is None:
            return None
        async with self._sem:
            try:
                r = await self._http.get(f"{self._api_url}/v1/markets/{mid}/orderbook")
                r.raise_for_status()
                payload = r.json()
            except httpx.HTTPError as e:
                log.debug("predictfun.book_failed", id=mid, error=str(e))
                return None
        if not isinstance(payload, dict):
            return None
        data = payload.get("data")
        return data if isinstance(data, dict) else None

    @staticmethod
    def _book_to_quotes(book: dict[str, Any]) -> tuple[Quote, Quote]:
        """Convert a single YES-side orderbook into (YES quote, NO quote).

        Predict.fun orderbook shape:
            asks: [[price, qty], ...]  best (lowest) first — these are YES asks
            bids: [[price, qty], ...]  best (highest) first — these are YES bids
        NO-side derives from the YES bid side (someone bidding for YES at p is
        effectively offering NO at 1 - p).
        """
        asks = book.get("asks") or []
        bids = book.get("bids") or []
        if not asks or not bids:
            raise ValueError("empty asks or bids — need both for YES+NO")

        try:
            yes_ask_price = Decimal(str(asks[0][0]))
            yes_ask_size = Decimal(str(asks[0][1]))
            yes_bid_price = Decimal(str(bids[0][0]))
            yes_bid_size = Decimal(str(bids[0][1]))
        except (IndexError, InvalidOperation, TypeError) as e:
            raise ValueError(f"bad book row: {e}") from e

        # YES quote: ask is direct top-of-book; bid we keep for completeness.
        yes_q = Quote(
            side=OutcomeSide.YES,
            ask_price=yes_ask_price,
            ask_size=yes_ask_size,
            bid_price=yes_bid_price,
            bid_size=yes_bid_size,
        )

        # NO quote derived: NO_ask is the complement of best YES bid.
        no_ask_price = Decimal(1) - yes_bid_price
        no_ask_size = yes_bid_size
        no_bid_price = Decimal(1) - yes_ask_price
        no_bid_size = yes_ask_size
        no_q = Quote(
            side=OutcomeSide.NO,
            ask_price=no_ask_price,
            ask_size=no_ask_size,
            bid_price=no_bid_price,
            bid_size=no_bid_size,
        )
        return yes_q, no_q

    @staticmethod
    def _build_market_quote(
        m: dict[str, Any], yes_q: Quote, no_q: Quote, now: datetime
    ) -> MarketQuote:
        title = str(m.get("title") or m.get("question") or "").strip()
        if not title:
            raise ValueError("empty title")

        market_id = str(m.get("id"))
        slug = m.get("categorySlug") or market_id

        # Cross-listing — ground truth from the venue itself, no fuzzy matching needed.
        linked_raw = m.get("polymarketConditionIds") or []
        linked = tuple(str(x) for x in linked_raw if x)

        expires = (
            PredictFunClient._parse_dt(m.get("boostEndsAt"))
            or PredictFunClient._parse_dt(m.get("createdAt"))
        )

        return MarketQuote(
            venue=Venue.PREDICTFUN,
            venue_market_id=market_id,
            title=title,
            expires_at=expires,
            yes=yes_q,
            no=no_q,
            fetched_at=now,
            url=f"https://predict.fun/market/{slug}",
            linked_market_ids=linked,
        )

    @staticmethod
    def _parse_dt(s: Any) -> datetime | None:
        if not s:
            return None
        try:
            return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
        except ValueError:
            try:
                return datetime.fromtimestamp(int(s), tz=timezone.utc)
            except (ValueError, TypeError):
                return None

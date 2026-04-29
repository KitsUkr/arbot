from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
import structlog

from ..models import MarketQuote, OutcomeSide, Quote, Venue

log = structlog.get_logger(__name__)


class PredictFunClient:
    """Predict.fun REST client.

    NOTE: endpoint paths and JSON field names below are best-effort placeholders
    until the real API contract is confirmed. All field lookups try multiple
    common names so cosmetic differences don't break the pipeline. Localize any
    schema changes inside `_to_market_quote` / `_extract_outcome`.
    """

    venue_name = "predictfun"
    _ENDPOINT_MARKETS = "/markets"

    def __init__(
        self,
        api_url: str,
        max_markets: int = 200,
        timeout_seconds: float = 15.0,
    ) -> None:
        self._api_url = api_url.rstrip("/")
        self._max_markets = max_markets
        self._http = httpx.AsyncClient(
            timeout=timeout_seconds,
            headers={"User-Agent": "arbot/0.1", "Accept": "application/json"},
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def fetch_markets(self) -> list[MarketQuote]:
        try:
            r = await self._http.get(
                f"{self._api_url}{self._ENDPOINT_MARKETS}",
                params={"status": "active", "limit": self._max_markets},
            )
            r.raise_for_status()
            payload = r.json()
        except httpx.HTTPError as e:
            log.warning("predictfun.fetch_failed", error=str(e))
            return []

        items = self._unwrap_list(payload)
        out: list[MarketQuote] = []
        now = datetime.now(timezone.utc)
        for raw in items[: self._max_markets]:
            try:
                q = self._to_market_quote(raw, now)
            except (KeyError, ValueError, TypeError, InvalidOperation) as e:
                log.debug("predictfun.skip_market", error=str(e))
                continue
            if q is not None:
                out.append(q)
        log.info("predictfun.quoted", n=len(out))
        return out

    @staticmethod
    def _unwrap_list(payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        if isinstance(payload, dict):
            for key in ("data", "markets", "results", "items"):
                v = payload.get(key)
                if isinstance(v, list):
                    return [x for x in v if isinstance(x, dict)]
        return []

    def _to_market_quote(self, raw: dict[str, Any], now: datetime) -> MarketQuote | None:
        title = str(raw.get("title") or raw.get("question") or raw.get("name") or "").strip()
        market_id = str(raw.get("id") or raw.get("market_id") or raw.get("address") or "")
        if not title or not market_id:
            return None

        expires = self._parse_dt(
            raw.get("close_time")
            or raw.get("end_date")
            or raw.get("endDate")
            or raw.get("expires_at")
            or raw.get("resolution_time")
        )

        outcomes = raw.get("outcomes") or raw.get("tokens") or raw.get("markets") or []
        if not isinstance(outcomes, list):
            return None

        yes_q = self._extract_outcome(outcomes, OutcomeSide.YES)
        no_q = self._extract_outcome(outcomes, OutcomeSide.NO)
        if yes_q is None or no_q is None:
            return None

        slug = raw.get("slug") or market_id
        return MarketQuote(
            venue=Venue.PREDICTFUN,
            venue_market_id=market_id,
            title=title,
            expires_at=expires,
            yes=yes_q,
            no=no_q,
            fetched_at=now,
            url=f"https://predict.fun/markets/{slug}",
        )

    @staticmethod
    def _extract_outcome(outcomes: list[Any], side: OutcomeSide) -> Quote | None:
        target = side.value
        match: dict[str, Any] | None = None
        for o in outcomes:
            if not isinstance(o, dict):
                continue
            label = str(o.get("name") or o.get("side") or o.get("outcome") or "").strip().lower()
            if label == target:
                match = o
                break
        if match is None:
            return None

        ask = (
            match.get("ask")
            or match.get("ask_price")
            or match.get("best_ask")
            or match.get("price")
        )
        if ask is None:
            return None
        ask_size = (
            match.get("ask_size")
            or match.get("liquidity")
            or match.get("size")
            or 0
        )
        bid = match.get("bid") or match.get("best_bid") or match.get("bid_price")
        bid_size = match.get("bid_size") or 0

        try:
            return Quote(
                side=side,
                ask_price=Decimal(str(ask)),
                ask_size=Decimal(str(ask_size)),
                bid_price=Decimal(str(bid)) if bid is not None else None,
                bid_size=Decimal(str(bid_size)) if bid is not None else None,
            )
        except (InvalidOperation, ValueError):
            return None

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

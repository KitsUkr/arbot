from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
from decimal import Decimal

import structlog

from .models import MarketPair, MarketQuote, Opportunity, Venue

log = structlog.get_logger(__name__)


class ArbitrageDetector:
    """Detects complementary binary arbitrage:
        Buy YES on venue X (ask) + Buy NO on venue Y (ask), if total + fees < 1.
    Tries both directions for each MarketPair.
    """

    def __init__(
        self,
        min_profit_threshold: Decimal | float | str = Decimal("0.01"),
        min_liquidity_usd: Decimal | float | str = Decimal("50"),
        fee_bps_by_venue: dict[Venue, int] | None = None,
    ) -> None:
        self._min_profit = Decimal(str(min_profit_threshold))
        self._min_liquidity = Decimal(str(min_liquidity_usd))
        self._fee_rates: dict[Venue, Decimal] = {
            v: Decimal(bps) / Decimal(10_000)
            for v, bps in (fee_bps_by_venue or {}).items()
        }

    def detect(self, pairs: Iterable[MarketPair]) -> list[Opportunity]:
        out: list[Opportunity] = []
        for pair in pairs:
            opp = self._build(pair, yes_from=pair.a, no_from=pair.b)
            if opp is not None:
                out.append(opp)
            opp = self._build(pair, yes_from=pair.b, no_from=pair.a)
            if opp is not None:
                out.append(opp)
        return out

    def _build(
        self,
        pair: MarketPair,
        yes_from: MarketQuote,
        no_from: MarketQuote,
    ) -> Opportunity | None:
        yes_q = yes_from.yes
        no_q = no_from.no

        if yes_q.ask_notional_usd < self._min_liquidity:
            return None
        if no_q.ask_notional_usd < self._min_liquidity:
            return None

        cost = yes_q.ask_price + no_q.ask_price
        fee_yes = yes_q.ask_price * self._fee_rates.get(yes_from.venue, Decimal(0))
        fee_no = no_q.ask_price * self._fee_rates.get(no_from.venue, Decimal(0))
        fees = fee_yes + fee_no
        net_cost = cost + fees

        profit = Decimal(1) - net_cost
        if profit < self._min_profit:
            return None
        if net_cost <= 0:
            return None

        roi = profit / net_cost
        max_pairs = min(yes_q.ask_size, no_q.ask_size)
        max_profit_usd = profit * max_pairs

        return Opportunity(
            pair=pair,
            buy_yes_venue=yes_from.venue,
            buy_no_venue=no_from.venue,
            yes_price=yes_q.ask_price,
            no_price=no_q.ask_price,
            cost=cost,
            fees=fees,
            net_cost=net_cost,
            profit_per_share=profit,
            roi=roi,
            max_size_shares=max_pairs,
            max_profit_usd=max_profit_usd,
            detected_at=datetime.now(timezone.utc),
        )

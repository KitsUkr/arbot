from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Venue(StrEnum):
    POLYMARKET = "polymarket"
    PREDICTFUN = "predictfun"


class OutcomeSide(StrEnum):
    YES = "yes"
    NO = "no"


class Quote(BaseModel):
    """Top of book for one outcome side. Ask = lowest sell offer (we are buyers)."""

    model_config = ConfigDict(frozen=True)

    side: OutcomeSide
    ask_price: Decimal
    ask_size: Decimal
    bid_price: Decimal | None = None
    bid_size: Decimal | None = None

    @property
    def ask_notional_usd(self) -> Decimal:
        return self.ask_price * self.ask_size

    @model_validator(mode="after")
    def _check(self) -> Self:
        if not (Decimal(0) < self.ask_price < Decimal(1)):
            raise ValueError(f"ask_price must be in (0,1): {self.ask_price}")
        if self.ask_size < 0:
            raise ValueError(f"ask_size must be non-negative: {self.ask_size}")
        return self


class MarketQuote(BaseModel):
    """A binary market on one venue with current YES and NO top-of-book."""

    model_config = ConfigDict(frozen=True)

    venue: Venue
    venue_market_id: str
    title: str
    expires_at: datetime | None
    yes: Quote
    no: Quote
    fetched_at: datetime
    url: str | None = None
    # IDs of equivalent markets on OTHER venues, when the venue itself
    # advertises a cross-listing (e.g. predict.fun's `polymarketConditionIds`).
    # Used by the matcher for direct ID-based pairing — bypasses fuzzy matching.
    linked_market_ids: tuple[str, ...] = Field(default_factory=tuple)

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.yes.side is not OutcomeSide.YES:
            raise ValueError("yes.side must be YES")
        if self.no.side is not OutcomeSide.NO:
            raise ValueError("no.side must be NO")
        if not self.title.strip():
            raise ValueError("title must be non-empty")
        return self


class MarketPair(BaseModel):
    """Two markets across venues believed to refer to the same real-world event."""

    model_config = ConfigDict(frozen=True)

    a: MarketQuote
    b: MarketQuote
    similarity: int  # 100 for direct ID match, fuzzy score (0..100) otherwise
    expiry_delta_hours: float | None
    match_method: str = "fuzzy"  # "direct" | "fuzzy"

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.a.venue == self.b.venue:
            raise ValueError("MarketPair must span two different venues")
        return self


class Opportunity(BaseModel):
    """Complementary arbitrage: buy YES on one venue, NO on the other."""

    model_config = ConfigDict(frozen=True)

    pair: MarketPair
    buy_yes_venue: Venue
    buy_no_venue: Venue
    yes_price: Decimal
    no_price: Decimal
    cost: Decimal
    fees: Decimal
    net_cost: Decimal
    payout: Decimal = Decimal(1)
    profit_per_share: Decimal
    roi: Decimal
    max_size_shares: Decimal
    max_profit_usd: Decimal
    detected_at: datetime

    @property
    def fingerprint(self) -> str:
        a_id = self.pair.a.venue_market_id
        b_id = self.pair.b.venue_market_id
        cost_bucket = f"{self.net_cost:.2f}"
        return f"{a_id}|{b_id}|{self.buy_yes_venue.value}|{cost_bucket}"

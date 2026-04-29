from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..models import MarketQuote


@runtime_checkable
class MarketDataClient(Protocol):
    """Pulls current top-of-book quotes for active binary markets on a venue."""

    venue_name: str

    async def fetch_markets(self) -> list[MarketQuote]: ...
    async def aclose(self) -> None: ...

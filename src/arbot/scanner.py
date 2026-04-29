from __future__ import annotations

import asyncio

import structlog

from .clients.base import MarketDataClient
from .dedup import TTLDedup
from .detector import ArbitrageDetector
from .matcher import MarketMatcher
from .notifier import TelegramNotifier

log = structlog.get_logger(__name__)


class Scanner:
    def __init__(
        self,
        client_a: MarketDataClient,
        client_b: MarketDataClient,
        matcher: MarketMatcher,
        detector: ArbitrageDetector,
        notifier: TelegramNotifier,
        dedup: TTLDedup,
        poll_interval_seconds: int,
    ) -> None:
        self._a = client_a
        self._b = client_b
        self._matcher = matcher
        self._detector = detector
        self._notifier = notifier
        self._dedup = dedup
        self._poll = poll_interval_seconds
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        log.info(
            "scanner.starting",
            venue_a=self._a.venue_name,
            venue_b=self._b.venue_name,
            poll_interval=self._poll,
        )
        await self._notifier.send_text("✅ Arbot scanner started")
        loop = asyncio.get_running_loop()
        try:
            while not self._stop.is_set():
                t0 = loop.time()
                try:
                    await self._cycle()
                except Exception as e:
                    log.exception("scanner.cycle_failed", error=str(e))
                elapsed = loop.time() - t0
                wait = max(0.0, self._poll - elapsed)
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=wait)
                except asyncio.TimeoutError:
                    pass
        finally:
            log.info("scanner.stopping")
            try:
                await self._notifier.send_text("⏹ Arbot scanner stopped")
            except Exception:
                pass

    async def _cycle(self) -> None:
        markets_a, markets_b = await asyncio.gather(
            self._a.fetch_markets(),
            self._b.fetch_markets(),
        )
        log.info("scanner.fetched", a=len(markets_a), b=len(markets_b))
        if not markets_a or not markets_b:
            return

        pairs = self._matcher.match(markets_a, markets_b)
        log.info("scanner.matched", pairs=len(pairs))
        if not pairs:
            return

        opps = self._detector.detect(pairs)
        log.info("scanner.detected", opps=len(opps))

        for opp in opps:
            if not self._dedup.is_new(opp.fingerprint):
                continue
            log.info(
                "opportunity.alert",
                title=opp.pair.a.title,
                yes_venue=opp.buy_yes_venue.value,
                no_venue=opp.buy_no_venue.value,
                net_cost=str(opp.net_cost),
                roi=f"{opp.roi * 100:.2f}%",
            )
            await self._notifier.send_opportunity(opp)

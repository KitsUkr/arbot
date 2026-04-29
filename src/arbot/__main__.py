from __future__ import annotations

import asyncio
import signal

import structlog

from .clients.polymarket import PolymarketClient
from .clients.predictfun import PredictFunClient
from .config import get_settings
from .dedup import TTLDedup
from .detector import ArbitrageDetector
from .logging import configure_logging
from .matcher import MarketMatcher
from .models import Venue
from .notifier import TelegramNotifier
from .scanner import Scanner


async def amain() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_json)
    log = structlog.get_logger("arbot")

    polymarket = PolymarketClient(
        gamma_url=settings.polymarket_gamma_url,
        clob_url=settings.polymarket_clob_url,
    )
    predictfun = PredictFunClient(
        api_url=settings.predictfun_api_url,
        api_key=settings.predictfun_api_key,
        max_markets=settings.predictfun_max_markets,
        concurrency=settings.predictfun_concurrency,
    )
    matcher = MarketMatcher(
        title_similarity_threshold=settings.title_similarity_threshold,
        max_expiry_delta_hours=settings.max_expiry_delta_hours,
    )
    detector = ArbitrageDetector(
        min_profit_threshold=settings.min_profit_threshold,
        min_liquidity_usd=settings.min_liquidity_usd,
        fee_bps_by_venue={
            Venue.POLYMARKET: settings.polymarket_fee_bps,
            Venue.PREDICTFUN: settings.predictfun_fee_bps,
        },
    )
    notifier = TelegramNotifier(
        bot_token=settings.telegram_bot_token,
        chat_id=settings.telegram_chat_id,
    )
    dedup = TTLDedup(ttl_seconds=settings.dedup_ttl_seconds)

    scanner = Scanner(
        client_a=polymarket,
        client_b=predictfun,
        matcher=matcher,
        detector=detector,
        notifier=notifier,
        dedup=dedup,
        poll_interval_seconds=settings.poll_interval_seconds,
    )

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, scanner.stop)
        except NotImplementedError:
            pass

    try:
        await scanner.run()
    finally:
        await polymarket.aclose()
        await predictfun.aclose()
        await notifier.close()
        log.info("arbot.exit")


def cli() -> None:
    asyncio.run(amain())


if __name__ == "__main__":
    cli()

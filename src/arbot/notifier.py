from __future__ import annotations

from decimal import Decimal

import structlog
from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError

from .models import Opportunity, Venue

log = structlog.get_logger(__name__)

_VENUE_LABEL: dict[Venue, str] = {
    Venue.POLYMARKET: "Polymarket",
    Venue.PREDICTFUN: "Predict.fun",
}


def _escape(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def format_opportunity(opp: Opportunity, sample_stake_usd: Decimal = Decimal(100)) -> str:
    yes_venue = _VENUE_LABEL[opp.buy_yes_venue]
    no_venue = _VENUE_LABEL[opp.buy_no_venue]
    title = opp.pair.a.title

    pairs_for_sample = sample_stake_usd / opp.net_cost if opp.net_cost > 0 else Decimal(0)
    capped_pairs = min(pairs_for_sample, opp.max_size_shares)
    sample_profit = capped_pairs * opp.profit_per_share

    yes_url = (opp.pair.a if opp.buy_yes_venue == opp.pair.a.venue else opp.pair.b).url
    no_url = (opp.pair.a if opp.buy_no_venue == opp.pair.a.venue else opp.pair.b).url

    lines = [
        "🔥 <b>Arbitrage Opportunity</b>",
        f"<b>Market:</b> {_escape(title)}",
        f"<b>Buy YES:</b> {yes_venue} @ {opp.yes_price:.4f}",
        f"<b>Buy NO:</b>  {no_venue} @ {opp.no_price:.4f}",
        f"<b>Total Cost:</b> ${opp.net_cost:.4f}  "
        f"(gross ${opp.cost:.4f} + fees ${opp.fees:.4f})",
        "<b>Guaranteed Payout:</b> $1.0000",
        f"<b>Profit / share-pair:</b> ${opp.profit_per_share:.4f}",
        f"<b>ROI:</b> {opp.roi * 100:.2f}%",
        f"<b>Top-of-book size:</b> {opp.max_size_shares:.2f} pairs "
        f"(max profit ${opp.max_profit_usd:.2f})",
        f"<b>For ${sample_stake_usd:.0f} stake:</b> ≈ ${sample_profit:.2f} profit "
        f"({capped_pairs:.2f} pairs)",
        f"<b>Title match:</b> {opp.pair.similarity}/100",
    ]
    if yes_url or no_url:
        link_bits = []
        if yes_url:
            link_bits.append(f'<a href="{_escape(yes_url)}">{yes_venue}</a>')
        if no_url:
            link_bits.append(f'<a href="{_escape(no_url)}">{no_venue}</a>')
        lines.append("<b>Links:</b> " + " · ".join(link_bits))
    return "\n".join(lines)


class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str) -> None:
        self._bot = Bot(
            token=bot_token,
            default=DefaultBotProperties(parse_mode=ParseMode.HTML),
        )
        self._chat_id = chat_id

    async def send_opportunity(self, opp: Opportunity) -> None:
        await self._send(format_opportunity(opp))

    async def send_text(self, text: str) -> None:
        await self._send(text)

    async def _send(self, text: str) -> None:
        try:
            await self._bot.send_message(
                self._chat_id,
                text,
                disable_web_page_preview=True,
            )
        except TelegramAPIError as e:
            log.error("telegram.send_failed", error=str(e))

    async def close(self) -> None:
        await self._bot.session.close()

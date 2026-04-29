from datetime import datetime, timezone
from decimal import Decimal

from arbot.detector import ArbitrageDetector
from arbot.models import MarketPair, MarketQuote, OutcomeSide, Quote, Venue


def _q(side: OutcomeSide, ask: str, size: str = "1000") -> Quote:
    return Quote(side=side, ask_price=Decimal(ask), ask_size=Decimal(size))


def _mkt(venue: Venue, mid: str, yes_ask: str, no_ask: str, size: str = "1000") -> MarketQuote:
    return MarketQuote(
        venue=venue,
        venue_market_id=mid,
        title="some market",
        expires_at=None,
        yes=_q(OutcomeSide.YES, yes_ask, size),
        no=_q(OutcomeSide.NO, no_ask, size),
        fetched_at=datetime.now(timezone.utc),
    )


def _pair(a: MarketQuote, b: MarketQuote) -> MarketPair:
    return MarketPair(a=a, b=b, similarity=95, expiry_delta_hours=0.0)


def test_detects_complementary_arb():
    a = _mkt(Venue.POLYMARKET, "p1", yes_ask="0.50", no_ask="0.92")
    b = _mkt(Venue.PREDICTFUN, "f1", yes_ask="0.01", no_ask="0.99")
    opps = ArbitrageDetector(min_profit_threshold="0.01", min_liquidity_usd="1").detect([_pair(a, b)])
    assert len(opps) == 1
    o = opps[0]
    assert o.buy_yes_venue == Venue.PREDICTFUN
    assert o.buy_no_venue == Venue.POLYMARKET
    assert o.yes_price == Decimal("0.01")
    assert o.no_price == Decimal("0.92")
    assert o.net_cost == Decimal("0.93")
    assert o.profit_per_share == Decimal("0.07")


def test_no_arb_when_too_expensive():
    a = _mkt(Venue.POLYMARKET, "p1", yes_ask="0.50", no_ask="0.55")
    b = _mkt(Venue.PREDICTFUN, "f1", yes_ask="0.50", no_ask="0.55")
    assert ArbitrageDetector(
        min_profit_threshold="0.01", min_liquidity_usd="1"
    ).detect([_pair(a, b)]) == []


def test_below_threshold_filtered():
    a = _mkt(Venue.POLYMARKET, "p1", yes_ask="0.50", no_ask="0.495")
    b = _mkt(Venue.PREDICTFUN, "f1", yes_ask="0.50", no_ask="0.50")
    assert ArbitrageDetector(
        min_profit_threshold="0.01", min_liquidity_usd="1"
    ).detect([_pair(a, b)]) == []


def test_liquidity_guard_blocks_thin_books():
    a = _mkt(Venue.POLYMARKET, "p1", yes_ask="0.50", no_ask="0.92", size="50")
    b = _mkt(Venue.PREDICTFUN, "f1", yes_ask="0.01", no_ask="0.99", size="50")
    assert ArbitrageDetector(
        min_profit_threshold="0.01", min_liquidity_usd="50"
    ).detect([_pair(a, b)]) == []


def test_fees_reduce_profit_correctly():
    a = _mkt(Venue.POLYMARKET, "p1", yes_ask="0.50", no_ask="0.92")
    b = _mkt(Venue.PREDICTFUN, "f1", yes_ask="0.01", no_ask="0.99")
    detector = ArbitrageDetector(
        min_profit_threshold="0.01",
        min_liquidity_usd="1",
        fee_bps_by_venue={Venue.POLYMARKET: 100, Venue.PREDICTFUN: 100},
    )
    opps = detector.detect([_pair(a, b)])
    assert len(opps) == 1
    assert abs(opps[0].fees - Decimal("0.0093")) < Decimal("1e-9")


def test_max_size_is_min_of_legs():
    a = _mkt(Venue.POLYMARKET, "p1", yes_ask="0.50", no_ask="0.92", size="100")
    b = _mkt(Venue.PREDICTFUN, "f1", yes_ask="0.01", no_ask="0.99", size="500")
    opps = ArbitrageDetector(min_profit_threshold="0.01", min_liquidity_usd="1").detect(
        [_pair(a, b)]
    )
    assert opps[0].max_size_shares == Decimal("100")

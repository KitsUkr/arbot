from datetime import datetime, timedelta, timezone
from decimal import Decimal

from arbot.matcher import MarketMatcher
from arbot.models import MarketQuote, OutcomeSide, Quote, Venue


def _q(side: OutcomeSide, ask: str = "0.5") -> Quote:
    return Quote(side=side, ask_price=Decimal(ask), ask_size=Decimal("1000"))


def _mkt(venue: Venue, mid: str, title: str, expires=None) -> MarketQuote:
    return MarketQuote(
        venue=venue,
        venue_market_id=mid,
        title=title,
        expires_at=expires,
        yes=_q(OutcomeSide.YES, "0.4"),
        no=_q(OutcomeSide.NO, "0.55"),
        fetched_at=datetime.now(timezone.utc),
    )


def test_matches_similar_titles():
    a = _mkt(Venue.POLYMARKET, "1", "Will Bitcoin reach $100k by end of 2025?")
    b = _mkt(Venue.PREDICTFUN, "x", "Bitcoin to hit 100k by end of 2025")
    pairs = MarketMatcher(80).match([a], [b])
    assert len(pairs) == 1
    assert pairs[0].similarity >= 80


def test_rejects_dissimilar_titles():
    a = _mkt(Venue.POLYMARKET, "1", "Bitcoin reaches $100k")
    b = _mkt(Venue.PREDICTFUN, "x", "Trump wins 2024 election")
    assert MarketMatcher(80).match([a], [b]) == []


def test_rejects_far_expirations():
    near = datetime.now(timezone.utc) + timedelta(hours=24)
    far = near + timedelta(days=10)
    a = _mkt(Venue.POLYMARKET, "1", "Will Bitcoin hit 100k by year end", expires=near)
    b = _mkt(Venue.PREDICTFUN, "x", "Bitcoin hit 100k by year end", expires=far)
    assert MarketMatcher(80, max_expiry_delta_hours=72).match([a], [b]) == []


def test_accepts_close_expirations():
    e1 = datetime.now(timezone.utc) + timedelta(hours=24)
    e2 = e1 + timedelta(hours=12)
    a = _mkt(Venue.POLYMARKET, "1", "Will Bitcoin hit 100k by year end", expires=e1)
    b = _mkt(Venue.PREDICTFUN, "x", "Bitcoin hit 100k by year end", expires=e2)
    assert len(MarketMatcher(80, max_expiry_delta_hours=72).match([a], [b])) == 1


def test_handles_empty_inputs():
    assert MarketMatcher().match([], []) == []

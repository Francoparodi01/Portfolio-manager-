from decimal import Decimal
import re

from src.collector.cocos_scraper import MARKET_PRICE_RE
from src.collector.data.normalizer import parse_decimal


def _parse_market_price(text: str) -> Decimal:
    match = re.search(MARKET_PRICE_RE, text)
    assert match is not None
    parsed = parse_decimal(match.group(1))
    assert parsed is not None
    return parsed


def test_market_price_regex_accepts_one_or_two_decimal_places():
    assert _parse_market_price("$9.530,0") == Decimal("9530.0")
    assert _parse_market_price("$10.040,00") == Decimal("10040.00")

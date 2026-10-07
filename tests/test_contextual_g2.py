from datetime import datetime, timedelta, timezone
import math

import pytest

from src.collector.contextual_g2 import migration_sql, observations_from_provider_sequence
from src.collector.data.models import AssetType, Currency, MarketCandle


UTC = timezone.utc


def candle(index: int, *, volume=100.0, ticker="TEST") -> MarketCandle:
    ts = datetime(2026, 1, 2, 14, tzinfo=UTC) + timedelta(days=index)
    return MarketCandle(
        ticker=ticker,
        long_ticker=f"BYMA:{ticker}",
        asset_type=AssetType.CEDEAR,
        currency=Currency.ARS,
        venue="BYMA",
        interval="1d",
        ts=ts,
        open_price=100 + index,
        high_price=102 + index,
        low_price=99 + index,
        close_price=101 + index,
        volume=volume,
        source="TEST_PROVIDER",
    )


def observations(values):
    return observations_from_provider_sequence(
        values,
        owner_chat_id=123,
        ingestion_run_id="12345678-1234-4234-8234-123456789012",
        provider_symbol="BYMA:TEST",
        scraped_at=datetime(2026, 2, 1, 18, tzinfo=UTC),
        code_version="test-sha",
        price_unit="ARS_per_instrument",
    )


def test_provider_sequence_uses_observed_availability_and_next_bar_close_proof():
    result = observations([candle(0), candle(1), candle(2)])
    assert result[0].bar_start == result[0].candle_timestamp
    assert result[0].bar_end == result[1].bar_start
    assert result[0].is_closed is True
    assert result[0].available_at == result[0].scraped_at
    assert result[0].provenance["availability_semantics"] == "first_observed_by_quantia"
    assert result[0].provenance["close_semantics"] == "confirmed_by_next_provider_bar_start"
    assert result[-1].bar_end is None
    assert result[-1].is_closed is False
    assert "BAR_END_UNCONFIRMED_NO_LATER_PROVIDER_BAR" in result[-1].missingness


def test_missing_or_zero_volume_remains_missing_or_partial():
    result = observations([candle(0, volume=None), candle(1, volume=0), candle(2)])
    assert result[0].volume is None
    assert result[0].quality["volume_status"] == "UNKNOWN"
    assert "VOLUME_MISSING" in result[0].missingness
    assert result[1].volume == 0
    assert result[1].quality["volume_status"] == "PARTIAL"
    assert "VOLUME_ZERO" in result[1].missingness


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -1.0])
def test_nonfinite_or_negative_volume_is_rejected(bad):
    with pytest.raises(ValueError, match="INVALID_VOLUME"):
        observations([candle(0, volume=bad), candle(1)])


def test_mixed_instrument_sequence_is_rejected():
    with pytest.raises(ValueError, match="AMBIGUOUS_PROVIDER_SEQUENCE_IDENTITY"):
        observations([candle(0), candle(1, ticker="OTHER")])


def test_migration_is_additive_and_preserves_unknown_legacy_fields():
    sql = migration_sql().upper()
    assert "ADD COLUMN IF NOT EXISTS" in sql
    assert "DROP COLUMN" not in sql
    assert "RENAME COLUMN" not in sql
    assert "UPDATE MARKET_CANDLES" not in sql
    assert "MARKET_CANDLE_OBSERVATIONS" in sql
    assert "CONTEXTUAL_MARKET_SNAPSHOTS" in sql
    assert "SHADOW_ONLY" in sql


def test_nonfinite_ohlc_is_rejected():
    value = candle(0)
    invalid = MarketCandle(**{**value.__dict__, "close_price": math.nan})
    with pytest.raises(ValueError, match="INVALID_OHLC"):
        observations([invalid, candle(1)])

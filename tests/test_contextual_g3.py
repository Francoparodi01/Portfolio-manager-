from dataclasses import replace
from datetime import datetime, timedelta, timezone

from src.collector.contextual_g2 import (
    bar_identity,
    g3_migration_sql,
    observation_digest,
    observations_from_provider_sequence,
)
from src.collector.data.models import AssetType, Currency, MarketCandle


UTC = timezone.utc


def _candles() -> list[MarketCandle]:
    start = datetime(2026, 1, 2, 14, tzinfo=UTC)
    return [
        MarketCandle(
            ticker="TEST",
            long_ticker="BYMA:TEST",
            asset_type=AssetType.CEDEAR,
            currency=Currency.ARS,
            venue="BYMA",
            interval="1d",
            ts=start + timedelta(days=index),
            open_price=100 + index,
            high_price=102 + index,
            low_price=99 + index,
            close_price=101 + index,
            volume=1000 + index,
            source="G3_TEST_PROVIDER",
        )
        for index in range(3)
    ]


def _observe(values, *, run_id: str, scraped_at: datetime):
    return observations_from_provider_sequence(
        values,
        owner_chat_id=123,
        ingestion_run_id=run_id,
        provider_symbol="BYMA:TEST",
        scraped_at=scraped_at,
        code_version="g3-test-sha",
        price_unit="ARS_per_instrument",
        source_contract="g3-test-v1",
    )


def test_correction_keeps_bar_identity_and_changes_observation_identity():
    values_a = _candles()
    values_b = list(values_a)
    values_b[1] = replace(values_b[1], close_price=104.25, high_price=105.0)
    t1 = datetime(2026, 2, 1, 18, tzinfo=UTC)
    t2 = t1 + timedelta(minutes=10)

    a = _observe(
        values_a,
        run_id="12345678-1234-4234-8234-123456789012",
        scraped_at=t1,
    )[1]
    b = _observe(
        values_b,
        run_id="22345678-1234-4234-8234-123456789012",
        scraped_at=t2,
    )[1]

    assert bar_identity(a) == bar_identity(b)
    assert a.observation_id != b.observation_id
    assert a.observation_digest != b.observation_digest
    assert observation_digest(a) == a.observation_digest
    assert observation_digest(b) == b.observation_digest


def test_observation_digest_is_stable_for_same_evidence():
    observed_at = datetime(2026, 2, 1, 18, tzinfo=UTC)
    run_id = "12345678-1234-4234-8234-123456789012"
    first = _observe(_candles(), run_id=run_id, scraped_at=observed_at)
    second = _observe(_candles(), run_id=run_id, scraped_at=observed_at)
    assert [item.observation_id for item in first] == [item.observation_id for item in second]
    assert [item.observation_digest for item in first] == [item.observation_digest for item in second]


def test_observation_identity_canonicalizes_equivalent_timestamp_offsets():
    observed_utc = datetime(2026, 2, 1, 18, tzinfo=UTC)
    observed_art = observed_utc.astimezone(timezone(timedelta(hours=-3)))
    values_utc = _candles()
    values_art = [replace(item, ts=item.ts.astimezone(timezone(timedelta(hours=-3)))) for item in values_utc]
    run_id = "12345678-1234-4234-8234-123456789012"

    first = _observe(values_utc, run_id=run_id, scraped_at=observed_utc)
    second = _observe(values_art, run_id=run_id, scraped_at=observed_art)

    assert [item.observation_id for item in first] == [item.observation_id for item in second]
    assert [item.observation_digest for item in first] == [item.observation_digest for item in second]


def test_g3_migration_extends_existing_evidence_store_without_parallel_candles():
    sql = g3_migration_sql().upper()
    assert "ALTER TABLE MARKET_CANDLE_OBSERVATIONS" in sql
    assert "OBSERVATION_DIGEST" in sql
    assert "MARKET_EVIDENCE_CAPTURE_EVENTS" in sql
    assert "MARKET_CANDLE_OBSERVATIONS_G3_IDENTITY_KEY" in sql
    assert "CONTEXTUAL_SNAPSHOT_CANDLES_G3_ROLE_OBSERVATION_KEY" in sql
    assert "BEFORE TRUNCATE" in sql
    assert "CREATE TABLE MARKET_CANDLES" not in sql
    assert "DROP COLUMN" not in sql
    assert "RENAME COLUMN" not in sql

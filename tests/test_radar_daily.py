"""Source contracts and scheduling safety, independent of scoring formulas."""
import asyncio
from copy import deepcopy
from datetime import date, datetime
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.collector.radar_close_evidence import (
    ART, HISTORY, assess_close, eligible_session, latest_due_session, validate_candles,
)
from scripts.run_daily_top10 import run_daily
from src.monitor.opportunities import present_top_opportunities
from src.core.top10_telegram import render_top10
from tests.test_top10_telegram import records

DAY = date(2026, 10, 19)
NOW = datetime(2026, 10, 19, 19, tzinfo=ART)


def primary():
    return dict(evidence=dict(received_at=NOW.isoformat(), symbol_metadata=dict(
        exchange="BYMA", currency_code="ARS", name="TEST", delay=1200, settlement_type="2")),
        candles=[dict(ticker="TEST", currency="ARS", venue="BYMA", interval="1d",
            asset_type="CEDEAR", ts="2026-10-19T13:30:00+00:00",
            open_price=98, high_price=103, low_price=97, close_price=100, volume=200)])


def independent():
    return dict(source="BYMADATA", url=HISTORY, status=200, received_at=NOW.isoformat(),
        request=dict(symbol="TEST", maturity="2", fromDate=str(DAY), toDate=str(DAY)),
        instrument=dict(symbol="TEST", market="BYMA", denominationCcy="ARS",
                        settlementType="2", securityType="CD"),
        response_sha256="a"*64,
        response=dict(data=[dict(fecha="2026-10-19 00:00:00.0", precioCierre=100, volumenNominal=200)]))


def test_confirmation_needs_dated_identity_not_clock_alone():
    result = assess_close(primary(), independent(), DAY, NOW)
    assert result["confirmed_close"]["price"] == 100
    assert result["independent_close"]["volume_unit"] == "nominales"
    assert result["volume_comparison"] == "MATCH"
    assert not result["missing_data"]
    assert assess_close(primary(), None, DAY, NOW)["confirmed_close"] is None


@pytest.mark.parametrize("fault", ["same_upstream", "stale", "duplicate", "currency",
    "symbol", "settlement", "negative", "nan", "future", "intraday", "delay_unknown"])
def test_invalid_secondary_or_primary_never_confirms(fault):
    p, e = primary(), independent()
    if fault == "same_upstream": e["source"] = "YAHOO_ICE"
    if fault == "stale": e["response"]["data"][0]["fecha"] = "2026-10-16 00:00:00.0"
    if fault == "duplicate": e["response"]["data"] *= 2
    if fault == "currency": e["instrument"]["denominationCcy"] = "USD"
    if fault == "symbol": e["request"]["symbol"] = "OTHER"
    if fault == "settlement": e["instrument"]["settlementType"] = "1"
    if fault == "negative": e["response"]["data"][0]["volumenNominal"] = -1
    if fault == "nan": e["response"]["data"][0]["precioCierre"] = "NaN"
    if fault == "future": e["received_at"] = "2026-10-20T19:00:00-03:00"
    if fault == "intraday": p["evidence"]["received_at"] = "2026-10-19T17:20:00-03:00"
    if fault == "delay_unknown": p["evidence"]["symbol_metadata"]["delay"] = None
    result = assess_close(p, e, DAY, NOW)
    assert result["confirmed_close"] is None
    assert result["missing_data"]


def test_matching_numbers_do_not_infer_settlement():
    p = primary()
    del p["evidence"]["symbol_metadata"]["settlement_type"]
    r = assess_close(p, independent(), DAY, NOW)
    assert r["close_comparison"] == "MATCH"
    assert r["confirmed_close"] is None
    assert not r["settlement_verified"]


def test_last_trade_and_official_close_are_not_silently_substituted():
    p, e = primary(), independent()
    before = deepcopy(p)
    e["response"]["data"][0]["precioCierre"] = 101
    r = assess_close(p, e, DAY, NOW)
    assert p == before
    assert r["independent_close"]["price"] == 101
    assert r["close_comparison"] == "DIFFERENT" and r["confirmed_close"] is None


def test_raw_response_checksum_and_parsed_content_must_agree():
    import hashlib
    e = independent()
    e["raw_response"] = json.dumps(e["response"])
    e["response_sha256"] = hashlib.sha256(e["raw_response"].encode()).hexdigest()
    assert assess_close(primary(), e, DAY, NOW)["confirmed_close"]
    e["response"]["data"][0]["precioCierre"] = 99
    assert assess_close(primary(), e, DAY, NOW)["confirmed_close"] is None
    e["response_sha256"] = "0"*64
    assert assess_close(primary(), e, DAY, NOW)["confirmed_close"] is None


def test_daily_bundles_cannot_use_legacy_partial_retry(tmp_path):
    from scripts.collect_radar_market import retry_collection
    (tmp_path/"manifest.json").write_text(json.dumps({"market_session": "2026-10-19"}))
    with pytest.raises(ValueError, match="new complete bundle"):
        asyncio.run(retry_collection(SimpleNamespace(retry_inputs=tmp_path)))


@pytest.mark.parametrize("snapshot,cutoff,valid", [
    ("2026-10-19T18:00:00-03:00", "2026-10-19T19:00:00-03:00", True),
    ("2026-10-20T01:00:00-03:00", "2026-10-20T08:00:00-03:00", True),
    ("2026-10-18T18:00:00-03:00", "2026-10-19T19:00:00-03:00", False),
    ("2026-10-20T09:00:00-03:00", "2026-10-20T08:00:00-03:00", False),
    ("2026-10-20T01:00:00-03:00", "2026-10-20T18:00:00-03:00", False),
])
def test_context_freshness_separates_market_day_from_recalculation(snapshot, cutoff, valid):
    from scripts.build_radar_market_snapshot import validate_owner_context
    call = lambda: validate_owner_context({"scraped_at": snapshot}, DAY, datetime.fromisoformat(cutoff))
    if valid:
        call()
    else:
        with pytest.raises(ValueError):
            call()


@pytest.mark.parametrize("fault", ["negative_volume", "missing_open", "wrong_high", "nan_close",
    "duplicate_day", "currency", "old_session", "timezone"])
def test_candle_quality_fails_without_filling_values(fault):
    p = primary()
    if fault == "negative_volume": p["candles"][0]["volume"] = -1
    if fault == "missing_open": p["candles"][0]["open_price"] = None
    if fault == "wrong_high": p["candles"][0]["high_price"] = 99
    if fault == "nan_close": p["candles"][0]["close_price"] = "NaN"
    if fault == "currency": p["candles"][0]["currency"] = "USD"
    if fault == "old_session": p["candles"][0]["ts"] = "2026-10-16T13:30:00+00:00"
    if fault == "timezone": p["candles"][0]["ts"] = "2026-10-19T13:30:00"
    if fault == "duplicate_day":
        p["candles"].append(dict(p["candles"][0], ts="2026-10-19T14:30:00+00:00"))
    with pytest.raises(ValueError):
        validate_candles(p, DAY)


@pytest.mark.parametrize("stamp,expected", [
    ("2026-10-19T17:44:59-03:00", None), ("2026-10-19T17:45:00-03:00", "2026-10-19"),
    ("2026-11-02T17:45:00-03:00", None), ("2026-11-02T18:45:00-03:00", "2026-11-02"),
    ("2026-10-12T19:00:00-03:00", None), ("2026-10-18T19:00:00-03:00", None),
    ("2027-01-05T19:00:00-03:00", None),
])
def test_schedule_version_holidays_and_delay(stamp, expected):
    result = eligible_session(datetime.fromisoformat(stamp))
    assert (str(result) if result else None) == expected


def args(tmp_path, publish=False):
    return SimpleNamespace(env_file=None, host=None, port=5432, output=tmp_path,
                           session=None, publish=publish)


def test_daily_default_only_writes_files(tmp_path):
    collector = AsyncMock()
    publisher = AsyncMock()
    run = dict(run_id="fixture", parameters={"independent_close_count": 1})
    result = asyncio.run(run_daily(args(tmp_path), now=NOW, collector=collector,
        builder=lambda _: ({}, run, []), publisher=publisher, ratios_loader=lambda _: "fixture"))
    assert result["status"] == "VALIDATED_FILES_ONLY"
    publisher.assert_not_awaited()
    assert collector.await_args.args[0].close_session == DAY


def test_no_official_evidence_blocks_publication(tmp_path):
    result = asyncio.run(run_daily(args(tmp_path), now=NOW, collector=AsyncMock(),
        builder=lambda _: ({}, {"parameters": {"independent_close_count": 0}}, []),
        ratios_loader=lambda _: "fixture"))
    assert result["status"] == "SOURCE_BLOCKED"


def test_publish_requires_explicit_enabled_environment(tmp_path, monkeypatch):
    monkeypatch.delenv("RADAR_TOP10_DAILY_ENABLED", raising=False)
    with pytest.raises(ValueError):
        asyncio.run(run_daily(args(tmp_path, True), now=NOW))


@pytest.mark.parametrize("values,expected", [([False], "BUSY"), ([True, "prior-id"], "ALREADY_PUBLISHED")])
def test_daily_lock_and_idempotence_before_collection(tmp_path, monkeypatch, values, expected):
    monkeypatch.setenv("RADAR_TOP10_DAILY_ENABLED", "true")
    monkeypatch.setenv("DATABASE_URL", "postgresql://fixture")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "123")
    conn = SimpleNamespace(fetchval=AsyncMock(side_effect=values), close=AsyncMock())
    collector = AsyncMock()
    result = asyncio.run(run_daily(args(tmp_path, True), now=NOW,
        connector=AsyncMock(return_value=conn), collector=collector))
    assert result["status"] == expected
    collector.assert_not_awaited()
    conn.close.assert_awaited_once()
    assert not list(tmp_path.iterdir())


def test_failed_collection_releases_lock_and_preserves_failure(tmp_path, monkeypatch):
    monkeypatch.setenv("RADAR_TOP10_DAILY_ENABLED", "true")
    monkeypatch.setenv("DATABASE_URL", "postgresql://fixture")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "123")
    conn = SimpleNamespace(fetchval=AsyncMock(side_effect=[True, None]), close=AsyncMock())
    with pytest.raises(TimeoutError):
        asyncio.run(run_daily(args(tmp_path, True), now=NOW, connector=AsyncMock(return_value=conn),
                             collector=AsyncMock(side_effect=TimeoutError)))
    conn.close.assert_awaited_once()
    receipts = list(tmp_path.rglob("job-result.json"))
    assert len(receipts) == 1 and json.loads(receipts[0].read_text())["status"] == "FAILED"


def test_daily_capture_expires_when_next_due_session_has_no_run():
    run, rows = records()
    run["parameters"] = {"daily_job": "top10-daily-v1"}
    run["captured_session"] = "2026-10-19"
    run["captured_at"] = NOW
    before = present_top_opportunities(run, rows, as_of=datetime(2026, 10, 20, 10, tzinfo=ART))
    after = present_top_opportunities(run, rows, as_of=datetime(2026, 10, 20, 18, tzinfo=ART))
    assert not before["stale"] and after["stale"]
    assert [r["ticker"] for r in before["items"]] == [r["ticker"] for r in after["items"]]
    assert all(r["display_state"] == "ESPERAR" for r in after["items"])
    assert "Captura vencida" in render_top10(after)[0]


def test_scheduler_registration_disabled_by_default(monkeypatch):
    from src.scheduler.runner import register_daily_top10
    from unittest.mock import Mock
    scheduler = Mock()
    monkeypatch.delenv("RADAR_TOP10_DAILY_ENABLED", raising=False)
    register_daily_top10(scheduler)
    scheduler.add_job.assert_not_called()
    monkeypatch.setenv("RADAR_TOP10_DAILY_ENABLED", "true")
    register_daily_top10(scheduler)
    assert scheduler.add_job.call_args.kwargs["max_instances"] == 1
    assert scheduler.add_job.call_args.kwargs["id"] == "radar_top10_daily"

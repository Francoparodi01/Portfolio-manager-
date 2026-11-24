from copy import deepcopy
from decimal import Decimal
import json

import pytest

from scripts.backtest_thesis_desk import capture_sql, main
from src.thesis_desk.historical_market import BASIS, recorded_daily_references


def quote(clock="16:45:00", price="100", day="2026-09-01"):
    return dict(ticker="AAA", asset_type="CEDEAR", currency="ARS", ts=f"{day}T{clock}-03:00", last_price=price)


def test_last_observation_window_end_and_no_price_adjustment_or_input_mutation():
    quotes = [quote("16:40:00", "100"), quote("16:55:00", "103"), quote("16:10:00", "999"), quote("17:00:00", "999")]
    original = deepcopy(quotes)
    bars, report = recorded_daily_references(quotes, "2026-11-12T20:00:00Z")
    assert quotes == original and len(bars) == 1
    bar = bars[0]
    assert bar["close_price"] == "103" and bar["adjustment_policy"] == "RAW"
    assert bar["scraped_at"] == "2026-09-01T16:55:00-03:00"
    assert bar["bar_end"] == bar["available_at"] == "2026-09-01T17:00:00-03:00"
    assert bar["basis"] == BASIS and not report["official_closes"]
    assert report["skipped"] == {"OUTSIDE_DEFINED_WINDOW": 2}


def test_no_partial_window_weekend_or_holiday_reference():
    bars, report = recorded_daily_references([quote(), quote(day="2026-08-30"), quote(day="2026-08-17")],
                                             "2026-09-01T16:59:59-03:00")
    assert bars == [] and report["skipped"]["WINDOW_NOT_CLOSED"] == 1


@pytest.mark.parametrize("price", ["0", "-1", "NaN", "Infinity"])
def test_invalid_prices_fail(price):
    with pytest.raises(ValueError):
        recorded_daily_references([quote(price=price)], "2026-11-12T20:00:00Z")


def test_conflict_future_timestamp_and_timezone_required():
    with pytest.raises(ValueError, match="Conflicting"):
        recorded_daily_references([quote(), quote(price="101")], "2026-11-12T20:00:00Z")
    with pytest.raises(ValueError):
        recorded_daily_references([quote(day="2027-01-01")], "2026-11-12T20:00:00Z")
    with pytest.raises(ValueError):
        recorded_daily_references([dict(quote(), ts="2026-09-01T16:45:00")], "2026-11-12T20:00:00Z")


def test_capture_includes_warmup_and_retains_default_contract():
    _, default = capture_sql("2026-09-01", "2026-10-31")
    sql, queries = capture_sql("2026-09-01", "2026-10-31", recorded_closes=True)
    assert len(default) == 4 and len(queries) == 5
    assert "READ ONLY" in sql and "2026-05-04" in queries["recorded_close_quotes"]
    assert "16:30" in queries["recorded_close_quotes"] and "17:00" in queries["recorded_close_quotes"]


def test_offline_runner_exports_reconstruction_and_refuses_missing_source(tmp_path):
    from tests.test_thesis_desk_backtest import fixture
    rows, evidence = fixture()
    rows["recorded_close_quotes"] = [quote(day="2026-08-31")]
    capture = tmp_path / "capture.json"
    capture.write_text(json.dumps(dict(captured_at="2026-11-12T20:00:00Z", observations=[dict(name=k, rows=v) for k,v in rows.items()])))
    ev = tmp_path / "evidence.json"
    ev.write_text(json.dumps(evidence))
    output = tmp_path / "run"
    assert main(["--capture", str(capture), "--evidence", str(ev), "--start", "2026-09-01", "--end", "2026-09-01",
                 "--output", str(output), "--recorded-closes"]) == 0
    assert json.loads((output / "price-recovery.json").read_text())["daily_references"] == 1
    assert json.loads((output / "BASE_V1.json").read_text())["technical_price_basis"] == BASIS
    manifest = json.loads((output / "manifest.json").read_text())
    assert "recorded-daily-references.json" in manifest["artifacts"]
    rows.pop("recorded_close_quotes")
    capture.write_text(json.dumps(dict(captured_at="2026-11-12T20:00:00Z", observations=[dict(name=k, rows=v) for k,v in rows.items()])))
    with pytest.raises(ValueError, match="recapture"):
        main(["--capture", str(capture), "--output", str(tmp_path / "missing"), "--recorded-closes"])

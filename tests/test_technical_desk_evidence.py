from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
import json

import pytest

from src.technical_desk.evidence_capture import content_hash, frame, normalize_rows, parse_frames
from src.technical_desk.packet_data import build_candidate


UTC = timezone.utc


def source_values(day, volume=100):
    return [int(datetime(2026, 11, day, 13, 30, tzinfo=UTC).timestamp()), 10, 12, 9, 11, volume]


def events(values):
    return [{"m": "timescale_update", "p": ["chart", {"s1": {"s": [{"v": row} for row in values]}}]}]


def fixture():
    capture = dict(ticker="AMD", requested_symbol="BYMA:AMD", requested_session="regular",
                   requested_adjustment="splits", interval="1D", download_complete=True,
                   available_at="2026-11-15T17:00:00+00:00",
                   source_metadata=dict(name="AMD", exchange="BYMA", currency_code="ARS", type="dr",
                       timezone="America/Argentina/Buenos_Aires", subsession_id="regular",
                       session="1030-1700#20261102/1030-1800", delay=1200),
                   bars=normalize_rows(events([source_values(d) for d in (6, 11, 12, 13)])))
    calendar = dict(timezone="America/Argentina/Buenos_Aires", captured_at="2026-11-15T18:00:00+00:00",
        verified_ranges=[dict(start="2026-11-01", end="2026-11-13")],
        closures=[dict(date=f"2026-11-{day}", trading=False) for day in ("09", "10")],
        last_expected_complete_session="2026-11-13", hours_rules=[dict(effective_from="2026-11-02",
            verified_through="2026-11-13", regular_close_local="18:00", closing_price_phase_end_local="18:15")])
    return capture, calendar


def build(capture, calendar, **kwargs):
    return build_candidate(capture, calendar, start_date="2026-11-06",
        cutoff=kwargs.pop("cutoff", datetime(2026, 11, 15, 19, tzinfo=UTC)),
        source_hash=kwargs.pop("source_hash", content_hash(capture)), **kwargs)


def test_download_frames_keep_precise_decimals_and_do_not_confirm_terminal_bar():
    body = frame("timescale_update", ["c", {"s1": {"s": [{"v": source_values(13, 0)}]}}])
    parsed = json.loads(parse_frames(body)[0], parse_float=Decimal)
    row = normalize_rows([parsed])[0]
    assert row["volume"] == Decimal(0) and row["volume_unit"] is None
    assert row["close_status"] == "UNCONFIRMED"


def test_partial_update_is_applied_and_null_volume_stays_missing():
    updated = source_values(12, None)
    updated[4] = 12
    bars = normalize_rows(events([source_values(12)]) + events([updated, source_values(13)]))
    assert len(bars) == 2 and bars[0]["close"] == 12 and bars[0]["volume"] is None
    assert bars[0]["close_confirmed_at"] == bars[1]["bar_start"]


@pytest.mark.parametrize("raw", ["bad", "~m~10~m~{}", "~m~2"])
def test_truncated_or_unframed_source_is_rejected(raw):
    with pytest.raises(ValueError):
        parse_frames(raw)


@pytest.mark.parametrize("values", [[1, 10, 9, 8, 11, 2], [1, 10, 12, 9, 11, -1], [1, "NaN", 12, 9, 11, 2]])
def test_impossible_price_or_volume_is_not_silently_dropped(values):
    with pytest.raises(ValueError):
        normalize_rows(events([values]))


def test_documented_close_guard_and_holidays_build_three_day_week_without_volume_assumption():
    capture, calendar = fixture()
    packet = build(capture, calendar)
    assert packet["quality"]["price_fresh"]
    assert packet["quality"]["missing_sessions"] == []
    assert len(packet["weekly"]) == 1
    week = packet["weekly"][0]
    assert len(week["component_hashes"]) == 3 and week["volume"] is None
    assert week["open"] == 10 and week["high"] == 12 and week["low"] == 9 and week["close"] == 11
    proof = packet["daily"][-1]["close_proof"]
    assert proof["safe_after"] == "2026-11-13T21:35:00+00:00"
    assert not proof["explicit_provider_final_flag"] and not proof["includes_postmarket"]
    assert packet["quality"]["ready"] is False and "VOLUME_UNIT_UNKNOWN" in packet["quality"]["blockers"]


def test_capture_before_delay_guard_does_not_turn_series_completed_into_close():
    capture, calendar = fixture()
    capture["available_at"] = "2026-11-13T21:10:00+00:00"
    packet = build(capture, calendar)
    assert packet["quality"]["missing_sessions"] == ["2026-11-13"]
    assert packet["weekly"] == [] and not packet["quality"]["price_fresh"]


def test_missing_midweek_is_not_replaced_by_weekly_aggregation():
    capture, calendar = fixture()
    capture["bars"] = [r for r in capture["bars"] if not r["bar_start"].startswith("2026-11-12")]
    packet = build(capture, calendar)
    assert packet["weekly"] == [] and packet["quality"]["missing_sessions"] == ["2026-11-12"]


def test_old_calendar_cannot_keep_marking_packets_fresh_after_its_covered_dates():
    capture, calendar = fixture()
    packet = build(capture, calendar, cutoff=datetime(2026, 11, 17, 22, tzinfo=UTC))
    assert not packet["quality"]["price_fresh"]
    assert "CALENDAR_NOT_COVERING_CURRENT_CUTOFF" in packet["quality"]["blockers"]


def test_verified_future_dates_do_not_extend_a_stale_last_complete_session_label():
    capture, calendar = fixture()
    calendar["verified_ranges"][0]["end"] = "2026-11-17"
    packet = build(capture, calendar, cutoff=datetime(2026, 11, 17, 22, tzinfo=UTC))
    assert not packet["quality"]["price_fresh"]
    assert "CALENDAR_LAST_COMPLETE_SESSION_NOT_REVALIDATED" in packet["quality"]["blockers"]


@pytest.mark.parametrize("change", ["identity", "source_future", "calendar_future", "hash", "bar_future"])
def test_identity_chronology_and_integrity_fail_closed(change):
    capture, calendar = fixture()
    supplied_hash = content_hash(capture)
    if change == "identity":
        capture["source_metadata"]["currency_code"] = "USD"
    elif change == "source_future":
        capture["available_at"] = "2026-11-16T17:00:00+00:00"
    elif change == "calendar_future":
        calendar["captured_at"] = "2026-11-16T18:00:00+00:00"
    elif change == "bar_future":
        capture["bars"][-1]["bar_start"] = "2026-11-16T13:30:00+00:00"
    else:
        capture["bars"][-1]["close"] = Decimal(10)
    with pytest.raises(ValueError):
        build(capture, calendar, source_hash=supplied_hash if change == "hash" else content_hash(capture))


def test_bar_and_week_hashes_cover_content_and_lineage():
    capture, calendar = fixture()
    packet = build(capture, calendar)
    for row in packet["daily"] + packet["weekly"]:
        unhashed = deepcopy(row)
        claimed = unhashed.pop("fact_hash")
        assert content_hash(unhashed) == claimed
        unhashed["close"] = Decimal(999)
        assert content_hash(unhashed) != claimed


@pytest.mark.parametrize("change, error", [
    ("missing", "FIVE_DISTINCT_CAPTURE_FILES_REQUIRED"),
    ("duplicate", "FIVE_DISTINCT_CAPTURE_FILES_REQUIRED"),
    ("blocked", "ALL_SOURCE_CAPTURES_REQUIRED"),
    ("identity", "CAPTURE_MANIFEST_IDENTITY_MISMATCH"),
])
def test_cli_rejects_manifest_cohort_or_capture_identity_mismatch(tmp_path, monkeypatch, change, error):
    from scripts.build_technical_desk_candidates import main

    capture_dir, original_dir = tmp_path / "capture", tmp_path / "original"
    capture_dir.mkdir()
    original_dir.mkdir()
    tickers = ["AMD", "GDX", "GOOGL", "MU", "NVDA"]
    capture, calendar = fixture()
    entries = []
    for ticker in tickers:
        source = deepcopy(capture)
        source.update(ticker=ticker, requested_symbol=f"BYMA:{ticker}")
        source["source_metadata"]["name"] = ticker
        if change == "identity" and ticker == "AMD":
            source["ticker"] = "NVDA"
        filename = ticker + ".json"
        (capture_dir / filename).write_text(json.dumps(source, default=str))
        entries.append(dict(ticker=ticker, status="CAPTURED", file=filename, content_hash=content_hash(source)))
    if change == "missing":
        entries.pop()
    elif change == "duplicate":
        entries[-1] = entries[0]
    elif change == "blocked":
        entries[0]["status"] = "SOURCE_BLOCKED"
    (capture_dir / "manifest.json").write_text(json.dumps(dict(status="CAPTURED", selected_tickers=tickers, captures=entries)))
    (original_dir / "rehash.json").write_text(json.dumps(dict(packets=[dict(ticker=t, first_bar="2026-11-06") for t in tickers])))
    calendar_path = tmp_path / "calendar.json"
    calendar_path.write_text(json.dumps(calendar))
    output = tmp_path / "output"
    monkeypatch.setattr("sys.argv", ["build", "--capture", str(capture_dir), "--calendar", str(calendar_path),
        "--original-packets", str(original_dir), "--output", str(output), "--cutoff", "2026-11-15T22:00:00+00:00"])
    with pytest.raises(ValueError, match=error):
        main()
    assert not (output / "manifest.json").exists()

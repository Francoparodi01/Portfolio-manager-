from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path

import pytest

from src.technical_desk.evidence_capture import content_hash, normalize_rows
from src.technical_desk.stage0c import (adjustment_volume_checks, build_stage0c_packet,
                                     relative_volume_features, role_input, terminal_atr)


POLICY = json.loads((Path(__file__).resolve().parents[1] / "docs/technical-desk-stage0c-policy.json").read_text())


def fixture():
    day = date(2025, 9, 17)
    values = []
    while day <= date(2025, 11, 28):
        if day.weekday() < 5 and day != date(2025, 10, 10):
            values.append([int(datetime.combine(day, datetime.min.time(), timezone.utc).timestamp())+14*3600, 10, 12, 9, 11, 100])
        day += timedelta(days=1)
    events = [{"m":"timescale_update", "p":["chart", {"s1":{"s":[{"v":v} for v in values]}}]}]
    capture = dict(ticker="AMD", requested_symbol="BYMA:AMD", requested_session="regular",
        requested_adjustment="splits", interval="1D", download_complete=True,
        available_at="2025-12-01T22:00:00+00:00",
        source_metadata=dict(name="AMD", exchange="BYMA", currency_code="ARS", type="dr",
            timezone="America/Argentina/Buenos_Aires", subsession_id="regular", session="1030-1700", delay=1200),
        bars=normalize_rows(events))
    calendar = dict(timezone="America/Argentina/Buenos_Aires", captured_at="2025-12-01T23:00:00+00:00",
        verified_ranges=[dict(start="2025-09-17",end="2025-10-09"),dict(start="2025-10-11",end="2025-11-30")],
        closures=[], last_expected_complete_session="2025-11-28",
        hours_rules=[dict(effective_from="2025-07-28",verified_through="2025-11-30",
                         regular_close_local="17:00",closing_price_phase_end_local="17:15")])
    return capture, calendar


def build():
    capture, calendar = fixture()
    return build_stage0c_packet(capture, calendar, original_start="2025-09-17",
        cutoff=datetime(2025,12,1,23,tzinfo=timezone.utc), source_hash=content_hash(capture),
        policy=POLICY, adjustments={"events":[],"coverage_status":"UNKNOWN"})


def test_relative_volume_is_causal_scale_invariant_and_warmup_is_null():
    rows = [dict(session_date=str(i),volume=100 if i<20 else 300) for i in range(22)]
    features = relative_volume_features(rows, 20)
    assert all(f["relative_volume"] is None for f in features[:20])
    assert features[20]["relative_volume"] == 3
    scaled = [dict(r, volume=r["volume"]*17) for r in rows]
    assert relative_volume_features(scaled,20) == features
    rows[-1]["volume"] = 999999
    assert relative_volume_features(rows,20)[:21] == features[:21]


@pytest.mark.parametrize("value,status", [(None,"MISSING_VOLUME"),(0,"ZERO_DENOMINATOR")])
def test_invalid_denominator_is_not_filled(value,status):
    rows = [dict(session_date=str(i),volume=value) for i in range(21)]
    assert relative_volume_features(rows,20)[-1]["status"] == status


@pytest.mark.parametrize("post,expected", [(300,True),(Decimal('33.333'),True),(200,False)])
def test_known_adjustment_level_jump_has_preregistered_symmetric_threshold(post, expected):
    rows = [dict(session_date=f"2026-01-{i+1:02}",volume=100 if i<10 else post) for i in range(20)]
    event = dict(instrument_id="BYMA:CEDEAR:AMD:ARS", date="2026-01-11", source="fixture")
    check, = adjustment_volume_checks(rows,[event],instrument_id=event["instrument_id"],window=10,threshold="3")
    assert check["status"] == "EVALUATED" and check["level_jump"] is expected
    assert check["action"] == "REPORT_ONLY_NO_AUTOMATIC_RESCALE"


def test_adjustment_missing_window_and_other_instrument_are_not_zero_jumps():
    event = dict(instrument_id="BYMA:CEDEAR:AMD:ARS",date="2026-01-11",source="fixture")
    rows = [dict(session_date="2026-01-11",volume=100)]
    checks = adjustment_volume_checks(rows,[event],instrument_id=event["instrument_id"],window=10,threshold=3)
    assert checks[0]["status"] == "INSUFFICIENT_WINDOW" and checks[0]["level_jump"] is None
    assert adjustment_volume_checks(rows,[event],instrument_id="BYMA:CEDEAR:NVDA:ARS",window=10,threshold=3) == []


def test_override_excludes_unknown_day_without_reclassifying_calendar_and_rehashes_lineage():
    capture, calendar = fixture()
    original_calendar = deepcopy(calendar)
    # Frozen Sunday cutoff: no future complete Monday inferred from its archive.
    capture["available_at"] = "2025-11-30T22:00:00+00:00"
    calendar["captured_at"] = "2025-11-30T23:00:00+00:00"
    expected_calendar = deepcopy(calendar)
    packet = build_stage0c_packet(capture,calendar,original_start="2025-09-17",
        cutoff=datetime(2025,11,30,23,tzinfo=timezone.utc),source_hash=content_hash(capture),policy=POLICY,
        adjustments={"events":[],"coverage_status":"UNKNOWN"})
    assert calendar == expected_calendar and original_calendar["verified_ranges"] == calendar["verified_ranges"]
    assert packet["history_start_override"] == "2025-10-13"
    assert packet["quality"]["status"] == "READY" and packet["volume_quality_status"] == "PARTIAL"
    assert packet["adjustment_check"]["status"] == "NO_KNOWN_EVENTS_NOT_EVIDENCE_OF_NONE"
    assert all(w["volume"] is None for w in packet["weekly"])
    hashes = {r["fact_hash"] for r in packet["daily"]}
    for row in packet["daily"]+packet["weekly"]:
        fields = dict(row)
        assert fields.pop("fact_hash") == content_hash(fields)
    assert all(set(w["component_hashes"]) <= hashes for w in packet["weekly"])
    wyckoff = role_input(packet,"wyckoff")
    assert all("volume" not in r and "relative_volume_feature" in r for r in wyckoff["daily"])
    assert all("volume" not in r for r in wyckoff["weekly"])
    assert role_input(packet,"elliott")["evaluation_cohort"] != wyckoff["evaluation_cohort"]


def test_non_volume_blockers_stay_partial():
    packet = build()
    assert packet["quality"]["status"] == "PARTIAL" and not packet["quality"]["ready"]
    assert "CALENDAR_NOT_COVERING_CURRENT_CUTOFF" in packet["quality"]["blockers"]


def test_wilder_atr_has_expected_seed_and_recursion():
    rows = [dict(high="12",low="9",close="11",fact_hash=str(i)) for i in range(15)]
    rows[-1]["high"] = "26"
    assert terminal_atr(rows)["value"] == 4
    assert terminal_atr(rows[:13])["value"] is None

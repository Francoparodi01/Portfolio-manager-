from copy import deepcopy
from datetime import date, timedelta
from decimal import Decimal, localcontext
import importlib.util
from pathlib import Path

import pytest

from src.technical_desk.volume_clusters import POLICY, cluster_volume_flags


def calendar():
    return dict(market="BYMA", weekmask=[0, 1, 2, 3, 4],
                verified_ranges=[dict(start="2026-01-01", end="2026-01-31")],
                closures=[dict(date="2026-01-01", trading=False)], special_sessions=[])


def data():
    rows, snapshots = [], []
    day = date(2026, 1, 2)
    while day <= date(2026, 1, 30):
        if day.weekday() < 5:
            stamp = day.isoformat() + "T13:30:00+00:00"
            rows.append(dict(session_date=day.isoformat(), bar_start=stamp, close="100"))
            snapshots.append(dict(time=stamp, known_at=day.isoformat()+"T21:00:00+00:00",
                                  indicators={"atr": dict(status="AVAILABLE", value="4", reason=None)}))
        day += timedelta(days=1)
    return rows, snapshots


def result(flags, rows=None, snapshots=None, cal=None):
    defaults = data()
    return cluster_volume_flags(flags, defaults[0] if rows is None else rows,
                                defaults[1] if snapshots is None else snapshots,
                                calendar() if cal is None else cal)


def test_cluster_distance_uses_byma_sessions_and_transitive_linkage():
    output = result(["2026-01-09", "2026-01-14", "2026-01-19", "2026-01-23"])
    assert output["cluster_count"] == 2
    first = output["clusters"][0]
    assert (first["start"], first["end"], first["flagged_count"]) == ("2026-01-09", "2026-01-19", 3)
    assert output["clusters"][1]["flagged_dates"] == ["2026-01-23"]
    assert output["use_in_causal_features"] is output["affects_analysis"] is output["affects_execution"] is False


def test_exchange_holiday_does_not_increase_session_distance():
    cal = calendar()
    cal["closures"].append(dict(date="2026-01-13", trading=False))
    assert result(["2026-01-09", "2026-01-15"], cal=cal)["cluster_count"] == 1


def test_first_flag_move_uses_previous_calendar_session_atr_only():
    rows, snapshots = data()
    rows[1]["close"] = "112"
    snapshots[1]["indicators"]["atr"]["value"] = "999"
    first = result(["2026-01-05"], rows, snapshots)["clusters"][0]
    assert first["previous_session"] == first["atr_session"] == "2026-01-02"
    assert first["price_move"] == Decimal("12")
    assert first["signed_move_atr"] == Decimal("3")
    assert first["atr_previous"] == Decimal("4")
    assert first["uses_candidate_session_atr"] is False


def test_signed_negative_move_is_not_absolutized():
    rows, snapshots = data()
    rows[1]["close"] = "90"
    first = result(["2026-01-05"], rows, snapshots)["clusters"][0]
    assert first["signed_move_atr"] == Decimal("-2.5")


def test_missing_prior_session_does_not_substitute_last_available_row():
    rows, snapshots = data()
    rows = [r for r in rows if r["session_date"] != "2026-01-08"]
    first = result(["2026-01-09"], rows, snapshots)["clusters"][0]
    assert first["movement_status"] == "NOT_EVALUABLE"
    assert first["movement_reason"] == "MISSING_FIRST_OR_PREVIOUS_SESSION_BAR"
    assert first["signed_move_atr"] is None


@pytest.mark.parametrize("value,status", [(None, "NOT_AVAILABLE"), ("0", "AVAILABLE"),
                                         ("-1", "AVAILABLE"), ("NaN", "AVAILABLE")])
def test_unavailable_zero_or_invalid_atr_stays_not_evaluable(value, status):
    rows, snapshots = data()
    snapshots[0]["indicators"]["atr"].update(value=value, status=status, reason="WARMUP")
    first = result(["2026-01-05"], rows, snapshots)["clusters"][0]
    assert first["movement_status"] == "NOT_EVALUABLE"
    assert first["signed_move_atr"] is None
    assert first["atr_source_reason"] == "WARMUP"


def test_missing_snapshot_stays_not_evaluable():
    rows, snapshots = data()
    first = result(["2026-01-05"], rows, snapshots[1:])["clusters"][0]
    assert first["atr_source_reason"] == "MISSING_SNAPSHOT"


def test_unknown_day_between_flags_is_not_silently_skipped():
    cal = calendar()
    cal["verified_ranges"] = [dict(start="2026-01-01", end="2026-01-12"),
                              dict(start="2026-01-14", end="2026-01-31")]
    output = result(["2026-01-09", "2026-01-14"], cal=cal)
    assert output["status"] == "NOT_EVALUABLE"
    assert output["cluster_count"] == 0
    assert output["unclustered_flags"] == ["2026-01-09", "2026-01-14"]


def test_unknown_prior_weekday_is_not_replaced_with_an_older_known_session():
    cal = calendar()
    cal["verified_ranges"] = [dict(start="2026-01-01", end="2026-01-08"),
                              dict(start="2026-01-10", end="2026-01-31")]
    first = result(["2026-01-12"], cal=cal)["clusters"][0]
    assert first["movement_reason"] == "PREVIOUS_SESSION_CALENDAR_UNKNOWN"
    assert first["previous_session"] is first["signed_move_atr"] is None


def test_flag_on_closed_session_is_not_evaluable():
    output = result(["2026-01-10"])
    assert output["status"] == "NOT_EVALUABLE"
    assert output["clusters"] == []


def test_policy_cannot_be_tuned_under_same_version():
    policy = deepcopy(POLICY)
    policy["maximum_consecutive_flag_session_distance"] = 4
    with pytest.raises(ValueError, match="POLICY_MISMATCH"):
        cluster_volume_flags([], [], [], calendar(), policy=policy)


def test_duplicate_flags_rejected_and_empty_flags_explicit():
    with pytest.raises(ValueError, match="FLAGS_NOT_STRICTLY_INCREASING"):
        result(["2026-01-05", "2026-01-05"])
    output = result([])
    assert output["status"] == "EVALUATED"
    assert output["cluster_count"] == output["flagged_count"] == 0


def test_decimal_calculation_does_not_depend_on_caller_context():
    rows, snapshots = data()
    rows[1]["close"] = "112.12345"
    expected = result(["2026-01-05"], rows, snapshots)
    with localcontext() as context:
        context.prec = 3
        assert result(["2026-01-05"], rows, snapshots) == expected


@pytest.mark.parametrize("field", ["close", "bar_start"])
def test_missing_first_or_previous_session_close_is_not_inferred(field):
    rows, snapshots = data()
    if field == "close":
        rows[1].pop("close")
        first = result(["2026-01-05"], rows, snapshots)["clusters"][0]
        assert first["movement_reason"] == "MISSING_OR_INVALID_CLOSE"
    else:
        snapshots.clear()
        first = result(["2026-01-05"], rows, snapshots)["clusters"][0]
        assert first["movement_status"] == "NOT_EVALUABLE"


@pytest.mark.parametrize("event", ["socket.connect", "socket.getaddrinfo", "sqlite3.connect", "subprocess.Popen"])
def test_cli_offline_guard_rejects_external_io(event):
    path = Path(__file__).resolve().parents[1] / "scripts/build_technical_desk_stage1a1_volume.py"
    spec = importlib.util.spec_from_file_location("volume_cluster_cli", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with pytest.raises(RuntimeError, match="EXTERNAL_IO_PROHIBITED"):
        module.offline_guard(event, ())
    assert module.offline_guard("open", ()) is None

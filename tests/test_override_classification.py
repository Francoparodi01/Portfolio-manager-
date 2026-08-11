from pathlib import Path

from src.analysis.override_classification import (
    classify_override,
    dominant_override_status,
    override_delta,
    override_opposite_ratio,
    override_same_ratio,
)


def _row(**overrides):
    row = {
        "target_amount_ars": 100_000,
        "same_amount_ars": 0,
        "opposite_amount_ars": 0,
        "match_start_at": "2026-07-23T14:00:00+00:00",
        "match_basis": "intraday",
    }
    row.update(overrides)
    return row


def test_classify_override_statuses_match_legacy_contract():
    assert classify_override(_row(match_start_at=None)) == "PENDING_OPEN"
    assert classify_override(_row(match_basis="pending_open_revalidation")) == "PENDING_OPEN"
    assert classify_override(_row(opposite_amount_ars=20_000)) == "OPPOSITE"
    assert classify_override(_row(same_amount_ars=140_000)) == "OVERFOLLOWED"
    assert classify_override(_row(same_amount_ars=75_000)) == "FOLLOWED"
    assert classify_override(_row(same_amount_ars=15_000)) == "PARTIAL"
    assert classify_override(_row(same_amount_ars=14_999)) == "IGNORED"


def test_override_ratios_use_target_amount_floor():
    row = _row(target_amount_ars=0, same_amount_ars=2, opposite_amount_ars=3)

    assert override_same_ratio(row) == 2.0
    assert override_opposite_ratio(row) == 3.0


def test_override_delta_signs_are_human_vs_bot():
    assert override_delta("IGNORED", 0.10) == -0.10
    assert override_delta("OPPOSITE", -0.20) == 0.20
    assert override_delta("PARTIAL", 0.10) == -0.05
    assert override_delta("FOLLOWED", 0.10) == 0.0
    assert override_delta("OVERFOLLOWED", 0.10) == 0.0
    assert override_delta("PENDING_OPEN", 0.10) is None
    assert override_delta("IGNORED", None) is None


def test_dominant_override_status_uses_shared_rank():
    assert dominant_override_status(["IGNORED", "PARTIAL", "FOLLOWED"]) == "FOLLOWED"
    assert dominant_override_status([]) == "UNKNOWN"


def test_monitor_override_audit_keeps_executed_plans_visible():
    source = (Path(__file__).resolve().parents[1] / "src" / "monitor" / "api.py").read_text(
        encoding="utf-8"
    )
    start = source.index("async def override_audit")
    end = source.index("async def decision_ledger", start)

    assert "status IN ('APPROVED', 'EXECUTED')" in source[start:end]

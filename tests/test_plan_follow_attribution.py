from datetime import datetime, timezone

from src.analysis.plan_follow_attribution import (
    canonicalize_movements,
    normalize_plan_execution_attributions,
)


UTC = timezone.utc


def _plan(plan_id, decided_at, *, ticker="TEST", side="BUY", target=100_000):
    return {
        "id": plan_id,
        "owner_chat_id": 1,
        "decided_at": decided_at,
        "ticker": ticker,
        "decision": side,
        "target_amount_ars": target,
        "match_start_at": decided_at,
        "match_day": decided_at.date(),
    }


def _movement(
    movement_id,
    executed_at,
    *,
    ticker="TEST",
    side="BUY",
    amount=100_000,
    precision="date_only",
    external_id=None,
    window_start=None,
):
    return {
        "id": movement_id,
        "executed_at": executed_at,
        "executed_at_precision": precision,
        "executed_at_source": "test",
        "external_movement_id": external_id or str(movement_id),
        "ticker": ticker,
        "movement_type": side,
        "amount": amount,
        "quantity": 1,
        "price": amount,
        "observation_window_start_at": window_start,
    }


def test_canonicalize_movements_prefers_real_row_over_synthetic_duplicates():
    executed_at = datetime(2026, 7, 21, 3, tzinfo=UTC)
    rows = [
        _movement(1, executed_at, amount=60_000, external_id="real-1"),
        _movement(2, executed_at, amount=30_000, external_id="synthetic:a"),
        _movement(3, executed_at, amount=30_000, external_id="synthetic:b"),
    ]

    canonical = canonicalize_movements(rows)

    assert [row["id"] for row in canonical] == [1]


def test_repeated_plans_and_one_movement_become_one_primary_attribution():
    first_plan_at = datetime(2026, 8, 7, 19, 0, tzinfo=UTC)
    latest_plan_at = datetime(2026, 8, 10, 14, 48, tzinfo=UTC)
    observed_at = datetime(2026, 8, 10, 15, 2, tzinfo=UTC)
    previous_snapshot_at = datetime(2026, 8, 10, 14, 50, tzinfo=UTC)
    plans = [
        _plan(10, first_plan_at, side="SELL"),
        _plan(11, latest_plan_at, side="SELL"),
    ]
    movements = [
        _movement(
            50,
            observed_at,
            side="SELL",
            precision="observed_after",
            window_start=previous_snapshot_at,
        )
    ]

    rows = normalize_plan_execution_attributions(plans, movements)

    assert len(rows) == 1
    assert rows[0]["representative_decision_log_id"] == 11
    assert rows[0]["eligible_for_viability"] is True
    assert rows[0]["follow_status"] == "FOLLOWED"
    assert {link["plan"]["id"] for link in rows[0]["plan_links"]} == {10, 11}


def test_same_day_date_only_movement_stays_ambiguous_and_outside_viability():
    plan_at = datetime(2026, 7, 29, 20, 0, tzinfo=UTC)
    movement_at = datetime(2026, 7, 29, 3, 0, tzinfo=UTC)

    rows = normalize_plan_execution_attributions(
        [_plan(20, plan_at)],
        [_movement(60, movement_at)],
    )

    assert len(rows) == 1
    assert rows[0]["temporal_quality"] == "AMBIGUOUS_SAME_DAY"
    assert rows[0]["eligible_for_viability"] is False

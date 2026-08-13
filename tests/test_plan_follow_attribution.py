from datetime import datetime, timezone

from src.analysis.plan_follow_attribution import (
    canonicalize_movements,
    compute_execution_session_outcomes,
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
    quantity=1,
    price=None,
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
        "quantity": quantity,
        "price": amount / quantity if price is None else price,
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


def test_attribution_uses_quantity_weighted_real_execution_price():
    plan_at = datetime(2026, 8, 10, 14, 0, tzinfo=UTC)
    movements = [
        _movement(
            70,
            datetime(2026, 8, 10, 15, 0, tzinfo=UTC),
            amount=20_000,
            quantity=2,
            price=10_000,
            precision="exact",
        ),
        _movement(
            71,
            datetime(2026, 8, 10, 15, 5, tzinfo=UTC),
            amount=36_000,
            quantity=3,
            price=12_000,
            precision="exact",
        ),
    ]

    rows = normalize_plan_execution_attributions(
        [_plan(30, plan_at, target=50_000)],
        movements,
    )

    assert len(rows) == 1
    assert rows[0]["execution_quantity"] == 5
    assert rows[0]["execution_notional_ars"] == 56_000
    assert rows[0]["execution_price"] == 11_200


def test_execution_outcome_counts_exact_market_sessions_after_fill():
    executed_at = datetime(2026, 8, 10, 15, 0, tzinfo=UTC)
    candles = [
        {"ts": datetime(2026, 8, day, tzinfo=UTC), "close_price": price, "source": "COCOS"}
        for day, price in [(11, 101), (12, 102), (13, 103), (14, 104), (18, 110)]
    ]

    outcomes = compute_execution_session_outcomes(
        execution_price=100,
        executed_at=executed_at,
        side="BUY",
        candles=candles,
    )

    assert outcomes["outcome_date_5d"].isoformat() == "2026-08-18"
    assert outcomes["outcome_5d"] == 0.10


def test_execution_outcome_rebases_entry_for_confirmed_corporate_action():
    executed_at = datetime(2026, 8, 10, 15, 0, tzinfo=UTC)
    candles = [
        {"ts": datetime(2026, 8, day, tzinfo=UTC), "close_price": price, "source": "COCOS"}
        for day, price in [(11, 101), (12, 51), (13, 49), (14, 47), (18, 45)]
    ]
    effects = [
        {
            "effective_at": datetime(2026, 8, 12, 13, 0, tzinfo=UTC),
            "lifecycle_status": "CONFIRMED",
            "price_factor": 0.5,
        }
    ]

    outcomes = compute_execution_session_outcomes(
        execution_price=100,
        executed_at=executed_at,
        side="SELL",
        candles=candles,
        corporate_effects=effects,
    )

    assert outcomes["outcome_5d"] == 0.10

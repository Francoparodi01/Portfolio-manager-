from __future__ import annotations

from datetime import date

from src.collector.db import _manual_broker_layer_patch


def test_manual_broker_movement_layer_patch_includes_manual_run_context():
    patch = _manual_broker_layer_patch(
        decision_source="broker_movement",
        fill_date=date(2026, 7, 27),
        ticker="NVDA",
        side="BUY",
        owner_chat_id=123,
        external_ids=["mov-1", "mov-2"],
        quantity=3.0,
        avg_fill_price=100.0,
        executed_amount=300.0,
        fees_ars=1.5,
    )

    assert set(patch) == {"run_context", "broker_movement"}
    assert patch["broker_movement"] == {
        "reconciliation_mode": "manual_or_unplanned",
        "external_fill_ids": ["mov-1", "mov-2"],
        "fill_date": "2026-07-27",
        "quantity": 3.0,
        "avg_fill_price": 100.0,
        "gross_amount_ars": 300.0,
        "fees_ars": 1.5,
    }
    assert patch["run_context"] == {
        "run_id": patch["run_context"]["run_id"],
        "strategy_id": "manual",
        "strategy_version": "unknown",
        "planner_version": "none",
        "optimizer_version": "none",
        "model_version": "none",
        "prompt_version": "none",
        "decided_at": "2026-07-27T15:00:00-03:00",
        "market_snapshot_id": None,
        "portfolio_snapshot_id": None,
        "feature_snapshot_id": None,
    }
    assert patch["run_context"]["run_id"].startswith("manual:")


def test_manual_broker_run_context_id_is_stable_for_same_fill_group():
    kwargs = {
        "decision_source": "broker_movement",
        "fill_date": date(2026, 7, 27),
        "ticker": "NVDA",
        "side": "BUY",
        "owner_chat_id": 123,
        "external_ids": ["mov-1", "mov-2"],
        "quantity": 3.0,
        "avg_fill_price": 100.0,
        "executed_amount": 300.0,
        "fees_ars": 1.5,
    }

    first = _manual_broker_layer_patch(**kwargs)["run_context"]["run_id"]
    second = _manual_broker_layer_patch(**kwargs)["run_context"]["run_id"]

    assert first == second

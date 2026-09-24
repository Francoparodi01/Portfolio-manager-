import json
from datetime import timedelta
import pytest
from test_decision_lab import T, dataset_fixture, recorded_plan
from src.decision_lab.models import Experiment, canonical
from src.decision_lab.runner import replay
from src.decision_lab.persistence import write_artifacts, validate_artifacts


def test_artifact_replay_and_tampering(tmp_path):
    data = dataset_fixture()
    state, plan = recorded_plan(data)
    run, frozen = replay(
        data,
        requests=[{"as_of": T.isoformat(), "plan_id": "plan"}],
        owner=123,
        strategies=[plan.strategy],
        mode="RECORDED_PLAN_EVALUATION",
        evaluated_as_of=T + timedelta(days=45),
        experiment=Experiment(
            experiment_id="e", registered_at=T, bootstrap_resamples=100
        ),
    )
    folder = write_artifacts(tmp_path, run, frozen)
    assert validate_artifacts(folder, recompute=True)["valid"]
    assert write_artifacts(tmp_path, run, frozen) == folder
    (folder / "tables/dva.csv").write_text("tampered", encoding="utf-8")
    with pytest.raises(ValueError, match="hash mismatch"):
        validate_artifacts(folder)
    with pytest.raises(ValueError, match="immutable artifact conflict"):
        write_artifacts(tmp_path, run, frozen)


def test_label_scrubber_and_calendar_coverage():
    from src.decision_lab.exporter import _clean_features, sessions_between

    assert _clean_features({"score": 0.2, "nested": {"outcome_20d": 0.9, "x": 1}}) == {
        "score": 0.2,
        "nested": {"x": 1},
    }
    with pytest.raises(ValueError, match="does not cover"):
        sessions_between(T.replace(year=1990), T)


def test_prospective_capture_preserves_original_plan_without_mutation():
    import asyncio, json
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from src.decision_lab.capture import capture_plan
    from src.decision_lab.exporter import captured_records

    conn = SimpleNamespace(fetchval=AsyncMock(return_value=True), execute=AsyncMock())
    plan = {
        "cash_before": 1000,
        "feasible": True,
        "gate": "NORMAL",
        "decisions": [],
        "sell_orders": [
            {
                "ticker": "AAA",
                "side": "SELL",
                "quantity_est": 4,
                "reference_price": 100,
                "amount_ars": 400,
                "action": "SELL_PARTIAL",
            }
        ],
        "buy_orders": [],
        "blocked_orders": [],
    }
    portfolio = {
        "owner_chat_id": 123,
        "scraped_at": T.isoformat(),
        "cash_ars": 1000,
        "positions": [
            {"ticker": "AAA", "currency": "ARS", "quantity": 10, "current_price": 100}
        ],
    }
    result = asyncio.run(
        capture_plan(
            conn,
            plan_id="source",
            owner=123,
            decision_at=T,
            plan=plan,
            portfolio=portfolio,
            signals=[],
            macro=None,
        )
    )
    assert result["status"] == "CAPTURED"
    payload = conn.execute.call_args.args[-1]
    plan["sell_orders"][0]["quantity_est"] = 999
    records, request = captured_records(
        {"payload": payload, "capture_hash": result["capture_hash"]}
    )
    frozen = next(e for e in records if e.kind == "PLAN")
    assert frozen.payload["orders"][0]["quantity"] == 4
    assert "outcome" not in payload
    assert request["plan_id"] == frozen.record_id

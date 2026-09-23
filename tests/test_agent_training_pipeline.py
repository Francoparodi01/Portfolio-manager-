from __future__ import annotations

from datetime import datetime, timezone

from src.training.dataset import build_training_dataset, temporal_split
from src.training.gates import evaluate_promotion_gate


UTC = timezone.utc


def _base_rows():
    runs = [
        {
            "id": "run-1",
            "goal": "analizar cartera",
            "model": "test-model",
            "status": "COMPLETED",
            "stop_reason": "goal_completed",
            "started_at": "2026-09-01T12:00:00+00:00",
        }
    ]
    steps = [
        {
            "run_id": "run-1",
            "step_no": 1,
            "tool_name": "market_context",
            "confidence": 0.7,
            "observation_ok": True,
        },
        {
            "run_id": "run-1",
            "step_no": 2,
            "tool_name": "portfolio_risk",
            "confidence": 0.8,
            "observation_ok": True,
        },
    ]
    jev = [
        {
            "run_id": "run-1",
            "eligible": True,
            "evidence_complete": True,
            "label": "PROCESS_OK",
            "score": 0.82,
            "version": "jev-test",
            "decision_log_id": 10,
            "action": "HOLD",
            "risk_guard_violations": 0,
            "pre_decision_features": {"vix": 18.2, "portfolio_vol_60d": 0.21},
        }
    ]
    outcomes = [
        {
            "run_id": "run-1",
            "decision_log_id": 10,
            "matured": True,
            "data_quality_ok": True,
            "horizon_days": 5,
            "directional_outcome": 0.012,
            "alpha_vs_benchmark": 0.004,
            "max_adverse_excursion": -0.018,
            "outcome_basis": "canonical_cocos",
        }
    ]
    return runs, steps, jev, outcomes


def test_dataset_joins_agent_jev_and_outcome_without_raw_goal():
    runs, steps, jev, outcomes = _base_rows()
    examples, exclusions = build_training_dataset(
        agent_runs=runs,
        agent_steps=steps,
        jev_assessments=jev,
        outcomes=outcomes,
    )
    assert exclusions == {}
    assert len(examples) == 1
    row = examples[0]
    assert row.tool_sequence == ("market_context", "portfolio_risk")
    assert row.goal_sha256 != runs[0]["goal"]
    assert row.features["vix"] == 18.2
    assert "directional_outcome" not in row.features


def test_dataset_rejects_obvious_future_leakage():
    runs, steps, jev, outcomes = _base_rows()
    jev[0]["pre_decision_features"]["return_5d"] = 0.12
    examples, exclusions = build_training_dataset(
        agent_runs=runs,
        agent_steps=steps,
        jev_assessments=jev,
        outcomes=outcomes,
    )
    assert examples == []
    assert exclusions["invalid_or_leaky_features"] == 1


def test_temporal_split_keeps_latest_rows_in_validation():
    runs, steps, jev, outcomes = _base_rows()
    examples = []
    for idx in range(5):
        run = dict(runs[0])
        run["id"] = f"run-{idx}"
        run["started_at"] = f"2026-09-0{idx + 1}T12:00:00+00:00"
        step_rows = [dict(row, run_id=run["id"]) for row in steps]
        j = dict(jev[0], run_id=run["id"], decision_log_id=10 + idx)
        out = dict(outcomes[0], run_id=run["id"], decision_log_id=10 + idx)
        rows, _ = build_training_dataset(
            agent_runs=[run],
            agent_steps=step_rows,
            jev_assessments=[j],
            outcomes=[out],
        )
        examples.extend(rows)
    train, validation = temporal_split(examples, validation_fraction=0.4)
    assert max(row.decided_at for row in train) < min(row.decided_at for row in validation)


def test_promotion_gate_never_accepts_risk_guard_violation():
    champion = {
        "jev_mean_score": 0.70,
        "net_ev": 0.01,
        "max_drawdown": 0.08,
    }
    challenger = {
        "jev_mean_score": 0.72,
        "net_ev": 0.02,
        "max_drawdown": 0.07,
        "risk_guard_violations": 1,
        "data_quality_pass_rate": 1.0,
    }
    result = evaluate_promotion_gate(
        champion=champion,
        challenger=challenger,
        total_samples=50,
        holdout_samples=15,
        temporal_holdout=True,
    )
    assert not result.eligible_for_review
    assert not result.checks["risk_guard_violations"]
    assert result.to_dict()["auto_promoted"] is False


def test_promotion_gate_can_only_mark_challenger_for_review():
    champion = {
        "jev_mean_score": 0.70,
        "net_ev": 0.01,
        "max_drawdown": 0.08,
    }
    challenger = {
        "jev_mean_score": 0.72,
        "net_ev": 0.02,
        "max_drawdown": 0.07,
        "risk_guard_violations": 0,
        "data_quality_pass_rate": 1.0,
    }
    result = evaluate_promotion_gate(
        champion=champion,
        challenger=challenger,
        total_samples=50,
        holdout_samples=15,
        temporal_holdout=True,
    )
    assert result.eligible_for_review
    assert result.to_dict()["auto_promoted"] is False

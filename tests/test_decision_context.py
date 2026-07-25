from datetime import datetime, timezone

from scripts import run_analysis
from src.analysis.decision_context import (
    DecisionRunContext,
    build_decision_run_context,
    short_config_hash,
)


def test_build_decision_run_context_with_all_fields():
    decided_at = datetime(2026, 7, 22, 14, 30, tzinfo=timezone.utc)

    ctx = build_decision_run_context(
        "run-123",
        strategy_id="quantia_core",
        strategy_version="core-v1",
        planner_version="planner-v1",
        optimizer_version="optimizer-v1",
        model_version="llama-test",
        prompt_version="prompt-v1",
        decided_at=decided_at,
        market_snapshot_id="market-1",
        portfolio_snapshot_id="portfolio-1",
        feature_snapshot_id="features-1",
    )

    assert ctx == DecisionRunContext(
        run_id="run-123",
        strategy_id="quantia_core",
        strategy_version="core-v1",
        planner_version="planner-v1",
        optimizer_version="optimizer-v1",
        model_version="llama-test",
        prompt_version="prompt-v1",
        decided_at=decided_at,
        market_snapshot_id="market-1",
        portfolio_snapshot_id="portfolio-1",
        feature_snapshot_id="features-1",
    )


def test_build_decision_run_context_with_only_run_id_uses_defaults():
    ctx = build_decision_run_context("run-only")

    assert ctx.run_id == "run-only"
    assert ctx.strategy_id == "quantia_core"
    assert ctx.strategy_version == "unknown"
    assert ctx.planner_version == "unknown"
    assert ctx.optimizer_version == "unknown"
    assert ctx.model_version == "none"
    assert ctx.prompt_version == "none"
    assert ctx.market_snapshot_id is None
    assert ctx.portfolio_snapshot_id is None
    assert ctx.feature_snapshot_id is None
    assert ctx.decided_at.tzinfo is not None


def test_decision_run_context_serializes_to_layers_ready_dict():
    decided_at = datetime(2026, 7, 22, 14, 30, tzinfo=timezone.utc)
    ctx = build_decision_run_context(
        "run-serialized",
        strategy_config={"risk": "standard", "cash_floor": 0.07},
        planner_version="planner-v1",
        optimizer_version="optimizer-v1",
        decided_at=decided_at,
    )

    assert ctx.strategy_version == short_config_hash(
        {"cash_floor": 0.07, "risk": "standard"}
    )
    assert ctx.to_dict() == {
        "run_id": "run-serialized",
        "strategy_id": "quantia_core",
        "strategy_version": ctx.strategy_version,
        "planner_version": "planner-v1",
        "optimizer_version": "optimizer-v1",
        "model_version": "none",
        "prompt_version": "none",
        "decided_at": "2026-07-22T14:30:00+00:00",
        "market_snapshot_id": None,
        "portfolio_snapshot_id": None,
        "feature_snapshot_id": None,
    }


def test_layers_payload_for_decision_includes_run_context():
    decided_at = datetime(2026, 7, 23, 15, 45, tzinfo=timezone.utc)

    payload = run_analysis._layers_payload_for_decision(
        None,
        extra={"source": "execution_plan", "status": "APPROVED"},
        run_id="5d90a2b3-0ca5-4d19-a8b1-c4bd299af593",
        decided_at=decided_at,
        portfolio_snapshot_id="2026-07-23T15:40:00+00:00",
    )

    feature_snapshot_id = payload["feature_snapshot"]["feature_snapshot_id"]
    assert payload["run_context"] == {
        "run_id": "5d90a2b3-0ca5-4d19-a8b1-c4bd299af593",
        "strategy_id": "quantia_core",
        "strategy_version": "unknown",
        "planner_version": "unknown",
        "optimizer_version": "unknown",
        "model_version": "none",
        "prompt_version": "none",
        "decided_at": "2026-07-23T15:45:00+00:00",
        "market_snapshot_id": None,
        "portfolio_snapshot_id": "2026-07-23T15:40:00+00:00",
        "feature_snapshot_id": feature_snapshot_id,
    }
    assert feature_snapshot_id.startswith("features:")

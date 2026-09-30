from src.agentic.harness.context import ContextSelector
from src.agentic.harness.schemas import TaskSpec


def _task(intent: str, *, objective: str = "answer_user") -> TaskSpec:
    return TaskSpec(
        intent=intent,
        objective=objective,
        raw_message="fixture",
    )


def test_portfolio_review_exposes_claim_to_tool_mapping():
    plan = ContextSelector().select(
        _task("portfolio_review", objective="review_portfolio"),
        {"get_portfolio_snapshot", "get_persisted_decision_evidence"},
    )
    claims = {claim.claim_id: claim for claim in plan.evidence_claims}

    assert set(plan.required_tools) == {
        "get_portfolio_snapshot",
        "get_persisted_decision_evidence",
    }
    assert claims["portfolio_state"].required is True
    assert claims["portfolio_state"].available_tools == ["get_portfolio_snapshot"]
    assert claims["current_decision"].required is True
    assert claims["current_decision"].available_tools == ["get_persisted_decision_evidence"]


def test_position_analysis_distinguishes_required_and_optional_claims():
    available = {
        "get_portfolio_snapshot",
        "get_decision_evidence",
        "get_decision_value_added",
        "get_macro_context",
        "analyze_ticker",
    }
    plan = ContextSelector().select(
        _task("position_analysis", objective="evaluate_position"),
        available,
    )
    claims = {claim.claim_id: claim for claim in plan.evidence_claims}

    assert claims["portfolio_state"].required is True
    assert claims["current_decision"].required is True
    assert claims["historical_edge"].required is False
    assert claims["market_context"].required is False


def test_meta_policy_plan_keeps_policy_and_base_decision_separate():
    available = {"get_meta_policy", "get_decision_evidence", "get_decision_value_added"}
    plan = ContextSelector().select(
        _task("meta_policy", objective="explain_shadow_meta_policy"),
        available,
    )
    claims = {claim.claim_id: claim for claim in plan.evidence_claims}

    assert plan.required_tools == ["get_meta_policy"]
    assert claims["meta_policy_state"].required is True
    assert claims["current_decision"].required is False
    assert claims["historical_edge"].required is False
    assert all(set(claim.available_tools) <= set(plan.allowed_tools) for claim in claims.values())


def test_normalized_bot_follow_pnl_claim_respects_context_policy():
    plan = ContextSelector().select(
        _task("bot_follow_pnl", objective="explain_normalized_follow_pnl"),
        {"get_bot_follow_pnl", "get_normalized_bot_follow_pnl"},
    )

    assert plan.allowed_tools == ["get_normalized_bot_follow_pnl"]
    assert plan.required_tools == ["get_normalized_bot_follow_pnl"]
    assert len(plan.evidence_claims) == 1
    claim = plan.evidence_claims[0]
    assert claim.claim_id == "bot_counterfactual"
    assert claim.required is True
    assert claim.available_tools == ["get_normalized_bot_follow_pnl"]

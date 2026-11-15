from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.analysis.economic_meta_evaluator import (
    MetaOutcome,
    evaluate_policy,
    promotion_snapshot,
)
from src.analysis.economic_meta_policy import (
    CandidateDecision,
    MetaDecision,
    assert_shadow_only,
    evaluate_candidate,
    preregistered_policies,
)
from src.analysis.economic_meta_store import EconomicMetaShadowStore


NOW = datetime(2026, 9, 24, 15, 0, tzinfo=timezone.utc)


def _candidate(
    *,
    action: str = "SELL",
    score: float = -0.13,
    cost_bps: float = 75.0,
    turnover: float = 0.10,
    edge_bps: float | None = None,
    uncertainty_bps: float | None = None,
) -> CandidateDecision:
    return CandidateDecision(
        ticker="NVDA",
        candidate_action=action,
        candidate_score=score,
        as_of=NOW,
        estimated_cost_bps=cost_bps,
        portfolio_turnover=turnover,
        market_regime="TRANSITIONAL",
        expected_edge_vs_hold_bps=edge_bps,
        edge_uncertainty_bps=uncertainty_bps,
        opportunity_id="opp-1",
    )


def test_preregistered_policies_are_shadow_only_and_20d_primary() -> None:
    policies = preregistered_policies()
    assert set(policies) == {"META-A", "META-B", "META-C"}
    for policy in policies.values():
        policy.validate()
        assert policy.primary_horizon_days == 20
        assert policy.affects_production_recommendation is False
        assert policy.affects_sizing is False
        assert policy.affects_execution is False
        assert policy.capital_effect is False


def test_meta_b_is_asymmetric_and_stricter_for_buy() -> None:
    policy = preregistered_policies()["META-B"]
    sell = evaluate_candidate(_candidate(action="SELL", score=-0.11), policy)
    buy = evaluate_candidate(_candidate(action="BUY", score=0.11), policy)

    assert sell.decision == MetaDecision.ALLOW_SHADOW.value
    assert buy.decision == MetaDecision.REJECT_TO_HOLD.value
    assert "SCORE_BELOW_PREREGISTERED_GATE" in (buy.rejection_reason or "")


def test_meta_c_requires_edge_evidence() -> None:
    policy = preregistered_policies()["META-C"]
    result = evaluate_candidate(_candidate(action="SELL", score=-0.15), policy)
    assert result.decision == MetaDecision.REJECT_TO_HOLD.value
    assert "EDGE_EVIDENCE_REQUIRED" in (result.rejection_reason or "")


def test_meta_c_edge_must_clear_cost_uncertainty_and_buffer() -> None:
    policy = preregistered_policies()["META-C"]
    reject = evaluate_candidate(
        _candidate(
            action="SELL",
            score=-0.15,
            cost_bps=75.0,
            edge_bps=120.0,
            uncertainty_bps=25.0,
        ),
        policy,
    )
    allow = evaluate_candidate(
        _candidate(
            action="SELL",
            score=-0.15,
            cost_bps=75.0,
            edge_bps=130.0,
            uncertainty_bps=25.0,
        ),
        policy,
    )
    assert reject.decision == MetaDecision.REJECT_TO_HOLD.value
    assert "EDGE_DOES_NOT_CLEAR_COST_UNCERTAINTY_BUFFER" in (
        reject.rejection_reason or ""
    )
    assert allow.decision == MetaDecision.ALLOW_SHADOW.value


def test_hold_never_becomes_a_trade() -> None:
    policy = preregistered_policies()["META-A"]
    result = evaluate_candidate(_candidate(action="HOLD", score=0.99), policy)
    assert result.decision == MetaDecision.REJECT_TO_HOLD.value
    assert result.rejection_reason == "SOURCE_ALREADY_HOLD"


def test_records_have_hard_no_capital_effect_guards() -> None:
    policy = preregistered_policies()["META-A"]
    result = evaluate_candidate(_candidate(action="SELL", score=-0.13), policy)
    assert_shadow_only(result)
    assert result.shadow_only is True
    assert result.affects_production_recommendation is False
    assert result.affects_sizing is False
    assert result.affects_execution is False
    assert result.capital_effect is False


def test_shadow_store_round_trip(tmp_path) -> None:
    path = tmp_path / "meta.jsonl"
    store = EconomicMetaShadowStore(path)
    record = evaluate_candidate(
        _candidate(action="SELL", score=-0.13),
        preregistered_policies()["META-A"],
        run_id="run-1",
    )
    store.append(record)
    rows = store.read_all()
    assert len(rows) == 1
    assert rows[0]["run_id"] == "run-1"
    assert rows[0]["shadow_only"] is True
    assert rows[0]["capital_effect"] is False


def test_evaluator_computes_dva_profit_factor_and_effective_n() -> None:
    rows = [
        MetaOutcome(
            policy_name="META-A",
            policy_version="v1",
            opportunity_id=f"opp-{index}",
            horizon_days=20,
            entry_at=NOW + timedelta(days=index * 30),
            exit_at=NOW + timedelta(days=index * 30 + 20),
            meta_return_net=meta,
            hold_return_net=hold,
            current_return_net=current,
            turnover=0.10,
            cost_drag=0.005,
        )
        for index, (meta, hold, current) in enumerate(
            [
                (0.05, 0.01, 0.02),
                (-0.02, 0.00, -0.01),
                (0.03, 0.01, 0.01),
            ]
        )
    ]
    metrics = evaluate_policy(rows, horizon_days=20)
    assert metrics.n == 3
    assert metrics.n_effective == 3
    assert metrics.mean_net_return == pytest.approx(0.02)
    assert metrics.mean_dva_vs_hold == pytest.approx(0.013333333333333334)
    assert metrics.mean_delta_vs_current == pytest.approx(0.013333333333333334)
    assert metrics.profit_factor == pytest.approx(4.0)
    assert metrics.interpretation == "PRIMARY_20D"


def test_promotion_never_auto_enables_capital() -> None:
    rows = [
        MetaOutcome(
            policy_name="META-A",
            policy_version="v1",
            opportunity_id=f"opp-{index}",
            horizon_days=20,
            entry_at=NOW + timedelta(days=index * 25),
            exit_at=NOW + timedelta(days=index * 25 + 20),
            meta_return_net=0.03,
            hold_return_net=0.01,
            current_return_net=0.02,
        )
        for index in range(20)
    ]
    metrics = evaluate_policy(rows, horizon_days=20)
    snapshot = promotion_snapshot([metrics], min_effective_n=20)
    assert snapshot["promotable"] is False
    assert snapshot["reason"] == "MANUAL_RISK_REVIEW_REQUIRED"
    assert "never auto-promotes" in snapshot["note"]

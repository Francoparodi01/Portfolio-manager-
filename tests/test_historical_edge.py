from datetime import datetime, timedelta, timezone

from src.analysis.economic_meta_policy import evaluate_all_preregistered
from src.analysis.historical_edge import (
    MAX_PROFIT_FACTOR,
    build_directional_episodes,
    match_historical_edge,
    score_bucket,
)


AS_OF = datetime(2026, 9, 26, 18, 0, tzinfo=timezone.utc)


def _row(
    idx: int,
    *,
    ticker: str,
    outcome: float,
    days_ago: int,
    action: str = "SELL",
    score: float = -0.14,
    regime: str = "TRANSITIONAL",
    filled_after_as_of: bool = False,
    run_id: str | None = None,
    metric_scope: str = "planner_audit",
):
    decided_at = AS_OF - timedelta(days=days_ago)
    filled_at = AS_OF + timedelta(days=1) if filled_after_as_of else AS_OF - timedelta(hours=1)
    return {
        "id": idx,
        "run_id": run_id or f"run-{days_ago}",
        "decided_at": decided_at,
        "ticker": ticker,
        "decision": action,
        "final_score": score,
        "regime": regime,
        "outcome_20d": outcome,
        "outcome_basis": "canonical_cocos",
        "outcome_filled_at": filled_at,
        "status": "APPROVED",
        "metric_scope": metric_scope,
    }


def _profitable_history():
    rows = []
    # 20 independent ticker episodes spread across 10 dates. 15 winners and
    # 5 losers remain strongly positive after the 150 bps research cost.
    for idx in range(20):
        days_ago = 80 + (idx // 2) * 3
        rows.append(
            _row(
                idx,
                ticker=f"T{idx:02d}",
                outcome=0.055 if idx < 15 else -0.005,
                days_ago=days_ago,
                run_id=f"run-{days_ago}",
            )
        )
    return rows


def test_score_bucket_uses_fixed_preregistered_bands():
    assert score_bucket(0.079) == "LT_008"
    assert score_bucket(0.10) == "008_012"
    assert score_bucket(-0.14) == "012_018"
    assert score_bucket(0.20) == "GE_018"


def test_repeated_same_direction_is_one_episode_until_a_break():
    rows = [
        _row(1, ticker="NVDA", outcome=0.04, days_ago=90, run_id="r1"),
        _row(2, ticker="NVDA", outcome=0.03, days_ago=89, run_id="r2"),
        {
            **_row(3, ticker="NVDA", outcome=0.0, days_ago=88, run_id="r3"),
            "decision": "HOLD",
            "status": "APPROVED",
        },
        _row(4, ticker="NVDA", outcome=0.05, days_ago=87, run_id="r4"),
    ]
    episodes = build_directional_episodes(rows)
    assert len(episodes) == 2
    assert episodes[0]["recommendation_count"] == 2
    assert episodes[1]["recommendation_count"] == 1


def test_same_signal_after_intervening_run_is_new_episode():
    rows = [
        _row(1, ticker="NVDA", outcome=0.04, days_ago=90, run_id="r1"),
        _row(
            2,
            ticker="AMD",
            outcome=0.03,
            days_ago=89,
            action="BUY",
            score=0.14,
            run_id="r2",
        ),
        _row(3, ticker="NVDA", outcome=0.05, days_ago=88, run_id="r3"),
    ]
    episodes = build_directional_episodes(rows)
    nvda = [episode for episode in episodes if episode["ticker"] == "NVDA"]
    assert len(nvda) == 2
    assert [episode["recommendation_count"] for episode in nvda] == [1, 1]


def test_blocked_audit_is_not_learned_as_formal_winner():
    rows = _profitable_history()
    rows.append(
        _row(
            99,
            ticker="BLOCKED",
            outcome=0.90,
            days_ago=40,
            metric_scope="blocked_audit",
        )
    )
    match = match_historical_edge(
        rows,
        candidate_action="SELL",
        candidate_score=-0.14,
        candidate_regime="TRANSITIONAL",
        as_of=AS_OF,
        cost_bps=150,
    )
    assert match.n_episodes == 20
    assert match.win_rate_net == 0.75


def test_profitable_historical_pattern_passes_shadow_gate():
    match = match_historical_edge(
        _profitable_history(),
        candidate_action="SELL",
        candidate_score=-0.14,
        candidate_regime="TRANSITIONAL",
        as_of=AS_OF,
        lookback_days=365,
        cost_bps=150,
    )
    assert match.specificity == "ACTION_SCORE_REGIME"
    assert match.n_episodes == 20
    assert match.n_dates == 10
    assert match.win_rate_net == 0.75
    assert match.mean_net_return is not None and match.mean_net_return > 0.0025
    assert match.median_net_return is not None and match.median_net_return > 0
    assert match.profit_factor_net is not None and match.profit_factor_net > 1.10
    assert match.passes_shadow_gate is True
    assert match.dva_vs_hold_status == "UNAVAILABLE_NOT_A_PORTFOLIO_HOLD_COUNTERFACTUAL"


def test_outcome_not_known_at_candidate_time_is_excluded():
    rows = _profitable_history()
    rows.append(
        _row(
            99,
            ticker="FUTURE",
            outcome=0.90,
            days_ago=40,
            filled_after_as_of=True,
        )
    )
    match = match_historical_edge(
        rows,
        candidate_action="SELL",
        candidate_score=-0.14,
        candidate_regime="TRANSITIONAL",
        as_of=AS_OF,
        cost_bps=150,
    )
    assert match.n_episodes == 20
    assert match.win_rate_net == 0.75


def test_rows_outside_lookback_are_excluded_even_for_pure_matcher():
    rows = _profitable_history()
    rows.extend(
        _row(
            200 + idx,
            ticker=f"OLD{idx:02d}",
            outcome=0.50,
            days_ago=500,
        )
        for idx in range(10)
    )
    match = match_historical_edge(
        rows,
        candidate_action="SELL",
        candidate_score=-0.14,
        candidate_regime="TRANSITIONAL",
        as_of=AS_OF,
        lookback_days=365,
        cost_bps=150,
    )
    assert match.n_episodes == 20
    assert match.win_rate_net == 0.75


def test_zero_loss_profit_factor_is_finite_and_json_safe():
    rows = [
        _row(
            idx,
            ticker=f"W{idx:02d}",
            outcome=0.05,
            days_ago=80 + idx,
        )
        for idx in range(20)
    ]
    match = match_historical_edge(
        rows,
        candidate_action="SELL",
        candidate_score=-0.14,
        candidate_regime="TRANSITIONAL",
        as_of=AS_OF,
        cost_bps=150,
    )
    assert match.profit_factor_net == MAX_PROFIT_FACTOR
    assert match.profit_factor_net < float("inf")


def test_meta_d_allows_only_when_historical_profile_clears_gate():
    match = match_historical_edge(
        _profitable_history(),
        candidate_action="SELL",
        candidate_score=-0.14,
        candidate_regime="TRANSITIONAL",
        as_of=AS_OF,
        cost_bps=150,
    )
    payload = {
        "ticker": "MU",
        "candidate_action": "SELL",
        "candidate_score": -0.14,
        "as_of": AS_OF,
        "estimated_cost_bps": 75,
        "portfolio_turnover": 0.10,
        "market_regime": "TRANSITIONAL",
        "historical_edge": match.to_dict(),
    }
    records = evaluate_all_preregistered(payload, run_id="test-run")
    meta_d = next(record for record in records if record.policy_name == "META-D")
    assert meta_d.decision == "ALLOW_SHADOW"
    assert meta_d.capital_effect is False
    assert meta_d.metadata["historical_edge"]["n_episodes"] == 20


def test_meta_d_fails_closed_without_historical_evidence():
    records = evaluate_all_preregistered(
        {
            "ticker": "MU",
            "candidate_action": "SELL",
            "candidate_score": -0.14,
            "as_of": AS_OF,
            "estimated_cost_bps": 75,
            "portfolio_turnover": 0.10,
            "market_regime": "TRANSITIONAL",
        },
        run_id="test-run",
    )
    meta_d = next(record for record in records if record.policy_name == "META-D")
    assert meta_d.decision == "REJECT_TO_HOLD"
    assert "HISTORICAL_EDGE_REQUIRED" in str(meta_d.rejection_reason)

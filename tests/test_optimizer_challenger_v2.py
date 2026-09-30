from datetime import date, timedelta
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from src.analysis.optimizer_calibration_v2 import (
    CAPITAL_AUTHORITY,
    MODE,
    CalibrationEpisode,
    OptimizerRunContext,
    calibrated_view,
)
from src.analysis.optimizer_challenger_v2 import run_calibrated_optimizer_v2
from src.analysis.optimizer_challenger_audit import (
    StrategyObservation,
    assess_promotion,
    compare_same_snapshot,
)
from src.analysis.synthesis import LayerScore, SynthesisResult
from src.analysis.versioning import config_hash, version_manifest


def _history(n=80, *, available_lag=6):
    start = date(2025, 1, 1)
    rows = []
    for i in range(n):
        signal_date = start + timedelta(days=i)
        score = -0.3 + 0.6 * (i / max(1, n - 1))
        realized = -0.03 + 0.06 * (i / max(1, n - 1))
        rows.append(CalibrationEpisode(
            ticker=f"T{i % 8}",
            asof_date=signal_date,
            horizon_days=5,
            score=score,
            future_return=realized,
            regime="RANGE",
            asset_group="TEST",
            quality="HIGH",
            scope="PRIMARY",
            outcome_date=signal_date + timedelta(days=5),
            outcome_available_at=signal_date + timedelta(days=available_lag),
            price=100 + i,
            outcome="UP" if realized > 0 else "DOWN",
            dataset_scope="PRIMARY",
        ))
    return rows


def test_pit_uses_outcome_available_at_not_just_outcome_date():
    asof = date(2025, 6, 1)
    base = _history(40)
    delayed = CalibrationEpisode(
        ticker="LEAK",
        asof_date=date(2025, 1, 1),
        horizon_days=5,
        score=0.2,
        future_return=9.0,
        regime="RANGE",
        asset_group="TEST",
        quality="HIGH",
        scope="PRIMARY",
        outcome_date=date(2025, 1, 6),
        outcome_available_at=date(2025, 12, 1),
        dataset_scope="PRIMARY",
    )
    view = calibrated_view(
        base + [delayed],
        asof_date=asof,
        horizon_days=5,
        score=0.1,
        regime="RANGE",
        asset_group="TEST",
    )
    assert view.n == 40
    assert view.expected_return is not None
    assert view.dispersion is not None
    assert view.horizon_consistency is not None


def test_calibration_never_mixes_audit_debug_or_radar_scope():
    primary = _history(35)
    extras = []
    for scope in ("AUDIT", "DEBUG", "RADAR"):
        extras.append(CalibrationEpisode(
            ticker=f"{scope}1",
            asof_date=date(2025, 1, 1),
            horizon_days=5,
            score=0.2,
            future_return=5.0,
            regime="RANGE",
            asset_group="TEST",
            quality="HIGH",
            scope=scope,
            outcome_date=date(2025, 1, 6),
            outcome_available_at=date(2025, 1, 7),
            dataset_scope=scope,
        ))
    view = calibrated_view(
        primary + extras,
        asof_date=date(2025, 6, 1),
        horizon_days=5,
        score=0.1,
        regime="RANGE",
        asset_group="TEST",
    )
    assert view.n == 35


def test_shadow_challenger_runs_same_context_and_has_no_capital_authority():
    idx = pd.date_range("2025-01-01", periods=90, freq="B")
    returns = pd.DataFrame({
        "AAA": np.linspace(-0.012, 0.014, 90),
        "BBB": np.linspace(0.009, -0.008, 90),
    }, index=idx)
    history = _history(80)
    current_positions = [
        {"ticker": "AAA", "market_value": 450.0},
        {"ticker": "BBB", "market_value": 450.0},
    ]
    results = [
        SimpleNamespace(ticker="AAA", final_score=0.10, conviction=0.8, technical_regime="RANGE"),
        SimpleNamespace(ticker="BBB", final_score=0.08, conviction=0.7, technical_regime="RANGE"),
    ]
    context = OptimizerRunContext("p1", "m1", "constraints-v1", 40.0, 0.006, 0.06)
    target = run_calibrated_optimizer_v2(
        history=history,
        asof_date=date(2025, 6, 1),
        horizon_days=5,
        returns=returns,
        current_positions=current_positions,
        portfolio_value_ars=1000.0,
        cash_ars=40.0,
        synthesis_results=results,
        champion_context=context,
        challenger_context=context,
        asset_groups={"AAA": "TEST", "BBB": "TEST"},
        gate_state="NORMAL",
    )
    assert target.status == "OK_SHADOW", target.reason
    assert target.mode == MODE == "SHADOW_ONLY"
    assert target.capital_authority is CAPITAL_AUTHORITY is False
    assert abs(target.frozen_weight + sum(target.weights.values()) + target.cash_weight - 1.0) < 1e-6
    assert set(target.view_confidence) == {"AAA", "BBB"}


def _obs(ticker, horizon=5, *, realized=0.02, expected=0.015, turnover=0.1):
    return StrategyObservation(
        ticker=ticker,
        horizon_days=horizon,
        regime="RANGE",
        asset_group="TEST",
        score_bucket="+0.08:+0.12",
        expected_return=expected,
        realized_return=realized,
        turnover=turnover,
        fees=0.002,
        drawdown=-0.05,
        concentration=0.2,
        tracking_error=0.03,
    )


def test_champion_challenger_comparison_requires_identical_observation_cohort():
    context = OptimizerRunContext("p1", "m1", "c1", 1000.0, 0.006, 0.0)
    with pytest.raises(ValueError, match="cohort mismatch"):
        compare_same_snapshot(context, context, [_obs("AAA")], [_obs("BBB")])


def test_promotion_gate_is_not_win_rate_based_and_fails_closed_on_narrow_sample():
    champion = [_obs("AAA", realized=0.01, turnover=0.1) for _ in range(20)]
    challenger = [_obs("AAA", realized=0.03, turnover=0.1) for _ in range(20)]
    assessment = assess_promotion(
        champion_rows=champion,
        challenger_rows=challenger,
        temporal_stability=False,
    )
    assert assessment.eligible is False
    assert any(reason.startswith("INSUFFICIENT_OOS_SAMPLE") for reason in assessment.reasons)
    assert "SINGLE_TICKER_DEPENDENCY" in assessment.reasons
    assert "TEMPORAL_STABILITY_NOT_PROVEN" in assessment.reasons


def test_version_manifest_hashes_real_quant_config_not_empty_dict():
    manifest = version_manifest()
    assert manifest["config_hash"] != config_hash({})
    assert manifest["optimizer_version"]
    assert manifest["planner_version"]
    assert manifest["synthesis_version"]
    assert manifest["risk_policy_version"]
    assert manifest["calibration_version"]


def test_synthesis_output_names_risk_regime_and_trend_explicitly():
    result = SynthesisResult(
        ticker="IREN",
        decision="HOLD",
        confidence=0.8,
        final_score=-0.008,
        position_size=0.1,
        conviction=0.8,
        layers=[
            LayerScore("technical", 0.2, 0.30, 0.060),
            LayerScore("macro", 0.06, 0.30, 0.018),
            LayerScore("risk", -0.4, 0.25, -0.100),
            LayerScore("sentiment", 0.0933, 0.15, 0.014),
        ],
        technical_regime="RANGE",
        trend_score=-0.178,
    )
    rendered = result.to_telegram()
    assert "Risk -0.100" in rendered
    assert "Regime <b>RANGE</b>" in rendered
    assert "Trend <code>-0.178</code>" in rendered

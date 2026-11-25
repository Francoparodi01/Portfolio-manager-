import numpy as np
import pandas as pd

from src.analysis.regression_audit import (
    RegressionAuditConfig,
    _build_human_reading,
    prepare_model_frame,
    run_regression_audit_sync,
)


def _frame(n_dates=60, per_day=2, *, seed=0, sell_every=2, slope=0.4, noise=0.01):
    rng = np.random.default_rng(seed)
    rows = []
    for i, day in enumerate(pd.bdate_range("2026-02-02", periods=n_dates)):
        for j in range(per_day):
            strength = float(rng.uniform(0.02, 0.25))
            is_sell = (i * per_day + j) % sell_every == 0
            rows.append(
                {
                    "id": len(rows) + 1,
                    "decided_at": day.tz_localize("UTC") + pd.Timedelta(hours=15),
                    "ticker": "T",
                    "decision": "SELL" if is_sell else "BUY",
                    # A SELL comes from a negative score; its outcome is stored directional.
                    "final_score": -strength if is_sell else strength,
                    "outcome_5d": slope * strength + float(rng.normal(0, noise)),
                    "source": "execution_plan",
                    "status": "EXECUTED",
                    "decision_type": "executable",
                    "outcome_basis": "canonical_cocos",
                    "is_primary_metric": True,
                }
            )
    return pd.DataFrame(rows)


def _config(**kwargs):
    return RegressionAuditConfig(
        database_url="postgresql://unused", horizons=("5d",), mode="execution", **kwargs
    )


def _baseline(report):
    return next(m for m in report.models if m.model_name == "baseline_score" and m.horizon == "5d")


def test_directional_regression_aligns_sell_scores():
    report = run_regression_audit_sync(_frame(), _config())
    model = _baseline(report)
    # Raw scores would put good SELLs (negative score, positive outcome) against the slope.
    assert model.score_coef > 0.3
    assert model.ic > 0.5


def test_regression_uses_date_block_clusters_and_needs_enough_dates():
    import pytest

    pytest.importorskip("statsmodels")
    many = _baseline(run_regression_audit_sync(_frame(n_dates=60), _config()))
    assert many.se_method == "cluster_date_blocks"
    assert many.n_clusters >= 10

    few = _baseline(run_regression_audit_sync(_frame(n_dates=8, per_day=8), _config(min_n=12)))
    assert few.se_method == "hc1"
    assert few.suggested_buy_threshold is None
    assert "solapamiento" in (few.threshold_reason or "")


def test_regression_excludes_non_canonical_outcomes():
    frame = _frame(n_dates=20)
    frame.loc[:9, "outcome_basis"] = "legacy_external"
    report = run_regression_audit_sync(frame, _config())
    assert _baseline(report).n == len(frame) - 10
    assert any("legacy_external" in w for w in report.warnings)


def test_regression_prefers_executable_outcome():
    frame = pd.DataFrame(
        [{"decision": "BUY", "final_score": 0.1, "outcome_5d": 0.02, "executable_outcome_5d": -0.01}]
    )
    prepared, target, _actions, _warnings = prepare_model_frame(frame, "5d", _config())
    assert prepared[target].tolist() == [-0.01]


def test_reading_is_not_green_without_corrected_p_value():
    report = run_regression_audit_sync(_frame(n_dates=8, per_day=8), _config(min_n=12))
    reading = "\n".join(_build_human_reading(report))
    assert "🟢" not in reading

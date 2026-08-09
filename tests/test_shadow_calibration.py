from __future__ import annotations

import ast
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.analysis.shadow_calibration import (
    MODEL_VERSION,
    CalibrationExample,
    apply_calibration,
    calibration_gate_status,
    fit_calibration_model,
    walk_forward_metrics,
)
from src.analysis.shadow_calibration_store import SHADOW_CALIBRATION_SCHEMA_SQL
from scripts.run_shadow_calibration import render_report
from src.scheduler.runner import _render_shadow_calibration_gate_alert


ROOT = Path(__file__).resolve().parents[1]


def _examples(*, cohorts: int = 4, per_cohort: int = 100) -> list[CalibrationExample]:
    start = datetime(2026, 1, 2, tzinfo=timezone.utc)
    rows = []
    forecast_id = 1
    for cohort in range(cohorts):
        as_of = start + timedelta(days=cohort * 10)
        for index in range(per_cohort):
            raw_probability = 0.75 if index % 2 == 0 else 0.25
            raw_return = 0.08 if raw_probability > 0.5 else -0.04
            # Deliberately inverted evidence: high P+ forecasts fell and low P+ rose.
            realized = -0.03 if raw_probability > 0.5 else 0.02
            rows.append(
                CalibrationExample(
                    forecast_id=forecast_id,
                    ticker=f"T{index:03d}",
                    as_of_ts=as_of,
                    target_session_ts=as_of + timedelta(days=6),
                    horizon_sessions=5,
                    raw_expected_return=raw_return,
                    raw_probability_up=raw_probability,
                    raw_lower_return=raw_return - 0.10,
                    raw_upper_return=raw_return + 0.10,
                    realized_return=realized,
                )
            )
            forecast_id += 1
    return rows


def test_calibrator_detects_inverted_direction_and_keeps_raw_forecast_immutable():
    model = fit_calibration_model(_examples(), horizon_sessions=5)

    assert model.probability_slope < 0
    assert model.diagnostics["direction_inverted"] is True
    assert model.fit_metrics["calibrated_brier"] < model.fit_metrics["raw_brier"]

    projection = apply_calibration(
        source_forecast_id=99,
        raw_expected_return=0.08,
        raw_probability_up=0.75,
        model=model,
    )
    assert projection.source_forecast_id == 99
    assert projection.calibrated_probability_up < 0.5
    assert projection.calibration_status == "CALIBRATED_INVERTED_SIGNAL"
    assert projection.calibrated_lower_return <= projection.calibrated_upper_return


def test_walk_forward_uses_only_outcomes_available_before_test_cohort():
    metrics = walk_forward_metrics(_examples(), horizon_sessions=5)

    assert metrics["status"] == "AVAILABLE_LIMITED"
    assert metrics["samples"] == 100
    assert metrics["cohorts"] == 1
    assert metrics["leakage_guard"] == "train.target_session_ts < test.as_of_ts"


def test_gate_keeps_an_improving_but_small_walk_forward_sample_pending():
    assert calibration_gate_status({
        "status": "AVAILABLE_LIMITED",
        "cohorts": 1,
        "raw_brier": 0.30,
        "calibrated_brier": 0.25,
        "raw_mae": 0.06,
        "calibrated_mae": 0.05,
        "calibrated_interval_coverage": 0.80,
    }) == "PENDING_MORE_COHORTS"


def test_gate_rejects_a_calibrator_that_worsens_out_of_sample_metrics():
    assert calibration_gate_status({
        "status": "AVAILABLE_LIMITED",
        "cohorts": 4,
        "raw_brier": 0.25,
        "calibrated_brier": 0.34,
        "raw_mae": 0.05,
        "calibrated_mae": 0.06,
        "calibrated_interval_coverage": 0.69,
    }) == "FAILED_WALK_FORWARD"


def test_v3_schema_and_module_remain_outside_operational_tables():
    for table in (
        "shadow_calibration_runs",
        "shadow_calibration_models",
        "shadow_calibrated_forecasts",
        "shadow_calibration_gate_state",
        "shadow_calibration_gate_events",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in SHADOW_CALIBRATION_SCHEMA_SQL
    assert "decision_log" not in SHADOW_CALIBRATION_SCHEMA_SQL
    assert "horizon_sessions IN (5, 20)" in SHADOW_CALIBRATION_SCHEMA_SQL

    module_path = ROOT / "src" / "analysis" / "shadow_calibration.py"
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    imports = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }
    assert MODEL_VERSION == "price_trend_calibrated_shadow_v3"
    assert all("optimizer" not in name for name in imports)
    assert all("execution" not in name for name in imports)


def test_scheduler_runs_v3_only_after_a_successful_v2_run():
    source = (ROOT / "src" / "scheduler" / "runner.py").read_text(encoding="utf-8")
    shadow_job = source.index("async def run_thesis_shadow_job")
    calibration_job = source.index("async def run_shadow_calibration_job")
    call = source.index("await run_shadow_calibration_job()", shadow_job)

    assert shadow_job < call < calibration_job
    assert source.index('if proc.returncode != 0:', shadow_job) < call
    assert 'scripts/run_shadow_calibration.py' in source


def test_latest_report_accepts_jsonb_values_returned_as_text():
    report = render_report({
        "ok": True,
        "model_version": MODEL_VERSION,
        "models": [{
            "horizon_sessions": 5,
            "sample_count": 300,
            "cohort_count": 4,
            "parameters": '{"probability_slope": -0.2, "return_slope": -0.5}',
            "fit_metrics": '{"raw_brier": 0.3, "calibrated_brier": 0.2, "raw_mae": 0.06, "calibrated_mae": 0.05}',
            "walk_forward_metrics": '{"status": "AVAILABLE_LIMITED", "samples": 50, "cohorts": 1}',
            "diagnostics": '{"direction_inverted": true, "promotion_gate": "FAILED_WALK_FORWARD"}',
        }],
    })

    assert "gate=FAILED_WALK_FORWARD" in report


def test_gate_alert_reports_only_the_transition_and_operational_boundary():
    report = _render_shadow_calibration_gate_alert([
        {
            "horizon_sessions": 5,
            "previous_gate": "PENDING_MORE_COHORTS",
            "new_gate": "FAILED_WALK_FORWARD",
        }
    ])

    assert "5r" in report
    assert "Muestra insuficiente" in report
    assert "Rechazado" in report
    assert "No cambia Analisis, Radar, planes ni ordenes" in report
    assert "probability" not in report

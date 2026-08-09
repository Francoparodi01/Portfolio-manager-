"""Post-hoc calibration for the independent shadow price forecast.

This module consumes persisted v2 forecasts and their matured outcomes. It is
deliberately unable to create decisions, plans, sizing, or orders.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from math import exp, log
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
from sklearn.linear_model import HuberRegressor, LinearRegression, LogisticRegression


SOURCE_MODEL_VERSION = "price_trend_context_overlay_v2"
MODEL_VERSION = "price_trend_calibrated_shadow_v3"
SCHEMA_VERSION = 1
SUPPORTED_HORIZONS = (5, 20)
MIN_TRAIN_SAMPLES = 200
MIN_TRAIN_COHORTS = 3
MAX_ABS_REALIZED_RETURN = 1.0
PROBABILITY_EPSILON = 1e-4


@dataclass(frozen=True)
class CalibrationExample:
    forecast_id: int
    ticker: str
    as_of_ts: datetime
    target_session_ts: datetime
    horizon_sessions: int
    raw_expected_return: float
    raw_probability_up: float
    raw_lower_return: float
    raw_upper_return: float
    realized_return: float


@dataclass(frozen=True)
class CalibrationModel:
    horizon_sessions: int
    sample_count: int
    cohort_count: int
    train_start_ts: datetime
    train_end_ts: datetime
    probability_intercept: float
    probability_slope: float
    return_intercept: float
    return_slope: float
    residual_lower: float
    residual_upper: float
    fit_metrics: dict[str, Any]
    diagnostics: dict[str, Any]

    def to_parameters(self) -> dict[str, float]:
        return {
            "probability_intercept": self.probability_intercept,
            "probability_slope": self.probability_slope,
            "return_intercept": self.return_intercept,
            "return_slope": self.return_slope,
            "residual_lower": self.residual_lower,
            "residual_upper": self.residual_upper,
        }


@dataclass(frozen=True)
class CalibratedProjection:
    source_forecast_id: int
    calibrated_expected_return: float
    calibrated_probability_up: float
    calibrated_lower_return: float
    calibrated_upper_return: float
    calibration_status: str


def fit_calibration_model(
    examples: Sequence[CalibrationExample],
    *,
    horizon_sessions: int,
) -> CalibrationModel:
    clean = _clean_examples(examples, horizon_sessions=horizon_sessions)
    cohorts = {item.as_of_ts.date() for item in clean}
    if len(clean) < MIN_TRAIN_SAMPLES or len(cohorts) < MIN_TRAIN_COHORTS:
        raise ValueError(
            f"horizon {horizon_sessions}: insufficient calibration evidence "
            f"(samples={len(clean)}, cohorts={len(cohorts)})"
        )

    weights = _cohort_weights(clean)
    raw_probabilities = np.asarray(
        [_clip(item.raw_probability_up, PROBABILITY_EPSILON, 1.0 - PROBABILITY_EPSILON) for item in clean],
        dtype=float,
    )
    probability_feature = np.asarray(
        [[_logit(value)] for value in raw_probabilities],
        dtype=float,
    )
    direction = np.asarray([item.realized_return > 0.0 for item in clean], dtype=int)

    if len(set(direction.tolist())) == 1:
        base_rate = _weighted_mean(direction.astype(float), weights)
        probability_intercept = _logit(
            _clip(base_rate, PROBABILITY_EPSILON, 1.0 - PROBABILITY_EPSILON)
        )
        probability_slope = 0.0
        probability_fit = "constant_single_class"
    else:
        probability_model = LogisticRegression(C=1.0, solver="lbfgs", max_iter=1000)
        probability_model.fit(probability_feature, direction, sample_weight=weights)
        probability_intercept = float(probability_model.intercept_[0])
        probability_slope = float(probability_model.coef_[0][0])
        probability_fit = "logistic_logit"

    expected = np.asarray([item.raw_expected_return for item in clean], dtype=float)
    realized = np.asarray([item.realized_return for item in clean], dtype=float)
    return_fit = "huber"
    try:
        return_model = HuberRegressor(epsilon=1.35, alpha=0.01, max_iter=1000)
        return_model.fit(expected.reshape(-1, 1), realized, sample_weight=weights)
    except ValueError:
        return_fit = "weighted_linear_fallback"
        return_model = LinearRegression()
        return_model.fit(expected.reshape(-1, 1), realized, sample_weight=weights)
    return_intercept = float(return_model.intercept_)
    return_slope = float(return_model.coef_[0])

    calibrated_expected = np.asarray(
        [_calibrated_return(value, return_intercept, return_slope) for value in expected],
        dtype=float,
    )
    residuals = realized - calibrated_expected
    residual_lower = _weighted_quantile(residuals, weights, 0.10)
    residual_upper = _weighted_quantile(residuals, weights, 0.90)
    if residual_lower > residual_upper:
        residual_lower, residual_upper = residual_upper, residual_lower

    calibrated_probabilities = np.asarray(
        [
            _sigmoid(probability_intercept + probability_slope * _logit(value))
            for value in raw_probabilities
        ],
        dtype=float,
    )
    calibrated_lower = calibrated_expected + residual_lower
    calibrated_upper = calibrated_expected + residual_upper
    fit_metrics = {
        "scope": "fit_only_not_out_of_sample",
        "raw_brier": round(_weighted_mean((raw_probabilities - direction) ** 2, weights), 8),
        "calibrated_brier": round(
            _weighted_mean((calibrated_probabilities - direction) ** 2, weights), 8
        ),
        "raw_mae": round(_weighted_mean(np.abs(expected - realized), weights), 8),
        "calibrated_mae": round(
            _weighted_mean(np.abs(calibrated_expected - realized), weights), 8
        ),
        "raw_interval_coverage": round(
            _weighted_mean(
                np.asarray(
                    [
                        item.raw_lower_return <= item.realized_return <= item.raw_upper_return
                        for item in clean
                    ],
                    dtype=float,
                ),
                weights,
            ),
            8,
        ),
        "calibrated_interval_coverage": round(
            _weighted_mean(
                ((realized >= calibrated_lower) & (realized <= calibrated_upper)).astype(float),
                weights,
            ),
            8,
        ),
        "observed_up_rate": round(_weighted_mean(direction.astype(float), weights), 8),
    }
    diagnostics = {
        "probability_fit": probability_fit,
        "return_fit": return_fit,
        "cohort_weighting": "equal_weight_per_as_of_date",
        "interval": "weighted_residual_q10_q90",
        "direction_inverted": probability_slope < 0.0,
        "operational_effect": False,
    }
    return CalibrationModel(
        horizon_sessions=int(horizon_sessions),
        sample_count=len(clean),
        cohort_count=len(cohorts),
        train_start_ts=min(item.as_of_ts for item in clean),
        train_end_ts=max(item.target_session_ts for item in clean),
        probability_intercept=round(probability_intercept, 10),
        probability_slope=round(probability_slope, 10),
        return_intercept=round(return_intercept, 10),
        return_slope=round(return_slope, 10),
        residual_lower=round(float(residual_lower), 10),
        residual_upper=round(float(residual_upper), 10),
        fit_metrics=fit_metrics,
        diagnostics=diagnostics,
    )


def apply_calibration(
    *,
    source_forecast_id: int,
    raw_expected_return: float,
    raw_probability_up: float,
    model: CalibrationModel,
) -> CalibratedProjection:
    raw_probability = _clip(
        float(raw_probability_up), PROBABILITY_EPSILON, 1.0 - PROBABILITY_EPSILON
    )
    probability = _sigmoid(
        model.probability_intercept + model.probability_slope * _logit(raw_probability)
    )
    expected = _calibrated_return(
        float(raw_expected_return), model.return_intercept, model.return_slope
    )
    lower = _clip(expected + model.residual_lower, -0.95, 2.0)
    upper = _clip(expected + model.residual_upper, -0.95, 2.0)
    gate = str(model.diagnostics.get("promotion_gate") or "").upper()
    if gate == "FAILED_WALK_FORWARD":
        status = "EVALUATION_ONLY_FAILED_GATE"
    elif gate in {"PENDING_PROSPECTIVE_EVIDENCE", "PENDING_MORE_COHORTS"}:
        status = "EVALUATION_ONLY_PENDING"
    elif gate == "CANDIDATE_AFTER_FORWARD_TEST":
        status = "EVALUATION_ONLY_CANDIDATE"
    else:
        status = (
            "CALIBRATED_INVERTED_SIGNAL"
            if model.diagnostics.get("direction_inverted")
            else "CALIBRATED_SHADOW"
        )
    return CalibratedProjection(
        source_forecast_id=int(source_forecast_id),
        calibrated_expected_return=round(expected, 8),
        calibrated_probability_up=round(_clip(probability, 0.0, 1.0), 6),
        calibrated_lower_return=round(min(lower, upper), 8),
        calibrated_upper_return=round(max(lower, upper), 8),
        calibration_status=status,
    )


def walk_forward_metrics(
    examples: Sequence[CalibrationExample],
    *,
    horizon_sessions: int,
) -> dict[str, Any]:
    clean = _clean_examples(examples, horizon_sessions=horizon_sessions)
    cohorts = sorted({item.as_of_ts for item in clean})
    predictions: list[tuple[CalibrationExample, CalibratedProjection]] = []
    tested_cohorts = 0
    for test_as_of in cohorts:
        train = [item for item in clean if item.target_session_ts < test_as_of]
        test = [item for item in clean if item.as_of_ts == test_as_of]
        if not test:
            continue
        try:
            model = fit_calibration_model(train, horizon_sessions=horizon_sessions)
        except ValueError:
            continue
        tested_cohorts += 1
        predictions.extend(
            (
                item,
                apply_calibration(
                    source_forecast_id=item.forecast_id,
                    raw_expected_return=item.raw_expected_return,
                    raw_probability_up=item.raw_probability_up,
                    model=model,
                ),
            )
            for item in test
        )

    if not predictions:
        return {
            "status": "PENDING_PROSPECTIVE_EVIDENCE",
            "samples": 0,
            "cohorts": 0,
            "note": "No leakage-free cohort has enough previously matured outcomes.",
        }

    evaluated_examples = [item for item, _ in predictions]
    weights = _cohort_weights(evaluated_examples)
    direction = np.asarray(
        [item.realized_return > 0.0 for item in evaluated_examples], dtype=float
    )
    raw_probability = np.asarray(
        [item.raw_probability_up for item in evaluated_examples], dtype=float
    )
    calibrated_probability = np.asarray(
        [projection.calibrated_probability_up for _, projection in predictions], dtype=float
    )
    raw_expected = np.asarray(
        [item.raw_expected_return for item in evaluated_examples], dtype=float
    )
    calibrated_expected = np.asarray(
        [projection.calibrated_expected_return for _, projection in predictions], dtype=float
    )
    realized = np.asarray([item.realized_return for item in evaluated_examples], dtype=float)
    coverage = np.asarray(
        [
            projection.calibrated_lower_return
            <= item.realized_return
            <= projection.calibrated_upper_return
            for item, projection in predictions
        ],
        dtype=float,
    )
    return {
        "status": "AVAILABLE_LIMITED" if tested_cohorts < 5 else "AVAILABLE",
        "samples": len(predictions),
        "cohorts": tested_cohorts,
        "raw_brier": round(_weighted_mean((raw_probability - direction) ** 2, weights), 8),
        "calibrated_brier": round(
            _weighted_mean((calibrated_probability - direction) ** 2, weights), 8
        ),
        "raw_mae": round(_weighted_mean(np.abs(raw_expected - realized), weights), 8),
        "calibrated_mae": round(
            _weighted_mean(np.abs(calibrated_expected - realized), weights), 8
        ),
        "calibrated_interval_coverage": round(_weighted_mean(coverage, weights), 8),
        "leakage_guard": "train.target_session_ts < test.as_of_ts",
    }


def calibration_gate_status(metrics: Mapping[str, Any]) -> str:
    """Keep every v3 result experimental until it beats v2 out of sample."""
    if not metrics or not str(metrics.get("status") or "").startswith("AVAILABLE"):
        return "PENDING_PROSPECTIVE_EVIDENCE"
    raw_brier = float(metrics.get("raw_brier") or 0.0)
    calibrated_brier = float(metrics.get("calibrated_brier") or 0.0)
    raw_mae = float(metrics.get("raw_mae") or 0.0)
    calibrated_mae = float(metrics.get("calibrated_mae") or 0.0)
    coverage = float(metrics.get("calibrated_interval_coverage") or 0.0)
    if (
        calibrated_brier >= raw_brier
        or calibrated_mae >= raw_mae
        or not 0.75 <= coverage <= 0.85
    ):
        return "FAILED_WALK_FORWARD"
    if int(metrics.get("cohorts") or 0) < 5:
        return "PENDING_MORE_COHORTS"
    return "CANDIDATE_AFTER_FORWARD_TEST"


def examples_from_rows(rows: Iterable[dict[str, Any]]) -> list[CalibrationExample]:
    return [
        CalibrationExample(
            forecast_id=int(row["forecast_id"]),
            ticker=str(row["ticker"]).upper(),
            as_of_ts=row["as_of_ts"],
            target_session_ts=row["target_session_ts"],
            horizon_sessions=int(row["horizon_sessions"]),
            raw_expected_return=float(row["raw_expected_return"]),
            raw_probability_up=float(row["raw_probability_up"]),
            raw_lower_return=float(row["raw_lower_return"]),
            raw_upper_return=float(row["raw_upper_return"]),
            realized_return=float(row["realized_return"]),
        )
        for row in rows
    ]


def _clean_examples(
    examples: Sequence[CalibrationExample],
    *,
    horizon_sessions: int,
) -> list[CalibrationExample]:
    return [
        item
        for item in examples
        if item.horizon_sessions == int(horizon_sessions)
        and abs(item.realized_return) <= MAX_ABS_REALIZED_RETURN
        and -1.0 < item.raw_expected_return < 5.0
        and 0.0 <= item.raw_probability_up <= 1.0
    ]


def _cohort_weights(examples: Sequence[CalibrationExample]) -> np.ndarray:
    counts = Counter(item.as_of_ts.date() for item in examples)
    return np.asarray([1.0 / counts[item.as_of_ts.date()] for item in examples], dtype=float)


def _weighted_mean(values: np.ndarray, weights: np.ndarray) -> float:
    return float(np.average(np.asarray(values, dtype=float), weights=weights))


def _weighted_quantile(
    values: np.ndarray,
    weights: np.ndarray,
    quantile: float,
) -> float:
    order = np.argsort(values)
    ordered_values = np.asarray(values, dtype=float)[order]
    ordered_weights = np.asarray(weights, dtype=float)[order]
    cumulative = np.cumsum(ordered_weights)
    threshold = _clip(float(quantile), 0.0, 1.0) * float(cumulative[-1])
    index = min(int(np.searchsorted(cumulative, threshold, side="left")), len(ordered_values) - 1)
    return float(ordered_values[index])


def _calibrated_return(raw: float, intercept: float, slope: float) -> float:
    return _clip(float(intercept) + float(slope) * float(raw), -0.95, 2.0)


def _logit(probability: float) -> float:
    value = _clip(float(probability), PROBABILITY_EPSILON, 1.0 - PROBABILITY_EPSILON)
    return log(value / (1.0 - value))


def _sigmoid(value: float) -> float:
    if value >= 0.0:
        factor = exp(-value)
        return 1.0 / (1.0 + factor)
    factor = exp(value)
    return factor / (1.0 + factor)


def _clip(value: float, low: float, high: float) -> float:
    return max(low, min(high, float(value)))

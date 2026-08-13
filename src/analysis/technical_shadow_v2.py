"""Regime-aware technical score evaluated in shadow only."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from src.analysis.trend_regime import TrendRegime


TECHNICAL_SHADOW_V2_VERSION = "technical-shadow-v2"
EVALUATION_HORIZONS = (5, 10, 20, 40)
POSITIVE_BIAS_MIN = 0.20
NEGATIVE_BIAS_MAX = -0.20


def _clip(value: float) -> float:
    return max(-1.0, min(1.0, float(value)))


@dataclass(frozen=True, slots=True)
class TechnicalShadowV2:
    version: str
    regime: str
    score: float
    bias: str
    rule: str
    trend_input: float
    reversion_input: float
    trend_contribution: float
    reversion_contribution: float
    extension_risk: float
    structural_break_gate: bool
    reversal_confirmation_required: bool
    baseline_score_raw: float | None
    calibration_status: str = "UNVALIDATED"
    evaluation_horizons: tuple[int, ...] = EVALUATION_HORIZONS
    affects_analysis: bool = False
    affects_execution: bool = False

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["evaluation_horizons"] = list(self.evaluation_horizons)
        return payload


def build_technical_shadow_v2(
    *,
    regime: str,
    trend_score: float,
    reversion_score: float,
    structural_break_confirmed: bool = False,
    baseline_score_raw: float | None = None,
) -> TechnicalShadowV2:
    """Combine existing shadow factors according to market regime.

    This function is intentionally disconnected from synthesis and execution.
    It creates a candidate score for later fixed-horizon evaluation.
    """
    normalized_regime = str(regime or TrendRegime.TRANSITIONAL.value).upper()
    trend = _clip(trend_score)
    reversion = _clip(reversion_score)
    extension_risk = 0.0
    reversal_confirmation_required = False

    if normalized_regime == TrendRegime.STRONG_UPTREND.value:
        trend_contribution = 0.85 * trend
        reversion_contribution = 0.15 * max(reversion, 0.0)
        extension_risk = max(-reversion, 0.0)
        rule = "trend_continuation"
    elif normalized_regime == TrendRegime.RANGE.value:
        trend_contribution = 0.25 * trend
        reversion_contribution = 0.75 * reversion
        rule = "range_reversion"
    elif normalized_regime == TrendRegime.DOWNTREND.value:
        trend_contribution = 0.85 * trend
        reversion_contribution = 0.15 * min(reversion, 0.0)
        reversal_confirmation_required = reversion > 0.0
        rule = "downtrend_confirmation"
    else:
        normalized_regime = TrendRegime.TRANSITIONAL.value
        trend_contribution = 0.45 * trend
        reversion_contribution = 0.25 * reversion
        rule = "transitional_reduced"

    score = _clip(trend_contribution + reversion_contribution)
    structural_break_gate = bool(structural_break_confirmed)
    if structural_break_gate:
        score = min(score, 0.0)

    if score >= POSITIVE_BIAS_MIN:
        bias = "POSITIVE"
    elif score <= NEGATIVE_BIAS_MAX:
        bias = "NEGATIVE"
    else:
        bias = "NEUTRAL"

    return TechnicalShadowV2(
        version=TECHNICAL_SHADOW_V2_VERSION,
        regime=normalized_regime,
        score=round(score, 4),
        bias=bias,
        rule=rule,
        trend_input=round(trend, 4),
        reversion_input=round(reversion, 4),
        trend_contribution=round(trend_contribution, 4),
        reversion_contribution=round(reversion_contribution, 4),
        extension_risk=round(extension_risk, 4),
        structural_break_gate=structural_break_gate,
        reversal_confirmation_required=reversal_confirmation_required,
        baseline_score_raw=(
            round(float(baseline_score_raw), 4)
            if baseline_score_raw is not None
            else None
        ),
    )


__all__ = [
    "EVALUATION_HORIZONS",
    "TECHNICAL_SHADOW_V2_VERSION",
    "TechnicalShadowV2",
    "build_technical_shadow_v2",
]

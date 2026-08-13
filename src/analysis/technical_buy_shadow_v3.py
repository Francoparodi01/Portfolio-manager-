"""BUY-only technical candidate classification evaluated in shadow.

The V3 layer narrows technical-shadow-v2 to the discovery problem: finding
new long candidates for a 20-day outcome review. It cannot emit SELL decisions or
change Radar ranking, portfolio analysis, plans, sizing, or execution.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping

from src.analysis.trend_regime import TrendRegime


TECHNICAL_BUY_SHADOW_V3_VERSION = "technical-buy-shadow-v3"
SOURCE_VERSION = "technical-shadow-v2"
TARGET_HORIZON_DAYS = 20
MIN_SOURCE_SCORE = 0.20
MIN_VOLUME_QUALITY_WARNING = 0.50


@dataclass(frozen=True, slots=True)
class TechnicalBuyShadowV3:
    version: str
    source_version: str
    objective: str
    target_horizon_days: int
    classification: str
    priority_tier: str
    eligible_for_buy_research: bool
    regime: str
    source_score: float
    trend_input: float
    reversion_input: float
    asset_type: str
    source_mode: str
    volume_quality_20: float | None
    gates: tuple[str, ...]
    warnings: tuple[str, ...]
    benchmark: str = "same_date_eligible_universe_median"
    calibration_status: str = "SHADOW_UNVALIDATED"
    affects_radar_ranking: bool = False
    affects_analysis: bool = False
    affects_execution: bool = False

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["gates"] = list(self.gates)
        payload["warnings"] = list(self.warnings)
        return payload


def build_technical_buy_shadow_v3(
    *,
    technical_shadow_v2: Mapping[str, Any] | None,
    regime: str,
    trend_score: float,
    reversion_score: float,
    structural_break_confirmed: bool = False,
    asset_type: str = "UNKNOWN",
    source_mode: str = "unknown",
    volume_quality_20: float | None = None,
) -> TechnicalBuyShadowV3:
    """Classify a V2 observation for BUY research without changing policy."""
    source = dict(technical_shadow_v2 or {})
    normalized_regime = str(regime or TrendRegime.TRANSITIONAL.value).upper()
    source_score = _number(source.get("score"))
    source_bias = str(source.get("bias") or "NEUTRAL").upper()
    gates: list[str] = []
    warnings: list[str] = []

    if source.get("version") not in {None, "", SOURCE_VERSION}:
        gates.append("SOURCE_VERSION_MISMATCH")
    if source_bias != "POSITIVE" or source_score < MIN_SOURCE_SCORE:
        gates.append("V2_BUY_NOT_ACTIVE")
    if structural_break_confirmed or bool(source.get("structural_break_gate")):
        gates.append("STRUCTURAL_BREAK")
    if normalized_regime == TrendRegime.DOWNTREND.value:
        gates.append("DOWNTREND_NOT_ELIGIBLE")

    normalized_asset_type = str(asset_type or "UNKNOWN").upper()
    normalized_source_mode = str(source_mode or "unknown").lower()
    normalized_volume_quality = (
        max(0.0, min(1.0, float(volume_quality_20)))
        if volume_quality_20 is not None
        else None
    )
    if normalized_volume_quality is None:
        warnings.append("VOLUME_COVERAGE_UNKNOWN")
    elif normalized_volume_quality < MIN_VOLUME_QUALITY_WARNING:
        warnings.append("VOLUME_COVERAGE_LOW")
    if normalized_source_mode in {"mixed", "reconstructed", "unknown"}:
        warnings.append("PRICE_SOURCE_NOT_FULLY_OFFICIAL")
    if normalized_asset_type == "CEDEAR":
        warnings.append("CEDEAR_LOCAL_PRICE_INCLUDES_CCL")

    if gates:
        classification = "REJECTED_FOR_BUY_RESEARCH"
        priority_tier = "REJECTED"
        eligible = False
    elif normalized_regime == TrendRegime.STRONG_UPTREND.value:
        classification = "PRIMARY_BUY_CANDIDATE"
        priority_tier = "A"
        eligible = True
    elif normalized_regime == TrendRegime.RANGE.value and float(reversion_score) > 0.0:
        classification = "SECONDARY_BUY_CANDIDATE"
        priority_tier = "B"
        eligible = True
    else:
        classification = "WATCH_BUY_SETUP"
        priority_tier = "C"
        eligible = False

    return TechnicalBuyShadowV3(
        version=TECHNICAL_BUY_SHADOW_V3_VERSION,
        source_version=SOURCE_VERSION,
        objective="NEW_POSITION_BUY_DISCOVERY",
        target_horizon_days=TARGET_HORIZON_DAYS,
        classification=classification,
        priority_tier=priority_tier,
        eligible_for_buy_research=eligible,
        regime=normalized_regime,
        source_score=round(source_score, 4),
        trend_input=round(float(trend_score), 4),
        reversion_input=round(float(reversion_score), 4),
        asset_type=normalized_asset_type,
        source_mode=normalized_source_mode,
        volume_quality_20=(
            round(normalized_volume_quality, 4)
            if normalized_volume_quality is not None
            else None
        ),
        gates=tuple(gates),
        warnings=tuple(dict.fromkeys(warnings)),
    )


def _number(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


__all__ = [
    "MIN_SOURCE_SCORE",
    "SOURCE_VERSION",
    "TARGET_HORIZON_DAYS",
    "TECHNICAL_BUY_SHADOW_V3_VERSION",
    "TechnicalBuyShadowV3",
    "build_technical_buy_shadow_v3",
]

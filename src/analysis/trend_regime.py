"""Per-ticker trend regime and shadow trend score.

This module is deliberately independent from the production scoring model.
Its outputs are audit context except for the defensive strong-uptrend sell
guard consumed by the execution planner.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class TrendRegime(str, Enum):
    STRONG_UPTREND = "STRONG_UPTREND"
    RANGE = "RANGE"
    DOWNTREND = "DOWNTREND"
    TRANSITIONAL = "TRANSITIONAL"


@dataclass(frozen=True)
class TrendAssessment:
    regime: TrendRegime
    trend_score: float
    structural_break_confirmed: bool
    overbought_momentum: bool
    components: dict[str, float]


def _clip(value: float, low: float = -1.0, high: float = 1.0) -> float:
    return max(low, min(high, float(value)))


def classify_regime(indicator) -> TrendRegime:
    """Classify the requested ADX/DI/SMA200 regime without changing scores."""
    adx = float(indicator.adx_14)
    if (
        adx > 25.0
        and float(indicator.di_plus) > float(indicator.di_minus)
        and float(indicator.sma_200) > 0.0
        and float(indicator.close) > float(indicator.sma_200)
    ):
        return TrendRegime.STRONG_UPTREND
    if adx < 20.0:
        return TrendRegime.RANGE
    if adx > 25.0 and float(indicator.di_minus) > float(indicator.di_plus):
        return TrendRegime.DOWNTREND
    return TrendRegime.TRANSITIONAL


def calculate_trend_score(indicator) -> tuple[float, dict[str, float]]:
    """Return a bounded shadow score based only on trend structure."""
    di_direction = 1.0 if indicator.di_plus > indicator.di_minus else -1.0
    adx_strength = _clip((float(indicator.adx_14) - 20.0) / 20.0, 0.0, 1.0)
    directional_adx = di_direction * adx_strength

    sma_200 = float(indicator.sma_200)
    if sma_200 > 0.0:
        distance_200 = _clip((float(indicator.close) - sma_200) / sma_200 / 0.10)
    else:
        distance_200 = 0.0

    if indicator.sma_20 > indicator.sma_50 > indicator.sma_200 > 0:
        moving_average_alignment = 1.0
    elif 0 < indicator.sma_20 < indicator.sma_50 < indicator.sma_200:
        moving_average_alignment = -1.0
    else:
        moving_average_alignment = 0.0

    macd_direction = 1.0 if indicator.macd_hist > 0 else (-1.0 if indicator.macd_hist < 0 else 0.0)
    components = {
        "directional_adx": round(directional_adx, 4),
        "price_vs_sma200": round(distance_200, 4),
        "ma_alignment": moving_average_alignment,
        "macd_direction": macd_direction,
    }
    score = (
        0.40 * directional_adx
        + 0.30 * distance_200
        + 0.20 * moving_average_alignment
        + 0.10 * macd_direction
    )
    return round(_clip(score), 4), components


def assess_trend(indicator) -> TrendAssessment:
    """Build the full shadow assessment used by reports and audit logging."""
    trend_score, components = calculate_trend_score(indicator)
    structural_break = bool(
        float(indicator.sma_50) > 0.0
        and float(indicator.close) < float(indicator.sma_50)
        and float(indicator.ema_12) < float(indicator.ema_26)
        and float(indicator.macd_hist) < 0.0
    )
    overbought = bool(
        float(indicator.rsi_14) > 70.0
        or float(indicator.stoch_k) > 80.0
        or float(indicator.williams_r) > -20.0
    )
    return TrendAssessment(
        regime=classify_regime(indicator),
        trend_score=trend_score,
        structural_break_confirmed=structural_break,
        overbought_momentum=overbought,
        components=components,
    )

"""Optimizer Calibration v2 challenger.

Pure, auditable, fail-closed calibration utilities. This module has no capital
authority and never replaces the production optimizer.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import date
from math import sqrt
from statistics import mean, pstdev
from typing import Iterable, Optional

HORIZONS = (5, 10, 20, 40)
ALLOWED_SCOPE = "PRIMARY"
CALIBRATION_VERSION = "calibrated-optimizer-v2-shadow-1"
MIN_SAMPLE = 30

@dataclass(frozen=True)
class CalibrationEpisode:
    ticker: str
    asof_date: date
    horizon_days: int
    score: float
    future_return: float
    regime: str = "UNKNOWN"
    asset_group: str = "UNKNOWN"
    quality: str = "LOW"
    scope: str = "PRIMARY"
    technical: float = 0.0
    macro: float = 0.0
    risk: float = 0.0
    sentiment: float = 0.0
    trend: float = 0.0
    conviction: float = 0.0
    outcome_date: Optional[date] = None

@dataclass(frozen=True)
class CalibratedView:
    expected_return: Optional[float]
    confidence: float
    n: int
    rank_ic: Optional[float]
    ci_low: Optional[float]
    ci_high: Optional[float]
    quality: str
    fail_closed: bool
    reason: str

@dataclass
class WalkForwardCalibration:
    train_end: date
    score_bins: list[tuple[float, float, float, int]] = field(default_factory=list)

    def predict(self, score: float) -> Optional[float]:
        matches = [row for row in self.score_bins if row[0] <= score <= row[1]]
        if not matches:
            return None
        return matches[0][2]

def _rank(values):
    order = sorted(range(len(values)), key=lambda i: values[i])
    ranks = [0.0] * len(values)
    for rank, idx in enumerate(order, start=1):
        ranks[idx] = float(rank)
    return ranks

def _corr(x, y):
    if len(x) < 3 or len(x) != len(y):
        return None
    mx, my = mean(x), mean(y)
    dx, dy = [v-mx for v in x], [v-my for v in y]
    denom = sqrt(sum(v*v for v in dx) * sum(v*v for v in dy))
    return None if denom <= 1e-12 else sum(a*b for a,b in zip(dx,dy)) / denom

def _dedupe_primary(episodes: Iterable[CalibrationEpisode]) -> list[CalibrationEpisode]:
    chosen = {}
    quality_rank = {"HIGH": 3, "MEDIUM": 2, "LOW": 1}
    for ep in episodes:
        if ep.scope != ALLOWED_SCOPE or ep.horizon_days not in HORIZONS:
            continue
        if ep.outcome_date is None or ep.outcome_date <= ep.asof_date:
            continue
        key = (ep.ticker.upper(), ep.asof_date, ep.horizon_days)
        prior = chosen.get(key)
        if prior is None or quality_rank.get(ep.quality.upper(), 0) > quality_rank.get(prior.quality.upper(), 0):
            chosen[key] = ep
    return sorted(chosen.values(), key=lambda e: (e.asof_date, e.ticker, e.horizon_days))

def fit_walk_forward_binning(
    episodes: Iterable[CalibrationEpisode],
    *,
    train_end: date,
    horizon_days: int,
    bins: int = 5,
) -> WalkForwardCalibration:
    train = [
        ep for ep in _dedupe_primary(episodes)
        if ep.horizon_days == horizon_days and ep.outcome_date <= train_end
    ]
    if len(train) < MIN_SAMPLE:
        return WalkForwardCalibration(train_end=train_end)
    ordered = sorted(train, key=lambda e: e.score)
    size = max(1, len(ordered) // bins)
    rows = []
    for start in range(0, len(ordered), size):
        chunk = ordered[start:start+size]
        if len(chunk) < 3:
            continue
        rows.append((chunk[0].score, chunk[-1].score, mean(e.future_return for e in chunk), len(chunk)))
    return WalkForwardCalibration(train_end=train_end, score_bins=rows)

def calibrated_view(
    episodes: Iterable[CalibrationEpisode],
    *,
    asof_date: date,
    horizon_days: int,
    score: float,
    regime: str = "UNKNOWN",
    asset_group: str = "UNKNOWN",
) -> CalibratedView:
    eligible = [
        ep for ep in _dedupe_primary(episodes)
        if ep.horizon_days == horizon_days
        and ep.outcome_date <= asof_date
        and (regime == "UNKNOWN" or ep.regime == regime)
        and (asset_group == "UNKNOWN" or ep.asset_group == asset_group)
    ]
    n = len(eligible)
    if n < MIN_SAMPLE:
        return CalibratedView(None, 0.0, n, None, None, None, "LOW", True, "insufficient_oos_sample")
    model = fit_walk_forward_binning(eligible, train_end=asof_date, horizon_days=horizon_days)
    predicted = model.predict(score)
    if predicted is None:
        return CalibratedView(None, 0.0, n, None, None, None, "LOW", True, "score_outside_calibrated_support")
    returns = [ep.future_return for ep in eligible]
    ic = _corr(_rank([ep.score for ep in eligible]), _rank(returns))
    sigma = pstdev(returns) if n > 1 else 0.0
    se = sigma / sqrt(n) if n else 0.0
    ci_low, ci_high = predicted - 1.96 * se, predicted + 1.96 * se
    high_quality = sum(ep.quality.upper() == "HIGH" for ep in eligible) / n
    ic_strength = max(0.0, min(1.0, (ic or 0.0) / 0.10))
    sample_strength = max(0.0, min(1.0, (n - MIN_SAMPLE) / 70.0))
    confidence = min(0.95, 0.15 + 0.35*sample_strength + 0.30*ic_strength + 0.20*high_quality)
    quality = "HIGH" if confidence >= 0.70 else "MEDIUM" if confidence >= 0.40 else "LOW"
    return CalibratedView(predicted, confidence, n, ic, ci_low, ci_high, quality, False, "oos_primary_only")

@dataclass(frozen=True)
class OptimizerRunContext:
    portfolio_snapshot_id: str
    market_snapshot_id: str
    constraints_hash: str
    cash_ars: float
    fees_pct: float
    frozen_weight: float

def assert_same_run_context(champion: OptimizerRunContext, challenger: OptimizerRunContext) -> None:
    if champion != challenger:
        raise ValueError("Champion/challenger context mismatch")

PROMOTION_CRITERIA = {
    "min_oos_episodes": 100,
    "requires_multiple_regimes": True,
    "requires_multiple_tickers": True,
    "net_ev_must_improve": True,
    "no_material_drawdown_deterioration": True,
    "turnover_and_fees_must_be_reasonable": True,
    "single_ticker_dependency_forbidden": True,
    "temporal_stability_required": True,
    "win_rate_alone_is_never_sufficient": True,
}
CAPITAL_AUTHORITY = False
MODE = "SHADOW_ONLY"

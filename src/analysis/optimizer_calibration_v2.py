"""Optimizer Calibration v2 challenger.

Pure, auditable, fail-closed calibration utilities. This module has no capital
authority and never replaces the production optimizer. Evidence is PRIMARY-only,
point-in-time, deduplicated by ticker/day/horizon and evaluated walk-forward.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from math import sqrt
from statistics import mean, pstdev
from typing import Iterable, Optional

HORIZONS = (5, 10, 20, 40)
ALLOWED_SCOPE = "PRIMARY"
CALIBRATION_VERSION = "calibrated-optimizer-v2-shadow-2"
MIN_SAMPLE = 30
MODE = "SHADOW_ONLY"
CAPITAL_AUTHORITY = False


@dataclass(frozen=True)
class CalibrationEpisode:
    # Keep the original positional field order for backward compatibility.
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
    # v2.2 PIT/audit fields; appended so old constructors keep working.
    outcome_available_at: Optional[date] = None
    price: Optional[float] = None
    outcome: Optional[str] = None
    quality_flags: tuple[str, ...] = ()
    dataset_scope: str = "PRIMARY"


@dataclass(frozen=True)
class CalibratedView:
    # Original fields first for compatibility.
    expected_return: Optional[float]
    confidence: float
    n: int
    rank_ic: Optional[float]
    ci_low: Optional[float]
    ci_high: Optional[float]
    quality: str
    fail_closed: bool
    reason: str
    # Extra evidence used to calibrate confidence.
    dispersion: Optional[float] = None
    horizon_consistency: Optional[float] = None
    score_bucket: str = "UNKNOWN"
    reconstruction_quality: float = 0.0
    calibration_version: str = CALIBRATION_VERSION


@dataclass
class WalkForwardCalibration:
    train_end: date
    score_bins: list[tuple[float, float, float, int]] = field(default_factory=list)

    def predict(self, score: float) -> Optional[float]:
        matches = [row for row in self.score_bins if row[0] <= score <= row[1]]
        if not matches:
            return None
        return matches[0][2]


@dataclass(frozen=True)
class WalkForwardPrediction:
    ticker: str
    asof_date: date
    horizon_days: int
    score: float
    predicted_return: float
    realized_return: float
    confidence: float
    n_train: int
    train_end: date


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


def _available_at(ep: CalibrationEpisode) -> Optional[date]:
    """Timestamp when the outcome was actually usable by the calibrator."""
    return ep.outcome_available_at or ep.outcome_date


def _rank(values):
    """Average-tie ranks for a small auditable Spearman-style rank IC."""
    indexed = sorted(enumerate(values), key=lambda item: item[1])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(indexed):
        j = i
        while j + 1 < len(indexed) and indexed[j + 1][1] == indexed[i][1]:
            j += 1
        avg_rank = ((i + 1) + (j + 1)) / 2.0
        for k in range(i, j + 1):
            ranks[indexed[k][0]] = avg_rank
        i = j + 1
    return ranks


def _corr(x, y):
    if len(x) < 3 or len(x) != len(y):
        return None
    mx, my = mean(x), mean(y)
    dx, dy = [v - mx for v in x], [v - my for v in y]
    denom = sqrt(sum(v * v for v in dx) * sum(v * v for v in dy))
    return None if denom <= 1e-12 else sum(a * b for a, b in zip(dx, dy)) / denom


def _score_bucket(score: float) -> str:
    if score < -0.18:
        return "<-0.18"
    if score < -0.12:
        return "-0.18:-0.12"
    if score < -0.08:
        return "-0.12:-0.08"
    if score < -0.05:
        return "-0.08:-0.05"
    if score < 0.05:
        return "-0.05:+0.05"
    if score < 0.08:
        return "+0.05:+0.08"
    if score < 0.12:
        return "+0.08:+0.12"
    if score < 0.18:
        return "+0.12:+0.18"
    return ">=+0.18"


def _dedupe_primary(episodes: Iterable[CalibrationEpisode]) -> list[CalibrationEpisode]:
    chosen: dict[tuple[str, date, int], CalibrationEpisode] = {}
    quality_rank = {"HIGH": 3, "MEDIUM": 2, "LOW": 1}
    for ep in episodes:
        if ep.scope.upper() != ALLOWED_SCOPE or ep.dataset_scope.upper() != ALLOWED_SCOPE:
            # PRIMARY/AUDIT/DEBUG/RADAR are never mixed.
            continue
        if ep.horizon_days not in HORIZONS:
            continue
        available_at = _available_at(ep)
        if available_at is None or available_at <= ep.asof_date:
            continue
        key = (ep.ticker.upper(), ep.asof_date, ep.horizon_days)
        prior = chosen.get(key)
        if prior is None:
            chosen[key] = ep
            continue
        prior_rank = quality_rank.get(prior.quality.upper(), 0)
        new_rank = quality_rank.get(ep.quality.upper(), 0)
        if new_rank > prior_rank:
            chosen[key] = ep
        elif new_rank == prior_rank and ep != prior:
            # Same ticker/day/horizon and same quality but conflicting evidence is
            # not silently averaged. Exclude it by marking the key conflicted.
            chosen[key] = CalibrationEpisode(
                ticker=ep.ticker,
                asof_date=ep.asof_date,
                horizon_days=ep.horizon_days,
                score=ep.score,
                future_return=ep.future_return,
                regime=ep.regime,
                asset_group=ep.asset_group,
                quality="INVALID",
                scope=ep.scope,
                outcome_date=ep.outcome_date,
                outcome_available_at=ep.outcome_available_at,
                quality_flags=("CONFLICTING_DUPLICATE",),
                dataset_scope=ep.dataset_scope,
            )
    return sorted(
        (ep for ep in chosen.values() if ep.quality.upper() != "INVALID"),
        key=lambda e: (e.asof_date, e.ticker, e.horizon_days),
    )


def fit_walk_forward_binning(
    episodes: Iterable[CalibrationEpisode],
    *,
    train_end: date,
    horizon_days: int,
    bins: int = 5,
) -> WalkForwardCalibration:
    train = [
        ep for ep in _dedupe_primary(episodes)
        if ep.horizon_days == horizon_days and (_available_at(ep) or date.max) <= train_end
    ]
    if len(train) < MIN_SAMPLE:
        return WalkForwardCalibration(train_end=train_end)
    ordered = sorted(train, key=lambda e: e.score)
    size = max(1, len(ordered) // bins)
    rows = []
    for start in range(0, len(ordered), size):
        chunk = ordered[start:start + size]
        if len(chunk) < 3:
            continue
        rows.append((
            chunk[0].score,
            chunk[-1].score,
            mean(e.future_return for e in chunk),
            len(chunk),
        ))
    return WalkForwardCalibration(train_end=train_end, score_bins=rows)


def _quality_fraction(episodes: list[CalibrationEpisode]) -> float:
    if not episodes:
        return 0.0
    weights = {"HIGH": 1.0, "MEDIUM": 0.65, "LOW": 0.25}
    return mean(weights.get(ep.quality.upper(), 0.0) for ep in episodes)


def _horizon_consistency(
    episodes: Iterable[CalibrationEpisode],
    *,
    asof_date: date,
    score: float,
    regime: str,
    asset_group: str,
) -> float:
    """Agreement of historical conditional-return sign across 5/10/20/40D."""
    bucket = _score_bucket(score)
    signs: list[int] = []
    base = _dedupe_primary(episodes)
    for horizon in HORIZONS:
        rows = [
            ep for ep in base
            if ep.horizon_days == horizon
            and (_available_at(ep) or date.max) <= asof_date
            and _score_bucket(ep.score) == bucket
            and (regime == "UNKNOWN" or ep.regime == regime)
            and (asset_group == "UNKNOWN" or ep.asset_group == asset_group)
        ]
        if len(rows) < 8:
            continue
        avg = mean(ep.future_return for ep in rows)
        if abs(avg) > 1e-9:
            signs.append(1 if avg > 0 else -1)
    if len(signs) < 2:
        return 0.50
    return max(signs.count(1), signs.count(-1)) / len(signs)


def calibrated_view(
    episodes: Iterable[CalibrationEpisode],
    *,
    asof_date: date,
    horizon_days: int,
    score: float,
    regime: str = "UNKNOWN",
    asset_group: str = "UNKNOWN",
) -> CalibratedView:
    all_primary = _dedupe_primary(episodes)
    eligible = [
        ep for ep in all_primary
        if ep.horizon_days == horizon_days
        and (_available_at(ep) or date.max) <= asof_date
        and (regime == "UNKNOWN" or ep.regime == regime)
        and (asset_group == "UNKNOWN" or ep.asset_group == asset_group)
    ]
    n = len(eligible)
    bucket = _score_bucket(score)
    quality_fraction = _quality_fraction(eligible)
    consistency = _horizon_consistency(
        all_primary,
        asof_date=asof_date,
        score=score,
        regime=regime,
        asset_group=asset_group,
    )
    if n < MIN_SAMPLE:
        return CalibratedView(
            None, 0.0, n, None, None, None, "LOW", True,
            "insufficient_oos_sample",
            dispersion=None,
            horizon_consistency=consistency,
            score_bucket=bucket,
            reconstruction_quality=quality_fraction,
        )

    model = fit_walk_forward_binning(
        eligible,
        train_end=asof_date,
        horizon_days=horizon_days,
    )
    predicted = model.predict(score)
    if predicted is None:
        return CalibratedView(
            None, 0.0, n, None, None, None, "LOW", True,
            "score_outside_calibrated_support",
            dispersion=None,
            horizon_consistency=consistency,
            score_bucket=bucket,
            reconstruction_quality=quality_fraction,
        )

    returns = [ep.future_return for ep in eligible]
    ic = _corr(_rank([ep.score for ep in eligible]), _rank(returns))
    sigma = pstdev(returns) if n > 1 else 0.0
    se = sigma / sqrt(n) if n else 0.0
    ci_low, ci_high = predicted - 1.96 * se, predicted + 1.96 * se

    # Confidence is evidence-derived, not abs(score): sample size, rank IC,
    # interval/dispersion, PIT reconstruction quality and cross-horizon stability.
    sample_strength = max(0.0, min(1.0, (n - MIN_SAMPLE) / 70.0))
    ic_strength = max(0.0, min(1.0, (ic or 0.0) / 0.10))
    relative_width = (ci_high - ci_low) / max(abs(predicted), 0.01)
    interval_strength = 1.0 / (1.0 + max(0.0, relative_width))
    dispersion_strength = 1.0 / (1.0 + sigma / 0.05)
    confidence = (
        0.30 * sample_strength
        + 0.25 * ic_strength
        + 0.15 * interval_strength
        + 0.10 * dispersion_strength
        + 0.10 * quality_fraction
        + 0.10 * consistency
    )
    confidence = max(0.0, min(0.95, confidence))
    quality = "HIGH" if confidence >= 0.70 else "MEDIUM" if confidence >= 0.40 else "LOW"
    return CalibratedView(
        predicted, confidence, n, ic, ci_low, ci_high, quality, False,
        "walk_forward_primary_pit_only",
        dispersion=sigma,
        horizon_consistency=consistency,
        score_bucket=bucket,
        reconstruction_quality=quality_fraction,
    )


def walk_forward_predictions(
    episodes: Iterable[CalibrationEpisode],
    *,
    horizon_days: int,
) -> list[WalkForwardPrediction]:
    """Strict chronological OOS predictions: a row never trains on its own outcome."""
    primary = [ep for ep in _dedupe_primary(episodes) if ep.horizon_days == horizon_days]
    predictions: list[WalkForwardPrediction] = []
    for test in primary:
        # `calibrated_view` only admits outcomes available by test.asof_date.
        view = calibrated_view(
            primary,
            asof_date=test.asof_date,
            horizon_days=horizon_days,
            score=test.score,
            regime=test.regime,
            asset_group=test.asset_group,
        )
        if view.fail_closed or view.expected_return is None:
            continue
        predictions.append(WalkForwardPrediction(
            ticker=test.ticker,
            asof_date=test.asof_date,
            horizon_days=horizon_days,
            score=test.score,
            predicted_return=view.expected_return,
            realized_return=test.future_return,
            confidence=view.confidence,
            n_train=view.n,
            train_end=test.asof_date,
        ))
    return predictions


def annualize_horizon_return(value: float, horizon_days: int) -> float:
    if horizon_days not in HORIZONS:
        raise ValueError(f"unsupported horizon: {horizon_days}")
    if value <= -1.0:
        raise ValueError("return <= -100% cannot be annualized")
    return (1.0 + value) ** (252.0 / horizon_days) - 1.0


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

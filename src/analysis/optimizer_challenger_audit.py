"""Champion/challenger audit comparison. SHADOW_ONLY; no execution authority."""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from math import sqrt
from statistics import mean, pstdev
from typing import Iterable

from .optimizer_calibration_v2 import OptimizerRunContext, assert_same_run_context


@dataclass(frozen=True)
class StrategyObservation:
    ticker: str
    horizon_days: int
    regime: str
    asset_group: str
    score_bucket: str
    expected_return: float
    realized_return: float
    turnover: float
    fees: float
    drawdown: float
    concentration: float
    tracking_error: float


@dataclass(frozen=True)
class ComparisonMetrics:
    n: int
    calibration_mae: float | None
    realized_return: float | None
    ev_net: float | None
    turnover: float | None
    fees: float | None
    max_drawdown: float | None
    realized_sharpe: float | None
    concentration: float | None
    tracking_error: float | None


@dataclass(frozen=True)
class PromotionAssessment:
    eligible: bool
    reasons: tuple[str, ...]
    n_oos: int
    regimes: int
    tickers: int
    top_ticker_share: float | None


def summarize(rows: Iterable[StrategyObservation]) -> ComparisonMetrics:
    rows = list(rows)
    if not rows:
        return ComparisonMetrics(0, None, None, None, None, None, None, None, None, None)
    realized = [r.realized_return for r in rows]
    net = [r.realized_return - r.fees for r in rows]
    horizons = {r.horizon_days for r in rows}
    sigma = pstdev(net) if len(net) > 1 else 0.0
    # Do not pretend mixed 5/10/20/40D episode returns are daily returns.
    sharpe = None
    if sigma > 1e-12 and len(horizons) == 1:
        horizon = next(iter(horizons))
        sharpe = mean(net) / sigma * sqrt(252.0 / horizon)
    return ComparisonMetrics(
        len(rows),
        mean(abs(r.expected_return - r.realized_return) for r in rows),
        mean(realized),
        mean(net),
        mean(r.turnover for r in rows),
        mean(r.fees for r in rows),
        min(r.drawdown for r in rows),
        sharpe,
        mean(r.concentration for r in rows),
        mean(r.tracking_error for r in rows),
    )


def _key(row: StrategyObservation) -> tuple:
    return (
        row.horizon_days,
        row.ticker.upper(),
        row.regime,
        row.asset_group,
        row.score_bucket,
    )


def _assert_same_observation_cohort(
    champion: list[StrategyObservation],
    challenger: list[StrategyObservation],
) -> None:
    champion_keys = Counter(_key(row) for row in champion)
    challenger_keys = Counter(_key(row) for row in challenger)
    if champion_keys != challenger_keys:
        missing = champion_keys - challenger_keys
        extra = challenger_keys - champion_keys
        raise ValueError(
            "Champion/challenger observation cohort mismatch: "
            f"missing={dict(missing)} extra={dict(extra)}"
        )


def compare_same_snapshot(
    champion_context: OptimizerRunContext,
    challenger_context: OptimizerRunContext,
    champion_rows: Iterable[StrategyObservation],
    challenger_rows: Iterable[StrategyObservation],
) -> dict:
    assert_same_run_context(champion_context, challenger_context)
    champion = list(champion_rows)
    challenger = list(challenger_rows)
    _assert_same_observation_cohort(champion, challenger)

    def grouped(rows):
        out = {}
        for row in rows:
            out.setdefault(_key(row), []).append(row)
        return {key: summarize(vals) for key, vals in out.items()}

    horizons = sorted({row.horizon_days for row in champion})
    by_horizon = {
        horizon: {
            "champion": summarize(row for row in champion if row.horizon_days == horizon),
            "challenger": summarize(row for row in challenger if row.horizon_days == horizon),
        }
        for horizon in horizons
    }
    return {
        "mode": "SHADOW_ONLY",
        "capital_authority": False,
        "same_context": True,
        "same_observation_cohort": True,
        "champion": summarize(champion),
        "challenger": summarize(challenger),
        "by_horizon": by_horizon,
        "segments": {
            "champion": grouped(champion),
            "challenger": grouped(challenger),
        },
    }


def assess_promotion(
    *,
    champion_rows: Iterable[StrategyObservation],
    challenger_rows: Iterable[StrategyObservation],
    temporal_stability: bool,
    minimum_oos: int = 100,
    max_drawdown_deterioration: float = 0.02,
    max_turnover_ratio: float = 1.25,
    max_top_ticker_share: float = 0.35,
) -> PromotionAssessment:
    """Conservative evidence gate. It never changes CAPITAL_AUTHORITY."""
    champion = list(champion_rows)
    challenger = list(challenger_rows)
    _assert_same_observation_cohort(champion, challenger)
    reasons: list[str] = []
    n = len(challenger)
    regimes = len({r.regime for r in challenger})
    tickers = len({r.ticker.upper() for r in challenger})
    counts = Counter(r.ticker.upper() for r in challenger)
    top_share = (max(counts.values()) / n) if n else None

    if n < minimum_oos:
        reasons.append(f"INSUFFICIENT_OOS_SAMPLE:{n}<{minimum_oos}")
    if regimes < 2:
        reasons.append("INSUFFICIENT_REGIME_COVERAGE")
    if tickers < 3:
        reasons.append("INSUFFICIENT_TICKER_COVERAGE")
    if top_share is None or top_share > max_top_ticker_share:
        reasons.append("SINGLE_TICKER_DEPENDENCY")
    if not temporal_stability:
        reasons.append("TEMPORAL_STABILITY_NOT_PROVEN")

    c = summarize(champion)
    q = summarize(challenger)
    if c.ev_net is None or q.ev_net is None or q.ev_net <= c.ev_net:
        reasons.append("NET_EV_NOT_SUPERIOR")
    if (
        c.max_drawdown is not None and q.max_drawdown is not None
        and q.max_drawdown < c.max_drawdown - max_drawdown_deterioration
    ):
        reasons.append("MATERIAL_DRAWDOWN_DETERIORATION")
    if (
        c.turnover is not None and q.turnover is not None
        and c.turnover > 0 and q.turnover > c.turnover * max_turnover_ratio
    ):
        reasons.append("TURNOVER_COST_TOO_HIGH")
    # No win-rate criterion by design.
    return PromotionAssessment(
        eligible=not reasons,
        reasons=tuple(dict.fromkeys(reasons)),
        n_oos=n,
        regimes=regimes,
        tickers=tickers,
        top_ticker_share=top_share,
    )

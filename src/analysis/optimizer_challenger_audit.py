"""Champion/challenger audit comparison. SHADOW_ONLY; no execution authority."""
from __future__ import annotations
from dataclasses import dataclass
from math import sqrt
from statistics import mean, pstdev
from typing import Iterable, Mapping

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

def summarize(rows: Iterable[StrategyObservation]) -> ComparisonMetrics:
    rows=list(rows)
    if not rows:
        return ComparisonMetrics(0,None,None,None,None,None,None,None,None,None)
    realized=[r.realized_return for r in rows]
    net=[r.realized_return-r.fees for r in rows]
    sigma=pstdev(net) if len(net)>1 else 0.0
    sharpe=(mean(net)/sigma*sqrt(252)) if sigma>1e-12 else None
    return ComparisonMetrics(
        len(rows),
        mean(abs(r.expected_return-r.realized_return) for r in rows),
        mean(realized), mean(net), mean(r.turnover for r in rows),
        mean(r.fees for r in rows), min(r.drawdown for r in rows),
        sharpe, mean(r.concentration for r in rows),
        mean(r.tracking_error for r in rows),
    )

def compare_same_snapshot(
    champion_context: OptimizerRunContext,
    challenger_context: OptimizerRunContext,
    champion_rows: Iterable[StrategyObservation],
    challenger_rows: Iterable[StrategyObservation],
) -> dict:
    assert_same_run_context(champion_context, challenger_context)
    champion=list(champion_rows); challenger=list(challenger_rows)
    def grouped(rows):
        out={}
        for r in rows:
            key=(r.horizon_days,r.ticker,r.regime,r.asset_group,r.score_bucket)
            out.setdefault(key,[]).append(r)
        return {key:summarize(vals) for key,vals in out.items()}
    return {
        "mode":"SHADOW_ONLY",
        "capital_authority":False,
        "champion":summarize(champion),
        "challenger":summarize(challenger),
        "segments":{"champion":grouped(champion),"challenger":grouped(challenger)},
    }

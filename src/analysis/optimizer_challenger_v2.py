"""Calibrated Optimizer v2 portfolio challenger.

This runner consumes the exact champion snapshot/context, replaces only the
score->expected-return/confidence mapping with PIT historical calibration, and
produces a theoretical shadow target. It has no execution/capital authority.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Iterable, Mapping, Any

import numpy as np

from . import optimizer_core as champion_core
from .optimizer_calibration_v2 import (
    CALIBRATION_VERSION,
    CAPITAL_AUTHORITY,
    MODE,
    CalibrationEpisode,
    OptimizerRunContext,
    annualize_horizon_return,
    assert_same_run_context,
    calibrated_view,
)
from .portfolio_budget import build_portfolio_budget, scale_optimizer_sleeve


@dataclass(frozen=True)
class ChallengerTarget:
    status: str
    reason: str
    context: OptimizerRunContext
    horizon_days: int
    weights: dict[str, float] = field(default_factory=dict)
    cash_weight: float = 0.0
    frozen_weight: float = 0.0
    expected_return_annual: dict[str, float] = field(default_factory=dict)
    view_confidence: dict[str, float] = field(default_factory=dict)
    calibration_n: dict[str, int] = field(default_factory=dict)
    calibration_quality: dict[str, str] = field(default_factory=dict)
    mode: str = MODE
    calibration_version: str = CALIBRATION_VERSION
    capital_authority: bool = CAPITAL_AUTHORITY


def _fail(context: OptimizerRunContext, horizon_days: int, reason: str) -> ChallengerTarget:
    return ChallengerTarget(
        status="FAIL_CLOSED",
        reason=reason,
        context=context,
        horizon_days=horizon_days,
    )


def run_calibrated_optimizer_v2(
    *,
    history: Iterable[CalibrationEpisode],
    asof_date: date,
    horizon_days: int,
    returns,
    current_positions: list[dict],
    portfolio_value_ars: float,
    cash_ars: float,
    synthesis_results: Iterable[Any],
    champion_context: OptimizerRunContext,
    challenger_context: OptimizerRunContext,
    asset_groups: Mapping[str, str] | None = None,
    gate_state: str = "NORMAL",
) -> ChallengerTarget:
    """Run a calibrated Black-Litterman target on the champion's exact context.

    The input ``returns`` must be the same common return panel used by champion.
    Any context mismatch, missing historical support, low sample, incomplete
    return panel, or optimizer failure returns FAIL_CLOSED and no weights.
    """
    assert_same_run_context(champion_context, challenger_context)
    context = challenger_context
    asset_groups = {str(k).upper(): str(v).upper() for k, v in (asset_groups or {}).items()}

    if abs(float(context.cash_ars) - float(cash_ars or 0.0)) > 0.01:
        return _fail(
            context,
            horizon_days,
            f"CASH_CONTEXT_MISMATCH:{float(cash_ars or 0.0):.2f}!={float(context.cash_ars):.2f}",
        )
    if getattr(returns, "empty", True):
        return _fail(context, horizon_days, "EMPTY_COMMON_RETURN_PANEL")
    universe = [str(t).upper() for t in list(returns.columns)]
    if not universe:
        return _fail(context, horizon_days, "EMPTY_UNIVERSE")

    by_ticker = {
        str(getattr(result, "ticker", "") or "").upper(): result
        for result in synthesis_results
    }
    if any(ticker not in by_ticker for ticker in universe):
        return _fail(context, horizon_days, "MISSING_CHAMPION_SCORE_FOR_UNIVERSE")

    budget = build_portfolio_budget(
        current_positions,
        optimized_tickers=universe,
        portfolio_value_ars=portfolio_value_ars,
        cash_reserved_ars=cash_ars,
    )
    if abs(float(context.frozen_weight) - budget.frozen_weight) > 2e-3:
        return _fail(
            context,
            horizon_days,
            f"FROZEN_WEIGHT_CONTEXT_MISMATCH:{budget.frozen_weight:.6f}!={context.frozen_weight:.6f}",
        )

    views: dict[str, float] = {}
    confidences: dict[str, float] = {}
    n_by_ticker: dict[str, int] = {}
    quality_by_ticker: dict[str, str] = {}
    score_map: dict[str, float] = {}
    conviction_map: dict[str, float] = {}

    history_rows = list(history)
    for ticker in universe:
        result = by_ticker[ticker]
        score = float(getattr(result, "final_score", 0.0) or 0.0)
        regime = str(getattr(result, "technical_regime", "UNKNOWN") or "UNKNOWN").upper()
        asset_group = asset_groups.get(ticker, "UNKNOWN")
        view = calibrated_view(
            history_rows,
            asof_date=asof_date,
            horizon_days=horizon_days,
            score=score,
            regime=regime,
            asset_group=asset_group,
        )
        if view.fail_closed or view.expected_return is None:
            return _fail(
                context,
                horizon_days,
                f"INSUFFICIENT_CALIBRATION:{ticker}:{view.reason}:n={view.n}",
            )
        try:
            annual = annualize_horizon_return(view.expected_return, horizon_days)
        except ValueError as exc:
            return _fail(context, horizon_days, f"INVALID_CALIBRATED_RETURN:{ticker}:{exc}")
        views[ticker] = float(annual)
        confidences[ticker] = float(view.confidence)
        n_by_ticker[ticker] = int(view.n)
        quality_by_ticker[ticker] = view.quality
        score_map[ticker] = score
        conviction_map[ticker] = float(
            getattr(result, "conviction", getattr(result, "confidence", 0.5)) or 0.5
        )

    # Same score-driven portfolio constraints as champion; only expected-return
    # views and BL confidence are challenged.
    total = float(portfolio_value_ars or 0.0)
    current_weight = {
        str(position.get("ticker", "") or "").upper():
            max(0.0, float(position.get("market_value", 0.0) or 0.0)) / total
        for position in current_positions
        if total > 0 and str(position.get("ticker", "") or "").strip()
    }
    upper = np.array([
        champion_core._dynamic_w_max(
            score_map[ticker],
            current_weight.get(ticker, 0.0),
            gate_state,
            conviction_map[ticker],
        )
        for ticker in universe
    ], dtype=float)
    lower = np.full(len(universe), champion_core.W_MIN, dtype=float)
    if np.any(lower > upper) or float(lower.sum()) > 1.0 + 1e-9:
        return _fail(context, horizon_days, "INFEASIBLE_CHAMPION_BOUNDS")

    try:
        import pandas as pd
        from pypfopt.black_litterman import BlackLittermanModel
        from pypfopt.efficient_frontier import EfficientFrontier

        panel = returns[universe]
        covariance = panel.cov() * 252
        bl = BlackLittermanModel(
            cov_matrix=covariance,
            pi="equal",
            absolute_views=views,
            omega="idzorek",
            view_confidences=[confidences[ticker] for ticker in universe],
            tau=champion_core.TAU,
            risk_aversion=champion_core.RISK_AVERSION,
        )
        bl_returns = bl.bl_returns().reindex(universe)
        bl_cov = bl.bl_cov().reindex(index=universe, columns=universe)

        cash_floor = max(0.0, 1.0 - float(upper.sum()))
        ef_returns = bl_returns.copy()
        ef_cov = bl_cov.copy()
        ef_lower = lower.copy()
        ef_upper = upper.copy()
        if cash_floor > 1e-9:
            cash_ticker = "__CASH__"
            ef_returns = pd.concat([
                ef_returns,
                pd.Series({cash_ticker: champion_core.RF_ANNUAL}, dtype=float),
            ])
            ef_cov = ef_cov.reindex(
                index=[*universe, cash_ticker],
                columns=[*universe, cash_ticker],
                fill_value=0.0,
            )
            ef_cov.loc[cash_ticker, cash_ticker] = 1e-10
            ef_lower = np.append(ef_lower, cash_floor)
            ef_upper = np.append(ef_upper, cash_floor)

        ef = EfficientFrontier(ef_returns, ef_cov, weight_bounds=(ef_lower, ef_upper))
        ef.max_sharpe(risk_free_rate=champion_core.RF_ANNUAL)
        raw = dict(zip(ef.tickers, np.asarray(ef.weights, dtype=float)))
        risky_raw = {ticker: max(0.0, float(raw.get(ticker, 0.0))) for ticker in universe}
        optimizer_cash_raw = max(0.0, float(raw.get("__CASH__", 0.0)))
    except Exception as exc:
        return _fail(context, horizon_days, f"CHALLENGER_OPTIMIZATION_ERROR:{type(exc).__name__}:{exc}")

    weights, optimizer_cash = scale_optimizer_sleeve(
        risky_raw,
        optimizer_cash_weight=optimizer_cash_raw,
        budget=budget.optimizable_budget,
    )
    budget.validate(
        optimized_weight=sum(weights.values()),
        optimizer_cash_weight=optimizer_cash,
    )
    return ChallengerTarget(
        status="OK_SHADOW",
        reason="CALIBRATED_BL_PRIMARY_PIT_IDZOREK",
        context=context,
        horizon_days=horizon_days,
        weights={ticker: round(value, 8) for ticker, value in weights.items()},
        cash_weight=round(budget.cash_reserved_weight + optimizer_cash, 8),
        frozen_weight=round(budget.frozen_weight, 8),
        expected_return_annual={ticker: round(value, 8) for ticker, value in views.items()},
        view_confidence={ticker: round(value, 6) for ticker, value in confidences.items()},
        calibration_n=n_by_ticker,
        calibration_quality=quality_by_ticker,
    )

"""Budget-safe facade over the current production optimizer champion.

``optimizer_core`` contains the implementation that existed on this branch before
this facade. The champion math and thresholds stay unchanged. This module only
repairs full-portfolio accounting after the optimizer sleeve is solved and keeps
theoretical targets intact for the execution planner.
"""
from __future__ import annotations

import logging
from typing import Optional

import numpy as np

from . import optimizer_core as _core
from .portfolio_budget import build_portfolio_budget, scale_optimizer_sleeve
from .versioning import OPTIMIZER_VERSION

logger = logging.getLogger(__name__)

for _name in dir(_core):
    if not _name.startswith("__"):
        globals().setdefault(_name, getattr(_core, _name))


def __getattr__(name):
    return getattr(_core, name)


# Tests and diagnostic tooling historically monkeypatch helpers on
# src.analysis.optimizer. Mirror those overrides into the frozen core before a run
# so this facade remains drop-in compatible.
_PATCHABLE_CORE_NAMES = (
    "_fetch_returns", "_select_method", "_optimize_black_litterman",
    "_optimize_max_sharpe_np", "_optimize_min_variance_np",
    "_optimize_risk_parity_np", "_dynamic_w_max", "_get_risk_gate_state",
    "_apply_risk_gate_to_trades", "_portfolio_stats",
)


def _sync_core_overrides() -> None:
    for name in _PATCHABLE_CORE_NAMES:
        if name in globals():
            setattr(_core, name, globals()[name])


def _current_weight_map(current_positions, portfolio_value_ars: float) -> dict[str, float]:
    total = float(portfolio_value_ars or 0.0)
    if total <= 0:
        return {}
    out = {}
    for position in current_positions or []:
        ticker = str(position.get("ticker", "") or "").upper().strip()
        if not ticker:
            continue
        out[ticker] = max(0.0, float(position.get("market_value", 0.0) or 0.0)) / total
    return out


def _theoretical_action(delta: float, current: float, threshold: float) -> str:
    if abs(delta) < threshold:
        return "MANTENER"
    if delta > 0:
        return "NUEVO" if current <= 1e-12 else "COMPRAR"
    return "REDUCIR" if delta > -0.10 else "VENDER"


def _rebuild_theoretical_trades(report, *, current_positions, portfolio_value_ars, threshold):
    current = _current_weight_map(current_positions, portfolio_value_ars)
    optimized = set(report.optimization.weights)
    trades = []
    for ticker in sorted(set(current) | optimized):
        w_cur = current.get(ticker, 0.0)
        # A candidate that disappeared from the return panel is frozen, not sold.
        w_target = float(report.optimization.weights.get(ticker, w_cur) or 0.0)
        delta = w_target - w_cur
        trade = _core.RebalanceTrade(
            ticker=ticker,
            weight_current=round(w_cur, 4),
            weight_optimal=round(w_target, 4),
            delta=round(delta, 4),
            action=_theoretical_action(delta, w_cur, threshold),
            amount_ars=round(abs(delta) * float(portfolio_value_ars or 0.0), 0),
        )
        trade.theoretical_weight_optimal = round(w_target, 6)
        trades.append(trade)
    report.trades = trades
    report.total_sells_ars = round(sum(t.amount_ars for t in trades if t.action in {"VENDER", "REDUCIR"}), 0)
    report.total_buys_ars = round(sum(t.amount_ars for t in trades if t.action in {"COMPRAR", "NUEVO"}), 0)
    report.net_cash_needed = round(
        report.total_buys_ars
        - (float(getattr(report, "cash_reserved_ars", 0.0) or 0.0) + report.total_sells_ars),
        0,
    )
    report.n_trades = sum(t.action != "MANTENER" for t in trades)


def _attach_sharpe_contract(report, *, history_frames, current_positions, portfolio_value_ars):
    opt = report.optimization
    # Never relabel a theoretical portfolio metric as executable.
    opt.executable_target_sharpe = None
    opt.current_portfolio_sharpe = None
    if float(getattr(opt, "frozen_weight", 0.0) or 0.0) > 2e-3:
        opt.theoretical_target_sharpe = None
        opt.sharpe_unavailable_reason = "FROZEN_ASSETS_OUTSIDE_COMMON_RETURN_PANEL"
        return
    tickers = list((opt.weights or {}).keys())
    if not tickers:
        opt.theoretical_target_sharpe = None
        opt.sharpe_unavailable_reason = "NO_OPTIMIZABLE_ASSETS"
        return
    returns = _core._fetch_returns(tickers, history_frames=history_frames)
    if returns.empty or set(returns.columns) != set(tickers):
        opt.theoretical_target_sharpe = None
        opt.sharpe_unavailable_reason = "INCOMPLETE_RETURN_PANEL"
        return
    returns = returns[tickers]
    mu = returns.mean().values * 252
    cov = returns.cov().values * 252
    target = np.asarray([opt.weights[t] for t in tickers], dtype=float)
    _, _, target_sharpe = _core._portfolio_stats(
        target, mu, cov, cash_weight=float(getattr(opt, "cash_weight", 0.0) or 0.0)
    )
    opt.theoretical_target_sharpe = round(float(target_sharpe), 3)
    current = _current_weight_map(current_positions, portfolio_value_ars)
    current_arr = np.asarray([current.get(t, 0.0) for t in tickers], dtype=float)
    _, _, current_sharpe = _core._portfolio_stats(
        current_arr, mu, cov,
        cash_weight=float(getattr(opt, "reserved_cash_weight", 0.0) or 0.0),
    )
    opt.current_portfolio_sharpe = round(float(current_sharpe), 3)
    opt.sharpe_unavailable_reason = ""


def run_optimizer(
    current_positions: list[dict],
    portfolio_value_ars: float,
    cash_ars: float,
    macro_regime: dict,
    vix: Optional[float],
    synthesis_results: list,
    market_assets: list[dict],
    threshold: float = _core.REBALANCE_THRESH,
    portfolio_drawdown: float = 0.0,
    history_frames: Optional[dict[str, object]] = None,
    write_diagnostics: bool = True,
):
    _sync_core_overrides()
    report = _core.run_optimizer(
        current_positions=current_positions,
        portfolio_value_ars=portfolio_value_ars,
        cash_ars=cash_ars,
        macro_regime=macro_regime,
        vix=vix,
        synthesis_results=synthesis_results,
        market_assets=market_assets,
        threshold=threshold,
        portfolio_drawdown=portfolio_drawdown,
        history_frames=history_frames,
        write_diagnostics=write_diagnostics,
    )
    if report is None:
        return None

    report.optimizer_version = OPTIMIZER_VERSION
    report.cash_reserved_ars = max(0.0, float(cash_ars or 0.0))
    if getattr(report, "risk_gate_state", "") == "BLOCKED" or not report.optimization.weights:
        return report

    # Critical contract: capital filtered before run_optimizer (e.g. NVS /
    # USESPECIE) is inferred from the full portfolio denominator. No price is
    # invented and no implicit liquidation is allowed.
    optimized_tickers = set(report.optimization.weights)
    budget = build_portfolio_budget(
        current_positions,
        optimized_tickers=optimized_tickers,
        portfolio_value_ars=portfolio_value_ars,
        cash_reserved_ars=cash_ars,
    )

    core_reserved = float(getattr(report.optimization, "reserved_cash_weight", 0.0) or 0.0)
    core_total_cash = float(getattr(report.optimization, "cash_weight", core_reserved) or 0.0)
    core_optimizer_cash = max(0.0, core_total_cash - core_reserved)
    scaled_weights, optimizer_cash = scale_optimizer_sleeve(
        report.optimization.weights,
        optimizer_cash_weight=core_optimizer_cash,
        budget=budget.optimizable_budget,
    )
    budget.validate(
        optimized_weight=sum(scaled_weights.values()),
        optimizer_cash_weight=optimizer_cash,
    )

    opt = report.optimization
    opt.weights = {ticker: round(weight, 8) for ticker, weight in scaled_weights.items()}
    opt.frozen_weight = round(budget.frozen_weight, 8)
    opt.reserved_cash_weight = round(budget.cash_reserved_weight, 8)
    opt.optimizable_budget = round(budget.optimizable_budget, 8)
    opt.cash_weight = round(budget.cash_reserved_weight + optimizer_cash, 8)
    report.frozen_weight = opt.frozen_weight
    report.reserved_cash_weight = opt.reserved_cash_weight
    report.optimizable_budget = opt.optimizable_budget
    report.implicit_frozen_value_ars = round(budget.implicit_frozen_value_ars, 2)
    report.excluded_candidate_value_ars = round(budget.excluded_candidate_value_ars, 2)
    report.budget_invariant_total = round(
        budget.frozen_weight + sum(scaled_weights.values())
        + budget.cash_reserved_weight + optimizer_cash,
        10,
    )

    _rebuild_theoretical_trades(
        report,
        current_positions=current_positions,
        portfolio_value_ars=portfolio_value_ars,
        threshold=threshold,
    )
    _attach_sharpe_contract(
        report,
        history_frames=history_frames,
        current_positions=current_positions,
        portfolio_value_ars=portfolio_value_ars,
    )
    logger.info(
        "Full portfolio budget: frozen=%.2f%% cash_reserved=%.2f%% optimizable=%.2f%% invariant=%.6f",
        budget.frozen_weight * 100,
        budget.cash_reserved_weight * 100,
        budget.optimizable_budget * 100,
        report.budget_invariant_total,
    )
    return report

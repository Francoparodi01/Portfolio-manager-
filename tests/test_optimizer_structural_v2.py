from datetime import date
from types import SimpleNamespace

import numpy as np
import pandas as pd

from src.analysis import optimizer
from src.analysis.execution_planner import (
    AssetSignal, DecisionIntent, PositionSnapshot, reconcile_funding,
    SignalClass, PortfolioIntent,
)
from src.analysis.enums import DecisionType
from src.analysis.upcoming_earnings import UpcomingEarningsEvent, deduplicate_earnings_events


def _result(ticker, score=0.2):
    return SimpleNamespace(ticker=ticker, final_score=score, conviction=0.8, confidence=0.8)


def test_non_evaluable_positions_stay_frozen_and_budget_is_not_renormalized(monkeypatch):
    idx = pd.date_range("2026-01-01", periods=80, freq="B")
    returns = pd.DataFrame({"AAA": np.linspace(-.01,.01,80), "BBB": np.linspace(.01,-.01,80)}, index=idx)
    monkeypatch.setattr(optimizer, "_fetch_returns", lambda *a, **k: returns)
    monkeypatch.setattr(optimizer, "_select_method", lambda *a, **k: ("MAX_SHARPE", "test"))
    monkeypatch.setattr(optimizer, "_optimize_max_sharpe_np", lambda *a, **k: np.array([0.5, 0.5]))
    positions = [
        {"ticker":"AAA","market_value":450.0},
        {"ticker":"BBB","market_value":450.0},
        {"ticker":"FROZEN","market_value":60.0},
    ]
    report = optimizer.run_optimizer(
        positions, 1000.0, 40.0, {"market":"neutral"}, 15.0,
        [_result("AAA"), _result("BBB")], [], threshold=0.01,
    )
    assert report is not None
    assert report.optimization.frozen_weight == 0.06
    assert report.optimization.reserved_cash_weight == 0.04
    assert report.optimization.optimizable_budget == 0.90
    assert abs(sum(report.optimization.weights.values()) - 0.90) < 1e-9
    frozen = next(t for t in report.trades if t.ticker == "FROZEN")
    assert frozen.weight_current == frozen.weight_optimal == 0.06
    assert abs(report.optimization.frozen_weight + sum(report.optimization.weights.values()) + report.optimization.cash_weight - 1.0) < 1e-9


def test_blocked_buy_keeps_executable_target_at_current_weight():
    d = DecisionIntent(
        ticker="GDX", action=DecisionType.BUY, reason_primary="increase",
        reason_secondary=None, current_weight=0.127, target_weight=0.331,
        delta_weight=0.204, score=0.195, conviction=0.8, theoretical_ars=204000,
        signal_class=SignalClass.POS_STRONG, portfolio_intent=PortfolioIntent.INCREASE,
    )
    plan = reconcile_funding(
        [d], {"GDX": PositionSnapshot("GDX", 10, 10000, 127000, 0.127)},
        cash_before=0, portfolio_value_ars=1_000_000, gate="NORMAL",
    )
    assert plan.buy_orders == []
    assert d.action == DecisionType.WATCH
    assert d.theoretical_target_weight == 0.331
    assert d.executable_target_weight == 0.127


def test_earnings_same_fiscal_period_dedupes_and_exposes_date_conflict():
    a = UpcomingEarningsEvent("Y:1","SEC:SNDK","SNDK",date(2026,10,29),"unknown","YAHOO",.8,"ANNOUNCED",fiscal_year=2026,fiscal_quarter=3)
    b = UpcomingEarningsEvent("N:1","SEC:SNDK","SNDK",date(2026,11,6),"after_close","NASDAQ",.9,"CONFIRMED",fiscal_year=2026,fiscal_quarter=3)
    out = deduplicate_earnings_events([a,b])
    assert len(out) == 1
    assert out[0].event_date == date(2026,11,6)
    assert out[0].date_conflict is True
    assert out[0].conflicting_dates == (date(2026,10,29), date(2026,11,6))

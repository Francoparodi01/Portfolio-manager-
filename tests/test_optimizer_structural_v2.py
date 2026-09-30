from datetime import date, timedelta
from types import SimpleNamespace

import numpy as np
import pandas as pd

from src.analysis import optimizer
from src.analysis.execution_planner import (
    AssetSignal, DecisionIntent, PositionSnapshot, reconcile_funding,
    SignalClass, PortfolioIntent,
)
from src.analysis.enums import DecisionType
from src.analysis.optimizer_calibration_v2 import (
    CalibrationEpisode, OptimizerRunContext, calibrated_view,
    assert_same_run_context, MODE, CAPITAL_AUTHORITY,
)
from src.analysis.upcoming_earnings import UpcomingEarningsEvent, deduplicate_earnings_events
from src.analysis.macro_profiles import shadow_macro_profile_for_ticker


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


def _episodes(n=35, scope="PRIMARY"):
    base=date(2026,1,1)
    return [
        CalibrationEpisode(
            ticker=f"T{i%5}", asof_date=base+timedelta(days=i),
            outcome_date=base+timedelta(days=i+6), horizon_days=5,
            score=-.3 + .6*(i/(n-1)), future_return=-.02 + .04*(i/(n-1)),
            regime="RANGE", asset_group="TEST", quality="HIGH", scope=scope,
        ) for i in range(n)
    ]


def test_calibration_is_point_in_time_and_low_sample_fails_closed():
    view = calibrated_view(_episodes(10), asof_date=date(2026,9,1), horizon_days=5, score=.1, regime="RANGE", asset_group="TEST")
    assert view.fail_closed and view.confidence == 0.0
    future = CalibrationEpisode("X",date(2026,8,30),5,.9,9.0,"RANGE","TEST","HIGH","PRIMARY",outcome_date=date(2026,9,10))
    view2 = calibrated_view(_episodes(35)+[future], asof_date=date(2026,9,1), horizon_days=5, score=.1, regime="RANGE", asset_group="TEST")
    assert view2.n == 35
    assert view2.expected_return is not None


def test_champion_and_challenger_must_share_exact_context():
    ctx=OptimizerRunContext("p1","m1","c1",1000,0.006,0.06)
    assert_same_run_context(ctx,ctx)
    other=OptimizerRunContext("p2","m1","c1",1000,0.006,0.06)
    try:
        assert_same_run_context(ctx,other)
    except ValueError:
        pass
    else:
        raise AssertionError("mismatched snapshots must fail closed")


def test_challenger_and_macro_profiles_have_no_capital_authority():
    assert MODE == "SHADOW_ONLY"
    assert CAPITAL_AUTHORITY is False
    p = shadow_macro_profile_for_ticker("GDX")
    assert p and p.name == "gold_miners" and p.mode == "SHADOW_ONLY"

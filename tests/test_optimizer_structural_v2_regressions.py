from datetime import date
from types import SimpleNamespace

import numpy as np
import pandas as pd

from src.analysis import optimizer
from src.analysis.execution_planner import (
    build_positions_from_snapshot,
    build_signals_from_synthesis,
    derive_decision_intents,
    reconcile_funding,
)
from src.analysis.enums import DecisionType
from src.analysis.upcoming_earnings import (
    UpcomingEarningsEvent,
    deduplicate_earnings_events,
    render_upcoming_earnings_report,
)


def _result(ticker: str, score: float = 0.2):
    return SimpleNamespace(
        ticker=ticker,
        final_score=score,
        conviction=0.8,
        confidence=0.8,
        layers=[],
        technical_regime="RANGE",
        trend_score=0.0,
        structural_break_confirmed=False,
        stop_triggered=False,
        overbought_momentum=False,
    )


def _returns():
    idx = pd.date_range("2026-01-01", periods=80, freq="B")
    return pd.DataFrame(
        {
            "AAA": np.linspace(-0.01, 0.01, 80),
            "BBB": np.linspace(0.01, -0.01, 80),
        },
        index=idx,
    )


def test_frozen_filtered_before_optimizer_is_inferred_from_full_denominator(monkeypatch):
    """Regression for NVS/USESPECIE: they are absent from optimizer_positions."""
    monkeypatch.setattr(optimizer, "_fetch_returns", lambda *a, **k: _returns())
    monkeypatch.setattr(optimizer, "_select_method", lambda *a, **k: ("MAX_SHARPE", "test"))
    monkeypatch.setattr(
        optimizer, "_optimize_max_sharpe_np",
        lambda *a, **k: np.array([0.5, 0.5]),
    )

    # Full portfolio = 1000. Only 900 reaches run_optimizer; 60 is frozen and
    # 40 is explicit cash. The optimizable targets must sum to 90%, not 96/100%.
    optimizer_positions = [
        {"ticker": "AAA", "market_value": 450.0},
        {"ticker": "BBB", "market_value": 450.0},
    ]
    report = optimizer.run_optimizer(
        optimizer_positions,
        1000.0,
        40.0,
        {"market": "neutral"},
        15.0,
        [_result("AAA"), _result("BBB")],
        [],
        threshold=0.01,
    )
    assert report is not None
    assert abs(report.optimization.frozen_weight - 0.06) < 1e-9
    assert abs(report.optimization.reserved_cash_weight - 0.04) < 1e-9
    assert abs(report.optimization.optimizable_budget - 0.90) < 1e-9
    assert abs(sum(report.optimization.weights.values()) - 0.90) < 1e-9
    assert abs(
        report.optimization.frozen_weight
        + sum(report.optimization.weights.values())
        + report.optimization.cash_weight
        - 1.0
    ) < 1e-9


def test_risk_gate_does_not_destroy_theoretical_target(monkeypatch):
    monkeypatch.setattr(optimizer, "_fetch_returns", lambda *a, **k: _returns())
    monkeypatch.setattr(optimizer, "_select_method", lambda *a, **k: ("MAX_SHARPE", "test"))
    monkeypatch.setattr(
        optimizer, "_optimize_max_sharpe_np",
        lambda *a, **k: np.array([0.7, 0.3]),
    )
    positions = [
        {"ticker": "AAA", "market_value": 400.0, "quantity": 4, "current_price": 100.0},
        {"ticker": "BBB", "market_value": 400.0, "quantity": 4, "current_price": 100.0},
    ]
    report = optimizer.run_optimizer(
        positions,
        1000.0,
        200.0,
        {"market": "risk_off"},
        30.0,
        [_result("AAA", 0.195), _result("BBB", 0.0)],
        [],
        threshold=0.01,
    )
    assert report is not None and report.risk_gate_state == "CAUTIOUS"
    aaa = next(t for t in report.trades if t.ticker == "AAA")
    assert aaa.weight_optimal > aaa.weight_current

    signals = build_signals_from_synthesis([_result("AAA", 0.195), _result("BBB", 0.0)])
    current = build_positions_from_snapshot(positions, 1000.0)
    decisions = derive_decision_intents(report, signals, current, 1000.0, report.risk_gate_state)
    plan = reconcile_funding(decisions, current, 200.0, 1000.0, report.risk_gate_state)
    decision = next(d for d in plan.decisions if d.ticker == "AAA")
    assert decision.action == DecisionType.BLOCKED
    assert decision.theoretical_target_weight > decision.current_weight
    assert decision.executable_target_weight == decision.current_weight


def test_sharpe_is_never_reused_as_executable(monkeypatch):
    monkeypatch.setattr(optimizer, "_fetch_returns", lambda *a, **k: _returns())
    monkeypatch.setattr(optimizer, "_select_method", lambda *a, **k: ("MAX_SHARPE", "test"))
    monkeypatch.setattr(
        optimizer, "_optimize_max_sharpe_np",
        lambda *a, **k: np.array([0.5, 0.5]),
    )
    report = optimizer.run_optimizer(
        [{"ticker": "AAA", "market_value": 450.0}, {"ticker": "BBB", "market_value": 450.0}],
        1000.0, 40.0, {"market": "neutral"}, 15.0,
        [_result("AAA"), _result("BBB")], [], threshold=0.01,
    )
    assert report is not None
    assert report.optimization.executable_target_sharpe is None
    # Frozen capital means a full target Sharpe cannot be honestly reconstructed.
    assert report.optimization.theoretical_target_sharpe is None


def test_earnings_without_fiscal_metadata_exposes_source_date_conflict():
    a = UpcomingEarningsEvent(
        "Y:1", "SEC:SNDK", "SNDK", date(2026, 10, 29), "unknown",
        "YAHOO", 0.8, "ANNOUNCED",
    )
    b = UpcomingEarningsEvent(
        "N:1", "SEC:SNDK", "SNDK", date(2026, 11, 6), "after_close",
        "NASDAQ", 0.9, "CONFIRMED",
    )
    events = deduplicate_earnings_events([a, b])
    assert len(events) == 1
    assert events[0].date_conflict is True
    rendered = render_upcoming_earnings_report(events, today=date(2026, 9, 30))
    assert "Conflicto de fecha" in rendered
    assert "29/10" in rendered and "06/11" in rendered

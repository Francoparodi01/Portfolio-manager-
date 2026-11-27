"""Regresiones de la revisión de /analisis_full del 26/11/2026."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import numpy as np
import pandas as pd

from scripts import run_analysis
from src.analysis import optimizer, optimizer_core
from src.analysis.audit_scope import ART_TZ
from src.analysis.enums import DecisionType
from src.analysis.execution_planner import (
    AssetSignal,
    PositionSnapshot,
    derive_decision_intents,
    reconcile_funding,
)
from src.analysis.unified_audit import bot_calibration

PORTFOLIO = 2_421_928.0
CASH = 1_325_193.0


def _signal(ticker: str, score: float) -> AssetSignal:
    return AssetSignal(ticker=ticker, score=score, conviction=0.5, technical=0.0, macro=0.0, sentiment=0.0)


def _plan_26_11(googl_score: float, googl_weight: float = 0.254):
    """GOOGL baja al target MIN_VARIANCE para financiar SH/PAMP, con gate CAUTIOUS."""
    report = SimpleNamespace(trades=[
        SimpleNamespace(ticker="GOOGL", weight_current=googl_weight, weight_optimal=0.088),
        SimpleNamespace(ticker="SH", weight_current=0.028, weight_optimal=0.130),
        SimpleNamespace(ticker="PAMP", weight_current=0.040, weight_optimal=0.086),
    ])
    signals = {
        "GOOGL": _signal("GOOGL", googl_score),
        "SH": _signal("SH", 0.176),
        "PAMP": _signal("PAMP", -0.048),
    }
    googl_qty = round(googl_weight * PORTFOLIO / 8_550)
    positions = {
        "GOOGL": PositionSnapshot("GOOGL", googl_qty, 8_550, googl_qty * 8_550, googl_weight),
        "SH": PositionSnapshot("SH", 10, 6_780, 67_800, 0.028),
        "PAMP": PositionSnapshot("PAMP", 20, 4_840, 96_800, 0.040),
    }
    decisions = derive_decision_intents(report, signals, positions, PORTFOLIO, gate="CAUTIOUS")
    plan = reconcile_funding(decisions, positions, CASH, PORTFOLIO, gate="CAUTIOUS")
    return plan, {d.ticker: d for d in plan.decisions}


def test_weak_negative_sell_is_cancelled_when_rotation_buys_are_blocked():
    plan, by_ticker = _plan_26_11(googl_score=-0.054)

    assert plan.sell_orders == []
    assert by_ticker["GOOGL"].action == DecisionType.WATCH
    assert by_ticker["GOOGL"].reason_primary == "Venta condicional cancelada"
    assert "SH" in by_ticker["GOOGL"].reason_secondary


def test_blocked_order_shows_why_it_is_blocked():
    plan, _ = _plan_26_11(googl_score=-0.054)
    reasons = {o.ticker: o.reason for o in plan.blocked_orders}

    assert reasons["SH"].startswith("Gate CAUTIOUS — compra bloqueada")
    assert "Delta objetivo" in reasons["SH"]


def test_operable_negative_sell_stands_alone_with_signal_origin():
    plan, _ = _plan_26_11(googl_score=-0.10)

    assert [o.ticker for o in plan.sell_orders] == ["GOOGL"]
    assert plan.sell_orders[0].origin == "SIGNAL"


def test_weak_negative_keeps_only_concentration_part():
    plan, by_ticker = _plan_26_11(googl_score=-0.054, googl_weight=0.32)

    assert [o.ticker for o in plan.sell_orders] == ["GOOGL"]
    order = plan.sell_orders[0]
    assert order.origin == "CONCENTRATION"
    # Solo baja el exceso sobre MAX_WEIGHT_CONC (25%), redondeado a nominales.
    assert abs(by_ticker["GOOGL"].target_weight - 0.25) < 0.01


def test_min_variance_caps_inverse_etf(monkeypatch):
    rng = np.random.default_rng(7)
    base = rng.normal(0, 0.01, 250)
    returns = pd.DataFrame({
        "AAA": base + rng.normal(0, 0.004, 250),
        "BBB": base + rng.normal(0, 0.004, 250),
        "SH": -base + rng.normal(0, 0.002, 250),
    })
    fakes = {
        "_fetch_returns": lambda tickers, history_frames=None: returns[tickers],
        "_select_method": lambda *_args: ("MIN_VARIANCE", "test"),
        "_get_risk_gate_state": lambda *_args: ("NORMAL", ""),
    }
    # La fachada copia sus overrides al core y no los deshace: parchear ambos
    # para que monkeypatch restaure los dos al terminar.
    for name, fake in fakes.items():
        monkeypatch.setattr(optimizer, name, fake)
        monkeypatch.setattr(optimizer_core, name, fake)

    result = lambda t: type("R", (), {"ticker": t, "final_score": 0.2, "confidence": 0.8})()
    report = optimizer.run_optimizer(
        current_positions=[
            {"ticker": "AAA", "market_value": 48},
            {"ticker": "BBB", "market_value": 48},
            {"ticker": "SH", "market_value": 4},
        ],
        portfolio_value_ars=100,
        cash_ars=0,
        macro_regime={"market": "neutral"},
        vix=20,
        synthesis_results=[result("AAA"), result("BBB"), result("SH")],
        market_assets=[],
        history_frames={t: pd.DataFrame({"Close": [1, 2]}) for t in ("AAA", "BBB", "SH")},
    )

    assert report is not None
    assert report.optimization.weights["SH"] <= 0.05 + 1e-9


def _calibration_frame() -> pd.DataFrame:
    rows = []
    start = datetime(2026, 6, 1, 15, tzinfo=timezone.utc)
    for i in range(40):
        strength = 0.05 + 0.01 * (i % 10)
        sell = i % 2 == 0
        rows.append({
            "id": i,
            "decided_at": start + timedelta(days=i),
            "ticker": f"T{i % 5}",
            "decision": "SELL" if sell else "BUY",
            # Outcome direccional: más fuerza de señal, más acierto.
            "final_score": -strength if sell else strength,
            "outcome_5d": strength * 10 - 0.5,
            "outcome_10d": strength * 10 - 0.5,
            "outcome_20d": strength * 10 - 0.5,
            "source": "execution_plan",
            "status": "EXECUTED",
            "outcome_basis": "canonical_cocos",
        })
    return pd.DataFrame(rows)


def test_ic_aligns_sell_scores_before_correlating():
    calibration = bot_calibration(_calibration_frame(), ("5d",))

    # Sin alinear, los SELL acertados (score negativo, outcome positivo) darían IC negativo.
    assert calibration["5d"]["estimate"] > 0.9
    assert calibration["5d"]["n_obs"] == 40


def test_ic_regime_uses_the_95_range_not_the_point_value():
    crossing = {"5d": {"rank_ic": -0.27, "lower": -0.5, "upper": 0.1}}
    below = {"5d": {"rank_ic": -0.27, "lower": -0.5, "upper": -0.1}}

    assert run_analysis._build_ic_text_regime(crossing).label == "NO CONCLUYENTE"
    assert run_analysis._build_ic_text_regime(below).label == "CAUTELA ALTA"
    assert run_analysis._ic_evidence_label(below["5d"]) == "NEGATIVO"


def test_agreement_follows_score_direction():
    # GOOGL 26/11: T -0.017, M +0.041, S -0.052 con score negativo.
    agreement = run_analysis._calculate_agreement_from_layers(
        technical=-0.017, macro=0.041, sentiment=-0.052, direction=-1,
    )
    assert abs(agreement - 40 / 60) < 1e-9


def test_snapshot_after_close_is_fresh_until_next_open():
    snapshot = {"scraped_at": "2026-11-26T19:02:00-03:00"}

    assert run_analysis._portfolio_snapshot_stale_reason(
        snapshot, now=datetime(2026, 11, 26, 21, 20, tzinfo=ART_TZ)
    ) is None
    assert run_analysis._portfolio_snapshot_stale_reason(
        {"scraped_at": "2026-11-27T19:00:00-03:00"}, now=datetime(2026, 11, 28, 12, 0, tzinfo=ART_TZ)
    ) is None
    assert run_analysis._portfolio_snapshot_stale_reason(
        snapshot, now=datetime(2026, 11, 27, 11, 0, tzinfo=ART_TZ)
    ) is not None
    assert run_analysis._portfolio_snapshot_stale_reason(
        {"scraped_at": "2026-11-26T12:00:00-03:00"}, now=datetime(2026, 11, 26, 14, 0, tzinfo=ART_TZ)
    ) is not None

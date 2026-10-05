from datetime import datetime, time, timezone
from pathlib import Path

import pytest

from scripts.bot_stats_dashboard import metrics as dashboard_metrics
from src.analysis.raw_bot_signal_metrics import compute, sessions_after


def test_dashboard_exports_the_shared_raw_signal_calculator():
    assert dashboard_metrics.compute is compute


def test_shared_contract_uses_net_price_return_and_exact_sessions():
    decided_at = datetime(2026, 9, 1, 12, tzinfo=timezone.utc)
    sessions = sessions_after(decided_at.date(), 5)
    rows = [{
        "plan_id": "plan-1", "intent_id": 1, "created_at": decided_at,
        "source": "execution_plan", "feasible": True, "ticker": "TEST", "side": "BUY",
        "is_executable": True, "was_blocked": False,
    }]
    candles = []
    for index, session in enumerate(sessions):
        candles.append({
            "ticker": "TEST", "long_ticker": "TV:BYMA:TEST", "source": "TRADINGVIEW_BYMA",
            "currency": "ARS", "venue": "BYMA", "interval": "1d",
            "ts": datetime.combine(session, time(0), tzinfo=timezone.utc),
            "scraped_at": datetime.combine(session, time(22), tzinfo=timezone.utc),
            "open_price": 100.0 if index == 0 else 105.0,
            "close_price": 110.0 if index == len(sessions) - 1 else 105.0,
        })

    result = compute(
        rows, candles, as_of=datetime(2026, 9, 30, tzinfo=timezone.utc), cost_bps=75,
        events=(), events_available=True,
    )

    metric = result["metrics"]["5"]
    assert metric["n"] == 1
    assert metric["mean_pct"] == pytest.approx(9.25)
    assert metric["win_pct"] == 100.0
    assert result["signals"][0]["details"]["5"]["source"] == "TRADINGVIEW_BYMA"


def test_ledger_and_audit_expose_the_same_raw_signal_contract():
    root = Path(__file__).resolve().parents[1]
    ledger = (root / "src" / "analysis" / "decision_ledger.py").read_text(encoding="utf-8")
    audit = (root / "src" / "analysis" / "decision_market_audit.py").read_text(encoding="utf-8")
    counterfactual = (root / "src" / "analysis" / "bot_counterfactual.py").read_text(encoding="utf-8")

    assert "load_raw_bot_signal_stats" in ledger
    assert "RAW_FORMAL_SIGNAL_BYMA_V1" in ledger
    assert "load_raw_bot_signal_stats" in audit
    assert "Señales formales crudas normalizadas" in audit
    assert "raw_signal_reference" in counterfactual
    assert "comparison_note" in counterfactual

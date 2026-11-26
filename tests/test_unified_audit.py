from datetime import datetime, timezone

import pandas as pd

from src.analysis.unified_audit import (
    compute_unified_audit,
    dedupe_intentions,
    render_unified_audit,
)
from src.analysis.viability_audit import ViabilityAuditConfig
from src.core.telegram_format import validate_telegram_html
from tests.test_viability_audit import _bench_candles, _dated_rows


def _config(**kwargs):
    return ViabilityAuditConfig(
        database_url="postgresql://unused", horizons=("5d", "20d"), min_sample=30, cost_bps=75, **kwargs
    )


def _frame():
    rows = _dated_rows(source="execution_plan", status="EXECUTED", n_dates=40, signal=0.4, noise_seed=1)
    manual = _dated_rows(source="broker_movement", status="EXECUTED_MANUAL", n_dates=40, signal=0.0, noise_seed=2)
    for row in manual:
        row["outcome_5d"] -= 0.04
    plans = _dated_rows(source="execution_plan", status="APPROVED", n_dates=40, signal=0.4, noise_seed=3)
    radar = _dated_rows(source="radar", status="THEORETICAL", n_dates=40, signal=0.0, noise_seed=4)
    for i, row in enumerate(plans + radar):
        row["ticker"] = f"T{i % 7}"
    # Every plan is repeated by a second run the same day: same intention.
    plans += [{**row, "decided_at": row["decided_at"] + pd.Timedelta(hours=2)} for row in plans]
    frame = pd.DataFrame(rows + manual + plans + radar)
    frame.attrs["benchmark_candles"] = _bench_candles(daily=0.001)
    return frame


def _health(**extra):
    return {
        "last_market_price_at": datetime.now(tz=timezone.utc),
        "fills": 60,
        "fills_with_fees": 0,
        "follow_status": {"OVERFOLLOWED": 18},
        **extra,
    }


def test_unified_audit_uses_one_layer_for_every_section():
    report = compute_unified_audit(_frame(), _config(), health=_health())
    cohorts = {c.cohort: c for c in report.cohorts}
    assert cohorts["bot"].by_horizon["5d"].n == 80
    # Repeated plans of the same ticker + side + day count once.
    assert cohorts["plan"].rows == 80
    assert cohorts["manual"].by_horizon["5d"].net_ev < cohorts["bot"].by_horizon["5d"].net_ev
    # Plans beat manual trades by ~4 pp: the 95% range sits above zero.
    assert report.plan_vs_manual["lower"] > 0
    assert report.calibration["bot"]["5d"]["lower"] > 0
    # Same verdict object as /viability.
    assert report.viability.verdict_level in {"PASS", "INCONCLUSIVE", "FAIL", "NO_SAMPLE"}


def test_unified_audit_alerts_and_render():
    report = compute_unified_audit(_frame(), _config(), health=_health())
    alerts = "\n".join(report.alerts)
    assert "0 de 60 fills guardan comision" in alerts
    assert "18/18 atribuciones son OVER" in alerts
    assert "Reglas sin congelar" in alerts
    text = render_unified_audit(report)
    assert validate_telegram_html(text)[0] is True
    assert len(text) < 4096
    for section in ("1 · Veredicto", "2 · Cohortes", "3 · Bot vs humano", "4 · Calibración", "5 · Salud", "6 · Alertas"):
        assert section in text


def test_missing_benchmark_and_empty_frame_do_not_break_the_report():
    frame = _frame()
    frame.attrs["benchmark_candles"] = []
    report = compute_unified_audit(frame, _config(), health={})
    assert any("Sin velas de SPY" in a for a in report.alerts)
    empty = compute_unified_audit(pd.DataFrame(), _config(), health={})
    assert "Auditoría unificada" in render_unified_audit(empty)


def test_dedupe_keeps_latest_row_per_ticker_side_day():
    frame = pd.DataFrame(
        {
            "ticker": ["spcx", "SPCX", "SPCX", "GDX"],
            "decision": ["SELL", "SELL", "SELL", "SELL"],
            "decided_at": pd.to_datetime(
                ["2026-11-25 14:38", "2026-11-25 16:02", "2026-11-26 14:00", "2026-11-25 16:02"]
            ).tz_localize("America/Argentina/Buenos_Aires"),
            "id": [1, 2, 3, 4],
        }
    )
    assert sorted(dedupe_intentions(frame)["id"].tolist()) == [2, 3, 4]

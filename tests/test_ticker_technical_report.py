from datetime import datetime, timezone

import pandas as pd

from src.analysis.ticker_technical_report import (
    TickerDecisionContext,
    TickerPositionContext,
    TickerTechnicalReport,
    build_ticker_technical_report,
    normalize_ticker,
    render_ticker_technical_chart,
    render_ticker_technical_charts,
    render_ticker_telegram_report,
)
from src.analysis.technical import Signal
from src.core.telegram_format import validate_telegram_html


def _frame(rows: int = 220) -> pd.DataFrame:
    index = pd.date_range(
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        periods=rows,
        freq="D",
    )
    closes = [100 + i * 0.45 + ((i % 9) - 4) * 0.08 for i in range(rows)]
    frame = pd.DataFrame(
        {
            "Open": [c - 0.35 for c in closes],
            "High": [c + 1.25 for c in closes],
            "Low": [c - 1.10 for c in closes],
            "Close": closes,
            "Volume": [1000 + (i % 12) * 35 for i in range(rows)],
            "Source": ["COCOS"] * rows,
        },
        index=index,
    )
    frame.attrs["candle_sources"] = ("COCOS",)
    frame.attrs["candle_source_counts"] = {"COCOS": rows}
    frame.attrs["has_reconstructed_candles"] = False
    return frame


def _correction_frame() -> pd.DataFrame:
    rows = 260
    index = pd.date_range(
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        periods=rows,
        freq="D",
    )
    closes = []
    for i in range(rows):
        if i < 160:
            closes.append(100 + i * 0.45)
        elif i < 225:
            closes.append(172 + (i - 160) * 3.2)
        else:
            closes.append(380 - (i - 225) * 2.7)
    frame = pd.DataFrame(
        {
            "Open": [c + 0.5 for c in closes],
            "High": [c + 4.0 for c in closes],
            "Low": [c - 4.0 for c in closes],
            "Close": closes,
            "Volume": [1000 + (i % 8) * 60 for i in range(rows)],
            "Source": ["COCOS"] * rows,
        },
        index=index,
    )
    frame.attrs["candle_sources"] = ("COCOS",)
    frame.attrs["candle_source_counts"] = {"COCOS": rows}
    frame.attrs["has_reconstructed_candles"] = False
    return frame


def test_ticker_report_renders_context_and_valid_telegram_html():
    report = build_ticker_technical_report(
        "nvda",
        _frame(),
        position=TickerPositionContext(
            quantity=3,
            current_price=199.0,
            market_value_ars=597_000,
            portfolio_weight=0.12,
            snapshot_at="2026-07-27T15:00:00Z",
        ),
        latest_decision=TickerDecisionContext(
            decision_id=42,
            decided_at="2026-07-25T15:00:00Z",
            decision="BUY",
            status="EXECUTED",
            final_score=0.23,
            source="execution_plan",
        ),
    )

    text = render_ticker_telegram_report(report)

    assert "NVDA - " in text
    assert "Señal operativa" in text
    assert "Intensidad" in text
    assert "Tesis" in text
    assert "Qué cambiaría la visión" in text
    assert "Riesgos actuales" in text
    assert "Datos del análisis" in text
    assert "Contexto cartera" in text
    assert "Última decisión registrada" in text
    assert "Modo: <b>read-only</b>" in text
    assert "Confianza argumentada" in text
    assert not text.rstrip().endswith("no cambia thresholds.</i>")
    valid, errors = validate_telegram_html(text)
    assert valid, errors


def test_ticker_report_renders_hold_without_position_as_wait():
    frame = _correction_frame()
    report = TickerTechnicalReport(
        ticker="MU",
        signal=Signal(
            ticker="MU",
            signal="HOLD",
            strength=0.94,
            score_raw=-0.70,
            reasons=["EMA12 < EMA26", "MACD bajista"],
            price_usd=float(frame["Close"].iloc[-1]),
            technical_regime="RANGE",
            trend_score=0.40,
        ),
        frame=frame,
        data_source="market_candles",
        asset_type="CEDEAR",
    )

    text = render_ticker_telegram_report(report)

    assert "MU - esperar" in text
    assert "no abrir posición" in text
    assert "Señal operativa: <b>SIN ENTRADA</b>" in text
    assert "Intensidad: <b>94%</b> <i>(no es probabilidad)</i>" in text
    assert "Soporte inmediato" in text
    assert "Soporte crítico" in text
    assert "Invalidación: cierre sostenido debajo" in text
    assert "<b>Tesis</b>" in text
    assert "Momentum corto plazo deteriorado" in text
    assert "Recuperar $" in text
    assert "▼ " in text
    assert "Resultado: <b>No abrir posición</b>." in text
    assert "Soporte inmediato: <b>$" in text
    assert "–" in text
    assert "Razones tecnicas" not in text
    assert "CEDEAR/precio local" in text
    valid, errors = validate_telegram_html(text)
    assert valid, errors


def test_ticker_report_chart_writes_png(tmp_path):
    report = build_ticker_technical_report("TSM", _frame())
    chart_path = render_ticker_technical_chart(report, tmp_path / "tsm.png")

    assert chart_path.exists()
    assert chart_path.read_bytes().startswith(b"\x89PNG")


def test_ticker_report_charts_write_price_and_momentum_pngs(tmp_path):
    report = build_ticker_technical_report("TSM", _frame(rows=260))
    chart_paths = render_ticker_technical_charts(report, tmp_path / "tsm.png")

    assert len(chart_paths) == 2
    assert chart_paths[0].name == "tsm.png"
    assert chart_paths[1].name == "tsm_momentum.png"
    for chart_path in chart_paths:
        assert chart_path.exists()
        assert chart_path.read_bytes().startswith(b"\x89PNG")


def test_normalize_ticker_rejects_empty_input():
    try:
        normalize_ticker("   ")
    except ValueError as exc:
        assert "ticker vacio" in str(exc)
    else:
        raise AssertionError("expected ValueError")

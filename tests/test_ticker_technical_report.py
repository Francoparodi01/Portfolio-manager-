from datetime import datetime, timezone

import pandas as pd

from src.analysis.ticker_technical_report import (
    TickerDecisionContext,
    TickerPositionContext,
    build_ticker_technical_report,
    normalize_ticker,
    render_ticker_technical_chart,
    render_ticker_telegram_report,
)
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

    assert "Analisis por accion: NVDA" in text
    assert "Contexto cartera" in text
    assert "Ultima decision registrada" in text
    assert "Read-only" in text
    valid, errors = validate_telegram_html(text)
    assert valid, errors


def test_ticker_report_chart_writes_png(tmp_path):
    report = build_ticker_technical_report("TSM", _frame())
    chart_path = render_ticker_technical_chart(report, tmp_path / "tsm.png")

    assert chart_path.exists()
    assert chart_path.read_bytes().startswith(b"\x89PNG")


def test_normalize_ticker_rejects_empty_input():
    try:
        normalize_ticker("   ")
    except ValueError as exc:
        assert "ticker vacio" in str(exc)
    else:
        raise AssertionError("expected ValueError")

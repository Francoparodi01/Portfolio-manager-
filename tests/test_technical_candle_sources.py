from datetime import datetime, timedelta, timezone

from src.analysis.technical import analyze_ticker_from_frame
from src.collector.cocos_history import candles_to_frame


def _rows(source: str):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return [
        {
            "ts": start + timedelta(days=i),
            "open_price": 100 + i,
            "high_price": 101 + i,
            "low_price": 99 + i,
            "close_price": 100.5 + i,
            "volume": 1000 + i,
            "source": source,
        }
        for i in range(80)
    ]


def test_technical_signal_marks_reconstructed_history():
    frame = candles_to_frame(_rows("internal_snapshot"))

    signal = analyze_ticker_from_frame("T", frame)

    assert signal is not None
    assert signal.candle_source_mode == "reconstructed"
    assert signal.has_reconstructed_candles is True

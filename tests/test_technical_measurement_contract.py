"""Targeted numerical regression: technical indicators and Radar ordering.

Synthetic OHLCV only. This suite verifies computation contracts, not
profitability or correctness of live market data.
"""
from __future__ import annotations

from dataclasses import replace
import math

import pandas as pd
import pytest

from src.analysis.technical import (
    _rsi,
    compute_indicators,
    generate_signals,
)
from src.analysis.opportunity_screener import (
    AsymmetryMetrics,
    CandidateStatus,
    OpportunityCandidate,
    TradeType,
    opportunity_rank_score,
)


def frame(rows: int = 220, *, missing_volume: bool = False) -> pd.DataFrame:
    index = pd.date_range("2025-01-02", periods=rows, freq="B", tz="UTC")
    closes = [100.0 + index * 0.4 for index in range(rows)]
    volumes = [1000.0] * rows
    if missing_volume:
        volumes[-1] = float("nan")
    return pd.DataFrame(
        {
            "Open": closes,
            "High": [price + 2.0 for price in closes],
            "Low": [price - 2.0 for price in closes],
            "Close": closes,
            "Volume": volumes,
        },
        index=index,
    )


def test_rsi_rising_window_is_100_not_missing():
    series = pd.Series([100.0 + n for n in range(60)])
    rsi = _rsi(series)
    assert math.isnan(rsi.iloc[13])
    assert rsi.iloc[14] == pytest.approx(100.0)
    assert rsi.iloc[-1] == pytest.approx(100.0)


def test_rsi_falling_window_is_zero():
    series = pd.Series([100.0 - 0.5 * n for n in range(60)])
    assert _rsi(series).iloc[-1] == pytest.approx(0.0)


def test_rsi_flat_window_is_neutral():
    series = pd.Series([100.0] * 60)
    assert _rsi(series).iloc[-1] == pytest.approx(50.0)


def test_rsi_mixed_window_keeps_existing_rolling_mean_formula():
    prices = [100.0 + n * 0.3 + (n % 5) * 2.0 for n in range(80)]
    changes = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    trailing = changes[-14:]
    avg_gain = sum(max(x, 0.0) for x in trailing) / 14
    avg_loss = sum(max(-x, 0.0) for x in trailing) / 14
    expected = 100 - 100 / (1 + avg_gain / avg_loss)
    assert _rsi(pd.Series(prices)).iloc[-1] == pytest.approx(expected)


def test_indicators_match_independent_ma_bollinger_macd_atr_checks():
    bars = frame()
    indicators = compute_indicators(bars, "TEST")
    assert indicators is not None
    close = bars["Close"]
    assert indicators.sma_20 == pytest.approx(float(close.iloc[-20:].mean()))
    assert indicators.sma_50 == pytest.approx(float(close.iloc[-50:].mean()))
    assert indicators.ema_12 == pytest.approx(float(close.ewm(span=12, adjust=False).mean().iloc[-1]))
    assert indicators.ema_26 == pytest.approx(float(close.ewm(span=26, adjust=False).mean().iloc[-1]))
    assert indicators.macd_line == pytest.approx(indicators.ema_12 - indicators.ema_26)
    assert indicators.macd_hist == pytest.approx(indicators.macd_line - indicators.macd_signal)
    assert indicators.bb_middle == pytest.approx(float(close.iloc[-20:].mean()))
    assert indicators.bb_upper == pytest.approx(indicators.bb_middle + 2 * float(close.iloc[-20:].std()))
    assert indicators.bb_lower == pytest.approx(indicators.bb_middle - 2 * float(close.iloc[-20:].std()))
    assert indicators.atr_14 == pytest.approx(4.0)
    assert indicators.vol_ratio == pytest.approx(1.0)
    assert indicators.rsi_14 == pytest.approx(100.0)


def test_rsi_extreme_reaches_signal_reversion_component():
    indicators = compute_indicators(frame(), "TEST")
    assert indicators is not None
    signal = generate_signals(indicators)
    assert signal.reversion_components["rsi"] == pytest.approx(-2.5)
    # Do not assert BUY/SELL: RSI is one contribution, not a standalone trade.
    assert signal.signal in {"BUY", "SELL", "HOLD"}


def test_missing_volume_never_becomes_zero_coverage_confirmation():
    indicators = compute_indicators(frame(missing_volume=True), "TEST")
    assert indicators is not None
    assert indicators.vol_ratio is None
    assert indicators.obv is None


def test_unavailable_last_bar_is_rejected_by_cutoff():
    bars = frame()
    bars.attrs["cutoff"] = bars.index[-2].to_pydatetime()
    assert compute_indicators(bars, "TEST") is None


def test_radar_ranking_does_not_use_prior_outcomes():
    item = OpportunityCandidate(
        ticker="TEST",
        status=CandidateStatus.VIGILANCIA_A,
        trade_type=TradeType.NEW_ENTRY,
        final_score=0.15,
        conviction=0.6,
        asymmetry=AsymmetryMetrics(
            ticker="TEST", asymmetry_ratio=2.0, risk_reward=2.0, rr_valid=True,
        ),
    )
    before = opportunity_rank_score(item)
    item.prior_radar_outcomes = {5: -0.9, 10: 0.8, 20: 0.1}
    assert opportunity_rank_score(item) == pytest.approx(before)
    expected = 3 * 10 + 0.15 * 0.6 * (1 + 2 / 3)
    assert before == pytest.approx(expected)

from types import SimpleNamespace

from src.analysis.trend_regime import TrendRegime, assess_trend, classify_regime


def _indicator(**overrides):
    values = {
        "adx_14": 30.0,
        "di_plus": 35.0,
        "di_minus": 15.0,
        "close": 120.0,
        "sma_20": 115.0,
        "sma_50": 110.0,
        "sma_200": 100.0,
        "ema_12": 114.0,
        "ema_26": 111.0,
        "macd_hist": 1.0,
        "rsi_14": 65.0,
        "stoch_k": 70.0,
        "williams_r": -30.0,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_classifies_requested_strong_uptrend_and_range_rules():
    assert classify_regime(_indicator()) == TrendRegime.STRONG_UPTREND
    assert classify_regime(_indicator(adx_14=18.0)) == TrendRegime.RANGE


def test_classifies_requested_downtrend_rule():
    indicator = _indicator(
        adx_14=31.0,
        di_plus=10.0,
        di_minus=32.0,
        close=90.0,
        sma_20=92.0,
        sma_50=96.0,
        sma_200=100.0,
        ema_12=91.0,
        ema_26=94.0,
        macd_hist=-1.0,
    )
    assert classify_regime(indicator) == TrendRegime.DOWNTREND
    assert assess_trend(indicator).trend_score < 0


def test_assessment_flags_overbought_and_confirmed_structural_break():
    assessment = assess_trend(
        _indicator(
            close=105.0,
            sma_50=110.0,
            ema_12=108.0,
            ema_26=111.0,
            macd_hist=-0.5,
            rsi_14=74.0,
        )
    )
    assert assessment.overbought_momentum is True
    assert assessment.structural_break_confirmed is True

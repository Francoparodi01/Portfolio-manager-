from src.analysis.technical import Signal, build_telegram_report
from types import SimpleNamespace


def test_build_telegram_report_renders_scheduler_payload():
    report = build_telegram_report(
        [
            Signal(
                ticker="T",
                signal="BUY",
                strength=0.8,
                score_raw=4.0,
                reasons=["momentum"],
                price_usd=120.0,
            )
        ],
        100000.0,
    )

    assert "Análisis técnico" in report
    assert "T" in report
    assert "100,000 ARS" in report


def test_report_includes_regime_macro_sentiment_and_divergence():
    report = build_telegram_report(
        [
            Signal(
                ticker="NVDA",
                signal="SELL",
                strength=0.8,
                score_raw=-4.0,
                reasons=["momentum"],
                price_usd=120.0,
                technical_regime="STRONG_UPTREND",
                trend_score=0.72,
            )
        ],
        100000.0,
        macro_snapshot=SimpleNamespace(
            sp500_chg=1.2,
            vix=17.0,
            wti=75.0,
            wti_chg=0.5,
        ),
        sentiment_events=[{"source": "Reuters", "summary": "Evento relevante"}],
    )

    assert "STRONG_UPTREND" in report
    assert "Trend shadow" in report
    assert "SP500 1.2%" in report
    assert "Evento relevante" in report
    assert "Divergencia técnico/macro" in report

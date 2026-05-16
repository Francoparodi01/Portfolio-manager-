from src.analysis.technical import Signal, build_telegram_report


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

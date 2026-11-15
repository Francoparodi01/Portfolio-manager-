from datetime import date

from src.analysis.technical_buy_shadow_v3_audit import (
    build_technical_buy_shadow_v3_audit,
)


def _row(day: str, ticker: str, regime: str, outcome: float, direction: str = "BUY") -> dict:
    return {
        "date": day,
        "ticker": ticker,
        "v2_direction": direction,
        "v2_score": 0.4 if direction == "BUY" else 0.0,
        "regime": regime,
        "trend_score": 0.7,
        "reversion_score": 0.1,
        "structural_break": False,
        "volume_quality_20": 1.0,
        "source_mode": "official",
        "return_20d": outcome,
        "quality_20d": True,
    }


def test_audit_uses_first_row_of_each_buy_episode_and_reports_net_ev():
    rows = [
        _row("2026-06-01", "AAA", "STRONG_UPTREND", 0.10),
        _row("2026-06-02", "AAA", "STRONG_UPTREND", -0.50),
        _row("2026-06-01", "BBB", "RANGE", -0.02),
        _row("2026-06-01", "CCC", "TRANSITIONAL", 0.01, direction="HOLD"),
        _row("2026-07-02", "AAA", "TRANSITIONAL", 0.01, direction="HOLD"),
        _row("2026-07-03", "AAA", "STRONG_UPTREND", 0.04),
    ]

    report = build_technical_buy_shadow_v3_audit(
        rows,
        cost_rate=0.01,
        split_date=date(2026, 7, 1),
    )
    metrics = {metric.sample: metric for metric in report.metrics}

    assert report.v2_buy_episodes == 3
    assert metrics["V3_PRIMARY_ALL"].n == 2
    assert metrics["V3_PRIMARY_ALL"].gross_ev == 0.07
    assert round(metrics["V3_PRIMARY_ALL"].net_ev or 0.0, 6) == 0.06
    assert metrics["V3_PRIMARY_BEFORE_SPLIT"].n == 1
    assert metrics["V3_PRIMARY_AFTER_SPLIT"].n == 1

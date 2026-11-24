import pandas as pd

from src.analysis.opportunity_screener import (
    OpportunityCandidate,
    OpportunityReport,
    TradeType,
    CandidateStatus,
    _build_entry_reasons,
    render_opportunity_report,
    screen_universe,
)


def _frame(*, close: float, volume: float) -> pd.DataFrame:
    closes = [close + (i * 8) + (120 if i % 2 == 0 else -120) for i in range(260)]
    return pd.DataFrame(
        {
            "Close": closes,
            "High": [value * 1.01 for value in closes],
            "Low": [value * 0.99 for value in closes],
            "Volume": [volume for _ in closes],
        }
    )


def test_cedear_uses_turnover_not_legacy_unit_volume_threshold():
    frame = _frame(close=20_000, volume=20_000)

    results = screen_universe(
        ["AAPL"],
        history_frames={"SPY": frame, "QQQ": frame, "AAPL": frame},
        asset_types={"AAPL": "CEDEAR"},
    )

    assert results[0].asset_type == "CEDEAR"
    assert results[0].avg_volume < 500_000
    assert results[0].avg_turnover_ars > 100_000_000
    assert results[0].passes_screen is True


def test_low_turnover_cedear_is_rejected():
    reference = _frame(close=20_000, volume=20_000)
    thin = _frame(close=20_000, volume=1_000)

    results = screen_universe(
        ["EA"],
        history_frames={"SPY": reference, "QQQ": reference, "EA": thin},
        asset_types={"EA": "CEDEAR"},
    )

    assert results[0].passes_screen is False
    assert "monto operado bajo" in results[0].fail_reason


def test_radar_header_makes_the_funnel_explicit():
    report = OpportunityReport(
        universe_size=34,
        screened_count=20,
        ranked_count=10,
        displayed_count=8,
    )

    output = render_opportunity_report(report)

    assert "34 tickers → 20 pasaron screener → 10 ideas rankeadas → top 8 mostradas" in output


def test_cedear_uses_qqq_when_spy_is_missing():
    qqq = _frame(close=20_000, volume=20_000)
    aapl = _frame(close=20_000, volume=20_000)

    results = screen_universe(
        ["AAPL"],
        history_frames={"QQQ": qqq, "AAPL": aapl},
        asset_types={"AAPL": "CEDEAR"},
    )

    assert results[0].rs_benchmark_ticker == "QQQ"


def test_accion_skips_global_relative_strength_without_local_benchmark():
    qqq = _frame(close=20_000, volume=20_000)
    ggal = _frame(close=6_000, volume=300_000)

    results = screen_universe(
        ["GGAL"],
        history_frames={"QQQ": qqq, "GGAL": ggal},
        asset_types={"GGAL": "ACCION"},
    )

    assert results[0].asset_type == "ACCION"
    assert results[0].rs_benchmark_ticker == ""


def test_entry_reasons_name_the_actual_relative_strength_benchmark():
    metric = screen_universe(
        ["AAPL"],
        history_frames={
            "QQQ": _frame(close=20_000, volume=20_000),
            "AAPL": _frame(close=20_000, volume=20_000),
        },
        asset_types={"AAPL": "CEDEAR"},
    )[0]
    metric.rs_vs_spy_20d = 0.08
    candidate = OpportunityCandidate(
        ticker="AAPL",
        status=CandidateStatus.VIGILANCIA_A,
        trade_type=TradeType.WATCHLIST,
        final_score=0.1,
        conviction=0.5,
    )

    reasons = _build_entry_reasons(candidate, metric)

    assert any("RS fuerte vs QQQ" in reason for reason in reasons)

import pytest
from datetime import datetime, timedelta

from scripts.run_analysis import ART_TZ, _portfolio_equity_total, _portfolio_snapshot_stale_reason
from src.analysis.risk import build_portfolio_risk_report


def test_run_analysis_portfolio_total_includes_cash_for_optimizer_denominator():
    positions = [
        {"ticker": "VST", "market_value": 152_250},
        {"ticker": "IBM", "market_value": 135_800},
    ]

    total = _portfolio_equity_total(
        positions,
        cash_ars=1_330_905,
        snapshot_total_ars=288_050,
    )

    assert total == 1_618_955


def test_risk_report_uses_equity_total_without_double_counting_cash():
    positions = [{"ticker": "MU", "market_value": 100.0}]

    report = build_portfolio_risk_report(
        positions=positions,
        prices_map={},
        total_ars=1_100.0,
        cash_ars=1_000.0,
        history=[],
    )

    assert report.total_value_ars == pytest.approx(1_100.0)
    assert report.cash_pct == pytest.approx(1_000.0 / 1_100.0)
    assert report.positions[0]["current_pct"] == round(100.0 / 1_100.0, 4)


def test_risk_report_keeps_legacy_invested_total_compatible():
    positions = [{"ticker": "MU", "market_value": 100.0}]

    report = build_portfolio_risk_report(
        positions=positions,
        prices_map={},
        total_ars=100.0,
        cash_ars=1_000.0,
        history=[],
    )

    assert report.total_value_ars == pytest.approx(1_100.0)
    assert report.cash_pct == pytest.approx(1_000.0 / 1_100.0)
    assert report.positions[0]["current_pct"] == round(100.0 / 1_100.0, 4)


def test_portfolio_snapshot_staleness_rejects_old_intraday_snapshot():
    now = datetime(2026, 6, 25, 17, 12, tzinfo=ART_TZ)
    snapshot = {"scraped_at": now - timedelta(minutes=75)}

    reason = _portfolio_snapshot_stale_reason(snapshot, now, max_age_minutes=60)

    assert reason is not None
    assert "antigüedad 75 min" in reason


def test_portfolio_snapshot_staleness_allows_recent_eod_snapshot():
    now = datetime(2026, 6, 25, 17, 12, tzinfo=ART_TZ)
    snapshot = {"scraped_at": now - timedelta(minutes=10)}

    assert _portfolio_snapshot_stale_reason(snapshot, now, max_age_minutes=60) is None

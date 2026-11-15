from datetime import datetime, timezone

from src.analysis.technical_shadow_audit import (
    build_technical_shadow_audit,
    directional_return,
    recover_raw_return,
)


def test_recovers_raw_return_from_buy_and_sell_parent_events():
    assert recover_raw_return(0.10, "BUY") == 0.10
    assert round(recover_raw_return(0.10, "SELL"), 6) == -0.090909


def test_applies_independent_baseline_and_shadow_directions():
    assert directional_return(0.10, 1.0) == 0.10
    assert round(directional_return(0.10, -1.0), 6) == -0.090909
    assert directional_return(0.10, 0.0) is None


def test_audit_deduplicates_ticker_day_and_compares_scores():
    rows = [
        {
            "id": 1,
            "decided_at": datetime(2026, 8, 1, 15, 0, tzinfo=timezone.utc),
            "ticker": "AMD",
            "decision": "BUY",
            "baseline_score_raw": -2.0,
            "shadow_score": 0.4,
            "outcome_5d": 0.10,
        },
        {
            "id": 2,
            "decided_at": datetime(2026, 8, 1, 18, 0, tzinfo=timezone.utc),
            "ticker": "AMD",
            "decision": "BUY",
            "baseline_score_raw": 2.0,
            "shadow_score": -0.4,
            "outcome_5d": -0.20,
        },
    ]

    report = build_technical_shadow_audit(rows)
    metric = report.metrics[0]

    assert report.rows_loaded == 2
    assert report.episodes == 1
    assert metric.baseline_n == 1
    assert metric.baseline_win_rate == 0.0
    assert metric.shadow_n == 1
    assert metric.shadow_win_rate == 1.0

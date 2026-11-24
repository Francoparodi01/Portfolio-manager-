from __future__ import annotations

from datetime import datetime, timezone

from src.analysis.thesis_shadow import (
    HorizonForecast,
    ShadowThesis,
    render_shadow_ticker_telegram_report,
)


def _forecast(horizon: int, expected: float, probability: float) -> HorizonForecast:
    return HorizonForecast(
        horizon_sessions=horizon,
        expected_return=expected,
        probability_up=probability,
        lower_return=expected - 0.10,
        upper_return=expected + 0.15,
        uncertainty=0.08,
        confidence=0.62,
        signal_strength="MODERATE",
    )


def test_shadow_ticker_telegram_report_renders_single_asset_detail():
    thesis = ShadowThesis(
        ticker="AMD",
        universe_role="CANDIDATE",
        as_of_ts=datetime(2026, 6, 26, 20, 0, tzinfo=timezone.utc),
        reference_price=81_825.0,
        thesis_action="ENTRY_WATCH",
        thesis_confidence=0.71,
        forecasts=(
            _forecast(5, 0.041, 0.67),
            _forecast(20, 0.231, 0.79),
            _forecast(40, 0.485, 0.81),
        ),
        input_sessions=180,
        feature_snapshot={
            "input_start_ts": "2025-10-01T00:00:00+00:00",
            "input_end_ts": "2026-06-26T20:00:00+00:00",
            "trend_windows": {
                "20": {"daily_log_slope": 0.002, "r_squared": 0.44},
                "60": {"daily_log_slope": 0.0015, "r_squared": 0.38},
            },
            "context_overlay": {
                "context_flags": ["sentiment positivo +0.20", "régimen neutral"],
                "macro_score": 0.05,
                "sentiment_score": 0.20,
                "sentiment_confidence": 0.42,
                "cash_pct": 0.12,
            },
        },
        rationale=(
            "20-session expected return +23.10%",
            "20-session probability up 79.0%",
            "shadow only; no sizing or order instruction",
        ),
    )

    rendered = render_shadow_ticker_telegram_report([thesis], "amd")

    assert "Shadow · AMD" in rendered
    assert "20r: retorno <code>+23.1%</code>" in rendered
    assert "sentiment positivo" in rendered
    assert "Price forecast y contexto son capas separadas" in rendered


def test_shadow_ticker_telegram_report_handles_missing_ticker():
    rendered = render_shadow_ticker_telegram_report([], "ZZZZ")

    assert "No encontré ese ticker" in rendered
    assert "ZZZZ" in rendered

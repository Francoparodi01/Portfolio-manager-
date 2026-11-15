from __future__ import annotations

import ast
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from math import exp
from pathlib import Path

from src.analysis.thesis_shadow import (
    MODEL_VERSION,
    ShadowContext,
    build_shadow_thesis,
    mature_forecast,
    partition_fresh_theses,
    render_shadow_telegram_report,
)


ROOT = Path(__file__).resolve().parents[1]


def _candles(*, daily_log_return: float, sessions: int = 140, noise=None):
    start = datetime(2025, 1, 2, tzinfo=timezone.utc)
    rows = []
    log_price = 4.5
    for index in range(sessions):
        perturbation = noise(index) if noise else 0.0
        log_price += daily_log_return + perturbation
        rows.append(
            {
                "ts": start + timedelta(days=index),
                "close_price": exp(log_price),
            }
        )
    return rows


def test_uptrend_and_downtrend_produce_directional_price_forecasts():
    up = build_shadow_thesis(
        "UP",
        _candles(daily_log_return=0.002),
        universe_role="CANDIDATE",
    )
    down = build_shadow_thesis(
        "DOWN",
        _candles(daily_log_return=-0.002),
        universe_role="POSITION",
    )

    assert up.model_version == MODEL_VERSION
    assert up.forecast_for(20).expected_return > 0.03
    assert up.forecast_for(20).probability_up > 0.58
    assert up.thesis_action == "ENTRY_WATCH"

    assert down.forecast_for(20).expected_return < -0.03
    assert down.forecast_for(20).probability_up < 0.42
    assert down.thesis_action == "EXIT_WATCH"


def test_alternating_noise_does_not_become_an_entry_signal():
    thesis = build_shadow_thesis(
        "FLAT",
        _candles(
            daily_log_return=0.0,
            noise=lambda index: 0.012 if index % 2 == 0 else -0.012,
        ),
        universe_role="CANDIDATE",
    )

    assert thesis.thesis_action == "ABSTAIN"
    assert 0.42 < thesis.forecast_for(20).probability_up < 0.58


def test_shadow_context_overlay_does_not_change_price_forecast():
    candles = _candles(daily_log_return=0.002)
    price_only = build_shadow_thesis(
        "MU",
        candles,
        universe_role="CANDIDATE",
    )
    contextual = build_shadow_thesis(
        "MU",
        candles,
        universe_role="CANDIDATE",
        context=ShadowContext(
            cash_pct=0.0,
            sentiment_score=-0.20,
            sentiment_confidence=0.50,
            macro_score=-0.25,
            macro_regime="risk_off",
        ),
    )

    assert contextual.forecast_for(20).expected_return == price_only.forecast_for(20).expected_return
    assert contextual.feature_snapshot["forecast_return_basis"] == "price_only"
    assert contextual.feature_snapshot["context_overlay"]["price_forecast_adjusted"] is False
    assert contextual.thesis_action == "CONTEXT_REVIEW"
    assert contextual.thesis_confidence < price_only.thesis_confidence


def test_position_concentration_context_requires_review_even_in_uptrend():
    thesis = build_shadow_thesis(
        "AMAT",
        _candles(daily_log_return=0.002),
        universe_role="POSITION",
        context=ShadowContext(current_weight=0.42, cash_pct=0.05),
    )

    assert thesis.thesis_action == "CONTEXT_REVIEW"
    flags = thesis.feature_snapshot["context_overlay"]["context_flags"]
    assert any("concentración alta" in flag for flag in flags)

    report = render_shadow_telegram_report([thesis])
    assert "REVISAR CONTEXTO" in report
    assert "CONTEXT_REVIEW" not in report


def test_outcome_matures_by_future_sessions_not_calendar_age():
    as_of = datetime(2026, 6, 19, tzinfo=timezone.utc)
    future = [
        {"ts": as_of + timedelta(days=offset), "close_price": price}
        for offset, price in ((3, 101.0), (4, 102.0), (5, 103.0), (6, 104.0), (7, 105.0))
    ]

    assert mature_forecast(
        as_of_ts=as_of,
        reference_price=100.0,
        horizon_sessions=5,
        expected_return=0.04,
        future_candles=future[:4],
    ) is None

    outcome = mature_forecast(
        as_of_ts=as_of,
        reference_price=100.0,
        horizon_sessions=5,
        expected_return=0.04,
        future_candles=future,
    )
    assert outcome is not None
    assert outcome.target_session_ts == future[-1]["ts"]
    assert outcome.realized_return == 0.05
    assert outcome.direction_correct is True


def test_stale_candle_series_are_excluded_from_the_run_ranking():
    current = build_shadow_thesis(
        "CURRENT",
        _candles(daily_log_return=0.001),
        universe_role="CANDIDATE",
    )
    stale = replace(current, ticker="STALE", as_of_ts=current.as_of_ts - timedelta(days=30))

    fresh, rejected = partition_fresh_theses([stale, current])

    assert [item.ticker for item in fresh] == ["CURRENT"]
    assert [item.ticker for item in rejected] == ["STALE"]


def test_telegram_report_is_readable_and_explicitly_non_executable():
    position = build_shadow_thesis(
        "AMD",
        _candles(daily_log_return=0.0015),
        universe_role="POSITION",
    )
    candidate = build_shadow_thesis(
        "MU",
        _candles(daily_log_return=0.002),
        universe_role="CANDIDATE",
    )

    report = render_shadow_telegram_report([position, candidate])

    assert "<b>Tesis shadow 5/20/40</b>" in report
    assert "no ejecuta compras, ventas ni rebalanceos" in report
    assert "<b>Tu cartera</b>" in report
    assert "<b>Candidatos en vigilancia</b>" in report
    assert "Probabilidad positiva a 20r" in report
    assert "todavía no hay pronósticos maduros" in report
    assert "Audit:" not in report
    assert len(report) < 3900


def test_shadow_module_has_no_production_decision_dependencies():
    path = ROOT / "src" / "analysis" / "thesis_shadow.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imports = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }

    assert all("optimizer" not in name for name in imports)
    assert all("execution_planner" not in name for name in imports)
    assert all("synthesis" not in name for name in imports)


def test_shadow_schema_is_separate_and_horizon_versioned():
    sql = (ROOT / "init.sql").read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS shadow_thesis_runs" in sql
    assert "CREATE TABLE IF NOT EXISTS shadow_thesis_forecasts" in sql
    assert "CREATE TABLE IF NOT EXISTS shadow_thesis_outcomes" in sql
    assert "horizon_sessions IN (5, 20, 40)" in sql
    assert "schema_version" in sql


def test_scheduler_registers_shadow_after_canonical_candle_verification():
    source = (ROOT / "src" / "scheduler" / "runner.py").read_text(encoding="utf-8")

    assert 'id="thesis_shadow"' in source
    assert '_business_day_cron(hour=17, minute=18)' in source
    assert source.index('id="verify_daily_candles"') < source.index('id="thesis_shadow"')
    assert 'scripts/run_thesis_shadow.py' in source

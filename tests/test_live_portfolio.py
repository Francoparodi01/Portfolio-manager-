from datetime import datetime, timezone
import json

from src.collector.live_portfolio import (
    build_live_portfolio,
    render_live_portfolio_alert,
    select_portfolio_move_alerts,
)


def _snapshot():
    return {
        "snapshot_id": "snap-1",
        "scraped_at": "2026-05-18T13:30:00+00:00",
        "cash_ars": 10_000,
        "positions": [
            {
                "ticker": "NVDA",
                "quantity": 10,
                "current_price": 100,
                "market_value": 1_000,
            },
            {
                "ticker": "AMD",
                "quantity": 20,
                "current_price": 50,
                "market_value": 1_000,
            },
        ],
    }


def test_build_live_portfolio_revalues_from_market_prices():
    live = build_live_portfolio(
        _snapshot(),
        [
            {"ticker": "NVDA", "last_price": 103, "change_pct_1d": 0.03},
            {"ticker": "AMD", "last_price": 48, "change_pct_1d": -0.04},
        ],
        generated_at=datetime(2026, 5, 18, 16, 0, tzinfo=timezone.utc),
    )

    assert live["valuation_mode"] == "live_market_prices"
    assert live["price_coverage_count"] == 2
    assert live["invested_ars"] == 1990
    assert live["total_value_ars"] == 11990
    assert live["positions"][0]["market_value"] == 1030
    assert live["positions"][1]["market_value"] == 960


def test_build_live_portfolio_is_json_serializable_for_redis_cache():
    live = build_live_portfolio(
        _snapshot(),
        [
            {
                "ticker": "NVDA",
                "last_price": 103,
                "change_pct_1d": 0.03,
                "ts": datetime(2026, 5, 18, 16, 0, tzinfo=timezone.utc),
            },
        ],
    )

    assert json.loads(json.dumps(live))["positions"][0]["market_price_ts"].startswith("2026-05-18")


def test_select_portfolio_move_alerts_uses_major_or_weighted_thresholds():
    live = {
        "positions": [
            {
                "ticker": "NVDA",
                "change_pct_1d": 0.031,
                "weight_in_portfolio": 0.08,
                "market_value": 80_000,
            },
            {
                "ticker": "AMD",
                "change_pct_1d": -0.021,
                "weight_in_portfolio": 0.20,
                "market_value": 200_000,
            },
            {
                "ticker": "KO",
                "change_pct_1d": 0.021,
                "weight_in_portfolio": 0.05,
                "market_value": 50_000,
            },
        ]
    }

    alerts = select_portfolio_move_alerts(live)

    assert [(a.ticker, a.level, a.direction) for a in alerts] == [
        ("NVDA", "MAJOR", "UP"),
        ("AMD", "WEIGHTED", "DOWN"),
    ]


def test_render_live_portfolio_alert_includes_trigger_and_updated_portfolio():
    live = build_live_portfolio(
        _snapshot(),
        [
            {"ticker": "NVDA", "last_price": 103, "change_pct_1d": 0.03},
            {"ticker": "AMD", "last_price": 48, "change_pct_1d": -0.04},
        ],
    )
    alerts = select_portfolio_move_alerts(live, min_weight=0.0)

    output = render_live_portfolio_alert(alerts, live)

    assert "Movimiento relevante en cartera" in output
    assert "<b>AMD</b> -4.00% hoy" in output
    assert "Portfolio actualizado" in output
    assert "Valuacion live estimada con market_prices" not in output
    assert "Valuación live estimada con market_prices" in output

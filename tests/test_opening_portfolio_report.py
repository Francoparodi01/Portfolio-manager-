from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from src.scheduler import runner


@pytest.mark.parametrize("market_status", ["fresh", "stale", "missing"])
def test_opening_report_waits_for_market_refresh_and_keeps_freshness_guard(
    monkeypatch, market_status
):
    now = datetime.now(runner.ART_TZ)
    previous_day = now - timedelta(days=1)
    prices = [
        {"ticker": "AMD", "last_price": 100.0, "ts": previous_day}
    ]
    events = []
    snapshot = {
        "snapshot_id": "opening-test",
        "scraped_at": now,
        "cash_ars": 50.0,
        "positions": [
            {"ticker": "AMD", "quantity": 2, "current_price": 100.0}
        ],
    }

    async def refresh(**kwargs):
        if kwargs.get("include_market"):
            # Publish the quotes only after the asynchronous refresh completes.
            await asyncio.sleep(0)
            if market_status == "fresh":
                prices[:] = [{"ticker": "AMD", "last_price": 102.0, "ts": now}]
            elif market_status == "missing":
                prices.clear()
            events.append("market_saved")
        return {"ok": True}

    async def latest_prices():
        events.append("prices_read")
        return [dict(row) for row in prices]

    db = SimpleNamespace(
        connect=AsyncMock(),
        close=AsyncMock(),
        get_latest_snapshot=AsyncMock(return_value=snapshot),
        get_latest_market_prices=latest_prices,
        get_previous_candle_closes=AsyncMock(return_value={"AMD": 100.0}),
        get_corporate_action_effects=AsyncMock(return_value=[]),
    )
    cfg = SimpleNamespace(
        scraper=SimpleNamespace(telegram_bot_token="test", telegram_chat_id="test"),
        database=SimpleNamespace(url="unused"),
    )
    notifier = Mock()
    cache = AsyncMock()
    monkeypatch.setattr(runner, "_now_art", lambda: now)
    monkeypatch.setattr(runner, "_is_business_day", lambda *_args: True)
    monkeypatch.setattr(runner, "get_config", lambda: cfg)
    monkeypatch.setattr(runner, "TelegramNotifier", lambda *_args: notifier)
    monkeypatch.setattr(runner, "PortfolioDatabase", lambda *_args: db)
    monkeypatch.setattr(runner, "request_portfolio_refresh", refresh)
    monkeypatch.setattr(runner, "cache_live_portfolio", cache)
    monkeypatch.setattr(
        runner, "_safe_manual_event_risk_by_ticker", AsyncMock(return_value={})
    )

    result = asyncio.run(runner.run_opening_portfolio_report())

    assert result["success"] is True
    assert events == ["market_saved", "prices_read"]
    db.close.assert_awaited_once()
    notifier.notify_critical_error.assert_not_called()
    notifier.send_raw.assert_called_once()
    live = cache.await_args.args[0]
    report = notifier.send_raw.call_args.args[0]
    if market_status == "fresh":
        assert result["price_coverage"] == 1
        assert live["total_value_ars"] == pytest.approx(254.0)
        assert live["day_change_pct"] == pytest.approx(0.02)
        assert "APERTURA DE MERCADO - PORTFOLIO ACTUALIZADO" in report
        assert "Precios de mercado: <b>1/1</b>" in report
    else:
        assert result["price_coverage"] == 0
        assert live["total_value_ars"] == pytest.approx(250.0)
        assert live["day_change_pct"] is None
        assert "APERTURA - PRECIOS AUN NO DISPONIBLES" in report
        assert "Movimiento cartera: <b>N/A</b>" in report

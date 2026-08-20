import asyncio
from datetime import datetime

from src.scheduler import runner


def test_run_market_refresh_persists_one_combined_batch(monkeypatch):
    requests = []

    async def fake_refresh(**kwargs):
        requests.append(kwargs)
        return {
            "ok": True,
            "acciones": 1,
            "cedears": 2,
            "market_rows": 3,
        }

    monkeypatch.setattr(runner, "request_portfolio_refresh", fake_refresh)
    monkeypatch.setattr(runner, "_is_business_day", lambda now: True)
    monkeypatch.setattr(runner, "RADAR_INTRADAY_SETUP_ALERTS_ENABLED", False)
    monkeypatch.setattr(
        runner,
        "_now_art",
        lambda: datetime(2026, 8, 10, 12, 0, tzinfo=runner.ART_TZ),
    )
    result = asyncio.run(runner.run_market_refresh("12:00_MARKET"))

    assert result == {
        "success": True,
        "run_type": "12:00_MARKET",
        "acciones": 1,
        "cedears": 2,
        "prices": 3,
    }
    assert requests == [{
        "requester": "scheduler:12:00_MARKET",
        "include_fills": False,
        "include_market": True,
        "timeout_seconds": 360,
    }]

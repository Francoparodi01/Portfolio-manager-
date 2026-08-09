import asyncio
from datetime import datetime
from types import SimpleNamespace

from src.scheduler import runner


def test_run_market_refresh_persists_one_combined_batch(monkeypatch):
    saved_batches = []
    heartbeats = []

    class FakeDatabase:
        def __init__(self, url):
            assert url == "postgresql://test"

        async def connect(self):
            return None

        async def save_market_prices(self, rows):
            saved_batches.append(rows)

        async def close(self):
            return None

    class FakeScraper:
        def __init__(self, config):
            assert config == "scraper-config"

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def login(self):
            return None

        async def scrape_market(self, segment):
            assert segment == "ACCIONES"
            return ["GGAL"]

        async def scrape_cedears_segments(self):
            return ["AAPL", "MSFT"]

    async def fake_redis_set(*args, **kwargs):
        return None

    async def fake_redis_delete(*args, **kwargs):
        return None

    async def fake_heartbeat(key):
        heartbeats.append(key)

    monkeypatch.setattr(
        runner,
        "get_config",
        lambda: SimpleNamespace(
            database=SimpleNamespace(url="postgresql://test"),
            scraper="scraper-config",
        ),
    )
    monkeypatch.setattr(runner, "PortfolioDatabase", FakeDatabase)
    monkeypatch.setattr(runner, "CocosCapitalScraper", FakeScraper)
    monkeypatch.setattr(runner, "_is_business_day", lambda now: True)
    monkeypatch.setattr(
        runner,
        "_now_art",
        lambda: datetime(2026, 8, 10, 12, 0, tzinfo=runner.ART_TZ),
    )
    monkeypatch.setattr(runner, "_get_scraper_lock", lambda: runner.asyncio.Lock())
    monkeypatch.setattr(runner, "_redis_set", fake_redis_set)
    monkeypatch.setattr(runner, "_redis_delete", fake_redis_delete)
    monkeypatch.setattr(runner, "_heartbeat", fake_heartbeat)

    result = asyncio.run(runner.run_market_refresh("12:00_MARKET"))

    assert result == {
        "success": True,
        "run_type": "12:00_MARKET",
        "acciones": 1,
        "cedears": 2,
        "prices": 3,
    }
    assert saved_batches == [["GGAL", "AAPL", "MSFT"]]
    assert heartbeats == [runner.MARKET_HEARTBEAT_KEY]

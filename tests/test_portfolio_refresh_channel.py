from __future__ import annotations

import asyncio
import json
from pathlib import Path

from src.core import portfolio_refresh


class _FakeRedis:
    def __init__(self):
        self.values: dict[str, str] = {}
        self.lists: dict[str, list[str]] = {}

    async def delete(self, key: str):
        self.values.pop(key, None)

    async def rpush(self, key: str, value: str):
        self.lists.setdefault(key, []).append(value)

    async def lpop(self, key: str):
        values = self.lists.get(key) or []
        return values.pop(0) if values else None

    async def get(self, key: str):
        return self.values.get(key)

    async def set(self, key: str, value: str, ex: int | None = None):
        self.values[key] = value


def test_refresh_request_round_trip_uses_one_scheduler_owned_queue(monkeypatch):
    fake = _FakeRedis()
    monkeypatch.setattr(portfolio_refresh, "redis_client", fake)

    async def scenario():
        caller = asyncio.create_task(
            portfolio_refresh.request_portfolio_refresh(
                requester="telegram",
                include_fills=True,
                timeout_seconds=2,
            )
        )
        request = None
        for _ in range(20):
            request = await portfolio_refresh.pop_portfolio_refresh_request()
            if request is not None:
                break
            await asyncio.sleep(0.01)
        assert request is not None
        assert request.requester == "telegram"
        assert request.include_fills is True

        await portfolio_refresh.complete_portfolio_refresh_request(
            request,
            {"ok": True, "snapshot_id": "snapshot-1", "positions": 12},
        )
        return await caller

    result = asyncio.run(scenario())

    assert result["ok"] is True
    assert result["snapshot_id"] == "snapshot-1"
    assert result["positions"] == 12


def test_malformed_refresh_request_is_discarded(monkeypatch):
    fake = _FakeRedis()
    fake.lists[portfolio_refresh.PORTFOLIO_REFRESH_QUEUE_KEY] = [
        json.dumps({"requester": "missing-id"})
    ]
    monkeypatch.setattr(portfolio_refresh, "redis_client", fake)

    assert asyncio.run(portfolio_refresh.pop_portfolio_refresh_request()) is None


def test_telegram_sync_no_longer_launches_a_second_scraper():
    root = Path(portfolio_refresh.__file__).resolve().parents[2]
    source = (root / "scripts" / "telegram_bot.py").read_text(encoding="utf-8")
    start = source.index("async def sync_operational_state")
    end = source.index("def main_keyboard", start)
    block = source[start:end]

    assert "request_portfolio_refresh(" in block
    assert "run_once.py" not in block


def test_scheduler_owns_persistent_session_and_forces_requested_refresh():
    root = Path(portfolio_refresh.__file__).resolve().parents[2]
    source = (root / "src" / "scheduler" / "runner.py").read_text(encoding="utf-8")

    assert "force_refresh=refresh_request is not None" in source
    assert 'await start_intraday_loops()' in source
    assert 'id="intraday_stop"' not in source
    assert "run_persistent_eod_refresh," in source

    opening = source[
        source.index("async def run_opening_portfolio_report("):
        source.index("async def _latest_prices_with_previous_close", source.index("async def run_opening_portfolio_report("))
    ]
    assert "request_portfolio_refresh(" in opening
    assert "CocosCapitalScraper(" not in opening

    market = source[
        source.index("async def run_market_refresh("):
        source.index("async def _scrape_portfolio_with_retries", source.index("async def run_market_refresh("))
    ]
    assert "request_portfolio_refresh(" in market
    assert "CocosCapitalScraper(" not in market


def test_portfolio_command_requests_fresh_snapshot_from_persistent_session():
    root = Path(portfolio_refresh.__file__).resolve().parents[2]
    source = (root / "scripts" / "telegram_bot.py").read_text(encoding="utf-8")
    start = source.index("async def action_portfolio")
    end = source.index("async def action_weekly_summary", start)
    block = source[start:end]

    assert "sync_operational_state(" in block
    assert "force=True" in block
    assert "get_latest_snapshot" in block

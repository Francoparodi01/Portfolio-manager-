import asyncio
import sys
import types
from types import SimpleNamespace
from unittest.mock import patch
from contextlib import asynccontextmanager

redis_module = types.ModuleType("redis")
redis_asyncio_module = types.ModuleType("redis.asyncio")
redis_asyncio_module.from_url = lambda *_args, **_kwargs: object()
redis_module.asyncio = redis_asyncio_module
sys.modules.setdefault("redis", redis_module)
sys.modules.setdefault("redis.asyncio", redis_asyncio_module)

from src.scheduler import runner


class _FakeDb:
    def __init__(self, _url):
        pass

    async def connect(self):
        return None

    async def close(self):
        return None

    async def get_daily_candle_build_status(self):
        return {
            "business_day": "2026-05-15",
            "price_assets": 10,
            "internal_candles": 9,
            "missing_internal": 1,
        }


class _DecisionPriceConn:
    async def fetchrow(self, *_args):
        return {"total": 3, "missing_price": 1}


class _DecisionPricePool:
    @asynccontextmanager
    async def acquire(self):
        yield _DecisionPriceConn()


class _FakeDecisionPriceDb:
    def __init__(self, _url):
        pass

    async def connect(self):
        return None

    async def close(self):
        return None

    async def get_pool(self):
        return _DecisionPricePool()


def test_verify_daily_candles_logs_incomplete_status():
    cfg = SimpleNamespace(database=SimpleNamespace(url="postgresql://unused"))

    with patch("src.scheduler.runner.get_config", return_value=cfg):
        with patch("src.scheduler.runner.PortfolioDatabase", _FakeDb):
            with patch("src.scheduler.runner._is_business_day", return_value=True):
                with patch.object(runner.logger, "warning") as warning:
                    asyncio.run(runner.run_verify_daily_candles())

    warning.assert_called_once()


def test_verify_decision_prices_warns_missing_price_at_decision(monkeypatch):
    cfg = SimpleNamespace(
        database=SimpleNamespace(url="postgresql://unused"),
        scraper=SimpleNamespace(telegram_bot_token="", telegram_chat_id=""),
    )

    class _Notifier:
        messages = []

        def __init__(self, *_args):
            pass

        def send_raw(self, text):
            self.messages.append(text)

    monkeypatch.setattr(runner, "_is_business_day", lambda *_args: True)
    with patch("src.scheduler.runner.get_config", return_value=cfg):
        with patch("src.scheduler.runner.PortfolioDatabase", _FakeDecisionPriceDb):
            with patch("src.scheduler.runner.TelegramNotifier", _Notifier):
                asyncio.run(runner.run_verify_decision_prices())

    assert _Notifier.messages
    assert "sin price_at_decision" in _Notifier.messages[0]

import asyncio
import sys
import types
from types import SimpleNamespace
from unittest.mock import patch

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


def test_verify_daily_candles_logs_incomplete_status():
    cfg = SimpleNamespace(database=SimpleNamespace(url="postgresql://unused"))

    with patch("src.scheduler.runner.get_config", return_value=cfg):
        with patch("src.scheduler.runner.PortfolioDatabase", _FakeDb):
            with patch.object(runner.logger, "warning") as warning:
                asyncio.run(runner.run_verify_daily_candles())

    warning.assert_called_once()

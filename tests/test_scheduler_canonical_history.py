import asyncio
import sys
import types
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

redis_module = types.ModuleType("redis")
redis_asyncio_module = types.ModuleType("redis.asyncio")
redis_asyncio_module.from_url = lambda *_args, **_kwargs: object()
redis_module.asyncio = redis_asyncio_module
sys.modules.setdefault("redis", redis_module)
sys.modules.setdefault("redis.asyncio", redis_asyncio_module)

from src.scheduler.runner import _load_canonical_history_frames


def _rows(count: int):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return [
        {
            "ts": start + timedelta(days=i),
            "open_price": 100 + i,
            "high_price": 101 + i,
            "low_price": 99 + i,
            "close_price": 100.5 + i,
            "volume": 1000 + i,
            "source": "COCOS",
        }
        for i in range(count)
    ]


class _FakeDb:
    async def get_market_candles(self, ticker, **_kwargs):
        return _rows(60) if ticker == "T" else _rows(20)


def test_scheduler_technical_history_uses_canonical_candles():
    positions = [
        SimpleNamespace(ticker="T", asset_type=SimpleNamespace(value="CEDEAR")),
        SimpleNamespace(ticker="GGAL", asset_type=SimpleNamespace(value="ACCION")),
    ]

    frames = asyncio.run(_load_canonical_history_frames(_FakeDb(), positions))

    assert list(frames) == ["T"]
    assert frames["T"].attrs["candle_sources"] == ("COCOS",)

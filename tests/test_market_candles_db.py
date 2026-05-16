import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from src.collector.db import PortfolioDatabase


class _FakeConnection:
    async def fetch(self, query, *params):
        self.query = query
        self.params = params
        return [
            {
                "ts": datetime(2026, 5, 15, tzinfo=timezone.utc),
                "ticker": "T",
                "long_ticker": "T-0002-C-CT-ARS",
                "asset_type": "CEDEAR",
                "currency": "ARS",
                "venue": "BYMA",
                "interval": "1d",
                "open_price": 12210,
                "high_price": 12360,
                "low_price": 11830,
                "close_price": 11930,
                "volume": 9963,
                "source": "COCOS",
            },
            {
                "ts": datetime(2026, 5, 14, tzinfo=timezone.utc),
                "ticker": "T",
                "long_ticker": "T-0002-C-CT-ARS",
                "asset_type": "CEDEAR",
                "currency": "ARS",
                "venue": "BYMA",
                "interval": "1d",
                "open_price": 12230,
                "high_price": 12370,
                "low_price": 12150,
                "close_price": 12170,
                "volume": 7843,
                "source": "COCOS",
            },
        ]


class _FakePool:
    def __init__(self):
        self.conn = _FakeConnection()

    @asynccontextmanager
    async def acquire(self):
        yield self.conn


def test_get_market_candles_returns_ascending_history():
    db = PortfolioDatabase("postgresql://unused")
    db._pool = _FakePool()

    rows = asyncio.run(db.get_market_candles("T", asset_type="CEDEAR", limit=260))

    assert [row["ts"].day for row in rows] == [14, 15]
    assert db._pool.conn.params == ("T", "1d", "CEDEAR", 260)

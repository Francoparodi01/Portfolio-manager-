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


class _BuilderConnection:
    def __init__(self):
        self.saved_rows = []

    async def fetch(self, _query, *_params):
        return [
            {
                "ticker": "T",
                "asset_type": "CEDEAR",
                "currency": "ARS",
                "open_price": 12000,
                "high_price": 12100,
                "low_price": 11950,
                "close_price": 12080,
                "volume": 1500,
            }
        ]

    async def executemany(self, _query, rows):
        self.saved_rows.extend(rows)


class _FakePool:
    def __init__(self):
        self.conn = _FakeConnection()

    @asynccontextmanager
    async def acquire(self):
        yield self.conn


class _BuilderPool(_FakePool):
    def __init__(self):
        self.conn = _BuilderConnection()


def test_get_market_candles_returns_ascending_history():
    db = PortfolioDatabase("postgresql://unused")
    db._pool = _FakePool()

    rows = asyncio.run(db.get_market_candles("T", asset_type="CEDEAR", limit=260))

    assert [row["ts"].day for row in rows] == [14, 15]
    assert db._pool.conn.params == ("T", "1d", "CEDEAR", 260)
    assert "CASE WHEN source = 'COCOS' THEN 0 ELSE 1 END" in db._pool.conn.query
    assert "WHERE source_rank = 1" in db._pool.conn.query


def test_build_daily_candles_from_market_prices_uses_internal_snapshot_source():
    from datetime import date

    db = PortfolioDatabase("postgresql://unused")
    db._pool = _BuilderPool()

    saved = asyncio.run(db.build_daily_candles_from_market_prices(date(2026, 5, 15)))

    assert saved == 1
    row = db._pool.conn.saved_rows[0]
    assert row[1] == "T"
    assert row[2] == "INTERNAL:CEDEAR:T:ARS"
    assert row[12] == "internal_snapshot"

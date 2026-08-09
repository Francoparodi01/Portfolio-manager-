from __future__ import annotations

import asyncio
from datetime import date, datetime, timezone

from src.collector.db import PortfolioDatabase


def test_get_cocos_universe_assets_preserves_asset_type():
    db = PortfolioDatabase("postgresql://unused")

    async def fake_latest_prices(**_kwargs):
        return [
            {"ticker": "ggal", "asset_type": "ACCION", "currency": "ARS"},
            {"ticker": "t", "asset_type": "CEDEAR", "currency": "ARS"},
        ]

    db.get_latest_market_prices = fake_latest_prices

    assets = asyncio.run(db.get_cocos_universe_assets())

    assert assets == [
        {"ticker": "GGAL", "asset_type": "ACCION", "currency": "ARS"},
        {"ticker": "T", "asset_type": "CEDEAR", "currency": "ARS"},
    ]


def test_get_cocos_universe_assets_keeps_cocos_only_tickers():
    db = PortfolioDatabase("postgresql://unused")

    async def fake_latest_prices(**_kwargs):
        return [
            {"ticker": "come", "asset_type": "ACCION", "currency": "ARS"},
            {"ticker": "ypfd", "asset_type": "ACCION", "currency": "ARS"},
            {"ticker": "brkb", "asset_type": "CEDEAR", "currency": "ARS"},
        ]

    db.get_latest_market_prices = fake_latest_prices

    assets = asyncio.run(db.get_cocos_universe_assets())

    assert [asset["ticker"] for asset in assets] == ["COME", "YPFD", "BRKB"]


def test_get_latest_market_prices_fresh_only_bounds_scan_to_latest_valid_day():
    class _Connection:
        def __init__(self):
            self.fetchrow_statements = []
            self.fetch_statement = None
            self.fetch_args = None

        async def fetchrow(self, statement, *_args):
            self.fetchrow_statements.append(statement)
            return {"market_date": date(2026, 6, 22), "ticker_count": 2}

        async def fetch(self, statement, *args):
            self.fetch_statement = statement
            self.fetch_args = args
            return [
                {
                    "ticker": "FRESH",
                    "asset_type": "CEDEAR",
                    "currency": "ARS",
                    "last_price": 100.0,
                    "change_pct_1d": 0.0,
                    "ts": datetime(2026, 6, 22, 20, 0, tzinfo=timezone.utc),
                },
            ]

    class _Pool:
        def __init__(self):
            self.conn = _Connection()

        def acquire(self):
            return _AcquireWithConnection(self.conn)

    class _AcquireWithConnection:
        def __init__(self, conn):
            self.conn = conn

        async def __aenter__(self):
            return self.conn

        async def __aexit__(self, exc_type, exc, tb):
            return False

    db = PortfolioDatabase("postgresql://unused")
    pool = _Pool()
    db._pool = pool

    rows = asyncio.run(
        db.get_latest_market_prices(fresh_only=True, min_fresh_tickers=2)
    )

    assert [row["ticker"] for row in rows] == ["FRESH"]
    assert rows[0] == {
        "ticker": "FRESH",
        "asset_type": "CEDEAR",
        "currency": "ARS",
        "last_price": 100.0,
        "change_pct_1d": 0.0,
        "ts": datetime(2026, 6, 22, 20, 0, tzinfo=timezone.utc),
    }
    assert "ts >= $1::date" in pool.conn.fetch_statement
    assert "latest_per_ticker" not in pool.conn.fetch_statement
    assert pool.conn.fetch_args == (date(2026, 6, 22),)
    assert "INTERVAL '14 days'" in pool.conn.fetchrow_statements[0]


def test_get_latest_market_prices_falls_back_when_recent_window_is_empty():
    class _Connection:
        def __init__(self):
            self.fetchrow_calls = 0

        async def fetchrow(self, _statement, *_args):
            self.fetchrow_calls += 1
            if self.fetchrow_calls == 1:
                return None
            return {"market_date": date(2026, 5, 1), "ticker_count": 2}

        async def fetch(self, _statement, *_args):
            return []

    class _Acquire:
        def __init__(self, conn):
            self.conn = conn

        async def __aenter__(self):
            return self.conn

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _Pool:
        def __init__(self):
            self.conn = _Connection()

        def acquire(self):
            return _Acquire(self.conn)

    db = PortfolioDatabase("postgresql://unused")
    pool = _Pool()
    db._pool = pool

    rows = asyncio.run(
        db.get_latest_market_prices(fresh_only=True, min_fresh_tickers=2)
    )

    assert rows == []
    assert pool.conn.fetchrow_calls == 2

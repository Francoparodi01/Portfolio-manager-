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


def test_get_latest_market_prices_fresh_only_excludes_stale_ticker(caplog):
    class _Connection:
        async def fetchrow(self, _statement, *_args):
            return {"market_date": date(2026, 6, 22), "ticker_count": 2}

        async def fetch(self, _statement, *_args):
            return [
                {
                    "ticker": "FRESH",
                    "asset_type": "CEDEAR",
                    "currency": "ARS",
                    "last_price": 100.0,
                    "change_pct_1d": 0.0,
                    "ts": datetime(2026, 6, 22, 20, 0, tzinfo=timezone.utc),
                    "latest_price_date": date(2026, 6, 22),
                    "excluded_by_freshness": False,
                },
                {
                    "ticker": "STALE",
                    "asset_type": "CEDEAR",
                    "currency": "ARS",
                    "last_price": 90.0,
                    "change_pct_1d": 0.0,
                    "ts": datetime(2026, 6, 19, 20, 0, tzinfo=timezone.utc),
                    "latest_price_date": date(2026, 6, 19),
                    "excluded_by_freshness": True,
                },
            ]

    class _Acquire:
        async def __aenter__(self):
            return _Connection()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _Pool:
        def acquire(self):
            return _Acquire()

    db = PortfolioDatabase("postgresql://unused")
    db._pool = _Pool()

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
    assert "STALE@2026-06-19" in caplog.text

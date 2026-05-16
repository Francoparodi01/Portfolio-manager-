from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

from scripts import run_opportunity


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
        }
        for i in range(count)
    ]


class _FakeDatabase:
    def __init__(self, _url):
        pass

    async def connect(self):
        return None

    async def close(self):
        return None

    async def get_cocos_universe_assets(self):
        return [
            {"ticker": "T", "asset_type": "CEDEAR"},
            {"ticker": "GGAL", "asset_type": "ACCION"},
        ]

    async def get_market_candles(self, ticker, **_kwargs):
        return _rows(60 if ticker == "T" else 20)


def test_load_cocos_universe_assets_from_db():
    cfg = SimpleNamespace(database=SimpleNamespace(url="postgresql://unused"))

    with patch("scripts.run_opportunity.PortfolioDatabase", _FakeDatabase):
        assets = asyncio.run(run_opportunity._load_cocos_universe_assets(cfg))

    assert assets == [
        {"ticker": "T", "asset_type": "CEDEAR"},
        {"ticker": "GGAL", "asset_type": "ACCION"},
    ]


def test_load_cocos_history_frames_for_opportunities_requires_sufficient_history():
    cfg = SimpleNamespace(database=SimpleNamespace(url="postgresql://unused"))
    assets = [
        {"ticker": "T", "asset_type": "CEDEAR"},
        {"ticker": "GGAL", "asset_type": "ACCION"},
    ]

    with patch("scripts.run_opportunity.PortfolioDatabase", _FakeDatabase):
        frames = asyncio.run(run_opportunity._load_cocos_history_frames(cfg, assets))

    assert list(frames) == ["T"]

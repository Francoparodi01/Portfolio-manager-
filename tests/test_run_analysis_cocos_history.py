import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

from scripts import run_analysis


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
    def __init__(self, url):
        self.url = url

    async def connect(self):
        return None

    async def close(self):
        return None

    async def get_market_candles(self, ticker, **kwargs):
        return _rows(60) if ticker == "T" else _rows(20)


def test_load_cocos_history_frames_only_accepts_sufficient_history():
    cfg = SimpleNamespace(database=SimpleNamespace(url="postgresql://unused"))
    positions = [
        {"ticker": "T", "asset_type": "CEDEAR"},
        {"ticker": "GGAL", "asset_type": "ACCION"},
    ]

    with patch("scripts.run_analysis.PortfolioDatabase", _FakeDatabase):
        frames = asyncio.run(run_analysis._load_cocos_history_frames(cfg, positions))

    assert list(frames) == ["T"]
    assert len(frames["T"]) == 60


def test_count_assets_by_type_preserves_market_segments():
    counts = run_analysis._count_assets_by_type(
        [
            {"ticker": "GGAL", "asset_type": "ACCION"},
            {"ticker": "T", "asset_type": "CEDEAR"},
            {"ticker": "NVDA", "asset_type": "CEDEAR"},
        ]
    )

    assert counts == {"ACCION": 1, "CEDEAR": 2}

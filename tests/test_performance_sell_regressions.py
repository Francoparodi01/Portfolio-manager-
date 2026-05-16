import asyncio
from contextlib import asynccontextmanager

from src.collector.db import PortfolioDatabase


class _PerformanceConnection:
    def __init__(self, returns):
        self.returns = returns
        self.fetch_calls = 0

    async def fetch(self, _query, *_params):
        self.fetch_calls += 1
        if self.fetch_calls == 1:
            return [
                {
                    "id": idx,
                    "ticker": f"S{idx}",
                    "decision": "SELL",
                    "outcome_5d": value,
                    "outcome_10d": None,
                    "outcome_20d": None,
                    "was_correct": value > 0,
                    "size_pct": 0.1,
                }
                for idx, value in enumerate(self.returns, start=1)
            ]
        return []

    async def fetchval(self, _query, *_params):
        return 0


class _PerformancePool:
    def __init__(self, conn):
        self.conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self.conn


def test_win_rate_sell_above_50_on_gains():
    conn = _PerformanceConnection([0.10] * 6 + [-0.05] * 4)
    db = PortfolioDatabase("postgresql://unused")
    db._pool = _PerformancePool(conn)

    stats = asyncio.run(db.get_performance_stats())

    assert stats["win_rate"] > 0.50


def test_avg_return_sell_positive_on_net_gain():
    conn = _PerformanceConnection([0.10, 0.08, 0.06, -0.03])
    db = PortfolioDatabase("postgresql://unused")
    db._pool = _PerformancePool(conn)

    stats = asyncio.run(db.get_performance_stats())

    assert stats["avg_return_5d"] > 0

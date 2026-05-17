import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import pytest

from src.collector.db import PortfolioDatabase


class _RecomputeConnection:
    def __init__(self, row):
        self.row = row
        self.execute_calls = []

    async def fetch(self, *_args):
        return [self.row]

    async def execute(self, *args):
        self.execute_calls.append(args)


class _RecomputePool:
    def __init__(self, conn):
        self.conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self.conn


async def _async_result(value):
    return value


def test_recompute_outcomes_overwrites_legacy_sell_sign():
    decided_at = datetime.now(timezone.utc) - timedelta(days=10)
    conn = _RecomputeConnection(
        {
            "id": 7,
            "ticker": "QCOM",
            "decision": "SELL",
            "decided_at": decided_at,
            "price_at_decision": 100.0,
        }
    )
    db = PortfolioDatabase("postgresql://unused")
    db._pool = _RecomputePool(conn)
    db.get_market_candles = lambda *_args, **_kwargs: _async_result(
        [
            {
                "ts": decided_at + timedelta(days=5),
                "close_price": 110.0,
            }
        ]
    )

    updated = asyncio.run(db.recompute_outcomes())

    assert updated == 1
    persisted = conn.execute_calls[0]
    assert persisted[2] == pytest.approx(-0.10)
    assert persisted[5] is False
    assert persisted[6] == "canonical_cocos"


def test_recompute_outcomes_keeps_sell_gain_positive():
    decided_at = datetime.now(timezone.utc) - timedelta(days=10)
    conn = _RecomputeConnection(
        {
            "id": 8,
            "ticker": "DOW",
            "decision": "SELL",
            "decided_at": decided_at,
            "price_at_decision": 100.0,
        }
    )
    db = PortfolioDatabase("postgresql://unused")
    db._pool = _RecomputePool(conn)
    db.get_market_candles = lambda *_args, **_kwargs: _async_result(
        [
            {
                "ts": decided_at + timedelta(days=5),
                "close_price": 90.0,
            }
        ]
    )

    updated = asyncio.run(db.recompute_outcomes())

    assert updated == 1
    persisted = conn.execute_calls[0]
    assert persisted[2] == pytest.approx(0.10)
    assert persisted[5] is True
    assert persisted[6] == "canonical_cocos"


def test_recompute_outcomes_clears_incompatible_legacy_basis():
    decided_at = datetime.now(timezone.utc) - timedelta(days=10)
    conn = _RecomputeConnection(
        {
            "id": 9,
            "ticker": "NVDA",
            "decision": "BUY",
            "decided_at": decided_at,
            "price_at_decision": 100.0,
        }
    )
    db = PortfolioDatabase("postgresql://unused")
    db._pool = _RecomputePool(conn)
    db.get_market_candles = lambda *_args, **_kwargs: _async_result(
        [
            {
                "ts": decided_at + timedelta(days=1),
                "close_price": 12000.0,
            }
        ]
    )

    updated = asyncio.run(db.recompute_outcomes())

    assert updated == 0
    persisted = conn.execute_calls[0]
    assert "outcome_5d          = NULL" in persisted[0]
    assert persisted[2] == "legacy_external"
    assert persisted[3] == pytest.approx(120.0)


def test_recompute_outcomes_clears_rows_without_canonical_candles():
    decided_at = datetime.now(timezone.utc) - timedelta(days=10)
    conn = _RecomputeConnection(
        {
            "id": 10,
            "ticker": "NFLX",
            "decision": "SELL",
            "decided_at": decided_at,
            "price_at_decision": 100.0,
        }
    )
    db = PortfolioDatabase("postgresql://unused")
    db._pool = _RecomputePool(conn)
    db.get_market_candles = lambda *_args, **_kwargs: _async_result([])

    updated = asyncio.run(db.recompute_outcomes())

    assert updated == 0
    persisted = conn.execute_calls[0]
    assert "outcome_5d          = NULL" in persisted[0]
    assert persisted[2] == "legacy_external"

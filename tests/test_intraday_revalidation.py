import asyncio
import sys
import types
from contextlib import asynccontextmanager
from datetime import datetime


redis_module = types.ModuleType("redis")
redis_asyncio_module = types.ModuleType("redis.asyncio")
redis_asyncio_module.from_url = lambda *_args, **_kwargs: object()
redis_module.asyncio = redis_asyncio_module
sys.modules.setdefault("redis", redis_module)
sys.modules.setdefault("redis.asyncio", redis_asyncio_module)

from src.scheduler import runner


class _FakeConnection:
    def __init__(self, rows):
        self.rows = rows
        self.query = ""
        self.params = ()

    async def fetch(self, query, *params):
        self.query = query
        self.params = params
        return self.rows


class _FakePool:
    def __init__(self, rows):
        self.conn = _FakeConnection(rows)

    @asynccontextmanager
    async def acquire(self):
        yield self.conn


def _plan_row(ticker: str, *, decision_id: int, plan_price: float) -> dict:
    return {
        "id": decision_id,
        "decided_at": datetime(2026, 6, 19, 20, 12, tzinfo=runner.UTC),
        "ticker": ticker,
        "decision": "SELL",
        "plan_price": plan_price,
        "target_amount_ars": 100_000.0,
        "current_weight": 0.30,
        "target_weight": 0.25,
        "reason": "Reducir exposicion",
    }


def test_active_position_tickers_requires_positive_current_quantity():
    snapshot = {
        "positions": [
            {"ticker": "amd", "quantity": 8},
            {"ticker": "TSM", "quantity": "7"},
            {"ticker": "HMY", "quantity": 0},
            {"ticker": "MU", "quantity": -1},
        ]
    }

    assert runner._active_position_tickers(snapshot) == {"AMD", "TSM"}


def test_intraday_revalidation_only_uses_current_approved_positions(monkeypatch):
    now = datetime(2026, 6, 20, 13, 0, tzinfo=runner.ART_TZ)
    monkeypatch.setattr(runner, "_now_art", lambda: now)
    pool = _FakePool(
        [
            _plan_row("AMD", decision_id=1, plan_price=100.0),
            _plan_row("MU", decision_id=2, plan_price=200.0),
        ]
    )
    prices = [
        {"ticker": "AMD", "last_price": 110.0, "ts": now},
        {"ticker": "MU", "last_price": 250.0, "ts": now},
    ]
    manager = object.__new__(runner.IntradayManager)

    alerts = asyncio.run(
        manager._compute_intraday_revalidations(pool, prices, {"AMD"})
    )

    assert [alert.ticker for alert in alerts] == ["AMD"]
    assert pool.conn.params == (runner.INTRADAY_REVALIDATION_LOOKBACK_DAYS, ["AMD"])
    assert "status = 'APPROVED'" in pool.conn.query
    assert "ticker = ANY($2::text[])" in pool.conn.query
    assert "EXECUTED" not in pool.conn.query


def test_intraday_revalidation_skips_query_without_current_positions():
    pool = _FakePool([_plan_row("MU", decision_id=2, plan_price=200.0)])
    manager = object.__new__(runner.IntradayManager)

    alerts = asyncio.run(
        manager._compute_intraday_revalidations(
            pool,
            [{"ticker": "MU", "last_price": 250.0}],
            set(),
        )
    )

    assert alerts == []
    assert pool.conn.query == ""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

from src.collector.db import PortfolioDatabase


class _Connection:
    def __init__(self, row):
        self.row = row
        self.execute_calls = []

    async def fetch(self, *_args):
        return [self.row]

    async def execute(self, *args):
        self.execute_calls.append(args)


class _Pool:
    def __init__(self, conn):
        self.conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self.conn


async def _async_result(value):
    return value


def test_update_outcomes_marks_incompatible_rows_as_legacy_external():
    decided_at = datetime.now(timezone.utc) - timedelta(days=10)
    conn = _Connection(
        {
            "id": 11,
            "ticker": "QCOM",
            "decision": "BUY",
            "decided_at": decided_at,
            "price_at_decision": 100.0,
        }
    )
    db = PortfolioDatabase("postgresql://unused")
    db._pool = _Pool(conn)
    db.get_market_candles = lambda *_args, **_kwargs: _async_result(
        [
            {
                "ts": decided_at + timedelta(days=1),
                "close_price": 11000.0,
            }
        ]
    )

    updated = asyncio.run(db.update_outcomes())

    assert updated == 0
    persisted = conn.execute_calls[0]
    assert persisted[2] == "legacy_external"
    assert persisted[3] == 110.0


def test_assess_outcome_basis_accepts_same_unit_prices():
    db = PortfolioDatabase("postgresql://unused")
    decided_at = datetime.now(timezone.utc)

    basis, ratio = db._assess_outcome_basis(
        entry_price=100.0,
        decided_at=decided_at,
        candles=[
            {
                "ts": decided_at + timedelta(days=1),
                "close_price": 105.0,
            }
        ],
    )

    assert basis == "canonical_cocos"
    assert ratio == 1.05

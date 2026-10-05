import asyncio
from datetime import date, datetime, time, timezone
from types import SimpleNamespace
from uuid import uuid4

from src.collector.db import (
    PortfolioDatabase,
    _session_after,
)


def _candle(day: date, close: float) -> dict:
    return {
        "ts": datetime.combine(day, time(20), tzinfo=timezone.utc),
        "open_price": close,
        "close_price": close,
    }


def test_session_counter_skips_byma_closure():
    # July 9 is a configured BYMA closure in the project calendar.
    assert _session_after(date(2026, 7, 8), 1) == date(2026, 7, 10)


def test_directional_outcome_requires_exact_horizon_session():
    start = date(2026, 7, 8)
    target = _session_after(start, 5)
    following = _session_after(target, 1)
    database = PortfolioDatabase("postgresql://unused")

    outcomes = asyncio.run(
        database._compute_directional_outcomes(
            entry_price=100.0,
            decided_at=datetime.combine(start, time(15), tzinfo=timezone.utc),
            direction="BUY",
            now=datetime.combine(following, time(21), tzinfo=timezone.utc),
            candles=[_candle(following, 110.0)],
        )
    )

    # Missing market data is missing evidence; it must not silently shift 5D.
    assert "outcome_5d" not in outcomes


def test_executable_outcome_uses_entry_session_as_first_holding_session():
    entry = date(2026, 7, 10)
    exit_day = _session_after(entry, 4)
    database = PortfolioDatabase("postgresql://unused")

    outcomes = asyncio.run(
        database._compute_executable_outcomes(
            entry_price=100.0,
            start_day=entry,
            direction="BUY",
            now=datetime.combine(exit_day, time(21), tzinfo=timezone.utc),
            candles=[_candle(exit_day, 110.0)],
        )
    )

    assert outcomes["executable_outcome_5d"] == 0.1


def test_candle_reader_selects_one_complete_source_series():
    statements: list[str] = []

    class Connection:
        async def fetch(self, statement, *_args):
            statements.append(statement)
            return []

    class Acquire:
        async def __aenter__(self):
            return Connection()

        async def __aexit__(self, *_args):
            return False

    class Pool:
        def acquire(self):
            return Acquire()

    database = PortfolioDatabase("postgresql://unused")
    database._pool = Pool()
    assert asyncio.run(database.get_market_candles("YPFD", limit=20)) == []

    statement = statements[0]
    assert "selected_series" in statement
    assert "PARTITION BY source, long_ticker" in statement
    assert "session_coverage DESC" in statement


def test_snapshot_replay_keeps_first_payload_immutable():
    calls: list[str] = []

    class Transaction:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

    class Connection:
        def transaction(self):
            return Transaction()

        async def fetchval(self, statement, *_args):
            calls.append(statement)
            return None  # Existing snapshot_id.

        async def fetch(self, _statement, *_args):
            return []

        async def fetchrow(self, _statement, *_args):
            return {
                "snapshot_id": snapshot.snapshot_id,
                "scraped_at": snapshot.scraped_at,
                "total_value_ars": snapshot.total_value_ars,
                "cash_ars": snapshot.cash_ars,
                "confidence_score": snapshot.confidence_score,
                "dom_hash": snapshot.dom_hash,
                "raw_html_hash": snapshot.raw_html_hash,
            }

        async def execute(self, statement, *_args):
            calls.append(statement)

    class Acquire:
        async def __aenter__(self):
            return Connection()

        async def __aexit__(self, *_args):
            return False

    class Pool:
        def acquire(self):
            return Acquire()

    database = PortfolioDatabase("postgresql://unused")
    database._pool = Pool()
    snapshot = SimpleNamespace(
        snapshot_id=uuid4(),
        owner_chat_id=1,
        scraped_at=datetime.now(timezone.utc),
        total_value_ars=100.0,
        cash_ars=10.0,
        confidence_score=1.0,
        dom_hash=None,
        raw_html_hash=None,
        positions=[],
    )

    assert asyncio.run(database.save_snapshot(snapshot)) == snapshot.snapshot_id
    assert len(calls) == 1
    assert "ON CONFLICT (snapshot_id) DO NOTHING" in calls[0]

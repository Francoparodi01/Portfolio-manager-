import asyncio
from datetime import datetime, timedelta, timezone

from src.collector.broker_fills import BrokerFill
from src.collector.db import PortfolioDatabase
from src.analysis.fill_reconciliation import ExecutionCandidate, choose_execution_candidate


class _RecordingConnection:
    def __init__(self):
        self.executemany_calls = []
        self.execute_calls = []

    async def executemany(self, statement, rows):
        self.executemany_calls.append((statement, rows))

    async def fetch(self, statement, *args):
        if "UPDATE broker_fills synthetic" in statement:
            return []
        if "FROM broker_fills" in statement:
            return [
                {
                    "id": 1,
                    "source": "manual_import",
                    "external_fill_id": "fill-1",
                    "executed_at": datetime(2026, 5, 18, 14, 35, tzinfo=timezone.utc),
                    "executed_at_precision": "timestamp",
                    "executed_at_source": "broker",
                    "ticker": "NVDA",
                    "side": "BUY",
                    "quantity": 2,
                    "avg_fill_price": 100,
                    "gross_amount_ars": 200,
                    "fees_ars": 1,
                    "raw_payload": {},
                }
            ]
        if "FROM decision_log" in statement:
            return [
                {
                    "id": 99,
                    "ticker": "NVDA",
                    "decision": "BUY",
                    "decided_at": datetime(2026, 5, 18, 14, 0, tzinfo=timezone.utc),
                    "status": "APPROVED",
                    "theoretical_amount_ars": 200,
                }
            ]
        return []

    async def execute(self, statement, *args):
        self.execute_calls.append((statement, args))


class _AcquireContext:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _Pool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        return _AcquireContext(self.conn)


def test_save_broker_fills_upserts_rows():
    conn = _RecordingConnection()
    db = PortfolioDatabase("postgresql://unused")
    db._pool = _Pool(conn)

    saved = asyncio.run(
        db.save_broker_fills(
            [
                BrokerFill(
                    external_fill_id="fill-1",
                    executed_at=datetime(2026, 5, 18, 14, 35, tzinfo=timezone.utc),
                    ticker="NVDA",
                    side="BUY",
                    quantity=2,
                    avg_fill_price=100,
                    gross_amount_ars=200,
                )
            ],
            owner_chat_id=123,
        )
    )

    assert saved == 1
    assert len(conn.executemany_calls) == 1
    assert "INSERT INTO broker_fills" in conn.executemany_calls[0][0]
    assert "owner_chat_id" in conn.executemany_calls[0][0]
    assert conn.executemany_calls[0][1][0][12] == 123


def test_reconcile_broker_fills_promotes_approved_event_to_executed():
    conn = _RecordingConnection()
    db = PortfolioDatabase("postgresql://unused")
    db._pool = _Pool(conn)

    reconciled = asyncio.run(db.reconcile_broker_fills())

    assert reconciled == 1
    assert any("UPDATE broker_fills" in statement for statement, _ in conn.execute_calls)
    assert any("status = 'EXECUTED'" in statement for statement, _ in conn.execute_calls)


def test_choose_execution_candidate_accepts_same_calendar_day_without_fill_time():
    fill = BrokerFill(
        external_fill_id="fill-1",
        executed_at=datetime(2026, 5, 18, 0, 0, tzinfo=timezone.utc),
        ticker="NVDA",
        side="BUY",
        quantity=4,
        avg_fill_price=13940,
        gross_amount_ars=55760,
    )
    candidate = ExecutionCandidate(
        id=53,
        ticker="NVDA",
        decision="BUY",
        decided_at=datetime(2026, 5, 18, 14, 0, tzinfo=timezone.utc),
        status="APPROVED",
        theoretical_amount_ars=54677,
    )

    assert choose_execution_candidate(fill, [candidate], max_age=timedelta(days=3)) == candidate


def test_choose_execution_candidate_matches_sell_variants():
    fill = BrokerFill(
        external_fill_id="fill-2",
        executed_at=datetime(2026, 5, 18, 15, 0, tzinfo=timezone.utc),
        ticker="TSLA",
        side="SELL",
        quantity=4,
        avg_fill_price=42140,
        gross_amount_ars=-168560,
    )
    candidate = ExecutionCandidate(
        id=42,
        ticker="TSLA",
        decision="SELL_FULL",
        decided_at=datetime(2026, 5, 18, 14, 0, tzinfo=timezone.utc),
        status="APPROVED",
        theoretical_amount_ars=168000,
    )

    assert choose_execution_candidate(fill, [candidate], max_age=timedelta(days=3)) == candidate

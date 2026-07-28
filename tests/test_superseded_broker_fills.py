import asyncio

from src.collector.db import PortfolioDatabase


class _Acquire:
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
        return _Acquire(self.conn)


def _db_with_conn(conn):
    db = PortfolioDatabase("postgresql://unused")
    db._pool = _Pool(conn)
    db._execution_timestamp_meta_ready = True
    db._decision_audit_scope_ready = True
    db._outcome_horizon_ready = True
    return db


def test_mark_superseded_broker_fills_uses_historical_mapping():
    class _Connection:
        def __init__(self):
            self.fetch_calls = []

        async def fetch(self, statement, *args):
            self.fetch_calls.append((statement, args))
            return [{"id": fill_id} for fill_id in args[0]]

    conn = _Connection()
    db = _db_with_conn(conn)

    marked = asyncio.run(
        db.mark_superseded_broker_fills(
            {
                15286: 17336,
                15435: 17336,
                12528: 14637,
                26387: 28241,
                26890: 28241,
                85036: 87186,
            }
        )
    )

    assert marked == 6
    statement, args = conn.fetch_calls[0]
    assert "superseded_by_real" in statement
    assert args[0] == [12528, 15286, 15435, 26387, 26890, 85036]
    assert args[1] == [14637, 17336, 17336, 28241, 28241, 87186]


def test_reconcile_broker_fills_excludes_superseded_placeholders():
    class _Connection:
        def __init__(self):
            self.fetch_statements = []

        async def fetch(self, statement, *args):
            self.fetch_statements.append(statement)
            return []

    conn = _Connection()
    db = _db_with_conn(conn)

    reconciled = asyncio.run(db.reconcile_broker_fills())

    assert reconciled == 0
    fill_query = next(stmt for stmt in conn.fetch_statements if "FROM broker_fills" in stmt)
    assert "superseded_by_real" in fill_query


def test_materialize_unmatched_broker_fills_excludes_superseded_placeholders():
    class _Connection:
        def __init__(self):
            self.fetch_statements = []

        async def fetch(self, statement, *args):
            self.fetch_statements.append(statement)
            return []

    conn = _Connection()
    db = _db_with_conn(conn)

    materialized = asyncio.run(db.materialize_unmatched_broker_fills())

    assert materialized == 0
    group_query = next(stmt for stmt in conn.fetch_statements if "GROUP BY" in stmt)
    assert "superseded_by_real" in group_query


def test_performance_stats_excludes_decisions_backed_only_by_superseded_fills():
    class _Connection:
        def __init__(self):
            self.fetch_statements = []

        async def fetch(self, statement, *args):
            self.fetch_statements.append(statement)
            return []

        async def fetchval(self, statement, *args):
            return 0

    conn = _Connection()
    db = _db_with_conn(conn)

    stats = asyncio.run(db.get_performance_stats())

    assert stats["total_trades"] == 0
    raw_query = next(
        stmt
        for stmt in conn.fetch_statements
        if "COALESCE(executable_outcome_5d, outcome_5d)" in stmt
    )
    assert "superseded_by_real" in raw_query
    assert "live_bf" in raw_query

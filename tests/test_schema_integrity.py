import ast
import asyncio
import sqlite3
from pathlib import Path

import pytest

from src.collector.db import PortfolioDatabase


ROOT = Path(__file__).resolve().parents[1]


class _FailingConnection:
    async def execute(self, _statement):
        raise sqlite3.OperationalError("ddl failed")


class _AcquireContext:
    async def __aenter__(self):
        return _FailingConnection()

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FailingPool:
    def acquire(self):
        return _AcquireContext()


class _RecordingConnection:
    def __init__(self):
        self.executed: list[str] = []

    async def execute(self, statement):
        self.executed.append(statement)


class _RecordingAcquireContext:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _RecordingPool:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        return _RecordingAcquireContext(self._conn)


def _string_literals(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    ]


def test_init_schema_raises_on_ddl_error():
    db = PortfolioDatabase("postgresql://unused")
    db._pool = _FailingPool()

    with pytest.raises(sqlite3.OperationalError):
        asyncio.run(db.init_schema())


def test_init_schema_executes_full_sql_document_once():
    conn = _RecordingConnection()
    db = PortfolioDatabase("postgresql://unused")
    db._pool = _RecordingPool(conn)

    asyncio.run(db.init_schema())

    assert len(conn.executed) == 1
    assert "DO $$" in conn.executed[0]


def test_init_sql_makes_timescaledb_optional():
    init_sql = (ROOT / "init.sql").read_text(encoding="utf-8")

    assert "FROM pg_available_extensions" in init_sql
    assert "WHERE name = 'timescaledb'" in init_sql
    assert "FROM pg_extension" in init_sql
    assert init_sql.count("PERFORM create_hypertable") == 4


def test_existing_table_migrations_run_before_dependent_indexes():
    init_sql = (ROOT / "init.sql").read_text(encoding="utf-8")

    assert init_sql.index("ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS decision_date") < init_sql.index(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_decision_log_unique_daily_action"
    )
    assert init_sql.index("ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS stop_loss_price") < init_sql.index(
        "CREATE INDEX IF NOT EXISTS idx_decision_log_stops"
    )


def test_duplicate_daily_decisions_are_deduped_before_unique_index():
    init_sql = (ROOT / "init.sql").read_text(encoding="utf-8")

    dedupe_sql = "WITH ranked_daily_decisions AS"
    unique_index_sql = "CREATE UNIQUE INDEX IF NOT EXISTS idx_decision_log_unique_daily_action"

    assert dedupe_sql in init_sql
    assert "ORDER BY decided_at DESC, id DESC" in init_sql
    assert init_sql.index(dedupe_sql) < init_sql.index(unique_index_sql)


def test_decision_log_unique_constraint():
    init_sql = (ROOT / "init.sql").read_text(encoding="utf-8")
    assert "idx_decision_log_unique_daily_action" in init_sql

    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE decision_log (
            ticker TEXT NOT NULL,
            decision_date TEXT NOT NULL,
            decision TEXT NOT NULL,
            UNIQUE (ticker, decision_date, decision)
        )
        """
    )
    conn.execute(
        "INSERT INTO decision_log (ticker, decision_date, decision) VALUES (?, ?, ?)",
        ("NVDA", "2026-05-14", "BUY"),
    )

    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO decision_log (ticker, decision_date, decision) VALUES (?, ?, ?)",
            ("NVDA", "2026-05-14", "BUY"),
        )


def test_db_module_has_no_ddl():
    literals = _string_literals(ROOT / "src" / "collector" / "db.py")
    assert all("CREATE TABLE" not in value for value in literals)
    assert all("ALTER TABLE" not in value for value in literals)


def test_trade_lifecycle_has_no_ddl():
    literals = _string_literals(ROOT / "src" / "analysis" / "trade_lifecycle.py")
    assert all("CREATE TABLE" not in value for value in literals)
    assert all("ALTER TABLE" not in value for value in literals)

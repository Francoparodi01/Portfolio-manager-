from __future__ import annotations

import asyncio

import src.analysis.audit_scope as audit_scope


class _Row(dict):
    pass


class _ReadOnlyConn:
    def __init__(self, columns: set[str]):
        self.columns = columns
        self.executed = False

    async def fetchval(self, query: str, *_args):
        assert "transaction_read_only" in query
        return "on"

    async def fetch(self, _query: str, *_args):
        return [_Row(column_name=name) for name in sorted(self.columns)]

    async def execute(self, _query: str):
        self.executed = True
        raise AssertionError("read-only schema validation must never execute migration SQL")


class _WritableConn:
    def __init__(self):
        self.executed = False

    async def fetchval(self, query: str, *_args):
        assert "transaction_read_only" in query
        return "off"

    async def fetch(self, _query: str, *_args):
        raise AssertionError("writable migration path does not need schema probe")

    async def execute(self, query: str):
        assert "ALTER TABLE decision_log" in query
        self.executed = True


def test_read_only_connection_validates_existing_schema_without_writes(monkeypatch):
    monkeypatch.setattr(audit_scope, "_MIGRATION_DONE", False)
    conn = _ReadOnlyConn(set(audit_scope._AUDIT_SCOPE_COLUMNS))
    asyncio.run(audit_scope.ensure_decision_audit_scope_columns(conn))
    assert not conn.executed
    assert audit_scope._MIGRATION_DONE


def test_writable_connection_keeps_self_healing_migration(monkeypatch):
    monkeypatch.setattr(audit_scope, "_MIGRATION_DONE", False)
    conn = _WritableConn()
    asyncio.run(audit_scope.ensure_decision_audit_scope_columns(conn))
    assert conn.executed
    assert audit_scope._MIGRATION_DONE

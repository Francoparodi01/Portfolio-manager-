import asyncio
from datetime import datetime, timezone

import run as audit_run

UTC = timezone.utc


class Tx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None


class FakeConnection:
    def __init__(self):
        self.calls = []
        self.closed = False

    def transaction(self, **kwargs):
        assert kwargs == {"readonly": True, "isolation": "repeatable_read"}
        return Tx()

    async def fetchval(self, sql, *args):
        self.calls.append((sql, args))
        if "SELECT NOW()" in sql:
            return datetime(2026, 10, 2, 20, tzinfo=UTC)
        if "current_setting('transaction_read_only')" in sql:
            return "on"
        if "decision_lab_plan_captures" in sql:
            return False
        if "count(*)" in sql and "owner_chat_id IS NULL" in sql:
            return 0
        if "corporate_events" in sql and "to_regclass" in sql:
            return True
        raise AssertionError(sql)

    async def fetch(self, sql, *args):
        self.calls.append((sql, args))
        if "information_schema.columns" in sql:
            return [{"column_name": x} for x in sorted(audit_run.REQUIRED_DECISION_COLUMNS)]
        if "SELECT DISTINCT owner_chat_id FROM portfolio_snapshots" in sql:
            return [{"owner_chat_id": 123}]
        if "FROM execution_plans p" in sql and "JOIN order_intents" in sql:
            return []
        if "FROM decision_log" in sql and "GROUP BY 1,2,3" in sql:
            return []
        raise AssertionError(sql)

    async def fetchrow(self, sql, *args):
        self.calls.append((sql, args))
        if "formal_plans" in sql:
            return {
                "plan_rows": 0,
                "formal_plans": 0,
                "feasible_plans": 0,
                "explicit_owner_plans": 0,
                "null_owner_plans": 0,
            }
        if "portfolio_snapshots" in sql and "broker_fills" in sql:
            return {"snapshots": 0, "fills": 0}
        raise AssertionError(sql)

    async def close(self):
        self.closed = True


def test_loader_is_repeatable_read_select_only_and_never_queries_legacy_outcomes(monkeypatch):
    db = FakeConnection()

    async def connect(dsn, **kwargs):
        assert kwargs["server_settings"]["default_transaction_read_only"] == "on"
        assert kwargs["server_settings"]["statement_timeout"] == "20000"
        return db

    monkeypatch.setattr(audit_run.asyncpg, "connect", connect)
    monkeypatch.setenv("DATABASE_URL", "postgresql://test.invalid/test")

    result = asyncio.run(audit_run.load_raw(123, 180))
    assert result["readonly"] == "on"
    assert result["legacy_owner_verified"] is True
    assert db.closed is True

    sql_text = "\n".join(sql for sql, _ in db.calls).lower()
    assert "outcome_5d" not in sql_text
    assert "outcome_10d" not in sql_text
    assert "outcome_20d" not in sql_text
    assert "outcome_40d" not in sql_text

    for sql, _ in db.calls:
        assert sql.lstrip().upper().startswith("SELECT")


def test_owner_inference_requires_exact_single_explicit_owner():
    db = FakeConnection()

    async def multiple_owners(sql, *args):
        if "SELECT DISTINCT owner_chat_id FROM portfolio_snapshots" in sql:
            return [{"owner_chat_id": 123}, {"owner_chat_id": 456}]
        return await FakeConnection.fetch(db, sql, *args)

    db.fetch = multiple_owners
    verified, owners = asyncio.run(audit_run._owner_state(db, 123))
    assert verified is False
    assert owners == [123, 456]


def test_report_declares_formal_signal_source_separately_from_decision_log():
    assert "decision_log_used_as_formal_signal_source" not in audit_run.REQUIRED_DECISION_COLUMNS
    # The reconstruction module derives ticker/side from execution plan intents.
    from reconstruct import rows_for_confidence

    raw = [{
        "plan_id": "p1",
        "plan_run_id": "r1",
        "created_at": datetime(2026, 9, 1, 20, tzinfo=UTC),
        "plan_source": "execution_plan",
        "feasible": True,
        "intent_id": 1,
        "ticker": "NVDA",
        "side": "BUY",
        "is_executable": True,
        "was_blocked": False,
    }]
    episodes = [{"intent_id": 1, "confidence": "MEDIUM"}]
    selected = rows_for_confidence(raw, episodes, {"HIGH", "MEDIUM"})
    assert selected[0]["ticker"] == "NVDA"
    assert selected[0]["side"] == "BUY"

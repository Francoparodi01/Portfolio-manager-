import asyncio
import json
from datetime import datetime, timezone
from uuid import UUID

import pytest

from src.collector.schema_migrations import (
    EXECUTION_EVIDENCE_V2_SQL,
    EXECUTION_PLAN_PERSISTENCE_SQL,
    ensure_execution_plan_persistence,
)
from src.decision_lab.capture import capture_plan


UTC = timezone.utc
NOW = datetime(2026, 10, 4, 12, 0, tzinfo=UTC)
RUN_ID = UUID("11111111-2222-3333-4444-555555555555")
PLAN_ID = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")


class FakeCaptureConnection:
    def __init__(self, *, owner=123, run_id=RUN_ID, payload_version="execution-plan-v2-immutable"):
        self.owner = owner
        self.run_id = run_id
        self.payload_version = payload_version
        self.executed = []

    async def fetchval(self, sql, *args):
        if "to_regclass('public.decision_lab_plan_captures')" in sql:
            return "decision_lab_plan_captures"
        raise AssertionError(sql)

    async def fetchrow(self, sql, *args):
        if "FROM execution_plans" in sql:
            return {
                "owner_chat_id": self.owner,
                "run_id": self.run_id,
                "created_at": NOW,
                "payload_version": self.payload_version,
            }
        raise AssertionError(sql)

    async def execute(self, sql, *args):
        self.executed.append((sql, args))
        return "INSERT 0 1"


class FakeMigrationConnection:
    def __init__(self):
        self.executed = []

    async def execute(self, sql, *args):
        self.executed.append(sql)
        return "OK"


def test_schema_requires_owner_and_run_before_formal_plan_insert():
    sql = EXECUTION_EVIDENCE_V2_SQL
    assert "formal execution plan requires explicit owner_chat_id" in sql
    assert "formal execution plan requires run_id" in sql
    assert "BEFORE INSERT ON execution_plans" in sql
    assert "execution-plan-v2-immutable" in sql


def test_schema_creates_identity_capture_in_same_plan_insert_path():
    sql = EXECUTION_EVIDENCE_V2_SQL
    assert "AFTER INSERT ON execution_plans" in sql
    assert "INSERT INTO decision_lab_plan_captures" in sql
    assert "PERSISTENCE_IDENTITY" in sql
    assert "'run_id', NEW.run_id::text" in sql
    assert "'owner', NEW.owner_chat_id" in sql


def test_schema_locks_new_plan_and_order_intent_evidence():
    sql = EXECUTION_EVIDENCE_V2_SQL
    assert "BEFORE UPDATE OR DELETE ON execution_plans" in sql
    assert "BEFORE UPDATE OR DELETE ON order_intents" in sql
    assert "execution-plan-v2 evidence is immutable" in sql
    assert "order_intent for execution-plan-v2 evidence is immutable" in sql


def test_schema_does_not_rewrite_legacy_rows():
    sql = EXECUTION_EVIDENCE_V2_SQL
    assert "OLD.payload_version = 'execution-plan-v2-immutable'" in sql
    assert "UPDATE execution_plans SET" not in sql
    assert "UPDATE order_intents SET" not in sql


def test_ensure_execution_plan_persistence_installs_base_then_evidence_v2():
    conn = FakeMigrationConnection()
    asyncio.run(ensure_execution_plan_persistence(conn))
    assert conn.executed == [EXECUTION_PLAN_PERSISTENCE_SQL, EXECUTION_EVIDENCE_V2_SQL]


def test_full_context_capture_binds_owner_and_run_to_persisted_plan():
    conn = FakeCaptureConnection()
    result = asyncio.run(
        capture_plan(
            conn,
            plan_id=PLAN_ID,
            owner=123,
            decision_at=NOW,
            plan={"gate": "NORMAL", "feasible": True},
            portfolio={"owner_chat_id": 123, "positions": []},
            signals=[],
            macro={"vix": 15.0},
        )
    )

    assert result["status"] == "CAPTURED"
    assert result["run_id"] == str(RUN_ID)
    assert len(conn.executed) == 1

    _, args = conn.executed[0]
    assert args[1] == 123
    assert args[2] == str(PLAN_ID)
    payload = json.loads(args[4])
    assert payload["schema"] == "decision-lab-plan-capture-v2"
    assert payload["capture_kind"] == "FULL_CONTEXT"
    assert payload["owner"] == 123
    assert payload["run_id"] == str(RUN_ID)
    assert payload["payload_version"] == "execution-plan-v2-immutable"


def test_full_context_capture_fails_closed_on_cross_owner_plan():
    conn = FakeCaptureConnection(owner=456)
    with pytest.raises(ValueError, match="owner does not match"):
        asyncio.run(
            capture_plan(
                conn,
                plan_id=PLAN_ID,
                owner=123,
                decision_at=NOW,
                plan={"gate": "NORMAL"},
                portfolio={"owner_chat_id": 123},
                signals=[],
                macro={},
            )
        )
    assert conn.executed == []


def test_full_context_capture_fails_closed_when_run_id_is_missing():
    conn = FakeCaptureConnection(run_id=None)
    result = asyncio.run(
        capture_plan(
            conn,
            plan_id=PLAN_ID,
            owner=123,
            decision_at=NOW,
            plan={"gate": "NORMAL"},
            portfolio={"owner_chat_id": 123},
            signals=[],
            macro={},
        )
    )
    assert result == {
        "status": "INSUFFICIENT",
        "reason": "PERSISTED_RUN_ID_REQUIRED",
    }
    assert conn.executed == []

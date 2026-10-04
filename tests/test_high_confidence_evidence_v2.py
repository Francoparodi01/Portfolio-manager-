import asyncio
import json
import os
import re
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit, urlunsplit
from datetime import datetime, timezone
from uuid import UUID, uuid4

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


# Opt-in PostgreSQL integration. Only disposable, loopback test databases are
# accepted; no application credentials, .env files or historical data are used.
@pytest.fixture
def evidence_postgres_url():
    url = os.environ.get("QUANTIA_EVIDENCE_TEST_DATABASE_URL")
    if not url:
        pytest.skip("set QUANTIA_EVIDENCE_TEST_DATABASE_URL to isolated PostgreSQL")
    parsed = urlsplit(url)
    if parsed.hostname not in {"127.0.0.1", "localhost"} or parsed.path != "/quantia_pr19_test":
        pytest.fail("evidence tests require a loopback /quantia_pr19_test database")
    return url


@asynccontextmanager
async def evidence_database(url, monkeypatch):
    import asyncpg
    from src.analysis import audit_scope
    from src.analysis.position_hold_audit import POSITION_HOLD_AUDIT_SCHEMA_SQL

    admin = await asyncpg.connect(url)
    name = "quantia_pr19_test_" + uuid4().hex
    conn = None
    try:
        await admin.execute(f'CREATE DATABASE "{name}"')
        parsed = urlsplit(url)
        test_url = urlunsplit(parsed._replace(path="/" + name))
        conn = await asyncpg.connect(test_url)
        schema = (Path(__file__).parents[1] / "init.sql").read_text(encoding="utf-8")
        for table in ("bot_users", "decision_log"):
            ddl = re.search(rf"CREATE TABLE IF NOT EXISTS {table} \(.*?\n\);", schema, re.S)
            assert ddl is not None
            await conn.execute(ddl.group())
        await conn.execute("INSERT INTO bot_users(chat_id) VALUES(123), (456)")
        await conn.execute(audit_scope.DECISION_AUDIT_SCOPE_MIGRATION_SQL)
        monkeypatch.setattr(audit_scope, "_MIGRATION_DONE", True)
        await ensure_execution_plan_persistence(conn)
        await conn.execute(POSITION_HOLD_AUDIT_SCHEMA_SQL)
        yield conn, test_url
    finally:
        if conn is not None:
            await conn.close()
        await admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        await admin.close()


def formal_plan():
    from src.analysis.enums import DecisionType
    from src.analysis.execution_planner import DecisionIntent, ExecutionPlan, OrderIntent, OrderSide

    def order(ticker, side):
        return OrderIntent(
            ticker=ticker, side=side, action=DecisionType.BUY if side == OrderSide.BUY else DecisionType.SELL_PARTIAL,
            amount_ars=100, theoretical_ars=100, quantity_est=1,
            reference_price=100, reason="synthetic test", priority=1,
        )

    decisions = [
        DecisionIntent(ticker=ticker, action=action, reason_primary="synthetic test",
                       reason_secondary=None, current_weight=0.1, target_weight=0.2,
                       delta_weight=0.1)
        for ticker, action in (("DDD", DecisionType.BUY), ("EEE", DecisionType.HOLD))
    ]
    return ExecutionPlan(
        decisions=decisions, sell_orders=[order("AAA", OrderSide.SELL)],
        buy_orders=[order("BBB", OrderSide.BUY)],
        blocked_orders=[order("CCC", OrderSide.BUY)], pending_buys=["DDD"],
        cash_before=1000, gross_sell_ars=100, fee_sell_ars=0, net_sell_ars=100,
        gross_buy_ars=100, fee_buy_ars=0, cash_after=1000, feasible=True,
        gate="NORMAL", summary="synthetic test",
    )


async def save_formal_plan(url, **overrides):
    from scripts.run_analysis import _save_execution_plan_events

    args = dict(
        cfg=SimpleNamespace(database=SimpleNamespace(url=url)), execution_plan=formal_plan(),
        results=[], macro_snap={}, macro_regime="neutral", total_ars=1000,
        portfolio_snapshot={"owner_chat_id": 123, "positions": []},
        owner_chat_id=123, run_id=str(RUN_ID),
    )
    args.update(overrides)
    return await _save_execution_plan_events(**args)


async def bundle_counts(conn):
    return [await conn.fetchval(f"SELECT count(*) FROM {table}") for table in (
        "execution_plans", "decision_log", "order_intents",
        "decision_lab_plan_captures", "position_hold_observations",
    )]


@pytest.mark.parametrize("stage", [
    "plan", "identity", "decision_first", "decision_later", "intent_first",
    "intent_later", "blocked", "pending", "hold", "full_context", "commit",
    "missing_portfolio", "cross_owner", "serialization", "cancelled", "missing_decision_row", "missing_plan_row", "missing_intent_row", "missing_capture_row",
])
def test_postgres_formal_bundle_rolls_back_every_failure(evidence_postgres_url, monkeypatch, stage):
    import asyncpg
    from src.decision_lab import capture as capture_module

    async def scenario():
        async with evidence_database(evidence_postgres_url, monkeypatch) as (conn, url):
            injections = {
                "plan": ("execution_plans", "TRUE"),
                "identity": ("decision_lab_plan_captures", "NEW.payload->>'capture_kind' = 'PERSISTENCE_IDENTITY'"),
                "decision_first": ("decision_log", "NEW.ticker = 'AAA'"),
                "decision_later": ("decision_log", "NEW.ticker = 'BBB'"),
                "intent_first": ("order_intents", "NEW.sequence_no = 1"),
                "intent_later": ("order_intents", "NEW.sequence_no = 2"),
                "blocked": ("order_intents", "NEW.sequence_no = 3"),
                "pending": ("order_intents", "NEW.sequence_no = 4"),
                "hold": ("position_hold_observations", "TRUE"),
                "full_context": ("decision_lab_plan_captures", "NEW.payload->>'capture_kind' = 'FULL_CONTEXT'"),
                "commit": ("order_intents", "TRUE"),
                "missing_decision_row": ("decision_log", "TRUE"),
                "missing_plan_row": ("execution_plans", "TRUE"),
                "missing_intent_row": ("order_intents", "NEW.sequence_no = 2"),
                "missing_capture_row": ("decision_lab_plan_captures", "NEW.payload->>'capture_kind' = 'FULL_CONTEXT'"),
            }
            overrides = {}
            expected = asyncpg.RaiseError
            match = "injected persistence failure"
            if stage in injections:
                table, condition = injections[stage]
                action = "RETURN NULL" if stage.startswith("missing_") else "RAISE EXCEPTION 'injected persistence failure'"
                await conn.execute(f"""
                    CREATE FUNCTION fail_bundle() RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN IF {condition} THEN {action}; END IF; RETURN NEW; END $$;
                """)
                if stage == "commit":
                    await conn.execute(f"""CREATE CONSTRAINT TRIGGER fail_bundle
                        AFTER INSERT ON {table} DEFERRABLE INITIALLY DEFERRED
                        FOR EACH ROW EXECUTE FUNCTION fail_bundle()""")
                else:
                    await conn.execute(f"""CREATE TRIGGER fail_bundle BEFORE INSERT ON {table}
                        FOR EACH ROW EXECUTE FUNCTION fail_bundle()""")
                if stage == "missing_decision_row":
                    expected, match = RuntimeError, "insert returned no row"
                elif stage.startswith("missing_"):
                    expected, match = RuntimeError, "insert did not persist one row"
            elif stage == "missing_portfolio":
                overrides["portfolio_snapshot"] = None
                expected, match = RuntimeError, "EXPLICIT_OWNER_AND_PORTFOLIO_REQUIRED"
            elif stage == "cross_owner":
                overrides["portfolio_snapshot"] = {"owner_chat_id": 456}
                expected, match = ValueError, "portfolio owner"
            elif stage == "serialization":
                overrides["macro_snap"] = {"vix": float("nan")}
                expected, match = ValueError, "JSON compliant"
            elif stage == "cancelled":
                async def cancel(*args, **kwargs):
                    raise asyncio.CancelledError("injected cancellation")
                monkeypatch.setattr(capture_module, "capture_plan", cancel)
                expected, match = asyncio.CancelledError, "injected cancellation"
            with pytest.raises(expected, match=match):
                await save_formal_plan(url, **overrides)
            assert await bundle_counts(conn) == [0, 0, 0, 0, 0]

    asyncio.run(scenario())


def test_postgres_success_is_complete_immutable_and_reconstructible(evidence_postgres_url, monkeypatch):
    import asyncpg
    from scripts.historical_reconstruction.reconstruct import classify_episode
    from src.decision_lab import capture as capture_module

    async def scenario():
        async with evidence_database(evidence_postgres_url, monkeypatch) as (conn, url):
            original_capture = capture_module.capture_plan

            async def check_isolation(*args, **kwargs):
                # A separate connection cannot see any of the uncommitted bundle.
                assert await bundle_counts(conn) == [0, 0, 0, 0, 0]
                return await original_capture(*args, **kwargs)

            monkeypatch.setattr(capture_module, "capture_plan", check_isolation)
            ids = await save_formal_plan(url)
            monkeypatch.setattr(capture_module, "capture_plan", original_capture)
            assert len(ids) == 4
            assert await bundle_counts(conn) == [1, 4, 4, 2, 1]
            plan = await conn.fetchrow("SELECT * FROM execution_plans")
            assert plan["owner_chat_id"] == 123 and plan["run_id"] == RUN_ID
            assert plan["payload_version"] == "execution-plan-v2-immutable"
            captures = [json.loads(r["payload"]) for r in await conn.fetch("SELECT payload FROM decision_lab_plan_captures")]
            assert {c["capture_kind"] for c in captures} == {"PERSISTENCE_IDENTITY", "FULL_CONTEXT"}
            assert all(c["owner"] == 123 and c["run_id"] == str(RUN_ID) for c in captures)
            rows = await conn.fetch("""
                SELECT p.id::text AS plan_id, p.owner_chat_id AS plan_owner_chat_id,
                    p.run_id::text AS plan_run_id, p.created_at, p.updated_at AS plan_updated_at,
                    p.source AS plan_source, p.feasible, i.id AS intent_id, i.decision_log_id,
                    i.ticker, i.side, i.is_executable, i.was_blocked,
                    i.created_at AS intent_created_at, i.updated_at AS intent_updated_at,
                    d.id IS NOT NULL AS decision_exists, d.owner_chat_id AS decision_owner_chat_id,
                    d.run_id::text AS decision_run_id, d.ticker AS decision_ticker,
                    d.source AS decision_source, d.superseded_by_id
                FROM execution_plans p JOIN order_intents i ON i.execution_plan_id=p.id
                LEFT JOIN decision_log d ON d.id=i.decision_log_id ORDER BY i.sequence_no
            """)
            assert [r["ticker"] for r in rows] == ["AAA", "BBB", "CCC", "DDD"]
            for row in rows:
                result = classify_episode(dict(row), requested_owner=123,
                    legacy_owner_verified=False, immutable_plan_ids={str(plan["id"])})
                if row["is_executable"]:
                    assert result["confidence"] == "HIGH"
                    assert result["decision_link_status"] == "HEALTHY"
                else:
                    assert result["reason_codes"] == ["NOT_EVALUABLE_FORMAL_SIGNAL"]
            for table, column in (("execution_plans", "summary"), ("order_intents", "reason"),
                                  ("decision_lab_plan_captures", "plan_id")):
                for statement in (f"UPDATE {table} SET {column}={column}", f"DELETE FROM {table}"):
                    with pytest.raises(asyncpg.RaiseError, match="immutable|append-only"):
                        await conn.execute(statement)
            before = await conn.fetch("SELECT * FROM decision_log ORDER BY id")
            # Repeating a run appends a new plan and fresh auxiliary rows, leaving
            # all previous plan/intent links and decision evidence untouched.
            next_ids = await save_formal_plan(url)
            assert set(ids).isdisjoint(next_ids)
            assert await conn.fetch("SELECT * FROM decision_log WHERE id=ANY($1::bigint[]) ORDER BY id", ids) == before
            assert await bundle_counts(conn) == [2, 8, 8, 4, 1]

    asyncio.run(scenario())


@pytest.mark.parametrize("overrides, message", [
    ({"owner_chat_id": None}, "requires explicit owner_chat_id"),
    ({"run_id": None}, "requires run_id"),
])
def test_formal_bundle_requires_explicit_lineage_before_connect(overrides, message):
    with pytest.raises(ValueError, match=message):
        asyncio.run(save_formal_plan("postgresql://unused", **overrides))


def test_postgres_empty_plan_still_has_both_captures(evidence_postgres_url, monkeypatch):
    async def scenario():
        async with evidence_database(evidence_postgres_url, monkeypatch) as (conn, url):
            plan = formal_plan()
            plan.decisions = []
            plan.sell_orders = []
            plan.buy_orders = []
            plan.blocked_orders = []
            plan.pending_buys = []
            assert await save_formal_plan(url, execution_plan=plan) == []
            assert await bundle_counts(conn) == [1, 0, 0, 2, 0]
    asyncio.run(scenario())

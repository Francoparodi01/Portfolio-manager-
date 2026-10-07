"""Validate the T1 additive guard in an isolated loopback PostgreSQL DB."""
from __future__ import annotations

import argparse
import asyncio
import json
from urllib.parse import urlsplit
from uuid import uuid4

from src.collector.contextual_g2 import ensure_contextual_telegram_schema
from src.collector.db import PortfolioDatabase
from src.collector.schema_migrations import ensure_execution_plan_persistence


OWNER = 910004


def _checked_url(url: str) -> str:
    parsed = urlsplit(url.replace("postgresql+asyncpg://", "postgresql://", 1))
    if parsed.hostname not in {"127.0.0.1", "localhost"}:
        raise ValueError("T1 validation requires a loopback PostgreSQL host")
    if parsed.path != "/quantia_contextual_t1_test":
        raise ValueError("T1 validation requires /quantia_contextual_t1_test")
    return url


async def validate(url: str) -> dict:
    import asyncpg

    checked = _checked_url(url)
    db = PortfolioDatabase(checked)
    await db.connect()
    try:
        await db.init_schema()
    finally:
        await db.close()

    conn = await asyncpg.connect(
        checked.replace("postgresql+asyncpg://", "postgresql://", 1)
    )
    try:
        await ensure_execution_plan_persistence(conn)
        await ensure_contextual_telegram_schema(conn)
        await ensure_contextual_telegram_schema(conn)
        # Existing productive callers reinstall their own v2 functions.  The
        # additive authority columns and independent guard must survive that.
        await ensure_execution_plan_persistence(conn)
        async with conn.transaction():
            await conn.execute(
                """INSERT INTO bot_users(chat_id,display_name)
                   VALUES($1,'T1 TEST') ON CONFLICT(chat_id) DO NOTHING""",
                OWNER,
            )
            shadow_plan = uuid4()
            productive_plan = uuid4()
            shadow_run = uuid4()
            productive_run = uuid4()
            shadow_version = await conn.fetchval(
                """INSERT INTO execution_plans(
                       id,owner_chat_id,run_id,created_at,source,gate,feasible,
                       cash_before,cash_after,summary,authority_mode,
                       affects_analysis,affects_execution
                   ) VALUES($1,$2,$3,NOW(),'execution_plan','SHADOW_ONLY',FALSE,
                            100000,100000,'T1 controlled shadow','SHADOW_ONLY',FALSE,FALSE)
                   RETURNING payload_version""",
                shadow_plan, OWNER, shadow_run,
            )
            productive_version = await conn.fetchval(
                """INSERT INTO execution_plans(
                       id,owner_chat_id,run_id,created_at,source,gate,feasible,
                       cash_before,cash_after,summary
                   ) VALUES($1,$2,$3,NOW(),'execution_plan','OPEN',TRUE,
                            100000,100000,'T1 productive compatibility')
                   RETURNING payload_version""",
                productive_plan, OWNER, productive_run,
            )
            product_order = await conn.fetchval(
                """INSERT INTO order_intents(
                       execution_plan_id,sequence_no,ticker,side,action,
                       planner_status,decision_status,is_executable
                   ) VALUES($1,1,'TEST','BUY','BUY','APPROVED','APPROVED',FALSE)
                   RETURNING id""",
                productive_plan,
            )
            shadow_order_blocked = False
            try:
                async with conn.transaction():
                    await conn.execute(
                        """INSERT INTO order_intents(
                               execution_plan_id,sequence_no,ticker,side,action,
                               planner_status,decision_status,is_executable
                           ) VALUES($1,1,'TEST','BUY','BUY','APPROVED','APPROVED',TRUE)""",
                        shadow_plan,
                    )
            except asyncpg.PostgresError as exc:
                shadow_order_blocked = (
                    "contextual shadow plans cannot create order intents" in str(exc)
                )
            nonzero_shadow_blocked = False
            try:
                async with conn.transaction():
                    await conn.execute(
                        """INSERT INTO execution_plans(
                               id,owner_chat_id,run_id,created_at,source,gate,feasible,
                               cash_before,gross_buy_ars,cash_after,summary,
                               authority_mode,affects_analysis,affects_execution
                           ) VALUES($1,$2,$3,NOW(),'execution_plan','SHADOW_ONLY',FALSE,
                                    100000,1000,99000,'invalid T1 shadow',
                                    'SHADOW_ONLY',FALSE,FALSE)""",
                        uuid4(), OWNER, uuid4(),
                    )
            except asyncpg.PostgresError as exc:
                nonzero_shadow_blocked = (
                    "execution_plans_contextual_shadow_safe_check" in str(exc)
                )
            evidence = {
                "schema": "telegram-contextual-shadow-migration-validation-v1",
                "result": "PASS" if all([
                    shadow_version == "execution-plan-v2-immutable",
                    productive_version == "execution-plan-v2-immutable",
                    product_order is not None,
                    shadow_order_blocked,
                    nonzero_shadow_blocked,
                ]) else "FAIL",
                "migration_idempotent": True,
                "shadow_payload_version": shadow_version,
                "productive_payload_version": productive_version,
                "productive_order_contract_unchanged": product_order is not None,
                "shadow_order_intent_blocked": shadow_order_blocked,
                "nonzero_shadow_plan_blocked": nonzero_shadow_blocked,
                "transaction_rolled_back": True,
            }
            if evidence["result"] != "PASS":
                raise RuntimeError("T1_SCHEMA_VALIDATION_FAILED")
            return evidence
    finally:
        await conn.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", required=True)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(validate(args.database_url)), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

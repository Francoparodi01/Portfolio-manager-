"""Bounded E0 extraction. SELECT only, owner exact, no app startup/migrations.

Run using the existing runtime environment; stdout contains private account data.
Save outside git. Historical candles retrieved now are NOT a PIT replay.
"""
import asyncio
import json
import os

import asyncpg


async def extract(conn, owner):
    if owner <= 0:
        raise ValueError("positive explicit owner required")
    async with conn.transaction(isolation="repeatable_read", readonly=True):
        plan = await conn.fetchrow(
            "SELECT * FROM execution_plans WHERE owner_chat_id=$1 ORDER BY created_at DESC LIMIT 1", owner)
        if not plan:
            return {"status": "NO_OWNER_PLAN"}
        captures = await conn.fetch(
            "SELECT capture_hash,captured_at,payload FROM decision_lab_plan_captures "
            "WHERE owner_chat_id=$1 AND plan_id=$2 ORDER BY captured_at DESC LIMIT 4", owner, str(plan["id"]))
        decisions = await conn.fetch(
            "SELECT id,ticker,decision,final_score,confidence,layers,run_id,owner_chat_id "
            "FROM decision_log WHERE owner_chat_id=$1 AND run_id=$2 ORDER BY id LIMIT 100", owner, plan["run_id"])
        orders = await conn.fetch(
            "SELECT o.* FROM order_intents o JOIN execution_plans p ON p.id=o.execution_plan_id "
            "WHERE p.owner_chat_id=$1 AND p.id=$2 ORDER BY o.sequence_no LIMIT 100", owner, plan["id"])
        parsed_captures = [{**dict(c), "payload": json.loads(c["payload"])} for c in captures]
        snapshot = await conn.fetchrow(
            "SELECT * FROM portfolio_snapshots WHERE owner_chat_id=$1 AND scraped_at <= $2 "
            "ORDER BY scraped_at DESC LIMIT 1", owner, plan["created_at"])
        tickers = sorted({d["ticker"] for d in decisions} |
                         {s["ticker"] for c in parsed_captures for s in c["payload"].get("signals", [])})[:30]
        candles = {}
        for ticker in tickers:
            # Observation timestamp alone does not establish bar end/availability.
            rows = await conn.fetch(
                "SELECT * FROM market_candles WHERE ticker=$1 AND ts <= $2 AND interval='1d' "
                "ORDER BY ts DESC,source LIMIT 520", ticker, plan["created_at"])
            candles[ticker] = [dict(r) for r in rows]
        return {
            "scope": "RECORDED_RUN_WITH_CURRENT_DB_CANDLES_NOT_PIT_REPLAY",
            "read_only": await conn.fetchval("SHOW transaction_read_only"),
            "plan": dict(plan), "captures": parsed_captures,
            "decisions": [{**dict(d), "layers": json.loads(d["layers"])} for d in decisions],
            "orders": [dict(o) for o in orders],
            "nearest_prior_snapshot_not_assumed_origin": dict(snapshot) if snapshot else None,
            "candles": candles,
        }


async def main():
    owner = int(os.environ["TELEGRAM_CHAT_ID"])
    conn = await asyncpg.connect(
        os.environ["DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://"), timeout=8,
        server_settings={"default_transaction_read_only": "on", "statement_timeout": "8000"})
    try:
        print(json.dumps(await extract(conn, owner), default=str, allow_nan=False))
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())

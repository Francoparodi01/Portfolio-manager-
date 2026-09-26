"""Exercise additive PostgreSQL persistence in a transaction that is rolled back."""

import asyncio
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import sys
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.decision_lab.models import Evidence, Dataset, Session, Experiment, canonical
from src.decision_lab.strategies import current_spec
from src.decision_lab.runner import replay
from src.decision_lab.persistence import SCHEMA, persist_run


async def main():
    import asyncpg

    t = datetime(2026, 3, 3, 20, tzinfo=timezone.utc)
    records = []
    sessions = []

    def evidence(kind, payload, rid, at, owner=None):
        return Evidence(
            kind=kind,
            record_id=rid,
            effective_at=at,
            available_at=at,
            source="database-validation-fixture",
            quality="POINT_IN_TIME_SAFE",
            owner=owner,
            payload_json=canonical(payload),
        )

    records.append(
        evidence(
            "PORTFOLIO",
            {
                "cash_ars": 1000,
                "positions": [{"ticker": "TEST", "quantity": 10, "mark_ars": 100}],
            },
            "portfolio",
            t,
            123,
        )
    )
    records.append(
        evidence(
            "PLAN",
            {"complete": True, "cash_before": 1000, "feasible": True, "orders": []},
            "plan",
            t,
            123,
        )
    )
    for i in range(1, 6):
        close = t + timedelta(days=i)
        sessions.append(
            Session(
                session_id=str(i), open_at=close - timedelta(hours=6), close_at=close
            )
        )
        records.append(
            evidence(
                "BAR",
                {
                    "ticker": "TEST",
                    "session_id": str(i),
                    "open": 100,
                    "close": 101,
                    "price_mode": "RAW_AS_TRADED",
                },
                str(i),
                close,
            )
        )
    dataset = Dataset(
        records=tuple(records), sessions=tuple(sessions), calendar_version="fixture"
    )
    spec = current_spec(
        "recorded_plan_v1",
        registered_at=t,
        code_version="database-validation",
        config_hash="fixture",
    )
    run, _ = replay(
        dataset,
        requests=[{"as_of": t.isoformat(), "plan_id": "plan"}],
        owner=123,
        strategies=[spec],
        mode="RECORDED_PLAN_EVALUATION",
        evaluated_as_of=t + timedelta(days=6),
        experiment=Experiment(
            experiment_id="db-test",
            registered_at=t,
            horizons=(5,),
            bootstrap_resamples=100,
        ),
    )
    conn = await asyncpg.connect(os.environ["DATABASE_URL"])
    tx = conn.transaction()
    await tx.start()
    schema = "decision_lab_test_" + uuid4().hex
    try:
        await conn.execute('CREATE SCHEMA "' + schema + '"')
        await conn.execute('SET LOCAL search_path TO "' + schema + '"')
        await conn.execute(SCHEMA)
        await persist_run(conn, run)
        await persist_run(conn, run)
        assert await conn.fetchval("SELECT count(*) FROM decision_lab_runs") == 1
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM decision_lab_runs WHERE owner_chat_id=999"
            )
            == 0
        )
        objects = await conn.fetchval("SELECT count(*) FROM decision_lab_objects")
        for table in ("decision_lab_objects", "decision_lab_runs"):
            rejected = False
            try:
                async with conn.transaction():
                    await conn.execute("DELETE FROM " + table)
            except asyncpg.RaiseError:
                rejected = True
            assert rejected
        assert (
            await conn.fetchval("SELECT count(*) FROM decision_lab_objects") == objects
        )
        print(
            canonical(
                {
                    "status": "PASS",
                    "idempotent": True,
                    "cross_owner_rows": 0,
                    "mutation_rejected": True,
                    "objects": objects,
                    "cleanup": "TRANSACTION_ROLLBACK",
                }
            )
        )
    finally:
        await tx.rollback()
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())

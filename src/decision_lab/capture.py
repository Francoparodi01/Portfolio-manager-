"""Append-only capture at formal plan persistence; never changes the plan.

This is recorded decision evidence. It does not claim complete replay inputs:
raw market history, full universe, FX vintages and model registration remain
separate required evidence for historical policy reconstruction.
"""

from dataclasses import asdict, is_dataclass
from enum import Enum
from datetime import datetime, timezone
from pathlib import Path

from .models import canonical, digest


def clean(value):
    if is_dataclass(value):
        return clean(asdict(value))
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {str(k): clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [clean(v) for v in value]
    return value


async def capture_plan(
    conn, *, plan_id, owner, decision_at, plan, portfolio, signals, macro, events=()
):
    """Persist an immutable full-context capture linked to the stored plan/run.

    The execution-plan v2 database trigger already creates a minimal immutable
    identity capture atomically with the plan INSERT. This richer capture adds the
    portfolio, signals, macro and event context, but it must agree with the owner
    and run lineage already persisted in ``execution_plans``.
    """
    owner = owner or (portfolio or {}).get("owner_chat_id")
    if not owner or not portfolio:
        return {
            "status": "INSUFFICIENT",
            "reason": "EXPLICIT_OWNER_AND_PORTFOLIO_REQUIRED",
        }
    portfolio_owner = portfolio.get("owner_chat_id")
    if portfolio_owner is not None and int(portfolio_owner) != int(owner):
        raise ValueError("portfolio owner does not match capture owner")
    if not await conn.fetchval(
        "SELECT to_regclass('public.decision_lab_plan_captures')"
    ):
        return {"status": "INSUFFICIENT", "reason": "CAPTURE_SCHEMA_NOT_INITIALIZED"}

    persisted = await conn.fetchrow(
        """
        SELECT owner_chat_id, run_id, created_at, payload_version
        FROM execution_plans
        WHERE id=$1::uuid
        """,
        str(plan_id),
    )
    if not persisted:
        return {"status": "INSUFFICIENT", "reason": "PLAN_NOT_PERSISTED"}

    persisted_owner = persisted["owner_chat_id"]
    if persisted_owner is None or int(persisted_owner) != int(owner):
        raise ValueError("capture owner does not match persisted execution plan")
    if persisted["run_id"] is None:
        return {"status": "INSUFFICIENT", "reason": "PERSISTED_RUN_ID_REQUIRED"}

    captured_at = datetime.now(timezone.utc)
    root = Path(__file__).resolve().parents[2]
    sources = (
        "scripts/run_analysis.py",
        "src/analysis/execution_planner.py",
        "src/analysis/contextual_contracts.py",
        "src/analysis/contextual_market.py",
        "src/analysis/feature_snapshot.py",
        "src/collector/cocos_history.py",
        "src/collector/data/models.py",
        "src/collector/db.py",
        "src/analysis/versioning.py",
        "init.sql",
        "src/analysis/optimizer.py",
        "src/analysis/technical.py",
        "src/analysis/synthesis.py",
        "src/analysis/risk.py",
        "src/analysis/macro.py",
    )
    payload = {
        "schema": "decision-lab-plan-capture-v2",
        "capture_kind": "FULL_CONTEXT",
        "plan_id": str(plan_id),
        "owner": int(owner),
        "run_id": str(persisted["run_id"]),
        "decision_at": decision_at or persisted["created_at"],
        "captured_at": captured_at,
        "payload_version": persisted["payload_version"],
        "plan": clean(plan),
        "portfolio": clean(portfolio),
        "signals": clean(signals),
        "macro": clean(macro),
        "events": clean(events),
        "code_hashes": {
            name: digest((root / name).read_text(encoding="utf-8")) for name in sources
        },
        "scope": "RECORDED_PROPOSAL_NOT_FULL_HISTORICAL_POLICY_RECONSTRUCTION",
    }
    key = digest(payload)
    inserted = await conn.execute(
        """INSERT INTO decision_lab_plan_captures(capture_hash,owner_chat_id,plan_id,captured_at,payload)
        VALUES($1,$2,$3,$4,$5::jsonb) ON CONFLICT DO NOTHING""",
        key,
        int(owner),
        str(plan_id),
        captured_at,
        canonical(payload),
    )
    if inserted != "INSERT 0 1":
        raise RuntimeError("full-context capture insert did not persist one row")
    return {
        "status": "CAPTURED",
        "capture_hash": key,
        "run_id": str(persisted["run_id"]),
    }

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
    owner = owner or (portfolio or {}).get("owner_chat_id")
    if not owner or not portfolio:
        return {
            "status": "INSUFFICIENT",
            "reason": "EXPLICIT_OWNER_AND_PORTFOLIO_REQUIRED",
        }
    if not await conn.fetchval(
        "SELECT to_regclass('public.decision_lab_plan_captures')"
    ):
        return {"status": "INSUFFICIENT", "reason": "CAPTURE_SCHEMA_NOT_INITIALIZED"}
    captured_at = datetime.now(timezone.utc)
    root = Path(__file__).resolve().parents[2]
    sources = (
        "scripts/run_analysis.py",
        "src/analysis/execution_planner.py",
        "src/analysis/optimizer.py",
        "src/analysis/technical.py",
        "src/analysis/synthesis.py",
        "src/analysis/risk.py",
        "src/analysis/macro.py",
    )
    payload = {
        "schema": "decision-lab-plan-capture-v1",
        "plan_id": str(plan_id),
        "owner": int(owner),
        "decision_at": decision_at,
        "captured_at": captured_at,
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
    await conn.execute(
        """INSERT INTO decision_lab_plan_captures(capture_hash,owner_chat_id,plan_id,captured_at,payload)
        VALUES($1,$2,$3,$4,$5::jsonb) ON CONFLICT DO NOTHING""",
        key,
        int(owner),
        str(plan_id),
        captured_at,
        canonical(payload),
    )
    return {"status": "CAPTURED", "capture_hash": key}

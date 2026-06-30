from __future__ import annotations

from datetime import datetime, time
from typing import Any
from uuid import UUID
from zoneinfo import ZoneInfo

ART_TZ_NAME = "America/Argentina/Buenos_Aires"
ART_TZ = ZoneInfo(ART_TZ_NAME)

VALID_RUN_INTENTS = {
    "broker_sync",
    "scheduled_context",
    "formal_plan",
    "exploratory",
    "operational_audit",
}

VALID_DECISION_STAGES = {
    "idea",
    "pending_open",
    "approved_decision",
    "executed",
    "blocked",
    "expired",
    "superseded",
}

VALID_METRIC_SCOPES = {
    "primary",
    "planner_audit",
    "radar_audit",
    "blocked_audit",
    "debug",
}

_MIGRATION_DONE = False


DECISION_AUDIT_SCOPE_MIGRATION_SQL = """
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS run_id UUID;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS run_intent TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS decision_stage TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS metric_scope TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS is_primary_metric BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS superseded_by_id BIGINT REFERENCES decision_log(id) ON DELETE SET NULL;

UPDATE decision_log
SET
    run_intent = COALESCE(run_intent, CASE
        WHEN COALESCE(source, layers->>'source', '') IN ('broker_movement', 'broker_fill')
            THEN 'broker_sync'
        WHEN COALESCE(source, layers->>'source', '') = 'execution_plan'
            THEN 'formal_plan'
        WHEN COALESCE(source, layers->>'source', '') = 'radar'
            THEN 'scheduled_context'
        WHEN COALESCE(source, layers->>'source', '') = 'optimizer'
            THEN 'exploratory'
        ELSE 'exploratory'
    END),
    decision_stage = COALESCE(decision_stage, CASE
        WHEN COALESCE(status, '') IN ('EXECUTED', 'EXECUTED_MANUAL')
            THEN 'executed'
        WHEN COALESCE(status, '') = 'APPROVED'
             AND COALESCE(source, layers->>'source', '') = 'execution_plan'
             AND (
                (decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::time >= TIME '17:00'
                OR (decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::time < TIME '10:30'
             )
            THEN 'pending_open'
        WHEN COALESCE(status, '') = 'APPROVED'
            THEN 'approved_decision'
        WHEN COALESCE(status, '') = 'BLOCKED'
            THEN 'blocked'
        WHEN COALESCE(source, layers->>'source', '') = 'radar'
            THEN 'idea'
        ELSE 'idea'
    END),
    metric_scope = COALESCE(metric_scope, CASE
        WHEN (
            COALESCE(source, layers->>'source', '') IN ('broker_movement', 'broker_fill')
            AND COALESCE(status, '') IN ('EXECUTED', 'EXECUTED_MANUAL')
        ) OR (
            COALESCE(source, layers->>'source', '') = 'execution_plan'
            AND COALESCE(status, '') IN ('EXECUTED', 'EXECUTED_MANUAL')
        )
            THEN 'primary'
        WHEN COALESCE(source, layers->>'source', '') = 'execution_plan'
             AND COALESCE(status, '') = 'BLOCKED'
            THEN 'blocked_audit'
        WHEN COALESCE(source, layers->>'source', '') = 'execution_plan'
            THEN 'planner_audit'
        WHEN COALESCE(source, layers->>'source', '') = 'radar'
            THEN 'radar_audit'
        ELSE 'debug'
    END),
    is_primary_metric = CASE
        WHEN (
            COALESCE(source, layers->>'source', '') IN ('broker_movement', 'broker_fill')
            AND COALESCE(status, '') IN ('EXECUTED', 'EXECUTED_MANUAL')
        ) OR (
            COALESCE(source, layers->>'source', '') = 'execution_plan'
            AND COALESCE(status, '') IN ('EXECUTED', 'EXECUTED_MANUAL')
        )
            THEN TRUE
        ELSE FALSE
    END
WHERE
    run_intent IS NULL
    OR decision_stage IS NULL
    OR metric_scope IS NULL
    OR is_primary_metric IS DISTINCT FROM CASE
        WHEN (
            COALESCE(source, layers->>'source', '') IN ('broker_movement', 'broker_fill')
            AND COALESCE(status, '') IN ('EXECUTED', 'EXECUTED_MANUAL')
        ) OR (
            COALESCE(source, layers->>'source', '') = 'execution_plan'
            AND COALESCE(status, '') IN ('EXECUTED', 'EXECUTED_MANUAL')
        )
            THEN TRUE
        ELSE FALSE
    END;

CREATE INDEX IF NOT EXISTS idx_decision_log_metric_scope
    ON decision_log(metric_scope, decided_at DESC);

CREATE INDEX IF NOT EXISTS idx_decision_log_primary_metric
    ON decision_log(decided_at DESC)
    WHERE is_primary_metric = TRUE;
"""


def _norm(value: Any, default: str = "") -> str:
    return str(value if value is not None else default).strip().lower()


def _upper(value: Any, default: str = "") -> str:
    return str(value if value is not None else default).strip().upper()


def _needs_open_revalidation(decided_at: datetime | None) -> bool:
    dt = decided_at or datetime.now()
    if dt.tzinfo is not None:
        dt = dt.astimezone(ART_TZ)
    if dt.weekday() >= 5:
        return True
    local_time = dt.time()
    return local_time >= time(17, 0) or local_time < time(10, 30)


def is_art_business_day(value: datetime | None = None) -> bool:
    dt = value or datetime.now(ART_TZ)
    if dt.tzinfo is not None:
        dt = dt.astimezone(ART_TZ)
    return dt.weekday() < 5


def is_regular_market_session(value: datetime | None = None) -> bool:
    dt = value or datetime.now(ART_TZ)
    if dt.tzinfo is not None:
        dt = dt.astimezone(ART_TZ)
    if not is_art_business_day(dt):
        return False
    local_time = dt.time()
    return time(10, 30) <= local_time < time(17, 0)


def classify_decision_audit_scope(
    *,
    source: str | None,
    status: str | None,
    decision_type: str | None = None,
    decided_at: datetime | None = None,
    run_intent: str | None = None,
) -> dict[str, object]:
    src = _norm(source)
    st = _upper(status)
    dtype = _norm(decision_type)

    primary = (
        src in {"broker_movement", "broker_fill"} and st in {"EXECUTED", "EXECUTED_MANUAL"}
    ) or (
        src == "execution_plan" and st in {"EXECUTED", "EXECUTED_MANUAL"}
    )

    if src in {"broker_movement", "broker_fill"}:
        inferred_intent = "broker_sync"
        stage = "executed" if st in {"EXECUTED", "EXECUTED_MANUAL"} else "idea"
        scope = "primary" if primary else "debug"
    elif src == "execution_plan":
        inferred_intent = "formal_plan"
        if st in {"EXECUTED", "EXECUTED_MANUAL"}:
            stage = "executed"
            scope = "primary"
        elif st == "APPROVED":
            stage = "pending_open" if _needs_open_revalidation(decided_at) else "approved_decision"
            scope = "planner_audit"
        elif st == "BLOCKED" or dtype == "blocked":
            stage = "blocked"
            scope = "blocked_audit"
        else:
            stage = "idea"
            scope = "planner_audit"
    elif src == "radar":
        inferred_intent = "scheduled_context"
        stage = "idea"
        scope = "radar_audit"
    elif src == "optimizer":
        inferred_intent = "exploratory"
        stage = "idea"
        scope = "debug"
    else:
        inferred_intent = "exploratory"
        stage = "idea"
        scope = "debug"

    intent = _norm(run_intent or inferred_intent)
    if intent not in VALID_RUN_INTENTS:
        intent = inferred_intent

    if intent == "exploratory" and scope != "primary":
        scope = "debug"

    return {
        "run_intent": intent,
        "decision_stage": stage,
        "metric_scope": scope,
        "is_primary_metric": bool(primary),
    }


async def ensure_decision_audit_scope_columns(conn) -> None:
    global _MIGRATION_DONE
    if _MIGRATION_DONE:
        return
    await conn.execute(DECISION_AUDIT_SCOPE_MIGRATION_SQL)
    _MIGRATION_DONE = True


def run_id_to_db(value: UUID | str | None) -> str | None:
    return str(value) if value else None

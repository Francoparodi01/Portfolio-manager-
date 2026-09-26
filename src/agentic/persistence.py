from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg
from .read_only import connect_read_only


AGENT_AUDIT_SQL = """
CREATE TABLE IF NOT EXISTS agent_runs (
    id              UUID PRIMARY KEY,
    owner_chat_id   BIGINT,
    goal            TEXT NOT NULL,
    model           TEXT NOT NULL,
    status          TEXT NOT NULL,
    stop_reason     TEXT,
    max_steps       INTEGER NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ,
    final_answer    TEXT,
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_agent_runs_started_at
    ON agent_runs(started_at DESC);

CREATE INDEX IF NOT EXISTS idx_agent_runs_owner_started
    ON agent_runs(owner_chat_id, started_at DESC)
    WHERE owner_chat_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS agent_steps (
    id                  BIGSERIAL PRIMARY KEY,
    run_id              UUID NOT NULL REFERENCES agent_runs(id) ON DELETE CASCADE,
    step_no             INTEGER NOT NULL,
    decision_kind       TEXT NOT NULL,
    tool_name           TEXT,
    tool_arguments      JSONB NOT NULL DEFAULT '{}'::jsonb,
    rationale           TEXT,
    confidence          FLOAT,
    observation_ok      BOOLEAN,
    observation         TEXT,
    observation_sha256  TEXT,
    observation_cached  BOOLEAN NOT NULL DEFAULT FALSE,
    elapsed_ms          INTEGER NOT NULL DEFAULT 0,
    error               TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, step_no)
);

CREATE INDEX IF NOT EXISTS idx_agent_steps_run
    ON agent_steps(run_id, step_no);
"""


def _dsn_for_asyncpg(dsn: str) -> str:
    if dsn.startswith("postgresql+asyncpg://"):
        return "postgresql://" + dsn[len("postgresql+asyncpg://") :]
    return dsn


class AgentRunStore:
    def __init__(self, dsn: str) -> None:
        self.dsn = _dsn_for_asyncpg(dsn)

    async def _connect(self):
        return await asyncpg.connect(self.dsn, timeout=15, command_timeout=30)

    async def ensure_schema(self) -> None:
        conn = await self._connect()
        try:
            await conn.execute(AGENT_AUDIT_SQL)
        finally:
            await conn.close()

    async def recent_context(self, owner_chat_id: int, *, as_of: datetime | None = None,
                             namespace: str = "interactive") -> list[dict]:
        """Up to three user questions in this owner's latest 24h conversation.

        Never reuse assistant conclusions as market evidence. A reset/run in a
        new conversation is a boundary even when that run subsequently fails.
        Legacy runs without a conversation ID are deliberately not inherited.
        """
        if not owner_chat_id:
            raise ValueError("context requires an explicit owner")
        cutoff = as_of or datetime.now(timezone.utc)
        if cutoff.tzinfo is None:
            raise ValueError("context cutoff must be timezone aware")
        conn = await connect_read_only(self.dsn, command_timeout=30)
        try:
            rows = await conn.fetch("""
                WITH latest AS (
                    SELECT metadata->>'conversation_id' AS conversation_id
                    FROM agent_runs WHERE owner_chat_id=$1 AND started_at BETWEEN $2 AND $3
                      AND metadata->>'context_namespace'=$4
                    ORDER BY started_at DESC, id DESC LIMIT 1
                )
                SELECT id, goal, started_at, metadata->>'conversation_id' AS conversation_id
                FROM agent_runs
                WHERE owner_chat_id=$1 AND started_at BETWEEN $2 AND $3
                  AND metadata->>'context_namespace'=$4
                  AND metadata->>'conversation_id'=(SELECT conversation_id FROM latest)
                ORDER BY started_at DESC, id DESC LIMIT 3
                """, owner_chat_id, cutoff - timedelta(hours=24), cutoff, namespace)
            return [{"run_id": str(row["id"]), "goal": row["goal"],
                     "started_at": row["started_at"].isoformat(),
                     "conversation_id": row["conversation_id"]} for row in reversed(rows)]
        finally:
            await conn.close()

    async def start_run(
        self,
        *,
        run_id: str,
        owner_chat_id: int | None,
        goal: str,
        model: str,
        max_steps: int,
        started_at: datetime,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        conn = await self._connect()
        try:
            await conn.execute(
                """
                INSERT INTO agent_runs (
                    id, owner_chat_id, goal, model, status, max_steps,
                    started_at, metadata
                )
                VALUES ($1::uuid, $2, $3, $4, 'RUNNING', $5, $6, $7::jsonb)
                """,
                run_id,
                owner_chat_id,
                goal,
                model,
                max_steps,
                started_at,
                json.dumps(metadata or {}, ensure_ascii=False),
            )
        finally:
            await conn.close()

    async def record_step(
        self,
        *,
        run_id: str,
        step_no: int,
        decision_kind: str,
        tool_name: str | None,
        tool_arguments: dict[str, Any],
        rationale: str,
        confidence: float | None,
        observation_ok: bool | None,
        observation: str | None,
        observation_sha256: str | None,
        observation_cached: bool,
        elapsed_ms: int,
        error: str | None,
    ) -> None:
        conn = await self._connect()
        try:
            await conn.execute(
                """
                INSERT INTO agent_steps (
                    run_id, step_no, decision_kind, tool_name, tool_arguments,
                    rationale, confidence, observation_ok, observation,
                    observation_sha256, observation_cached, elapsed_ms, error
                )
                VALUES (
                    $1::uuid, $2, $3, $4, $5::jsonb,
                    $6, $7, $8, $9, $10, $11, $12, $13
                )
                """,
                run_id,
                step_no,
                decision_kind,
                tool_name,
                json.dumps(tool_arguments or {}, ensure_ascii=False),
                rationale,
                confidence,
                observation_ok,
                observation,
                observation_sha256,
                observation_cached,
                elapsed_ms,
                error,
            )
        finally:
            await conn.close()

    async def finish_run(
        self,
        *,
        run_id: str,
        status: str,
        stop_reason: str,
        final_answer: str,
        finished_at: datetime,
        metadata_patch: dict[str, Any] | None = None,
    ) -> None:
        conn = await self._connect()
        try:
            await conn.execute(
                """
                UPDATE agent_runs
                SET status = $2,
                    stop_reason = $3,
                    final_answer = $4,
                    finished_at = $5,
                    metadata = metadata || $6::jsonb
                WHERE id = $1::uuid
                """,
                run_id,
                status,
                stop_reason,
                final_answer,
                finished_at,
                json.dumps(metadata_patch or {}, ensure_ascii=False),
            )
        finally:
            await conn.close()

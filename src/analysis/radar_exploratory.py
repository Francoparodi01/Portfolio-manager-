"""Immutable audit trail for manual Telegram Radar runs."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import re
from typing import Any, Mapping, Sequence
from uuid import uuid4


USER_ACTION_FOLLOW = "FOLLOW"
USER_ACTION_DISMISS = "DISMISS"
RADAR_EXPLORATORY_PROTOCOL_VERSION = "radar-manual-exploratory-v1"


RADAR_EXPLORATORY_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS radar_exploratory_runs (
    run_id UUID PRIMARY KEY,
    owner_chat_id BIGINT NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL,
    scoring_version TEXT NOT NULL,
    protocol_version TEXT NOT NULL,
    source_command TEXT NOT NULL DEFAULT '/radar',
    report_hash TEXT NOT NULL,
    report_text TEXT NOT NULL,
    metric_scope TEXT NOT NULL DEFAULT 'exploratory'
        CHECK (metric_scope = 'exploratory'),
    is_primary_metric BOOLEAN NOT NULL DEFAULT FALSE
        CHECK (is_primary_metric = FALSE),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (owner_chat_id, report_hash, scoring_version)
);

CREATE TABLE IF NOT EXISTS radar_exploratory_candidates (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES radar_exploratory_runs(run_id) ON DELETE CASCADE,
    owner_chat_id BIGINT NOT NULL,
    ticker TEXT NOT NULL,
    rank_position INTEGER NOT NULL,
    radar_score FLOAT,
    edge FLOAT,
    risk_reward FLOAT,
    v3_tier TEXT,
    v3_classification TEXT,
    action_text TEXT,
    user_action TEXT CHECK (user_action IN ('FOLLOW', 'DISMISS')),
    user_action_at TIMESTAMPTZ,
    broker_fill_id BIGINT REFERENCES broker_fills(id) ON DELETE SET NULL,
    broker_fill_linked_at TIMESTAMPTZ,
    follow_match_status TEXT,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, ticker)
);

CREATE INDEX IF NOT EXISTS idx_radar_exploratory_owner_recent
    ON radar_exploratory_runs(owner_chat_id, generated_at DESC);
CREATE INDEX IF NOT EXISTS idx_radar_exploratory_candidate_owner
    ON radar_exploratory_candidates(owner_chat_id, ticker, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_radar_exploratory_follow_pending
    ON radar_exploratory_candidates(owner_chat_id, user_action_at)
    WHERE user_action = 'FOLLOW' AND broker_fill_id IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_radar_exploratory_broker_fill
    ON radar_exploratory_candidates(broker_fill_id)
    WHERE broker_fill_id IS NOT NULL;
"""


@dataclass(frozen=True, slots=True)
class RadarExploratoryCandidate:
    ticker: str
    rank_position: int
    radar_score: float | None
    edge: float | None
    risk_reward: float | None
    v3_tier: str | None
    v3_classification: str | None
    action_text: str | None
    evidence: dict[str, Any]


def parse_compact_radar_candidates(report: str) -> list[RadarExploratoryCandidate]:
    """Parse the exact compact report shown to the user."""
    text = str(report or "")
    legacy_pattern = re.compile(
        r"(?m)^(?P<rank>\d+)\.\s+.*?<b>(?P<ticker>[A-Z0-9.\-]+)</b>\s+"
        r"\|\s+score\s+<code>(?P<score>[^<]+)</code>\s+"
        r"\|\s+edge\s+<code>(?P<edge>[^<]+)</code>\s+"
        r"\|\s+R/R\s+(?P<rr>[^\s]+)x\s*$"
    )
    modern_pattern = re.compile(
        r"(?m)^(?P<rank>\d+)\.\s+.*?<b>(?P<ticker>[A-Z0-9.\-]+)</b>\s+·\s+"
        r"V3\s+<b>(?P<tier>[^<]+)</b>\s+\((?P<classification>[^)]+)\)\s*$"
        r"\n\s+Señal\s+<code>(?P<score>[^<]+)</code>\s+·\s+"
        r"R/R\s+<code>(?P<rr>[^<]+)</code>\s*$"
    )
    matches = list(legacy_pattern.finditer(text))
    modern = False
    if not matches:
        matches = list(modern_pattern.finditer(text))
        modern = True
    candidates: list[RadarExploratoryCandidate] = []
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        block = text[match.start():end]
        v3_match = None if modern else re.search(
            r"Compra V3:\s*<b>([^<]+)</b>\s*·\s*([^·\n]+)", block
        )
        action_match = re.search(
            r"(?:Idea radar|Revalidar idea|Próximo paso):\s*(.+)",
            block,
        )
        tier = match.group("tier") if modern else (
            v3_match.group(1) if v3_match else None
        )
        if tier and tier.strip().upper() == "RECHAZADA":
            tier = "REJECTED"
        classification = match.group("classification") if modern else (
            v3_match.group(2) if v3_match else None
        )
        candidates.append(
            RadarExploratoryCandidate(
                ticker=match.group("ticker").upper(),
                rank_position=int(match.group("rank")),
                radar_score=_optional_float(match.group("score")),
                edge=(None if modern else _optional_float(match.group("edge"))),
                risk_reward=_optional_float(match.group("rr")),
                v3_tier=(tier.strip().upper() if tier else None),
                v3_classification=(classification.strip() if classification else None),
                action_text=(action_match.group(1).strip() if action_match else None),
                evidence={"rendered_block": block.strip()},
            )
        )
    return candidates


def exploratory_callback(action: str, candidate_id: int) -> str:
    normalized = str(action or "").lower()
    if normalized not in {"follow", "dismiss"}:
        raise ValueError(f"Acción Radar exploratoria inválida: {action}")
    return f"re:{normalized}:{int(candidate_id)}"


def parse_exploratory_callback(value: str) -> tuple[str, int] | None:
    match = re.fullmatch(r"re:(follow|dismiss):(\d+)", str(value or ""))
    if not match:
        return None
    return match.group(1), int(match.group(2))


class RadarExploratoryStore:
    def __init__(self, pool: Any):
        self.pool = pool

    async def ensure_schema(self) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(RADAR_EXPLORATORY_SCHEMA_SQL)

    async def save_report(
        self,
        *,
        owner_chat_id: int,
        report_text: str,
        scoring_version: str,
        candidates: Sequence[RadarExploratoryCandidate],
        generated_at: datetime | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        await self.ensure_schema()
        owner = int(owner_chat_id)
        report = str(report_text or "")
        report_hash = hashlib.sha256(report.encode("utf-8")).hexdigest()
        generated = _aware(generated_at or datetime.now(timezone.utc))
        run_id = uuid4()
        inserted = False

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                saved_run_id = await conn.fetchval(
                    """
                    INSERT INTO radar_exploratory_runs (
                        run_id, owner_chat_id, generated_at, scoring_version,
                        protocol_version, report_hash, report_text, metadata
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
                    ON CONFLICT (owner_chat_id, report_hash, scoring_version)
                    DO NOTHING
                    RETURNING run_id
                    """,
                    run_id,
                    owner,
                    generated,
                    str(scoring_version or "unknown"),
                    RADAR_EXPLORATORY_PROTOCOL_VERSION,
                    report_hash,
                    report,
                    json.dumps(dict(metadata or {}), ensure_ascii=True, default=str),
                )
                if saved_run_id is None:
                    saved_run_id = await conn.fetchval(
                        """
                        SELECT run_id FROM radar_exploratory_runs
                        WHERE owner_chat_id=$1 AND report_hash=$2
                          AND scoring_version=$3
                        """,
                        owner,
                        report_hash,
                        str(scoring_version or "unknown"),
                    )
                    if saved_run_id is None:
                        raise RuntimeError("No se pudo recuperar el Radar exploratorio")
                else:
                    inserted = True
                    for candidate in candidates:
                        await conn.execute(
                            """
                            INSERT INTO radar_exploratory_candidates (
                                run_id, owner_chat_id, ticker, rank_position,
                                radar_score, edge, risk_reward, v3_tier,
                                v3_classification, action_text, evidence
                            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb)
                            ON CONFLICT (run_id, ticker) DO NOTHING
                            """,
                            saved_run_id,
                            owner,
                            candidate.ticker,
                            candidate.rank_position,
                            candidate.radar_score,
                            candidate.edge,
                            candidate.risk_reward,
                            candidate.v3_tier,
                            candidate.v3_classification,
                            candidate.action_text,
                            json.dumps(candidate.evidence, ensure_ascii=True, default=str),
                        )

                rows = await conn.fetch(
                    """
                    SELECT id, ticker, rank_position, radar_score, edge,
                           risk_reward, v3_tier, v3_classification,
                           action_text, user_action, follow_match_status
                    FROM radar_exploratory_candidates
                    WHERE run_id=$1
                    ORDER BY rank_position, id
                    """,
                    saved_run_id,
                )
        return {
            "run_id": str(saved_run_id),
            "inserted": inserted,
            "duplicate": not inserted,
            "candidates": [dict(row) for row in rows],
        }

    async def get_candidate(
        self,
        candidate_id: int,
        *,
        owner_chat_id: int,
    ) -> dict[str, Any] | None:
        await self.ensure_schema()
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT * FROM radar_exploratory_candidates
                WHERE id=$1 AND owner_chat_id=$2
                """,
                int(candidate_id),
                int(owner_chat_id),
            )
        return dict(row) if row is not None else None

    async def record_user_action(
        self,
        candidate_id: int,
        *,
        owner_chat_id: int,
        action: str,
    ) -> dict[str, Any] | None:
        normalized = str(action or "").upper()
        if normalized not in {USER_ACTION_FOLLOW, USER_ACTION_DISMISS}:
            raise ValueError(f"Acción Radar exploratoria inválida: {action}")
        await self.ensure_schema()
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE radar_exploratory_candidates
                SET user_action=$3,
                    user_action_at=NOW(),
                    follow_match_status=CASE
                        WHEN $3='FOLLOW' THEN 'AWAITING_BUY_FILL'
                        ELSE 'DISMISSED'
                    END,
                    updated_at=NOW()
                WHERE id=$1 AND owner_chat_id=$2 AND user_action IS NULL
                RETURNING *
                """,
                int(candidate_id),
                int(owner_chat_id),
                normalized,
            )
            action_recorded = row is not None
            if row is None:
                row = await conn.fetchrow(
                    """
                    SELECT * FROM radar_exploratory_candidates
                    WHERE id=$1 AND owner_chat_id=$2
                    """,
                    int(candidate_id),
                    int(owner_chat_id),
                )
        if row is None:
            return None
        result = dict(row)
        result["action_recorded"] = action_recorded
        result["action_conflict"] = bool(
            not action_recorded
            and str(result.get("user_action") or "").upper() != normalized
        )
        return result

    async def reconcile_followed_fills(
        self,
        *,
        owner_chat_id: int,
        max_calendar_days: int = 16,
    ) -> int:
        await self.ensure_schema()
        async with self.pool.acquire() as conn:
            setup_alerts_exist = bool(
                await conn.fetchval(
                    "SELECT to_regclass('public.radar_setup_alerts') IS NOT NULL"
                )
            )
            setup_exclusion = (
                """
                      AND NOT EXISTS (
                          SELECT 1 FROM radar_setup_alerts setup
                          WHERE setup.broker_fill_id=f.id
                      )
                """
                if setup_alerts_exist
                else ""
            )
            result = await conn.execute(
                f"""
                WITH eligible_pairs AS (
                    SELECT
                        c.id AS candidate_id,
                        f.id AS broker_fill_id,
                        ROW_NUMBER() OVER (
                            PARTITION BY f.id
                            ORDER BY c.user_action_at DESC, c.id DESC
                        ) AS fill_rank,
                        ROW_NUMBER() OVER (
                            PARTITION BY c.id
                            ORDER BY f.executed_at, f.id
                        ) AS candidate_rank
                    FROM radar_exploratory_candidates c
                    JOIN broker_fills f
                      ON f.owner_chat_id=c.owner_chat_id
                     AND UPPER(f.ticker)=UPPER(c.ticker)
                     AND f.side='BUY'
                     AND f.created_at >= c.user_action_at
                     AND CASE
                         WHEN f.executed_at_precision='date_only'
                         THEN (f.executed_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date
                              > (
                                  c.user_action_at
                                  AT TIME ZONE 'America/Argentina/Buenos_Aires'
                                )::date
                         ELSE f.executed_at >= c.user_action_at
                     END
                     AND (f.executed_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date
                         <= (c.user_action_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date
                            + $2::integer
                    WHERE c.owner_chat_id=$1
                      AND c.user_action='FOLLOW'
                      AND c.user_action_at IS NOT NULL
                      AND c.broker_fill_id IS NULL
                      AND NOT (COALESCE(f.raw_payload, '{{}}'::jsonb) ? 'superseded_by_real')
                      AND NOT EXISTS (
                          SELECT 1 FROM radar_exploratory_candidates linked
                          WHERE linked.broker_fill_id=f.id
                      )
                      {setup_exclusion}
                ), candidate_matches AS (
                    SELECT candidate_id, broker_fill_id
                    FROM eligible_pairs
                    WHERE fill_rank=1 AND candidate_rank=1
                )
                UPDATE radar_exploratory_candidates c
                SET broker_fill_id=candidate_matches.broker_fill_id,
                    broker_fill_linked_at=NOW(),
                    follow_match_status='MATCHED_OWNER_TICKER_TIME',
                    updated_at=NOW()
                FROM candidate_matches
                WHERE c.id=candidate_matches.candidate_id
                """,
                int(owner_chat_id),
                max(int(max_calendar_days), 1),
            )
        return _command_count(result)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _optional_float(value: Any) -> float | None:
    try:
        raw = str(value).strip().replace(",", ".").removesuffix("x")
        if raw in {"", "—", "-", "n/d"}:
            return None
        return float(raw)
    except (TypeError, ValueError):
        return None


def _command_count(value: Any) -> int:
    try:
        return int(str(value).split()[-1])
    except (TypeError, ValueError, IndexError):
        return 0


__all__ = [
    "RADAR_EXPLORATORY_PROTOCOL_VERSION",
    "RADAR_EXPLORATORY_SCHEMA_SQL",
    "RadarExploratoryCandidate",
    "RadarExploratoryStore",
    "USER_ACTION_DISMISS",
    "USER_ACTION_FOLLOW",
    "exploratory_callback",
    "parse_compact_radar_candidates",
    "parse_exploratory_callback",
]

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
from typing import Any

import asyncpg


REPORT_ARTIFACT_VERSION = os.getenv(
    "TELEGRAM_REPORT_ARTIFACT_VERSION",
    "telegram-reports-v1",
)
REPORT_ARTIFACT_MAX_AGE_SECONDS = max(
    60,
    int(os.getenv("TELEGRAM_REPORT_MAX_AGE_SECONDS", "900")),
)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
REPORT_RUNNERS = {
    "analysis": PROJECT_ROOT / "scripts" / "run_analysis.py",
    "radar": PROJECT_ROOT / "scripts" / "run_opportunity.py",
}

REPORT_ARTIFACTS_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS telegram_report_artifacts (
    report_type        TEXT NOT NULL,
    owner_chat_id      BIGINT NOT NULL,
    input_fingerprint  TEXT NOT NULL,
    artifact_version   TEXT NOT NULL,
    report_text        TEXT NOT NULL,
    generated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    portfolio_snapshot_id UUID,
    portfolio_at       TIMESTAMPTZ,
    market_data_at     TIMESTAMPTZ,
    candle_data_at     TIMESTAMPTZ,
    metadata           JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (report_type, owner_chat_id)
);

CREATE INDEX IF NOT EXISTS idx_telegram_report_artifacts_generated_at
    ON telegram_report_artifacts(generated_at DESC);
"""


@dataclass(frozen=True, slots=True)
class ReportInputs:
    fingerprint: str
    portfolio_snapshot_id: str | None
    portfolio_at: datetime | None
    market_data_at: datetime | None
    candle_data_at: datetime | None


def _db_url(dsn: str) -> str:
    return dsn.replace("postgresql+asyncpg://", "postgresql://")


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _market_bucket(value: datetime | None) -> str | None:
    utc_value = _as_utc(value)
    if utc_value is None:
        return None
    bucket_minute = (utc_value.minute // 15) * 15
    return utc_value.replace(minute=bucket_minute, second=0, microsecond=0).isoformat()


def _runner_fingerprint(report_type: str) -> str:
    path = REPORT_RUNNERS.get(str(report_type))
    if path is None or not path.exists():
        return "unknown"
    return hashlib.sha256(path.read_bytes()).hexdigest()[:16]


async def ensure_report_artifacts_schema(dsn: str) -> None:
    conn = await asyncpg.connect(_db_url(dsn))
    try:
        await conn.execute(REPORT_ARTIFACTS_SCHEMA_SQL)
    finally:
        await conn.close()


async def read_report_inputs(
    dsn: str,
    *,
    report_type: str,
    owner_chat_id: int,
) -> ReportInputs:
    conn = await asyncpg.connect(_db_url(dsn))
    try:
        row = await conn.fetchrow(
            """
            WITH latest_portfolio AS (
                SELECT snapshot_id::text AS snapshot_id, scraped_at
                FROM portfolio_snapshots
                WHERE owner_chat_id = $1 OR owner_chat_id IS NULL
                ORDER BY (owner_chat_id = $1) DESC, scraped_at DESC
                LIMIT 1
            )
            SELECT
                (SELECT snapshot_id FROM latest_portfolio) AS portfolio_snapshot_id,
                (SELECT scraped_at FROM latest_portfolio) AS portfolio_at,
                (
                    SELECT ts
                    FROM market_prices
                    ORDER BY ts DESC
                    LIMIT 1
                ) AS market_data_at,
                (
                    SELECT ts
                    FROM market_candles
                    ORDER BY ts DESC
                    LIMIT 1
                ) AS candle_data_at
            """,
            int(owner_chat_id),
        )
    finally:
        await conn.close()

    values = dict(row or {})
    payload = {
        "version": REPORT_ARTIFACT_VERSION,
        "report_type": str(report_type),
        "owner_chat_id": int(owner_chat_id),
        "runner_fingerprint": _runner_fingerprint(report_type),
        "portfolio_snapshot_id": values.get("portfolio_snapshot_id"),
        "market_bucket": _market_bucket(values.get("market_data_at")),
        "candle_data_at": _as_utc(values.get("candle_data_at")).isoformat()
        if values.get("candle_data_at")
        else None,
    }
    fingerprint = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return ReportInputs(
        fingerprint=fingerprint,
        portfolio_snapshot_id=values.get("portfolio_snapshot_id"),
        portfolio_at=values.get("portfolio_at"),
        market_data_at=values.get("market_data_at"),
        candle_data_at=values.get("candle_data_at"),
    )


async def load_report_artifact(
    dsn: str,
    *,
    report_type: str,
    owner_chat_id: int,
    market_open: bool,
    now: datetime | None = None,
) -> dict[str, Any] | None:
    inputs = await read_report_inputs(
        dsn,
        report_type=report_type,
        owner_chat_id=owner_chat_id,
    )
    conn = await asyncpg.connect(_db_url(dsn))
    try:
        row = await conn.fetchrow(
            """
            SELECT report_text, generated_at, portfolio_at, market_data_at,
                   candle_data_at, metadata
            FROM telegram_report_artifacts
            WHERE report_type = $1
              AND owner_chat_id = $2
              AND artifact_version = $3
              AND input_fingerprint = $4
            """,
            str(report_type),
            int(owner_chat_id),
            REPORT_ARTIFACT_VERSION,
            inputs.fingerprint,
        )
    except asyncpg.UndefinedTableError:
        return None
    finally:
        await conn.close()

    if not row:
        return None
    generated_at = _as_utc(row["generated_at"])
    current = _as_utc(now or datetime.now(timezone.utc))
    if market_open and generated_at and current:
        if current - generated_at > timedelta(seconds=REPORT_ARTIFACT_MAX_AGE_SECONDS):
            return None
    return dict(row)


async def save_report_artifact(
    dsn: str,
    *,
    report_type: str,
    owner_chat_id: int,
    report_text: str,
    metadata: dict[str, Any] | None = None,
) -> None:
    inputs = await read_report_inputs(
        dsn,
        report_type=report_type,
        owner_chat_id=owner_chat_id,
    )
    conn = await asyncpg.connect(_db_url(dsn))
    try:
        await conn.execute(
            """
            INSERT INTO telegram_report_artifacts (
                report_type, owner_chat_id, input_fingerprint, artifact_version,
                report_text, generated_at, portfolio_snapshot_id, portfolio_at,
                market_data_at, candle_data_at, metadata
            )
            VALUES ($1, $2, $3, $4, $5, NOW(), $6::uuid, $7, $8, $9, $10::jsonb)
            ON CONFLICT (report_type, owner_chat_id) DO UPDATE SET
                input_fingerprint = EXCLUDED.input_fingerprint,
                artifact_version = EXCLUDED.artifact_version,
                report_text = EXCLUDED.report_text,
                generated_at = EXCLUDED.generated_at,
                portfolio_snapshot_id = EXCLUDED.portfolio_snapshot_id,
                portfolio_at = EXCLUDED.portfolio_at,
                market_data_at = EXCLUDED.market_data_at,
                candle_data_at = EXCLUDED.candle_data_at,
                metadata = EXCLUDED.metadata
            """,
            str(report_type),
            int(owner_chat_id),
            inputs.fingerprint,
            REPORT_ARTIFACT_VERSION,
            report_text,
            inputs.portfolio_snapshot_id,
            inputs.portfolio_at,
            inputs.market_data_at,
            inputs.candle_data_at,
            json.dumps(metadata or {}, ensure_ascii=True),
        )
    finally:
        await conn.close()


__all__ = [
    "REPORT_ARTIFACTS_SCHEMA_SQL",
    "REPORT_ARTIFACT_MAX_AGE_SECONDS",
    "REPORT_ARTIFACT_VERSION",
    "ensure_report_artifacts_schema",
    "load_report_artifact",
    "read_report_inputs",
    "save_report_artifact",
]

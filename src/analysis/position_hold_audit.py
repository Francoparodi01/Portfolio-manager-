"""Audit-only persistence and outcome resolution for final planner HOLDs."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Sequence
from uuid import UUID
from zoneinfo import ZoneInfo

from src.analysis.plan_follow_attribution import (
    compute_execution_session_outcomes,
    fetch_canonical_outcome_candles,
    fetch_outcome_corporate_effects,
)


POSITION_HOLD_AUDIT_VERSION = "position-hold-sessions-v1"
ART_TZ = ZoneInfo("America/Argentina/Buenos_Aires")

POSITION_HOLD_AUDIT_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS position_hold_observations (
    id                    BIGSERIAL PRIMARY KEY,
    owner_chat_id         BIGINT REFERENCES bot_users(chat_id) ON DELETE SET NULL,
    run_id                UUID NOT NULL,
    execution_plan_id     UUID REFERENCES execution_plans(id) ON DELETE SET NULL,
    observed_at           TIMESTAMPTZ NOT NULL,
    observed_session      DATE NOT NULL,
    ticker                TEXT NOT NULL,
    action                TEXT NOT NULL DEFAULT 'HOLD' CHECK (action = 'HOLD'),
    status                TEXT NOT NULL DEFAULT 'OBSERVED',
    source                TEXT NOT NULL DEFAULT 'position_analysis',
    metric_scope          TEXT NOT NULL DEFAULT 'hold_audit',
    final_score           FLOAT,
    confidence            FLOAT,
    reference_price       NUMERIC(20,4),
    current_weight        FLOAT,
    target_weight         FLOAT,
    delta_weight          FLOAT,
    reason_primary        TEXT,
    reason_secondary      TEXT,
    regime                TEXT,
    layers                JSONB NOT NULL DEFAULT '{}'::jsonb,
    portfolio_snapshot_id TEXT,
    outcome_5d            FLOAT,
    outcome_10d           FLOAT,
    outcome_20d           FLOAT,
    outcome_40d           FLOAT,
    outcome_date_5d       DATE,
    outcome_date_10d      DATE,
    outcome_date_20d      DATE,
    outcome_date_40d      DATE,
    outcome_price_5d      NUMERIC(20,4),
    outcome_price_10d     NUMERIC(20,4),
    outcome_price_20d     NUMERIC(20,4),
    outcome_price_40d     NUMERIC(20,4),
    outcome_basis         TEXT,
    outcome_version       TEXT,
    outcome_filled_at     TIMESTAMPTZ,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, ticker)
);

CREATE INDEX IF NOT EXISTS idx_position_hold_owner_observed
    ON position_hold_observations(owner_chat_id, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_position_hold_ticker_observed
    ON position_hold_observations(ticker, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_position_hold_pending
    ON position_hold_observations(observed_at, id)
    WHERE reference_price > 0 AND outcome_40d IS NULL;
"""


async def ensure_position_hold_audit_schema(conn: Any) -> None:
    await conn.execute(POSITION_HOLD_AUDIT_SCHEMA_SQL)


async def persist_position_holds(
    conn: Any,
    *,
    owner_chat_id: int | None,
    run_id: str | UUID,
    execution_plan_id: str | UUID,
    observed_at: datetime,
    observations: Sequence[Mapping[str, Any]],
) -> int:
    """Persist final planner HOLDs without writing to decision_log or orders."""
    if not observations:
        return 0
    await ensure_position_hold_audit_schema(conn)
    normalized_run_id = UUID(str(run_id))
    normalized_plan_id = UUID(str(execution_plan_id))
    aware_observed_at = _aware_datetime(observed_at)
    observed_session = aware_observed_at.astimezone(ART_TZ).date()
    saved = 0

    for raw in observations:
        ticker = str(raw.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        result = await conn.execute(
            """
            INSERT INTO position_hold_observations (
                owner_chat_id, run_id, execution_plan_id,
                observed_at, observed_session, ticker,
                final_score, confidence, reference_price,
                current_weight, target_weight, delta_weight,
                reason_primary, reason_secondary, regime, layers,
                portfolio_snapshot_id
            ) VALUES (
                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16::jsonb,$17
            )
            ON CONFLICT (run_id, ticker) DO NOTHING
            """,
            owner_chat_id,
            normalized_run_id,
            normalized_plan_id,
            aware_observed_at,
            observed_session,
            ticker,
            _optional_float(raw.get("final_score")),
            _optional_float(raw.get("confidence")),
            _positive_float(raw.get("reference_price")),
            _optional_float(raw.get("current_weight")),
            _optional_float(raw.get("target_weight")),
            _optional_float(raw.get("delta_weight")),
            _optional_text(raw.get("reason_primary")),
            _optional_text(raw.get("reason_secondary")),
            _optional_text(raw.get("regime")),
            json.dumps(dict(raw.get("layers") or {}), default=str),
            _optional_text(raw.get("portfolio_snapshot_id")),
        )
        if str(result).endswith(" 1"):
            saved += 1
    return saved


async def resolve_position_hold_outcomes(
    conn: Any,
    *,
    lookback_days: int = 180,
    owner_chat_id: int | None = None,
) -> int:
    """Resolve raw held returns at exact 5/10/20/40 trading sessions."""
    await ensure_position_hold_audit_schema(conn)
    rows = await conn.fetch(
        """
        SELECT id, ticker, observed_at, reference_price,
               outcome_5d, outcome_10d, outcome_20d, outcome_40d
        FROM position_hold_observations
        WHERE observed_at >= NOW() - ($1::int * INTERVAL '1 day')
          AND observed_at <= NOW() - INTERVAL '5 days'
          AND reference_price > 0
          AND ($2::bigint IS NULL OR owner_chat_id = $2)
          AND (
                outcome_5d IS NULL
             OR outcome_10d IS NULL
             OR outcome_20d IS NULL
             OR outcome_40d IS NULL
          )
        ORDER BY observed_at, id
        """,
        max(1, int(lookback_days)),
        owner_chat_id,
    )
    if not rows:
        return 0

    tickers = sorted({str(row["ticker"]).upper() for row in rows})
    earliest = min(_aware_datetime(row["observed_at"]).date() for row in rows)
    since = earliest - timedelta(days=7)
    candles_by_ticker = await fetch_canonical_outcome_candles(
        conn,
        tickers,
        since=since,
    )
    effects_by_ticker = await fetch_outcome_corporate_effects(
        conn,
        tickers,
        since=since,
    )
    updated = 0

    for row in rows:
        ticker = str(row["ticker"]).upper()
        outcomes = compute_execution_session_outcomes(
            execution_price=float(row["reference_price"]),
            executed_at=_aware_datetime(row["observed_at"]),
            side="BUY",
            candles=candles_by_ticker.get(ticker, ()),
            corporate_effects=effects_by_ticker.get(ticker, ()),
        )
        if not outcomes:
            continue
        values = {
            horizon: outcomes.get(f"outcome_{horizon}d")
            for horizon in (5, 10, 20, 40)
        }
        if not any(value is not None for value in values.values()):
            continue
        result = await conn.execute(
            """
            UPDATE position_hold_observations SET
                outcome_5d = COALESCE(outcome_5d, $2),
                outcome_10d = COALESCE(outcome_10d, $3),
                outcome_20d = COALESCE(outcome_20d, $4),
                outcome_40d = COALESCE(outcome_40d, $5),
                outcome_date_5d = COALESCE(outcome_date_5d, $6),
                outcome_date_10d = COALESCE(outcome_date_10d, $7),
                outcome_date_20d = COALESCE(outcome_date_20d, $8),
                outcome_date_40d = COALESCE(outcome_date_40d, $9),
                outcome_price_5d = COALESCE(outcome_price_5d, $10),
                outcome_price_10d = COALESCE(outcome_price_10d, $11),
                outcome_price_20d = COALESCE(outcome_price_20d, $12),
                outcome_price_40d = COALESCE(outcome_price_40d, $13),
                outcome_basis = 'canonical_cocos',
                outcome_version = $14,
                outcome_filled_at = NOW(),
                updated_at = NOW()
            WHERE id = $1
            """,
            int(row["id"]),
            values[5],
            values[10],
            values[20],
            values[40],
            outcomes.get("outcome_date_5d"),
            outcomes.get("outcome_date_10d"),
            outcomes.get("outcome_date_20d"),
            outcomes.get("outcome_date_40d"),
            outcomes.get("outcome_price_5d"),
            outcomes.get("outcome_price_10d"),
            outcomes.get("outcome_price_20d"),
            outcomes.get("outcome_price_40d"),
            POSITION_HOLD_AUDIT_VERSION,
        )
        if str(result).endswith(" 1"):
            updated += 1
    return updated


def hold_observations_from_plan(
    decisions: Iterable[Any],
    *,
    result_by_ticker: Mapping[str, Any],
    price_by_ticker: Mapping[str, float],
    regime: str,
    portfolio_snapshot_id: str | None,
    layers_by_ticker: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Extract only final planner HOLD decisions into persistence payloads."""
    observations: list[dict[str, Any]] = []
    for decision in decisions:
        action = str(
            getattr(getattr(decision, "action", None), "value", getattr(decision, "action", ""))
            or ""
        ).upper()
        if action != "HOLD":
            continue
        ticker = str(getattr(decision, "ticker", "") or "").upper().strip()
        if not ticker:
            continue
        result = result_by_ticker.get(ticker)
        score = getattr(decision, "score", None)
        if score is None and result is not None:
            score = getattr(result, "final_score", None)
        confidence = getattr(decision, "conviction", None)
        if confidence is None and result is not None:
            confidence = getattr(result, "conviction", getattr(result, "confidence", None))
        observations.append({
            "ticker": ticker,
            "final_score": score,
            "confidence": confidence,
            "reference_price": price_by_ticker.get(ticker),
            "current_weight": getattr(decision, "current_weight", None),
            "target_weight": getattr(decision, "target_weight", None),
            "delta_weight": getattr(decision, "delta_weight", None),
            "reason_primary": getattr(decision, "reason_primary", None),
            "reason_secondary": getattr(decision, "reason_secondary", None),
            "regime": regime,
            "layers": dict(layers_by_ticker.get(ticker) or {}),
            "portfolio_snapshot_id": portfolio_snapshot_id,
        })
    return observations


def _aware_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _positive_float(value: Any) -> float | None:
    parsed = _optional_float(value)
    return parsed if parsed is not None and parsed > 0 else None


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


__all__ = [
    "POSITION_HOLD_AUDIT_SCHEMA_SQL",
    "POSITION_HOLD_AUDIT_VERSION",
    "ensure_position_hold_audit_schema",
    "hold_observations_from_plan",
    "persist_position_holds",
    "resolve_position_hold_outcomes",
]

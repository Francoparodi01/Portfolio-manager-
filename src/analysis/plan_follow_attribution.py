"""Persisted plan -> real movement attribution for followed-plan viability.

This layer is derived audit data. It never changes plans, movements, outcomes,
scoring, or execution behavior.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from src.collector.schema_migrations import PLAN_EXECUTION_ATTRIBUTION_SQL
from src.core.market_calendar import is_trading_day


ART = ZoneInfo("America/Argentina/Buenos_Aires")
MATCHING_VERSION = "plan-follow-v1"
FOLLOW_STATUSES = {"PARTIAL", "FOLLOWED", "OVERFOLLOWED"}


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return default if value is None else float(value)
    except (TypeError, ValueError):
        return default


def _as_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _as_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.astimezone(ART).date()
    if isinstance(value, date):
        return value
    if not value:
        return None
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _sessions_between(start: date, end: date) -> int:
    if end < start:
        return -1
    sessions = 0
    cursor = start
    while cursor < end:
        cursor += timedelta(days=1)
        if is_trading_day(cursor):
            sessions += 1
    return sessions


def _follow_status(ratio: float) -> str | None:
    if ratio >= 1.35:
        return "OVERFOLLOWED"
    if ratio >= 0.75:
        return "FOLLOWED"
    if ratio >= 0.15:
        return "PARTIAL"
    return None


def _movement_amount(row: Mapping[str, Any]) -> float:
    amount = _as_float(row.get("amount"))
    if amount:
        return abs(amount)
    return abs(_as_float(row.get("quantity")) * _as_float(row.get("price")))


def canonicalize_movements(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Prefer real Cocos rows over same-day synthetic fallback duplicates."""
    materialized = [dict(row) for row in rows]
    groups: dict[tuple[date | None, str, str], list[dict[str, Any]]] = {}
    for row in materialized:
        executed_at = _as_datetime(row.get("executed_at"))
        key = (
            executed_at.astimezone(ART).date() if executed_at else None,
            str(row.get("ticker") or "").upper(),
            str(row.get("movement_type") or row.get("side") or "").upper(),
        )
        groups.setdefault(key, []).append(row)

    canonical: list[dict[str, Any]] = []
    for group in groups.values():
        real = [
            row
            for row in group
            if not str(row.get("external_movement_id") or "").startswith("synthetic:")
        ]
        canonical.extend(real or group)
    return canonical


def _movement_matches_plan(
    plan: Mapping[str, Any],
    movement: Mapping[str, Any],
    *,
    match_window_sessions: int,
) -> bool:
    match_start = _as_datetime(plan.get("match_start_at"))
    executed_at = _as_datetime(movement.get("executed_at"))
    if match_start is None or executed_at is None:
        return False

    precision = str(movement.get("executed_at_precision") or "unknown").lower()
    executed_day = executed_at.astimezone(ART).date()
    match_day = _as_date(plan.get("match_day")) or match_start.astimezone(ART).date()
    sessions = _sessions_between(match_day, executed_day)
    if sessions < 0 or sessions > max(1, int(match_window_sessions)):
        return False
    if precision == "date_only":
        return True
    return executed_at >= match_start


def _sequence_quality(plan: Mapping[str, Any], movement: Mapping[str, Any]) -> str:
    decided_at = _as_datetime(plan.get("decided_at"))
    executed_at = _as_datetime(movement.get("executed_at"))
    if decided_at is None or executed_at is None:
        return "AMBIGUOUS_SAME_DAY"

    plan_day = decided_at.astimezone(ART).date()
    movement_day = executed_at.astimezone(ART).date()
    if movement_day > plan_day:
        return "CONFIRMED_SEQUENCE"
    if movement_day < plan_day:
        return "AMBIGUOUS_SAME_DAY"

    precision = str(movement.get("executed_at_precision") or "unknown").lower()
    if precision == "observed_after":
        window_start = _as_datetime(movement.get("observation_window_start_at"))
        if window_start is not None and decided_at <= window_start:
            return "CONFIRMED_SEQUENCE"
        return "AMBIGUOUS_SAME_DAY"
    if precision != "date_only" and executed_at >= decided_at:
        return "CONFIRMED_SEQUENCE"
    return "AMBIGUOUS_SAME_DAY"


def _candidate_for_plan(
    plan: Mapping[str, Any],
    movements: list[dict[str, Any]],
    *,
    match_window_sessions: int,
) -> dict[str, Any] | None:
    target = abs(_as_float(plan.get("target_amount_ars")))
    if target <= 0:
        return None

    ticker = str(plan.get("ticker") or "").upper()
    side = str(plan.get("decision") or "").upper()
    matched = [
        movement
        for movement in movements
        if str(movement.get("ticker") or "").upper() == ticker
        and str(movement.get("movement_type") or movement.get("side") or "").upper() == side
        and _movement_matches_plan(
            plan,
            movement,
            match_window_sessions=match_window_sessions,
        )
    ]
    if not matched:
        return None

    amount = sum(_movement_amount(row) for row in matched)
    ratio = amount / target
    status = _follow_status(ratio)
    if status is None:
        return None

    first_movement = min(
        matched,
        key=lambda row: _as_datetime(row.get("executed_at"))
        or datetime.max.replace(tzinfo=ART),
    )
    return {
        "plan": dict(plan),
        "movements": matched,
        "movement_ids": {int(row["id"]) for row in matched},
        "target_amount_ars": target,
        "matched_amount_ars": amount,
        "follow_ratio": ratio,
        "follow_status": status,
        "temporal_quality": _sequence_quality(plan, first_movement),
    }


def _candidate_components(candidates: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    parent = list(range(len(candidates)))

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left: int, right: int) -> None:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    for left in range(len(candidates)):
        for right in range(left + 1, len(candidates)):
            same_owner = (
                candidates[left]["plan"].get("owner_chat_id")
                == candidates[right]["plan"].get("owner_chat_id")
            )
            if same_owner and candidates[left]["movement_ids"] & candidates[right]["movement_ids"]:
                union(left, right)

    groups: dict[int, list[dict[str, Any]]] = {}
    for index, candidate in enumerate(candidates):
        groups.setdefault(find(index), []).append(candidate)
    return list(groups.values())


def normalize_plan_execution_attributions(
    plans: Iterable[Mapping[str, Any]],
    movements: Iterable[Mapping[str, Any]],
    *,
    match_window_sessions: int = 2,
    matching_version: str = MATCHING_VERSION,
) -> list[dict[str, Any]]:
    movement_input = [dict(row) for row in movements]
    canonical_movements = canonicalize_movements(movement_input)
    candidates = [
        candidate
        for plan in plans
        for candidate in [
            _candidate_for_plan(
                plan,
                canonical_movements,
                match_window_sessions=match_window_sessions,
            )
        ]
        if candidate is not None
    ]

    attributions: list[dict[str, Any]] = []
    for component in _candidate_components(candidates):
        movement_by_id = {
            int(movement["id"]): movement
            for candidate in component
            for movement in candidate["movements"]
        }
        movement_rows = list(movement_by_id.values())
        first_movement = min(
            movement_rows,
            key=lambda row: _as_datetime(row.get("executed_at"))
            or datetime.max.replace(tzinfo=ART),
        )

        strict_candidates = [
            candidate
            for candidate in component
            if _sequence_quality(candidate["plan"], first_movement) == "CONFIRMED_SEQUENCE"
        ]
        representative = max(
            strict_candidates or component,
            key=lambda candidate: _as_datetime(candidate["plan"].get("decided_at"))
            or datetime.min.replace(tzinfo=ART),
        )
        temporal_quality = (
            "CONFIRMED_SEQUENCE" if strict_candidates else "AMBIGUOUS_SAME_DAY"
        )
        executed_amount = sum(_movement_amount(row) for row in movement_rows)
        target_amount = representative["target_amount_ars"]
        follow_ratio = executed_amount / target_amount
        follow_status = _follow_status(follow_ratio)
        if follow_status not in FOLLOW_STATUSES:
            continue

        movement_ids = sorted(movement_by_id)
        key_source = (
            f"{str(representative['plan'].get('ticker') or '').upper()}|"
            f"{str(representative['plan'].get('decision') or '').upper()}|"
            + ",".join(str(value) for value in movement_ids)
        )
        attribution_key = hashlib.sha256(key_source.encode("utf-8")).hexdigest()
        precisions = {
            str(row.get("executed_at_precision") or "unknown") for row in movement_rows
        }
        sources = {str(row.get("executed_at_source") or "unknown") for row in movement_rows}
        executed_at = _as_datetime(first_movement.get("executed_at"))
        plan = representative["plan"]

        attributions.append(
            {
                "attribution_key": attribution_key,
                "representative_decision_log_id": int(plan["id"]),
                "owner_chat_id": plan.get("owner_chat_id"),
                "ticker": str(plan.get("ticker") or "").upper(),
                "side": str(plan.get("decision") or "").upper(),
                "plan_decided_at": _as_datetime(plan.get("decided_at")),
                "executed_at": executed_at,
                "executed_at_precision": next(iter(precisions)) if len(precisions) == 1 else "mixed",
                "executed_at_source": next(iter(sources)) if len(sources) == 1 else "mixed",
                "target_amount_ars": target_amount,
                "executed_amount_ars": executed_amount,
                "follow_ratio": follow_ratio,
                "follow_status": follow_status,
                "temporal_quality": temporal_quality,
                "eligible_for_viability": temporal_quality == "CONFIRMED_SEQUENCE",
                "match_window_sessions": int(match_window_sessions),
                "matching_version": matching_version,
                "metadata": {
                    "plan_count": len(component),
                    "movement_count": len(movement_rows),
                    "synthetic_fallbacks_excluded": len(movement_input) - len(canonical_movements),
                },
                "plan_links": component,
                "movement_rows": movement_rows,
            }
        )

    return sorted(attributions, key=lambda row: row["executed_at"], reverse=True)


async def ensure_plan_execution_attribution_schema(conn) -> None:
    await conn.execute(PLAN_EXECUTION_ATTRIBUTION_SQL)


async def _fetch_plans(conn, *, cutoff: datetime) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        """
        WITH decision_base AS (
            SELECT
                id, decided_at, owner_chat_id, ticker, decision, run_id,
                ABS(COALESCE(
                    NULLIF(layers->>'amount_ars', '')::numeric,
                    NULLIF(executed_amount_ars, 0),
                    theoretical_amount_ars,
                    0
                )) AS target_amount_ars,
                next_executable_at,
                CASE
                    WHEN next_executable_at IS NOT NULL THEN next_executable_at
                    WHEN (decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::time >= TIME '17:00'
                        THEN ((((decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date + 1) + TIME '10:30') AT TIME ZONE 'America/Argentina/Buenos_Aires')
                    WHEN (decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::time < TIME '10:30'
                        THEN (((decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date + TIME '10:30') AT TIME ZONE 'America/Argentina/Buenos_Aires')
                    ELSE decided_at
                END AS provisional_match_start_at,
                (
                    (decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::time >= TIME '17:00'
                    OR (decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::time < TIME '10:30'
                ) AS needs_open_revalidation
            FROM decision_log
            WHERE decided_at >= $1
              AND COALESCE(source, layers->>'source') = 'execution_plan'
              AND status IN ('APPROVED', 'EXECUTED')
              AND decision_type = 'executable'
              AND decision IN ('BUY', 'SELL')
              AND price_at_decision IS NOT NULL
        )
        SELECT
            d.*,
            CASE
                WHEN d.next_executable_at IS NOT NULL THEN d.next_executable_at
                WHEN d.needs_open_revalidation AND open_price.first_price_at IS NOT NULL
                    THEN open_price.first_price_at
                ELSE d.provisional_match_start_at
            END AS match_start_at,
            (
                CASE
                    WHEN d.next_executable_at IS NOT NULL THEN d.next_executable_at
                    WHEN d.needs_open_revalidation AND open_price.first_price_at IS NOT NULL
                        THEN open_price.first_price_at
                    ELSE d.provisional_match_start_at
                END AT TIME ZONE 'America/Argentina/Buenos_Aires'
            )::date AS match_day
        FROM decision_base d
        LEFT JOIN LATERAL (
            SELECT MIN(mp.ts) AS first_price_at
            FROM market_prices mp
            WHERE mp.ticker = d.ticker
              AND mp.last_price IS NOT NULL
              AND mp.last_price > 0
              AND mp.ts >= d.provisional_match_start_at
              AND mp.ts < d.provisional_match_start_at + INTERVAL '1 day'
        ) open_price ON TRUE
        ORDER BY d.decided_at
        """,
        cutoff,
    )
    return [dict(row) for row in rows]


async def _fetch_movements(conn, *, cutoff: datetime) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT
            bm.id, bm.executed_at, bm.executed_at_precision,
            bm.executed_at_source, bm.external_movement_id,
            bm.ticker, bm.movement_type, bm.amount, bm.quantity, bm.price,
            previous_snapshot.scraped_at AS observation_window_start_at
        FROM broker_movements bm
        LEFT JOIN LATERAL (
            SELECT MAX(ps.scraped_at) AS scraped_at
            FROM portfolio_snapshots ps
            JOIN positions position
              ON position.snapshot_id = ps.snapshot_id
             AND position.ticker = bm.ticker
            WHERE ps.scraped_at < bm.executed_at
        ) previous_snapshot ON bm.executed_at_precision = 'observed_after'
        WHERE bm.executed_at >= $1
          AND bm.movement_type IN ('BUY', 'SELL')
          AND bm.ticker IS NOT NULL
          AND bm.quantity IS NOT NULL
          AND bm.price IS NOT NULL
          AND NOT (COALESCE(bm.raw_payload, '{}'::jsonb) ? 'superseded_by_real')
        ORDER BY bm.executed_at, bm.id
        """,
        cutoff,
    )
    return [dict(row) for row in rows]


async def sync_plan_execution_attributions(
    conn,
    *,
    days: int = 180,
    match_window_sessions: int = 2,
) -> dict[str, int]:
    await ensure_plan_execution_attribution_schema(conn)
    cutoff = datetime.now(tz=ART) - timedelta(days=max(1, int(days)))
    plans = await _fetch_plans(conn, cutoff=cutoff)
    movements = await _fetch_movements(conn, cutoff=cutoff)
    attributions = normalize_plan_execution_attributions(
        plans,
        movements,
        match_window_sessions=match_window_sessions,
    )
    keys = [row["attribution_key"] for row in attributions]

    async with conn.transaction():
        for row in attributions:
            attribution_id = await conn.fetchval(
                """
                INSERT INTO plan_execution_attributions (
                    attribution_key, representative_decision_log_id, owner_chat_id,
                    ticker, side, plan_decided_at, executed_at,
                    executed_at_precision, executed_at_source,
                    target_amount_ars, executed_amount_ars, follow_ratio,
                    follow_status, temporal_quality, eligible_for_viability,
                    match_window_sessions, matching_version, metadata, updated_at
                ) VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18::jsonb,NOW()
                )
                ON CONFLICT (attribution_key) DO UPDATE SET
                    representative_decision_log_id = EXCLUDED.representative_decision_log_id,
                    owner_chat_id = EXCLUDED.owner_chat_id,
                    ticker = EXCLUDED.ticker,
                    side = EXCLUDED.side,
                    plan_decided_at = EXCLUDED.plan_decided_at,
                    executed_at = EXCLUDED.executed_at,
                    executed_at_precision = EXCLUDED.executed_at_precision,
                    executed_at_source = EXCLUDED.executed_at_source,
                    target_amount_ars = EXCLUDED.target_amount_ars,
                    executed_amount_ars = EXCLUDED.executed_amount_ars,
                    follow_ratio = EXCLUDED.follow_ratio,
                    follow_status = EXCLUDED.follow_status,
                    temporal_quality = EXCLUDED.temporal_quality,
                    eligible_for_viability = EXCLUDED.eligible_for_viability,
                    match_window_sessions = EXCLUDED.match_window_sessions,
                    matching_version = EXCLUDED.matching_version,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW()
                RETURNING id
                """,
                row["attribution_key"],
                row["representative_decision_log_id"],
                row["owner_chat_id"],
                row["ticker"],
                row["side"],
                row["plan_decided_at"],
                row["executed_at"],
                row["executed_at_precision"],
                row["executed_at_source"],
                row["target_amount_ars"],
                row["executed_amount_ars"],
                row["follow_ratio"],
                row["follow_status"],
                row["temporal_quality"],
                row["eligible_for_viability"],
                row["match_window_sessions"],
                row["matching_version"],
                json.dumps(row["metadata"]),
            )
            await conn.execute(
                "DELETE FROM plan_execution_attribution_plans WHERE attribution_id = $1",
                attribution_id,
            )
            await conn.execute(
                "DELETE FROM plan_execution_attribution_movements WHERE attribution_id = $1",
                attribution_id,
            )
            for candidate in row["plan_links"]:
                plan = candidate["plan"]
                await conn.execute(
                    """
                    INSERT INTO plan_execution_attribution_plans (
                        attribution_id, decision_log_id, is_representative,
                        target_amount_ars, matched_amount_ars, follow_ratio,
                        follow_status, temporal_quality
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                    ON CONFLICT (attribution_id, decision_log_id) DO UPDATE SET
                        is_representative = EXCLUDED.is_representative,
                        target_amount_ars = EXCLUDED.target_amount_ars,
                        matched_amount_ars = EXCLUDED.matched_amount_ars,
                        follow_ratio = EXCLUDED.follow_ratio,
                        follow_status = EXCLUDED.follow_status,
                        temporal_quality = EXCLUDED.temporal_quality
                    """,
                    attribution_id,
                    int(plan["id"]),
                    int(plan["id"]) == row["representative_decision_log_id"],
                    candidate["target_amount_ars"],
                    candidate["matched_amount_ars"],
                    candidate["follow_ratio"],
                    candidate["follow_status"],
                    candidate["temporal_quality"],
                )
            for movement in row["movement_rows"]:
                await conn.execute(
                    """
                    INSERT INTO plan_execution_attribution_movements (
                        attribution_id, broker_movement_id, amount_ars
                    ) VALUES ($1,$2,$3)
                    ON CONFLICT (broker_movement_id) DO UPDATE SET
                        attribution_id = EXCLUDED.attribution_id,
                        amount_ars = EXCLUDED.amount_ars
                    """,
                    attribution_id,
                    int(movement["id"]),
                    _movement_amount(movement),
                )

        if keys:
            await conn.execute(
                """
                DELETE FROM plan_execution_attributions
                WHERE matching_version = $1
                  AND plan_decided_at >= $2
                  AND NOT (attribution_key = ANY($3::text[]))
                """,
                MATCHING_VERSION,
                cutoff,
                keys,
            )
        else:
            await conn.execute(
                """
                DELETE FROM plan_execution_attributions
                WHERE matching_version = $1 AND plan_decided_at >= $2
                """,
                MATCHING_VERSION,
                cutoff,
            )

    return {
        "plans": len(plans),
        "movements": len(movements),
        "attributions": len(attributions),
        "eligible": sum(bool(row["eligible_for_viability"]) for row in attributions),
        "ambiguous": sum(not bool(row["eligible_for_viability"]) for row in attributions),
    }


__all__ = [
    "FOLLOW_STATUSES",
    "MATCHING_VERSION",
    "canonicalize_movements",
    "ensure_plan_execution_attribution_schema",
    "normalize_plan_execution_attributions",
    "sync_plan_execution_attributions",
]

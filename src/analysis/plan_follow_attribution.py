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

from src.analysis.decision_engine import directional_return
from src.collector.schema_migrations import PLAN_EXECUTION_ATTRIBUTION_SQL
from src.core.market_calendar import is_trading_day


ART = ZoneInfo("America/Argentina/Buenos_Aires")
MATCHING_VERSION = "plan-follow-v1"
OUTCOME_VERSION = "execution-sessions-v1"
OUTCOME_BASIS = "canonical_cocos"
OUTCOME_HORIZONS = (5, 10, 20, 40)
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


def _execution_reference(
    movements: Iterable[Mapping[str, Any]],
) -> tuple[float, float, float]:
    quantity = 0.0
    notional = 0.0
    for row in movements:
        row_quantity = abs(_as_float(row.get("quantity")))
        row_price = _as_float(row.get("price"))
        if row_quantity <= 0 or row_price <= 0:
            continue
        quantity += row_quantity
        notional += row_quantity * row_price
    price = notional / quantity if quantity > 0 else 0.0
    return quantity, price, notional


def _session_target_days(
    execution_day: date,
    horizons: Iterable[int] = OUTCOME_HORIZONS,
) -> dict[int, date]:
    wanted = sorted({int(value) for value in horizons if int(value) > 0})
    if not wanted:
        return {}
    targets: dict[int, date] = {}
    session = 0
    cursor = execution_day
    while session < wanted[-1]:
        cursor += timedelta(days=1)
        if not is_trading_day(cursor):
            continue
        session += 1
        if session in wanted:
            targets[session] = cursor
    return targets


def compute_execution_session_outcomes(
    *,
    execution_price: float,
    executed_at: datetime,
    side: str,
    candles: Iterable[Mapping[str, Any]],
    corporate_effects: Iterable[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """Compute exact trading-session outcomes from the real execution basis."""
    if execution_price <= 0:
        return {}
    execution_day = executed_at.astimezone(ART).date()
    target_days = _session_target_days(execution_day)
    candles_by_day: dict[date, Mapping[str, Any]] = {}
    for candle in candles:
        ts = candle.get("ts")
        candle_day = ts.date() if isinstance(ts, datetime) else _as_date(ts)
        close = _as_float(candle.get("close_price"))
        if candle_day is not None and close > 0:
            candles_by_day[candle_day] = candle

    effects = []
    for effect in corporate_effects:
        effective_at = _as_datetime(effect.get("effective_at"))
        price_factor = _as_float(effect.get("price_factor"), 1.0)
        lifecycle_status = str(effect.get("lifecycle_status") or "").upper()
        if (
            effective_at is not None
            and price_factor > 0
            and lifecycle_status in {"CONFIRMED", "EFFECTIVE"}
        ):
            effects.append((effective_at.astimezone(ART).date(), price_factor))

    result: dict[str, Any] = {}
    for horizon, target_day in target_days.items():
        candle = candles_by_day.get(target_day)
        if candle is None:
            continue
        adjusted_entry = float(execution_price)
        for effect_day, price_factor in effects:
            if execution_day < effect_day <= target_day:
                adjusted_entry *= price_factor
        close = _as_float(candle.get("close_price"))
        result[f"outcome_{horizon}d"] = directional_return(
            adjusted_entry,
            close,
            side,
        )
        result[f"outcome_date_{horizon}d"] = target_day
        result[f"outcome_price_{horizon}d"] = close
        result[f"outcome_source_{horizon}d"] = str(candle.get("source") or "unknown")
    return result


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
        execution_quantity, execution_price, execution_notional = _execution_reference(
            movement_rows
        )

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
                "execution_quantity": execution_quantity,
                "execution_price": execution_price,
                "execution_notional_ars": execution_notional,
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


async def fetch_canonical_outcome_candles(
    conn,
    tickers: Iterable[str],
    *,
    since: date,
) -> dict[str, list[dict[str, Any]]]:
    clean_tickers = sorted({str(ticker).upper() for ticker in tickers if ticker})
    if not clean_tickers:
        return {}
    rows = await conn.fetch(
        """
        WITH ranked AS (
            SELECT
                ts, ticker, close_price, source,
                ROW_NUMBER() OVER (
                    PARTITION BY UPPER(ticker), (ts AT TIME ZONE 'UTC')::date
                    ORDER BY
                        CASE
                            WHEN source = 'COCOS' THEN 0
                            WHEN source = 'TRADINGVIEW_BYMA' THEN 1
                            WHEN source = 'internal_snapshot' THEN 2
                            ELSE 3
                        END,
                        scraped_at DESC,
                        ts DESC
                ) AS source_rank
            FROM market_candles
            WHERE UPPER(ticker) = ANY($1::text[])
              AND ts >= $2::date
              AND interval = '1d'
              AND close_price IS NOT NULL
              AND close_price > 0
        )
        SELECT ts, UPPER(ticker) AS ticker, close_price, source
        FROM ranked
        WHERE source_rank = 1
        ORDER BY ticker, ts
        """,
        clean_tickers,
        since,
    )
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        item = dict(row)
        grouped.setdefault(str(item["ticker"]), []).append(item)
    return grouped


async def fetch_outcome_corporate_effects(
    conn,
    tickers: Iterable[str],
    *,
    since: date,
) -> dict[str, list[dict[str, Any]]]:
    clean_tickers = sorted({str(ticker).upper() for ticker in tickers if ticker})
    if not clean_tickers:
        return {}
    ready = await conn.fetchval(
        "SELECT to_regclass('public.corporate_event_instrument_effects') IS NOT NULL"
    )
    if not ready:
        return {}
    rows = await conn.fetch(
        """
        SELECT
            UPPER(effect.ticker) AS ticker,
            event.effective_at,
            event.lifecycle_status,
            effect.price_factor
        FROM corporate_event_instrument_effects effect
        JOIN corporate_events event ON event.id = effect.event_id
        WHERE effect.is_active = TRUE
          AND event.lifecycle_status IN ('CONFIRMED', 'EFFECTIVE')
          AND UPPER(effect.ticker) = ANY($1::text[])
          AND event.effective_at >= $2::date
        ORDER BY ticker, event.effective_at, event.id, effect.id
        """,
        clean_tickers,
        since,
    )
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        item = dict(row)
        grouped.setdefault(str(item["ticker"]), []).append(item)
    return grouped


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
    outcome_since = min(
        row["executed_at"].astimezone(ART).date() for row in attributions
    ) if attributions else cutoff.astimezone(ART).date()
    candles_by_ticker = await fetch_canonical_outcome_candles(
        conn,
        (row["ticker"] for row in attributions),
        since=outcome_since,
    )
    effects_by_ticker = await fetch_outcome_corporate_effects(
        conn,
        (row["ticker"] for row in attributions),
        since=outcome_since,
    )
    for row in attributions:
        outcomes = compute_execution_session_outcomes(
            execution_price=float(row["execution_price"]),
            executed_at=row["executed_at"],
            side=row["side"],
            candles=candles_by_ticker.get(row["ticker"], ()),
            corporate_effects=effects_by_ticker.get(row["ticker"], ()),
        )
        row["outcomes"] = outcomes
        row["metadata"]["outcome_sources"] = {
            str(horizon): outcomes.get(f"outcome_source_{horizon}d")
            for horizon in OUTCOME_HORIZONS
            if outcomes.get(f"outcome_source_{horizon}d")
        }
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
                    match_window_sessions, matching_version,
                    execution_quantity, execution_price, execution_notional_ars,
                    metadata, updated_at
                ) VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,
                    $18,$19,$20,$21::jsonb,NOW()
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
                    execution_quantity = EXCLUDED.execution_quantity,
                    execution_price = EXCLUDED.execution_price,
                    execution_notional_ars = EXCLUDED.execution_notional_ars,
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
                row["execution_quantity"],
                row["execution_price"],
                row["execution_notional_ars"],
                json.dumps(row["metadata"]),
            )
            outcomes = row["outcomes"]
            has_outcome = any(
                outcomes.get(f"outcome_{horizon}d") is not None
                for horizon in OUTCOME_HORIZONS
            )
            await conn.execute(
                """
                UPDATE plan_execution_attributions SET
                    outcome_5d = $2,
                    outcome_10d = $3,
                    outcome_20d = $4,
                    outcome_40d = $5,
                    outcome_date_5d = $6,
                    outcome_date_10d = $7,
                    outcome_date_20d = $8,
                    outcome_date_40d = $9,
                    outcome_price_5d = $10,
                    outcome_price_10d = $11,
                    outcome_price_20d = $12,
                    outcome_price_40d = $13,
                    outcome_basis = $14,
                    outcome_version = $15,
                    outcome_filled_at = CASE
                        WHEN $16 THEN COALESCE(outcome_filled_at, NOW())
                        ELSE NULL
                    END,
                    updated_at = NOW()
                WHERE id = $1
                """,
                attribution_id,
                outcomes.get("outcome_5d"),
                outcomes.get("outcome_10d"),
                outcomes.get("outcome_20d"),
                outcomes.get("outcome_40d"),
                outcomes.get("outcome_date_5d"),
                outcomes.get("outcome_date_10d"),
                outcomes.get("outcome_date_20d"),
                outcomes.get("outcome_date_40d"),
                outcomes.get("outcome_price_5d"),
                outcomes.get("outcome_price_10d"),
                outcomes.get("outcome_price_20d"),
                outcomes.get("outcome_price_40d"),
                OUTCOME_BASIS,
                OUTCOME_VERSION,
                has_outcome,
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
    "fetch_canonical_outcome_candles",
    "fetch_outcome_corporate_effects",
    "MATCHING_VERSION",
    "OUTCOME_BASIS",
    "OUTCOME_HORIZONS",
    "OUTCOME_VERSION",
    "canonicalize_movements",
    "compute_execution_session_outcomes",
    "ensure_plan_execution_attribution_schema",
    "normalize_plan_execution_attributions",
    "sync_plan_execution_attributions",
]

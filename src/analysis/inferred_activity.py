from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg

from src.analysis.corporate_actions import (
    corporate_action_effect_from_row,
    effects_by_ticker,
    matching_effect_for_quantity_transition,
)


async def fetch_inferred_activity(
    conn: asyncpg.Connection,
    *,
    days: int = 7,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Return stable quantity deltas observed across consecutive snapshots."""
    rows = await conn.fetch(
        """
        WITH snaps AS (
            SELECT
                snapshot_id,
                scraped_at,
                LAG(snapshot_id) OVER (ORDER BY scraped_at) AS prev_snapshot_id,
                LAG(scraped_at) OVER (ORDER BY scraped_at) AS prev_scraped_at,
                LEAD(snapshot_id) OVER (ORDER BY scraped_at) AS next_snapshot_id
            FROM portfolio_snapshots
            WHERE scraped_at >= NOW() - ($1::int * INTERVAL '1 day')
            ORDER BY scraped_at
        ),
        pairs AS (
            SELECT *
            FROM snaps
            WHERE prev_snapshot_id IS NOT NULL
        ),
        pair_tickers AS (
            SELECT DISTINCT
                p.snapshot_id,
                p.prev_snapshot_id,
                p.next_snapshot_id,
                p.scraped_at,
                p.prev_scraped_at,
                pos.ticker
            FROM pairs p
            JOIN positions pos
              ON pos.snapshot_id IN (p.snapshot_id, p.prev_snapshot_id)
        ),
        deltas AS (
            SELECT
                pt.prev_scraped_at,
                pt.scraped_at,
                pt.next_snapshot_id,
                pt.ticker,
                COALESCE(prev.quantity, 0)::float AS previous_quantity,
                COALESCE(cur.quantity, 0)::float AS current_quantity,
                COALESCE(nxt.quantity, 0)::float AS next_quantity,
                COALESCE(cur.quantity, 0)::float - COALESCE(prev.quantity, 0)::float AS quantity_delta,
                COALESCE(cur.current_price, prev.current_price)::float AS reference_price
            FROM pair_tickers pt
            LEFT JOIN positions cur
                ON cur.snapshot_id = pt.snapshot_id
               AND cur.ticker = pt.ticker
            LEFT JOIN positions prev
                ON prev.snapshot_id = pt.prev_snapshot_id
               AND prev.ticker = pt.ticker
            LEFT JOIN positions nxt
                ON nxt.snapshot_id = pt.next_snapshot_id
               AND nxt.ticker = pt.ticker
        )
        SELECT
            d.prev_scraped_at,
            d.scraped_at,
            d.ticker,
            d.previous_quantity,
            d.current_quantity,
            CASE WHEN d.quantity_delta > 0 THEN 'BUY' ELSE 'SELL' END AS side,
            ABS(d.quantity_delta) AS quantity,
            d.reference_price,
            ABS(d.quantity_delta * COALESCE(d.reference_price, 0)) AS inferred_amount_ars,
            bm.executed_at AS confirmed_at
        FROM deltas d
        LEFT JOIN LATERAL (
            SELECT executed_at
            FROM broker_movements bm
            WHERE bm.ticker = d.ticker
              AND bm.movement_type = CASE WHEN d.quantity_delta > 0 THEN 'BUY' ELSE 'SELL' END
              AND (bm.executed_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date =
                  (d.scraped_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date
              AND ABS(ABS(bm.quantity::float) - ABS(d.quantity_delta)) <= 0.000001
              AND bm.quantity IS NOT NULL
              AND bm.price IS NOT NULL
            ORDER BY ABS(EXTRACT(EPOCH FROM (bm.executed_at - d.scraped_at))) ASC
            LIMIT 1
        ) bm ON TRUE
        WHERE d.ticker IS NOT NULL
          AND ABS(d.quantity_delta) > 0.000001
          AND ABS(d.quantity_delta * COALESCE(d.reference_price, 0)) >= 1000
          AND ABS(d.quantity_delta) / GREATEST(ABS(d.previous_quantity), ABS(d.current_quantity), 1) >= 0.01
          AND d.next_snapshot_id IS NOT NULL
          AND ABS(d.next_quantity - d.current_quantity) <= 0.000001
        ORDER BY d.scraped_at DESC, d.ticker
        LIMIT $2
        """,
        max(1, min(int(days), 30)),
        max(1, min(int(limit), 200)),
    )
    return [dict(row) for row in rows]


async def mark_inferred_activity_types(
    conn: asyncpg.Connection,
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not rows:
        return rows
    schema_ready = await conn.fetchval(
        """
        SELECT
            to_regclass('public.corporate_events') IS NOT NULL
            AND to_regclass('public.corporate_event_instrument_effects') IS NOT NULL
        """
    )
    effects = []
    if schema_ready:
        timestamps = [
            value
            for row in rows
            for value in (row.get("prev_scraped_at"), row.get("scraped_at"))
            if isinstance(value, datetime)
        ]
        since = min(timestamps) - timedelta(days=2) if timestamps else datetime.now(timezone.utc) - timedelta(days=32)
        until = max(timestamps) + timedelta(days=2) if timestamps else datetime.now(timezone.utc)
        tickers = sorted({str(row.get("ticker") or "").upper() for row in rows})
        effect_rows = await conn.fetch(
            """
            SELECT
                e.id AS event_id,
                effect.id AS effect_id,
                e.event_key,
                e.issuer_id,
                e.event_type,
                e.lifecycle_status,
                e.effective_at,
                e.expires_at,
                e.source_name,
                e.source_url,
                e.ingestion_method,
                e.evidence_level,
                e.detector_score,
                effect.instrument_id,
                effect.ticker,
                effect.venue,
                effect.asset_type,
                effect.currency,
                effect.quantity_factor,
                effect.price_factor,
                effect.cost_basis_factor,
                effect.depositary_ratio_before,
                effect.depositary_ratio_after,
                effect.metadata
            FROM corporate_event_instrument_effects effect
            JOIN corporate_events e ON e.id = effect.event_id
            WHERE effect.is_active = TRUE
              AND e.lifecycle_status IN ('CONFIRMED', 'EFFECTIVE')
              AND e.effective_at >= $1
              AND e.effective_at <= $2
              AND UPPER(effect.ticker) = ANY($3::text[])
            ORDER BY e.effective_at, e.id, effect.id
            """,
            since,
            until,
            tickers,
        )
        effects = [corporate_action_effect_from_row(dict(row)) for row in effect_rows]

    grouped = effects_by_ticker(effects)
    for row in rows:
        ticker = str(row.get("ticker") or "").upper()
        effect = None
        if not row.get("confirmed_at"):
            effect = matching_effect_for_quantity_transition(
                ticker=ticker,
                previous_quantity=row.get("previous_quantity"),
                current_quantity=row.get("current_quantity"),
                previous_at=row.get("prev_scraped_at"),
                current_at=row.get("scraped_at"),
                effects=grouped.get(ticker, ()),
            )
        row["activity_type"] = "CORPORATE_ACTION" if effect else "HUMAN_TRADE_CANDIDATE"
    return rows


__all__ = ["fetch_inferred_activity", "mark_inferred_activity_types"]

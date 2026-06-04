from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from html import escape
from zoneinfo import ZoneInfo

import asyncpg


ART = ZoneInfo("America/Argentina/Buenos_Aires")


def _as_float(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _mean(values: list[float]) -> float | None:
    clean = [float(v) for v in values if v is not None]
    return sum(clean) / len(clean) if clean else None


def _sum(values: list[float | None]) -> float | None:
    clean = [float(v) for v in values if v is not None]
    return sum(clean) if clean else None


def _pct(value) -> str:
    if value is None:
        return "N/A"
    return f"{float(value):+.1%}"


def _rate(value) -> str:
    if value is None:
        return "N/A"
    return f"{float(value):.1%}"


def _money(value) -> str:
    if value is None:
        return "N/A"
    return f"${float(value):,.0f}".replace(",", ".")


def _clean_text(value) -> str:
    text = "" if value is None else str(value)
    return (
        text.replace("posici?n", "posicion")
        .replace("exposici?n", "exposicion")
        .replace("se?al", "senal")
        .replace("te?rico", "teorico")
        .replace("ejecuci?n", "ejecucion")
        .replace(" ? ", " -> ")
        .replace("?", "")
    )


def _precision_label(value) -> str:
    text = str(value or "").strip().lower()
    if text == "date_only":
        return "DATE_ONLY"
    if text == "exact":
        return "EXACT"
    if text == "window":
        return "WINDOW"
    if text == "inferred":
        return "INFERRED"
    return ""


def _fmt_dt(value) -> str:
    if value is None:
        return "?"
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
            return parsed.astimezone(ART).strftime("%d/%m %H:%M")
        except Exception:
            return value
    if isinstance(value, datetime):
        return value.astimezone(ART).strftime("%d/%m %H:%M")
    if isinstance(value, date):
        return value.strftime("%d/%m")
    return str(value)


def _row(row) -> dict:
    if not row:
        return {}
    out: dict = {}
    for key, value in dict(row).items():
        if isinstance(value, (datetime, date)):
            out[key] = value.isoformat()
        elif isinstance(value, Decimal):
            out[key] = float(value)
        else:
            out[key] = value
    return out


def _target(row: dict) -> float:
    return max(_as_float(row.get("target_amount_ars")), 1.0)


def _same_ratio(row: dict) -> float:
    return _as_float(row.get("same_amount_ars")) / _target(row)


def _opposite_ratio(row: dict) -> float:
    return _as_float(row.get("opposite_amount_ars")) / _target(row)


def classify_override(row: dict) -> str:
    if row.get("match_basis") == "pending_open_revalidation" or row.get("match_start_at") is None:
        return "PENDING_OPEN"

    same_ratio = _same_ratio(row)
    opposite_ratio = _opposite_ratio(row)
    if same_ratio < 0.15 and opposite_ratio >= 0.15:
        return "OPPOSITE"
    if same_ratio >= 1.35:
        return "OVERFOLLOWED"
    if same_ratio >= 0.75:
        return "FOLLOWED"
    if same_ratio >= 0.15:
        return "PARTIAL"
    return "IGNORED"


def _directional_pnl(amount_ars, outcome) -> float | None:
    if outcome is None:
        return None
    return abs(_as_float(amount_ars)) * float(outcome)


def _plan_money(row: dict, horizon: str = "5d") -> tuple[float | None, float | None, float | None]:
    outcome = row.get(f"outcome_{horizon}")
    if outcome is None:
        return None, None, None
    target = _target(row)
    same = _as_float(row.get("same_amount_ars"))
    opposite = _as_float(row.get("opposite_amount_ars"))
    bot_pnl = target * float(outcome)
    human_pnl = (same - opposite) * float(outcome)
    return bot_pnl, human_pnl, human_pnl - bot_pnl


async def fetch_decision_ledger(
    conn: asyncpg.Connection,
    *,
    days: int = 90,
    match_window_days: int = 2,
    owner_chat_id: int | None = None,
) -> dict:
    real_rows = await conn.fetch(
        """
        SELECT
            id,
            decided_at,
            ticker,
            decision,
            COALESCE(source, layers->>'source') AS source,
            COALESCE(status, 'UNKNOWN') AS status,
            COALESCE(decision_type, 'unknown') AS decision_type,
            price_at_decision,
            ABS(COALESCE(NULLIF(executed_amount_ars, 0), theoretical_amount_ars, 0)) AS amount_ars,
            layers#>>'{broker_fill,executed_at_precision}' AS execution_precision,
            layers#>>'{broker_fill,executed_at_source}' AS execution_timestamp_source,
            COALESCE(executable_outcome_5d, outcome_5d) AS outcome_5d,
            COALESCE(executable_outcome_10d, outcome_10d) AS outcome_10d,
            COALESCE(executable_outcome_20d, outcome_20d) AS outcome_20d
        FROM decision_log
        WHERE decided_at >= NOW() - ($1::int * INTERVAL '1 day')
          AND ($2::bigint IS NULL OR owner_chat_id = $2)
          AND decision IN ('BUY', 'SELL')
          AND COALESCE(outcome_basis, '') <> 'legacy_external'
          AND (
            (
              COALESCE(source, layers->>'source') IN ('broker_movement', 'broker_fill')
              AND status IN ('EXECUTED', 'EXECUTED_MANUAL')
            )
            OR (
              COALESCE(source, layers->>'source') = 'execution_plan'
              AND status IN ('EXECUTED', 'EXECUTED_MANUAL')
            )
          )
        ORDER BY decided_at DESC, id DESC
        """,
        days,
        owner_chat_id,
    )

    plan_rows = await conn.fetch(
        """
        WITH decision_base AS (
            SELECT
                id,
                decided_at,
                owner_chat_id,
                ticker,
                decision,
                final_score,
                price_at_decision,
                ABS(COALESCE(theoretical_amount_ars, executed_amount_ars, 0)) AS target_amount_ars,
                COALESCE(executable_outcome_5d, outcome_5d) AS outcome_5d,
                COALESCE(executable_outcome_10d, outcome_10d) AS outcome_10d,
                COALESCE(executable_outcome_20d, outcome_20d) AS outcome_20d,
                next_executable_at,
                next_executable_price,
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
                ) AS needs_open_revalidation,
                layers->>'reason' AS reason
            FROM decision_log
            WHERE decided_at >= NOW() - ($1::int * INTERVAL '1 day')
              AND ($3::bigint IS NULL OR owner_chat_id = $3)
              AND COALESCE(source, layers->>'source') = 'execution_plan'
              AND status IN ('APPROVED', 'EXECUTED')
              AND decision_type = 'executable'
              AND decision IN ('BUY', 'SELL')
              AND price_at_decision IS NOT NULL
        ),
        decisions AS (
            SELECT
                d.*,
                CASE
                    WHEN d.next_executable_at IS NOT NULL THEN d.next_executable_at
                    WHEN d.needs_open_revalidation THEN open_price.first_price_at
                    ELSE d.provisional_match_start_at
                END AS match_start_at,
                CASE
                    WHEN d.next_executable_at IS NOT NULL
                        THEN (d.next_executable_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date
                    WHEN d.needs_open_revalidation AND open_price.first_price_at IS NOT NULL
                        THEN (open_price.first_price_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date
                    WHEN NOT d.needs_open_revalidation
                        THEN (d.provisional_match_start_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date
                    ELSE NULL
                END AS match_day,
                CASE
                    WHEN d.next_executable_at IS NOT NULL THEN 'next_executable'
                    WHEN d.needs_open_revalidation AND open_price.first_price_at IS NOT NULL THEN 'fresh_open_price'
                    WHEN d.needs_open_revalidation THEN 'pending_open_revalidation'
                    ELSE 'intraday'
                END AS match_basis
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
        )
        SELECT
            d.*,
            same_fill.first_at AS same_executed_at,
            same_fill.executed_at_precision AS same_executed_at_precision,
            same_fill.executed_at_source AS same_executed_at_source,
            same_fill.amount_ars AS same_amount_ars,
            opposite_fill.first_at AS opposite_executed_at,
            opposite_fill.executed_at_precision AS opposite_executed_at_precision,
            opposite_fill.executed_at_source AS opposite_executed_at_source,
            opposite_fill.amount_ars AS opposite_amount_ars
        FROM decisions d
        LEFT JOIN LATERAL (
            SELECT
                MIN(executed_at) AS first_at,
                (ARRAY_AGG(COALESCE(executed_at_precision, 'unknown') ORDER BY executed_at, id))[1] AS executed_at_precision,
                (ARRAY_AGG(COALESCE(executed_at_source, 'unknown') ORDER BY executed_at, id))[1] AS executed_at_source,
                SUM(ABS(COALESCE(amount, quantity * price, 0))) AS amount_ars
            FROM broker_movements bm
            WHERE bm.ticker = d.ticker
              AND bm.movement_type = d.decision
              AND d.match_start_at IS NOT NULL
              AND (
                  (
                      bm.executed_at >= d.match_start_at
                      AND bm.executed_at < d.match_start_at + ($2::int * INTERVAL '1 day')
                  )
                  OR (
                      d.match_day IS NOT NULL
                      AND (bm.executed_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date >= d.match_day
                      AND (bm.executed_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date < d.match_day + $2::int
                  )
              )
              AND bm.quantity IS NOT NULL
              AND bm.price IS NOT NULL
        ) same_fill ON TRUE
        LEFT JOIN LATERAL (
            SELECT
                MIN(executed_at) AS first_at,
                (ARRAY_AGG(COALESCE(executed_at_precision, 'unknown') ORDER BY executed_at, id))[1] AS executed_at_precision,
                (ARRAY_AGG(COALESCE(executed_at_source, 'unknown') ORDER BY executed_at, id))[1] AS executed_at_source,
                SUM(ABS(COALESCE(amount, quantity * price, 0))) AS amount_ars
            FROM broker_movements bm
            WHERE bm.ticker = d.ticker
              AND bm.movement_type = CASE WHEN d.decision = 'BUY' THEN 'SELL' ELSE 'BUY' END
              AND d.match_start_at IS NOT NULL
              AND (
                  (
                      bm.executed_at >= d.match_start_at
                      AND bm.executed_at < d.match_start_at + ($2::int * INTERVAL '1 day')
                  )
                  OR (
                      d.match_day IS NOT NULL
                      AND (bm.executed_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date >= d.match_day
                      AND (bm.executed_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date < d.match_day + $2::int
                  )
              )
              AND bm.quantity IS NOT NULL
              AND bm.price IS NOT NULL
        ) opposite_fill ON TRUE
        ORDER BY d.decided_at DESC, d.id DESC
        """,
        days,
        match_window_days,
        owner_chat_id,
    )

    radar_rows = await conn.fetch(
        """
        WITH radar AS (
            SELECT
                id,
                decided_at,
                (decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date AS decision_day,
                COALESCE(
                    (next_executable_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date,
                    (decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date
                ) AS audit_start_day,
                ticker,
                decision,
                final_score,
                confidence,
                status,
                decision_type,
                price_at_decision,
                COALESCE(NULLIF(next_executable_price, 0), price_at_decision) AS audit_entry_price,
                ABS(COALESCE(theoretical_amount_ars, NULLIF(executed_amount_ars, 0), 0)) AS amount_ars,
                rr_ratio,
                block_reason,
                layers->>'trade_type' AS trade_type,
                NULLIF(layers->>'edge_vs', '') AS edge_vs,
                layers->>'candidate_status' AS candidate_status,
                layers->>'edge_label' AS edge_label,
                outcome_5d,
                outcome_10d,
                outcome_20d,
                executable_outcome_5d,
                executable_outcome_10d,
                executable_outcome_20d
            FROM decision_log
            WHERE decided_at >= NOW() - ($1::int * INTERVAL '1 day')
              AND ($2::bigint IS NULL OR owner_chat_id = $2)
              AND COALESCE(source, layers->>'source') = 'radar'
              AND decision IN ('BUY', 'SELL')
              AND price_at_decision IS NOT NULL
              AND price_at_decision > 0
        )
        SELECT
            r.*,
            path.price_2d,
            path.close_5d,
            path.close_10d,
            path.close_20d,
            path.mae_10d,
            path.mfe_10d,
            bench.entry_price AS edge_entry_price,
            bench.close_5d AS edge_close_5d,
            bench.close_10d AS edge_close_10d,
            bench.close_20d AS edge_close_20d,
            CASE
                WHEN r.decision = 'SELL' AND path.price_2d IS NOT NULL THEN (r.audit_entry_price / path.price_2d) - 1
                WHEN path.price_2d IS NOT NULL THEN (path.price_2d / r.audit_entry_price) - 1
                ELSE NULL
            END AS outcome_2d,
            COALESCE(r.executable_outcome_5d, r.outcome_5d, CASE
                WHEN r.decision = 'SELL' AND path.close_5d IS NOT NULL THEN (r.audit_entry_price / path.close_5d) - 1
                WHEN path.close_5d IS NOT NULL THEN (path.close_5d / r.audit_entry_price) - 1
                ELSE NULL
            END) AS outcome_5d,
            COALESCE(r.executable_outcome_10d, r.outcome_10d, CASE
                WHEN r.decision = 'SELL' AND path.close_10d IS NOT NULL THEN (r.audit_entry_price / path.close_10d) - 1
                WHEN path.close_10d IS NOT NULL THEN (path.close_10d / r.audit_entry_price) - 1
                ELSE NULL
            END) AS outcome_10d,
            COALESCE(r.executable_outcome_20d, r.outcome_20d, CASE
                WHEN r.decision = 'SELL' AND path.close_20d IS NOT NULL THEN (r.audit_entry_price / path.close_20d) - 1
                WHEN path.close_20d IS NOT NULL THEN (path.close_20d / r.audit_entry_price) - 1
                ELSE NULL
            END) AS outcome_20d,
            CASE
                WHEN bench.entry_price IS NOT NULL AND bench.close_5d IS NOT NULL
                    THEN (bench.close_5d / bench.entry_price) - 1
                ELSE NULL
            END AS edge_outcome_5d,
            CASE
                WHEN bench.entry_price IS NOT NULL AND bench.close_10d IS NOT NULL
                    THEN (bench.close_10d / bench.entry_price) - 1
                ELSE NULL
            END AS edge_outcome_10d,
            CASE
                WHEN bench.entry_price IS NOT NULL AND bench.close_20d IS NOT NULL
                    THEN (bench.close_20d / bench.entry_price) - 1
                ELSE NULL
            END AS edge_outcome_20d
        FROM radar r
        LEFT JOIN LATERAL (
            WITH candles AS (
                SELECT
                    ts::date AS day,
                    close_price::float AS close_price,
                    high_price::float AS high_price,
                    low_price::float AS low_price
                FROM market_candles
                WHERE ticker = r.ticker
                  AND ts::date >= r.audit_start_day
                  AND ts::date <= r.audit_start_day + 20
                  AND close_price IS NOT NULL
                ORDER BY ts ASC
            )
            SELECT
                (SELECT close_price FROM candles WHERE day >= r.audit_start_day + 2 LIMIT 1) AS price_2d,
                (SELECT close_price FROM candles WHERE day >= r.audit_start_day + 5 LIMIT 1) AS close_5d,
                (SELECT close_price FROM candles WHERE day >= r.audit_start_day + 10 LIMIT 1) AS close_10d,
                (SELECT close_price FROM candles WHERE day >= r.audit_start_day + 20 LIMIT 1) AS close_20d,
                CASE
                    WHEN r.decision = 'SELL' THEN MIN((r.audit_entry_price / NULLIF(high_price, 0)) - 1)
                    ELSE MIN((low_price / NULLIF(r.audit_entry_price, 0)) - 1)
                END AS mae_10d,
                CASE
                    WHEN r.decision = 'SELL' THEN MAX((r.audit_entry_price / NULLIF(low_price, 0)) - 1)
                    ELSE MAX((high_price / NULLIF(r.audit_entry_price, 0)) - 1)
                END AS mfe_10d
            FROM candles
            WHERE day <= r.audit_start_day + 10
        ) path ON TRUE
        LEFT JOIN LATERAL (
            WITH candles AS (
                SELECT ts::date AS day, close_price::float AS close_price
                FROM market_candles
                WHERE ticker = r.edge_vs
                  AND ts::date >= r.audit_start_day
                  AND ts::date <= r.audit_start_day + 20
                  AND close_price IS NOT NULL
                ORDER BY ts ASC
            )
            SELECT
                (SELECT close_price FROM candles WHERE day >= r.audit_start_day LIMIT 1) AS entry_price,
                (SELECT close_price FROM candles WHERE day >= r.audit_start_day + 5 LIMIT 1) AS close_5d,
                (SELECT close_price FROM candles WHERE day >= r.audit_start_day + 10 LIMIT 1) AS close_10d,
                (SELECT close_price FROM candles WHERE day >= r.audit_start_day + 20 LIMIT 1) AS close_20d
        ) bench ON TRUE
        ORDER BY r.decided_at DESC, r.id DESC
        LIMIT 240
        """,
        days,
        owner_chat_id,
    )

    pending_mark_rows = await conn.fetch(
        """
        WITH latest AS (
            SELECT DISTINCT ON (ticker)
                ticker,
                ts,
                last_price::float AS last_price
            FROM market_prices
            ORDER BY ticker, ts DESC
        )
        SELECT
            dl.id,
            dl.decided_at,
            dl.ticker,
            dl.decision,
            COALESCE(dl.source, dl.layers->>'source') AS source,
            COALESCE(dl.status, 'UNKNOWN') AS status,
            COALESCE(dl.decision_type, 'unknown') AS decision_type,
            dl.price_at_decision,
            ABS(COALESCE(NULLIF(dl.executed_amount_ars, 0), dl.theoretical_amount_ars, 0)) AS amount_ars,
            latest.last_price,
            latest.ts AS latest_price_at,
            CASE
                WHEN dl.decision = 'SELL' THEN (dl.price_at_decision / NULLIF(latest.last_price, 0)) - 1
                ELSE (latest.last_price / NULLIF(dl.price_at_decision, 0)) - 1
            END AS mark_return
        FROM decision_log dl
        JOIN latest ON latest.ticker = dl.ticker
        WHERE dl.decided_at >= NOW() - INTERVAL '10 days'
          AND ($1::bigint IS NULL OR dl.owner_chat_id = $1)
          AND dl.decision IN ('BUY', 'SELL')
          AND dl.price_at_decision IS NOT NULL
          AND dl.price_at_decision > 0
          AND COALESCE(dl.executable_outcome_5d, dl.outcome_5d) IS NULL
          AND COALESCE(dl.source, dl.layers->>'source') IN ('execution_plan', 'broker_movement', 'radar')
        ORDER BY dl.decided_at DESC, dl.id DESC
        LIMIT 30
        """,
        owner_chat_id,
    )

    real = [dict(r) for r in real_rows]
    plans = [dict(r) for r in plan_rows]
    radar = [dict(r) for r in radar_rows]
    pending = [dict(r) for r in pending_mark_rows]

    for row in real:
        for horizon in ("5d", "10d", "20d"):
            row[f"pnl_{horizon}_ars"] = _directional_pnl(
                row.get("amount_ars"),
                row.get(f"outcome_{horizon}"),
            )

    for row in plans:
        row["override_status"] = classify_override(row)
        row["same_ratio"] = _same_ratio(row)
        row["opposite_ratio"] = _opposite_ratio(row)
        for horizon in ("5d", "10d", "20d"):
            bot, human, delta = _plan_money(row, horizon)
            row[f"bot_pnl_{horizon}_ars"] = bot
            row[f"human_pnl_{horizon}_ars"] = human
            row[f"human_vs_bot_{horizon}_ars"] = delta

    for row in radar:
        for horizon in ("5d", "10d", "20d"):
            outcome = row.get(f"outcome_{horizon}")
            edge_outcome = row.get(f"edge_outcome_{horizon}")
            row[f"candidate_pnl_{horizon}_ars"] = _directional_pnl(row.get("amount_ars"), outcome)
            if outcome is not None and edge_outcome is not None:
                row[f"swap_alpha_{horizon}"] = float(outcome) - float(edge_outcome)
                row[f"swap_alpha_{horizon}_ars"] = abs(_as_float(row.get("amount_ars"))) * row[f"swap_alpha_{horizon}"]
            else:
                row[f"swap_alpha_{horizon}"] = None
                row[f"swap_alpha_{horizon}_ars"] = None

    for row in pending:
        row["mark_pnl_ars"] = _directional_pnl(row.get("amount_ars"), row.get("mark_return"))

    def closed(rows: list[dict], key: str) -> list[dict]:
        return [r for r in rows if r.get(key) is not None]

    real_closed_5d = closed(real, "pnl_5d_ars")
    plan_closed_5d = closed(plans, "bot_pnl_5d_ars")
    radar_closed_5d = closed(radar, "candidate_pnl_5d_ars")
    swap_closed_5d = [r for r in radar if r.get("swap_alpha_5d_ars") is not None]
    radar_operable = [
        r for r in radar
        if str(r.get("status") or "").upper() == "THEORETICAL"
    ]
    radar_blocked = [
        r for r in radar
        if str(r.get("status") or "").upper() == "BLOCKED"
    ]
    radar_operable_closed_5d = closed(radar_operable, "candidate_pnl_5d_ars")
    radar_blocked_closed_5d = closed(radar_blocked, "candidate_pnl_5d_ars")

    return {
        "days": days,
        "match_window_days": match_window_days,
        "summary": {
            "real_total": len(real),
            "real_closed_5d": len(real_closed_5d),
            "real_pending_5d": len(real) - len(real_closed_5d),
            "real_pnl_5d_ars": _sum([r.get("pnl_5d_ars") for r in real]),
            "real_pnl_10d_ars": _sum([r.get("pnl_10d_ars") for r in real]),
            "real_pnl_20d_ars": _sum([r.get("pnl_20d_ars") for r in real]),
            "real_win_rate_5d": _mean([1.0 if _as_float(r.get("pnl_5d_ars")) > 0 else 0.0 for r in real_closed_5d]),
            "plans_total": len(plans),
            "plans_closed_5d": len(plan_closed_5d),
            "bot_full_pnl_5d_ars": _sum([r.get("bot_pnl_5d_ars") for r in plans]),
            "human_matched_pnl_5d_ars": _sum([r.get("human_pnl_5d_ars") for r in plans]),
            "human_vs_bot_5d_ars": _sum([r.get("human_vs_bot_5d_ars") for r in plans]),
            "radar_total": len(radar),
            "radar_closed_5d": len(radar_closed_5d),
            "radar_candidate_pnl_5d_ars": _sum([r.get("candidate_pnl_5d_ars") for r in radar]),
            "radar_avg_5d": _mean([_as_float(r.get("outcome_5d")) for r in radar_closed_5d]),
            "radar_operable_total": len(radar_operable),
            "radar_operable_closed_5d": len(radar_operable_closed_5d),
            "radar_operable_avg_5d": _mean([_as_float(r.get("outcome_5d")) for r in radar_operable_closed_5d]),
            "radar_operable_pnl_5d_ars": _sum([r.get("candidate_pnl_5d_ars") for r in radar_operable]),
            "radar_blocked_total": len(radar_blocked),
            "radar_blocked_closed_5d": len(radar_blocked_closed_5d),
            "radar_blocked_avg_5d": _mean([_as_float(r.get("outcome_5d")) for r in radar_blocked_closed_5d]),
            "radar_blocked_pnl_5d_ars": _sum([r.get("candidate_pnl_5d_ars") for r in radar_blocked]),
            "swap_total": len([r for r in radar if r.get("edge_vs")]),
            "swap_closed_5d": len(swap_closed_5d),
            "swap_alpha_5d_ars": _sum([r.get("swap_alpha_5d_ars") for r in radar]),
            "swap_avg_alpha_5d": _mean([_as_float(r.get("swap_alpha_5d")) for r in swap_closed_5d]),
            "pending_mark_count": len(pending),
            "pending_mark_pnl_ars": _sum([r.get("mark_pnl_ars") for r in pending]),
        },
        "real_executions": [_row(r) for r in real[:60]],
        "bot_vs_human": [_row(r) for r in plans[:60]],
        "radar": [_row(r) for r in radar[:80]],
        "pending_mark": [_row(r) for r in pending[:30]],
    }


def render_decision_ledger(data: dict) -> str:
    summary = data.get("summary") or {}
    plans = data.get("bot_vs_human") or []
    real = data.get("real_executions") or []
    radar = data.get("radar") or []
    pending = data.get("pending_mark") or []
    real_pnl = summary.get("real_pnl_5d_ars")
    human_delta = summary.get("human_vs_bot_5d_ars")
    radar_avg = summary.get("radar_avg_5d")
    swap_alpha = summary.get("swap_avg_alpha_5d")

    executive_lines = []
    if real_pnl is not None:
        executive_lines.append(
            "La operatoria real cerrada viene positiva."
            if _as_float(real_pnl) > 0
            else "La operatoria real cerrada viene negativa."
        )
    if human_delta is not None:
        executive_lines.append(
            "El humano supero al bot full hipotetico en la muestra cerrada."
            if _as_float(human_delta) > 0
            else "El bot full hipotetico supero a la ejecucion humana en la muestra cerrada."
        )
    if radar_avg is not None:
        executive_lines.append(
            "El radar muestra deteccion positiva, pero parte de la muestra es teorica o bloqueada."
            if _as_float(radar_avg) > 0
            else "El radar no muestra ventaja promedio en las ideas cerradas."
        )
    if swap_alpha is not None:
        executive_lines.append(
            "Los swaps maduros muestran alpha positivo contra el activo sacrificado."
            if _as_float(swap_alpha) > 0
            else "Los swaps maduros no superan al activo sacrificado."
        )

    lines = [
        "<b>DECISION LEDGER</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"Periodo: <b>{int(data.get('days') or 0)} dias</b> | ventana match: <b>{int(data.get('match_window_days') or 0)}d</b>",
        "",
        "<b>QUE MIDE</b>",
        "   Atribucion economica de decisiones, ejecuciones reales y radar.",
        "   No toca analysis, scores, optimizer, planner ni thresholds.",
        "   Usa retornos direccionales: BUY gana si sube; SELL gana si baja.",
        "",
    ]

    if executive_lines:
        lines += [
            "<b>LECTURA EJECUTIVA</b>",
            f"   {escape(' '.join(executive_lines))}",
            "   Muestra aun limitada: usar como auditoria, no como prueba estadistica final.",
            "",
        ]

    lines += [
        "<b>RESUMEN ARS</b>",
        (
            f"   Ejecucion real: <b>{summary.get('real_closed_5d', 0)}</b> cerradas 5D / "
            f"{summary.get('real_total', 0)} eventos | PnL 5D {_money(summary.get('real_pnl_5d_ars'))} | "
            f"win {_rate(summary.get('real_win_rate_5d'))}"
        ),
        (
            f"   Bot vs humano: <b>{summary.get('plans_closed_5d', 0)}</b> planes cerrados 5D | "
            f"bot full {_money(summary.get('bot_full_pnl_5d_ars'))} | "
            f"humano {_money(summary.get('human_matched_pnl_5d_ars'))} | "
            f"delta {_money(summary.get('human_vs_bot_5d_ars'))}"
        ),
        (
            f"   Radar: <b>{summary.get('radar_closed_5d', 0)}</b> ideas cerradas 5D / "
            f"{summary.get('radar_total', 0)} | avg {_pct(summary.get('radar_avg_5d'))} | "
            f"PnL teorico {_money(summary.get('radar_candidate_pnl_5d_ars'))}"
        ),
        (
            f"   Swaps radar: <b>{summary.get('swap_closed_5d', 0)}</b> comparados 5D / "
            f"{summary.get('swap_total', 0)} | alpha avg {_pct(summary.get('swap_avg_alpha_5d'))} | "
            f"alpha ARS {_money(summary.get('swap_alpha_5d_ars'))}"
        ),
        "",
        "<b>RADAR POR OPERABILIDAD</b>",
        (
            f"   Operable/teorico: <b>{summary.get('radar_operable_closed_5d', 0)}</b> cerradas / "
            f"{summary.get('radar_operable_total', 0)} | avg {_pct(summary.get('radar_operable_avg_5d'))} | "
            f"PnL {_money(summary.get('radar_operable_pnl_5d_ars'))}"
        ),
        (
            f"   Bloqueado/vigilancia: <b>{summary.get('radar_blocked_closed_5d', 0)}</b> cerradas / "
            f"{summary.get('radar_blocked_total', 0)} | avg {_pct(summary.get('radar_blocked_avg_5d'))} | "
            f"PnL {_money(summary.get('radar_blocked_pnl_5d_ars'))}"
        ),
        "   Nota: separar esto evita que radar teórico infle conclusiones operativas.",
        "",
    ]

    if real:
        lines += ["<b>EJECUCIONES REALES RECIENTES</b>"]
        shown = 0
        for row in real:
            if shown >= 6:
                break
            pnl = row.get("pnl_5d_ars")
            outcome = row.get("outcome_5d")
            if pnl is None and shown >= 3:
                continue
            precision = _precision_label(row.get("execution_precision"))
            precision_txt = f" <code>{precision}</code>" if precision else ""
            lines.append(
                f"   {_fmt_dt(row.get('decided_at'))} <b>{escape(str(row.get('decision')))} {escape(str(row.get('ticker')))}</b> "
                f"{_money(row.get('amount_ars'))}{precision_txt} -> 5D {_pct(outcome)} / {_money(pnl)}"
            )
            shown += 1
        lines.append("")

    if plans:
        lines += ["<b>BOT VS HUMANO ECONOMICO</b>"]
        shown = 0
        for row in plans:
            if shown >= 8:
                break
            status = row.get("override_status") or "UNKNOWN"
            bot_pnl = row.get("bot_pnl_5d_ars")
            if bot_pnl is None and shown >= 4:
                continue
            precision = _precision_label(
                row.get("same_executed_at_precision")
                or row.get("opposite_executed_at_precision")
            )
            precision_txt = f" | <code>{precision}</code>" if precision else ""
            lines.append(
                f"   {_fmt_dt(row.get('decided_at'))} <b>{escape(str(row.get('decision')))} {escape(str(row.get('ticker')))}</b> "
                f"<code>{escape(str(status))}</code> target {_money(row.get('target_amount_ars'))} | "
                f"bot {_money(bot_pnl)} | humano {_money(row.get('human_pnl_5d_ars'))} | "
                f"delta {_money(row.get('human_vs_bot_5d_ars'))}{precision_txt}"
            )
            reason = row.get("reason")
            if reason and status in {"IGNORED", "PARTIAL", "OPPOSITE", "OVERFOLLOWED"}:
                lines.append(f"      Motivo: {escape(_clean_text(reason))[:160]}")
            shown += 1
        lines.append("")

    swap_rows = [row for row in radar if row.get("edge_vs")]
    if swap_rows:
        lines += ["<b>RADAR / SWAPS</b>"]
        swap_display = [
            row for row in swap_rows if row.get("swap_alpha_5d") is not None
        ] + [
            row for row in swap_rows if row.get("swap_alpha_5d") is None
        ]
        for row in swap_display[:8]:
            lines.append(
                f"   {_fmt_dt(row.get('decided_at'))} <b>{escape(str(row.get('ticker')))}</b> vs "
                f"<b>{escape(str(row.get('edge_vs')))}</b> | "
                f"cand {_pct(row.get('outcome_5d'))} vs base {_pct(row.get('edge_outcome_5d'))} | "
                f"alpha {_pct(row.get('swap_alpha_5d'))} / {_money(row.get('swap_alpha_5d_ars'))}"
            )
        lines.append("   Nota: swap mide candidato vs activo sacrificado guardado en edge_vs.")
        lines.append("")

    if pending:
        lines += ["<b>PENDIENTES VIVOS MARK-TO-LATEST</b>"]
        lines.append("   No son outcomes cerrados; muestran como vienen con ultimo precio guardado.")
        for row in pending[:8]:
            lines.append(
                f"   {_fmt_dt(row.get('decided_at'))} <b>{escape(str(row.get('decision')))} {escape(str(row.get('ticker')))}</b> "
                f"[{escape(str(row.get('source')))}] {_money(row.get('amount_ars'))} -> "
                f"{_pct(row.get('mark_return'))} / {_money(row.get('mark_pnl_ars'))}"
            )
        lines.append("")

    lines += [
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "<i>Ledger descriptivo: mide dinero y costo de oportunidad; no recalibra el cerebro del bot.</i>",
    ]
    return "\n".join(lines)

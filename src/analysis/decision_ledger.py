from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from html import escape
from zoneinfo import ZoneInfo

import asyncpg

from src.analysis.audit_scope import ensure_decision_audit_scope_columns
from src.analysis.override_classification import (
    classify_override,
    override_opposite_ratio as _opposite_ratio,
    override_same_ratio as _same_ratio,
    override_target as _target,
)
from src.analysis.plan_follow_reporting import (
    apply_plan_follow_overlay,
    fetch_plan_follow_reporting_data,
)
from src.analysis.plan_follow_attribution import (
    compute_execution_session_outcomes,
    fetch_canonical_outcome_candles,
    fetch_outcome_corporate_effects,
)
from src.core.telegram_format import (
    header as tg_header,
    note as tg_note,
    section as tg_section,
    validate_telegram_html,
)


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


def _signed_money(value) -> str:
    if value is None:
        return "N/A"
    number = float(value)
    if number > 0:
        rendered = f"+${number:,.0f}"
    elif number < 0:
        rendered = f"-${abs(number):,.0f}"
    else:
        rendered = "$0"
    return rendered.replace(",", ".")


def _result_icon(value) -> str:
    if value is None:
        return "⚪"
    number = _as_float(value)
    if number > 0:
        return "🟢"
    if number < 0:
        return "🔴"
    return "⚪"


def _status_display(value) -> tuple[str, str]:
    status = str(value or "UNKNOWN").upper()
    labels = {
        "FOLLOWED": ("✅", "SEGUIDO"),
        "OVERFOLLOWED": ("✅", "SOBRE-EJECUTADO"),
        "PARTIAL": ("🟡", "PARCIAL"),
        "IGNORED": ("⚪", "IGNORADO"),
        "OPPOSITE": ("🔄", "CONTRARIO"),
        "PENDING_OPEN": ("⏳", "PENDIENTE"),
    }
    return labels.get(status, ("⚪", status))


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
    await ensure_decision_audit_scope_columns(conn)

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
            COALESCE(run_intent, 'unknown') AS run_intent,
            COALESCE(decision_stage, 'idea') AS decision_stage,
            COALESCE(metric_scope, 'debug') AS metric_scope,
            price_at_decision,
            ABS(COALESCE(NULLIF(executed_amount_ars, 0), theoretical_amount_ars, 0)) AS amount_ars,
            layers#>>'{broker_fill,executed_at_precision}' AS execution_precision,
            layers#>>'{broker_fill,executed_at_source}' AS execution_timestamp_source,
            COALESCE(executable_outcome_5d, outcome_5d) AS outcome_5d,
            COALESCE(executable_outcome_10d, outcome_10d) AS outcome_10d,
            COALESCE(executable_outcome_20d, outcome_20d) AS outcome_20d
        FROM decision_log dl
        WHERE dl.decided_at >= NOW() - ($1::int * INTERVAL '1 day')
          AND ($2::bigint IS NULL OR dl.owner_chat_id = $2)
          AND dl.decision IN ('BUY', 'SELL')
          AND COALESCE(dl.outcome_basis, '') <> 'legacy_external'
          AND dl.is_primary_metric = TRUE
          AND COALESCE(dl.source, dl.layers->>'source') IN ('broker_movement', 'broker_fill')
          AND NOT EXISTS (
              SELECT 1
              FROM plan_execution_attribution_movements link
              JOIN plan_execution_attributions attribution
                ON attribution.id = link.attribution_id
               AND attribution.eligible_for_viability = TRUE
              JOIN broker_movements movement
                ON movement.id = link.broker_movement_id
              JOIN broker_fills bf
                ON bf.external_fill_id = movement.external_movement_id
              WHERE bf.decision_log_id = dl.id
                AND ($2::bigint IS NULL OR attribution.owner_chat_id = $2)
          )
        ORDER BY dl.decided_at DESC, dl.id DESC
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
                COALESCE(run_intent, 'formal_plan') AS run_intent,
                COALESCE(decision_stage, 'approved_decision') AS decision_stage,
                COALESCE(metric_scope, 'planner_audit') AS metric_scope,
                ABS(COALESCE(
                    NULLIF(layers->>'amount_ars', '')::numeric,
                    NULLIF(executed_amount_ars, 0),
                    theoretical_amount_ars,
                    0
                )) AS target_amount_ars,
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
              AND COALESCE(run_intent, 'formal_plan') = 'formal_plan'
              AND COALESCE(metric_scope, 'planner_audit') IN ('planner_audit', 'primary')
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
              AND NOT (COALESCE(bm.raw_payload, '{}'::jsonb) ? 'superseded_by_real')
              AND d.match_start_at IS NOT NULL
              AND (
                  (
                      bm.executed_at >= d.match_start_at
                      AND bm.executed_at < d.match_start_at + ($2::int * INTERVAL '1 day')
                  )
                  OR (
                      COALESCE(bm.executed_at_precision, 'unknown') = 'date_only'
                      AND
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
              AND NOT (COALESCE(bm.raw_payload, '{}'::jsonb) ? 'superseded_by_real')
              AND d.match_start_at IS NOT NULL
              AND (
                  (
                      bm.executed_at >= d.match_start_at
                      AND bm.executed_at < d.match_start_at + ($2::int * INTERVAL '1 day')
                  )
                  OR (
                      COALESCE(bm.executed_at_precision, 'unknown') = 'date_only'
                      AND
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

    attribution_data = await fetch_plan_follow_reporting_data(
        conn,
        days=days,
        owner_chat_id=owner_chat_id,
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
                COALESCE(run_intent, 'scheduled_context') AS run_intent,
                COALESCE(decision_stage, 'idea') AS decision_stage,
                COALESCE(metric_scope, 'radar_audit') AS metric_scope,
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
              AND COALESCE(metric_scope, 'radar_audit') = 'radar_audit'
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
        JOIN LATERAL (
            SELECT
                mp.ts,
                mp.last_price::float AS last_price
            FROM market_prices mp
            WHERE mp.ticker = dl.ticker
            ORDER BY mp.ts DESC
            LIMIT 1
        ) latest ON TRUE
        WHERE dl.decided_at >= NOW() - INTERVAL '10 days'
          AND ($1::bigint IS NULL OR dl.owner_chat_id = $1)
          AND dl.decision IN ('BUY', 'SELL')
          AND dl.price_at_decision IS NOT NULL
          AND dl.price_at_decision > 0
          AND COALESCE(dl.executable_outcome_5d, dl.outcome_5d) IS NULL
          AND COALESCE(dl.metric_scope, 'debug') IN ('primary', 'planner_audit', 'radar_audit')
        ORDER BY dl.decided_at DESC, dl.id DESC
        LIMIT 30
        """,
        owner_chat_id,
    )

    real = [dict(r) for r in real_rows]
    for operation in attribution_data.get("operations") or []:
        if not operation.get("eligible_for_viability"):
            continue
        real.append(
            {
                "id": -int(operation["attribution_id"]),
                "decided_at": operation.get("executed_at"),
                "ticker": operation.get("ticker"),
                "decision": operation.get("side"),
                "source": "plan_execution_attribution",
                "status": "EXECUTED_FOLLOWED",
                "decision_type": "followed_execution",
                "run_intent": "formal_plan",
                "decision_stage": "executed",
                "metric_scope": "followed_plan",
                "price_at_decision": operation.get("execution_price"),
                "amount_ars": (
                    operation.get("execution_notional_ars")
                    or operation.get("executed_amount_ars")
                ),
                "execution_precision": operation.get("executed_at_precision"),
                "execution_timestamp_source": operation.get("executed_at_source"),
                "outcome_5d": operation.get("outcome_5d"),
                "outcome_10d": operation.get("outcome_10d"),
                "outcome_20d": operation.get("outcome_20d"),
            }
        )
    real.sort(key=lambda row: row.get("decided_at") or datetime.min.replace(tzinfo=ART), reverse=True)
    plans = [dict(r) for r in plan_rows]
    radar = [dict(r) for r in radar_rows]
    pending = [dict(r) for r in pending_mark_rows]

    edge_tickers = sorted(
        {str(row.get("edge_vs") or "").upper() for row in radar if row.get("edge_vs")}
    )
    edge_start_days = [row.get("audit_start_day") for row in radar if row.get("edge_vs")]
    if edge_tickers and edge_start_days:
        edge_candles = await fetch_canonical_outcome_candles(
            conn,
            edge_tickers,
            since=min(edge_start_days),
        )
        edge_effects = await fetch_outcome_corporate_effects(
            conn,
            edge_tickers,
            since=min(edge_start_days),
        )
        for row in radar:
            edge_ticker = str(row.get("edge_vs") or "").upper()
            if not edge_ticker:
                continue
            for horizon in (5, 10, 20):
                row[f"edge_close_{horizon}d"] = None
                row[f"edge_outcome_{horizon}d"] = None

            start_day = row.get("audit_start_day")
            candles = edge_candles.get(edge_ticker, [])
            entry = next(
                (
                    candle
                    for candle in candles
                    if candle.get("ts") is not None
                    and candle["ts"].date() >= start_day
                ),
                None,
            )
            if entry is None:
                row["edge_entry_price"] = None
                continue

            entry_day = entry["ts"].date()
            entry_price = _as_float(entry.get("close_price"))
            row["edge_entry_price"] = entry_price
            edge_outcomes = compute_execution_session_outcomes(
                execution_price=entry_price,
                executed_at=datetime.combine(entry_day, time(12), tzinfo=ART),
                side="BUY",
                candles=candles,
                corporate_effects=edge_effects.get(edge_ticker, ()),
            )
            for horizon in (5, 10, 20):
                row[f"edge_close_{horizon}d"] = edge_outcomes.get(
                    f"outcome_price_{horizon}d"
                )
                row[f"edge_outcome_{horizon}d"] = edge_outcomes.get(
                    f"outcome_{horizon}d"
                )

    for row in real:
        for horizon in ("5d", "10d", "20d"):
            row[f"pnl_{horizon}_ars"] = _directional_pnl(
                row.get("amount_ars"),
                row.get(f"outcome_{horizon}"),
            )

    apply_plan_follow_overlay(plans, attribution_data.get("links_by_plan") or {})
    for row in plans:
        row["override_status"] = row.get("normalized_override_status") or classify_override(row)
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
            "followed_normalized": attribution_data.get("summary") or {},
        },
        "real_executions": [_row(r) for r in real[:60]],
        "bot_vs_human": [_row(r) for r in plans[:60]],
        "radar": [_row(r) for r in radar[:80]],
        "pending_mark": [_row(r) for r in pending[:30]],
    }


def render_decision_ledger(data: dict) -> str:
    summary = data.get("summary") or {}
    followed = summary.get("followed_normalized") or {}
    plans = data.get("bot_vs_human") or []
    real = data.get("real_executions") or []
    radar = data.get("radar") or []
    pending = data.get("pending_mark") or []
    pending_by_id = {row.get("id"): row for row in pending}

    lines = tg_header(
        "📒 Decision Ledger",
        subtitle=f"{int(data.get('days') or 0)}d · resultado 5D · lectura económica",
    ) + [
        tg_section("Resultados"),
        "💼 <b>Ejecución real</b>",
        (
            f"   {summary.get('real_closed_5d', 0)}/{summary.get('real_total', 0)} cerradas · "
            f"{_rate(summary.get('real_win_rate_5d'))} positivas"
        ),
        (
            f"   PnL direccional: {_result_icon(summary.get('real_pnl_5d_ars'))} "
            f"<b>{_signed_money(summary.get('real_pnl_5d_ars'))}</b>"
        ),
        "",
        "🧭 <b>Planes seguidos</b> <code>NORMALIZADO</code>",
        (
            f"   {followed.get('closed_5d', 0)}/{followed.get('eligible', 0)} maduros · "
            f"{_rate(followed.get('win_rate_5d'))} positivos"
        ),
        (
            f"   Retorno bruto {_pct(followed.get('avg_return_5d'))} · "
            f"PnL direccional bruto {_result_icon(followed.get('actual_pnl_5d_ars'))} "
            f"<b>{_signed_money(followed.get('actual_pnl_5d_ars'))}</b>"
        ),
        "",
        "🤖 <b>Bot vs ejecución humana</b> <code>PLAN-LEVEL</code>",
        f"   {summary.get('plans_closed_5d', 0)} planes maduros · no deduplicado",
        (
            f"   Bot {_signed_money(summary.get('bot_full_pnl_5d_ars'))} · "
            f"Humano {_signed_money(summary.get('human_matched_pnl_5d_ars'))}"
        ),
        f"   Delta humano-bot: <b>{_signed_money(summary.get('human_vs_bot_5d_ars'))}</b>",
        "",
        tg_section("Radar y swaps · TEÓRICO"),
        (
            f"🔎 Radar: {summary.get('radar_closed_5d', 0)}/{summary.get('radar_total', 0)} ideas · "
            f"retorno medio {_pct(summary.get('radar_avg_5d'))}"
        ),
        (
            f"   Operables {summary.get('radar_operable_closed_5d', 0)} · "
            f"{_pct(summary.get('radar_operable_avg_5d'))}  |  "
            f"Vigilancia {summary.get('radar_blocked_closed_5d', 0)} · "
            f"{_pct(summary.get('radar_blocked_avg_5d'))}"
        ),
        (
            f"🔁 Swaps: {summary.get('swap_closed_5d', 0)}/{summary.get('swap_total', 0)} · "
            f"alpha medio <b>{_pct(summary.get('swap_avg_alpha_5d'))}</b> · "
            f"{_signed_money(summary.get('swap_alpha_5d_ars'))}"
        ),
        "",
    ]

    closed_real = [row for row in real if row.get("outcome_5d") is not None]
    if closed_real:
        lines.append(tg_section("Últimas ejecuciones"))
        for row in closed_real[:4]:
            outcome = row.get("outcome_5d")
            lines.append(
                f"{_result_icon(outcome)} {_fmt_dt(row.get('decided_at'))} "
                f"<b>{escape(str(row.get('decision')))} {escape(str(row.get('ticker')))}</b> · "
                f"{_pct(outcome)} · {_signed_money(row.get('pnl_5d_ars'))}"
            )
        lines.append("")

    if plans:
        lines.append(tg_section("Planes recientes"))
        for row in plans[:5]:
            icon, label = _status_display(row.get("override_status"))
            mark = pending_by_id.get(row.get("id")) or {}
            if mark.get("mark_return") is not None:
                result = f"bot mark {_pct(mark.get('mark_return'))}"
            elif row.get("outcome_5d") is not None:
                result = f"bot 5D {_pct(row.get('outcome_5d'))}"
            else:
                result = "sin outcome maduro"
            lines.append(
                f"{icon} {_fmt_dt(row.get('decided_at'))} "
                f"<b>{escape(str(row.get('decision')))} {escape(str(row.get('ticker')))}</b> · "
                f"<code>{label}</code> · {result}"
            )
        lines.append("")

    mature_swaps = [row for row in radar if row.get("swap_alpha_5d") is not None]
    if mature_swaps:
        ranked = sorted(mature_swaps, key=lambda row: _as_float(row.get("swap_alpha_5d")), reverse=True)
        highlighted = ranked[:2]
        highlighted += [row for row in ranked[-2:] if row not in highlighted]
        lines.append(tg_section("Swaps destacados · 2 mejores / 2 peores"))
        for row in highlighted:
            alpha = row.get("swap_alpha_5d")
            lines.append(
                f"{_result_icon(alpha)} <b>{escape(str(row.get('ticker')))}</b> vs "
                f"{escape(str(row.get('edge_vs')))} · alpha {_pct(alpha)}"
            )
        lines.append("")

    lines += [
        tg_note("Bruto = antes de costos. Radar = teórico. Plan-level repite recomendaciones; normalizado deduplica operaciones."),
        tg_note("/viability muestra EV neto, costos y gates. Solo auditoría; no cambia decisiones."),
    ]
    report = "\n".join(lines)
    valid_html, errors = validate_telegram_html(report)
    if not valid_html:
        lines.append(tg_note(f"Advertencia interna: HTML con formato revisable ({'; '.join(errors[:2])})."))
        report = "\n".join(lines)
    return report

"""
Bot vs Humano audit.

Read-only report:
- compares approved execution_plan decisions vs real Cocos movements;
- classifies FOLLOWED / OVERFOLLOWED / PARTIAL / IGNORED / OPPOSITE;
- when outcomes exist, estimates whether following the bot or ignoring it
  would have helped over 5D/10D/20D.

It does not change analysis, thresholds, planner, fills or outcomes.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime
from html import escape
from zoneinfo import ZoneInfo

import asyncpg

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.collector.notifier import TelegramNotifier
from src.core.config import get_config


ART = ZoneInfo("America/Argentina/Buenos_Aires")


def _pct(value) -> str:
    if value is None:
        return "N/A"
    return f"{float(value):+.1%}"


def _money(value) -> str:
    if value is None:
        return "N/A"
    return f"${float(value):,.0f}".replace(",", ".")


def _fmt_dt(value) -> str:
    if not value:
        return "?"
    if isinstance(value, datetime):
        return value.astimezone(ART).strftime("%d/%m")
    return str(value)


def _as_float(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


STATUS_LABELS = {
    "FOLLOWED": "FOLLOWED",
    "OVERFOLLOWED": "OVERFOLLOWED",
    "PARTIAL": "PARTIAL",
    "IGNORED": "IGNORED",
    "OPPOSITE": "OPPOSITE",
}


def _target(row: dict) -> float:
    return max(_as_float(row.get("target_amount_ars")), 1.0)


def _same_ratio(row: dict) -> float:
    return _as_float(row.get("same_amount_ars")) / _target(row)


def _opposite_ratio(row: dict) -> float:
    return _as_float(row.get("opposite_amount_ars")) / _target(row)


def _classify(row: dict) -> str:
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


def _override_delta(status: str, bot_outcome: float | None) -> float | None:
    if bot_outcome is None:
        return None
    # Positive means the human override beat the bot instruction.
    if status == "IGNORED":
        return -float(bot_outcome)
    if status == "OPPOSITE":
        return -float(bot_outcome)
    if status in {"FOLLOWED", "OVERFOLLOWED"}:
        return 0.0
    if status == "PARTIAL":
        return -0.5 * float(bot_outcome)
    return None


def _mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _intent_level_summary(rows: list[dict]) -> dict:
    groups: dict[tuple[str, str], list[dict]] = {}
    for row in rows:
        key = (str(row.get("ticker") or "").upper(), str(row.get("decision") or "").upper())
        groups.setdefault(key, []).append(row)

    bot_returns: list[float] = []
    override_deltas: list[float] = []
    closed_intents = 0
    by_status: dict[str, int] = {}
    status_rank = {
        "OVERFOLLOWED": 5,
        "FOLLOWED": 4,
        "PARTIAL": 3,
        "OPPOSITE": 2,
        "IGNORED": 1,
    }

    for group_rows in groups.values():
        statuses = [str(r.get("override_status") or "UNKNOWN") for r in group_rows]
        dominant = max(statuses, key=lambda st: status_rank.get(st, 0)) if statuses else "UNKNOWN"
        by_status[dominant] = by_status.get(dominant, 0) + 1

        group_bot_returns = [
            float(r["outcome_5d"])
            for r in group_rows
            if r.get("outcome_5d") is not None
        ]
        group_deltas = [
            delta
            for r in group_rows
            if r.get("outcome_5d") is not None
            for delta in [_override_delta(str(r.get("override_status")), float(r["outcome_5d"]))]
            if delta is not None
        ]
        if group_bot_returns:
            closed_intents += 1
            bot_returns.append(_mean(group_bot_returns) or 0.0)
        if group_deltas:
            override_deltas.append(_mean(group_deltas) or 0.0)

    return {
        "total": len(groups),
        "closed_5d": closed_intents,
        "by_status": by_status,
        "avg_bot_5d": _mean(bot_returns),
        "avg_override_delta_5d": _mean(override_deltas),
    }


async def _fetch_audit_rows(
    conn: asyncpg.Connection,
    *,
    days: int,
    match_window_days: int,
    owner_chat_id: int | None,
) -> tuple[list[dict], list[dict]]:
    rows = await conn.fetch(
        """
        WITH decisions AS (
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
                        THEN (((decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date + INTERVAL '1 day') AT TIME ZONE 'America/Argentina/Buenos_Aires')
                    WHEN (decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::time < TIME '10:30'
                        THEN (((decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date) AT TIME ZONE 'America/Argentina/Buenos_Aires')
                    ELSE decided_at
                END AS match_start_at,
                layers->>'reason' AS reason
            FROM decision_log
            WHERE decided_at >= NOW() - ($1::int * INTERVAL '1 day')
              AND ($3::bigint IS NULL OR owner_chat_id = $3)
              AND COALESCE(source, layers->>'source') = 'execution_plan'
              AND status = 'APPROVED'
              AND decision_type = 'executable'
              AND decision IN ('BUY', 'SELL')
              AND price_at_decision IS NOT NULL
        )
        SELECT
            d.*,
            same_fill.first_at AS same_executed_at,
            same_fill.amount_ars AS same_amount_ars,
            opposite_fill.first_at AS opposite_executed_at,
            opposite_fill.amount_ars AS opposite_amount_ars
        FROM decisions d
        LEFT JOIN LATERAL (
            SELECT
                MIN(executed_at) AS first_at,
                SUM(ABS(COALESCE(amount, quantity * price, 0))) AS amount_ars
            FROM broker_movements bm
            WHERE bm.ticker = d.ticker
              AND bm.movement_type = d.decision
              AND bm.executed_at >= d.match_start_at
              AND bm.executed_at < d.match_start_at + ($2::int * INTERVAL '1 day')
              AND bm.quantity IS NOT NULL
              AND bm.price IS NOT NULL
        ) same_fill ON TRUE
        LEFT JOIN LATERAL (
            SELECT
                MIN(executed_at) AS first_at,
                SUM(ABS(COALESCE(amount, quantity * price, 0))) AS amount_ars
            FROM broker_movements bm
            WHERE bm.ticker = d.ticker
              AND bm.movement_type = CASE WHEN d.decision = 'BUY' THEN 'SELL' ELSE 'BUY' END
              AND bm.executed_at >= d.match_start_at
              AND bm.executed_at < d.match_start_at + ($2::int * INTERVAL '1 day')
              AND bm.quantity IS NOT NULL
              AND bm.price IS NOT NULL
        ) opposite_fill ON TRUE
        ORDER BY d.decided_at DESC
        """,
        days,
        match_window_days,
        owner_chat_id,
    )

    manual_only = await conn.fetch(
        """
        SELECT
            bm.executed_at,
            bm.ticker,
            bm.movement_type,
            ABS(COALESCE(bm.amount, bm.quantity * bm.price, 0)) AS amount_ars,
            bm.price
        FROM broker_movements bm
        WHERE bm.executed_at >= NOW() - ($1::int * INTERVAL '1 day')
          AND bm.movement_type IN ('BUY', 'SELL')
          AND bm.ticker IS NOT NULL
          AND bm.quantity IS NOT NULL
          AND bm.price IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM broker_fills bf
              JOIN decision_log linked
                ON linked.id = bf.decision_log_id
              WHERE bf.source = 'cocos_movements'
                AND bf.external_fill_id = bm.external_movement_id
                AND COALESCE(linked.source, linked.layers->>'source') = 'execution_plan'
                AND ($3::bigint IS NULL OR linked.owner_chat_id = $3)
          )
          AND NOT EXISTS (
              SELECT 1
              FROM decision_log dl
              WHERE dl.ticker = bm.ticker
                AND dl.decision = bm.movement_type
                AND COALESCE(dl.source, dl.layers->>'source') = 'execution_plan'
                AND dl.status = 'APPROVED'
                AND dl.decision_type = 'executable'
                AND ($3::bigint IS NULL OR dl.owner_chat_id = $3)
                AND (
                    CASE
                        WHEN dl.next_executable_at IS NOT NULL THEN dl.next_executable_at
                        WHEN (dl.decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::time >= TIME '17:00'
                            THEN (((dl.decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date + INTERVAL '1 day') AT TIME ZONE 'America/Argentina/Buenos_Aires')
                        WHEN (dl.decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::time < TIME '10:30'
                            THEN (((dl.decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date) AT TIME ZONE 'America/Argentina/Buenos_Aires')
                        ELSE dl.decided_at
                    END
                ) <= bm.executed_at
                AND (
                    CASE
                        WHEN dl.next_executable_at IS NOT NULL THEN dl.next_executable_at
                        WHEN (dl.decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::time >= TIME '17:00'
                            THEN (((dl.decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date + INTERVAL '1 day') AT TIME ZONE 'America/Argentina/Buenos_Aires')
                        WHEN (dl.decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::time < TIME '10:30'
                            THEN (((dl.decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date) AT TIME ZONE 'America/Argentina/Buenos_Aires')
                        ELSE dl.decided_at
                    END
                ) >= bm.executed_at - ($2::int * INTERVAL '1 day')
          )
        ORDER BY bm.executed_at DESC
        LIMIT 12
        """,
        days,
        match_window_days,
        owner_chat_id,
    )

    return [dict(r) for r in rows], [dict(r) for r in manual_only]


async def _fetch_activity_context(conn: asyncpg.Connection) -> dict:
    row = await conn.fetchrow(
        """
        WITH latest AS (
            SELECT
                (SELECT MAX(scraped_at) FROM portfolio_snapshots) AS latest_portfolio_at,
                (
                    SELECT MAX(executed_at)
                    FROM broker_movements
                    WHERE movement_type IN ('BUY', 'SELL')
                      AND quantity IS NOT NULL
                      AND price IS NOT NULL
                ) AS latest_broker_movement_at
        ),
        daily_snapshots AS (
            SELECT
                ps.snapshot_id,
                ps.cash_ars,
                STRING_AGG(
                    p.ticker || ':' || COALESCE(p.quantity::text, '0'),
                    ',' ORDER BY p.ticker
                ) AS positions_sig
            FROM portfolio_snapshots ps
            LEFT JOIN positions p ON p.snapshot_id = ps.snapshot_id
            WHERE (ps.scraped_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date =
                  (NOW() AT TIME ZONE 'America/Argentina/Buenos_Aires')::date
            GROUP BY ps.snapshot_id, ps.cash_ars
        )
        SELECT
            latest.latest_portfolio_at,
            latest.latest_broker_movement_at,
            COUNT(DISTINCT COALESCE(daily_snapshots.positions_sig, '') || '|cash:' || COALESCE(daily_snapshots.cash_ars::text, '0'))
                AS position_signatures_today
        FROM latest
        LEFT JOIN daily_snapshots ON TRUE
        GROUP BY latest.latest_portfolio_at, latest.latest_broker_movement_at
        """
    )
    return dict(row) if row else {}


async def _fetch_inferred_activity(conn: asyncpg.Connection, *, days: int = 7) -> list[dict]:
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
              AND bm.executed_at >= d.prev_scraped_at - INTERVAL '15 minutes'
              AND bm.executed_at <= d.scraped_at + INTERVAL '12 hours'
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
        LIMIT 12
        """,
        max(1, min(int(days), 30)),
    )
    return [dict(r) for r in rows]


def _summary(rows: list[dict]) -> dict:
    out = {
        "total": len(rows),
        "unique_intents": 0,
        "repeated_plans": 0,
        "closed_5d": 0,
        "by_status": {},
        "by_intent": {},
        "bot_wins_ignored": 0,
        "human_wins_ignored": 0,
        "avg_bot_5d": None,
        "avg_override_delta_5d": None,
    }
    bot_returns = []
    override_deltas = []
    intent_keys = set()

    for row in rows:
        intent_keys.add((row.get("ticker"), row.get("decision")))
        status = row["override_status"]
        out["by_status"][status] = out["by_status"].get(status, 0) + 1
        outcome = row.get("outcome_5d")
        if outcome is None:
            continue
        out["closed_5d"] += 1
        bot_returns.append(float(outcome))
        delta = _override_delta(status, float(outcome))
        if delta is not None:
            override_deltas.append(delta)
        if status in {"IGNORED", "OPPOSITE"}:
            if float(outcome) > 0:
                out["bot_wins_ignored"] += 1
            elif float(outcome) < 0:
                out["human_wins_ignored"] += 1

    if bot_returns:
        out["avg_bot_5d"] = sum(bot_returns) / len(bot_returns)
    if override_deltas:
        out["avg_override_delta_5d"] = sum(override_deltas) / len(override_deltas)
    out["unique_intents"] = len(intent_keys)
    out["repeated_plans"] = max(0, out["total"] - out["unique_intents"])
    out["by_intent"] = _intent_level_summary(rows)
    return out


def render_report(
    rows: list[dict],
    manual_only: list[dict],
    *,
    days: int,
    match_window_days: int,
    activity_context: dict | None = None,
    inferred_activity: list[dict] | None = None,
) -> str:
    for row in rows:
        row["override_status"] = _classify(row)

    summary = _summary(rows)
    by_status = summary["by_status"]
    by_intent = summary.get("by_intent") or {}
    by_intent_status = by_intent.get("by_status") or {}

    lines = [
        "🧑‍✈️ <b>BOT VS HUMANO</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"📅 Ultimos <b>{days}</b> dias | ventana match: <b>{match_window_days}d</b>",
        "",
        "<b>QUE MIDE</b>",
        "   Compara planes aprobados del bot contra movimientos reales Cocos.",
        "   No cambia analysis, optimizer, planner ni thresholds.",
        "",
        "<b>RESUMEN</b>",
        f"   Planes aprobados: <b>{summary['total']}</b>",
        f"   Intenciones unicas: <b>{summary['unique_intents']}</b> (ticker + lado) | repetidas: <b>{summary['repeated_plans']}</b>",
        f"   Cerrados 5D: <b>{summary['closed_5d']}</b> planes | <b>{by_intent.get('closed_5d', 0)}</b> intenciones",
        f"   FOLLOWED: {by_status.get('FOLLOWED', 0)} | OVER: {by_status.get('OVERFOLLOWED', 0)} | PARTIAL: {by_status.get('PARTIAL', 0)} | IGNORED: {by_status.get('IGNORED', 0)} | OPPOSITE: {by_status.get('OPPOSITE', 0)}",
        f"   Bot hipotetico 5D por plan: <b>{_pct(summary['avg_bot_5d'])}</b>",
        f"   Delta humano vs bot 5D por plan: <b>{_pct(summary['avg_override_delta_5d'])}</b>",
        f"   Bot hipotetico 5D por intencion: <b>{_pct(by_intent.get('avg_bot_5d'))}</b>",
        f"   Delta humano vs bot 5D por intencion: <b>{_pct(by_intent.get('avg_override_delta_5d'))}</b>",
        "   Nota: una recomendacion repetida en dias distintos cuenta como varios planes; por intencion pesa una sola vez.",
        "",
    ]

    ctx = activity_context or {}
    latest_portfolio_at = ctx.get("latest_portfolio_at")
    latest_movement_at = ctx.get("latest_broker_movement_at")
    signatures_today = int(ctx.get("position_signatures_today") or 0)
    if latest_portfolio_at and latest_movement_at:
        latest_portfolio_art = latest_portfolio_at.astimezone(ART)
        latest_movement_art = latest_movement_at.astimezone(ART)
        if latest_portfolio_art.date() > latest_movement_art.date() and signatures_today > 1:
            lines[8:8] = [
                "<b>AVISO DE SINCRONIZACION</b>",
                (
                    "   El portfolio cambio hoy, pero el ultimo movimiento canonico "
                    f"en Cocos movements es {_fmt_dt(latest_movement_at)}. "
                    "Los cambios de hoy pueden aparecer como IGNORED/MANUAL_ONLY pendiente hasta que Cocos publique el movimiento."
                ),
                "",
            ]

    if by_intent_status:
        lines += [
            "<b>LECTURA POR INTENCION</b>",
            f"   FOLLOWED: {by_intent_status.get('FOLLOWED', 0)} | OVER: {by_intent_status.get('OVERFOLLOWED', 0)} | PARTIAL: {by_intent_status.get('PARTIAL', 0)} | IGNORED: {by_intent_status.get('IGNORED', 0)} | OPPOSITE: {by_intent_status.get('OPPOSITE', 0)}",
            "   Esta vista reduce el peso de senales repetidas del mismo ticker/lado.",
            "",
        ]

    inferred = inferred_activity or []
    if inferred:
        pending_inferred = sum(1 for row in inferred if not row.get("confirmed_at"))
        lines += [
            "<b>ACTIVIDAD HUMANA INFERIDA</b>",
            (
                f"   {len(inferred)} cambios de cantidad detectados por snapshots "
                f"({pending_inferred} sin confirmar por Cocos movements)."
            ),
            "   Provisional: no entra al EV ni cambia FOLLOWED/IGNORED hasta tener movimiento canonico.",
        ]
        for row in inferred[:6]:
            status = "confirmado" if row.get("confirmed_at") else "inferido"
            lines.append(
                f"   {_fmt_dt(row.get('scraped_at'))} <b>{escape(str(row.get('side')))} {escape(str(row.get('ticker')))}</b> "
                f"{_as_float(row.get('quantity')):g} ≈ {_money(row.get('inferred_amount_ars'))} | {status}"
            )
        lines.append("")

    ignored_closed = summary["bot_wins_ignored"] + summary["human_wins_ignored"]
    if ignored_closed:
        lines += [
            "<b>IGNORADAS / CONTRARIAS CERRADAS</b>",
            f"   Bot habria tenido razon: <b>{summary['bot_wins_ignored']}</b>",
            f"   Humano evito/mejoro: <b>{summary['human_wins_ignored']}</b>",
            "   Muestra chica: usar como auditoria, no como regla.",
            "",
        ]
    else:
        lines += [
            "<b>LECTURA</b>",
            "   Todavia no hay suficientes overrides cerrados para juzgar.",
            "   Por ahora sirve para trazabilidad: que dijo el bot vs que hiciste.",
            "",
        ]

    recent = rows[:10]
    if recent:
        lines.append("<b>CASOS RECIENTES</b>")
        for row in recent:
            status = row["override_status"]
            target = _money(row.get("target_amount_ars"))
            same = _money(row.get("same_amount_ars")) if row.get("same_amount_ars") else "$0"
            same_ratio = _same_ratio(row)
            same_ratio_txt = f" ({same_ratio:.1f}x)" if same_ratio >= 0.15 else ""
            outcome = row.get("outcome_5d")
            result = "pendiente" if outcome is None else f"bot {_pct(outcome)}"
            human_delta = _override_delta(status, float(outcome)) if outcome is not None else None
            if human_delta is not None and status not in {"FOLLOWED", "OVERFOLLOWED"}:
                result += f" | humano-vs-bot {_pct(human_delta)}"
            lines.append(
                f"   {_fmt_dt(row.get('decided_at'))} <b>{escape(str(row.get('decision')))} {escape(str(row.get('ticker')))}</b> "
                f"<code>{status}</code> target {target} | mov. {same}{same_ratio_txt} -> {result}"
            )
            reason = row.get("reason")
            if reason and status in {"IGNORED", "OPPOSITE", "PARTIAL", "OVERFOLLOWED"}:
                lines.append(f"      Motivo bot: {escape(str(reason))[:180]}")
        lines.append("")

    if manual_only:
        lines.append("<b>MANUAL_ONLY RECIENTE</b>")
        lines.append("   Movimientos reales sin plan aprobado cercano del mismo lado.")
        for row in manual_only[:6]:
            lines.append(
                f"   {_fmt_dt(row.get('executed_at'))} <b>{escape(str(row.get('movement_type')))} {escape(str(row.get('ticker')))}</b> "
                f"{_money(row.get('amount_ars'))} @ {_money(row.get('price'))}"
            )
        lines.append("")

    lines += [
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "<i>Override Audit es descriptivo. No afirma edge ni modifica reglas.</i>",
    ]
    return "\n".join(lines)


async def async_main(args: argparse.Namespace) -> int:
    cfg = get_config()
    dsn = cfg.database.url.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    activity_context = {}
    inferred_activity = []
    try:
        rows, manual_only = await _fetch_audit_rows(
            conn,
            days=args.days,
            match_window_days=args.match_window_days,
            owner_chat_id=args.owner_chat_id,
        )
        activity_context = await _fetch_activity_context(conn)
        inferred_activity = await _fetch_inferred_activity(conn, days=min(args.days, 7))
    finally:
        await conn.close()

    report = render_report(
        rows,
        manual_only,
        days=args.days,
        match_window_days=args.match_window_days,
        activity_context=activity_context,
        inferred_activity=inferred_activity,
    )
    print(report)

    if not args.no_telegram and cfg.scraper.telegram_enabled:
        TelegramNotifier(cfg.scraper.telegram_bot_token, cfg.scraper.telegram_chat_id).send_raw(report)

    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Auditoria bot vs humano")
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--match-window-days", type=int, default=2)
    parser.add_argument("--owner-chat-id", type=int, default=None)
    parser.add_argument("--no-telegram", action="store_true")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(async_main(args)))


if __name__ == "__main__":
    main()

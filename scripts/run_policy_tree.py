#!/usr/bin/env python
"""
Operational Policy Tree report.

Read-only Telegram output that explains why the latest formal execution plan
ended in APPROVED, BLOCKED, WATCH or pending execution states. It does not run
analysis, change scores, persist decisions or update outcomes.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime
from html import escape
from typing import Any
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.audit_scope import ensure_decision_audit_scope_columns
from src.collector.db import PortfolioDatabase
from src.core.config import get_config
from src.core.telegram_format import validate_telegram_html


ART_TZ = ZoneInfo("America/Argentina/Buenos_Aires")


def _parse_layers(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    try:
        return dict(value)
    except Exception:
        return {}


def _fmt_dt(value: Any) -> str:
    if not value:
        return "N/A"
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except Exception:
            return str(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ART_TZ)
    return dt.astimezone(ART_TZ).strftime("%d/%m %H:%M")


def _fmt_pct(value: Any, *, signed: bool = False) -> str:
    try:
        v = float(value or 0.0)
    except Exception:
        v = 0.0
    return f"{v:+.1%}" if signed else f"{v:.1%}"


def _fmt_score(value: Any) -> str:
    try:
        return f"{float(value or 0.0):+.3f}"
    except Exception:
        return "+0.000"


def _fmt_money(value: Any) -> str:
    try:
        v = float(value or 0.0)
    except Exception:
        v = 0.0
    sign = "-" if v < 0 else ""
    return f"{sign}${abs(v):,.0f}".replace(",", ".")


def _source_counts(layers: dict[str, Any]) -> dict[str, int]:
    raw = layers.get("technical_candle_source_counts") or {}
    if not isinstance(raw, dict):
        return {}
    out: dict[str, int] = {}
    for key, value in raw.items():
        try:
            out[str(key)] = int(value or 0)
        except Exception:
            out[str(key)] = 0
    return out


def _data_gate(layers: dict[str, Any], price: Any) -> tuple[str, str]:
    counts = _source_counts(layers)
    total = sum(counts.values())
    canonical = counts.get("COCOS", 0) + counts.get("TRADINGVIEW_BYMA", 0)
    internal = counts.get("internal_snapshot", 0)
    price_ok = False
    try:
        price_ok = float(price or 0.0) > 0
    except Exception:
        price_ok = False

    if not price_ok:
        return "DATA_BLOCK", "sin precio válido"
    if total <= 0:
        return "DATA_WARNING", "sin conteo de velas en layers"

    internal_share = internal / total if total else 0.0
    if canonical >= 200 and internal_share <= 0.10:
        return "DATA_OK", f"{canonical}/{total} velas canónicas"
    if canonical >= 60:
        return "DATA_WARNING", f"{canonical}/{total} canónicas; fallback {internal_share:.0%}"
    return "DATA_BLOCK", f"solo {canonical} velas canónicas"


def _history_gate(layers: dict[str, Any]) -> tuple[str, str]:
    counts = _source_counts(layers)
    total = sum(counts.values())
    canonical = counts.get("COCOS", 0) + counts.get("TRADINGVIEW_BYMA", 0)
    if canonical >= 200:
        return "HISTORIA_ALTA", f"{canonical} velas canónicas"
    if canonical >= 60:
        return "HISTORIA_MEDIA", f"{canonical} velas canónicas"
    if total > 0:
        return "HISTORIA_CORTA", f"{canonical} canónicas / {total} total"
    return "HISTORIA_DESCONOCIDA", "sin metadata suficiente"


def _evidence_gate(row: dict[str, Any]) -> tuple[str, str]:
    n = int(row.get("similar_closed_5d") or 0)
    avg = row.get("similar_avg_5d")
    if n >= 50:
        label = "EDGE_VALIDABLE"
    elif n >= 20:
        label = "EDGE_PRELIMINAR"
    elif n > 0:
        label = "MUESTRA_CHICA"
    else:
        return "SIN_MUESTRA", "sin outcomes similares cerrados"
    return label, f"n={n} | avg 5D {_fmt_pct(avg, signed=True)}"


def _signal_gate(row: dict[str, Any], layers: dict[str, Any]) -> tuple[str, str]:
    decision = str(row.get("decision") or "").upper()
    score = float(row.get("final_score") or 0.0)
    risk = layers.get("risk") if isinstance(layers.get("risk"), dict) else {}
    risk_weighted = float(risk.get("weighted") or 0.0)

    if decision == "BUY":
        if score >= 0.13:
            return "STRONG_SIGNAL", f"BUY con score {_fmt_score(score)}"
        if score >= 0.08:
            return "VALID_SIGNAL", f"BUY con score {_fmt_score(score)}"
        return "WEAK_SIGNAL", f"BUY bajo umbral fuerte ({_fmt_score(score)})"

    if decision == "SELL":
        if score <= -0.08:
            return "NEGATIVE_SIGNAL", f"SELL por score {_fmt_score(score)}"
        if risk_weighted < -0.03:
            return "RISK_REDUCE", f"SELL con riesgo {risk_weighted:+.3f}"
        return "REBALANCE_SIGNAL", f"SELL/recorte con score {_fmt_score(score)}"

    return "NO_TRADE_SIGNAL", f"{decision or 'N/A'} con score {_fmt_score(score)}"


def _portfolio_gate(row: dict[str, Any], layers: dict[str, Any]) -> tuple[str, str]:
    current = row.get("current_weight")
    target = row.get("target_weight")
    delta = row.get("delta_weight")
    amount = row.get("executed_amount_ars") or row.get("theoretical_amount_ars")
    reason = str(layers.get("reason") or row.get("block_reason") or "").strip()

    try:
        current_f = float(current or 0.0)
        target_f = float(target or 0.0)
    except Exception:
        current_f = target_f = 0.0

    if row.get("was_blocked"):
        return "PORTFOLIO_BLOCK", str(row.get("block_reason") or "bloqueado")
    if abs(float(delta or 0.0)) < 0.015:
        return "PORTFOLIO_HOLD", f"peso estable {_fmt_pct(current_f)} → {_fmt_pct(target_f)}"
    label = "PORTFOLIO_OK"
    detail = f"{_fmt_pct(current_f)} → {_fmt_pct(target_f)} | {_fmt_money(amount)}"
    if reason:
        detail += f" | {escape(reason[:90])}"
    return label, detail


def _execution_gate(row: dict[str, Any]) -> tuple[str, str]:
    status = str(row.get("status") or "").upper()
    stage = str(row.get("decision_stage") or "").lower()
    scope = str(row.get("metric_scope") or "").lower()
    if status == "EXECUTED":
        return "EXECUTED", "fill confirmado; entra a ejecución real"
    if status == "BLOCKED" or row.get("was_blocked"):
        return "BLOCKED", str(row.get("block_reason") or "bloqueado por guards")
    if stage == "pending_open":
        return "PENDING_OPEN", "plan EOD; requiere apertura fresca"
    if status == "APPROVED":
        return "APPROVED_NO_FILL", "plan aprobado; falta fill confirmado"
    return status or "UNKNOWN", f"stage={stage or '-'} | scope={scope or '-'}"


def _result_gate(row: dict[str, Any]) -> tuple[str, str]:
    status = str(row.get("status") or "").upper()
    outcome = row.get("outcome_5d")
    if status == "EXECUTED":
        if outcome is None:
            return "PENDIENTE", "ejecución real sin outcome 5D cerrado"
        return "MEDIDO_5D", f"outcome 5D {_fmt_pct(outcome, signed=True)}"
    if outcome is not None:
        return "AUDITORIA", f"outcome 5D {_fmt_pct(outcome, signed=True)}; no EV real si no hubo fill"
    return "NO_EV_REAL", "no entra al EV real hasta fill confirmado"


def _decision_label(row: dict[str, Any]) -> str:
    status = str(row.get("status") or "").upper()
    decision = str(row.get("decision") or "").upper()
    if status == "BLOCKED":
        return "BLOQUEADO"
    if status == "EXECUTED":
        return "EJECUTADO"
    if status == "APPROVED":
        return "APROBADO SIN FILL"
    return decision or "SIN DECISIÓN"


async def _fetch_latest_policy_rows(days: int, limit: int, owner_chat_id: int | None) -> list[dict[str, Any]]:
    cfg = get_config()
    db = PortfolioDatabase(cfg.database.url)
    await db.connect()
    try:
        pool = await db.get_pool()
        if pool is None:
            return []
        async with pool.acquire() as conn:
            await ensure_decision_audit_scope_columns(conn)
            latest_run_id = await conn.fetchval(
                """
                SELECT run_id
                FROM decision_log
                WHERE decided_at >= NOW() - ($1::int * INTERVAL '1 day')
                  AND COALESCE(source, layers->>'source') = 'execution_plan'
                  AND COALESCE(run_intent, 'formal_plan') = 'formal_plan'
                  AND COALESCE(owner_chat_id, 0) = COALESCE($2::bigint, 0)
                  AND run_id IS NOT NULL
                ORDER BY decided_at DESC
                LIMIT 1
                """,
                days,
                owner_chat_id,
            )
            if latest_run_id:
                rows = await conn.fetch(
                    """
                    WITH base AS (
                        SELECT *
                        FROM decision_log
                        WHERE run_id = $1::uuid
                          AND COALESCE(source, layers->>'source') = 'execution_plan'
                          AND COALESCE(owner_chat_id, 0) = COALESCE($2::bigint, 0)
                    )
                    SELECT
                        b.id, b.decided_at, b.ticker, b.decision, b.status,
                        b.decision_type, b.decision_stage, b.metric_scope,
                        b.final_score, b.confidence, b.layers,
                        b.price_at_decision, b.current_weight, b.target_weight,
                        b.delta_weight, b.block_reason, b.theoretical_amount_ars,
                        b.executed_amount_ars, b.was_blocked, b.outcome_5d,
                        b.run_id,
                        (
                            SELECT COUNT(*)
                            FROM decision_log h
                            WHERE h.ticker = b.ticker
                              AND h.decision = b.decision
                              AND COALESCE(h.executable_outcome_5d, h.outcome_5d) IS NOT NULL
                              AND h.decided_at < b.decided_at
                        ) AS similar_closed_5d,
                        (
                            SELECT AVG(COALESCE(h.executable_outcome_5d, h.outcome_5d))
                            FROM decision_log h
                            WHERE h.ticker = b.ticker
                              AND h.decision = b.decision
                              AND COALESCE(h.executable_outcome_5d, h.outcome_5d) IS NOT NULL
                              AND h.decided_at < b.decided_at
                        ) AS similar_avg_5d
                    FROM base b
                    ORDER BY
                        CASE b.status WHEN 'APPROVED' THEN 0 WHEN 'BLOCKED' THEN 1 ELSE 2 END,
                        ABS(COALESCE(b.delta_weight, 0)) DESC,
                        b.ticker
                    LIMIT $3
                    """,
                    latest_run_id,
                    owner_chat_id,
                    limit,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT
                        b.id, b.decided_at, b.ticker, b.decision, b.status,
                        b.decision_type, b.decision_stage, b.metric_scope,
                        b.final_score, b.confidence, b.layers,
                        b.price_at_decision, b.current_weight, b.target_weight,
                        b.delta_weight, b.block_reason, b.theoretical_amount_ars,
                        b.executed_amount_ars, b.was_blocked, b.outcome_5d,
                        b.run_id,
                        0 AS similar_closed_5d,
                        NULL::float AS similar_avg_5d
                    FROM decision_log b
                    WHERE b.decided_at >= NOW() - ($1::int * INTERVAL '1 day')
                      AND COALESCE(b.source, b.layers->>'source') = 'execution_plan'
                      AND COALESCE(b.owner_chat_id, 0) = COALESCE($2::bigint, 0)
                    ORDER BY b.decided_at DESC, ABS(COALESCE(b.delta_weight, 0)) DESC
                    LIMIT $3
                    """,
                    days,
                    owner_chat_id,
                    limit,
                )
        return [dict(row) for row in rows]
    finally:
        await db.close()


def render_policy_tree(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return (
            "<b>ÁRBOL OPERATIVO</b>\n"
            "Sin planes formales recientes en decision_log.\n\n"
            "<i>No ejecuta análisis ni modifica métricas.</i>"
        )

    decided_at = rows[0].get("decided_at")
    run_id = rows[0].get("run_id")
    approved = sum(1 for r in rows if str(r.get("status") or "").upper() == "APPROVED")
    blocked = sum(1 for r in rows if str(r.get("status") or "").upper() == "BLOCKED" or r.get("was_blocked"))
    executed = sum(1 for r in rows if str(r.get("status") or "").upper() == "EXECUTED")

    lines = [
        "<b>ÁRBOL OPERATIVO</b>",
        "Lectura explicativa del último plan formal. No cambia decisiones ni thresholds.",
        "",
        f"Plan: <b>{_fmt_dt(decided_at)} ART</b>",
        f"Run: <code>{escape(str(run_id)[:8]) if run_id else 'sin_run'}</code>",
        f"Resumen: {approved} aprobadas sin fill | {blocked} bloqueadas | {executed} ejecutadas",
        "",
    ]

    for row in rows:
        layers = _parse_layers(row.get("layers"))
        data_label, data_detail = _data_gate(layers, row.get("price_at_decision"))
        history_label, history_detail = _history_gate(layers)
        evidence_label, evidence_detail = _evidence_gate(row)
        signal_label, signal_detail = _signal_gate(row, layers)
        portfolio_label, portfolio_detail = _portfolio_gate(row, layers)
        execution_label, execution_detail = _execution_gate(row)
        result_label, result_detail = _result_gate(row)

        ticker = escape(str(row.get("ticker") or "?").upper())
        decision = escape(str(row.get("decision") or "?").upper())
        label = escape(_decision_label(row))
        score = _fmt_score(row.get("final_score"))

        lines.extend(
            [
                f"<b>{ticker}</b> — {decision} | <b>{label}</b> | score <code>{score}</code>",
                f"Datos: <b>{escape(data_label)}</b> — {escape(data_detail)}",
                f"Historia: <b>{escape(history_label)}</b> — {escape(history_detail)}",
                f"Evidencia: <b>{escape(evidence_label)}</b> — {escape(evidence_detail)}",
                f"Señal: <b>{escape(signal_label)}</b> — {escape(signal_detail)}",
                f"Cartera: <b>{escape(portfolio_label)}</b> — {escape(portfolio_detail)}",
                f"Ejecución: <b>{escape(execution_label)}</b> — {escape(execution_detail)}",
                f"Resultado: <b>{escape(result_label)}</b> — {escape(result_detail)}",
                "",
            ]
        )

    lines.extend(
        [
            "<i>Policy Tree es explicativo: separa señal, cartera, ejecución y auditoría.</i>",
            "<i>No entra al EV real y no reemplaza performance ni ledger.</i>",
        ]
    )
    return "\n".join(lines)


async def async_main(args: argparse.Namespace) -> int:
    owner_chat_id = args.owner_chat_id
    rows = await _fetch_latest_policy_rows(args.days, args.limit, owner_chat_id)
    report = render_policy_tree(rows)
    valid, errors = validate_telegram_html(report)
    if not valid:
        print("WARNING policy_tree HTML potencialmente inválido:", errors[:3], file=sys.stderr)
    print(report)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Árbol operativo explicativo de decisiones formales")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--limit", type=int, default=8)
    parser.add_argument("--owner-chat-id", type=int, default=None)
    parser.add_argument("--no-telegram", action="store_true", help="Compatibilidad: imprime por stdout")
    args = parser.parse_args()
    return asyncio.run(async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())

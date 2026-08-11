from __future__ import annotations

import csv
from collections import Counter, defaultdict
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo

import asyncpg

from src.analysis.audit_scope import ensure_decision_audit_scope_columns
from src.analysis.execution_planner import FEE_PCT, SLIPPAGE_PCT
from src.core.telegram_format import header as tg_header
from src.core.telegram_format import html_text, note as tg_note, section as tg_section
from src.core.telegram_format import validate_telegram_html


ART = ZoneInfo("America/Argentina/Buenos_Aires")
HORIZONS = (5, 10, 20, 40)
ESTIMATED_COST_RATE = FEE_PCT + SLIPPAGE_PCT

SCOPE_LABELS = {
    "real_bot": "Bot ejecutado",
    "real_manual": "Manual real",
    "plan": "Plan sin fill",
    "blocked": "Bloqueada",
    "radar": "Radar",
    "theoretical": "Teorica",
    "audit": "Otra auditable",
}


def _float(value, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _serializable(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def classify_scope(row: dict) -> str:
    source = str(row.get("source") or "").lower()
    status = str(row.get("status") or "").upper()
    metric_scope = str(row.get("metric_scope") or "").lower()
    decision_type = str(row.get("decision_type") or "").lower()

    if source == "execution_plan" and status == "EXECUTED":
        return "real_bot"
    if source in {"broker_fill", "broker_movement"} and status in {"EXECUTED", "EXECUTED_MANUAL"}:
        return "real_manual"
    if source == "execution_plan" and status == "APPROVED":
        return "plan"
    if source == "execution_plan" and status == "BLOCKED":
        return "blocked"
    if source == "radar" or metric_scope == "radar_audit":
        return "radar"
    if source == "optimizer" or status == "THEORETICAL" or decision_type == "theoretical":
        return "theoretical"
    return "audit"


def enrich_decision(raw: dict) -> dict:
    row = {key: _serializable(value) for key, value in dict(raw).items()}
    scope = classify_scope(row)
    fill_count = int(row.get("fill_count") or 0)
    fee_count = int(row.get("fee_count") or 0)
    fill_amount = abs(_float(row.get("fill_amount_ars"), 0.0) or 0.0)
    stored_executed = abs(_float(row.get("executed_amount_ars"), 0.0) or 0.0)
    theoretical = abs(_float(row.get("theoretical_amount_ars"), 0.0) or 0.0)

    if scope in {"real_bot", "real_manual"}:
        amount = fill_amount or stored_executed or theoretical
    else:
        amount = theoretical or stored_executed

    has_complete_actual_fees = fill_count > 0 and fee_count == fill_count and amount > 0
    if has_complete_actual_fees:
        cost_ars = abs(_float(row.get("fill_fees_ars"), 0.0) or 0.0)
        cost_rate = cost_ars / amount
        cost_basis = "actual_fill_fees"
    else:
        cost_rate = ESTIMATED_COST_RATE
        cost_ars = amount * cost_rate if amount > 0 else None
        cost_basis = "estimated_policy"

    row.update(
        {
            "scope": scope,
            "scope_label": SCOPE_LABELS[scope],
            "amount_ars": amount or None,
            "cost_rate": cost_rate,
            "cost_ars": cost_ars,
            "cost_basis": cost_basis,
        }
    )

    for horizon in HORIZONS:
        gross = _float(row.get(f"outcome_{horizon}d"))
        net = gross - cost_rate if gross is not None else None
        row[f"gross_{horizon}d"] = gross
        row[f"net_{horizon}d"] = net
        row[f"net_pnl_{horizon}d_ars"] = amount * net if amount > 0 and net is not None else None
        row[f"net_win_{horizon}d"] = net > 0 if net is not None else None
    return row


def summarize_rows(rows: Iterable[dict]) -> dict:
    materialized = list(rows)
    summary: dict = {"decisions": len(materialized)}
    for horizon in HORIZONS:
        mature = [row for row in materialized if row.get(f"net_{horizon}d") is not None]
        weighted = [row for row in mature if (_float(row.get("amount_ars"), 0.0) or 0.0) > 0]
        total_amount = sum(float(row["amount_ars"]) for row in weighted)
        if total_amount > 0:
            net_return = sum(
                float(row["amount_ars"]) * float(row[f"net_{horizon}d"])
                for row in weighted
            ) / total_amount
            gross_return = sum(
                float(row["amount_ars"]) * float(row[f"gross_{horizon}d"])
                for row in weighted
            ) / total_amount
        elif mature:
            net_return = sum(float(row[f"net_{horizon}d"]) for row in mature) / len(mature)
            gross_return = sum(float(row[f"gross_{horizon}d"]) for row in mature) / len(mature)
        else:
            net_return = None
            gross_return = None

        pnl_values = [row[f"net_pnl_{horizon}d_ars"] for row in mature]
        pnl_values = [float(value) for value in pnl_values if value is not None]
        summary[f"n_{horizon}d"] = len(mature)
        summary[f"avg_gross_{horizon}d"] = (
            sum(float(row[f"gross_{horizon}d"]) for row in mature) / len(mature)
            if mature else None
        )
        summary[f"avg_net_{horizon}d"] = (
            sum(float(row[f"net_{horizon}d"]) for row in mature) / len(mature)
            if mature else None
        )
        summary[f"hit_{horizon}d"] = (
            sum(1 for row in mature if row[f"net_win_{horizon}d"]) / len(mature)
            if mature else None
        )
        summary[f"gross_{horizon}d"] = gross_return
        summary[f"net_{horizon}d"] = net_return
        summary[f"net_pnl_{horizon}d_ars"] = sum(pnl_values) if pnl_values else None
    return summary


def aggregate_runs(rows: Iterable[dict]) -> list[dict]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        run_id = str(row.get("run_id") or "").strip()
        if run_id:
            grouped[run_id].append(row)

    runs: list[dict] = []
    for run_id, run_rows in grouped.items():
        summary = summarize_rows(run_rows)
        decided_at = max(str(row.get("decided_at") or "") for row in run_rows)
        scopes = Counter(str(row.get("scope") or "audit") for row in run_rows)
        summary.update(
            {
                "run_id": run_id,
                "decided_at": decided_at,
                "scopes": dict(scopes),
                "tickers": sorted({str(row.get("ticker") or "") for row in run_rows}),
            }
        )
        runs.append(summary)
    return sorted(runs, key=lambda row: row["decided_at"], reverse=True)


async def fetch_net_decision_report(
    conn: asyncpg.Connection,
    *,
    days: int = 180,
    owner_chat_id: int | None = None,
) -> dict:
    await ensure_decision_audit_scope_columns(conn)
    records = await conn.fetch(
        """
        WITH fill_totals AS (
            SELECT
                decision_log_id,
                COUNT(*) AS fill_count,
                COUNT(fees_ars) AS fee_count,
                SUM(ABS(COALESCE(gross_amount_ars, quantity * avg_fill_price))) AS fill_amount_ars,
                SUM(COALESCE(fees_ars, 0)) AS fill_fees_ars
            FROM broker_fills
            WHERE decision_log_id IS NOT NULL
              AND NOT (COALESCE(raw_payload, '{}'::jsonb) ? 'superseded_by_real')
            GROUP BY decision_log_id
        )
        SELECT
            d.id,
            d.decided_at,
            d.run_id,
            d.ticker,
            d.decision,
            d.final_score,
            d.price_at_decision,
            COALESCE(d.source, d.layers->>'source') AS source,
            COALESCE(d.status, 'UNKNOWN') AS status,
            COALESCE(d.decision_type, 'unknown') AS decision_type,
            COALESCE(d.run_intent, 'unknown') AS run_intent,
            COALESCE(d.decision_stage, 'idea') AS decision_stage,
            COALESCE(d.metric_scope, 'debug') AS metric_scope,
            COALESCE(d.is_primary_metric, FALSE) AS is_primary_metric,
            d.theoretical_amount_ars,
            d.executed_amount_ars,
            d.outcome_basis,
            COALESCE(d.executable_outcome_5d, d.outcome_5d) AS outcome_5d,
            COALESCE(d.executable_outcome_10d, d.outcome_10d) AS outcome_10d,
            COALESCE(d.executable_outcome_20d, d.outcome_20d) AS outcome_20d,
            COALESCE(d.executable_outcome_40d, d.outcome_40d) AS outcome_40d,
            COALESCE(f.fill_count, 0) AS fill_count,
            COALESCE(f.fee_count, 0) AS fee_count,
            f.fill_amount_ars,
            f.fill_fees_ars
        FROM decision_log d
        LEFT JOIN fill_totals f ON f.decision_log_id = d.id
        WHERE d.decided_at >= NOW() - ($1::int * INTERVAL '1 day')
          AND ($2::bigint IS NULL OR d.owner_chat_id = $2)
          AND d.decision IN ('BUY', 'SELL')
          AND COALESCE(d.outcome_basis, '') <> 'legacy_external'
          AND NOT EXISTS (
              SELECT 1
              FROM broker_fills superseded
              WHERE superseded.decision_log_id = d.id
                AND COALESCE(superseded.raw_payload, '{}'::jsonb) ? 'superseded_by_real'
                AND NOT EXISTS (
                    SELECT 1
                    FROM broker_fills live_fill
                    WHERE live_fill.decision_log_id = d.id
                      AND NOT (COALESCE(live_fill.raw_payload, '{}'::jsonb) ? 'superseded_by_real')
                )
          )
        ORDER BY d.decided_at DESC, d.id DESC
        """,
        days,
        owner_chat_id,
    )
    rows = [enrich_decision(dict(record)) for record in records]
    scopes = {
        scope: summarize_rows(row for row in rows if row["scope"] == scope)
        for scope in SCOPE_LABELS
    }
    actual_fee_rows = sum(1 for row in rows if row["cost_basis"] == "actual_fill_fees")
    return {
        "days": days,
        "rows": rows,
        "runs": aggregate_runs(rows),
        "scopes": scopes,
        "actual_fee_rows": actual_fee_rows,
        "estimated_fee_rows": len(rows) - actual_fee_rows,
        "with_run_id": sum(1 for row in rows if row.get("run_id")),
        "generated_at": datetime.now(ART).isoformat(),
    }


def _pct(value, *, signed: bool = True) -> str:
    if value is None:
        return "N/A"
    return f"{float(value):+0.1%}" if signed else f"{float(value):0.1%}"


def _money(value) -> str:
    if value is None:
        return "N/A"
    return f"${float(value):+,.0f}".replace(",", ".")


def _fmt_dt(value) -> str:
    try:
        parsed = datetime.fromisoformat(str(value))
        if parsed.tzinfo:
            parsed = parsed.astimezone(ART)
        return parsed.strftime("%d/%m %H:%M")
    except (TypeError, ValueError):
        return str(value or "N/A")


def _horizon_line(summary: dict, horizon: int) -> str:
    n = int(summary.get(f"n_{horizon}d") or 0)
    if not n:
        return f"{horizon}r: sin outcomes maduros"
    return (
        f"{horizon}r: capital <b>{_pct(summary.get(f'net_{horizon}d'))}</b> · "
        f"EV {_pct(summary.get(f'avg_net_{horizon}d'))} · "
        f"acierto <b>{_pct(summary.get(f'hit_{horizon}d'), signed=False)}</b> · "
        f"N={n} · {_money(summary.get(f'net_pnl_{horizon}d_ars'))} ARS"
    )


def render_net_decision_report(data: dict, *, limit_runs: int = 6) -> str:
    rows = data.get("rows") or []
    lines = tg_header(
        "RESULTADO NETO POR ANALISIS",
        subtitle=f"Ventana {int(data.get('days') or 0)} dias; lectura de decision_log y fills",
    )
    lines.append(
        f"Decisiones: <b>{len(rows)}</b> · con corrida: <b>{int(data.get('with_run_id') or 0)}</b> · "
        f"fills con fees reales: <b>{int(data.get('actual_fee_rows') or 0)}</b>"
    )
    lines.append(
        tg_note(
            f"Neto = outcome direccional menos costo. Sin fee real se estima {ESTIMATED_COST_RATE:.2%} "
            f"({FEE_PCT:.2%} fee + {SLIPPAGE_PCT:.2%} slippage)."
        )
    )

    lines.extend(["", tg_section("EJECUCION REAL")])
    real_scopes = [
        (scope, data.get("scopes", {}).get(scope, {}))
        for scope in ("real_bot", "real_manual")
    ]
    if not any(summary.get("decisions") for _, summary in real_scopes):
        lines.append("Sin ejecuciones reales en la ventana.")
    for scope, summary in real_scopes:
        if not summary.get("decisions"):
            continue
        lines.append(f"<b>{html_text(SCOPE_LABELS[scope])}</b> · {int(summary['decisions'])} registros")
        lines.extend(_horizon_line(summary, horizon) for horizon in HORIZONS)

    lines.extend(["", tg_section("SEPARACION DE EVIDENCIA")])
    for scope in ("plan", "blocked", "radar", "theoretical"):
        summary = data.get("scopes", {}).get(scope, {})
        decisions = int(summary.get("decisions") or 0)
        mature_5d = int(summary.get("n_5d") or 0)
        if decisions:
            lines.append(
                f"{html_text(SCOPE_LABELS[scope])}: <b>{decisions}</b> · "
                f"5r neto {_pct(summary.get('net_5d'))} · N={mature_5d}"
            )

    all_runs = data.get("runs") or []
    pending_runs = [run for run in all_runs if not int(run.get("n_5d") or 0)]
    lines.extend(["", tg_section("ULTIMAS CORRIDAS CON RESULTADO")])
    runs = [run for run in all_runs if int(run.get("n_5d") or 0)][: max(1, limit_runs)]
    if not runs:
        lines.append("Sin corridas con outcomes maduros en la ventana.")
    for run in runs:
        scope_text = "/".join(
            f"{SCOPE_LABELS.get(scope, scope)} {count}"
            for scope, count in sorted((run.get("scopes") or {}).items())
        )
        net_values = " · ".join(
            f"{horizon}r {_pct(run.get(f'net_{horizon}d'))}"
            if run.get(f"n_{horizon}d") else f"{horizon}r pendiente"
            for horizon in HORIZONS
        )
        lines.append(
            f"<b>{_fmt_dt(run.get('decided_at'))}</b> · {int(run.get('decisions') or 0)} decisiones "
            f"[{html_text(scope_text)}]\n{net_values}"
        )

    if pending_runs:
        lines.append("")
        lines.append(
            f"Corridas aun sin outcome 5r: <b>{len(pending_runs)}</b> · "
            f"ultima {_fmt_dt(pending_runs[0].get('decided_at'))}"
        )

    lines.extend(
        [
            "",
            tg_note(
                "Bot ejecutado, manual, plan, bloqueada, radar y teorica se calculan por separado. "
                "En SELL el ARS es atribucion por la decision, no PnL contable realizado. "
                "Fills reemplazados y corridas sin BUY/SELL no generan una fila."
            ),
        ]
    )
    report = "\n".join(lines).strip()
    valid, errors = validate_telegram_html(report)
    if not valid:
        raise ValueError(f"Invalid Telegram HTML: {errors[:3]}")
    return report


CSV_FIELDS = [
    "decided_at", "run_id", "id", "ticker", "decision", "scope_label", "source", "status",
    "metric_scope", "decision_stage", "final_score", "price_at_decision", "amount_ars",
    "cost_basis", "cost_rate", "cost_ars", "fill_count", "fee_count",
] + [
    field
    for horizon in HORIZONS
    for field in (
        f"gross_{horizon}d", f"net_{horizon}d", f"net_pnl_{horizon}d_ars", f"net_win_{horizon}d"
    )
]


def write_decision_csv(rows: Iterable[dict], path: str | Path) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=CSV_FIELDS, delimiter=";")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in CSV_FIELDS})
    return target


RUN_CSV_FIELDS = ["decided_at", "run_id", "decisions", "scope_counts", "tickers"] + [
    field
    for horizon in HORIZONS
    for field in (
        f"n_{horizon}d", f"hit_{horizon}d", f"avg_gross_{horizon}d",
        f"avg_net_{horizon}d", f"gross_{horizon}d", f"net_{horizon}d",
        f"net_pnl_{horizon}d_ars",
    )
]


def write_run_csv(runs: Iterable[dict], path: str | Path) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=RUN_CSV_FIELDS, delimiter=";")
        writer.writeheader()
        for run in runs:
            row = dict(run)
            row["scope_counts"] = " | ".join(
                f"{SCOPE_LABELS.get(scope, scope)}={count}"
                for scope, count in sorted((run.get("scopes") or {}).items())
            )
            row["tickers"] = " | ".join(run.get("tickers") or [])
            writer.writerow({field: row.get(field) for field in RUN_CSV_FIELDS})
    return target

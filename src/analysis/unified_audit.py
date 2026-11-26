"""/auditoria: one report over one data layer.

Every section uses the same frame (``load_viability_decision_log``: canonical
outcome basis, executable outcome first, synthetic duplicate movements and
plan-attributed manual trades flagged), the same cost, the same 5d/20d
horizons, the same cohort definitions and the same 95% date-block ranges
(``audit_inference``). The older commands (/viability, /neto, /ledger,
/override, /performance, /confianza, /dcl, /regression) stay as drill-downs.

Read-only: it does not change decisions, thresholds or stored outcomes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from html import escape
from typing import Optional

import asyncpg
import numpy as np
import pandas as pd

from src.analysis.audit_inference import (
    aligned_scores,
    decision_dates,
    difference_interval,
    evidence,
    fmt_interval,
    rank_ic_interval,
)
from src.analysis.viability_audit import (
    GATE_SHORT_NAMES,
    HorizonMetrics,
    ViabilityAuditConfig,
    ViabilityAuditReport,
    VERDICT_ICONS,
    _attach_benchmark_returns,
    _compute_horizon_metrics,
    _prepare_frame,
    _scope_mask,
    load_benchmark_candles,
    load_viability_decision_log,
    run_viability_audit_sync,
)

ART = "America/Argentina/Buenos_Aires"
HORIZONS = ("5d", "20d")
COHORTS = (
    ("bot", "🤖 Bot ejecutado"),
    ("followed", "🧭 Seguido"),
    ("manual", "👤 Manual"),
    ("plan", "📋 Plan no ejecutado"),
    ("blocked", "🛡 Bloqueada"),
    ("radar", "🔎 Radar"),
)
# Ideas repeat run after run; count each ticker + side + local day once.
DEDUPED_COHORTS = ("plan", "blocked", "radar")


@dataclass
class CohortMetrics:
    cohort: str
    label: str
    rows: int
    by_horizon: dict[str, HorizonMetrics]


@dataclass
class UnifiedAuditReport:
    generated_at: datetime
    days: int
    cost_bps: float
    viability: ViabilityAuditReport
    cohorts: list[CohortMetrics]
    plan_vs_manual: Optional[dict]
    calibration: dict[str, dict[str, Optional[dict]]]
    health: dict
    alerts: list[str] = field(default_factory=list)


def cohort_mask(df: pd.DataFrame, cohort: str) -> pd.Series:
    source = df["source"].fillna("").astype(str).str.lower()
    status = df["status"].fillna("").astype(str).str.upper()
    metric_scope = df.get("metric_scope", pd.Series("", index=df.index)).fillna("").astype(str).str.lower()
    if cohort == "bot":
        return _scope_mask(df, "bot_only")
    if cohort == "followed":
        return _scope_mask(df, "followed")
    if cohort == "manual":
        return _scope_mask(df, "manual_only")
    if cohort == "plan":
        return source.eq("execution_plan") & status.eq("APPROVED")
    if cohort == "blocked":
        return source.eq("execution_plan") & status.eq("BLOCKED")
    if cohort == "radar":
        return source.eq("radar") | metric_scope.eq("radar_audit")
    raise ValueError(f"Unsupported cohort: {cohort}")


def dedupe_intentions(df: pd.DataFrame) -> pd.DataFrame:
    """Latest row per ticker + side + local decision day."""
    if df.empty or "decided_at" not in df.columns:
        return df
    data = df.copy()
    data["_decided"] = pd.to_datetime(data["decided_at"], utc=True, errors="coerce")
    data["_day"] = decision_dates(data["decided_at"]).values
    data["_ticker"] = data.get("ticker", pd.Series("", index=data.index)).astype(str).str.upper()
    data = data.sort_values("_decided", ascending=False, kind="stable")
    data = data.drop_duplicates(subset=["_ticker", "decision", "_day"], keep="first")
    return data.drop(columns=["_decided", "_day", "_ticker"])


def compute_unified_audit(
    frame: pd.DataFrame,
    config: ViabilityAuditConfig,
    *,
    health: Optional[dict] = None,
) -> UnifiedAuditReport:
    viability = run_viability_audit_sync(frame, config)
    cost = float(config.cost_bps) / 10_000.0
    horizons = tuple(h for h in HORIZONS if h in config.horizons)
    df = _prepare_frame(frame, config.horizons) if not frame.empty else pd.DataFrame()
    if config.benchmark_ticker and not df.empty:
        _attach_benchmark_returns(df, list(frame.attrs.get("benchmark_candles") or []), config.horizons)

    cohorts: list[CohortMetrics] = []
    subsets: dict[str, pd.DataFrame] = {}
    for cohort, label in COHORTS:
        subset = df[cohort_mask(df, cohort)] if not df.empty else df
        if cohort in DEDUPED_COHORTS:
            subset = dedupe_intentions(subset)
        subsets[cohort] = subset
        cohorts.append(
            CohortMetrics(
                cohort=cohort,
                label=label,
                rows=int(len(subset)),
                by_horizon={
                    h: _compute_horizon_metrics(subset, scope=cohort, horizon=h, cost=cost)
                    for h in horizons
                },
            )
        )

    plan_vs_manual = None
    if "5d" in horizons and not df.empty:
        plan, manual = subsets["plan"], subsets["manual"]
        plan_vs_manual = difference_interval(
            decision_dates(plan.get("decided_at", pd.Series(dtype=object))),
            pd.to_numeric(plan.get("effective_outcome_5d"), errors="coerce") - cost,
            decision_dates(manual.get("decided_at", pd.Series(dtype=object))),
            pd.to_numeric(manual.get("effective_outcome_5d"), errors="coerce") - cost,
            "5d",
        )

    calibration: dict[str, dict[str, Optional[dict]]] = {}
    if not df.empty:
        bot_ideas = pd.concat([subsets["bot"], subsets["plan"], subsets["blocked"]], ignore_index=True)
        for name, subset in (("bot", bot_ideas), ("radar", subsets["radar"])):
            calibration[name] = {}
            for h in horizons:
                if subset.empty:
                    calibration[name][h] = None
                    continue
                calibration[name][h] = rank_ic_interval(
                    decision_dates(subset["decided_at"]),
                    aligned_scores(subset["final_score"], subset["decision"]),
                    subset[f"effective_outcome_{h}"],
                    h,
                )

    health = dict(health or {})
    duplicates = int(
        frame.get("duplicate_movement", pd.Series(dtype=bool)).astype("boolean").fillna(False).sum()
    ) if not frame.empty else 0
    health["duplicate_movements"] = duplicates
    health["benchmark_candles"] = len(frame.attrs.get("benchmark_candles") or [])
    report = UnifiedAuditReport(
        generated_at=datetime.now(tz=timezone.utc),
        days=config.days,
        cost_bps=config.cost_bps,
        viability=viability,
        cohorts=cohorts,
        plan_vs_manual=plan_vs_manual,
        calibration=calibration,
        health=health,
    )
    report.alerts = build_alerts(report, config)
    return report


def build_alerts(report: UnifiedAuditReport, config: ViabilityAuditConfig) -> list[str]:
    health = report.health
    alerts: list[str] = []
    if config.benchmark_ticker and not health.get("benchmark_candles"):
        alerts.append(f"Sin velas de {config.benchmark_ticker} CEDEAR ARS: no se mide exceso.")
    if health.get("duplicate_movements"):
        alerts.append(
            f"{health['duplicate_movements']} movimientos manuales duplicados excluidos de las metricas."
        )
    fills, with_fees = health.get("fills"), health.get("fills_with_fees")
    if fills and not with_fees:
        alerts.append(f"0 de {fills} fills guardan comision: el costo es estimado (0.75%).")
    follow = health.get("follow_status") or {}
    total_follow = sum(follow.values())
    if total_follow >= 5 and follow.get("OVERFOLLOWED", 0) == total_follow:
        alerts.append(
            f"{total_follow}/{total_follow} atribuciones son OVER: revisar si se suman varias operaciones contra un solo plan."
        )
    stale = health.get("executed_missing_5d")
    if stale:
        alerts.append(f"{stale} operaciones ejecutadas con mas de 8 dias siguen sin outcome 5D.")
    if report.viability.freeze_note and "MODIFICADAS" in report.viability.freeze_note:
        alerts.append(report.viability.freeze_note)
    if not report.viability.frozen_since:
        alerts.append("Reglas sin congelar: no hay muestra prospectiva (scripts/freeze_decision_rules.py --freeze).")
    last_price = health.get("last_market_price_at")
    if isinstance(last_price, datetime) and datetime.now(tz=timezone.utc) - last_price > timedelta(days=4):
        alerts.append(f"Ultimo precio de mercado {last_price:%d/%m %H:%M} UTC: datos viejos.")
    return alerts


async def fetch_health(database_url: str, *, days: int) -> dict:
    """Cheap pipeline checks. Each query fails alone; a missing table never sinks the report."""
    dsn = database_url.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    health: dict = {}

    async def run(key: str, sql: str, *args, many: bool = False):
        try:
            health[key] = await (conn.fetch(sql, *args) if many else conn.fetchval(sql, *args))
        except Exception as exc:  # noqa: BLE001 - reported, not raised
            health.setdefault("errors", []).append(f"{key}: {type(exc).__name__}")

    try:
        await run("last_market_price_at", "SELECT MAX(ts) FROM market_prices")
        await run("fills", "SELECT COUNT(*) FROM broker_fills WHERE executed_at >= NOW() - ($1::int * INTERVAL '1 day')", days)
        await run(
            "fills_with_fees",
            "SELECT COUNT(*) FROM broker_fills WHERE executed_at >= NOW() - ($1::int * INTERVAL '1 day') AND COALESCE(fees_ars, 0) > 0",
            days,
        )
        await run(
            "executed_missing_5d",
            """
            SELECT COUNT(*) FROM decision_log
            WHERE status IN ('EXECUTED', 'EXECUTED_MANUAL', 'EXECUTED_FOLLOWED')
              AND decided_at < NOW() - INTERVAL '8 days'
              AND decided_at >= NOW() - ($1::int * INTERVAL '1 day')
              AND COALESCE(outcome_basis, '') <> 'legacy_external'
              AND outcome_5d IS NULL AND executable_outcome_5d IS NULL
            """,
            days,
        )
        await run(
            "follow_rows",
            """
            SELECT follow_status, COUNT(*) AS n FROM plan_execution_attributions
            WHERE eligible_for_viability AND plan_decided_at >= NOW() - ($1::int * INTERVAL '1 day')
            GROUP BY follow_status
            """,
            days,
            many=True,
        )
    finally:
        await conn.close()
    health["follow_status"] = {str(r["follow_status"]): int(r["n"]) for r in health.pop("follow_rows", None) or []}
    return health


async def run_unified_audit(config: ViabilityAuditConfig) -> UnifiedAuditReport:
    frame = await load_viability_decision_log(config)
    if config.benchmark_ticker:
        try:
            frame.attrs["benchmark_candles"] = await load_benchmark_candles(config)
        except Exception as exc:  # noqa: BLE001 - the audit runs without a benchmark
            frame.attrs["benchmark_error"] = f"{type(exc).__name__}: {exc}"[:160]
    health = await fetch_health(config.database_url, days=config.days)
    return compute_unified_audit(frame, config, health=health)


# ── Rendering ───────────────────────────────────────────────────────────────


def _pct(value: Optional[float]) -> str:
    return "-" if value is None or not np.isfinite(value) else f"{value * 100:+.1f}%"


def _num(value: Optional[float]) -> str:
    return "-" if value is None or not np.isfinite(value) else f"{value:+.2f}"


def _range(result: Optional[dict], fmt) -> str:
    text = fmt_interval(result, fmt)
    return "" if "n/d" in text else f" {text}"


def _mark(result: Optional[dict]) -> str:
    state = evidence(result)
    return "✅" if state is True else "❌" if state is False else "🟡"


def render_unified_audit(report: UnifiedAuditReport) -> str:
    v = report.viability
    cost = float(report.cost_bps) / 10_000.0
    health = report.health
    lines = [
        "<b>🧾 Auditoría unificada</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        (
            f"{int(report.days)}d · neto de {cost:.2%} · rango 95% por bloques de fechas"
            + (f" · benchmark {escape(v.benchmark_label)}" if v.benchmark_label else "")
        ),
        "",
        "<b>1 · Veredicto</b>",
        f"{VERDICT_ICONS.get(v.verdict_level, '⚪')} <b>{escape(v.verdict)}</b>",
    ]
    for gate in v.gates:
        icon = "✅" if gate.passed is True else "❌" if gate.passed is False else "🟡"
        name = GATE_SHORT_NAMES.get(gate.name, "muestra 5D" if "muestra" in gate.name else gate.name)
        lines.append(f"{icon} {escape(name)}: {escape(gate.detail)}")
    if v.frozen_since:
        p5 = v.prospective
        lines.append(
            f"🧊 Prospectivo desde {escape(str(v.frozen_since)[:10])}: n{p5.n if p5 else 0}/{v.prospective_target}"
            + (f" · EV {_pct(p5.net_ev)}{_range(p5.net_ev_interval, _pct)}" if p5 and p5.n else "")
        )

    lines += ["", "<b>2 · Cohortes · 5D neto</b>", "<i>Planes, bloqueadas y radar: 1 por ticker+lado+día.</i>"]
    for cohort in report.cohorts:
        m = cohort.by_horizon.get("5d")
        if m is None or m.n == 0:
            lines.append(f"{cohort.label} · n0")
            continue
        m20 = cohort.by_horizon.get("20d")
        lines.append(
            f"{cohort.label} · n{m.n} · acierto {_pct(m.win_rate).lstrip('+')} · "
            f"EV <b>{_pct(m.net_ev)}</b>{_range(m.net_ev_interval, _pct)}"
        )
        extra = []
        if m.n_excess:
            extra.append(f"exceso {_pct(m.excess_ev)}{_range(m.excess_interval, _pct)}")
        if m20 is not None and m20.n:
            extra.append(f"20D {_pct(m20.net_ev)} (n{m20.n})")
        if extra:
            lines.append("   " + " · ".join(extra))

    lines += ["", "<b>3 · Bot vs humano</b>"]
    if report.plan_vs_manual and report.plan_vs_manual.get("estimate") is not None:
        diff = report.plan_vs_manual
        lines.append(
            f"{_mark(diff)} Planes no ejecutados menos tus operaciones manuales, 5D: "
            f"<b>{_pct(diff['estimate'])}</b>{_range(diff, _pct)}"
        )
        lines.append("<i>Positivo = seguir los planes habría rendido más que operar por tu cuenta.</i>")
    else:
        lines.append("Sin muestra suficiente de planes y operaciones manuales.")
    follow = health.get("follow_status") or {}
    if follow:
        lines.append(
            "Atribución: " + " · ".join(f"{k} {v}" for k, v in sorted(follow.items()))
        )

    lines += ["", "<b>4 · Calibración del score</b> <i>(IC = correlación de rango score/resultado)</i>"]
    for name, title in (("bot", "Ideas del bot"), ("radar", "Radar")):
        parts = []
        for h, result in (report.calibration.get(name) or {}).items():
            if result and result.get("estimate") is not None:
                parts.append(f"{h.upper()} {_mark(result)} {_num(result['estimate'])}{_range(result, _num)}")
        lines.append(f"{title}: " + (" · ".join(parts) if parts else "sin muestra"))

    lines += ["", "<b>5 · Salud de datos</b>"]
    last_price = health.get("last_market_price_at")
    lines.append(
        f"Último precio: {last_price:%d/%m %H:%M} UTC" if isinstance(last_price, datetime) else "Último precio: -"
    )
    if health.get("fills") is not None:
        lines.append(f"Fills con comisión: {health.get('fills_with_fees', 0)}/{health.get('fills')}")
    lines.append(
        f"Velas del benchmark: {health.get('benchmark_candles', 0)} · "
        f"movimientos duplicados excluidos: {health.get('duplicate_movements', 0)}"
    )
    if health.get("errors"):
        lines.append("Consultas fallidas: " + escape(", ".join(health["errors"])))

    lines += ["", "<b>6 · Alertas</b>"]
    lines += [f"⚠️ {escape(alert)}" for alert in report.alerts] or ["Sin alertas."]
    lines += [
        "",
        "<i>✅/❌ solo si el rango 95% queda de un lado de 0. Detalle: /viability /neto /ledger /bot_vs_humano /calibracion /regression.</i>",
    ]
    return "\n".join(lines)

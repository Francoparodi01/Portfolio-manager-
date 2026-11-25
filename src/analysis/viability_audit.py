"""
Read-only retrospective viability audit for the trading project.

This module separates bot-only, followed-by-user and manual-only execution and
measures 5d/10d/20d/40d outcomes after costs. The bot-only 5d gates (rank IC,
net EV, net EV versus manual) are decided on 95% date-block bootstrap intervals:
outcomes of nearby decisions overlap, so a point estimate above zero is not
evidence. An interval that crosses zero is "not conclusive", never a pass.
The sequential drawdown is descriptive, not a portfolio-risk estimate, and
passing these historical gates does not validate allocating more capital.
It does not change guards, thresholds, optimizer weights, or execution logic.
"""

from __future__ import annotations

import math
import json
import re
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from html import escape
from pathlib import Path
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
    mean_interval,
    rank_ic,
    rank_ic_interval,
)
from src.analysis.regression_audit import DEFAULT_HORIZONS, normalize_decision_frame


ACTIVE_ACTIONS = ("BUY", "SELL", "SELL_PARTIAL", "SELL_FULL")
SELL_ACTIONS = ("SELL", "SELL_PARTIAL", "SELL_FULL")
SCOPE_ORDER = ("bot_only", "followed", "manual_only")
SCOPE_LABELS = {
    "bot_only": "bot-only",
    "followed": "seguido",
    "manual_only": "manual-only",
}


@dataclass(frozen=True)
class ViabilityAuditConfig:
    database_url: str
    days: int = 180
    since: Optional[str] = None
    cost_bps: float = 75.0
    min_sample: int = 30
    horizons: tuple[str, ...] = DEFAULT_HORIZONS
    # Fraction of equity committed to each trade when building the drawdown curve.
    position_fraction: float = 0.10
    # Benchmark in the same unit as the outcomes (CEDEAR prices in ARS include the CCL).
    benchmark_ticker: Optional[str] = "SPY"
    benchmark_asset_type: str = "CEDEAR"
    benchmark_currency: str = "ARS"
    # Decisions at or after this instant form the prospective (frozen rules) cohort.
    frozen_since: Optional[str] = None
    prospective_target: int = 40
    # Result of checking the rules freeze manifest ("reglas intactas..." / "REGLAS MODIFICADAS...").
    freeze_note: Optional[str] = None


@dataclass(frozen=True)
class HorizonMetrics:
    scope: str
    horizon: str
    n: int
    wins: int
    losses: int
    win_rate: Optional[float]
    avg_win: Optional[float]
    avg_loss: Optional[float]
    gross_ev: Optional[float]
    net_ev: Optional[float]
    profit_factor: Optional[float]
    max_drawdown: Optional[float]
    ic_final: Optional[float]
    ic_trend: Optional[float]
    ic_reversion: Optional[float]
    n_dates: int = 0
    net_ev_interval: Optional[dict] = None
    ic_interval: Optional[dict] = None
    # Net EV minus the benchmark return over the same window (sign-adjusted for sells).
    excess_ev: Optional[float] = None
    n_excess: int = 0
    excess_interval: Optional[dict] = None


@dataclass(frozen=True)
class ViabilityGate:
    name: str
    passed: Optional[bool]
    detail: str


@dataclass(frozen=True)
class ViabilityAuditReport:
    generated_at: datetime
    days: int
    cost_bps: float
    min_sample: int
    rows_loaded: int
    metrics: dict[str, dict[str, HorizonMetrics]]
    gates: list[ViabilityGate]
    verdict: str
    warnings: list[str]
    followed_summary: dict[str, int]
    # NO_SAMPLE | FAIL | INCONCLUSIVE | PASS — drives icons; never parse `verdict`.
    verdict_level: str = "NO_SAMPLE"
    benchmark_label: Optional[str] = None
    frozen_since: Optional[str] = None
    prospective: Optional[HorizonMetrics] = None
    prospective_target: int = 40
    freeze_note: Optional[str] = None


async def load_viability_decision_log(config: ViabilityAuditConfig) -> pd.DataFrame:
    dsn = config.database_url.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)

    try:
        rows = await conn.fetch(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'decision_log'
            """
        )
        cols = {str(r["column_name"]) for r in rows}

        wanted = [
            "id",
            "decided_at",
            "ticker",
            "decision",
            "final_score",
            "layers",
            "source",
            "status",
            "decision_type",
            "metric_scope",
            "outcome_basis",
            "outcome_5d",
            "outcome_10d",
            "outcome_20d",
            "outcome_40d",
            "executable_outcome_5d",
            "executable_outcome_10d",
            "executable_outcome_20d",
            "executable_outcome_40d",
        ]
        selected = [c for c in wanted if c in cols]
        if not selected:
            return pd.DataFrame()

        cutoff = _cutoff(config.days, config.since)
        rows = await conn.fetch(
            f"""
            SELECT {", ".join(selected)}
            FROM decision_log dl
            WHERE decided_at >= $1
              AND NOT EXISTS (
                  SELECT 1
                  FROM broker_fills bf
                  WHERE bf.decision_log_id = dl.id
                    AND COALESCE(bf.raw_payload, '{{}}'::jsonb) ? 'superseded_by_real'
                    AND NOT EXISTS (
                        SELECT 1
                        FROM broker_fills live_bf
                        WHERE live_bf.decision_log_id = dl.id
                          AND NOT (COALESCE(live_bf.raw_payload, '{{}}'::jsonb) ? 'superseded_by_real')
                    )
              )
            ORDER BY decided_at ASC
            """,
            cutoff,
        )
        rows = [dict(row) for row in rows]

        followed_rows = []
        followed_summary = {
            "attributions": 0,
            "eligible": 0,
            "ambiguous": 0,
            "plan_links": 0,
        }
        attribution_ready = await conn.fetchval(
            "SELECT to_regclass('public.plan_execution_attributions') IS NOT NULL"
        )
        if attribution_ready:
            selected_followed = []
            for column in selected:
                if column == "id":
                    selected_followed.append("attribution.id AS id")
                elif column == "decided_at":
                    selected_followed.append("attribution.executed_at AS decided_at")
                elif column == "source":
                    selected_followed.append("'plan_execution_attribution'::text AS source")
                elif column == "status":
                    selected_followed.append("'EXECUTED_FOLLOWED'::text AS status")
                elif column == "metric_scope":
                    selected_followed.append("'followed_plan'::text AS metric_scope")
                elif column == "is_primary_metric":
                    selected_followed.append("TRUE AS is_primary_metric")
                elif column == "outcome_basis":
                    selected_followed.append("attribution.outcome_basis AS outcome_basis")
                elif column in {
                    "outcome_5d",
                    "outcome_10d",
                    "outcome_20d",
                    "outcome_40d",
                }:
                    selected_followed.append(f"attribution.{column} AS {column}")
                elif column.startswith("executable_outcome_"):
                    selected_followed.append(f"NULL::float AS {column}")
                else:
                    selected_followed.append(f"dl.{column}")
            followed_rows = await conn.fetch(
                f"""
                SELECT
                    {", ".join(selected_followed)},
                    attribution.id AS attribution_id,
                    attribution.follow_status,
                    attribution.temporal_quality,
                    attribution.follow_ratio,
                    attribution.executed_amount_ars AS attributed_amount_ars,
                    attribution.executed_at AS attributed_executed_at
                FROM plan_execution_attributions attribution
                JOIN decision_log dl
                  ON dl.id = attribution.representative_decision_log_id
                WHERE attribution.executed_at >= $1
                  AND attribution.eligible_for_viability = TRUE
                ORDER BY attribution.executed_at
                """,
                cutoff,
            )
            summary_row = await conn.fetchrow(
                """
                SELECT
                    COUNT(*) AS attributions,
                    COUNT(*) FILTER (WHERE eligible_for_viability) AS eligible,
                    COUNT(*) FILTER (WHERE NOT eligible_for_viability) AS ambiguous,
                    (
                        SELECT COUNT(*)
                        FROM plan_execution_attribution_plans link
                        JOIN plan_execution_attributions linked
                          ON linked.id = link.attribution_id
                        WHERE linked.executed_at >= $1
                    ) AS plan_links
                FROM plan_execution_attributions
                WHERE executed_at >= $1
                """,
                cutoff,
            )
            if summary_row:
                followed_summary = {
                    key: int(summary_row[key] or 0)
                    for key in followed_summary
                }
            linked_external_ids = {
                str(row["external_movement_id"])
                for row in await conn.fetch(
                    """
                    SELECT bm.external_movement_id
                    FROM plan_execution_attribution_movements link
                    JOIN plan_execution_attributions attribution
                      ON attribution.id = link.attribution_id
                    JOIN broker_movements bm
                      ON bm.id = link.broker_movement_id
                    WHERE attribution.executed_at >= $1
                      AND attribution.eligible_for_viability = TRUE
                    """,
                    cutoff,
                )
            }
            linked_decision_ids = {
                int(row["decision_log_id"])
                for row in await conn.fetch(
                    """
                    SELECT DISTINCT bf.decision_log_id
                    FROM plan_execution_attribution_movements link
                    JOIN plan_execution_attributions attribution
                      ON attribution.id = link.attribution_id
                    JOIN broker_movements movement
                      ON movement.id = link.broker_movement_id
                    JOIN broker_fills bf
                      ON bf.external_fill_id = movement.external_movement_id
                    WHERE attribution.executed_at >= $1
                      AND attribution.eligible_for_viability = TRUE
                      AND bf.decision_log_id IS NOT NULL
                    """,
                    cutoff,
                )
            }
            duplicate_synthetic_ids = {
                str(row["external_movement_id"])
                for row in await conn.fetch(
                    """
                    SELECT synthetic.external_movement_id
                    FROM broker_movements synthetic
                    WHERE synthetic.external_movement_id LIKE 'synthetic:%'
                      AND synthetic.executed_at >= $1
                      AND EXISTS (
                          SELECT 1
                          FROM broker_movements real
                          WHERE real.external_movement_id NOT LIKE 'synthetic:%'
                            AND real.ticker = synthetic.ticker
                            AND real.movement_type = synthetic.movement_type
                            AND (real.executed_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date =
                                (synthetic.executed_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date
                            AND NOT (COALESCE(real.raw_payload, '{}'::jsonb) ? 'superseded_by_real')
                      )
                    """,
                    cutoff,
                )
            }
            for row in rows:
                payload = row.get("layers")
                if isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except (TypeError, ValueError):
                        payload = {}
                movement_meta = payload.get("broker_movement", {}) if isinstance(payload, dict) else {}
                external_ids = {
                    str(value) for value in movement_meta.get("external_fill_ids", []) if value
                }
                row["attributed_followed"] = (
                    int(row.get("id") or 0) in linked_decision_ids
                    or bool(external_ids & linked_external_ids)
                )
                row["duplicate_movement"] = bool(external_ids) and external_ids.issubset(
                    duplicate_synthetic_ids
                )
    finally:
        await conn.close()

    if not rows and not followed_rows:
        frame = pd.DataFrame(columns=selected)
        frame.attrs["followed_summary"] = followed_summary
        return frame

    frame = pd.concat(
        [
            pd.DataFrame([dict(row) for row in rows]),
            pd.DataFrame([dict(row) for row in followed_rows]),
        ],
        ignore_index=True,
        sort=False,
    )
    frame.attrs["followed_summary"] = followed_summary
    return frame


async def load_benchmark_candles(config: ViabilityAuditConfig) -> list[tuple[date, float]]:
    """Daily canonical closes of the benchmark, same source priority as get_market_candles."""
    if not config.benchmark_ticker:
        return []
    dsn = config.database_url.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    try:
        rows = await conn.fetch(
            """
            WITH candidates AS (
                SELECT * FROM market_candles
                WHERE ticker = $1 AND asset_type = $2 AND currency = $3 AND interval = '1d'
                  AND close_price IS NOT NULL AND close_price > 0
            ), ranked AS (
                SELECT ts, close_price, venue, long_ticker,
                    COUNT(DISTINCT venue) OVER () AS venues,
                    ROW_NUMBER() OVER (
                        PARTITION BY date_trunc('day', ts AT TIME ZONE 'UTC')
                        ORDER BY CASE source WHEN 'COCOS' THEN 0 WHEN 'TRADINGVIEW_BYMA' THEN 1
                                             WHEN 'internal_snapshot' THEN 2 ELSE 3 END,
                                 scraped_at DESC, ts DESC
                    ) AS source_rank
                FROM candidates
            )
            SELECT ts, close_price::float8 AS close_price, venues
            FROM ranked WHERE source_rank = 1 ORDER BY ts ASC
            """,
            config.benchmark_ticker.upper(),
            config.benchmark_asset_type.upper(),
            config.benchmark_currency.upper(),
        )
    finally:
        await conn.close()
    if rows and int(rows[0]["venues"] or 0) > 1:
        raise ValueError("AMBIGUOUS_SERIES_IDENTITY: benchmark has more than one venue")
    return [(row["ts"].date(), float(row["close_price"])) for row in rows]


async def run_viability_audit(config: ViabilityAuditConfig) -> ViabilityAuditReport:
    frame = await load_viability_decision_log(config)
    if config.benchmark_ticker:
        try:
            frame.attrs["benchmark_candles"] = await load_benchmark_candles(config)
        except Exception as exc:  # the audit must still run without a benchmark
            frame.attrs["benchmark_error"] = f"{type(exc).__name__}: {exc}"[:160]
    return run_viability_audit_sync(frame, config)


def run_viability_audit_sync(
    frame: pd.DataFrame,
    config: ViabilityAuditConfig,
) -> ViabilityAuditReport:
    generated_at = datetime.now(tz=timezone.utc)
    if frame.empty:
        return ViabilityAuditReport(
            generated_at=generated_at,
            days=config.days,
            cost_bps=config.cost_bps,
            min_sample=config.min_sample,
            rows_loaded=0,
            metrics=_empty_metrics(config),
            gates=[],
            verdict="SIN MUESTRA: todavia no hay decisiones ejecutadas con outcomes.",
            warnings=["No se cargaron filas desde decision_log."],
            followed_summary=dict(frame.attrs.get("followed_summary") or {}),
            verdict_level="NO_SAMPLE",
        )

    df = _prepare_frame(frame, config.horizons)
    benchmark_candles = list(frame.attrs.get("benchmark_candles") or [])
    benchmark_label = None
    if config.benchmark_ticker:
        benchmark_label = (
            f"{config.benchmark_ticker.upper()} {config.benchmark_asset_type.upper()} "
            f"{config.benchmark_currency.upper()}"
        )
        _attach_benchmark_returns(df, benchmark_candles, config.horizons)
    followed_summary = dict(frame.attrs.get("followed_summary") or {})
    followed_summary["comparable"] = int(_scope_mask(df, "followed").sum())
    metrics: dict[str, dict[str, HorizonMetrics]] = {}
    cost = float(config.cost_bps) / 10_000.0

    for scope in SCOPE_ORDER:
        scope_df = df[_scope_mask(df, scope)].copy()
        metrics[scope] = {}
        for horizon in config.horizons:
            metrics[scope][horizon] = _compute_horizon_metrics(
                scope_df,
                scope=scope,
                horizon=horizon,
                cost=cost,
                position_fraction=config.position_fraction,
            )

    warnings = _build_warnings(df, config)
    if config.benchmark_ticker and not benchmark_candles:
        warnings.append(
            f"Sin velas del benchmark {benchmark_label}"
            + (f" ({frame.attrs['benchmark_error']})" if frame.attrs.get("benchmark_error") else "")
            + ": el exceso no se puede medir y el veredicto no puede ser favorable."
        )
    prospective = None
    if config.frozen_since and "5d" in config.horizons:
        start = pd.Timestamp(config.frozen_since)
        start = start.tz_localize("America/Argentina/Buenos_Aires") if start.tzinfo is None else start
        decided = pd.to_datetime(df.get("decided_at"), utc=True, errors="coerce")
        prospective_df = df[_scope_mask(df, "bot_only") & (decided >= start)]
        prospective = _compute_horizon_metrics(
            prospective_df, scope="bot_only", horizon="5d", cost=cost,
            position_fraction=config.position_fraction,
        )
    gates = _build_gates(metrics, config, df=df, cost=cost)
    verdict, verdict_level = _build_verdict(gates)

    return ViabilityAuditReport(
        generated_at=generated_at,
        days=config.days,
        cost_bps=config.cost_bps,
        min_sample=config.min_sample,
        rows_loaded=len(df),
        metrics=metrics,
        gates=gates,
        verdict=verdict,
        warnings=warnings,
        followed_summary=followed_summary,
        verdict_level=verdict_level,
        benchmark_label=benchmark_label,
        frozen_since=config.frozen_since,
        prospective=prospective,
        prospective_target=config.prospective_target,
        freeze_note=config.freeze_note,
    )


def _attach_benchmark_returns(
    df: pd.DataFrame, candles: list[tuple[date, float]], horizons: tuple[str, ...]
) -> None:
    """Benchmark return over each decision's outcome window.

    Mirrors update_outcomes: entry on the decision's local (ART) day, exit on the
    first candle on or after decision day + N calendar days. Entry uses the last
    benchmark close on or before the decision day.
    """
    for horizon in horizons:
        df[f"benchmark_return_{horizon}"] = np.nan
    if not candles or "decided_at" not in df.columns or df.empty:
        return
    candles = sorted(candles)
    days = [day for day, _close in candles]
    closes = [close for _day, close in candles]
    decided = pd.to_datetime(df["decided_at"], utc=True, errors="coerce")
    local_days = decided.dt.tz_convert("America/Argentina/Buenos_Aires").dt.date
    for horizon in horizons:
        span = int(re.search(r"\d+", horizon).group())
        values = []
        for day in local_days:
            if day is None or pd.isna(day):
                values.append(np.nan)
                continue
            entry_index = bisect_right(days, day) - 1
            exit_index = bisect_left(days, day + timedelta(days=span))
            if entry_index < 0 or exit_index >= len(days):
                values.append(np.nan)
                continue
            values.append(closes[exit_index] / closes[entry_index] - 1.0)
        df[f"benchmark_return_{horizon}"] = values


VERDICT_ICONS = {"PASS": "🟢", "INCONCLUSIVE": "🟡", "FAIL": "🔴", "NO_SAMPLE": "⚪"}


def render_viability_audit(report: ViabilityAuditReport) -> str:
    cost = float(report.cost_bps) / 10_000.0
    verdict_icon = VERDICT_ICONS.get(report.verdict_level, "⚪")

    lines = [
        "<b>🧪 Viability</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        (
            f"{int(report.days)}d · costo <b>{cost:.2%}</b> · "
            f"gate mínimo <b>n{int(report.min_sample)}</b>"
            + (f" · benchmark <b>{escape(report.benchmark_label)}</b>" if report.benchmark_label else "")
        ),
        "",
        "<b>Veredicto</b>",
        f"{verdict_icon} <b>{escape(report.verdict)}</b>",
        "",
        "<b>Resultados netos por horizonte</b>",
        f"<i>Win y EV descuentan {cost:.2%} de costo por operación.</i>",
    ]

    horizons = list(report.metrics.get("bot_only", {}).keys())
    for horizon in horizons:
        bot = report.metrics.get("bot_only", {}).get(horizon)
        followed = report.metrics.get("followed", {}).get(horizon)
        manual = report.metrics.get("manual_only", {}).get(horizon)
        lines += [
            "",
            f"<b>{escape(horizon.upper())}</b>",
            (
                f"🤖 Bot · n{bot.n if bot else 0} · win {_fmt_pct(bot.win_rate if bot else None, signed=False)} · "
                f"EV <b>{_fmt_pct(bot.net_ev if bot else None)}</b> "
                f"{escape(fmt_interval(bot.net_ev_interval if bot else None, _fmt_pct))}"
                + (
                    f" · exceso <b>{_fmt_pct(bot.excess_ev)}</b> "
                    f"{escape(fmt_interval(bot.excess_interval, _fmt_pct))}"
                    if bot and bot.n_excess
                    else ""
                )
            ),
            (
                f"🧭 Seguido · n{followed.n if followed else 0} · "
                f"win {_fmt_pct(followed.win_rate if followed else None, signed=False)} · "
                f"EV <b>{_fmt_pct(followed.net_ev if followed else None)}</b>"
            ),
            (
                f"👤 Manual · n{manual.n if manual else 0} · "
                f"win {_fmt_pct(manual.win_rate if manual else None, signed=False)} · "
                f"EV <b>{_fmt_pct(manual.net_ev if manual else None)}</b>"
            ),
        ]

    gate_labels = {
        "muestra bot-only 5d": "Muestra bot 5D",
        "IC bot-only 5d positivo": "IC bot 5D (correlación de rango score/resultado)",
        "EV neto bot-only 5d positivo": "EV neto bot 5D",
        "EV neto bot-only 5d sobre benchmark": "Exceso vs benchmark 5D",
        "EV neto bot-only mayor que manual-only 5d": "Ventaja vs manual",
        "comparacion contra manual-only 5d": "Comparación manual",
    }
    lines += ["", "<b>Gates bot-only</b>"]
    if report.gates:
        for gate in report.gates:
            icon = "✅" if gate.passed is True else "❌" if gate.passed is False else "🟡"
            name = gate_labels.get(gate.name, gate.name)
            lines.append(f"{icon} {escape(name)} · {escape(gate.detail)}")
    else:
        lines.append("⚪ Sin muestra suficiente para evaluar gates.")

    followed = report.followed_summary or {}
    if followed.get("attributions"):
        lines += [
            "",
            "<b>Trazabilidad de seguimiento</b>",
            (
                f"🔗 {int(followed.get('attributions', 0))} operaciones · "
                f"{int(followed.get('eligible', 0))} elegibles · "
                f"{int(followed.get('comparable', 0))} comparables"
            ),
            (
                f"⚠️ {int(followed.get('ambiguous', 0))} ambiguas excluidas · "
                f"{int(followed.get('plan_links', 0))} planes vinculados"
            ),
        ]

    maturity_warning = next(
        (warning for warning in report.warnings if "40d" in warning.lower()),
        None,
    )
    if maturity_warning:
        lines += ["", f"⚠️ {escape(maturity_warning)}"]
    benchmark_warning = next((w for w in report.warnings if "benchmark" in w.lower()), None)
    if benchmark_warning:
        lines += ["", f"⚠️ {escape(benchmark_warning)}"]

    if report.frozen_since:
        p5 = report.prospective
        n = p5.n if p5 else 0
        lines += [
            "",
            f"<b>🧊 Prospectivo · reglas congeladas desde {escape(str(report.frozen_since)[:10])}</b>",
            f"Bot 5D · n<b>{n}</b>/{int(report.prospective_target)} objetivo",
        ]
        if report.freeze_note:
            icon = "⚠️" if "MODIFICADAS" in report.freeze_note else "🔒"
            lines.append(f"{icon} {escape(report.freeze_note)}")
        if p5 and p5.n:
            lines.append(
                f"EV <b>{_fmt_pct(p5.net_ev)}</b> {escape(fmt_interval(p5.net_ev_interval, _fmt_pct))}"
                + (
                    f" · exceso <b>{_fmt_pct(p5.excess_ev)}</b> {escape(fmt_interval(p5.excess_interval, _fmt_pct))}"
                    if p5.n_excess
                    else ""
                )
            )
            lines.append(
                f"IC <b>{_fmt_float(p5.ic_final)}</b> {escape(fmt_interval(p5.ic_interval, _fmt_float))}"
            )
        lines.append(
            "<i>Solo decisiones posteriores al congelamiento: es la evidencia que no se pudo ajustar mirando resultados.</i>"
        )

    lines += [
        "",
        "<i>✅/❌ solo si todo el rango 95% (bootstrap por bloques de fechas) queda de un lado de 0; 🟡 = no concluyente.</i>",
        "<i>IC = correlación de rango score/resultado, no intervalo de confianza.</i>",
        "<i>Exceso = EV neto menos el retorno del benchmark en la misma ventana (para SELL, se suma).</i>",
        "<i>Bot-only define los gates. Seguido es una cohorte descriptiva; manual-only queda separado.</i>",
        "<i>Guards y thresholds quedan intactos. Esto audita evidencia; no cambia decisiones.</i>",
    ]

    return "\n".join(lines)


def render_viability_chart(
    report: ViabilityAuditReport,
    output_path: str | Path,
) -> Path:
    from PIL import Image, ImageDraw, ImageFont

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    width, height = 1400, 1180
    bg = "#0b1117"
    panel = "#111b24"
    text = "#eef6fb"
    muted = "#9fb0bd"
    grid = "#23313d"
    bot_color = "#4cb3ff"
    followed_color = "#ffd166"
    manual_color = "#9bd46a"
    trend_color = "#b88cff"
    reversion_color = "#ffb86b"
    danger = "#ff6b6b"
    ok = "#6ee7a8"
    warn = "#ffd166"

    image = Image.new("RGB", (width, height), bg)
    draw = ImageDraw.Draw(image)

    fonts = _chart_fonts()
    title_font = fonts["title"]
    subtitle_font = fonts["subtitle"]
    body_font = fonts["body"]
    small_font = fonts["small"]
    mono_font = fonts["mono"]

    draw.text((54, 42), f"Viability Audit {int(report.days)}d", fill=text, font=title_font)
    draw.text(
        (56, 92),
        f"Bot-only / seguido / manual-only | costo {float(report.cost_bps) / 10_000:.2%} | muestra minima {report.min_sample}",
        fill=muted,
        font=small_font,
    )
    draw.text(
        (56, 124),
        "La atribucion es derivada: no cambia guards, thresholds ni planner.",
        fill=muted,
        font=small_font,
    )

    verdict_color = {"PASS": ok, "INCONCLUSIVE": warn, "FAIL": danger}.get(report.verdict_level, muted)
    _rounded(draw, (905, 40, 1346, 160), "#16232d", outline=verdict_color)
    draw.text((930, 62), "Lectura", fill=muted, font=small_font)
    _draw_wrapped(
        draw,
        _chart_verdict(report),
        (930, 92),
        max_width=390,
        fill=verdict_color,
        font=body_font,
        line_gap=4,
    )

    gate_x = 54
    gate_y = 184
    for gate in report.gates[:5]:
        color = ok if gate.passed is True else danger if gate.passed is False else muted
        label = "OK" if gate.passed is True else "NO" if gate.passed is False else "?"
        _rounded(draw, (gate_x, gate_y, gate_x + 246, gate_y + 60), "#121e28", outline=color)
        draw.text((gate_x + 16, gate_y + 12), label, fill=color, font=subtitle_font)
        draw.text((gate_x + 58, gate_y + 10), _short_gate_name(gate.name), fill=text, font=small_font)
        draw.text(
            (gate_x + 58, gate_y + 34),
            _gate_badge_detail(report, gate),
            fill=muted,
            font=mono_font,
        )
        gate_x += 258

    horizons = list(report.metrics.get("bot_only", {}).keys())
    if not horizons:
        horizons = list(report.metrics.get("manual_only", {}).keys())

    _draw_bar_panel(
        draw,
        box=(54, 260, 672, 610),
        title="EV neto despues de costos",
        horizons=horizons,
        series=[
            ("bot", [_metric(report, "bot_only", h).net_ev for h in horizons], bot_color),
            ("seguido", [_metric(report, "followed", h).net_ev for h in horizons], followed_color),
            ("manual", [_metric(report, "manual_only", h).net_ev for h in horizons], manual_color),
        ],
        value_fmt=_fmt_chart_pct,
        bg=panel,
        text=text,
        muted=muted,
        grid=grid,
        font=body_font,
        small_font=small_font,
        mono_font=mono_font,
    )

    _draw_bar_panel(
        draw,
        box=(728, 260, 1346, 610),
        title="IC de rango bot-only (score vs resultado)",
        horizons=horizons,
        series=[
            ("final", [_metric(report, "bot_only", h).ic_final for h in horizons], bot_color),
            ("trend", [_metric(report, "bot_only", h).ic_trend for h in horizons], trend_color),
            ("reversion", [_metric(report, "bot_only", h).ic_reversion for h in horizons], reversion_color),
        ],
        value_fmt=_fmt_chart_float,
        bg=panel,
        text=text,
        muted=muted,
        grid=grid,
        font=body_font,
        small_font=small_font,
        mono_font=mono_font,
    )

    _draw_bar_panel(
        draw,
        box=(54, 660, 672, 995),
        title="Drawdown secuencial (descriptivo, no cartera)",
        horizons=horizons,
        series=[
            ("bot", [_metric(report, "bot_only", h).max_drawdown for h in horizons], bot_color),
            ("seguido", [_metric(report, "followed", h).max_drawdown for h in horizons], followed_color),
            ("manual", [_metric(report, "manual_only", h).max_drawdown for h in horizons], manual_color),
        ],
        value_fmt=_fmt_chart_pct,
        bg=panel,
        text=text,
        muted=muted,
        grid=grid,
        font=body_font,
        small_font=small_font,
        mono_font=mono_font,
    )

    _draw_sample_panel(
        draw,
        box=(728, 660, 1346, 1125),
        report=report,
        bg=panel,
        text=text,
        muted=muted,
        bot_color=bot_color,
        followed_color=followed_color,
        manual_color=manual_color,
        danger=danger,
        ok=ok,
        font=body_font,
        small_font=small_font,
        mono_font=mono_font,
    )

    image.save(path, "PNG")
    return path


def _chart_fonts() -> dict[str, object]:
    from PIL import ImageFont

    regular_candidates = [
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "C:/Windows/Fonts/arial.ttf",
    ]
    bold_candidates = [
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "C:/Windows/Fonts/arialbd.ttf",
    ]
    mono_candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationMono-Regular.ttf",
        "C:/Windows/Fonts/consola.ttf",
    ]

    def load(size: int, candidates: list[str]):
        for candidate in candidates:
            try:
                return ImageFont.truetype(candidate, size=size)
            except Exception:
                continue
        try:
            return ImageFont.load_default(size=size)
        except TypeError:
            return ImageFont.load_default()

    return {
        "title": load(42, bold_candidates),
        "subtitle": load(25, bold_candidates),
        "body": load(22, bold_candidates),
        "small": load(18, regular_candidates),
        "mono": load(17, mono_candidates),
    }


def _metric(report: ViabilityAuditReport, scope: str, horizon: str) -> HorizonMetrics:
    return report.metrics.get(scope, {}).get(horizon, _empty_horizon(scope, horizon))


def _chart_verdict(report: ViabilityAuditReport) -> str:
    return {
        "PASS": "EDGE HISTORICO FAVORABLE\nno validado para escalar capital",
        "INCONCLUSIVE": "EDGE BOT NO CONCLUYENTE\nel rango 95% cruza 0",
        "FAIL": "EDGE BOT NO VALIDADO\nfalla al menos un gate",
        "NO_SAMPLE": "SIN MUESTRA SUFICIENTE\nfaltan outcomes bot-only",
    }.get(report.verdict_level, report.verdict)


def _short_gate_name(name: str) -> str:
    lowered = name.lower()
    if "muestra" in lowered:
        return "muestra 5d"
    if "ic" in lowered:
        return "IC rango 5d > 0"
    if "benchmark" in lowered:
        return "exceso > bench"
    if "ev neto" in lowered and "mayor" in lowered:
        return "EV > manual"
    if "ev neto" in lowered:
        return "EV neto > 0"
    if "comparacion" in lowered:
        return "vs manual (sin n)"
    return name[:22]


def _gate_badge_detail(report: ViabilityAuditReport, gate: ViabilityGate) -> str:
    name = gate.name.lower()
    bot = _metric(report, "bot_only", "5d")
    manual = _metric(report, "manual_only", "5d")
    if "muestra" in name:
        return f"n={bot.n}/{report.min_sample}"
    if "ic" in name:
        return _fmt_chart_float(bot.ic_final)
    if "benchmark" in name:
        return _fmt_chart_pct(bot.excess_ev)
    if "ev neto" in name and "mayor" in name:
        return f"{_fmt_chart_pct(bot.net_ev)} vs {_fmt_chart_pct(manual.net_ev)}"
    if "ev neto" in name:
        return _fmt_chart_pct(bot.net_ev)
    if "drawdown" in name:
        return f"{_fmt_chart_pct(bot.max_drawdown)} vs {_fmt_chart_pct(manual.max_drawdown)}"
    return gate.detail[:18]


def _rounded(draw, box, fill: str, *, outline: str | None = None, radius: int = 18) -> None:
    draw.rounded_rectangle(box, radius=radius, fill=fill, outline=outline, width=2 if outline else 1)


def _text_width(draw, value: str, font) -> int:
    left, _, right, _ = draw.textbbox((0, 0), value, font=font)
    return int(right - left)


def _draw_wrapped(
    draw,
    value: str,
    xy: tuple[int, int],
    *,
    max_width: int,
    fill: str,
    font,
    line_gap: int = 4,
) -> None:
    words = value.split()
    lines: list[str] = []
    current = ""
    for word in words:
        candidate = f"{current} {word}".strip()
        if current and _text_width(draw, candidate, font) > max_width:
            lines.append(current)
            current = word
        else:
            current = candidate
    if current:
        lines.append(current)

    x, y = xy
    for line in lines[:3]:
        draw.text((x, y), line, fill=fill, font=font)
        y += 28 + line_gap


def _draw_bar_panel(
    draw,
    *,
    box: tuple[int, int, int, int],
    title: str,
    horizons: list[str],
    series: list[tuple[str, list[Optional[float]], str]],
    value_fmt,
    bg: str,
    text: str,
    muted: str,
    grid: str,
    font,
    small_font,
    mono_font,
) -> None:
    x1, y1, x2, y2 = box
    _rounded(draw, box, bg)
    draw.text((x1 + 24, y1 + 20), title, fill=text, font=font)

    legend_x = x1 + 24
    for label, _, color in series:
        draw.rounded_rectangle((legend_x, y1 + 58, legend_x + 24, y1 + 72), radius=5, fill=color)
        draw.text((legend_x + 32, y1 + 52), label, fill=muted, font=small_font)
        legend_x += 118

    values = [
        float(value)
        for _, vals, _ in series
        for value in vals
        if value is not None and math.isfinite(float(value))
    ]
    if not values:
        draw.text((x1 + 24, y1 + 145), "Sin datos maduros para este panel.", fill=muted, font=small_font)
        return

    min_v = min(values + [0.0])
    max_v = max(values + [0.0])
    if abs(max_v - min_v) < 1e-9:
        min_v -= 0.01
        max_v += 0.01
    pad = (max_v - min_v) * 0.16
    min_v -= pad
    max_v += pad

    plot_x1 = x1 + 62
    plot_y1 = y1 + 96
    plot_x2 = x2 - 32
    plot_y2 = y2 - 58
    plot_h = plot_y2 - plot_y1
    plot_w = plot_x2 - plot_x1

    def y_for(value: float) -> float:
        return plot_y2 - ((float(value) - min_v) / (max_v - min_v)) * plot_h

    for ratio in [0.0, 0.25, 0.5, 0.75, 1.0]:
        value = min_v + (max_v - min_v) * ratio
        y = y_for(value)
        draw.line((plot_x1, y, plot_x2, y), fill=grid, width=1)
        draw.text((x1 + 14, y - 10), value_fmt(value), fill=muted, font=mono_font)

    zero_y = y_for(0.0)
    draw.line((plot_x1, zero_y, plot_x2, zero_y), fill="#d5e7f2", width=2)

    group_w = plot_w / max(len(horizons), 1)
    bar_count = max(len(series), 1)
    gap = 24 if bar_count == 2 else 10 if bar_count == 3 else 6
    bar_w = min(32, max(10, (group_w - 34 - gap * (bar_count - 1)) / bar_count))
    show_bar_labels = bar_count <= 2

    for h_idx, horizon in enumerate(horizons):
        group_center = plot_x1 + group_w * h_idx + group_w / 2
        first_x = group_center - (bar_count * bar_w + (bar_count - 1) * gap) / 2
        draw.text((group_center - 18, plot_y2 + 22), horizon, fill=muted, font=mono_font)

        for s_idx, (_, vals, color) in enumerate(series):
            value = vals[h_idx] if h_idx < len(vals) else None
            if value is None or not math.isfinite(float(value)):
                continue

            v = float(value)
            y = y_for(v)
            left = first_x + s_idx * (bar_w + gap)
            top = min(y, zero_y)
            bottom = max(y, zero_y)
            draw.rounded_rectangle(
                (left, top, left + bar_w, bottom),
                radius=5,
                fill=color,
            )

            if show_bar_labels:
                label = value_fmt(v)
                label_w = _text_width(draw, label, mono_font)
                label_x = left + bar_w / 2 - label_w / 2
                label_y = top - 22 if v >= 0 else bottom + 4
                draw.text((label_x, label_y), label, fill=text, font=mono_font)


def _draw_sample_panel(
    draw,
    *,
    box: tuple[int, int, int, int],
    report: ViabilityAuditReport,
    bg: str,
    text: str,
    muted: str,
    bot_color: str,
    followed_color: str,
    manual_color: str,
    danger: str,
    ok: str,
    font,
    small_font,
    mono_font,
) -> None:
    x1, y1, x2, y2 = box
    _rounded(draw, box, bg)
    draw.text((x1 + 24, y1 + 20), "Resumen para decision", fill=text, font=font)
    draw.text(
        (x1 + 24, y1 + 56),
        "OK solo si todo el rango 95% supera 0. ? = no concluyente.",
        fill=muted,
        font=small_font,
    )

    y = y1 + 105
    draw.text((x1 + 24, y), "Hz", fill=muted, font=mono_font)
    draw.text((x1 + 72, y), "bot n", fill=bot_color, font=mono_font)
    draw.text((x1 + 137, y), "bot EV", fill=bot_color, font=mono_font)
    draw.text((x1 + 226, y), "seg n", fill=followed_color, font=mono_font)
    draw.text((x1 + 294, y), "seg EV", fill=followed_color, font=mono_font)
    draw.text((x1 + 388, y), "man n", fill=manual_color, font=mono_font)
    draw.text((x1 + 458, y), "man EV", fill=manual_color, font=mono_font)
    y += 30

    for horizon in report.metrics.get("bot_only", {}):
        bot = _metric(report, "bot_only", horizon)
        followed = _metric(report, "followed", horizon)
        manual = _metric(report, "manual_only", horizon)
        ev_evidence = evidence(bot.net_ev_interval)
        ev_color = ok if ev_evidence is True else danger if ev_evidence is False else muted
        ic_color = ok if bot.ic_final is not None and bot.ic_final > 0 else danger
        draw.text((x1 + 24, y), horizon, fill=text, font=mono_font)
        draw.text((x1 + 72, y), str(bot.n), fill=text, font=mono_font)
        draw.text((x1 + 137, y), _fmt_chart_pct(bot.net_ev), fill=ev_color, font=mono_font)
        draw.text((x1 + 226, y), str(followed.n), fill=text, font=mono_font)
        draw.text((x1 + 294, y), _fmt_chart_pct(followed.net_ev), fill=followed_color, font=mono_font)
        draw.text((x1 + 388, y), str(manual.n), fill=text, font=mono_font)
        draw.text((x1 + 458, y), _fmt_chart_pct(manual.net_ev), fill=text, font=mono_font)
        y += 34

    y += 18
    draw.text((x1 + 24, y), "Gates", fill=muted, font=small_font)
    y += 30
    for gate in report.gates[:5]:
        color = ok if gate.passed is True else danger if gate.passed is False else muted
        label = "OK" if gate.passed is True else "NO" if gate.passed is False else "?"
        draw.text((x1 + 24, y), label, fill=color, font=mono_font)
        # First clause of the detail; the "; " inside a "[a; b]" range is not a separator.
        summary = re.split(r";\s(?![^\[]*\])", gate.detail)[0]
        draw.text((x1 + 74, y), summary[:46], fill=text, font=mono_font)
        y += 30


def _fmt_chart_pct(value: Optional[float], *, signed: bool = True) -> str:
    if value is None:
        return "-"
    prefix = "+" if signed else ""
    return format(float(value), f"{prefix}.1%")


def _fmt_chart_float(value: Optional[float]) -> str:
    if value is None:
        return "-"
    return f"{float(value):+.2f}"


def _prepare_frame(frame: pd.DataFrame, horizons: tuple[str, ...]) -> pd.DataFrame:
    df = normalize_decision_frame(frame)

    for col in ["final_score", "trend_score", "reversion_score"]:
        if col not in df.columns:
            df[col] = np.nan

    for horizon in horizons:
        base = f"outcome_{horizon}"
        executable = f"executable_outcome_{horizon}"
        effective = f"effective_outcome_{horizon}"

        if base not in df.columns:
            df[base] = np.nan
        if executable not in df.columns:
            df[executable] = np.nan

        df[effective] = pd.to_numeric(df[executable], errors="coerce").combine_first(
            pd.to_numeric(df[base], errors="coerce")
        )

    if "outcome_basis" in df.columns:
        basis = df["outcome_basis"].fillna("").astype(str).str.lower()
        df = df[basis.str.startswith("canonical_cocos") | basis.eq("")].copy()

    if "decision" in df.columns:
        df = df[df["decision"].isin(ACTIVE_ACTIONS)].copy()

    for col in ["final_score", "trend_score", "reversion_score"]:
        df[f"aligned_{col}"] = aligned_scores(df[col], df["decision"])

    return df


def _scope_mask(df: pd.DataFrame, scope: str) -> pd.Series:
    source = df["source"].fillna("").astype(str).str.lower()
    status = df["status"].fillna("").astype(str).str.upper()

    if scope == "bot_only":
        return source.eq("execution_plan") & status.eq("EXECUTED")

    if scope == "followed":
        return source.eq("plan_execution_attribution") & status.eq("EXECUTED_FOLLOWED")

    if scope == "manual_only":
        attributed = df.get(
            "attributed_followed",
            pd.Series(False, index=df.index, dtype=bool),
        ).astype("boolean").fillna(False).astype(bool)
        duplicate = df.get(
            "duplicate_movement",
            pd.Series(False, index=df.index, dtype=bool),
        ).astype("boolean").fillna(False).astype(bool)
        manual = status.eq("EXECUTED_MANUAL") | source.isin(["broker_movement", "broker_fill"])
        return manual & ~attributed & ~duplicate

    raise ValueError(f"Unsupported scope: {scope}")


def _compute_horizon_metrics(
    frame: pd.DataFrame,
    *,
    scope: str,
    horizon: str,
    cost: float,
    position_fraction: float = 1.0,
) -> HorizonMetrics:
    outcome_col = f"effective_outcome_{horizon}"
    if frame.empty or outcome_col not in frame.columns:
        return _empty_horizon(scope, horizon)

    data = frame.copy()
    data["_gross"] = pd.to_numeric(data[outcome_col], errors="coerce")
    data.loc[~np.isfinite(data["_gross"].astype(float)), "_gross"] = np.nan
    data = data.dropna(subset=["_gross"])

    if data.empty:
        return _empty_horizon(scope, horizon)

    if "decided_at" in data.columns:
        # The drawdown path must follow time, not database/row order.
        data["_decided_at"] = pd.to_datetime(data["decided_at"], errors="coerce", utc=True)
        data = data.sort_values("_decided_at", kind="stable", na_position="last")
        dates = decision_dates(data["decided_at"])
    else:
        dates = pd.Series([None] * len(data), index=data.index)

    data["_net"] = data["_gross"] - cost
    net = data["_net"].astype(float)
    gross = data["_gross"].astype(float)
    wins = net[net > 0]
    losses = net[net <= 0]

    gross_ev = float(gross.mean()) if len(gross) else None
    net_ev = float(net.mean()) if len(net) else None
    profit_factor = None
    loss_sum = float(abs(losses.sum())) if len(losses) else 0.0
    if loss_sum > 0:
        profit_factor = float(wins.sum()) / loss_sum

    final_scores = data.get("aligned_final_score")
    ic_interval = (
        rank_ic_interval(dates, final_scores, gross, horizon)
        if final_scores is not None
        else None
    )

    excess_ev, n_excess, excess_interval = None, 0, None
    bench_col = f"benchmark_return_{horizon}"
    if bench_col in data.columns:
        side = np.where(data["decision"].isin(SELL_ACTIONS), -1.0, 1.0) if "decision" in data.columns else 1.0
        excess = net - side * pd.to_numeric(data[bench_col], errors="coerce").astype(float)
        valid = excess.notna()
        n_excess = int(valid.sum())
        if n_excess:
            excess_ev = float(excess[valid].mean())
            excess_interval = mean_interval(dates[valid], excess[valid], horizon)

    return HorizonMetrics(
        scope=scope,
        horizon=horizon,
        n=int(len(data)),
        wins=int(len(wins)),
        losses=int(len(losses)),
        win_rate=float(len(wins) / len(data)) if len(data) else None,
        avg_win=float(wins.mean()) if len(wins) else None,
        avg_loss=float(losses.mean()) if len(losses) else None,
        gross_ev=gross_ev,
        net_ev=net_ev,
        profit_factor=profit_factor,
        max_drawdown=_max_drawdown(net.tolist(), position_fraction),
        ic_final=rank_ic(final_scores, gross) if final_scores is not None else None,
        ic_trend=rank_ic(data.get("aligned_trend_score"), gross),
        ic_reversion=rank_ic(data.get("aligned_reversion_score"), gross),
        n_dates=int(dates.dropna().nunique()),
        net_ev_interval=mean_interval(dates, net, horizon),
        ic_interval=ic_interval,
        excess_ev=excess_ev,
        n_excess=n_excess,
        excess_interval=excess_interval,
    )


def _scope_outcomes(
    df: pd.DataFrame, scope: str, horizon: str, cost: float
) -> tuple[pd.Series, pd.Series]:
    data = df[_scope_mask(df, scope)]
    net = pd.to_numeric(data.get(f"effective_outcome_{horizon}"), errors="coerce") - cost
    dates = (
        decision_dates(data["decided_at"])
        if "decided_at" in data.columns
        else pd.Series([None] * len(data), index=data.index)
    )
    return dates, net


def _build_gates(
    metrics: dict[str, dict[str, HorizonMetrics]],
    config: ViabilityAuditConfig,
    *,
    df: Optional[pd.DataFrame] = None,
    cost: float = 0.0,
) -> list[ViabilityGate]:
    bot5 = metrics["bot_only"].get("5d", _empty_horizon("bot_only", "5d"))
    manual5 = metrics["manual_only"].get("5d", _empty_horizon("manual_only", "5d"))

    gates = [
        ViabilityGate(
            "muestra bot-only 5d",
            bot5.n >= config.min_sample,
            f"n={bot5.n}, minimo={config.min_sample}, fechas={bot5.n_dates}",
        ),
        ViabilityGate(
            "IC bot-only 5d positivo",
            evidence(bot5.ic_interval),
            f"IC rango={_fmt_float(bot5.ic_final)} {fmt_interval(bot5.ic_interval, _fmt_float)}",
        ),
        ViabilityGate(
            "EV neto bot-only 5d positivo",
            evidence(bot5.net_ev_interval),
            f"EV neto={_fmt_pct(bot5.net_ev)} {fmt_interval(bot5.net_ev_interval, _fmt_pct)}",
        ),
    ]

    if config.benchmark_ticker:
        gates.append(
            ViabilityGate(
                "EV neto bot-only 5d sobre benchmark",
                evidence(bot5.excess_interval) if bot5.n_excess else None,
                (
                    f"exceso vs {config.benchmark_ticker.upper()}={_fmt_pct(bot5.excess_ev)} "
                    f"{fmt_interval(bot5.excess_interval, _fmt_pct)}; con benchmark n={bot5.n_excess}"
                    if bot5.n_excess
                    else f"sin velas de {config.benchmark_ticker.upper()} para estas fechas"
                ),
            )
        )

    if manual5.n >= config.min_sample:
        difference = None
        if df is not None and "5d" in config.horizons:
            bot_dates, bot_net = _scope_outcomes(df, "bot_only", "5d", cost)
            manual_dates, manual_net = _scope_outcomes(df, "manual_only", "5d", cost)
            difference = difference_interval(bot_dates, bot_net, manual_dates, manual_net, "5d")
        gates.append(
            ViabilityGate(
                "EV neto bot-only mayor que manual-only 5d",
                evidence(difference),
                (
                    f"bot={_fmt_pct(bot5.net_ev)} vs manual={_fmt_pct(manual5.net_ev)}; "
                    f"diferencia {fmt_interval(difference, _fmt_pct)}"
                ),
            )
        )
    else:
        gates.append(
            ViabilityGate(
                "comparacion contra manual-only 5d",
                None,
                f"manual n={manual5.n}, minimo={config.min_sample}",
            )
        )

    return gates


GATE_SHORT_NAMES = {
    "IC bot-only 5d positivo": "IC 5D",
    "EV neto bot-only 5d positivo": "EV neto 5D",
    "EV neto bot-only 5d sobre benchmark": "exceso vs benchmark 5D",
    "EV neto bot-only mayor que manual-only 5d": "ventaja vs manual",
}


def _build_verdict(gates: list[ViabilityGate]) -> tuple[str, str]:
    """Verdict text and level (NO_SAMPLE | FAIL | INCONCLUSIVE | PASS)."""
    if not gates:
        return "SIN MUESTRA: sin datos suficientes.", "NO_SAMPLE"

    sample = next((g for g in gates if "muestra" in g.name), None)
    if sample is not None and sample.passed is False:
        return f"SIN MUESTRA SUFICIENTE: edge bot sin evaluar ({sample.detail}).", "NO_SAMPLE"

    failed = [GATE_SHORT_NAMES.get(g.name, g.name) for g in gates if g.passed is False]
    if failed:
        return "EDGE BOT NO VALIDADO: falla " + "; ".join(failed) + ".", "FAIL"

    # Without enough manual-only trades there is nothing to compare against: informative, not blocking.
    open_gates = [
        GATE_SHORT_NAMES.get(g.name, g.name)
        for g in gates
        if g.passed is None and g.name != "comparacion contra manual-only 5d"
    ]
    if open_gates:
        return (
            "EDGE BOT NO CONCLUYENTE: " + ", ".join(open_gates) + " sin evidencia (rango 95% cruza 0).",
            "INCONCLUSIVE",
        )

    return (
        "EDGE HISTORICO FAVORABLE, NO VALIDADO PARA ESCALAR CAPITAL: todos los gates con rango "
        "95% sobre 0; retrospectivo, falta el periodo con reglas congeladas.",
        "PASS",
    )


def _build_warnings(df: pd.DataFrame, config: ViabilityAuditConfig) -> list[str]:
    warnings: list[str] = []
    if df.empty:
        return warnings

    source_counts = df["source"].value_counts(dropna=False).to_dict()
    bot_count = int(_scope_mask(df, "bot_only").sum())
    followed_count = int(_scope_mask(df, "followed").sum())
    manual_count = int(_scope_mask(df, "manual_only").sum())
    if bot_count == 0:
        warnings.append("No hay filas bot-only execution_plan/EXECUTED en la ventana.")
    if manual_count == 0:
        warnings.append("No hay filas manual-only/broker en la ventana.")
    if followed_count == 0:
        warnings.append("No hay atribuciones plan-seguido elegibles en la ventana.")
    else:
        warnings.append(
            f"Seguimiento normalizado: {followed_count} operaciones deduplicadas elegibles."
        )
    if bot_count and manual_count:
        warnings.append(
            f"Se separaron scopes: bot-only={bot_count}, manual-only={manual_count}; no se mezclan en el EV principal."
        )

    missing_40d = int(df.get("effective_outcome_40d", pd.Series(dtype=float)).isna().sum())
    if "40d" in config.horizons and missing_40d:
        warnings.append(
            f"Hay {missing_40d} filas sin outcome 40d; 40d puede estar inmaduro."
        )

    if source_counts:
        top_sources = ", ".join(f"{k}:{v}" for k, v in list(source_counts.items())[:4])
        warnings.append(f"Fuentes normalizadas: {top_sources}.")

    return warnings


def _empty_metrics(config: ViabilityAuditConfig) -> dict[str, dict[str, HorizonMetrics]]:
    return {
        scope: {horizon: _empty_horizon(scope, horizon) for horizon in config.horizons}
        for scope in SCOPE_ORDER
    }


def _empty_horizon(scope: str, horizon: str) -> HorizonMetrics:
    return HorizonMetrics(
        scope=scope,
        horizon=horizon,
        n=0,
        wins=0,
        losses=0,
        win_rate=None,
        avg_win=None,
        avg_loss=None,
        gross_ev=None,
        net_ev=None,
        profit_factor=None,
        max_drawdown=None,
        ic_final=None,
        ic_trend=None,
        ic_reversion=None,
    )


def _render_metric_table(report: ViabilityAuditReport) -> list[str]:
    rows = [
        (
            f"{'Scope':<12}{'Hz':<4}{'n':>4} {'Win':>6} {'EVgross':>8} "
            f"{'EVnet':>8} {'MaxDD':>8} {'IC':>7} {'ICtr':>7} {'ICrv':>7}"
        )
    ]
    for scope in SCOPE_ORDER:
        for horizon in report.metrics.get(scope, {}):
            metric = report.metrics[scope][horizon]
            rows.append(
                f"{SCOPE_LABELS[scope]:<12}"
                f"{horizon:<4}"
                f"{metric.n:>4} "
                f"{_fmt_pct(metric.win_rate, signed=False):>6} "
                f"{_fmt_pct(metric.gross_ev):>8} "
                f"{_fmt_pct(metric.net_ev):>8} "
                f"{_fmt_pct(metric.max_drawdown):>8} "
                f"{_fmt_float(metric.ic_final):>7} "
                f"{_fmt_float(metric.ic_trend):>7} "
                f"{_fmt_float(metric.ic_reversion):>7}"
            )
    return ["<pre>", *rows, "</pre>"]


def _cutoff(days: int, since: Optional[str]) -> datetime:
    if since:
        try:
            parsed = datetime.fromisoformat(since)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except Exception:
            pass
    return datetime.now(tz=timezone.utc) - timedelta(days=int(days))


def _max_drawdown(returns: list[float], position_fraction: float = 1.0) -> Optional[float]:
    """Max drawdown of a chronological curve that commits `position_fraction` of equity per trade.

    With 1.0 every trade compounds the whole account, which turns a long series of
    overlapping trades into a meaningless -99%. Overlap between trades is still not
    modelled, so this stays a descriptive figure, not a gate.
    """
    if not returns:
        return None
    equity = 1.0
    peak = 1.0
    worst = 0.0
    for value in returns:
        if value is None or not math.isfinite(float(value)):
            continue
        equity *= max(0.0, 1.0 + position_fraction * float(value))
        peak = max(peak, equity)
        if peak > 0:
            worst = min(worst, equity / peak - 1.0)
    return float(worst)


def _fmt_pct(value: Optional[float], *, signed: bool = True) -> str:
    if value is None:
        return "-"
    prefix = "+" if signed else ""
    return format(float(value), f"{prefix}.1%")


def _fmt_float(value: Optional[float]) -> str:
    if value is None:
        return "-"
    return f"{float(value):+.3f}"

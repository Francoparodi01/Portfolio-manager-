"""
Read-only viability audit for the trading project.

This module separates bot-only, followed-by-user and manual-only execution,
measures 5d/10d/20d/40d independently, and reports whether the bot clears a
conservative bar: positive IC, lower drawdown, and better net EV after costs.
It does not change guards, thresholds, optimizer weights, or execution logic.
"""

from __future__ import annotations

import math
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html import escape
from pathlib import Path
from typing import Optional

import asyncpg
import numpy as np
import pandas as pd

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
                if column == "source":
                    selected_followed.append("'plan_execution_attribution'::text AS source")
                elif column == "status":
                    selected_followed.append("'EXECUTED_FOLLOWED'::text AS status")
                elif column == "metric_scope":
                    selected_followed.append("'followed_plan'::text AS metric_scope")
                elif column == "is_primary_metric":
                    selected_followed.append("TRUE AS is_primary_metric")
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
                WHERE attribution.plan_decided_at >= $1
                  AND attribution.eligible_for_viability = TRUE
                ORDER BY attribution.plan_decided_at
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
                        WHERE linked.plan_decided_at >= $1
                    ) AS plan_links
                FROM plan_execution_attributions
                WHERE plan_decided_at >= $1
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
                    WHERE attribution.plan_decided_at >= $1
                      AND attribution.eligible_for_viability = TRUE
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
                row["attributed_followed"] = bool(external_ids & linked_external_ids)
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


async def run_viability_audit(config: ViabilityAuditConfig) -> ViabilityAuditReport:
    frame = await load_viability_decision_log(config)
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
        )

    df = _prepare_frame(frame, config.horizons)
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
            )

    warnings = _build_warnings(df, config)
    gates = _build_gates(metrics, config)
    verdict = _build_verdict(gates)

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
    )


def render_viability_audit(report: ViabilityAuditReport) -> str:
    cost = float(report.cost_bps) / 10_000.0
    lines = [
        "<b>VIABILITY AUDIT</b>",
        (
            f"Periodo: <b>{int(report.days)}d</b> | "
            f"costo: <b>{cost:.2%}</b> | "
            f"muestra minima: <b>{int(report.min_sample)}</b>"
        ),
        "Scopes separados: bot-only, planes seguidos por usuario y manual-only.",
        "Los gates siguen usando bot-only; seguido es evidencia descriptiva deduplicada.",
        "Guards y thresholds quedan intactos.",
        "",
        "<b>Metricas por horizonte</b>",
        *_render_metric_table(report),
        "",
        "<b>Gates</b>",
    ]

    if report.gates:
        for gate in report.gates:
            label = "OK" if gate.passed is True else "NO" if gate.passed is False else "N/A"
            lines.append(f"   {label} - {escape(gate.name)}: {escape(gate.detail)}")
    else:
        lines.append("   N/A - sin muestra suficiente para evaluar gates.")

    lines += [
        "",
        "<b>Lectura</b>",
        f"   {escape(report.verdict)}",
        "   No se aflojaron thresholds: esto mide evidencia, no cambia decisiones.",
    ]

    if report.warnings:
        lines += ["", "<b>Caveats</b>"]
        for warning in report.warnings[:6]:
            lines.append(f"   - {escape(warning)}")

    followed = report.followed_summary or {}
    if followed.get("attributions"):
        lines += [
            "",
            "<b>Trazabilidad seguida</b>",
            (
                f"   Operaciones: <b>{int(followed.get('attributions', 0))}</b> | "
                f"elegibles: <b>{int(followed.get('eligible', 0))}</b> | "
                f"comparables: <b>{int(followed.get('comparable', 0))}</b> | "
                f"hora ambigua: <b>{int(followed.get('ambiguous', 0))}</b> | "
                f"planes vinculados: <b>{int(followed.get('plan_links', 0))}</b>"
            ),
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

    verdict_color = ok if "VIABLE PARA" in report.verdict else warn if "EDGE BOT" in report.verdict else danger
    _rounded(draw, (905, 40, 1346, 160), "#16232d", outline=verdict_color)
    draw.text((930, 62), "Lectura", fill=muted, font=small_font)
    _draw_wrapped(
        draw,
        _chart_verdict(report.verdict),
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
        label = "OK" if gate.passed is True else "NO" if gate.passed is False else "N/A"
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
        title="IC bot-only por score",
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
        title="Max drawdown",
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


def _chart_verdict(verdict: str) -> str:
    upper = verdict.upper()
    if "EDGE BOT NO VALIDADO" in upper:
        return "EDGE BOT NO VALIDADO\nfalta muestra / EV neto"
    if "NO VIABLE PARA ESCALAR" in upper:
        return "NO ESCALAR CAPITAL\naun no supera gates"
    if "VIABLE PARA 180D" in upper:
        return "VIABLE PARA 180D\ncon guards actuales"
    if "SIN MUESTRA" in upper:
        return "SIN MUESTRA\nfaltan outcomes"
    return verdict


def _short_gate_name(name: str) -> str:
    lowered = name.lower()
    if "muestra" in lowered:
        return "muestra 5d"
    if "ic" in lowered:
        return "IC 5d > 0"
    if "ev neto" in lowered and "mayor" in lowered:
        return "EV > manual"
    if "ev neto" in lowered:
        return "EV neto > 0"
    if "drawdown" in lowered:
        return "DD < manual"
    return name[:22]


def _gate_badge_detail(report: ViabilityAuditReport, gate: ViabilityGate) -> str:
    name = gate.name.lower()
    bot = _metric(report, "bot_only", "5d")
    manual = _metric(report, "manual_only", "5d")
    if "muestra" in name:
        return f"n={bot.n}/{report.min_sample}"
    if "ic" in name:
        return _fmt_chart_float(bot.ic_final)
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
        "EV neto debe ser positivo y mejor que manual. IC debe ser > 0.",
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
        ev_color = ok if bot.net_ev is not None and bot.net_ev > 0 else danger
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
        label = "OK" if gate.passed is True else "NO" if gate.passed is False else "N/A"
        draw.text((x1 + 24, y), label, fill=color, font=mono_font)
        draw.text((x1 + 74, y), gate.detail[:48], fill=text, font=mono_font)
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
        df = df[basis.eq("canonical_cocos") | basis.eq("")].copy()

    if "decision" in df.columns:
        df = df[df["decision"].isin(ACTIVE_ACTIONS)].copy()

    df["score_side"] = np.where(df["decision"].isin(SELL_ACTIONS), -1.0, 1.0)
    for col in ["final_score", "trend_score", "reversion_score"]:
        df[f"aligned_{col}"] = pd.to_numeric(df[col], errors="coerce") * df["score_side"]

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
        max_drawdown=_max_drawdown(net.tolist()),
        ic_final=_corr(data.get("aligned_final_score"), gross),
        ic_trend=_corr(data.get("aligned_trend_score"), gross),
        ic_reversion=_corr(data.get("aligned_reversion_score"), gross),
    )


def _build_gates(
    metrics: dict[str, dict[str, HorizonMetrics]],
    config: ViabilityAuditConfig,
) -> list[ViabilityGate]:
    bot5 = metrics["bot_only"].get("5d", _empty_horizon("bot_only", "5d"))
    manual5 = metrics["manual_only"].get("5d", _empty_horizon("manual_only", "5d"))

    gates = [
        ViabilityGate(
            "muestra bot-only 5d",
            bot5.n >= config.min_sample,
            f"n={bot5.n}, minimo={config.min_sample}",
        ),
        ViabilityGate(
            "IC bot-only 5d positivo",
            _positive(bot5.ic_final),
            f"IC={_fmt_float(bot5.ic_final)}",
        ),
        ViabilityGate(
            "EV neto bot-only 5d positivo",
            _positive(bot5.net_ev),
            f"EV neto={_fmt_pct(bot5.net_ev)}",
        ),
    ]

    if manual5.n >= config.min_sample:
        gates.append(
            ViabilityGate(
                "EV neto bot-only mayor que manual-only 5d",
                _gt(bot5.net_ev, manual5.net_ev),
                f"bot={_fmt_pct(bot5.net_ev)} vs manual={_fmt_pct(manual5.net_ev)}",
            )
        )
        gates.append(
            ViabilityGate(
                "drawdown bot-only menor que manual-only 5d",
                _drawdown_better(bot5.max_drawdown, manual5.max_drawdown),
                f"bot={_fmt_pct(bot5.max_drawdown)} vs manual={_fmt_pct(manual5.max_drawdown)}",
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


def _build_verdict(gates: list[ViabilityGate]) -> str:
    if not gates:
        return "NO VALIDADO: sin datos suficientes."

    if any(g.passed is False and "muestra" in g.name for g in gates):
        return "VIABLE COMO PROYECTO, EDGE BOT NO VALIDADO: falta muestra bot-only cerrada."

    if any(g.passed is False for g in gates):
        return "VIABLE COMO SISTEMA, NO VIABLE PARA ESCALAR CAPITAL: no supera IC/EV/drawdown despues de costos."

    if any(g.passed is None for g in gates):
        return "VIABLE COMO PROYECTO, EDGE BOT PARCIAL: falta comparacion manual madura."

    return "VIABLE PARA 180D CON GUARDS: bot-only supera muestra, IC, EV neto y drawdown."


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


def _corr(x, y) -> Optional[float]:
    try:
        xs = pd.to_numeric(pd.Series(x), errors="coerce")
        ys = pd.to_numeric(pd.Series(y), errors="coerce")
        data = pd.concat([xs, ys], axis=1).dropna()
        if len(data) < 5:
            return None
        if data.iloc[:, 0].std() < 1e-12 or data.iloc[:, 1].std() < 1e-12:
            return None
        value = float(data.iloc[:, 0].corr(data.iloc[:, 1]))
        return value if math.isfinite(value) else None
    except Exception:
        return None


def _max_drawdown(returns: list[float]) -> Optional[float]:
    if not returns:
        return None
    equity = 1.0
    peak = 1.0
    worst = 0.0
    for value in returns:
        if value is None or not math.isfinite(float(value)):
            continue
        equity *= max(0.0, 1.0 + float(value))
        peak = max(peak, equity)
        if peak > 0:
            worst = min(worst, equity / peak - 1.0)
    return float(worst)


def _positive(value: Optional[float]) -> Optional[bool]:
    if value is None:
        return None
    return value > 0


def _gt(left: Optional[float], right: Optional[float]) -> Optional[bool]:
    if left is None or right is None:
        return None
    return left > right


def _drawdown_better(left: Optional[float], right: Optional[float]) -> Optional[bool]:
    if left is None or right is None:
        return None
    return left > right


def _fmt_pct(value: Optional[float], *, signed: bool = True) -> str:
    if value is None:
        return "-"
    prefix = "+" if signed else ""
    return format(float(value), f"{prefix}.1%")


def _fmt_float(value: Optional[float]) -> str:
    if value is None:
        return "-"
    return f"{float(value):+.3f}"

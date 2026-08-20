"""Telegram-friendly, read-only view of prospective Radar evidence."""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from datetime import datetime
from html import escape
import json
import os
import sys
from typing import Any, Mapping
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.radar_discovery import (
    DEFAULT_THEORETICAL_COST_BPS,
    RADAR_DISCOVERY_HORIZONS,
    RadarDiscoveryStore,
    summarize_comparisons,
)
from src.collector.db import PortfolioDatabase
from src.core.config import get_config


ART_TZ = ZoneInfo("America/Argentina/Buenos_Aires")


async def load_radar_metrics(
    pool: Any,
    *,
    owner_chat_id: int,
    cost_bps: int = DEFAULT_THEORETICAL_COST_BPS,
) -> dict[str, Any]:
    """Load one frozen Radar version and its accumulated mature outcomes."""
    async with pool.acquire() as conn:
        run_row = await conn.fetchrow(
            """
            SELECT run_id, captured_at, captured_session, scoring_version,
                   protocol_version, universe_count, control_count,
                   evaluated_count, eligible_count, selected_count
            FROM radar_discovery_runs
            WHERE owner_chat_id = $1
            ORDER BY captured_at DESC
            LIMIT 1
            """,
            int(owner_chat_id),
        )
        if run_row is None:
            return {
                "status": "NO_CAPTURE",
                "owner_chat_id": int(owner_chat_id),
                "cost_bps": int(cost_bps),
            }

        run = dict(run_row)
        scoring_version = str(run["scoring_version"])
        version_row = await conn.fetchrow(
            """
            SELECT COUNT(*)::int AS run_count,
                   MIN(captured_session) AS first_session,
                   MAX(captured_session) AS last_session
            FROM radar_discovery_runs
            WHERE owner_chat_id = $1 AND scoring_version = $2
            """,
            int(owner_chat_id),
            scoring_version,
        )
        snapshots = [
            dict(row)
            for row in await conn.fetch(
                """
                SELECT ticker, radar_eligible, rejection_reason,
                       in_portfolio, selected_top_n, price_quality_flag,
                       feature_quality_flag, trend_component_score,
                       relative_strength_component_score,
                       compression_component_score, setup_component_score,
                       discovery_score, setup_score, composite_shadow_score,
                       discovery_percentile, setup_percentile,
                       composite_shadow_percentile, readiness_state,
                       trigger_price, invalidation_price, target_price,
                       setup_risk_reward
                FROM radar_discovery_snapshots
                WHERE run_id = $1
                  AND rejection_reason IS DISTINCT FROM 'benchmark_only'
                ORDER BY composite_shadow_percentile DESC NULLS LAST,
                         composite_shadow_score DESC NULLS LAST,
                         ticker
                """,
                run["run_id"],
            )
        ]
        event_rows = [
            dict(row)
            for row in await conn.fetch(
                """
                SELECT e.event_status, COUNT(*)::int AS total
                FROM radar_setup_events e
                JOIN radar_discovery_snapshots s ON s.id = e.snapshot_id
                WHERE s.run_id = $1
                GROUP BY e.event_status
                ORDER BY e.event_status
                """,
                run["run_id"],
            )
        ]

    store = RadarDiscoveryStore(pool)
    outcomes: dict[int, dict[str, Any]] = {}
    for horizon in RADAR_DISCOVERY_HORIZONS:
        discovery_rows = await store.comparison_rows(
            scoring_version=scoring_version,
            horizon_sessions=horizon,
            owner_chat_id=owner_chat_id,
        )
        trigger_rows = await store.setup_comparison_rows(
            scoring_version=scoring_version,
            horizon_sessions=horizon,
            owner_chat_id=owner_chat_id,
        )
        outcomes[int(horizon)] = {
            "discovery": summarize_comparisons(
                discovery_rows,
                cost_bps=cost_bps,
            ),
            "trigger": summarize_comparisons(
                trigger_rows,
                cost_bps=cost_bps,
            ),
        }

    complete = [
        row
        for row in snapshots
        if row.get("discovery_score") is not None
        and row.get("setup_score") is not None
    ]
    top_shadow = sorted(
        (
            row
            for row in complete
            if not row.get("in_portfolio")
            and str(row.get("feature_quality_flag") or "").upper()
            in {"GOOD", "PARTIAL"}
        ),
        key=lambda row: (
            _float_or(row.get("composite_shadow_percentile"), -1.0),
            _float_or(row.get("composite_shadow_score"), -1.0),
            str(row.get("ticker") or ""),
        ),
        reverse=True,
    )[:3]

    return {
        "status": "OK",
        "owner_chat_id": int(owner_chat_id),
        "cost_bps": int(cost_bps),
        "run": run,
        "version_stats": dict(version_row or {}),
        "snapshot_count": len(snapshots),
        "complete_score_count": len(complete),
        "feature_quality_counts": dict(
            sorted(
                Counter(
                    str(row.get("feature_quality_flag") or "UNKNOWN").upper()
                    for row in snapshots
                ).items()
            )
        ),
        "readiness_counts": dict(
            sorted(
                Counter(
                    str(row.get("readiness_state") or "UNKNOWN").upper()
                    for row in snapshots
                ).items()
            )
        ),
        "event_counts": {
            str(row["event_status"]): int(row["total"])
            for row in event_rows
        },
        "top_shadow": top_shadow,
        "outcomes": outcomes,
    }


def render_radar_metrics(payload: Mapping[str, Any]) -> str:
    """Render compact HTML without presenting shadow scores as probabilities."""
    lines = [
        "🔬 <b>RADAR · MÉTRICAS PROSPECTIVAS</b>",
        "<i>Shadow / audit-only · no cambia ranking, plan ni órdenes.</i>",
    ]
    if payload.get("status") != "OK":
        lines.extend(
            [
                "",
                "Todavía no hay una cohorte prospectiva capturada.",
                "El reporte se habilita después de la próxima corrida programada "
                "del Radar de las <b>16:50 ART</b>.",
                "",
                "No usa replay ni backfill: solo contará evidencia generada desde "
                "la activación del Ledger.",
            ]
        )
        return "\n".join(lines)

    run = dict(payload.get("run") or {})
    version_stats = dict(payload.get("version_stats") or {})
    snapshot_count = int(payload.get("snapshot_count") or 0)
    complete_count = int(payload.get("complete_score_count") or 0)
    coverage = complete_count / snapshot_count if snapshot_count else None

    lines.extend(
        [
            "",
            "<b>ÚLTIMA CAPTURA</b>",
            f"{_fmt_datetime(run.get('captured_at'))} · cohorte "
            f"<b>{escape(str(run.get('captured_session') or 'n/d'))}</b>",
            f"Versión congelada: <code>{escape(_short_version(run.get('scoring_version')))}</code>",
            f"Cohortes de esta versión: <b>{int(version_stats.get('run_count') or 0)}</b>",
            f"Universo: <b>{int(run.get('universe_count') or 0)}</b> · "
            f"evaluados {int(run.get('evaluated_count') or 0)} · "
            f"elegibles {int(run.get('eligible_count') or 0)} · "
            f"top alertado {int(run.get('selected_count') or 0)}",
            f"Scores completos: <b>{complete_count}/{snapshot_count}</b> "
            f"({_pct(coverage)})",
            f"Calidad: {_compact_counts(payload.get('feature_quality_counts'))}",
            f"Estados: {_compact_counts(payload.get('readiness_counts'))}",
        ]
    )

    top_shadow = list(payload.get("top_shadow") or [])
    lines.extend(["", "<b>TOP SHADOW FUERA DE CARTERA</b>"])
    if not top_shadow:
        lines.append("Sin filas comparables en la última cohorte.")
    else:
        for index, raw in enumerate(top_shadow, start=1):
            row = dict(raw)
            ticker = escape(str(row.get("ticker") or "n/d"))
            eligibility = "elegible" if row.get("radar_eligible") else "no elegible"
            lines.append(
                f"{index}. <b>{ticker}</b> · total {_score(row.get('composite_shadow_score'), 100)} "
                f"· D {_score(row.get('discovery_score'), 50)} "
                f"· S {_score(row.get('setup_score'), 50)}"
            )
            lines.append(
                f"   {escape(_state_label(row.get('readiness_state')))} · "
                f"{escape(eligibility)} · calidad "
                f"{escape(str(row.get('feature_quality_flag') or 'n/d'))}"
            )

    lines.extend(["", "<b>OUTCOMES MADUROS</b>"])
    any_mature = False
    outcomes = dict(payload.get("outcomes") or {})
    for horizon in RADAR_DISCOVERY_HORIZONS:
        horizon_payload = dict(outcomes.get(horizon) or outcomes.get(str(horizon)) or {})
        discovery = dict(horizon_payload.get("discovery") or {})
        trigger = dict(horizon_payload.get("trigger") or {})
        discovery_metrics = dict(
            dict(discovery.get("cohorts") or {}).get("discovery_top_quintile") or {}
        )
        trigger_metrics = dict(
            dict(trigger.get("cohorts") or {}).get("all_universe") or {}
        )
        discovery_n = int(discovery_metrics.get("n") or 0)
        trigger_n = int(trigger_metrics.get("n") or 0)
        if discovery_n <= 0 and trigger_n <= 0:
            continue
        any_mature = True
        parts = []
        if discovery_n > 0:
            parts.append(
                "desc top20 "
                f"n={discovery_n} · acierto {_pct(discovery_metrics.get('win_rate'))} · "
                f"neto {_pct(discovery_metrics.get('mean_net_return'), signed=True)} · "
                f"vs univ {_pct(discovery_metrics.get('mean_excess_vs_universe'), signed=True)}"
            )
        if trigger_n > 0:
            parts.append(
                "trigger "
                f"n={trigger_n} · neto {_pct(trigger_metrics.get('mean_net_return'), signed=True)} · "
                f"vs QQQ {_pct(trigger_metrics.get('mean_excess_vs_qqq'), signed=True)}"
            )
        lines.append(f"<b>{horizon}r</b> · " + " | ".join(parts))

    if not any_mature:
        lines.append(
            "Aún no hay resultados maduros. El primer 5r aparecerá cinco ruedas "
            "después de la primera captura."
        )

    lines.extend(
        [
            "",
            f"<i>Neto teórico descuenta {int(payload.get('cost_bps') or 0)} bps. "
            "D=Discovery (tendencia+fuerza relativa); "
            "S=Setup (compresión+disparador). Los scores no son probabilidades.</i>",
        ]
    )
    return "\n".join(lines)


def _compact_counts(values: Any) -> str:
    counts = dict(values or {})
    if not counts:
        return "sin datos"
    return " · ".join(
        f"{escape(str(name))} {int(total)}"
        for name, total in counts.items()
    )


def _fmt_datetime(value: Any) -> str:
    if not isinstance(value, datetime):
        return "Fecha n/d"
    if value.tzinfo is None:
        value = value.replace(tzinfo=ART_TZ)
    return value.astimezone(ART_TZ).strftime("%d/%m/%Y %H:%M ART")


def _short_version(value: Any) -> str:
    raw = str(value or "n/d")
    if ":" not in raw:
        return raw[:32]
    prefix, fingerprint = raw.rsplit(":", 1)
    family = prefix.split("+")[0]
    return f"{family}:{fingerprint[:10]}"


def _state_label(value: Any) -> str:
    return {
        "PRE_BREAKOUT": "pre-breakout",
        "TRIGGERED": "triggered",
        "EXTENDED": "extendido",
        "WATCH": "esperar",
        "DATA_INSUFFICIENT": "datos insuficientes",
    }.get(str(value or "UNKNOWN").upper(), str(value or "n/d").lower())


def _score(value: Any, maximum: int) -> str:
    if value is None:
        return "n/d"
    try:
        return f"{float(value):.1f}/{maximum}"
    except (TypeError, ValueError):
        return "n/d"


def _pct(value: Any, *, signed: bool = False) -> str:
    if value is None:
        return "n/d"
    try:
        return f"{float(value):+0.1%}" if signed else f"{float(value):.1%}"
    except (TypeError, ValueError):
        return "n/d"


def _float_or(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


async def main(args: argparse.Namespace) -> int:
    cfg = get_config()
    owner_chat_id = args.owner_chat_id
    if owner_chat_id is None:
        configured_owner = str(cfg.scraper.telegram_chat_id or "").strip()
        owner_chat_id = int(configured_owner) if configured_owner.isdigit() else None
    if owner_chat_id is None:
        print("Falta --owner-chat-id y TELEGRAM_CHAT_ID no es numérico.")
        return 2

    db = PortfolioDatabase(cfg.database.url)
    await db.connect()
    try:
        payload = await load_radar_metrics(
            await db.get_pool(),
            owner_chat_id=owner_chat_id,
            cost_bps=args.cost_bps,
        )
        if args.json:
            print(json.dumps(payload, indent=2, default=str))
        else:
            print(render_radar_metrics(payload))
        return 0
    finally:
        await db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Vista read-only de métricas prospectivas del Radar",
    )
    parser.add_argument("--owner-chat-id", type=int, default=None)
    parser.add_argument(
        "--cost-bps",
        type=int,
        default=DEFAULT_THEORETICAL_COST_BPS,
    )
    parser.add_argument("--json", action="store_true")
    raise SystemExit(asyncio.run(main(parser.parse_args())))

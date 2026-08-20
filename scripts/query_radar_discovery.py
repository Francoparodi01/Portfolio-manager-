"""Read-only comparisons for one frozen Radar Discovery scoring version."""
from __future__ import annotations

import argparse
import asyncio
from datetime import date
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.radar_discovery import (
    DEFAULT_THEORETICAL_COST_BPS,
    RadarDiscoveryStore,
    summarize_comparisons,
)
from src.collector.db import PortfolioDatabase
from src.core.config import get_config


async def main(args: argparse.Namespace) -> int:
    cfg = get_config()
    owner_chat_id = args.owner_chat_id
    if owner_chat_id is None:
        configured_owner = str(cfg.scraper.telegram_chat_id or "").strip()
        owner_chat_id = int(configured_owner) if configured_owner.isdigit() else None
    if owner_chat_id is None:
        print("Falta --owner-chat-id y TELEGRAM_CHAT_ID no es numerico.")
        return 2
    db = PortfolioDatabase(cfg.database.url)
    await db.connect()
    try:
        pool = await db.get_pool()
        store = RadarDiscoveryStore(pool)
        scoring_version = args.scoring_version
        if scoring_version == "latest":
            scoring_version = await store.latest_scoring_version(
                owner_chat_id=owner_chat_id
            )
        if not scoring_version:
            print("No hay una scoring_version prospectiva disponible.")
            return 1
        query_method = (
            store.setup_comparison_rows
            if args.anchor == "trigger"
            else store.comparison_rows
        )
        rows = await query_method(
            scoring_version=scoring_version,
            horizon_sessions=args.horizon,
            owner_chat_id=owner_chat_id,
            since=date.fromisoformat(args.since) if args.since else None,
        )
        payload = {
            "scoring_version": scoring_version,
            "owner_chat_id": owner_chat_id,
            "horizon_sessions": args.horizon,
            "anchor": args.anchor,
            **summarize_comparisons(rows, cost_bps=args.cost_bps),
        }
        if args.json:
            print(json.dumps(payload, indent=2, default=str))
        else:
            _print_report(payload)
        return 0
    finally:
        await db.close()


def _print_report(payload: dict) -> None:
    print("RADAR DISCOVERY LEDGER - COMPARACION PROSPECTIVA")
    print(f"Version: {payload['scoring_version']}")
    print(f"Ancla: {payload['anchor']}")
    print(
        f"Horizonte: {payload['horizon_sessions']} ruedas | "
        f"filas maduras: {payload['sample_rows']} + {payload['control_rows']} controles | "
        f"excluidas calidad: {payload['quality_excluded_rows']} | "
        f"costo: {payload['cost_bps']} bps"
    )
    print()
    for name, metrics in payload["cohorts"].items():
        print(
            f"{name:18} n={metrics['n']:4d} "
            f"win={_pct(metrics['win_rate'])} "
            f"net={_pct(metrics['mean_net_return'])} "
            f"exceso_univ={_pct(metrics['mean_excess_vs_universe'])} "
            f"drawdown={_pct(metrics['mean_max_drawdown'])}"
        )
    print()
    comparisons = payload["comparisons"]
    print(
        "Top 5 - resto elegible (neto): "
        f"{_pct(comparisons['top_5_minus_eligible_rest_net'])}"
    )
    print(
        "V3 A - V3 rechazados (neto): "
        f"{_pct(comparisons['v3_a_minus_rejected_net'])}"
    )
    quality = ", ".join(
        f"{name}={count}"
        for name, count in payload["price_quality_counts"].items()
    ) or "sin filas"
    print(f"Calidad de precios: {quality}")
    feature_quality = ", ".join(
        f"{name}={count}"
        for name, count in payload["feature_quality_counts"].items()
    ) or "sin mediciones shadow"
    print(f"Calidad de features: {feature_quality}")
    print()
    ic = payload["information_coefficient_spearman"]
    rho = "n/d" if ic["rho"] is None else f"{ic['rho']:+.3f}"
    print(
        f"IC Spearman secundario: media diaria={rho} "
        f"sesiones={ic['sessions']} n={ic['n']}"
    )


def _pct(value) -> str:
    return "n/d" if value is None else f"{float(value):+.2%}"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Consulta read-only del Radar Discovery Ledger"
    )
    parser.add_argument("--horizon", type=int, choices=[5, 10, 20, 40], default=20)
    parser.add_argument(
        "--anchor",
        choices=["discovery", "trigger"],
        default="discovery",
        help="Inicio del outcome: descubrimiento diario o trigger del setup.",
    )
    parser.add_argument(
        "--scoring-version",
        default="latest",
        help="Version exacta o 'latest'; las versiones nunca se mezclan.",
    )
    parser.add_argument("--owner-chat-id", type=int, default=None)
    parser.add_argument("--since", help="Fecha minima YYYY-MM-DD", default=None)
    parser.add_argument(
        "--cost-bps",
        type=int,
        default=DEFAULT_THEORETICAL_COST_BPS,
    )
    parser.add_argument("--json", action="store_true")
    raise SystemExit(asyncio.run(main(parser.parse_args())))

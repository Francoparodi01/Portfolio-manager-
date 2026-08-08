"""Audit blocked decisions in an isolated learning layer.

The command reads decision_log and shadow_thesis_* as evidence. It writes only
to learning_shadow_* tables and never changes analysis, scoring, guards, plans,
orders, forecasts, or canonical outcomes.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from uuid import uuid4
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.learning_shadow import (
    DEFAULT_MATERIAL_RETURN_BPS,
    LearningShadowCase,
    PLANNER_BLOCKED,
    build_metric_rows,
    build_rule_candidates,
)
from src.analysis.learning_shadow_store import LearningShadowStore
from src.collector.db import PortfolioDatabase
from src.core.config import get_config


ART_TZ = ZoneInfo("America/Argentina/Buenos_Aires")


async def main(args: argparse.Namespace) -> int:
    captured_at = datetime.now(timezone.utc)
    db = PortfolioDatabase(get_config().database.url)
    await db.connect()
    try:
        pool = await db.get_pool()
        if pool is None:
            raise RuntimeError("database pool unavailable")
        store = LearningShadowStore(pool)
        if not args.no_persist:
            await store.ensure_schema()
        rows = await store.load_case_inputs(
            owner_chat_id=args.owner_chat_id,
            lookback_days=args.days,
        )
        cases = [
            LearningShadowCase.from_mapping(
                row,
                as_of=captured_at,
                material_return_bps=args.material_return_bps,
            )
            for row in rows
        ]
        if args.no_persist:
            metrics = build_metric_rows(cases)
            run_id = None
        else:
            run_id = uuid4()
            metrics = await store.save_evaluation(
                run_id=run_id,
                owner_chat_id=args.owner_chat_id,
                captured_at=captured_at,
                snapshot_date=captured_at.astimezone(ART_TZ).date(),
                lookback_days=args.days,
                material_return_bps=args.material_return_bps,
                cases=cases,
            )
        rule_candidates = build_rule_candidates(cases)
        primary_metrics = [
            metric for metric in metrics
            if metric["case_population"] == PLANNER_BLOCKED
        ]

        payload = {
            "run_id": str(run_id) if run_id else None,
            "owner_chat_id": args.owner_chat_id,
            "lookback_days": args.days,
            "material_return_bps": args.material_return_bps,
            "decisions_seen": len({case.decision_log_id for case in cases}),
            "cases_evaluated": len(cases),
            "metrics": metrics,
            "primary_metrics": primary_metrics,
            "rule_candidates": rule_candidates,
            "operational_effect": False,
        }
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, default=str))
        else:
            print(
                "Learning shadow: "
                f"decisions={payload['decisions_seen']} cases={len(cases)} "
                f"persisted={'no' if args.no_persist else 'yes'}"
            )
            for metric in metrics:
                if metric["total_cases"] == 0:
                    continue
                rate = metric["potential_false_negative_rate"]
                coverage = metric["shadow_coverage_rate"]
                print(
                    f"  {metric['case_population']} {metric['horizon_days']}D: "
                    f"mature={metric['matured_cases']} "
                    f"potential_fn={metric['potential_false_negatives']} "
                    f"clean={metric['clean_missed_opportunities']} "
                    f"rate={rate if rate is not None else 'n/a'} "
                    f"shadow_coverage={coverage if coverage is not None else 'n/a'}"
                )
            print(f"  rule_candidates={len(rule_candidates)} status=PROPOSED")
            print("Boundary: audit only; no planner, scoring, guard or execution changes.")
        return 0
    finally:
        await db.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--owner-chat-id", type=int, default=0)
    parser.add_argument("--days", type=int, default=365)
    parser.add_argument(
        "--material-return-bps",
        type=int,
        default=DEFAULT_MATERIAL_RETURN_BPS,
        help="Umbral direccional para falso negativo potencial. Default: 75 bps.",
    )
    parser.add_argument("--no-persist", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    if args.days <= 0:
        parser.error("--days debe ser positivo")
    if args.material_return_bps < 0:
        parser.error("--material-return-bps no puede ser negativo")
    return args


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main(_parse_args())))

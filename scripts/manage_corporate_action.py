"""Load and inspect structured corporate actions.

This command does not search the web. Operators or structured ingestors must
provide the source and exact instrument transform.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import date, datetime, time
import json
import os
import sys
from uuid import UUID
from zoneinfo import ZoneInfo

import asyncpg

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.corporate_actions import (
    CorporateEventStatus,
    CorporateEventType,
    EvidenceLevel,
    IngestionMethod,
    instrument_id_for,
)
from src.collector.db import PortfolioDatabase
from src.core.config import get_config


ART_TZ = ZoneInfo("America/Argentina/Buenos_Aires")


def parse_ratio(value: str) -> tuple[float, int, int]:
    clean = str(value or "").strip()
    if ":" not in clean:
        raise argparse.ArgumentTypeError("ratio must use NEW:OLD, for example 10:1 or 3:2")
    numerator_raw, denominator_raw = clean.split(":", 1)
    try:
        numerator = int(numerator_raw)
        denominator = int(denominator_raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("ratio values must be integers") from exc
    if numerator <= 0 or denominator <= 0:
        raise argparse.ArgumentTypeError("ratio values must be positive")
    return numerator / denominator, numerator, denominator


def parse_effective_at(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("effective date must be ISO-8601") from exc
    if parsed.tzinfo is None:
        if "T" not in str(value):
            parsed = datetime.combine(parsed.date(), time(0, 0), tzinfo=ART_TZ)
        else:
            parsed = parsed.replace(tzinfo=ART_TZ)
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage structured corporate actions")
    subparsers = parser.add_subparsers(dest="command", required=True)

    upsert = subparsers.add_parser("upsert", help="Create or update an event and instrument effect")
    upsert.add_argument("--issuer-id", required=True)
    upsert.add_argument("--ticker", required=True)
    upsert.add_argument(
        "--event-type",
        choices=[item.value for item in CorporateEventType],
        required=True,
    )
    upsert.add_argument(
        "--status",
        choices=[item.value for item in CorporateEventStatus],
        default=CorporateEventStatus.CONFIRMED.value,
    )
    upsert.add_argument("--effective-at", type=parse_effective_at, required=True)
    upsert.add_argument("--ratio", type=parse_ratio, required=True, help="NEW:OLD quantity ratio")
    upsert.add_argument("--venue", default="BYMA")
    upsert.add_argument("--asset-type", default="ACCION")
    upsert.add_argument("--currency", default="ARS")
    upsert.add_argument("--instrument-id")
    upsert.add_argument("--event-key")
    upsert.add_argument("--source-name", required=True)
    upsert.add_argument("--source-url", required=True)
    upsert.add_argument(
        "--evidence-level",
        choices=[item.value for item in EvidenceLevel],
        default=EvidenceLevel.PRIMARY_OFFICIAL.value,
    )
    upsert.add_argument("--depositary-ratio-before", default="")
    upsert.add_argument("--depositary-ratio-after", default="")
    upsert.add_argument("--notes", default="")
    upsert.add_argument("--dry-run", action="store_true")

    list_parser = subparsers.add_parser("list", help="List active and recent effects")
    list_parser.add_argument("--ticker", action="append", default=[])

    invalidate = subparsers.add_parser(
        "invalidate-run",
        help="Supersede an unexecuted ExecutionPlan run after a corporate-action incident",
    )
    invalidate.add_argument("--run-id", type=UUID, required=True)
    invalidate.add_argument("--event-key", required=True)
    invalidate.add_argument("--reason", required=True)
    return parser


def _upsert_payload(args: argparse.Namespace) -> dict:
    quantity_factor, ratio_numerator, ratio_denominator = args.ratio
    if quantity_factor == 1.0 and not (
        args.depositary_ratio_before
        and args.depositary_ratio_after
        and args.depositary_ratio_before != args.depositary_ratio_after
    ):
        raise ValueError(
            "an identity instrument transform requires different depositary ratios"
        )
    price_factor = 1.0 / quantity_factor
    ticker = str(args.ticker).upper().strip()
    instrument_id = args.instrument_id or instrument_id_for(
        ticker,
        venue=args.venue,
        asset_type=args.asset_type,
        currency=args.currency,
    )
    event_key = args.event_key or ":".join(
        (
            str(args.issuer_id).upper().strip(),
            ticker,
            str(args.event_type).upper(),
            args.effective_at.astimezone(ART_TZ).date().isoformat(),
        )
    )
    return {
        "event_key": event_key,
        "issuer_id": str(args.issuer_id).upper().strip(),
        "event_type": str(args.event_type).upper(),
        "lifecycle_status": str(args.status).upper(),
        "effective_at": args.effective_at,
        "instrument_id": instrument_id,
        "ticker": ticker,
        "quantity_factor": quantity_factor,
        "price_factor": price_factor,
        "cost_basis_factor": price_factor,
        "source_name": str(args.source_name).strip(),
        "source_url": str(args.source_url).strip(),
        "ingestion_method": IngestionMethod.MANUAL.value,
        "evidence_level": str(args.evidence_level).upper(),
        "venue": str(args.venue).upper(),
        "asset_type": str(args.asset_type).upper(),
        "currency": str(args.currency).upper(),
        "depositary_ratio_before": str(args.depositary_ratio_before),
        "depositary_ratio_after": str(args.depositary_ratio_after),
        "metadata": {
            "ratio_numerator": ratio_numerator,
            "ratio_denominator": ratio_denominator,
            "notes": str(args.notes or ""),
        },
        "raw_payload": {
            "operator_command": "manage_corporate_action upsert",
            "ratio": f"{ratio_numerator}:{ratio_denominator}",
        },
    }


def _serialize_payload(payload: dict) -> dict:
    return {
        key: value.isoformat() if isinstance(value, (datetime, date)) else value
        for key, value in payload.items()
    }


async def run(args: argparse.Namespace) -> int:
    if args.command == "upsert" and args.dry_run:
        print(json.dumps(_serialize_payload(_upsert_payload(args)), indent=2, ensure_ascii=False))
        return 0
    cfg = get_config()

    if args.command == "invalidate-run":
        conn = await asyncpg.connect(cfg.database.url)
        try:
            async with conn.transaction():
                event_id = await conn.fetchval(
                    "SELECT id FROM corporate_events WHERE event_key = $1",
                    str(args.event_key),
                )
                if event_id is None:
                    raise ValueError(f"corporate event not found: {args.event_key}")

                executed_count = await conn.fetchval(
                    """
                    SELECT COUNT(*)
                    FROM decision_log dl
                    WHERE dl.run_id = $1::uuid
                      AND COALESCE(dl.source, dl.layers->>'source') = 'execution_plan'
                      AND (
                            dl.status IN ('EXECUTED', 'EXECUTED_MANUAL')
                         OR EXISTS (
                                SELECT 1
                                FROM broker_fills bf
                                WHERE bf.decision_log_id = dl.id
                                  AND NOT (
                                      COALESCE(bf.raw_payload, '{}'::jsonb)
                                      ? 'superseded_by_real'
                                  )
                            )
                      )
                    """,
                    args.run_id,
                )
                if int(executed_count or 0) > 0:
                    raise ValueError(
                        f"run {args.run_id} has executed decisions or linked fills"
                    )

                rows = await conn.fetch(
                    """
                    UPDATE decision_log
                    SET
                        status = 'SUPERSEDED',
                        is_executable = FALSE,
                        was_blocked = TRUE,
                        block_reason = $3,
                        executed_amount_ars = 0,
                        decision_stage = 'superseded',
                        metric_scope = 'planner_audit',
                        is_primary_metric = FALSE,
                        layers = COALESCE(layers, '{}'::jsonb) || jsonb_build_object(
                            'invalidation',
                            jsonb_build_object(
                                'type', 'CORPORATE_ACTION',
                                'event_id', $2::bigint,
                                'event_key', $4::text,
                                'reason', $3::text,
                                'previous_status', status,
                                'previous_executed_amount_ars', executed_amount_ars,
                                'invalidated_at', NOW()
                            )
                        )
                    WHERE run_id = $1::uuid
                      AND COALESCE(source, layers->>'source') = 'execution_plan'
                      AND status = 'APPROVED'
                    RETURNING id, ticker, decision, status
                    """,
                    args.run_id,
                    int(event_id),
                    str(args.reason),
                    str(args.event_key),
                )
                if not rows:
                    raise ValueError(
                        f"run {args.run_id} has no APPROVED execution-plan rows"
                    )

            print(json.dumps({
                "run_id": str(args.run_id),
                "event_key": str(args.event_key),
                "status": "SUPERSEDED",
                "decisions": [dict(row) for row in rows],
            }, ensure_ascii=False, default=str))
            return 0
        finally:
            await conn.close()

    db = PortfolioDatabase(cfg.database.url)
    await db.connect()
    try:
        if args.command == "list":
            effects = await db.get_corporate_action_effects(tickers=args.ticker)
            print(json.dumps([
                {
                    "event_id": effect.event_id,
                    "effect_id": effect.effect_id,
                    "event_key": effect.event_key,
                    "ticker": effect.ticker,
                    "event_type": effect.event_type,
                    "status": effect.lifecycle_status,
                    "effective_at": effect.effective_at.isoformat(),
                    "quantity_factor": effect.quantity_factor,
                    "price_factor": effect.price_factor,
                    "source_name": effect.source_name,
                    "source_url": effect.source_url,
                }
                for effect in effects
            ], indent=2, ensure_ascii=False))
            return 0

        payload = _upsert_payload(args)
        event_id, effect_id = await db.upsert_corporate_action(**payload)
        print(json.dumps({
            "event_id": event_id,
            "effect_id": effect_id,
            "event_key": payload["event_key"],
            "ticker": payload["ticker"],
            "status": payload["lifecycle_status"],
        }, ensure_ascii=False))
        return 0
    finally:
        await db.close()


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return asyncio.run(run(args))
    except ValueError as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    raise SystemExit(main())

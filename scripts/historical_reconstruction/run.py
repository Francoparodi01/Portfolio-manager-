"""Historical Reconstruction v1: read-only audit of legacy Quantia evidence.

The command never writes to PostgreSQL. It reconstructs formal-plan lineage,
classifies evidence confidence and recalculates outcomes from raw BYMA candles.
"""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from datetime import timedelta
import json
import os
from pathlib import Path
import sys

import asyncpg
from dotenv import load_dotenv

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
DASHBOARD_DIR = ROOT / "scripts" / "bot_stats_dashboard"
if str(DASHBOARD_DIR) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_DIR))
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from metrics import compute as compute_price_returns, number  # noqa: E402
from reconstruct import (  # noqa: E402
    PRIMARY_LEVELS,
    attach_outcomes,
    reconstruct_episodes,
    reconstruction_summary,
    rows_for_confidence,
)

REQUIRED_DECISION_COLUMNS = {
    "id",
    "owner_chat_id",
    "run_id",
    "decided_at",
    "ticker",
    "source",
    "status",
    "metric_scope",
    "is_primary_metric",
    "superseded_by_id",
}
MAX_ROWS = 25000
MAX_CANDLES = 250000
MAX_EVENTS = 10000


def owner_chat_id() -> str | None:
    return os.environ.get("OWNER_CHAT_ID") or os.environ.get("TELEGRAM_CHAT_ID")


def _dsn() -> str:
    return os.environ["DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://")


async def _validate_read_schema(conn) -> None:
    rows = await conn.fetch(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public'
          AND table_name='decision_log'
          AND column_name = ANY($1::text[])
        """,
        sorted(REQUIRED_DECISION_COLUMNS),
    )
    present = {str(row["column_name"]) for row in rows}
    missing = sorted(REQUIRED_DECISION_COLUMNS - present)
    if missing:
        raise RuntimeError(
            "Historical reconstruction fails closed: decision_log schema missing "
            + ", ".join(missing)
        )


async def _owner_state(conn, owner: int) -> tuple[bool, list[int]]:
    rows = await conn.fetch(
        """
        SELECT DISTINCT owner_chat_id FROM portfolio_snapshots WHERE owner_chat_id IS NOT NULL
        UNION SELECT DISTINCT owner_chat_id FROM decision_log WHERE owner_chat_id IS NOT NULL
        UNION SELECT DISTINCT owner_chat_id FROM broker_fills WHERE owner_chat_id IS NOT NULL
        UNION SELECT DISTINCT owner_chat_id FROM execution_plans WHERE owner_chat_id IS NOT NULL
        """
    )
    owners = sorted({int(row["owner_chat_id"]) for row in rows})
    return owners == [owner], owners


async def load_raw(owner: int, days: int) -> dict:
    """Read one repeatable-read snapshot. No schema assurance or mutations."""
    if not owner or not 30 <= days <= 730:
        raise ValueError("Owner requerido; ventana entre 30 y 730 dias")

    conn = await asyncpg.connect(
        _dsn(),
        timeout=15,
        server_settings={
            "default_transaction_read_only": "on",
            "statement_timeout": "20000",
        },
    )
    try:
        async with conn.transaction(readonly=True, isolation="repeatable_read"):
            as_of = await conn.fetchval("SELECT NOW()")
            readonly = await conn.fetchval(
                "SELECT current_setting('transaction_read_only')"
            )
            if readonly != "on":
                raise RuntimeError("Historical reconstruction requires read-only DB")

            await _validate_read_schema(conn)
            start = as_of - timedelta(days=days)
            legacy_owner_verified, explicit_owners = await _owner_state(conn, owner)

            capture_table = bool(
                await conn.fetchval(
                    "SELECT to_regclass('public.decision_lab_plan_captures') IS NOT NULL"
                )
            )
            immutable_plan_ids: set[str] = set()
            if capture_table:
                capture_rows = await conn.fetch(
                    """
                    SELECT plan_id
                    FROM decision_lab_plan_captures
                    WHERE owner_chat_id=$1
                      AND captured_at >= $2 AND captured_at <= $3
                    """,
                    owner,
                    start,
                    as_of,
                )
                immutable_plan_ids = {
                    str(row["plan_id"]) for row in capture_rows if row["plan_id"] is not None
                }

            rows = await conn.fetch(
                """
                SELECT
                    p.id AS plan_id,
                    p.owner_chat_id AS plan_owner_chat_id,
                    p.run_id AS plan_run_id,
                    p.created_at,
                    p.updated_at AS plan_updated_at,
                    p.source AS plan_source,
                    p.feasible,
                    i.id AS intent_id,
                    i.decision_log_id,
                    i.ticker,
                    i.side,
                    i.is_executable,
                    i.was_blocked,
                    i.created_at AS intent_created_at,
                    i.updated_at AS intent_updated_at,
                    (d.id IS NOT NULL) AS decision_exists,
                    d.owner_chat_id AS decision_owner_chat_id,
                    d.run_id AS decision_run_id,
                    d.decided_at AS decision_decided_at,
                    d.ticker AS decision_ticker,
                    COALESCE(d.source, d.layers->>'source') AS decision_source,
                    d.status AS decision_status,
                    d.metric_scope AS decision_metric_scope,
                    d.is_primary_metric,
                    d.superseded_by_id
                FROM execution_plans p
                JOIN order_intents i ON i.execution_plan_id=p.id
                LEFT JOIN decision_log d ON d.id=i.decision_log_id
                WHERE p.created_at >= $2 AND p.created_at <= $3
                  AND (
                    p.owner_chat_id=$1
                    OR ($4::boolean AND p.owner_chat_id IS NULL)
                  )
                ORDER BY p.created_at,p.id,i.sequence_no,i.id
                LIMIT 25001
                """,
                owner,
                start,
                as_of,
                legacy_owner_verified,
            )

            legacy_null_plan_count = await conn.fetchval(
                """
                SELECT count(*)
                FROM execution_plans
                WHERE owner_chat_id IS NULL
                  AND created_at >= $1 AND created_at <= $2
                """,
                start,
                as_of,
            )

            plan_inventory = dict(
                await conn.fetchrow(
                    """
                    SELECT
                      count(*) AS plan_rows,
                      count(*) FILTER (WHERE source='execution_plan') AS formal_plans,
                      count(*) FILTER (WHERE feasible) AS feasible_plans,
                      count(*) FILTER (WHERE owner_chat_id=$1) AS explicit_owner_plans,
                      count(*) FILTER (WHERE owner_chat_id IS NULL) AS null_owner_plans
                    FROM execution_plans
                    WHERE created_at >= $2 AND created_at <= $3
                      AND (
                        owner_chat_id=$1
                        OR ($4::boolean AND owner_chat_id IS NULL)
                      )
                    """,
                    owner,
                    start,
                    as_of,
                    legacy_owner_verified,
                )
            )

            decision_sources = [
                dict(row)
                for row in await conn.fetch(
                    """
                    SELECT
                      COALESCE(source,layers->>'source','unknown') AS source,
                      COALESCE(metric_scope,'unknown') AS metric_scope,
                      COALESCE(status,'unknown') AS status,
                      count(*) AS n
                    FROM decision_log
                    WHERE decided_at >= $2 AND decided_at <= $3
                      AND (
                        owner_chat_id=$1
                        OR ($4::boolean AND owner_chat_id IS NULL)
                      )
                    GROUP BY 1,2,3
                    ORDER BY 1,2,3
                    """,
                    owner,
                    start,
                    as_of,
                    legacy_owner_verified,
                )
            ]

            tickers = sorted(
                {
                    str(row["ticker"]).strip().upper()
                    for row in rows
                    if str(row["plan_source"] or "").strip().lower()
                    == "execution_plan"
                    and str(row["ticker"] or "").strip()
                }
            )
            candles = (
                await conn.fetch(
                    """
                    SELECT
                      ticker,long_ticker,source,currency,venue,interval,
                      ts,open_price,close_price,scraped_at
                    FROM market_candles
                    WHERE ticker=ANY($1::text[])
                      AND interval='1d'
                      AND currency='ARS'
                      AND venue='BYMA'
                      AND ts >= $2 AND ts <= $3
                      AND scraped_at <= $3
                    ORDER BY ticker,ts,source,long_ticker
                    LIMIT 250001
                    """,
                    tickers,
                    start - timedelta(days=7),
                    as_of,
                )
                if tickers
                else []
            )

            registry_available = bool(
                await conn.fetchval(
                    """
                    SELECT
                      to_regclass('public.corporate_events') IS NOT NULL
                      AND to_regclass('public.corporate_event_instrument_effects') IS NOT NULL
                    """
                )
            )
            events = (
                await conn.fetch(
                    """
                    SELECT
                      e.event_type,e.lifecycle_status,e.effective_at,
                      f.ticker,f.price_factor
                    FROM corporate_events e
                    JOIN corporate_event_instrument_effects f ON f.event_id=e.id
                    WHERE f.ticker=ANY($1::text[])
                      AND f.is_active
                      AND (f.venue IS NULL OR f.venue='BYMA')
                      AND (f.currency IS NULL OR f.currency='ARS')
                      AND e.effective_at >= $2 AND e.effective_at <= $3
                    LIMIT 10001
                    """,
                    tickers,
                    start,
                    as_of,
                )
                if registry_available and tickers
                else []
            )

            account = dict(
                await conn.fetchrow(
                    """
                    SELECT
                      (SELECT count(*) FROM portfolio_snapshots
                       WHERE scraped_at >= $2 AND scraped_at <= $3
                         AND (
                           owner_chat_id=$1
                           OR ($4::boolean AND owner_chat_id IS NULL)
                         )) AS snapshots,
                      (SELECT count(*) FROM broker_fills
                       WHERE executed_at >= $2 AND executed_at <= $3
                         AND (
                           owner_chat_id=$1
                           OR ($4::boolean AND owner_chat_id IS NULL)
                         )) AS fills
                    """,
                    owner,
                    start,
                    as_of,
                    legacy_owner_verified,
                )
            )

        if len(rows) > MAX_ROWS or len(candles) > MAX_CANDLES or len(events) > MAX_EVENTS:
            raise ValueError("Extraccion excede el limite; reducir la ventana")

        return {
            "rows": [dict(row) for row in rows],
            "candles": [dict(row) for row in candles],
            "events": [dict(row) for row in events],
            "events_available": registry_available,
            "immutable_plan_ids": immutable_plan_ids,
            "capture_table_available": capture_table,
            "legacy_owner_verified": legacy_owner_verified,
            "explicit_owner_count": len(explicit_owners),
            "legacy_null_plan_count": int(legacy_null_plan_count or 0),
            "plan_inventory": plan_inventory,
            "decision_sources": decision_sources,
            "account": account,
            "as_of": as_of,
            "start": start,
            "readonly": readonly,
        }
    finally:
        await conn.close()


def _context_source_counts(rows: list[dict]) -> dict[str, int]:
    counts = Counter()
    for row in rows:
        source = str(row.get("source") or "unknown").strip().lower()
        counts[source] += int(row.get("n") or 0)
    return dict(sorted(counts.items()))


async def build_report(owner: int, *, days: int = 180, cost_bps: float = 75.0) -> dict:
    if number(cost_bps) is None or not 0 <= float(cost_bps) <= 400:
        raise ValueError("Costo debe ser finito y estar entre 0 y 400 pb")
    raw = await load_raw(owner, days)
    reconstruction = reconstruct_episodes(
        raw["rows"],
        requested_owner=owner,
        legacy_owner_verified=raw["legacy_owner_verified"],
        immutable_plan_ids=set(raw["immutable_plan_ids"]),
    )

    primary_rows = rows_for_confidence(
        raw["rows"], reconstruction["episodes"], PRIMARY_LEVELS
    )
    low_rows = rows_for_confidence(
        raw["rows"], reconstruction["episodes"], {"LOW"}
    )

    primary = compute_price_returns(
        primary_rows,
        raw["candles"],
        as_of=raw["as_of"],
        cost_bps=float(cost_bps),
        events=raw["events"],
        events_available=raw["events_available"],
    )
    low = compute_price_returns(
        low_rows,
        raw["candles"],
        as_of=raw["as_of"],
        cost_bps=float(cost_bps),
        events=raw["events"],
        events_available=raw["events_available"],
    )
    combined_signals = {
        "signals": list(primary.get("signals", [])) + list(low.get("signals", []))
    }
    reconstruction["episodes"] = attach_outcomes(
        reconstruction["episodes"], combined_signals
    )

    owner_scope = (
        "EXPLICIT_PLUS_VERIFIED_LEGACY_NULL_AS_LOW"
        if raw["legacy_owner_verified"]
        else "EXPLICIT_ONLY_LEGACY_NULL_UNASSIGNED"
    )

    return {
        "schema": "quantia-historical-reconstruction-v1",
        "mode": "DRY_RUN_READ_ONLY",
        "as_of": raw["as_of"].isoformat(),
        "window_start": raw["start"].isoformat(),
        "days": days,
        "cost_bps": float(cost_bps),
        "owner_scope": owner_scope,
        "database_readonly": raw["readonly"],
        "invariants": {
            "historical_outcome_columns_used": False,
            "writes_performed": False,
            "radar_in_primary_sample": False,
            "optimizer_in_primary_sample": False,
            "low_in_primary_metrics": False,
            "unrecoverable_in_primary_metrics": False,
        },
        "inventory": {
            "plans": raw["plan_inventory"],
            "raw_joined_intents": len(raw["rows"]),
            "raw_candles": len(raw["candles"]),
            "corporate_events": len(raw["events"]),
            "corporate_registry_available": raw["events_available"],
            "immutable_capture_table_available": raw["capture_table_available"],
            "immutable_plan_captures": len(raw["immutable_plan_ids"]),
            "legacy_null_plan_rows_in_window": raw["legacy_null_plan_count"],
            "account_context_only": raw["account"],
        },
        "source_separation": {
            "decision_log_groups": raw["decision_sources"],
            "decision_source_counts": _context_source_counts(raw["decision_sources"]),
            "formal_sample_source": "execution_plans + order_intents only",
            "account_context_is_not_bot_outcome": True,
        },
        "reconstruction": reconstruction,
        "outcomes": {
            "primary": primary,
            "low": low,
            "unrecoverable": {
                "n": reconstruction["confidence_counts"]["UNRECOVERABLE"],
                "status": "EXCLUDED",
            },
        },
        "legacy_comparison": {
            "historical_outcome_columns_used": False,
            "comparison_basis": (
                "Counts, lineage contradictions and reconstructed raw-price outcomes only; "
                "stored outcome_* values are intentionally ignored."
            ),
            "primary_reconstructed_intents": len(primary_rows),
            "low_reconstructed_intents": len(low_rows),
            "unrecoverable_intents": reconstruction["confidence_counts"][
                "UNRECOVERABLE"
            ],
        },
        "limitations": [
            "HIGH requires an immutable formal-plan capture plus coherent explicit-owner lineage.",
            "MEDIUM uses coherent explicit-owner plan/order rows whose original pre-mutation version is not independently frozen.",
            "Strict single-owner NULL inference is always LOW and never enters primary metrics.",
            "Historical decision rows that contradict owner/source/ticker/run lineage are UNRECOVERABLE.",
            "Outcomes are nominal price counterfactuals, not realized portfolio PnL.",
            "Corporate-action coverage remains fail-closed through the audited dashboard calculator.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument("--days", type=int, default=180)
    parser.add_argument("--cost-bps", type=float, default=75.0)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=ROOT / "output" / "historical-reconstruction",
    )
    args = parser.parse_args()
    load_dotenv(args.env_file, override=False)
    if not os.environ.get("DATABASE_URL") or not owner_chat_id():
        raise SystemExit(
            "Se necesitan DATABASE_URL y OWNER_CHAT_ID o TELEGRAM_CHAT_ID"
        )

    report = asyncio.run(
        build_report(
            int(owner_chat_id()),
            days=args.days,
            cost_bps=args.cost_bps,
        )
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    json_path = args.output_dir / "historical-reconstruction-v1.json"
    md_path = args.output_dir / "historical-reconstruction-v1.md"
    json_path.write_text(
        json.dumps(report, ensure_ascii=False, default=str, allow_nan=False, indent=2),
        encoding="utf-8",
    )
    md_path.write_text(reconstruction_summary(report), encoding="utf-8")
    print(
        json.dumps(
            {
                "json": str(json_path),
                "summary": str(md_path),
                "mode": report["mode"],
                "owner_scope": report["owner_scope"],
                "confidence_counts": report["reconstruction"]["confidence_counts"],
                "readonly": report["database_readonly"],
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()

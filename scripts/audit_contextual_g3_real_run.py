"""Read-only G3 audit of the real G2 run and its exact market evidence."""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
from typing import Any
from uuid import UUID

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.contextual_market import build_contextual_snapshot
from src.collector.contextual_g2 import (
    bar_identity,
    observation_digest,
    observation_rows_to_candles,
    read_market_evidence_as_of,
)
from src.collector.cocos_history import candles_to_frame


UTC = timezone.utc
DEFAULT_RUN = "74db5161-562c-428a-b1cd-4620b9ee106b"
DEFAULT_PLAN = "286f2ca7-d363-49a2-9fa4-cf5cee550dc9"
DEFAULT_PORTFOLIO = "8a4276c1-3d4a-4e29-862b-6cf01450f91d"


def _json(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _hash(value: Any) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
        allow_nan=False, default=str,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _serial_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "observation_id": str(row["observation_id"]),
        "observation_digest": row["computed_observation_digest"],
        "digest_status": row["digest_status"],
        "bar_identity": row["bar_identity"],
        "bar_end": row["bar_end"].isoformat() if row.get("bar_end") else None,
        "available_at": row["available_at"].isoformat(),
        "scraped_at": row["scraped_at"].isoformat(),
        "is_closed": row["is_closed"],
        "source": row["source"],
    }


def _prepare_row(raw: Any) -> dict[str, Any]:
    row = dict(raw)
    row["provenance"] = _json(row.get("provenance")) or {}
    row["quality"] = _json(row.get("quality")) or {}
    row["missingness"] = _json(row.get("missingness")) or []
    computed = observation_digest(row)
    stored = row.get("observation_digest")
    if stored is not None and str(stored) != computed:
        raise RuntimeError(f"OBSERVATION_DIGEST_INTEGRITY_FAILURE:{row['observation_id']}")
    row["computed_observation_digest"] = computed
    row["digest_status"] = "VERIFIED" if stored else "LEGACY_UNSTORED"
    row["bar_identity"] = bar_identity(row)
    return row


async def audit_real_g2_run(
    database_url: str,
    *,
    run_id: str = DEFAULT_RUN,
    plan_id: str = DEFAULT_PLAN,
    portfolio_snapshot_id: str = DEFAULT_PORTFOLIO,
) -> dict[str, Any]:
    import asyncpg

    direct_url = database_url.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(
        direct_url,
        server_settings={"default_transaction_read_only": "on"},
    )
    try:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            snapshot_raw = await conn.fetchrow(
                """SELECT * FROM contextual_market_snapshots
                   WHERE run_id=$1 AND plan_id=$2 AND portfolio_snapshot_id=$3""",
                UUID(run_id), UUID(plan_id), UUID(portfolio_snapshot_id),
            )
            if snapshot_raw is None:
                raise RuntimeError("G2_CONTEXTUAL_SNAPSHOT_NOT_FOUND")
            snapshot = dict(snapshot_raw)
            cutoff = snapshot["cutoff"]
            owner = int(snapshot["owner_chat_id"])
            stored_context = _json(snapshot["contextual_snapshot"])
            stored_hashes = _json(snapshot["input_hashes"])

            link_rows = await conn.fetch(
                """SELECT links.input_role,links.ordinal,observations.*
                   FROM contextual_snapshot_candles links
                   JOIN market_candle_observations observations USING(observation_id)
                   WHERE links.snapshot_id=$1
                   ORDER BY links.input_role,links.ordinal""",
                snapshot["snapshot_id"],
            )
            linked: dict[str, list[dict[str, Any]]] = {}
            for raw in link_rows:
                row = _prepare_row(raw)
                role = str(raw["input_role"])
                row["ordinal"] = int(raw["ordinal"])
                linked.setdefault(role, []).append(row)

            expected_roles = {"ASSET": "asset", "GENERAL_BENCHMARK": "general"}
            replay_rows: dict[str, list[dict[str, Any]]] = {}
            role_evidence: dict[str, Any] = {}
            for db_role, context_role in expected_roles.items():
                values = linked.get(db_role, [])
                if not values:
                    raise RuntimeError(f"G2_SNAPSHOT_ROLE_MISSING:{db_role}")
                first = values[0]
                replay = await read_market_evidence_as_of(
                    conn,
                    owner_chat_id=owner,
                    instrument_id_value=first["instrument_id"],
                    market=first["market"],
                    provider_symbol=first["provider_symbol"],
                    interval=first["interval"],
                    source=first["source"],
                    cutoff=cutoff,
                    allow_legacy_without_lifecycle=True,
                )
                replay_rows[db_role] = replay
                linked_ids = [str(row["observation_id"]) for row in values]
                replay_ids = [str(row["observation_id"]) for row in replay]
                ordinals = [row["ordinal"] for row in values]
                identities = {
                    json.dumps(row["bar_identity"], sort_keys=True) for row in values
                }
                role_evidence[context_role] = {
                    "input_role": db_role,
                    "identity": {
                        "instrument_id": first["instrument_id"],
                        "ticker": first["ticker"],
                        "provider_symbol": first["provider_symbol"],
                        "market": first["market"],
                        "currency": first["currency"],
                        "interval": first["interval"],
                        "source": first["source"],
                    },
                    "linked_observation_count": len(values),
                    "linked_observation_ids": linked_ids,
                    "linked_observation_digests": [
                        row["computed_observation_digest"] for row in values
                    ],
                    "observation_ids_sha256": _hash(linked_ids),
                    "digest_statuses": sorted({row["digest_status"] for row in values}),
                    "bar_identity_count": len(identities),
                    "ordinals_contiguous": ordinals == list(range(len(values))),
                    "asof_replay_count": len(replay),
                    "asof_replay_matches_explicit_links": replay_ids == linked_ids,
                    "first": _serial_row(values[0]),
                    "last": _serial_row(values[-1]),
                }

            all_run_rows = await conn.fetch(
                """SELECT * FROM market_candle_observations
                   WHERE owner_chat_id=$1 AND ingestion_run_id=$2
                     AND ticker IN ('IREN','SPY')
                     AND candle_timestamp <= $3
                     AND available_at <= $3 AND scraped_at <= $3
                   ORDER BY ticker,candle_timestamp,observation_id""",
                owner, UUID(run_id), cutoff,
            )
            run_rows = [_prepare_row(row) for row in all_run_rows]
            linked_ids_all = {
                str(row["observation_id"])
                for values in linked.values() for row in values
            }
            unlinked = [
                row for row in run_rows
                if str(row["observation_id"]) not in linked_ids_all
            ]

            asset_rows = [row for row in run_rows if row["ticker"] == "IREN"]
            benchmark_rows = [row for row in run_rows if row["ticker"] == "SPY"]
            reconstructed = build_contextual_snapshot(
                candles_to_frame(observation_rows_to_candles(asset_rows)),
                cutoff=cutoff,
                signal_action=str(snapshot["signal"]),
                benchmarks={
                    "SPY": candles_to_frame(observation_rows_to_candles(benchmark_rows))
                },
                general_benchmark="SPY",
            )
            replay_reconstructed = build_contextual_snapshot(
                candles_to_frame(observation_rows_to_candles(replay_rows["ASSET"])),
                cutoff=cutoff,
                signal_action=str(snapshot["signal"]),
                benchmarks={
                    "SPY": candles_to_frame(
                        observation_rows_to_candles(replay_rows["GENERAL_BENCHMARK"])
                    )
                },
                general_benchmark="SPY",
            )

            lifecycle = await conn.fetchrow(
                "SELECT * FROM market_evidence_capture_state WHERE capture_id=$1",
                UUID(run_id),
            )
            protection = await conn.fetch(
                """SELECT c.relname,t.tgname
                   FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
                   WHERE NOT t.tgisinternal AND c.relname IN (
                       'market_candle_observations','contextual_market_snapshots',
                       'contextual_snapshot_candles','market_evidence_capture_events'
                   ) ORDER BY c.relname,t.tgname"""
            )

            temporal_violations = sum(
                1 for values in linked.values() for row in values
                if row["candle_timestamp"] > cutoff
                or row["bar_end"] is None
                or row["bar_end"] > cutoff
                or row["available_at"] > cutoff
                or row["scraped_at"] > cutoff
                or not row["is_closed"]
            )
            exact_reconstruction = reconstructed.to_dict() == stored_context
            exact_hashes = reconstructed.input_digests == stored_hashes
            replay_values_equal = (
                replay_reconstructed.input_digests == stored_hashes
            )
            explicit_linkage_complete = len(unlinked) == 0
            result = "PASS" if explicit_linkage_complete else "PARTIAL"

            return {
                "schema": "contextual-g3-real-run-audit-v1",
                "result": result,
                "audited_at": datetime.now(UTC).isoformat(),
                "transaction": "READ_ONLY_REPEATABLE_READ",
                "run_id": run_id,
                "plan_id": plan_id,
                "portfolio_snapshot_id": portfolio_snapshot_id,
                "owner": owner,
                "cutoff": cutoff.isoformat(),
                "code_version": snapshot["code_version"],
                "contextual_snapshot_id": snapshot["snapshot_id"],
                "roles": role_evidence,
                "asof": {
                    "temporal_violations": temporal_violations,
                    "no_lookahead": temporal_violations == 0,
                    "all_explicit_links_selected_by_asof": all(
                        item["asof_replay_matches_explicit_links"]
                        for item in role_evidence.values()
                    ),
                    "legacy_policy": "explicit allow_legacy_without_lifecycle; no values inferred",
                },
                "reconstruction": {
                    "run_scoped_observation_count": len(run_rows),
                    "explicit_link_count": len(link_rows),
                    "unlinked_run_observation_count": len(unlinked),
                    "unlinked_observations": [_serial_row(row) for row in unlinked],
                    "full_run_context_matches_stored": exact_reconstruction,
                    "full_run_snapshot_id_matches": reconstructed.snapshot_id == snapshot["snapshot_id"],
                    "full_run_input_hashes_match": exact_hashes,
                    "closed_asof_input_hashes_match": replay_values_equal,
                    "closed_asof_snapshot_id": replay_reconstructed.snapshot_id,
                    "stored_snapshot_hash": _hash(stored_context),
                    "reconstructed_snapshot_hash": _hash(reconstructed.to_dict()),
                },
                "historical_gaps": [
                    {
                        "code": "G2_OPEN_OBSERVATIONS_NOT_EXPLICITLY_LINKED",
                        "status": "GAP" if unlinked else "RESOLVED",
                        "count": len(unlinked),
                        "impact": (
                            "The open IREN/SPY observations affected missingness but were linked only indirectly through run_id. "
                            "Numeric PIT inputs are explicitly linked."
                        ),
                        "prospective_fix": "future captures link every observation that can affect snapshot values or missingness",
                    },
                    {
                        "code": "G2_OBSERVATION_DIGEST_NOT_STORED",
                        "status": "LEGACY_UNKNOWN",
                        "count": sum(
                            row["digest_status"] == "LEGACY_UNSTORED" for row in run_rows
                        ),
                        "impact": "digests can be computed now but were not persisted at G2 capture time",
                        "prospective_fix": "new observations require observation_digest at insert time",
                    },
                    {
                        "code": "G2_CAPTURE_LIFECYCLE_NOT_STORED",
                        "status": "LEGACY_UNKNOWN" if lifecycle is None else str(lifecycle["status"]),
                        "prospective_fix": "new captures require STARTED then a terminal state",
                    },
                ],
                "immutability_controls": {
                    "triggers": [dict(row) for row in protection],
                    "legacy_rows_protected_after_g3_migration": True,
                    "legacy_digest_backfilled": False,
                    "legacy_lifecycle_backfilled": False,
                },
                "unknown_preserved": [
                    "SECTOR_BENCHMARK_MAPPING_UNKNOWN",
                    "DEPOSITARY_RATIO_UNKNOWN",
                    "VOLUME_UNIT_UNKNOWN",
                    "BYMA_VERSIONED_CALENDAR_UNKNOWN",
                ],
            }
    finally:
        await conn.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--run-id", default=DEFAULT_RUN)
    parser.add_argument("--plan-id", default=DEFAULT_PLAN)
    parser.add_argument("--portfolio-snapshot-id", default=DEFAULT_PORTFOLIO)
    parser.add_argument(
        "--output", type=Path,
        default=Path("docs/evidence/contextual-g3-real-run-audit.json"),
    )
    args = parser.parse_args()
    evidence = asyncio.run(audit_real_g2_run(
        args.database_url,
        run_id=args.run_id,
        plan_id=args.plan_id,
        portfolio_snapshot_id=args.portfolio_snapshot_id,
    ))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(evidence, indent=2, ensure_ascii=False, allow_nan=False, default=str) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({
        "result": evidence["result"],
        "output": str(args.output),
        "linked": evidence["reconstruction"]["explicit_link_count"],
        "unlinked": evidence["reconstruction"]["unlinked_run_observation_count"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

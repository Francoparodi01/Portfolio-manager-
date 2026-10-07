"""Audit a prospective real G3 capture and write closure evidence."""
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
TERMINAL_STATES = {"COMPLETE", "ABORTED", "FAILED"}


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


def _prepare_row(raw: Any) -> dict[str, Any]:
    row = dict(raw)
    row["provenance"] = _json(row.get("provenance")) or {}
    row["quality"] = _json(row.get("quality")) or {}
    row["missingness"] = _json(row.get("missingness")) or []
    computed = observation_digest(row)
    stored = row.get("observation_digest")
    row["computed_observation_digest"] = computed
    row["digest_valid"] = stored is not None and str(stored) == computed
    row["bar_identity"] = bar_identity(row)
    return row


def _observation_evidence(row: dict[str, Any], *, effective: bool) -> dict[str, Any]:
    return {
        "observation_id": str(row["observation_id"]),
        "observation_identity": {
            "capture_id": str(row["ingestion_run_id"]),
            "scraped_at": row["scraped_at"].isoformat(),
            "source": row["source"],
            "digest": row["observation_digest"],
        },
        "bar_identity": row["bar_identity"],
        "provider_symbol": row["provider_symbol"],
        "source": row["source"],
        "market": row["market"],
        "currency": row["currency"],
        "interval": row["interval"],
        "candle_timestamp": row["candle_timestamp"].isoformat(),
        "bar_start": row["bar_start"].isoformat(),
        "bar_end": row["bar_end"].isoformat() if row.get("bar_end") else None,
        "available_at": row["available_at"].isoformat(),
        "scraped_at": row["scraped_at"].isoformat(),
        "is_closed": row["is_closed"],
        "effective_snapshot_input": effective,
        "digest_valid": row["digest_valid"],
        "quality": row["quality"],
        "missingness": row["missingness"],
    }


def _event_evidence(rows: list[dict[str, Any]]) -> dict[str, Any]:
    statuses = [str(row["status"]) for row in rows]
    owners = sorted({int(row["owner_chat_id"]) for row in rows})
    terminals = [status for status in statuses if status in TERMINAL_STATES]
    started = next((row for row in rows if row["status"] == "STARTED"), None)
    terminal = next((row for row in rows if row["status"] in TERMINAL_STATES), None)
    return {
        "events": [
            {
                "event_id": str(row["event_id"]),
                "status": row["status"],
                "occurred_at": row["occurred_at"].isoformat(),
                "created_at": row["created_at"].isoformat(),
                "owner": int(row["owner_chat_id"]),
                "reason_code": row["reason_code"],
                "code_version": row["code_version"],
            }
            for row in rows
        ],
        "transition": statuses,
        "started_then_complete": statuses == ["STARTED", "COMPLETE"],
        "terminal_count": len(terminals),
        "owner_stable": len(owners) == 1,
        "timestamps_coherent": bool(
            started and terminal
            and started["occurred_at"] <= terminal["occurred_at"]
            and started["created_at"] <= terminal["created_at"]
        ),
    }


def _productive_comparison(baseline: dict[str, Any], shadow: dict[str, Any]) -> dict[str, bool]:
    return {
        "scores": [item["final_score"] for item in baseline["signals"]]
                  == [item["final_score"] for item in shadow["signals"]],
        "signals": [item["decision"] for item in baseline["signals"]]
                   == [item["decision"] for item in shadow["signals"]],
        "decisions": baseline["decisions"] == shadow["decisions"],
        "orders": baseline["orders"] == shadow["orders"],
        "quantities": [
            order["quantity_est"]
            for values in baseline["orders"].values() for order in values
        ] == [
            order["quantity_est"]
            for values in shadow["orders"].values() for order in values
        ],
        "cash": baseline["cash"] == shadow["cash"],
    }


def _build_snapshot(
    rows_by_role: dict[str, list[dict[str, Any]]],
    *,
    cutoff: datetime,
    signal: str,
):
    asset_rows = rows_by_role["ASSET"]
    general_rows = rows_by_role["GENERAL_BENCHMARK"]
    general_ticker = str(general_rows[0]["ticker"]).upper()
    frames = {
        role: candles_to_frame(observation_rows_to_candles(rows))
        for role, rows in rows_by_role.items()
    }
    benchmarks = {general_ticker: frames["GENERAL_BENCHMARK"]}
    sector_ticker = None
    if rows_by_role.get("SECTOR_BENCHMARK"):
        sector_ticker = str(rows_by_role["SECTOR_BENCHMARK"][0]["ticker"]).upper()
        benchmarks[sector_ticker] = frames["SECTOR_BENCHMARK"]
    return build_contextual_snapshot(
        frames["ASSET"], cutoff=cutoff, signal_action=signal,
        benchmarks=benchmarks, general_benchmark=general_ticker,
        sector_benchmark=sector_ticker,
    )


async def _mutation_blocked(conn: Any, statement: str, *args: Any) -> bool:
    transaction = conn.transaction()
    await transaction.start()
    try:
        await conn.execute(statement, *args)
        blocked = False
    except Exception:
        blocked = True
    finally:
        await transaction.rollback()
    return blocked


async def audit_g3_closure(
    database_url: str,
    *,
    capture_id: str,
    run_id: str,
    plan_id: str,
    portfolio_snapshot_id: str,
    runner_evidence: dict[str, Any],
) -> dict[str, dict[str, Any]]:
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
                raise RuntimeError("G3_REAL_CONTEXTUAL_SNAPSHOT_NOT_FOUND")
            snapshot = dict(snapshot_raw)
            cutoff = snapshot["cutoff"]
            owner = int(snapshot["owner_chat_id"])
            stored_context = _json(snapshot["contextual_snapshot"])
            stored_input_hashes = _json(snapshot["input_hashes"])

            capture_events = [dict(row) for row in await conn.fetch(
                """SELECT * FROM market_evidence_capture_events
                   WHERE capture_id=$1 ORDER BY occurred_at,created_at,event_id""",
                UUID(capture_id),
            )]
            run_events = [dict(row) for row in await conn.fetch(
                """SELECT * FROM market_evidence_capture_events
                   WHERE capture_id=$1 ORDER BY occurred_at,created_at,event_id""",
                UUID(run_id),
            )]
            capture_lifecycle = _event_evidence(capture_events)
            run_lifecycle = _event_evidence(run_events)

            captured = [_prepare_row(row) for row in await conn.fetch(
                """SELECT * FROM market_candle_observations
                   WHERE ingestion_run_id=$1 AND owner_chat_id=$2
                   ORDER BY instrument_id,bar_start,observation_id""",
                UUID(capture_id), owner,
            )]
            links_raw = await conn.fetch(
                """SELECT links.input_role,links.ordinal,observations.*
                   FROM contextual_snapshot_candles links
                   JOIN market_candle_observations observations USING(observation_id)
                   WHERE links.snapshot_id=$1
                   ORDER BY links.input_role,links.ordinal""",
                snapshot["snapshot_id"],
            )
            links_by_role: dict[str, list[dict[str, Any]]] = {}
            for raw in links_raw:
                row = _prepare_row(raw)
                row["ordinal"] = int(raw["ordinal"])
                links_by_role.setdefault(str(raw["input_role"]), []).append(row)
            if "ASSET" not in links_by_role or "GENERAL_BENCHMARK" not in links_by_role:
                raise RuntimeError("G3_REAL_REQUIRED_INPUT_ROLE_MISSING")

            linked_ids = {
                str(row["observation_id"])
                for rows in links_by_role.values() for row in rows
            }
            excluded = [row for row in captured if str(row["observation_id"]) not in linked_ids]
            unrelated = [
                row for row in captured
                if not any(
                    row["instrument_id"] == candidate[0]["instrument_id"]
                    and row["provider_symbol"] == candidate[0]["provider_symbol"]
                    and row["source"] == candidate[0]["source"]
                    for candidate in links_by_role.values()
                )
            ]
            if unrelated:
                raise RuntimeError("G3_REAL_CAPTURE_HAS_UNMAPPED_SERIES")

            replay_1: dict[str, list[dict[str, Any]]] = {}
            replay_2: dict[str, list[dict[str, Any]]] = {}
            for role, linked_rows in links_by_role.items():
                first = linked_rows[0]
                kwargs = {
                    "owner_chat_id": owner,
                    "instrument_id_value": first["instrument_id"],
                    "market": first["market"],
                    "provider_symbol": first["provider_symbol"],
                    "interval": first["interval"],
                    "source": first["source"],
                    "cutoff": cutoff,
                }
                replay_1[role] = await read_market_evidence_as_of(conn, **kwargs)
                replay_2[role] = await read_market_evidence_as_of(conn, **kwargs)

            replay_snapshot_1 = _build_snapshot(
                replay_1, cutoff=cutoff, signal=str(snapshot["signal"])
            )
            replay_snapshot_2 = _build_snapshot(
                replay_2, cutoff=cutoff, signal=str(snapshot["signal"])
            )
            replay_payload_1 = replay_snapshot_1.to_dict()
            replay_payload_2 = replay_snapshot_2.to_dict()
            replay_ids_1 = {
                role: [str(row["observation_id"]) for row in rows]
                for role, rows in replay_1.items()
            }
            replay_ids_2 = {
                role: [str(row["observation_id"]) for row in rows]
                for role, rows in replay_2.items()
            }
            linked_ids_by_role = {
                role: [str(row["observation_id"]) for row in rows]
                for role, rows in links_by_role.items()
            }
            digests_by_role = {
                role: [str(row["observation_digest"]) for row in rows]
                for role, rows in links_by_role.items()
            }
            observation_inputs_hash = _hash(digests_by_role)
            context_hash = _hash(stored_context)

            temporal_violations = [
                str(row["observation_id"])
                for rows in replay_1.values() for row in rows
                if row["candle_timestamp"] > cutoff
                or row["bar_start"] > cutoff
                or row["bar_end"] is None
                or row["bar_end"] > cutoff
                or row["available_at"] > cutoff
                or row["scraped_at"] > cutoff
                or row["created_at"] > cutoff
                or row["is_closed"] is not True
            ]
            capture_complete = next(
                (row for row in capture_events if row["status"] == "COMPLETE"), None
            )
            capture_complete_known_at_cutoff = bool(
                capture_complete
                and capture_complete["occurred_at"] <= cutoff
                and capture_complete["created_at"] <= cutoff
            )
            roles_evidence: dict[str, Any] = {}
            for role, linked_rows in links_by_role.items():
                role_captured = [
                    row for row in captured
                    if row["instrument_id"] == linked_rows[0]["instrument_id"]
                    and row["provider_symbol"] == linked_rows[0]["provider_symbol"]
                    and row["source"] == linked_rows[0]["source"]
                ]
                role_linked_ids = {str(row["observation_id"]) for row in linked_rows}
                roles_evidence[role] = {
                    "ticker": linked_rows[0]["ticker"],
                    "captured_count": len(role_captured),
                    "effective_count": len(linked_rows),
                    "excluded_count": len(role_captured) - len(linked_rows),
                    "effective_observations": [
                        _observation_evidence(row, effective=True) for row in linked_rows
                    ],
                    "excluded_observations": [
                        _observation_evidence(row, effective=False)
                        for row in role_captured
                        if str(row["observation_id"]) not in role_linked_ids
                    ],
                    "ordinals_contiguous": [row["ordinal"] for row in linked_rows]
                    == list(range(len(linked_rows))),
                }

            baseline = _json(snapshot["productive_baseline"])
            shadow = _json(snapshot["productive_shadow"])
            productive_equal = _productive_comparison(baseline, shadow)
            stored_non_regression = _json(snapshot["non_regression"])
            context_confidence = stored_context["components"]["context_confidence"]

            capture_evidence = {
                "schema": "contextual-g3-real-capture-v1",
                "result": "PASS",
                "captured_at": runner_evidence["captured_at"],
                "capture_id": capture_id,
                "run_id": run_id,
                "plan_id": plan_id,
                "portfolio_snapshot_id": portfolio_snapshot_id,
                "owner": owner,
                "code_version": snapshot["code_version"],
                "cutoff": cutoff.isoformat(),
                "contextual_snapshot_id": snapshot["snapshot_id"],
                "market_lifecycle": capture_lifecycle,
                "analysis_lifecycle": run_lifecycle,
                "roles": roles_evidence,
                "counts": {
                    "captured": len(captured),
                    "effective": len(linked_ids),
                    "excluded": len(excluded),
                },
                "snapshot": {
                    "schema_version": stored_context["schema_version"],
                    "definition_version": stored_context["definition_version"],
                    "mode": snapshot["mode"],
                    "affects_analysis": snapshot["affects_analysis"],
                    "affects_execution": snapshot["affects_execution"],
                    "context_confidence": context_confidence,
                    "invalidators": stored_context["invalidators"],
                    "input_observation_ids": linked_ids_by_role,
                    "input_observation_digests": digests_by_role,
                    "observation_inputs_hash": observation_inputs_hash,
                    "context_input_digests": stored_input_hashes,
                    "contextual_snapshot_hash": context_hash,
                },
                "unknown_preserved": {
                    "sector_benchmark": "UNKNOWN" if "SECTOR_BENCHMARK" not in links_by_role else "CAPTURED",
                    "depositary_ratio": "UNKNOWN",
                    "volume_unit": "UNKNOWN",
                    "byma_versioned_calendar": "UNKNOWN",
                },
                "execution_safety": runner_evidence["execution_safety"],
            }
            replay_evidence = {
                "schema": "contextual-g3-real-replay-v1",
                "result": "PASS",
                "capture_id": capture_id,
                "run_id": run_id,
                "cutoff": cutoff.isoformat(),
                "linked_observation_ids": linked_ids_by_role,
                "replayed_observation_ids": replay_ids_1,
                "same_observation_ids": replay_ids_1 == linked_ids_by_role,
                "same_observation_digests": all(
                    [row["computed_observation_digest"] for row in replay_1[role]]
                    == digests_by_role[role]
                    for role in links_by_role
                ),
                "same_context_input_digests": replay_snapshot_1.input_digests == stored_input_hashes,
                "same_contextual_payload": replay_payload_1 == stored_context,
                "same_snapshot_id": replay_snapshot_1.snapshot_id == snapshot["snapshot_id"],
                "same_snapshot_hash": _hash(replay_payload_1) == context_hash,
                "observation_inputs_hash": observation_inputs_hash,
                "contextual_snapshot_hash": context_hash,
                "temporal_violations": temporal_violations,
                "no_lookahead": not temporal_violations,
                "open_bar_used": any(
                    row["is_closed"] is not True or row["bar_end"] is None
                    for rows in replay_1.values() for row in rows
                ),
                "incomplete_capture_promoted": not capture_complete_known_at_cutoff,
                "excluded_observation_ids": [str(row["observation_id"]) for row in excluded],
            }
            determinism_evidence = {
                "schema": "contextual-g3-real-determinism-v1",
                "result": "PASS",
                "capture_id": capture_id,
                "run_id": run_id,
                "replay_1_observation_ids": replay_ids_1,
                "replay_2_observation_ids": replay_ids_2,
                "replay_1_snapshot_id": replay_snapshot_1.snapshot_id,
                "replay_2_snapshot_id": replay_snapshot_2.snapshot_id,
                "replay_1_snapshot_hash": _hash(replay_payload_1),
                "replay_2_snapshot_hash": _hash(replay_payload_2),
                "replay_1_equals_replay_2": (
                    replay_ids_1 == replay_ids_2
                    and replay_payload_1 == replay_payload_2
                ),
                "snapshot_hashes_equal": _hash(replay_payload_1) == _hash(replay_payload_2),
                "original_equals_replay": stored_context == replay_payload_1,
            }
            non_regression_evidence = {
                "schema": "contextual-g3-real-non-regression-v1",
                "result": "PASS",
                "capture_id": capture_id,
                "run_id": run_id,
                "equal": productive_equal,
                "stored_non_regression": stored_non_regression,
                "baseline_productive_hash": _hash(baseline),
                "shadow_productive_hash": _hash(shadow),
                "shadow_contract": {
                    "mode": snapshot["mode"],
                    "affects_analysis": snapshot["affects_analysis"],
                    "affects_execution": snapshot["affects_execution"],
                },
                "broker_order_api_calls": runner_evidence["execution_safety"]["broker_order_api_calls"],
                "broker_orders_executed": runner_evidence["execution_safety"]["broker_orders_executed"],
                "telegram_sent": runner_evidence["execution_safety"]["telegram_sent"],
            }
    finally:
        await conn.close()

    mutation_conn = await asyncpg.connect(direct_url)
    try:
        before_count = await mutation_conn.fetchval(
            "SELECT COUNT(*) FROM market_candle_observations WHERE ingestion_run_id=$1",
            UUID(capture_id),
        )
        sample_id = UUID(next(iter(linked_ids)))
        protections = {
            "update_observation": await _mutation_blocked(
                mutation_conn,
                "UPDATE market_candle_observations SET close_price=1 WHERE observation_id=$1",
                sample_id,
            ),
            "delete_observation": await _mutation_blocked(
                mutation_conn,
                "DELETE FROM market_candle_observations WHERE observation_id=$1",
                sample_id,
            ),
            "delete_snapshot_link": await _mutation_blocked(
                mutation_conn,
                "DELETE FROM contextual_snapshot_candles WHERE snapshot_id=$1",
                snapshot["snapshot_id"],
            ),
            "update_capture_event": await _mutation_blocked(
                mutation_conn,
                "UPDATE market_evidence_capture_events SET status='FAILED' WHERE capture_id=$1",
                UUID(capture_id),
            ),
            "truncate_observations": await _mutation_blocked(
                mutation_conn, "TRUNCATE market_candle_observations"
            ),
        }
        after_count = await mutation_conn.fetchval(
            "SELECT COUNT(*) FROM market_candle_observations WHERE ingestion_run_id=$1",
            UUID(capture_id),
        )
    finally:
        await mutation_conn.close()

    capture_evidence["immutability"] = {
        "transactional_rollback_probe": True,
        "protections": protections,
        "row_count_before": int(before_count),
        "row_count_after": int(after_count),
        "evidence_preserved": before_count == after_count,
        "all_digests_valid": all(row["digest_valid"] for row in captured),
    }

    required = [
        capture_lifecycle["started_then_complete"],
        capture_lifecycle["terminal_count"] == 1,
        capture_lifecycle["owner_stable"],
        capture_lifecycle["timestamps_coherent"],
        run_lifecycle["started_then_complete"],
        run_lifecycle["terminal_count"] == 1,
        capture_complete_known_at_cutoff,
        len(captured) == len(linked_ids) + len(excluded),
        all(row["is_closed"] is not True or row["bar_end"] is None for row in excluded),
        replay_evidence["same_observation_ids"],
        replay_evidence["same_observation_digests"],
        replay_evidence["same_context_input_digests"],
        replay_evidence["same_contextual_payload"],
        replay_evidence["same_snapshot_hash"],
        replay_evidence["no_lookahead"],
        not replay_evidence["open_bar_used"],
        not replay_evidence["incomplete_capture_promoted"],
        determinism_evidence["replay_1_equals_replay_2"],
        determinism_evidence["snapshot_hashes_equal"],
        determinism_evidence["original_equals_replay"],
        all(protections.values()),
        before_count == after_count,
        all(row["digest_valid"] for row in captured),
        all(productive_equal.values()),
        snapshot["mode"] == "SHADOW_ONLY",
        snapshot["affects_analysis"] is False,
        snapshot["affects_execution"] is False,
        runner_evidence["execution_safety"]["broker_orders_executed"] == 0,
    ]
    if not all(required):
        capture_evidence["result"] = "FAIL"
        replay_evidence["result"] = "FAIL"
        determinism_evidence["result"] = "FAIL"
        non_regression_evidence["result"] = "FAIL"
        raise RuntimeError("G3_REAL_CLOSURE_ASSERTION_FAILED")

    return {
        "capture": capture_evidence,
        "replay": replay_evidence,
        "determinism": determinism_evidence,
        "non_regression": non_regression_evidence,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--capture-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--plan-id", required=True)
    parser.add_argument("--portfolio-snapshot-id", required=True)
    parser.add_argument("--runner-evidence", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("docs/evidence"))
    args = parser.parse_args()
    runner_evidence = json.loads(args.runner_evidence.read_text(encoding="utf-8"))
    result = asyncio.run(audit_g3_closure(
        args.database_url,
        capture_id=args.capture_id,
        run_id=args.run_id,
        plan_id=args.plan_id,
        portfolio_snapshot_id=args.portfolio_snapshot_id,
        runner_evidence=runner_evidence,
    ))
    names = {
        "capture": "contextual-g3-real-capture.json",
        "replay": "contextual-g3-real-replay.json",
        "determinism": "contextual-g3-real-determinism.json",
        "non_regression": "contextual-g3-real-non-regression.json",
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    for key, name in names.items():
        (args.output_dir / name).write_text(
            json.dumps(result[key], indent=2, ensure_ascii=False, allow_nan=False, default=str) + "\n",
            encoding="utf-8",
        )
    print(json.dumps({
        "result": "PASS",
        "capture_id": args.capture_id,
        "run_id": args.run_id,
        "outputs": sorted(names.values()),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

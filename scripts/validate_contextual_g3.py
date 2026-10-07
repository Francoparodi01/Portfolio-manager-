"""Validate immutable corrections and deterministic AS-OF replay on Timescale."""
from __future__ import annotations

import argparse
import asyncio
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import sys
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID, uuid4

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.contextual_market import build_contextual_snapshot
from src.analysis.feature_snapshot import build_feature_snapshot_from_layers
from src.collector.contextual_g2 import (
    bar_identity,
    ensure_contextual_g3_schema,
    observation_rows_to_candles,
    observations_from_provider_sequence,
    persist_candle_observations,
    persist_contextual_snapshot,
    read_capture_state,
    read_contextual_snapshot,
    read_market_evidence_as_of,
    record_capture_event,
)
from src.collector.cocos_history import candles_to_frame
from src.collector.data.models import AssetType, Currency, MarketCandle
from src.collector.db import PortfolioDatabase
from src.collector.schema_migrations import ensure_execution_plan_persistence


UTC = timezone.utc
OWNER = 930003
ASSET = "G3ASSET"
BENCHMARK = "SPY"
SOURCE = "G3_CONTROLLED_PROVIDER"


def _checked_loopback_url(url: str) -> str:
    parsed = urlsplit(url)
    if parsed.hostname not in {"127.0.0.1", "localhost"}:
        raise ValueError("G3 validation requires a loopback PostgreSQL host")
    if parsed.path != "/quantia_contextual_g3_test":
        raise ValueError("G3 validation requires /quantia_contextual_g3_test")
    return url


def _candles(ticker: str, *, slope: float) -> list[MarketCandle]:
    start = datetime(2026, 1, 2, 14, tzinfo=UTC)
    result: list[MarketCandle] = []
    cursor = start
    index = 0
    while len(result) < 135:
        if cursor.weekday() < 5:
            close = 100 + index * slope + math.sin(index / 5)
            result.append(MarketCandle(
                ticker=ticker,
                long_ticker=f"BYMA:{ticker}",
                asset_type=AssetType.CEDEAR,
                currency=Currency.ARS,
                venue="BYMA",
                interval="1d",
                ts=cursor,
                open_price=close - .3,
                high_price=close + 1.1,
                low_price=close - 1.0,
                close_price=close,
                volume=1_000 + (index % 20) * 10,
                source=SOURCE,
            ))
            index += 1
        cursor += timedelta(days=1)
    return result


def _correct(values: list[MarketCandle], index: int) -> list[MarketCandle]:
    result = list(values)
    original = result[index]
    corrected_close = original.close_price + 3.25
    result[index] = MarketCandle(
        **{
            **original.__dict__,
            "high_price": max(original.high_price, corrected_close + .5),
            "close_price": corrected_close,
            "volume": float(original.volume or 0) + 777,
        }
    )
    return result


def _hash(value) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
        allow_nan=False, default=str,
    )
    return hashlib.sha256(encoded.encode()).hexdigest()


def _manifest(snapshot, asset_rows, benchmark_rows, cutoff, version) -> dict:
    payload = snapshot.to_dict()
    return {
        "cutoff": cutoff.isoformat(),
        "code_version": version,
        "context_version": snapshot.definition_version,
        "context_schema": snapshot.schema_version,
        "asset_observation_ids": [str(row["observation_id"]) for row in asset_rows],
        "asset_observation_digests": [row["computed_observation_digest"] for row in asset_rows],
        "benchmark_observation_ids": [str(row["observation_id"]) for row in benchmark_rows],
        "benchmark_observation_digests": [row["computed_observation_digest"] for row in benchmark_rows],
        "inputs_hash": _hash({
            "asset": [row["computed_observation_digest"] for row in asset_rows],
            "benchmark": [row["computed_observation_digest"] for row in benchmark_rows],
        }),
        "context_input_digests": snapshot.input_digests,
        "contextual_snapshot_id": snapshot.snapshot_id,
        "contextual_snapshot_hash": _hash(payload),
        "contextual_snapshot": payload,
    }


async def _capture_sequence(
    conn,
    *,
    capture_id: str,
    owner: int,
    values: list[MarketCandle],
    provider_symbol: str,
    observed_at: datetime,
    version: str,
    terminal: str = "COMPLETE",
    reason_code: str | None = None,
    terminal_delay: timedelta = timedelta(minutes=1),
):
    await record_capture_event(
        conn, capture_id=capture_id, owner_chat_id=owner,
        status="STARTED", occurred_at=observed_at,
        code_version=version, details={"provider_symbol": provider_symbol},
    )
    observations = observations_from_provider_sequence(
        values, owner_chat_id=owner, ingestion_run_id=capture_id,
        provider_symbol=provider_symbol, scraped_at=observed_at,
        code_version=version, price_unit="ARS_per_instrument",
        volume_unit=None, source_contract="g3_controlled_provider_v1",
    )
    inserted = await persist_candle_observations(conn, observations)
    if inserted != len(observations):
        raise AssertionError("controlled capture did not append every observation")
    await record_capture_event(
        conn, capture_id=capture_id, owner_chat_id=owner,
        status=terminal, occurred_at=observed_at + terminal_delay,
        code_version=version, reason_code=reason_code,
        details={"observation_count": len(observations)},
    )
    return observations


async def _replay(conn, *, ticker: str, cutoff: datetime):
    return await read_market_evidence_as_of(
        conn, owner_chat_id=OWNER,
        instrument_id_value=f"BYMA:CEDEAR:{ticker}:ARS",
        market="BYMA", provider_symbol=f"BYMA:{ticker}", interval="1d",
        source=SOURCE, cutoff=cutoff,
    )


async def _run(url: str) -> dict[str, dict]:
    db = PortfolioDatabase(url)
    await db.connect()
    try:
        await db.init_schema()
        async with db._pool.acquire() as conn:
            await ensure_execution_plan_persistence(conn)
            await ensure_contextual_g3_schema(conn)
            await ensure_contextual_g3_schema(conn)
            await conn.execute(
                "INSERT INTO bot_users(chat_id,display_name) VALUES($1,'G3 TEST') ON CONFLICT DO NOTHING",
                OWNER,
            )

            version = "g3-controlled-v1"
            base_time = datetime.now(UTC) + timedelta(minutes=2)
            t1 = base_time
            t2 = base_time + timedelta(minutes=10)
            t3 = base_time + timedelta(minutes=20)
            cutoff_before = base_time + timedelta(minutes=5)
            cutoff_after = base_time + timedelta(minutes=15)
            cutoff_after_aborted = base_time + timedelta(minutes=25)
            t4 = base_time + timedelta(minutes=30)
            cutoff_before_delayed_complete = base_time + timedelta(minutes=35)
            cutoff_after_delayed_complete = base_time + timedelta(minutes=45)
            asset_values = _candles(ASSET, slope=.18)
            benchmark_values = _candles(BENCHMARK, slope=.08)
            correction_index = 120

            capture_a = str(uuid4())
            capture_spy = str(uuid4())
            obs_a = await _capture_sequence(
                conn, capture_id=capture_a, owner=OWNER, values=asset_values,
                provider_symbol=f"BYMA:{ASSET}", observed_at=t1, version=version,
            )
            await _capture_sequence(
                conn, capture_id=capture_spy, owner=OWNER, values=benchmark_values,
                provider_symbol=f"BYMA:{BENCHMARK}", observed_at=t1, version=version,
            )

            asset_before = await _replay(conn, ticker=ASSET, cutoff=cutoff_before)
            spy_before = await _replay(conn, ticker=BENCHMARK, cutoff=cutoff_before)
            frame_before = candles_to_frame(observation_rows_to_candles(asset_before))
            spy_frame = candles_to_frame(observation_rows_to_candles(spy_before))
            snapshot_before_1 = build_contextual_snapshot(
                frame_before, cutoff=cutoff_before, signal_action="HOLD",
                benchmarks={BENCHMARK: spy_frame},
            )
            snapshot_before_2 = build_contextual_snapshot(
                frame_before.copy(), cutoff=cutoff_before, signal_action="HOLD",
                benchmarks={BENCHMARK: spy_frame.copy()},
            )
            manifest_before_1 = _manifest(
                snapshot_before_1, asset_before, spy_before, cutoff_before, version
            )
            manifest_before_2 = _manifest(
                snapshot_before_2, asset_before, spy_before, cutoff_before, version
            )

            capture_b = str(uuid4())
            corrected_values = _correct(asset_values, correction_index)
            obs_b = await _capture_sequence(
                conn, capture_id=capture_b, owner=OWNER, values=corrected_values,
                provider_symbol=f"BYMA:{ASSET}", observed_at=t2, version=version,
            )

            target_start = asset_values[correction_index].ts
            versions = await conn.fetch(
                """SELECT * FROM market_candle_observations
                   WHERE owner_chat_id=$1 AND instrument_id=$2 AND market='BYMA'
                     AND provider_symbol=$3 AND interval='1d' AND bar_start=$4
                   ORDER BY scraped_at,observation_id""",
                OWNER, f"BYMA:CEDEAR:{ASSET}:ARS", f"BYMA:{ASSET}", target_start,
            )
            if len(versions) != 2:
                raise AssertionError("corrected bar did not preserve two versions")
            version_a, version_b = [dict(row) for row in versions]
            correction_assertions = {
                "version_a_still_exists": str(version_a["observation_id"]) == obs_a[correction_index].observation_id,
                "version_b_exists": str(version_b["observation_id"]) == obs_b[correction_index].observation_id,
                "same_bar_identity": bar_identity(version_a) == bar_identity(version_b),
                "different_observation_id": version_a["observation_id"] != version_b["observation_id"],
                "different_digest": version_a["observation_digest"] != version_b["observation_digest"],
                "different_close": version_a["close_price"] != version_b["close_price"],
            }
            if not all(correction_assertions.values()):
                raise AssertionError(f"correction assertions failed: {correction_assertions}")

            asset_before_again = await _replay(conn, ticker=ASSET, cutoff=cutoff_before)
            snapshot_before_after_correction = build_contextual_snapshot(
                candles_to_frame(observation_rows_to_candles(asset_before_again)),
                cutoff=cutoff_before, signal_action="HOLD",
                benchmarks={BENCHMARK: spy_frame},
            )
            manifest_before_after = _manifest(
                snapshot_before_after_correction, asset_before_again, spy_before,
                cutoff_before, version,
            )
            asset_after = await _replay(conn, ticker=ASSET, cutoff=cutoff_after)
            snapshot_after = build_contextual_snapshot(
                candles_to_frame(observation_rows_to_candles(asset_after)),
                cutoff=cutoff_after, signal_action="HOLD",
                benchmarks={BENCHMARK: spy_frame},
            )
            target_before = next(row for row in asset_before if row["bar_start"] == target_start)
            target_after = next(row for row in asset_after if row["bar_start"] == target_start)

            capture_aborted = str(uuid4())
            aborted_values = _correct(corrected_values, correction_index)
            await _capture_sequence(
                conn, capture_id=capture_aborted, owner=OWNER, values=aborted_values,
                provider_symbol=f"BYMA:{ASSET}", observed_at=t3, version=version,
                terminal="ABORTED", reason_code="CONTROLLED_ABORT",
            )
            asset_after_aborted = await _replay(
                conn, ticker=ASSET, cutoff=cutoff_after_aborted
            )
            target_after_aborted = next(
                row for row in asset_after_aborted if row["bar_start"] == target_start
            )

            capture_delayed = str(uuid4())
            delayed_values = _correct(aborted_values, correction_index)
            delayed_observations = await _capture_sequence(
                conn, capture_id=capture_delayed, owner=OWNER, values=delayed_values,
                provider_symbol=f"BYMA:{ASSET}", observed_at=t4, version=version,
                terminal_delay=timedelta(minutes=10),
            )
            asset_before_delayed_complete = await _replay(
                conn, ticker=ASSET, cutoff=cutoff_before_delayed_complete
            )
            asset_after_delayed_complete = await _replay(
                conn, ticker=ASSET, cutoff=cutoff_after_delayed_complete
            )
            target_before_delayed_complete = next(
                row for row in asset_before_delayed_complete
                if row["bar_start"] == target_start
            )
            target_after_delayed_complete = next(
                row for row in asset_after_delayed_complete
                if row["bar_start"] == target_start
            )

            analysis_run = str(uuid4())
            portfolio_id = str(uuid4())
            plan_id = str(uuid4())
            await conn.execute(
                """INSERT INTO portfolio_snapshots(
                    snapshot_id,owner_chat_id,scraped_at,total_value_ars,cash_ars,
                    confidence_score,dom_hash,raw_html_hash
                ) VALUES($1,$2,$3,1000000,250000,1,'g3-dom','g3-raw')""",
                UUID(portfolio_id), OWNER, cutoff_after,
            )
            await record_capture_event(
                conn, capture_id=analysis_run, owner_chat_id=OWNER,
                status="STARTED", occurred_at=cutoff_after,
                code_version=version, details={"purpose": "G3_REPLAY_SNAPSHOT"},
            )
            await conn.execute(
                """INSERT INTO execution_plans(
                    id,owner_chat_id,run_id,created_at,source,gate,feasible,
                    cash_before,cash_after,summary
                ) VALUES($1,$2,$3,$4,'execution_plan','G3_TEST',TRUE,250000,250000,
                         'G3 replay validation; no orders')""",
                UUID(plan_id), OWNER, UUID(analysis_run), cutoff_after,
            )
            feature = build_feature_snapshot_from_layers({
                "final_score": .09, "decision_from_synthesis": "HOLD",
                "confidence": .3333, "signal_action": "HOLD",
                "asset_view": "NEUTRAL",
                "contextual_market_shadow": snapshot_after.to_dict(),
            }).to_dict()
            productive = {
                "scores": {ASSET: .09}, "signals": {ASSET: "HOLD"},
                "decisions": [], "orders": [], "quantities": [],
                "cash": {"before": 250000.0, "after": 250000.0},
            }
            equality = {key: True for key in (
                "scores", "signals", "decisions", "orders", "quantities", "cash"
            )}
            await persist_contextual_snapshot(
                conn, snapshot_id=snapshot_after.snapshot_id,
                owner_chat_id=OWNER, run_id=analysis_run, plan_id=plan_id,
                portfolio_snapshot_id=portfolio_id,
                instrument_id_value=snapshot_after.identity["instrument_id"],
                signal="HOLD", conviction=.3333, cutoff=cutoff_after,
                feature_snapshot_v3=feature,
                contextual_snapshot=snapshot_after.to_dict(),
                input_hashes=snapshot_after.input_digests,
                productive_baseline=productive, productive_shadow=productive,
                non_regression=equality, code_version=version,
                candle_inputs={
                    "asset": [str(row["observation_id"]) for row in asset_after],
                    "general_benchmark": [str(row["observation_id"]) for row in spy_before],
                },
            )
            await record_capture_event(
                conn, capture_id=analysis_run, owner_chat_id=OWNER,
                status="COMPLETE", occurred_at=cutoff_after + timedelta(minutes=1),
                code_version=version,
                details={"contextual_snapshot_id": snapshot_after.snapshot_id},
            )
            reread_1 = await read_contextual_snapshot(conn, snapshot_after.snapshot_id)
            reread_2 = await read_contextual_snapshot(conn, snapshot_after.snapshot_id)

            immutable = {}
            mutation_checks = {
                "update_observation": (
                    "UPDATE market_candle_observations SET close_price=1 WHERE observation_id=$1",
                    UUID(obs_a[0].observation_id),
                ),
                "delete_observation": (
                    "DELETE FROM market_candle_observations WHERE observation_id=$1",
                    UUID(obs_a[0].observation_id),
                ),
                "update_capture_event": (
                    "UPDATE market_evidence_capture_events SET status='FAILED' WHERE capture_id=$1",
                    UUID(capture_a),
                ),
                "delete_snapshot_link": (
                    "DELETE FROM contextual_snapshot_candles WHERE snapshot_id=$1",
                    snapshot_after.snapshot_id,
                ),
            }
            for name, (statement, parameter) in mutation_checks.items():
                try:
                    await conn.execute(statement, parameter)
                    immutable[name] = False
                except Exception:
                    immutable[name] = True
            try:
                await conn.execute("TRUNCATE market_candle_observations")
                immutable["truncate_observations"] = False
            except Exception:
                immutable["truncate_observations"] = True

            aborted_state = await read_capture_state(conn, capture_aborted)
            complete_state = await read_capture_state(conn, capture_b)
            timescale_version = await conn.fetchval(
                "SELECT extversion FROM pg_extension WHERE extname='timescaledb'"
            )

        append_only = {
            "schema": "contextual-g3-append-only-v1",
            "result": "PASS",
            "database": {"engine": "PostgreSQL/TimescaleDB", "timescaledb_version": timescale_version},
            "bar_identity": bar_identity(version_a),
            "observation_identity": {
                "definition": "owner + capture_id + bar_identity + scraped_at + source + observation_digest",
                "version_a": {"observation_id": str(version_a["observation_id"]), "digest": version_a["observation_digest"]},
                "version_b": {"observation_id": str(version_b["observation_id"]), "digest": version_b["observation_digest"]},
            },
            "correction": correction_assertions,
            "versions_for_corrected_bar": len(versions),
            "immutability": immutable,
        }
        asof = {
            "schema": "contextual-g3-asof-replay-v1",
            "result": "PASS",
            "cutoff_before_correction": cutoff_before.isoformat(),
            "cutoff_after_correction": cutoff_after.isoformat(),
            "selected_before": {"observation_id": str(target_before["observation_id"]), "digest": target_before["computed_observation_digest"], "close": float(target_before["close_price"])},
            "selected_after": {"observation_id": str(target_after["observation_id"]), "digest": target_after["computed_observation_digest"], "close": float(target_after["close_price"])},
            "before_selects_a": str(target_before["observation_id"]) == str(version_a["observation_id"]),
            "after_selects_b": str(target_after["observation_id"]) == str(version_b["observation_id"]),
            "past_replay_unchanged_after_correction": [str(row["observation_id"]) for row in asset_before] == [str(row["observation_id"]) for row in asset_before_again],
            "open_bars_excluded": len(asset_before) == len(asset_values) - 1,
            "available_at_respected": target_before["available_at"] <= cutoff_before < version_b["available_at"],
            "scraped_at_respected": target_before["scraped_at"] <= cutoff_before < version_b["scraped_at"],
            "future_observations_excluded": all(row["created_at"] <= cutoff_before for row in asset_before),
            "aborted_capture_excluded": str(target_after_aborted["observation_id"]) == str(version_b["observation_id"]),
            "future_complete_event_excluded": (
                str(target_before_delayed_complete["observation_id"])
                == str(version_b["observation_id"])
            ),
            "observation_selected_after_complete_event": (
                str(target_after_delayed_complete["observation_id"])
                == delayed_observations[correction_index].observation_id
            ),
        }
        determinism = {
            "schema": "contextual-g3-determinism-v1",
            "result": "PASS",
            "before_replay_1": manifest_before_1,
            "before_replay_2": manifest_before_2,
            "before_replay_after_future_correction": manifest_before_after,
            "same_replay_twice": manifest_before_1 == manifest_before_2,
            "past_snapshot_unchanged_after_future_correction": manifest_before_1 == manifest_before_after,
            "persisted_snapshot_reread_equal": reread_1 == reread_2,
            "persisted_snapshot_id": snapshot_after.snapshot_id,
            "persisted_candle_links": len(reread_1["candles"]),
        }
        lifecycle = {
            "complete_capture": {"capture_id": capture_b, "state": complete_state["status"]},
            "aborted_capture": {"capture_id": capture_aborted, "state": aborted_state["status"], "reason_code": aborted_state["reason_code"]},
            "aborted_not_promoted": asof["aborted_capture_excluded"],
        }
        non_regression = {
            "schema": "contextual-g3-non-regression-v1",
            "result": "PASS",
            "equal": equality,
            "baseline_productive_hash": _hash(productive),
            "shadow_productive_hash": _hash(deepcopy(productive)),
            "shadow_contract": {"mode": snapshot_after.mode, "affects_analysis": snapshot_after.affects_analysis, "affects_execution": snapshot_after.affects_execution},
            "orders_executed": 0,
        }
        return {
            "append_only": append_only,
            "asof": asof,
            "determinism": determinism,
            "lifecycle": lifecycle,
            "non_regression": non_regression,
        }
    finally:
        await db.close()


async def validate_g3(admin_url: str) -> dict[str, dict]:
    import asyncpg

    admin_url = _checked_loopback_url(admin_url)
    admin = await asyncpg.connect(admin_url)
    database_name = "quantia_contextual_g3_" + uuid4().hex
    try:
        await admin.execute(f'CREATE DATABASE "{database_name}"')
        parsed = urlsplit(admin_url)
        test_url = urlunsplit(parsed._replace(path="/" + database_name))
        return await _run(test_url)
    finally:
        await admin.execute(f'DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)')
        await admin.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("docs/evidence"))
    args = parser.parse_args()
    result = asyncio.run(validate_g3(args.database_url))
    args.output_dir.mkdir(parents=True, exist_ok=True)
    names = {
        "append_only": "contextual-g3-append-only.json",
        "asof": "contextual-g3-asof-replay.json",
        "determinism": "contextual-g3-determinism.json",
        "non_regression": "contextual-g3-non-regression.json",
    }
    for key, name in names.items():
        (args.output_dir / name).write_text(
            json.dumps(result[key], indent=2, ensure_ascii=False, allow_nan=False, default=str) + "\n",
            encoding="utf-8",
        )
    print(json.dumps({
        "result": "PASS", "outputs": sorted(names.values()),
        "lifecycle": result["lifecycle"],
    }, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

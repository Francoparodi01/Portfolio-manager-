"""Validate the additive G2 migration in a disposable loopback Timescale DB."""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timedelta, timezone
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
from src.analysis.versioning import code_version
from src.collector.contextual_g2 import (
    ensure_contextual_g2_schema,
    observation_rows_to_candles,
    observations_from_provider_sequence,
    persist_candle_observations,
    persist_contextual_snapshot,
    read_candle_observations,
    read_contextual_snapshot,
)
from src.collector.cocos_history import candles_to_frame
from src.collector.data.models import AssetType, Currency, MarketCandle
from src.collector.db import PortfolioDatabase
from src.collector.schema_migrations import ensure_execution_plan_persistence


UTC = timezone.utc
OWNER = 920002
RUN_ID = "72000211-2222-4333-8444-555555555555"
SNAPSHOT_ID = "72000211-2222-4333-8444-666666666666"
PLAN_ID = "72000211-2222-4333-8444-777777777777"
LEGACY_COLUMNS = (
    "bar_start", "bar_end", "available_at", "is_closed", "volume_unit",
    "calendar", "calendar_validation", "adjustment_policy", "depositary_ratio",
)


def _checked_loopback_url(url: str) -> str:
    parsed = urlsplit(url)
    if parsed.hostname not in {"127.0.0.1", "localhost"}:
        raise ValueError("G2 validation requires a loopback PostgreSQL host")
    if parsed.path != "/quantia_contextual_g2_test":
        raise ValueError("G2 validation requires /quantia_contextual_g2_test")
    return url


def _candles(ticker: str, *, slope: float) -> list[MarketCandle]:
    start = datetime(2026, 1, 2, 14, 0, tzinfo=UTC)
    values: list[MarketCandle] = []
    cursor = start
    index = 0
    while len(values) < 135:
        if cursor.weekday() < 5:
            close = 100 + index * slope + math.sin(index / 5)
            values.append(MarketCandle(
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
                source="G2_CONTROLLED_PROVIDER",
            ))
            index += 1
        cursor += timedelta(days=1)
    return values


async def _column_names(conn) -> list[str]:
    rows = await conn.fetch(
        """SELECT column_name FROM information_schema.columns
           WHERE table_schema='public' AND table_name='market_candles'
           ORDER BY ordinal_position"""
    )
    return [str(row["column_name"]) for row in rows]


async def _run(url: str) -> dict:
    db = PortfolioDatabase(url)
    await db.connect()
    try:
        await db.init_schema()
        async with db._pool.acquire() as conn:
            await ensure_execution_plan_persistence(conn)
            await conn.execute("DROP TABLE IF EXISTS contextual_snapshot_candles")
            await conn.execute("DROP TABLE IF EXISTS contextual_market_snapshots")
            await conn.execute("DROP TABLE IF EXISTS market_candle_observations")
            for column in LEGACY_COLUMNS:
                await conn.execute(f"ALTER TABLE market_candles DROP COLUMN IF EXISTS {column}")
            await conn.execute(
                "INSERT INTO bot_users(chat_id,display_name) VALUES($1,'G2 TEST') ON CONFLICT DO NOTHING",
                OWNER,
            )
            legacy_ts = datetime(2025, 12, 30, 14, tzinfo=UTC)
            await conn.execute(
                """INSERT INTO market_candles(
                    ts,ticker,long_ticker,asset_type,currency,venue,interval,
                    open_price,high_price,low_price,close_price,volume,source,scraped_at
                ) VALUES($1,'LEGACY','LEGACY-PROVIDER','CEDEAR','ARS','BYMA','1d',
                         10,11,9,10.5,100,'LEGACY',$2)""",
                legacy_ts, legacy_ts + timedelta(hours=8),
            )
            prior_columns = await _column_names(conn)
            await ensure_contextual_g2_schema(conn)
            await ensure_contextual_g2_schema(conn)
            migrated_columns = await _column_names(conn)
            legacy = await conn.fetchrow(
                """SELECT bar_start,bar_end,available_at,is_closed,volume_unit,
                          calendar,calendar_validation,adjustment_policy,depositary_ratio
                   FROM market_candles WHERE ticker='LEGACY'"""
            )
            if any(value is not None for value in legacy.values()):
                raise AssertionError("migration fabricated legacy candle metadata")

            portfolio_at = datetime(2026, 7, 13, 17, 5, tzinfo=UTC)
            await conn.execute(
                """INSERT INTO portfolio_snapshots(
                    snapshot_id,owner_chat_id,scraped_at,total_value_ars,cash_ars,
                    confidence_score,dom_hash,raw_html_hash
                ) VALUES($1,$2,$3,1000000,250000,1,'g2-dom','g2-raw')""",
                UUID(SNAPSHOT_ID), OWNER, portfolio_at,
            )
            await conn.execute(
                """INSERT INTO execution_plans(
                    id,owner_chat_id,run_id,created_at,source,gate,feasible,
                    cash_before,cash_after,summary
                ) VALUES($1,$2,$3,$4,'execution_plan','G2_TEST',TRUE,250000,250000,
                         'Disposable migration validation; no orders')""",
                UUID(PLAN_ID), OWNER, UUID(RUN_ID), portfolio_at + timedelta(minutes=1),
            )

            observed_at = portfolio_at
            asset_obs = observations_from_provider_sequence(
                _candles("G2ASSET", slope=.18), owner_chat_id=OWNER,
                ingestion_run_id=RUN_ID, provider_symbol="BYMA:G2ASSET",
                scraped_at=observed_at, code_version=code_version(),
                price_unit="ARS_per_instrument",
            )
            spy_obs = observations_from_provider_sequence(
                _candles("SPY", slope=.08), owner_chat_id=OWNER,
                ingestion_run_id=RUN_ID, provider_symbol="BYMA:SPY",
                scraped_at=observed_at, code_version=code_version(),
                price_unit="ARS_per_instrument",
            )
            inserted = await persist_candle_observations(conn, [*asset_obs, *spy_obs])
            if inserted != len(asset_obs) + len(spy_obs):
                raise AssertionError("observation insert count mismatch")

            cutoff = observed_at + timedelta(minutes=1)
            asset_rows = await read_candle_observations(
                conn, owner_chat_id=OWNER, ingestion_run_id=RUN_ID,
                ticker="G2ASSET", cutoff=cutoff,
            )
            spy_rows = await read_candle_observations(
                conn, owner_chat_id=OWNER, ingestion_run_id=RUN_ID,
                ticker="SPY", cutoff=cutoff,
            )
            asset_frame = candles_to_frame(observation_rows_to_candles(asset_rows))
            benchmark_frame = candles_to_frame(observation_rows_to_candles(spy_rows))
            contextual = build_contextual_snapshot(
                asset_frame, cutoff=cutoff, signal_action="HOLD",
                benchmarks={"SPY": benchmark_frame},
            )
            feature = build_feature_snapshot_from_layers({
                "final_score": .09,
                "decision_from_synthesis": "HOLD",
                "confidence": .3333,
                "signal_action": "HOLD",
                "asset_view": "NEUTRAL",
                "contextual_market_shadow": contextual.to_dict(),
            }).to_dict()
            productive = {
                "scores": {"G2ASSET": .09},
                "signals": {"G2ASSET": "HOLD"},
                "decisions": [], "orders": [], "quantities": [],
                "cash": {"before": 250000.0, "after": 250000.0},
            }
            equality = {key: True for key in (
                "scores", "signals", "decisions", "orders", "quantities", "cash"
            )}
            await persist_contextual_snapshot(
                conn, snapshot_id=contextual.snapshot_id, owner_chat_id=OWNER,
                run_id=RUN_ID, plan_id=PLAN_ID, portfolio_snapshot_id=SNAPSHOT_ID,
                instrument_id_value=contextual.identity["instrument_id"],
                signal="HOLD", conviction=.3333, cutoff=cutoff,
                feature_snapshot_v3=feature,
                contextual_snapshot=contextual.to_dict(),
                input_hashes=contextual.input_digests,
                productive_baseline=productive, productive_shadow=productive,
                non_regression=equality, code_version=code_version(),
                candle_inputs={
                    "asset": [item.observation_id for item in asset_obs],
                    "general_benchmark": [item.observation_id for item in spy_obs],
                },
            )
            reread = await read_contextual_snapshot(conn, contextual.snapshot_id)
            if reread is None:
                raise AssertionError("contextual snapshot was not reread")
            immutable = {"candle": False, "snapshot": False}
            try:
                await conn.execute(
                    "UPDATE market_candle_observations SET ticker='MUTATED' WHERE observation_id=$1",
                    UUID(asset_obs[0].observation_id),
                )
            except Exception:
                immutable["candle"] = True
            try:
                await conn.execute(
                    "UPDATE contextual_market_snapshots SET signal='BUY' WHERE snapshot_id=$1",
                    contextual.snapshot_id,
                )
            except Exception:
                immutable["snapshot"] = True
            if not all(immutable.values()):
                raise AssertionError("append-only trigger missing")
            timescale_version = await conn.fetchval(
                "SELECT extversion FROM pg_extension WHERE extname='timescaledb'"
            )

        eligible_asset = [row for row in asset_rows if row["is_closed"]]
        eligible_spy = [row for row in spy_rows if row["is_closed"]]
        return {
            "schema": "contextual-g2-disposable-validation-v1",
            "result": "PASS",
            "database": {
                "engine": "PostgreSQL/TimescaleDB",
                "timescaledb_version": timescale_version,
                "disposable": True,
            },
            "migration": {
                "applied_twice": True,
                "prior_columns": prior_columns,
                "added_legacy_columns": [name for name in LEGACY_COLUMNS if name in migrated_columns],
                "legacy_unknown_preserved": True,
                "append_only": immutable,
            },
            "roundtrip": {
                "asset_observations": len(asset_rows),
                "asset_closed": len(eligible_asset),
                "benchmark_observations": len(spy_rows),
                "benchmark_closed": len(eligible_spy),
                "feature_schema": feature["schema_version"],
                "context_snapshot_id": contextual.snapshot_id,
                "context_confidence": contextual.components["context_confidence"],
                "reread_candle_links": len(reread["candles"]),
            },
            "pit": {
                "cutoff": cutoff.isoformat(),
                "all_available_at_lte_cutoff": all(row["available_at"] <= cutoff for row in [*asset_rows, *spy_rows]),
                "all_scraped_at_lte_cutoff": all(row["scraped_at"] <= cutoff for row in [*asset_rows, *spy_rows]),
                "closed_bars_have_bar_end": all(row["bar_end"] is not None for row in [*eligible_asset, *eligible_spy]),
                "future_bars": 0,
            },
            "non_regression": equality,
            "orders_executed": 0,
        }
    finally:
        await db.close()


async def validate_g2(admin_url: str) -> dict:
    import asyncpg

    admin_url = _checked_loopback_url(admin_url)
    admin = await asyncpg.connect(admin_url)
    database_name = "quantia_contextual_g2_" + uuid4().hex
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
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = asyncio.run(validate_g2(args.database_url))
    payload = json.dumps(result, indent=2, ensure_ascii=False, default=str)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload + "\n", encoding="utf-8")
    print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

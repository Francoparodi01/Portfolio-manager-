"""Validate E1 persistence against a disposable loopback PostgreSQL database.

The fixture is generated, explicitly labelled and used only to exercise the
real Timescale/PostgreSQL write and read paths. It never reads application
credentials, changes .env files or calls a broker.
"""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

from src.analysis.enums import DecisionType
from src.analysis.execution_planner import DecisionIntent, ExecutionPlan, OrderIntent, OrderSide
from src.analysis.synthesis import attach_technical_evidence, blend_scores
from src.analysis.technical import analyze_ticker_from_frame
from src.collector.cocos_history import candles_to_frame
from src.collector.data.models import AssetType, Currency, MarketCandle
from src.collector.db import PortfolioDatabase


UTC = timezone.utc
OWNER = 910001
TICKER = "G1TEST"
PROVIDER_SYMBOL = "G1TEST-0001-C-CT-ARS"
RUN_ID = "71000111-2222-4333-8444-555555555555"


def _checked_loopback_url(url: str) -> str:
    parsed = urlsplit(url)
    if parsed.hostname not in {"127.0.0.1", "localhost"}:
        raise ValueError("G1 validation requires a loopback PostgreSQL host")
    if parsed.path != "/quantia_contextual_g1_test":
        raise ValueError("G1 validation requires /quantia_contextual_g1_test")
    return url


def _fixture_candles() -> tuple[list[MarketCandle], datetime]:
    start = datetime(2025, 1, 2, 14, 0, tzinfo=UTC)
    candles = []
    for index in range(260):
        bar_start = start + timedelta(days=index)
        bar_end = bar_start + timedelta(hours=6)
        close = 100 + index * 0.15 + math.sin(index / 5)
        candles.append(MarketCandle(
            ticker=TICKER,
            long_ticker=PROVIDER_SYMBOL,
            asset_type=AssetType.CEDEAR,
            currency=Currency.ARS,
            venue="BYMA",
            interval="1d",
            ts=bar_start,
            open_price=close - 0.4,
            high_price=close + 1.2,
            low_price=close - 1.1,
            close_price=close,
            volume=1_000 + (index % 20) * 25,
            source="G1_CONTROLLED_FIXTURE",
            scraped_at=bar_end + timedelta(minutes=10),
            bar_start=bar_start,
            bar_end=bar_end,
            available_at=bar_end + timedelta(minutes=5),
            is_closed=True,
            volume_unit="units",
            calendar="G1_CONTROLLED_SESSIONS_V1",
            calendar_validation="controlled_fixture_complete_v1",
            adjustment_policy="unadjusted_controlled_fixture_v1",
            depositary_ratio="1:1",
        ))
    return candles, candles[-1].scraped_at + timedelta(minutes=5)


def _blocked_plan(result, price: float) -> ExecutionPlan:
    decision = DecisionIntent(
        ticker=TICKER,
        action=DecisionType.BLOCKED,
        reason_primary="G1 persistence validation; never executable",
        reason_secondary="controlled test infrastructure",
        current_weight=0.10,
        target_weight=0.10,
        delta_weight=0.0,
        score=result.final_score,
        conviction=result.conviction,
        signal_action=result.decision,
        asset_view=result.asset_view,
        data_quality=result.data_quality,
    )
    blocked = OrderIntent(
        ticker=TICKER,
        side=OrderSide.BUY,
        action=DecisionType.BLOCKED,
        amount_ars=0.0,
        theoretical_ars=0.0,
        quantity_est=0.0,
        reference_price=price,
        reason="G1 evidence only; broker execution is disabled",
        priority=1,
        block_code="G1_TEST_NO_EXECUTION",
        decision_override=result.decision,
    )
    return ExecutionPlan(
        decisions=[decision],
        sell_orders=[],
        buy_orders=[],
        blocked_orders=[blocked],
        cash_before=500_000.0,
        gross_sell_ars=0.0,
        fee_sell_ars=0.0,
        net_sell_ars=0.0,
        gross_buy_ars=0.0,
        fee_buy_ars=0.0,
        cash_after=500_000.0,
        feasible=True,
        gate="G1_TEST_NO_EXECUTION",
        summary="Controlled persistence validation; no executable orders",
    )


async def _run_in_database(url: str) -> dict:
    import asyncpg
    from scripts.run_analysis import _save_execution_plan_events

    db = PortfolioDatabase(url)
    await db.connect()
    try:
        await db.init_schema()
        async with db._pool.acquire() as conn:
            await conn.execute("INSERT INTO bot_users(chat_id, display_name) VALUES($1, 'G1 TEST')", OWNER)
            snapshot_id = uuid4()
            snapshot_at = datetime(2025, 9, 18, 21, 0, tzinfo=UTC)
            await conn.execute(
                """INSERT INTO portfolio_snapshots(
                    snapshot_id, owner_chat_id, scraped_at, total_value_ars,
                    cash_ars, confidence_score, dom_hash, raw_html_hash
                ) VALUES($1,$2,$3,$4,$5,$6,$7,$8)""",
                snapshot_id, OWNER, snapshot_at, 1_000_000, 500_000, 1.0,
                "g1-controlled-dom", "g1-controlled-payload",
            )
            await conn.execute(
                """INSERT INTO positions(
                    snapshot_id, scraped_at, ticker, asset_type, currency, quantity,
                    avg_cost, current_price, market_value, weight_in_portfolio
                ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)""",
                snapshot_id, snapshot_at, TICKER, "CEDEAR", "ARS", 100,
                1_000, 1_200, 120_000, 0.12,
            )

        candles, cutoff = _fixture_candles()
        assert await db.save_market_candles(candles) == len(candles)
        rows = await db.get_market_candles(
            TICKER,
            asset_type="CEDEAR",
            currency="ARS",
            venue="BYMA",
            long_ticker=PROVIDER_SYMBOL,
            interval="1d",
            cutoff=cutoff,
        )
        frame = candles_to_frame(rows)
        frame.attrs["cutoff"] = cutoff
        signal = analyze_ticker_from_frame(TICKER, frame)
        if signal is None:
            raise RuntimeError("controlled G1 candles did not produce a technical signal")
        result = blend_scores(
            TICKER, signal.signal, signal.strength, 0.10,
            {"risk_level": "NORMAL"}, 0.05, signal.score_raw,
            technical_candle_source_mode="official",
            technical_candle_sources=tuple(frame.attrs["candle_sources"]),
            technical_candle_source_counts=frame.attrs["candle_source_counts"],
        )
        attach_technical_evidence(result, signal)
        result.asset_view = "FAVORABLE" if result.final_score > 0 else "NEUTRAL"

        portfolio = {
            "snapshot_id": str(snapshot_id),
            "owner_chat_id": OWNER,
            "scraped_at": snapshot_at.isoformat(),
            "total_value_ars": 1_000_000.0,
            "cash_ars": 500_000.0,
            "positions": [{
                "ticker": TICKER, "asset_type": "CEDEAR", "currency": "ARS",
                "quantity": 100.0, "current_price": 1_200.0,
                "market_value": 120_000.0, "weight_in_portfolio": 0.12,
            }],
        }
        plan = _blocked_plan(result, float(frame.Close.iloc[-1]))
        saved_ids = await _save_execution_plan_events(
            cfg=SimpleNamespace(database=SimpleNamespace(url=url)),
            execution_plan=plan,
            results=[result],
            macro_snap={"regime": "G1_CONTROLLED", "observed_at": cutoff.isoformat()},
            macro_regime="NEUTRAL",
            total_ars=1_000_000,
            positions=portfolio["positions"],
            portfolio_snapshot=portfolio,
            owner_chat_id=OWNER,
            run_id=RUN_ID,
        )

        async with db._pool.acquire() as conn:
            plan_row = await conn.fetchrow("SELECT * FROM execution_plans WHERE run_id=$1", RUN_ID)
            decision = await conn.fetchrow("SELECT * FROM decision_log WHERE id=$1", saved_ids[0])
            intent = await conn.fetchrow(
                "SELECT * FROM order_intents WHERE execution_plan_id=$1 ORDER BY sequence_no",
                plan_row["id"],
            )
            captures = await conn.fetch(
                "SELECT capture_hash,payload FROM decision_lab_plan_captures WHERE plan_id=$1 ORDER BY captured_at",
                str(plan_row["id"]),
            )
            timescale_version = await conn.fetchval(
                "SELECT extversion FROM pg_extension WHERE extname='timescaledb'"
            )
        layers = json.loads(decision["layers"])
        capture_payloads = [json.loads(row["payload"]) for row in captures]
        full = next(item for item in capture_payloads if item["capture_kind"] == "FULL_CONTEXT")
        persisted_signal = full["signals"][0]
        quality = layers["data_quality"]
        identity = quality["series_identity"]
        expected_identity = {
            "ticker": TICKER, "asset_type": "CEDEAR", "currency": "ARS",
            "venue": "BYMA", "interval": "1d", "volume_unit": "units",
            "calendar": "G1_CONTROLLED_SESSIONS_V1",
            "adjustment_policy": "unadjusted_controlled_fixture_v1",
            "depositary_ratio": "1:1",
            "instrument_id": "BYMA:CEDEAR:G1TEST:ARS",
        }
        if identity != expected_identity:
            raise AssertionError(f"identity reread mismatch: {identity!r}")
        if any(quality[key] != "VALID" for key in ("price_status", "volume_status", "provenance_status")):
            raise AssertionError(f"quality did not survive as VALID: {quality!r}")
        if layers["feature_snapshot"]["schema_version"] != "feature_snapshot_v3":
            raise AssertionError("feature snapshot v3 missing")
        if intent["is_executable"] or not intent["was_blocked"]:
            raise AssertionError("G1 evidence unexpectedly became executable")
        if full["owner"] != OWNER or full["run_id"] != RUN_ID:
            raise AssertionError("owner/run lineage mismatch")
        if full["portfolio"]["snapshot_id"] != str(snapshot_id):
            raise AssertionError("portfolio snapshot lineage mismatch")

        return {
            "gate": "G1",
            "result": "PASS",
            "evidence_kind": "CONTROLLED_GENERATED_FIXTURE_IN_DISPOSABLE_TIMESCALE",
            "database": {"engine": "PostgreSQL/TimescaleDB", "timescaledb_version": timescale_version},
            "lineage": {
                "owner": full["owner"], "run_id": full["run_id"],
                "plan_id": str(plan_row["id"]), "portfolio_snapshot_id": str(snapshot_id),
                "decision_log_id": saved_ids[0], "capture_hashes": [row["capture_hash"] for row in captures],
            },
            "instrument": {
                "identity": identity, "provider_symbols": quality["provider_symbols"],
                "source": list(frame.attrs["candle_sources"]),
            },
            "temporal": {
                "interval": identity["interval"], "cutoff": quality["cutoff"],
                "candle_timestamp": quality["last_candle_timestamp"],
                "bar_start": quality["last_bar_start"], "bar_end": quality["last_bar_end"],
                "available_at": quality["last_available_at"],
                "scraped_at": quality["last_scraped_at"], "is_closed": quality["last_is_closed"],
            },
            "quality": {
                "price_status": quality["price_status"], "price_reasons": quality["price_reasons"],
                "volume_status": quality["volume_status"], "volume_reasons": quality["volume_reasons"],
                "provenance_status": quality["provenance_status"],
                "provenance_reasons": quality["provenance_reasons"],
                "volume_quality_20": quality["volume_quality_20"],
                "bar_count": quality["bar_count"], "series_digest": quality["series_digest"],
            },
            "analysis": {
                "signal": persisted_signal["decision"],
                "conviction": persisted_signal["conviction"],
                "asset_view": persisted_signal["asset_view"],
                "technical_regime": persisted_signal["technical_regime"],
                "trend_shadow": layers["trend_shadow"],
                "feature_snapshot": layers["feature_snapshot"],
            },
            "versions": layers["versions"],
            "plan": {
                "payload_version": plan_row["payload_version"],
                "cash_before": plan_row["cash_before"], "cash_after": plan_row["cash_after"],
                "intent_executable": intent["is_executable"], "intent_blocked": intent["was_blocked"],
                "plan": asdict(plan),
            },
            "hashes": {"code_hashes": full["code_hashes"], "feature_snapshot_id": layers["feature_snapshot"]["feature_snapshot_id"]},
            "gaps": [],
        }
    finally:
        await db.close()


async def validate_g1(admin_url: str) -> dict:
    import asyncpg

    admin_url = _checked_loopback_url(admin_url)
    admin = await asyncpg.connect(admin_url)
    database_name = "quantia_contextual_g1_" + uuid4().hex
    try:
        await admin.execute(f'CREATE DATABASE "{database_name}"')
        parsed = urlsplit(admin_url)
        test_url = urlunsplit(parsed._replace(path="/" + database_name))
        return await _run_in_database(test_url)
    finally:
        await admin.execute(f'DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)')
        await admin.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    evidence = asyncio.run(validate_g1(args.database_url))
    payload = json.dumps(evidence, indent=2, ensure_ascii=False, default=str)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload + "\n", encoding="utf-8")
    print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

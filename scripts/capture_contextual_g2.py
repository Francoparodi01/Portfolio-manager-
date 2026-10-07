"""Capture one real, non-executing Quantia run with append-only G2 evidence."""
from __future__ import annotations

import argparse
import asyncio
from contextlib import redirect_stdout
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import sys
from typing import Any, Mapping
from uuid import UUID, uuid4

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.backfill_tradingview_byma import _tv_symbol, fetch_tv_candles
from scripts.compare_contextual_e2 import _productive
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
from src.collector.schema_migrations import ensure_execution_plan_persistence


UTC = timezone.utc


def _json(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _hash(value: Any) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
        allow_nan=False, default=str,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _closed_used(rows: list[dict[str, Any]], cutoff: datetime) -> list[dict[str, Any]]:
    return [
        row for row in rows
        if row.get("is_closed") is True
        and row.get("bar_end") is not None
        and row["candle_timestamp"] <= cutoff
        and row["bar_end"] <= cutoff
        and row["available_at"] <= cutoff
        and row["scraped_at"] <= cutoff
    ]


def _pit_role(rows: list[dict[str, Any]], used: list[dict[str, Any]], cutoff: datetime) -> dict[str, Any]:
    identities = sorted({
        (
            row["instrument_id"], row["ticker"], row["provider_symbol"],
            row["market"], row["currency"], row["interval"],
        )
        for row in rows
    })
    return {
        "observations_known_at_cutoff": len(rows),
        "closed_observations_used": len(used),
        "identity_count": len(identities),
        "identity": list(identities[0]) if len(identities) == 1 else None,
        "first_candle_timestamp": used[0]["candle_timestamp"].isoformat() if used else None,
        "last_candle_timestamp": used[-1]["candle_timestamp"].isoformat() if used else None,
        "last_bar_end": used[-1]["bar_end"].isoformat() if used else None,
        "violations": {
            "candle_after_cutoff": sum(row["candle_timestamp"] > cutoff for row in used),
            "available_after_cutoff": sum(row["available_at"] > cutoff for row in used),
            "scraped_after_cutoff": sum(row["scraped_at"] > cutoff for row in used),
            "bar_end_after_cutoff": sum(row["bar_end"] > cutoff for row in used),
            "not_closed": sum(row.get("is_closed") is not True for row in used),
            "missing_bar_end": sum(row.get("bar_end") is None for row in used),
        },
    }


async def _latest_portfolio_asset(conn: Any, owner_chat_id: int, preferred: str | None) -> dict[str, Any]:
    row = await conn.fetchrow(
        """
        WITH latest AS (
            SELECT snapshot_id
            FROM portfolio_snapshots
            WHERE owner_chat_id=$1
            ORDER BY scraped_at DESC
            LIMIT 1
        )
        SELECT p.ticker,p.asset_type,p.currency,p.market_value,p.current_price,
               s.snapshot_id,s.scraped_at,s.total_value_ars,s.cash_ars
        FROM latest l
        JOIN portfolio_snapshots s USING(snapshot_id)
        JOIN positions p USING(snapshot_id)
        WHERE ($2::text IS NULL OR UPPER(p.ticker)=UPPER($2))
        ORDER BY p.market_value DESC NULLS LAST,p.ticker
        LIMIT 1
        """,
        owner_chat_id, preferred,
    )
    if row is None:
        raise RuntimeError("NO_OWNER_SCOPED_PORTFOLIO_ASSET")
    return dict(row)


async def capture_real_g2(
    database_url: str,
    *,
    owner_chat_id: int,
    asset_ticker: str | None,
    general_benchmark: str,
    sector_benchmark: str | None,
    bars: int,
    apply_migration: bool,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    import asyncpg

    conn = await asyncpg.connect(database_url.replace("postgresql+asyncpg://", "postgresql://", 1))
    run_id = str(uuid4())
    version = code_version()
    try:
        if apply_migration:
            await ensure_execution_plan_persistence(conn)
            await ensure_contextual_g2_schema(conn)
        required_tables = await conn.fetchval(
            """SELECT COUNT(*) FROM information_schema.tables
               WHERE table_schema='public'
                 AND table_name IN(
                    'market_candle_observations','contextual_market_snapshots',
                    'contextual_snapshot_candles'
                 )"""
        )
        if int(required_tables or 0) != 3:
            raise RuntimeError("CONTEXTUAL_G2_SCHEMA_NOT_INSTALLED")
        portfolio = await _latest_portfolio_asset(conn, owner_chat_id, asset_ticker)
        asset = str(portfolio["ticker"]).upper()
        asset_type = str(portfolio["asset_type"] or "UNKNOWN").upper()
        if str(portfolio["currency"] or "").upper() != "ARS":
            raise RuntimeError("G2_TRADINGVIEW_BYMA_CAPTURE_REQUIRES_ARS_ASSET")

        targets = [("asset", asset, asset_type), ("general_benchmark", general_benchmark.upper(), "CEDEAR")]
        if sector_benchmark:
            targets.append(("sector_benchmark", sector_benchmark.upper(), "CEDEAR"))
        observations: dict[str, list[Any]] = {}
        for role, ticker, kind in targets:
            fetched = await fetch_tv_candles(ticker, kind, bars=bars)
            observed_at = datetime.now(UTC)
            if len(fetched) < 125:
                raise RuntimeError(f"INSUFFICIENT_REAL_PROVIDER_BARS:{ticker}:{len(fetched)}")
            observations[role] = observations_from_provider_sequence(
                fetched,
                owner_chat_id=owner_chat_id,
                ingestion_run_id=run_id,
                provider_symbol=_tv_symbol(ticker),
                scraped_at=observed_at,
                code_version=version,
                price_unit="ARS_per_instrument",
                volume_unit=None,
            )
        inserted = await persist_candle_observations(
            conn, [item for values in observations.values() for item in values]
        )
        if inserted != sum(len(values) for values in observations.values()):
            raise RuntimeError("REAL_OBSERVATION_INSERT_COUNT_MISMATCH")

        # The productive pipeline never consumes the G2 tables.  It persists a
        # normal formal plan and returns its in-memory artifact for audit only.
        os.environ["DATABASE_URL"] = database_url
        os.environ["TELEGRAM_BOT_TOKEN"] = ""
        from src.core import config as config_module
        config_module._config = None
        from scripts.run_analysis import main as run_analysis_main

        captured_report = io.StringIO()
        with redirect_stdout(captured_report):
            runtime = await run_analysis_main(
                tickers_override=[], period="1y", no_telegram=True,
                no_llm=True, no_sentiment=True, no_optimizer=False,
                skip_radar=True, no_persist=False,
                owner_chat_id=owner_chat_id, run_intent="formal_plan",
                agent_json=False, analysis_run_id_override=run_id,
            )
        if runtime["no_persist"] or runtime["run_intent"] != "formal_plan":
            raise RuntimeError("REAL_RUN_WAS_NOT_A_PERSISTED_FORMAL_PLAN")

        plan = await conn.fetchrow(
            "SELECT * FROM execution_plans WHERE owner_chat_id=$1 AND run_id=$2",
            owner_chat_id, UUID(run_id),
        )
        if plan is None:
            raise RuntimeError("REAL_PLAN_NOT_PERSISTED")
        plan = dict(plan)
        cutoff = plan["created_at"]
        portfolio_snapshot_id = str(runtime["portfolio_snapshot"].get("snapshot_id") or "")
        if not portfolio_snapshot_id:
            raise RuntimeError("REAL_RUN_PORTFOLIO_SNAPSHOT_ID_MISSING")

        decision = await conn.fetchrow(
            """SELECT * FROM decision_log
               WHERE owner_chat_id=$1 AND run_id=$2 AND UPPER(ticker)=UPPER($3)
               ORDER BY id DESC LIMIT 1""",
            owner_chat_id, UUID(run_id), asset,
        )
        if decision is None:
            raise RuntimeError(f"REAL_RUN_DECISION_MISSING:{asset}")
        decision = dict(decision)
        layers = _json(decision.get("layers")) or {}
        captures = await conn.fetch(
            """SELECT capture_hash,captured_at,payload
               FROM decision_lab_plan_captures
               WHERE owner_chat_id=$1 AND plan_id=$2
               ORDER BY captured_at,capture_hash""",
            owner_chat_id, str(plan["id"]),
        )
        capture_payloads = [_json(row["payload"]) for row in captures]
        full_context = next(
            (item for item in capture_payloads if item.get("capture_kind") == "FULL_CONTEXT"),
            None,
        )
        if full_context is None:
            raise RuntimeError("REAL_RUN_FULL_CONTEXT_CAPTURE_MISSING")

        rows_by_role: dict[str, list[dict[str, Any]]] = {}
        frames: dict[str, Any] = {}
        for role, ticker, _ in targets:
            rows = await read_candle_observations(
                conn, owner_chat_id=owner_chat_id, ingestion_run_id=run_id,
                ticker=ticker, cutoff=cutoff,
            )
            rows_by_role[role] = rows
            frames[role] = candles_to_frame(observation_rows_to_candles(rows))
        benchmarks = {general_benchmark.upper(): frames["general_benchmark"]}
        if sector_benchmark:
            benchmarks[sector_benchmark.upper()] = frames["sector_benchmark"]
        contextual = build_contextual_snapshot(
            frames["asset"], cutoff=cutoff,
            signal_action=str(decision["decision"]),
            benchmarks=benchmarks,
            general_benchmark=general_benchmark.upper(),
            sector_benchmark=sector_benchmark.upper() if sector_benchmark else None,
        )
        feature_layers = dict(layers)
        feature_layers["contextual_market_shadow"] = contextual.to_dict()
        feature = build_feature_snapshot_from_layers(feature_layers).to_dict()

        baseline_capture = deepcopy(full_context)
        shadow_capture = deepcopy(full_context)
        matched = False
        for signal in shadow_capture.get("signals", []):
            if str(signal.get("ticker", "")).upper() == asset:
                signal["contextual_market_shadow"] = contextual.to_dict()
                matched = True
        if not matched:
            raise RuntimeError("REAL_RUN_SIGNAL_NOT_FOUND_IN_FULL_CONTEXT")
        productive_baseline = _productive(baseline_capture)
        productive_shadow = _productive(shadow_capture)
        equality = {
            "scores": [item["final_score"] for item in productive_baseline["signals"]]
                      == [item["final_score"] for item in productive_shadow["signals"]],
            "signals": [item["decision"] for item in productive_baseline["signals"]]
                       == [item["decision"] for item in productive_shadow["signals"]],
            "decisions": productive_baseline["decisions"] == productive_shadow["decisions"],
            "orders": productive_baseline["orders"] == productive_shadow["orders"],
            "quantities": [order["quantity_est"] for values in productive_baseline["orders"].values() for order in values]
                          == [order["quantity_est"] for values in productive_shadow["orders"].values() for order in values],
            "cash": productive_baseline["cash"] == productive_shadow["cash"],
        }
        if not all(equality.values()):
            raise RuntimeError("G2_SHADOW_CHANGED_PRODUCTIVE_OUTPUT")

        used_by_role = {
            role: _closed_used(rows, cutoff) for role, rows in rows_by_role.items()
        }
        await persist_contextual_snapshot(
            conn,
            snapshot_id=contextual.snapshot_id,
            owner_chat_id=owner_chat_id,
            run_id=run_id,
            plan_id=str(plan["id"]),
            portfolio_snapshot_id=portfolio_snapshot_id,
            instrument_id_value=contextual.identity["instrument_id"],
            signal=str(decision["decision"]),
            conviction=float(decision["confidence"]),
            cutoff=cutoff,
            feature_snapshot_v3=feature,
            contextual_snapshot=contextual.to_dict(),
            input_hashes=contextual.input_digests,
            productive_baseline=productive_baseline,
            productive_shadow=productive_shadow,
            non_regression=equality,
            code_version=version,
            candle_inputs={
                role: [str(row["observation_id"]) for row in values]
                for role, values in used_by_role.items()
            },
        )
        reread = await read_contextual_snapshot(conn, contextual.snapshot_id)
        if reread is None:
            raise RuntimeError("REAL_CONTEXTUAL_SNAPSHOT_REREAD_FAILED")
        reread_context = _json(reread["contextual_snapshot"])
        reread_feature = _json(reread["feature_snapshot_v3"])
        if reread_context != contextual.to_dict() or reread_feature != feature:
            raise RuntimeError("REAL_CONTEXTUAL_SNAPSHOT_REREAD_MISMATCH")

        pit_roles = {
            role: _pit_role(rows_by_role[role], used_by_role[role], cutoff)
            for role in rows_by_role
        }
        pit_violations = sum(
            value
            for role in pit_roles.values()
            for value in role["violations"].values()
        )
        ambiguous_roles = [name for name, role in pit_roles.items() if role["identity_count"] != 1]
        if pit_violations or ambiguous_roles:
            raise RuntimeError(
                f"REAL_PIT_AUDIT_FAILED:violations={pit_violations}:ambiguous={ambiguous_roles}"
            )

        real_run = {
            "schema": "contextual-g2-real-run-v1",
            "captured_at": datetime.now(UTC).isoformat(),
            "run_id": run_id,
            "plan_id": str(plan["id"]),
            "owner": owner_chat_id,
            "timestamp": cutoff.isoformat(),
            "code_version": version,
            "portfolio_snapshot_id": portfolio_snapshot_id,
            "portfolio": {
                "total_value_ars": float(runtime["total_ars"]),
                "cash_ars": float(runtime["cash_ars"]),
                "asset_ticker": asset,
                "asset_type": asset_type,
            },
            "analysis": {
                "decision_log_id": decision["id"],
                "signal": decision["decision"],
                "score": decision["final_score"],
                "conviction": decision["confidence"],
                "feature_snapshot_v3_id": feature["feature_snapshot_id"],
                "contextual_snapshot_id": contextual.snapshot_id,
                "mode": contextual.mode,
                "affects_analysis": contextual.affects_analysis,
                "affects_execution": contextual.affects_execution,
            },
            "candles": {
                role: {
                    "ticker": ticker,
                    "provider_symbol": _tv_symbol(ticker),
                    "fetched": len(observations[role]),
                    "used": len(used_by_role[role]),
                    "observation_ids_sha256": _hash([
                        str(row["observation_id"]) for row in used_by_role[role]
                    ]),
                }
                for role, ticker, _ in targets
            },
            "hashes": {
                "context_inputs": contextual.input_digests,
                "baseline_productive": _hash(productive_baseline),
                "shadow_productive": _hash(productive_shadow),
                "full_context_capture": _hash(full_context),
            },
            "persistence": {
                "plan_payload_version": plan["payload_version"],
                "capture_hashes": [str(row["capture_hash"]) for row in captures],
                "contextual_snapshot_reread": True,
                "reread_candle_links": len(reread["candles"]),
            },
            "execution_safety": {
                "telegram_sent": False,
                "broker_order_api_calls": 0,
                "broker_orders_executed": 0,
                "plan_only": True,
            },
        }
        pit_audit = {
            "schema": "contextual-g2-pit-audit-v1",
            "run_id": run_id,
            "plan_id": str(plan["id"]),
            "owner": owner_chat_id,
            "cutoff": cutoff.isoformat(),
            "roles": pit_roles,
            "no_lookahead": pit_violations == 0,
            "identity_unambiguous": not ambiguous_roles,
            "context": {
                "trend_daily": contextual.components["trend_daily"],
                "trend_weekly": contextual.components["trend_weekly"],
                "rvol": contextual.components["rvol"],
                "volume_state": contextual.components["volume_state"],
                "breakout_state": contextual.components["breakout_state"],
                "relative_strength": contextual.components["relative_strength"],
                "context_confidence": contextual.components["context_confidence"],
                "quality": contextual.quality,
                "missingness": contextual.missingness,
                "invalidators": contextual.invalidators,
            },
            "sector_benchmark": (
                {"ticker": sector_benchmark.upper(), "status": "CAPTURED"}
                if sector_benchmark else
                {"ticker": None, "status": "UNKNOWN", "reason": "NO_CONFIGURED_SECTOR_BENCHMARK_MAPPING"}
            ),
            "unknown_preserved": sorted({
                reason
                for values in observations.values()
                for item in values
                for reason in item.missingness
            }),
        }
        non_regression = {
            "schema": "contextual-g2-non-regression-v1",
            "run_id": run_id,
            "plan_id": str(plan["id"]),
            "comparison": "RECORDED_PRODUCTIVE_CAPTURE_VS_SAME_CAPTURE_WITH_E2_ATTACHMENT",
            "equal": equality,
            "baseline_productive_hash": _hash(productive_baseline),
            "shadow_productive_hash": _hash(productive_shadow),
            "shadow_contract": {
                "mode": contextual.mode,
                "affects_analysis": contextual.affects_analysis,
                "affects_execution": contextual.affects_execution,
            },
            "broker_order_api_calls": 0,
            "broker_orders_executed": 0,
        }
        return real_run, pit_audit, non_regression
    finally:
        await conn.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--owner-chat-id", required=True, type=int)
    parser.add_argument("--asset-ticker")
    parser.add_argument("--general-benchmark", default="SPY")
    parser.add_argument("--sector-benchmark")
    parser.add_argument("--bars", type=int, default=260)
    parser.add_argument("--apply-migration", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=Path("docs/evidence"))
    args = parser.parse_args()
    real_run, pit, non_regression = asyncio.run(capture_real_g2(
        args.database_url,
        owner_chat_id=args.owner_chat_id,
        asset_ticker=args.asset_ticker,
        general_benchmark=args.general_benchmark,
        sector_benchmark=args.sector_benchmark,
        bars=args.bars,
        apply_migration=args.apply_migration,
    ))
    args.output_dir.mkdir(parents=True, exist_ok=True)
    outputs = {
        "contextual-g2-real-run.json": real_run,
        "contextual-g2-pit-audit.json": pit,
        "contextual-g2-non-regression.json": non_regression,
    }
    for name, payload in outputs.items():
        (args.output_dir / name).write_text(
            json.dumps(payload, indent=2, ensure_ascii=False, allow_nan=False, default=str) + "\n",
            encoding="utf-8",
        )
    print(json.dumps({
        "result": "PASS", "run_id": real_run["run_id"],
        "plan_id": real_run["plan_id"], "outputs": sorted(outputs),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

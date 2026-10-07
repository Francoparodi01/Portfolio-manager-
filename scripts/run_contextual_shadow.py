"""Run /analisis_contextual with real G3 evidence and zero execution authority."""
from __future__ import annotations

import argparse
import asyncio
from contextlib import redirect_stdout
from copy import deepcopy
from datetime import datetime, timezone
import io
import json
import os
from pathlib import Path
import sys
from typing import Any
from uuid import UUID, uuid4

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.backfill_tradingview_byma import _tv_symbol, fetch_tv_candles
from scripts.run_analysis import _layers_payload_for_decision, main as run_analysis_main
from src.analysis.contextual_market import build_contextual_snapshot
from src.analysis.contextual_telegram import (
    SHADOW_MODE,
    SHADOW_PLAN_SOURCE,
    assert_neutral,
    assert_runtime_is_shadow_only,
    assert_shadow_plan_envelope,
    payload_hash,
    productive_view,
    render_contextual_telegram,
)
from src.analysis.feature_snapshot import build_feature_snapshot_from_layers
from src.analysis.versioning import code_version
from src.collector.cocos_history import candles_to_frame
from src.collector.contextual_g2 import (
    observation_rows_to_candles,
    observations_from_provider_sequence,
    persist_candle_observations,
    persist_contextual_snapshot,
    read_candle_observations,
    read_capture_state,
    read_contextual_snapshot,
    record_capture_event,
)
from src.core.config import get_config


UTC = timezone.utc
MIN_PROVIDER_BARS = 125
DEFAULT_BARS = 260
GENERAL_BENCHMARK = "SPY"


def _direct_dsn(value: str) -> str:
    return value.replace("postgresql+asyncpg://", "postgresql://", 1)


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


async def _portfolio_targets(conn: Any, owner_chat_id: int) -> tuple[str, list[dict[str, Any]]]:
    snapshot = await conn.fetchrow(
        """SELECT snapshot_id
             FROM portfolio_snapshots
            WHERE owner_chat_id=$1
            ORDER BY scraped_at DESC
            LIMIT 1""",
        owner_chat_id,
    )
    if snapshot is None:
        raise RuntimeError("NO_OWNER_SCOPED_PORTFOLIO_SNAPSHOT")
    rows = await conn.fetch(
        """SELECT ticker,asset_type,currency,market_value,current_price
             FROM positions
            WHERE snapshot_id=$1
              AND COALESCE(market_value,0) > 0
            ORDER BY market_value DESC NULLS LAST,ticker""",
        snapshot["snapshot_id"],
    )
    if not rows:
        raise RuntimeError("NO_OWNER_SCOPED_PORTFOLIO_POSITIONS")
    result = [dict(row) for row in rows]
    for row in result:
        if str(row.get("currency") or "").upper() != "ARS":
            raise RuntimeError(f"UNSUPPORTED_NON_ARS_PORTFOLIO_ASSET:{row.get('ticker')}")
    return str(snapshot["snapshot_id"]), result


async def _verify_schema(conn: Any) -> None:
    tables = await conn.fetchval(
        """SELECT COUNT(*)
             FROM information_schema.tables
            WHERE table_schema='public'
              AND table_name IN(
                  'execution_plans','order_intents','market_candle_observations',
                  'contextual_market_snapshots','contextual_snapshot_candles',
                  'market_evidence_capture_events'
              )"""
    )
    guard = await conn.fetchval(
        """SELECT EXISTS(
               SELECT 1 FROM pg_trigger
                WHERE tgname='quantia_contextual_shadow_order_intent_guard'
                  AND tgrelid='order_intents'::regclass
                  AND NOT tgisinternal
           )"""
    )
    if int(tables or 0) != 6 or guard is not True:
        raise RuntimeError("CONTEXTUAL_TELEGRAM_SCHEMA_NOT_INSTALLED")


async def _fetch_targets(
    targets: dict[str, str], *, bars: int,
) -> dict[str, tuple[list[Any], datetime]]:
    semaphore = asyncio.Semaphore(3)

    async def one(ticker: str, asset_type: str) -> tuple[str, list[Any], datetime]:
        async with semaphore:
            candles = await fetch_tv_candles(ticker, asset_type, bars=bars)
            observed_at = datetime.now(UTC)
        if len(candles) < MIN_PROVIDER_BARS:
            raise RuntimeError(
                f"INSUFFICIENT_REAL_PROVIDER_BARS:{ticker}:{len(candles)}"
            )
        return ticker, candles, observed_at

    fetched = await asyncio.gather(*(
        one(ticker, asset_type) for ticker, asset_type in targets.items()
    ))
    return {ticker: (candles, observed_at) for ticker, candles, observed_at in fetched}


def _result_map(runtime: dict[str, Any]) -> dict[str, Any]:
    return {
        str(getattr(item, "ticker", "") or "").upper(): item
        for item in (runtime.get("results") or [])
        if str(getattr(item, "ticker", "") or "").strip()
    }


def _productive_actions(runtime: dict[str, Any]) -> dict[str, str]:
    result = {
        ticker: str(getattr(item, "decision", "UNKNOWN") or "UNKNOWN")
        for ticker, item in _result_map(runtime).items()
    }
    plan = runtime.get("execution_plan")
    for decision in (getattr(plan, "decisions", []) or []):
        ticker = str(getattr(decision, "ticker", "") or "").upper()
        action = getattr(decision, "action", "UNKNOWN")
        result[ticker] = str(getattr(action, "value", action))
    return result


async def _insert_shadow_plan(
    conn: Any,
    *,
    plan_id: str,
    run_id: str,
    owner_chat_id: int,
    cutoff: datetime,
    cash_ars: float,
) -> dict[str, Any]:
    row = await conn.fetchrow(
        """INSERT INTO execution_plans(
               id,owner_chat_id,run_id,created_at,updated_at,source,gate,feasible,
               cash_before,gross_sell_ars,fee_sell_ars,net_sell_ars,
               gross_buy_ars,fee_buy_ars,cash_after,summary,warnings,
               authority_mode,affects_analysis,affects_execution
           ) VALUES(
               $1,$2,$3,$4,$4,'execution_plan','SHADOW_ONLY',FALSE,
               $5,0,0,0,0,0,$5,$6,$7::jsonb,'SHADOW_ONLY',FALSE,FALSE
           ) RETURNING *""",
        UUID(plan_id), int(owner_chat_id), UUID(run_id), cutoff, float(cash_ars),
        "Non-executable lineage envelope for /analisis_contextual.",
        json.dumps([
            "SHADOW_ONLY", "affects_analysis=false", "affects_execution=false",
            "order_intents_forbidden",
        ]),
    )
    if row is None:
        raise RuntimeError("SHADOW_PLAN_ENVELOPE_NOT_PERSISTED")
    order_count = await conn.fetchval(
        "SELECT COUNT(*) FROM order_intents WHERE execution_plan_id=$1",
        UUID(plan_id),
    )
    payload = dict(row)
    assert_shadow_plan_envelope(payload, int(order_count or 0))
    return payload


async def run_contextual_shadow(
    database_url: str,
    *,
    owner_chat_id: int,
    bars: int = DEFAULT_BARS,
) -> dict[str, Any]:
    """Produce one real, owner-scoped G3 capture for Telegram.

    Productive analysis is evaluated in memory with ``no_persist``.  The only
    plan row persisted is a zero-notional shadow lineage envelope protected by
    the database order-intent guard.
    """
    import asyncpg

    if int(owner_chat_id) <= 0:
        raise ValueError("owner_chat_id must be positive")
    if int(bars) < MIN_PROVIDER_BARS:
        raise ValueError(f"bars must be >= {MIN_PROVIDER_BARS}")

    version = code_version()
    capture_id = str(uuid4())
    run_id = str(uuid4())
    plan_id = str(uuid4())
    conn = await asyncpg.connect(_direct_dsn(database_url))
    market_started = market_terminal = False
    run_started = run_terminal = False
    phase = "SCHEMA"
    try:
        await _verify_schema(conn)
        portfolio_snapshot_id_before, positions = await _portfolio_targets(
            conn, owner_chat_id
        )
        targets = {
            str(row["ticker"]).upper(): str(row.get("asset_type") or "UNKNOWN").upper()
            for row in positions
        }
        targets.setdefault(GENERAL_BENCHMARK, "CEDEAR")

        phase = "MARKET_CAPTURE"
        await record_capture_event(
            conn, capture_id=capture_id, owner_chat_id=owner_chat_id,
            status="STARTED", occurred_at=datetime.now(UTC), code_version=version,
            details={
                "purpose": "TELEGRAM_CONTEXTUAL_SHADOW_MARKET_CAPTURE",
                "analysis_run_id": run_id,
                "targets": sorted(targets),
            },
        )
        market_started = True
        fetched = await _fetch_targets(targets, bars=bars)
        observations: dict[str, list[Any]] = {}
        for ticker, asset_type in targets.items():
            candles, observed_at = fetched[ticker]
            observations[ticker] = observations_from_provider_sequence(
                candles,
                owner_chat_id=owner_chat_id,
                ingestion_run_id=capture_id,
                provider_symbol=_tv_symbol(ticker),
                scraped_at=observed_at,
                code_version=version,
                price_unit="ARS_per_instrument",
                volume_unit=None,
            )
        flat_observations = [
            item for ticker in sorted(observations) for item in observations[ticker]
        ]
        inserted = await persist_candle_observations(conn, flat_observations)
        if inserted != len(flat_observations):
            raise RuntimeError("REAL_OBSERVATION_INSERT_COUNT_MISMATCH")
        await record_capture_event(
            conn, capture_id=capture_id, owner_chat_id=owner_chat_id,
            status="COMPLETE", occurred_at=datetime.now(UTC), code_version=version,
            details={
                "analysis_run_id": run_id,
                "observation_count": inserted,
                "targets": sorted(targets),
            },
        )
        market_terminal = True

        phase = "PRODUCTIVE_VIEW"
        await record_capture_event(
            conn, capture_id=run_id, owner_chat_id=owner_chat_id,
            status="STARTED", occurred_at=datetime.now(UTC), code_version=version,
            details={
                "purpose": "TELEGRAM_CONTEXTUAL_SHADOW_ANALYSIS",
                "market_capture_id": capture_id,
                "safety": {
                    "mode": SHADOW_MODE,
                    "affects_analysis": False,
                    "affects_execution": False,
                },
            },
        )
        run_started = True
        captured_stdout = io.StringIO()
        with redirect_stdout(captured_stdout):
            runtime = await run_analysis_main(
                tickers_override=[], period="1y", no_telegram=True,
                no_llm=True, no_sentiment=False, no_optimizer=False,
                skip_radar=True, no_persist=True,
                owner_chat_id=owner_chat_id, run_intent="exploratory",
                agent_json=False, analysis_run_id_override=run_id,
                allow_off_market_formal_plan_for_audit=False,
            )
        assert_runtime_is_shadow_only(runtime)
        runtime_snapshot = runtime.get("portfolio_snapshot") or {}
        portfolio_snapshot_id = str(runtime_snapshot.get("snapshot_id") or "")
        if not portfolio_snapshot_id:
            raise RuntimeError("CONTEXTUAL_RUN_PORTFOLIO_SNAPSHOT_ID_MISSING")
        if portfolio_snapshot_id != portfolio_snapshot_id_before:
            raise RuntimeError("PORTFOLIO_CHANGED_DURING_CONTEXTUAL_CAPTURE")
        results = _result_map(runtime)
        if not results:
            raise RuntimeError("PRODUCTIVE_PIPELINE_RETURNED_NO_SIGNALS")
        missing_market_targets = sorted(set(results) - set(observations))
        if missing_market_targets:
            raise RuntimeError(
                "PRODUCTIVE_ASSET_NOT_CAPTURED:" + ",".join(missing_market_targets)
            )

        cutoff = datetime.now(UTC)
        baseline = productive_view(runtime)
        baseline_hash = payload_hash(baseline)
        shadow_productive = deepcopy(baseline)
        neutrality = assert_neutral(baseline, shadow_productive)
        envelope = await _insert_shadow_plan(
            conn, plan_id=plan_id, run_id=run_id,
            owner_chat_id=owner_chat_id, cutoff=cutoff,
            cash_ars=float(runtime.get("cash_ars") or 0.0),
        )

        phase = "CONTEXTUAL_SNAPSHOTS"
        rows_by_ticker: dict[str, list[dict[str, Any]]] = {}
        used_by_ticker: dict[str, list[dict[str, Any]]] = {}
        frames: dict[str, Any] = {}
        for ticker in sorted(observations):
            rows = await read_candle_observations(
                conn, owner_chat_id=owner_chat_id,
                ingestion_run_id=capture_id, ticker=ticker, cutoff=cutoff,
            )
            used = _closed_used(rows, cutoff)
            if not used:
                raise RuntimeError(f"NO_EFFECTIVE_CLOSED_CONTEXT_INPUTS:{ticker}")
            rows_by_ticker[ticker] = rows
            used_by_ticker[ticker] = used
            frames[ticker] = candles_to_frame(observation_rows_to_candles(used))

        benchmark_frame = frames[GENERAL_BENCHMARK]
        actions = _productive_actions(runtime)
        assets: list[dict[str, Any]] = []
        snapshot_ids: list[str] = []
        effective_observation_ids: set[str] = set()
        for ticker, result in results.items():
            context = build_contextual_snapshot(
                frames[ticker], cutoff=cutoff,
                signal_action=str(getattr(result, "decision", "UNKNOWN") or "UNKNOWN"),
                benchmarks={GENERAL_BENCHMARK: benchmark_frame},
                general_benchmark=GENERAL_BENCHMARK,
                sector_benchmark=None,
            )
            context_payload = context.to_dict()
            layers = _layers_payload_for_decision(
                result,
                extra={
                    "source": SHADOW_PLAN_SOURCE,
                    "mode": SHADOW_MODE,
                    "affects_analysis": False,
                    "affects_execution": False,
                },
                run_id=run_id,
                decided_at=cutoff,
                portfolio_snapshot_id=portfolio_snapshot_id,
            )
            layers["contextual_market_shadow"] = context_payload
            feature = build_feature_snapshot_from_layers(layers).to_dict()
            non_regression = {
                **neutrality,
                "baseline_productive_hash": baseline_hash,
                "shadow_productive_hash": payload_hash(shadow_productive),
                "mode": SHADOW_MODE,
                "affects_analysis": False,
                "affects_execution": False,
            }
            candle_inputs = {
                "asset": [str(row["observation_id"]) for row in used_by_ticker[ticker]],
                "general_benchmark": [
                    str(row["observation_id"])
                    for row in used_by_ticker[GENERAL_BENCHMARK]
                ],
            }
            await persist_contextual_snapshot(
                conn,
                snapshot_id=context.snapshot_id,
                owner_chat_id=owner_chat_id,
                run_id=run_id,
                plan_id=plan_id,
                portfolio_snapshot_id=portfolio_snapshot_id,
                instrument_id_value=context.identity["instrument_id"],
                signal=str(getattr(result, "decision", "UNKNOWN") or "UNKNOWN"),
                conviction=float(
                    getattr(result, "conviction", getattr(result, "confidence", 0.0))
                    or 0.0
                ),
                cutoff=cutoff,
                feature_snapshot_v3=feature,
                contextual_snapshot=context_payload,
                input_hashes=context.input_digests,
                productive_baseline=baseline,
                productive_shadow=shadow_productive,
                non_regression=non_regression,
                code_version=version,
                candle_inputs=candle_inputs,
            )
            reread = await read_contextual_snapshot(conn, context.snapshot_id)
            if reread is None:
                raise RuntimeError(f"CONTEXTUAL_SNAPSHOT_REREAD_FAILED:{ticker}")
            if len(reread["candles"]) != sum(len(value) for value in candle_inputs.values()):
                raise RuntimeError(f"CONTEXTUAL_SNAPSHOT_LINK_COUNT_MISMATCH:{ticker}")
            snapshot_ids.append(context.snapshot_id)
            for values in candle_inputs.values():
                effective_observation_ids.update(values)
            missingness = sorted(set([
                *context.missingness,
                "SECTOR_BENCHMARK_NOT_CONFIGURED",
                "VOLUME_UNIT_UNKNOWN",
                "EXCHANGE_CALENDAR_VERSION_UNKNOWN",
                "DEPOSITARY_RATIO_UNKNOWN",
            ]))
            assets.append({
                "ticker": ticker,
                "productive_action": actions.get(ticker, "UNKNOWN"),
                "signal": str(getattr(result, "decision", "UNKNOWN") or "UNKNOWN"),
                "score": getattr(result, "final_score", None),
                "conviction": getattr(
                    result, "conviction", getattr(result, "confidence", None)
                ),
                "contextual_snapshot_id": context.snapshot_id,
                "feature_snapshot_v3_id": feature["feature_snapshot_id"],
                "context": context_payload,
                "missingness": missingness,
            })

        decision_writes = await conn.fetchval(
            "SELECT COUNT(*) FROM decision_log WHERE owner_chat_id=$1 AND run_id=$2",
            owner_chat_id, UUID(run_id),
        )
        order_writes = await conn.fetchval(
            "SELECT COUNT(*) FROM order_intents WHERE execution_plan_id=$1",
            UUID(plan_id),
        )
        if int(decision_writes or 0) != 0 or int(order_writes or 0) != 0:
            raise RuntimeError("FAIL_CLOSED:CONTEXTUAL_RUN_PERSISTED_PRODUCTIVE_INTENTS")
        assert_shadow_plan_envelope(envelope, int(order_writes or 0))

        confidences = [
            float(asset["context"]["components"]["context_confidence"]["value"])
            for asset in assets
        ]
        mean_confidence = sum(confidences) / len(confidences)
        confidence_status = (
            "HIGH" if mean_confidence >= 0.8
            else "MEDIUM" if mean_confidence >= 0.5
            else "LOW"
        )
        result_payload = {
            "schema": "telegram-contextual-shadow-v1",
            "mode": SHADOW_MODE,
            "affects_analysis": False,
            "affects_execution": False,
            "capture_id": capture_id,
            "run_id": run_id,
            "plan_id": plan_id,
            "portfolio_snapshot_id": portfolio_snapshot_id,
            "contextual_snapshot_ids": snapshot_ids,
            "owner": owner_chat_id,
            "cutoff": cutoff.isoformat(),
            "code_version": version,
            "portfolio_value_ars": float(runtime.get("total_ars") or 0.0),
            "cash_ars": float(runtime.get("cash_ars") or 0.0),
            "evidence_count": len(effective_observation_ids),
            "captured_observation_count": len(flat_observations),
            "context_confidence": confidence_status,
            "context_confidence_value": mean_confidence,
            "observation_ids": sorted(effective_observation_ids),
            "observation_ids_hash": payload_hash(sorted(effective_observation_ids)),
            "snapshot_hashes": {
                asset["ticker"]: payload_hash(asset["context"])
                for asset in assets
            },
            "inputs_hash": payload_hash({
                ticker: [
                    {
                        "observation_id": str(row["observation_id"]),
                        "digest": str(row["observation_digest"]),
                    }
                    for row in used_by_ticker[ticker]
                ]
                for ticker in sorted(used_by_ticker)
            }),
            "assets": assets,
            "productive_view": baseline,
            "non_regression": neutrality,
            "unknown_preserved": [
                "SECTOR_BENCHMARK_NOT_CONFIGURED",
                "VOLUME_UNIT_UNKNOWN",
                "EXCHANGE_CALENDAR_VERSION_UNKNOWN",
                "DEPOSITARY_RATIO_UNKNOWN",
            ],
            "safety": {
                "broker_order_api_calls": 0,
                "broker_orders_executed": 0,
                "decision_log_writes": int(decision_writes or 0),
                "order_intent_writes": int(order_writes or 0),
                "productive_plan_persisted": False,
                "shadow_plan_source": envelope["source"],
                "shadow_plan_authority_mode": envelope["authority_mode"],
                "shadow_plan_feasible": envelope["feasible"],
                "shadow_plan_payload_version": envelope["payload_version"],
            },
        }
        result_payload["telegram_html"] = render_contextual_telegram(result_payload)

        phase = "COMPLETE"
        await record_capture_event(
            conn, capture_id=run_id, owner_chat_id=owner_chat_id,
            status="COMPLETE", occurred_at=datetime.now(UTC), code_version=version,
            details={
                "market_capture_id": capture_id,
                "plan_id": plan_id,
                "portfolio_snapshot_id": portfolio_snapshot_id,
                "contextual_snapshot_ids": snapshot_ids,
                "effective_observation_count": len(effective_observation_ids),
                "inputs_hash": result_payload["inputs_hash"],
                "non_regression": neutrality,
            },
        )
        run_terminal = True
        market_state = await read_capture_state(conn, capture_id)
        run_state = await read_capture_state(conn, run_id)
        if not market_state or market_state["status"] != "COMPLETE":
            raise RuntimeError("MARKET_CAPTURE_NOT_COMPLETE_AFTER_TERMINAL_EVENT")
        if not run_state or run_state["status"] != "COMPLETE":
            raise RuntimeError("ANALYSIS_CAPTURE_NOT_COMPLETE_AFTER_TERMINAL_EVENT")
        result_payload["lifecycle"] = {
            "market_capture": ["STARTED", "COMPLETE"],
            "analysis_run": ["STARTED", "COMPLETE"],
        }
        return result_payload
    except Exception as exc:
        failed_at = datetime.now(UTC)
        if market_started and not market_terminal:
            try:
                await record_capture_event(
                    conn, capture_id=capture_id, owner_chat_id=owner_chat_id,
                    status="FAILED", occurred_at=failed_at, code_version=version,
                    reason_code=type(exc).__name__.upper(),
                    details={"phase": phase, "analysis_run_id": run_id},
                )
            except Exception:
                pass
        if run_started and not run_terminal:
            try:
                await record_capture_event(
                    conn, capture_id=run_id, owner_chat_id=owner_chat_id,
                    status="FAILED", occurred_at=failed_at, code_version=version,
                    reason_code=type(exc).__name__.upper(),
                    details={"phase": phase, "market_capture_id": capture_id},
                )
            except Exception:
                pass
        raise
    finally:
        await conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Telegram contextual shadow capture")
    parser.add_argument("--owner-chat-id", required=True, type=int)
    parser.add_argument("--bars", type=int, default=DEFAULT_BARS)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    cfg = get_config()
    payload = asyncio.run(run_contextual_shadow(
        cfg.database.url,
        owner_chat_id=args.owner_chat_id,
        bars=args.bars,
    ))
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False, allow_nan=False, default=str)
            + "\n",
            encoding="utf-8",
        )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, allow_nan=False, default=str))
    else:
        print(payload["telegram_html"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

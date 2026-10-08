"""Run the complete current /analisis core over reconstructed holdings.

The default mode reads PostgreSQL and writes local artifacts only.  ``--persist``
inserts evidence exclusively into the additive immutable historical-analysis
namespace.  It never writes live portfolios, decisions, plans, order intents,
fills or broker state.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
from datetime import datetime, timezone
import hashlib
from io import StringIO
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Mapping
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import asyncpg
import pandas as pd
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_historical_contextual_replay import (  # noqa: E402
    _dsn,
    _reconstruction_identity,
    _sha256,
    load_market_rows,
)
from src.analysis.historical_analysis_replay import (  # noqa: E402
    AUTHORITY_MODE,
    FULL_REPLAY_DATA_STATUS,
    FULL_REPLAY_METHOD_VERSION,
    FULL_REPLAY_SCHEMA,
    digest,
    full_report_markdown,
    replay_complete_analysis,
)


UTC = timezone.utc


def _code_version() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True, stderr=subprocess.DEVNULL
        ).strip()
    except Exception:
        return "UNKNOWN"


def _clean(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _clean(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_clean(item) for item in value]
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.isoformat()
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        return _clean(value.item())
    return value


def _json(value: Any) -> str:
    return json.dumps(
        _clean(value), ensure_ascii=False, allow_nan=False, sort_keys=True,
        separators=(",", ":"), default=str,
    )


def _float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if pd.notna(result) else None


async def load_historical_inputs(
    database_url: str,
    *,
    owner_chat_id: int,
    start: datetime,
    end: datetime,
) -> dict[str, Any]:
    conn = await asyncpg.connect(
        _dsn(database_url), timeout=20,
        server_settings={"default_transaction_read_only": "on", "statement_timeout": "60000"},
    )
    try:
        async with conn.transaction(readonly=True, isolation="repeatable_read"):
            if await conn.fetchval("SELECT current_setting('transaction_read_only')") != "on":
                raise RuntimeError("historical analysis extraction requires read-only DB")
            decision_rows = await conn.fetch(
                """
                WITH layer_evidence AS (
                    SELECT decided_at, decision_date, ticker, run_id, owner_chat_id,
                           source, vix_at_decision, regime, layers, id::text AS stable_id
                    FROM decision_log
                    WHERE decided_at >= $1 AND decided_at < $2
                      AND source IN ('execution_plan', 'radar', 'broker_movement')
                      AND (owner_chat_id = $3 OR owner_chat_id IS NULL)
                      AND run_id IS NOT NULL
                    UNION ALL
                    SELECT observed_at AS decided_at, observed_session AS decision_date,
                           ticker, run_id, owner_chat_id, 'position_hold' AS source,
                           NULL::double precision AS vix_at_decision, regime, layers,
                           'hold:' || id::text AS stable_id
                    FROM position_hold_observations
                    WHERE observed_at >= $1 AND observed_at < $2
                      AND (owner_chat_id = $3 OR owner_chat_id IS NULL)
                      AND run_id IS NOT NULL
                )
                SELECT decided_at, decision_date, ticker, run_id, owner_chat_id,
                       source, vix_at_decision, regime, layers
                FROM layer_evidence
                ORDER BY decided_at, stable_id
                LIMIT 20000
                """,
                start, end, owner_chat_id,
            )
            sentiment_rows = await conn.fetch(
                """
                SELECT bucket_ts, ticker, asset_scope, score, confidence,
                       event_count, high_impact_count, sources, updated_at
                FROM sentiment_aggregated
                WHERE bucket_ts >= $1::timestamptz - INTERVAL '12 hours'
                  AND bucket_ts < $2::timestamptz
                ORDER BY bucket_ts, ticker
                LIMIT 50000
                """,
                start, end,
            )
            effect_rows = await conn.fetch(
                """
                SELECT e.id AS event_id, x.id AS effect_id, e.event_key,
                       e.issuer_id, e.event_type, e.lifecycle_status,
                       e.effective_at, e.expires_at, e.source_name, e.source_url,
                       e.ingestion_method, e.evidence_level, e.detector_score,
                       x.instrument_id, x.ticker, x.venue, x.asset_type,
                       x.currency, x.quantity_factor, x.price_factor,
                       x.cost_basis_factor, x.depositary_ratio_before,
                       x.depositary_ratio_after, x.metadata
                FROM corporate_events e
                JOIN corporate_event_instrument_effects x ON x.event_id = e.id
                WHERE x.is_active = TRUE
                ORDER BY e.effective_at, x.ticker
                LIMIT 1000
                """
            )
            parent = await conn.fetchrow(
                """
                SELECT run.run_id, run.reconstruction_id
                FROM historical_contextual_replay_runs run
                JOIN historical_contextual_replay_state state USING (run_id)
                WHERE run.owner_chat_id=$1 AND state.status='COMPLETE'
                ORDER BY state.occurred_at DESC LIMIT 1
                """,
                owner_chat_id,
            )
        return {
            "decision_rows": [dict(row) for row in decision_rows],
            "sentiment_rows": [dict(row) for row in sentiment_rows],
            "corporate_effect_rows": [dict(row) for row in effect_rows],
            "parent": dict(parent) if parent else None,
        }
    finally:
        await conn.close()


def _event_id(run_id: UUID, status: str) -> UUID:
    return uuid5(NAMESPACE_URL, f"quantia:historical-analysis-replay:{run_id}:{status}")


async def persist_report(database_url: str, report: dict[str, Any]) -> dict[str, Any]:
    conn = await asyncpg.connect(_dsn(database_url), timeout=20)
    run_id = UUID(report["run_id"])
    owner = int(report["owner_chat_id"])
    try:
        schema_ready = await conn.fetchval(
            """
            SELECT to_regclass('public.historical_analysis_replay_runs') IS NOT NULL
               AND to_regclass('public.historical_analysis_replay_events') IS NOT NULL
               AND to_regclass('public.historical_analysis_replay_days') IS NOT NULL
               AND to_regclass('public.historical_analysis_replay_decisions') IS NOT NULL
               AND to_regclass('public.historical_analysis_replay_state') IS NOT NULL
            """
        )
        if not schema_ready:
            raise RuntimeError("historical analysis replay schema is not applied")
        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO historical_analysis_replay_runs(
                    run_id,reconstruction_id,parent_contextual_run_id,owner_chat_id,
                    evaluated_at,window_start,window_end,code_version,method_version,
                    data_status,mode,affects_analysis,affects_execution,
                    orders_executable,summary,source_manifest,content_hash
                ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'SHADOW_ONLY',FALSE,FALSE,
                         FALSE,$11::jsonb,$12::jsonb,$13)
                """,
                run_id,
                UUID(report["reconstruction_id"]),
                UUID(report["parent_contextual_run_id"]) if report.get("parent_contextual_run_id") else None,
                owner,
                pd.Timestamp(report["evaluated_at"]).to_pydatetime(),
                pd.Timestamp(report["period"]["start"]).date(),
                pd.Timestamp(report["period"]["end"]).date(),
                report["code_version"], report["method_version"], report["data_status"],
                _json(report["summary"]), _json(report["source_manifest"]), report["content_hash"],
            )
            await conn.execute(
                """
                INSERT INTO historical_analysis_replay_events(
                    event_id,run_id,owner_chat_id,status,occurred_at,details
                ) VALUES($1,$2,$3,'STARTED',$4,$5::jsonb)
                """,
                _event_id(run_id, "STARTED"), run_id, owner,
                pd.Timestamp(report["evaluated_at"]).to_pydatetime(),
                _json({"method_version": report["method_version"], "days": len(report["days"])}),
            )
        try:
            async with conn.transaction():
                for day in report["days"]:
                    complete = day.get("status") == "COMPLETE"
                    row_hash = day.get("day_hash") or digest(day)
                    await conn.execute(
                        """
                        INSERT INTO historical_analysis_replay_days(
                            run_id,observed_date,portfolio_snapshot_id,
                            source_analysis_run_id,cutoff,status,reason,
                            source_owner_scope,positions,signals,decisions,
                            order_intents,blocked_orders,gate,feasible,cash_before,
                            cash_after,productive_hash,contextual_hash,day_payload,row_hash
                        ) VALUES(
                            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,
                            $16,$17,$18,$19,$20::jsonb,$21
                        )
                        """,
                        run_id, pd.Timestamp(day["observed_date"]).date(),
                        str(day.get("portfolio_snapshot_id") or ""),
                        UUID(day["source_analysis_run_id"]) if day.get("source_analysis_run_id") else None,
                        pd.Timestamp(day["cutoff"]).to_pydatetime() if day.get("cutoff") else None,
                        day["status"], day.get("reason"), day.get("source_owner_scope"),
                        day.get("positions"), day.get("signals"), day.get("decisions"),
                        day.get("orders"), day.get("blocked_orders"), day.get("gate"),
                        day.get("feasible"), _float(day.get("cash_before")), _float(day.get("cash_after")),
                        day.get("productive_hash") if complete else None,
                        day.get("context_snapshot_hash") if complete else None,
                        _json(day), row_hash,
                    )
                for row in report["decisions"]:
                    await conn.execute(
                        """
                        INSERT INTO historical_analysis_replay_decisions(
                            run_id,observed_date,ticker,portfolio_snapshot_id,
                            source_analysis_run_id,cutoff,signal,score,conviction,
                            planner_action,portfolio_intent,current_weight,target_weight,
                            delta_weight,theoretical_ars,order_side,order_amount_ars,
                            order_quantity,order_blocked,context_severity,context_confidence,
                            outcome_5d_status,asset_return_5d,directional_return_5d,
                            outcome_10d_status,asset_return_10d,directional_return_10d,
                            outcome_20d_status,asset_return_20d,directional_return_20d,
                            decision_payload,row_hash
                        ) VALUES(
                            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,
                            $16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,
                            $29,$30,$31::jsonb,$32
                        )
                        """,
                        run_id, pd.Timestamp(row["observed_date"]).date(), row["ticker"],
                        row["portfolio_snapshot_id"], UUID(row["source_analysis_run_id"]),
                        pd.Timestamp(row["cutoff"]).to_pydatetime(), row["signal"],
                        row["score"], row["conviction"], row["planner_action"],
                        row.get("portfolio_intent"), row.get("current_weight"),
                        row.get("target_weight"), row.get("delta_weight"),
                        _float(row.get("theoretical_ars")), row.get("order_side"),
                        _float(row.get("order_amount_ars")), _float(row.get("order_quantity")),
                        bool(row.get("order_blocked")), row.get("context_severity"),
                        row.get("context_confidence"), row.get("outcome_5d_status"),
                        _float(row.get("asset_return_5d")), _float(row.get("directional_return_5d")),
                        row.get("outcome_10d_status"), _float(row.get("asset_return_10d")),
                        _float(row.get("directional_return_10d")), row.get("outcome_20d_status"),
                        _float(row.get("asset_return_20d")), _float(row.get("directional_return_20d")),
                        _json(row), row["row_hash"],
                    )
                await conn.execute(
                    """
                    INSERT INTO historical_analysis_replay_events(
                        event_id,run_id,owner_chat_id,status,occurred_at,details
                    ) VALUES($1,$2,$3,'COMPLETE',$4,$5::jsonb)
                    """,
                    _event_id(run_id, "COMPLETE"), run_id, owner, datetime.now(UTC),
                    _json({
                        "complete_dates": report["summary"]["population"]["complete_analysis_dates"],
                        "decisions": len(report["decisions"]),
                        "content_hash": report["content_hash"],
                    }),
                )
        except Exception as exc:
            await conn.execute(
                """
                INSERT INTO historical_analysis_replay_events(
                    event_id,run_id,owner_chat_id,status,occurred_at,reason_code,details
                ) VALUES($1,$2,$3,'FAILED',$4,$5,$6::jsonb)
                """,
                _event_id(run_id, "FAILED"), run_id, owner, datetime.now(UTC),
                type(exc).__name__.upper(), _json({"phase": "persist_children"}),
            )
            raise
        state = await conn.fetchrow(
            "SELECT status,occurred_at FROM historical_analysis_replay_state WHERE run_id=$1",
            run_id,
        )
        day_count = await conn.fetchval(
            "SELECT count(*) FROM historical_analysis_replay_days WHERE run_id=$1", run_id
        )
        decision_count = await conn.fetchval(
            "SELECT count(*) FROM historical_analysis_replay_decisions WHERE run_id=$1", run_id
        )
        if not state or state["status"] != "COMPLETE":
            raise RuntimeError("historical analysis replay did not reach COMPLETE")
        return {
            "run_id": str(run_id), "status": state["status"],
            "completed_at": state["occurred_at"].isoformat(),
            "persisted_days": int(day_count), "persisted_decisions": int(decision_count),
        }
    finally:
        await conn.close()


def write_artifacts(output_dir: Path, report: dict[str, Any]) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    files: dict[str, bytes] = {
        "historical_analysis_replay.json": (
            json.dumps(_clean(report), ensure_ascii=False, allow_nan=False, indent=2, default=str) + "\n"
        ).encode("utf-8"),
        "historical_analysis_replay_report.md": full_report_markdown(report).encode("utf-8"),
    }
    fields = [
        "observed_date", "cutoff", "portfolio_snapshot_id", "source_analysis_run_id",
        "ticker", "signal", "score", "conviction", "planner_action", "portfolio_intent",
        "current_weight", "target_weight", "delta_weight", "theoretical_ars",
        "order_side", "order_amount_ars", "order_quantity", "order_blocked",
        "macro_score", "macro_source_ticker", "macro_resolution", "sentiment_score",
        "sentiment_source", "context_severity", "context_confidence",
        "outcome_5d_status", "asset_return_5d", "directional_return_5d",
        "outcome_10d_status", "asset_return_10d", "directional_return_10d",
        "outcome_20d_status", "asset_return_20d", "directional_return_20d",
        "mode", "affects_analysis", "affects_execution", "row_hash",
    ]
    stream = StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=fields)
    writer.writeheader()
    writer.writerows([{key: _clean(row.get(key)) for key in fields} for row in report["decisions"]])
    files["historical_analysis_replay_decisions.csv"] = stream.getvalue().encode("utf-8")
    manifest = {
        "schema": "historical-analysis-replay-manifest-v1",
        "run_id": report["run_id"], "code_version": report["code_version"],
        "data_status": report["data_status"], "content_hash": report["content_hash"],
        "source_manifest": report["source_manifest"],
        "row_counts": {
            "days": len(report["days"]), "decisions": len(report["decisions"]),
            "complete_dates": report["summary"]["population"]["complete_analysis_dates"],
        },
        "files": {name: hashlib.sha256(content).hexdigest() for name, content in sorted(files.items())},
    }
    files["historical_analysis_replay_manifest.json"] = (
        json.dumps(manifest, ensure_ascii=False, allow_nan=False, indent=2) + "\n"
    ).encode("utf-8")
    for name, content in files.items():
        (output_dir / name).write_bytes(content)
    return {name: str(output_dir / name) for name in files}


async def run(args: argparse.Namespace) -> dict[str, Any]:
    canonical_path = args.canonical_csv.resolve()
    events_path = args.events_csv.resolve()
    canonical_frame = pd.read_csv(canonical_path, low_memory=False)
    canonical_rows = canonical_frame.to_dict("records")
    tickers = sorted({str(value).upper() for value in canonical_frame["ticker"]} | {"SPY"})
    start_date = pd.to_datetime(canonical_frame["date"], errors="raise").min().date()
    end_date = pd.to_datetime(canonical_frame["date"], errors="raise").max().date()
    start = (pd.Timestamp(start_date, tz="UTC") - pd.Timedelta(days=550)).to_pydatetime()
    end = (pd.Timestamp(end_date, tz="UTC") + pd.Timedelta(days=2)).to_pydatetime()
    market_rows = await load_market_rows(
        args.database_url, tickers=tickers, start=start,
        end=(pd.Timestamp(end_date, tz="UTC") + pd.Timedelta(days=45)).to_pydatetime(),
    )
    evidence = await load_historical_inputs(
        args.database_url, owner_chat_id=args.owner_chat_id,
        start=pd.Timestamp(start_date, tz="UTC").to_pydatetime(), end=end,
    )
    replay = replay_complete_analysis(
        canonical_rows, market_rows, evidence["decision_rows"], evidence["sentiment_rows"],
        evidence["corporate_effect_rows"], owner_chat_id=args.owner_chat_id,
    )
    canonical_hash = _sha256(canonical_path)
    events_hash = _sha256(events_path)
    reconstruction_id = _reconstruction_identity(args.owner_chat_id, canonical_hash, events_hash)
    parent = evidence.get("parent") or {}
    if parent and str(parent.get("reconstruction_id")) != str(reconstruction_id):
        raise RuntimeError("latest contextual replay uses a different reconstruction")
    code_version = args.code_version or _code_version()
    source_manifest = {
        "canonical_file": canonical_path.name, "canonical_sha256": canonical_hash,
        "events_file": events_path.name, "events_sha256": events_hash,
        "market_source_table": "market_candles", "market_rows_read": len(market_rows),
        "analysis_vintage_table": "decision_log", "analysis_vintage_rows_read": len(evidence["decision_rows"]),
        "sentiment_source_table": "sentiment_aggregated", "sentiment_rows_read": len(evidence["sentiment_rows"]),
        "corporate_action_tables": ["corporate_events", "corporate_event_instrument_effects"],
        "corporate_effect_rows_read": len(evidence["corporate_effect_rows"]),
        "database_extraction_readonly": True,
    }
    report = {
        **replay,
        "run_id": str(uuid4()), "reconstruction_id": str(reconstruction_id),
        "parent_contextual_run_id": str(parent.get("run_id")) if parent else None,
        "owner_chat_id": args.owner_chat_id, "evaluated_at": datetime.now(UTC).isoformat(),
        "period": {"start": start_date.isoformat(), "end": end_date.isoformat()},
        "code_version": code_version, "source_manifest": source_manifest,
        "orders_executable": False,
    }
    report["content_hash"] = digest({
        "schema": FULL_REPLAY_SCHEMA, "method": FULL_REPLAY_METHOD_VERSION,
        "reconstruction_id": report["reconstruction_id"], "code_version": code_version,
        "source_manifest": source_manifest, "days": report["days"],
        "decisions": report["decisions"],
    })
    report["persistence_status"] = "PENDING" if args.persist else "NOT_REQUESTED"
    artifacts = write_artifacts(args.output_dir.resolve(), report)
    persistence = None
    if args.persist:
        persistence = await persist_report(args.database_url, report)
        report["persistence_status"] = "COMPLETE"
        report["persistence"] = persistence
        artifacts = write_artifacts(args.output_dir.resolve(), report)
    return {
        "run_id": report["run_id"], "reconstruction_id": report["reconstruction_id"],
        "data_status": report["data_status"], "summary": report["summary"],
        "artifacts": artifacts, "persistence": persistence,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--canonical-csv", required=True, type=Path)
    parser.add_argument("--events-csv", required=True, type=Path)
    parser.add_argument("--owner-chat-id", required=True, type=int)
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "output" / "historical-analysis-replay")
    parser.add_argument("--persist", action="store_true")
    parser.add_argument("--code-version")
    args = parser.parse_args()
    load_dotenv(args.env_file, override=False)
    args.database_url = os.environ.get("DATABASE_URL", "")
    if not args.database_url:
        raise SystemExit("DATABASE_URL is required")
    if args.owner_chat_id <= 0:
        raise SystemExit("owner-chat-id must be positive")
    payload = asyncio.run(run(args))
    print(json.dumps(payload, ensure_ascii=False, allow_nan=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Replay baseline technical analysis plus E2 context on reconstructed holdings.

The default execution is read-only and writes local artifacts only.  ``--persist``
is allowed exclusively after the additive historical replay migration is applied;
it inserts immutable evidence in the dedicated historical namespace and never
updates operational portfolio, decision, plan or order tables.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Mapping
from uuid import UUID, NAMESPACE_URL, uuid4, uuid5

import asyncpg
import pandas as pd
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analysis.historical_contextual_replay import (  # noqa: E402
    AUTHORITY_MODE,
    DATA_STATUS,
    REPLAY_METHOD_VERSION,
    REPLAY_SCHEMA,
    digest,
    report_markdown,
    replay_holdings,
    summarize_replay,
)


UTC = timezone.utc
MAX_MARKET_ROWS = 250_000


def _sha256(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def _code_version() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return "UNKNOWN"


def _dsn(value: str) -> str:
    return value.replace("postgresql+asyncpg://", "postgresql://")


def _clean(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _clean(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_clean(item) for item in value]
    if isinstance(value, pd.Timestamp):
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
        _clean(value),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _int_or_none(value: Any) -> int | None:
    try:
        if value is None or pd.isna(value):
            return None
        return int(float(value))
    except (TypeError, ValueError, OverflowError):
        return None


def _float_or_none(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        number = float(value)
        return number if pd.notna(number) else None
    except (TypeError, ValueError, OverflowError):
        return None


async def load_market_rows(
    database_url: str,
    *,
    tickers: list[str],
    start: datetime,
    end: datetime,
) -> list[dict[str, Any]]:
    conn = await asyncpg.connect(
        _dsn(database_url),
        timeout=20,
        server_settings={
            "default_transaction_read_only": "on",
            "statement_timeout": "60000",
        },
    )
    try:
        async with conn.transaction(readonly=True, isolation="repeatable_read"):
            readonly = await conn.fetchval("SELECT current_setting('transaction_read_only')")
            if readonly != "on":
                raise RuntimeError("historical replay extraction requires read-only DB")
            rows = await conn.fetch(
                """
                SELECT
                    ticker,
                    long_ticker,
                    source,
                    currency,
                    venue,
                    interval,
                    ts,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    scraped_at
                FROM market_candles
                WHERE ticker = ANY($1::text[])
                  AND interval = '1d'
                  AND currency = 'ARS'
                  AND venue = 'BYMA'
                  AND ts >= $2
                  AND ts <= $3
                ORDER BY ticker, source, long_ticker, ts
                LIMIT $4
                """,
                tickers,
                start,
                end,
                MAX_MARKET_ROWS + 1,
            )
        if len(rows) > MAX_MARKET_ROWS:
            raise ValueError("market extraction exceeded bounded limit")
        return [dict(row) for row in rows]
    finally:
        await conn.close()


def _reconstruction_identity(
    owner_chat_id: int,
    canonical_hash: str,
    events_hash: str,
) -> UUID:
    return uuid5(
        NAMESPACE_URL,
        f"quantia:historical-portfolio:{owner_chat_id}:{canonical_hash}:{events_hash}",
    )


def _event_id(run_id: UUID, status: str) -> UUID:
    return uuid5(NAMESPACE_URL, f"quantia:historical-replay:{run_id}:{status}")


async def _schema_ready(conn: asyncpg.Connection) -> bool:
    return bool(
        await conn.fetchval(
            """
            SELECT
                to_regclass('public.historical_portfolio_reconstructions') IS NOT NULL
                AND to_regclass('public.historical_contextual_replay_runs') IS NOT NULL
                AND to_regclass('public.historical_contextual_replay_results') IS NOT NULL
            """
        )
    )


async def persist_report(
    database_url: str,
    *,
    report: dict[str, Any],
    canonical_rows: list[dict[str, Any]],
    event_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    conn = await asyncpg.connect(_dsn(database_url), timeout=20)
    run_id = UUID(report["run_id"])
    reconstruction_id = UUID(report["reconstruction_id"])
    owner = int(report["owner_chat_id"])
    try:
        if not await _schema_ready(conn):
            raise RuntimeError("historical replay schema is not applied")
        existing_reconstruction = await conn.fetchrow(
            """
            SELECT content_hash
            FROM historical_portfolio_reconstructions
            WHERE reconstruction_id=$1
            """,
            reconstruction_id,
        )
        if existing_reconstruction and existing_reconstruction["content_hash"] != report["reconstruction_content_hash"]:
            raise RuntimeError("immutable reconstruction identity conflict")

        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO historical_portfolio_reconstructions(
                    reconstruction_id,owner_chat_id,window_start,window_end,
                    canonical_rows,canonical_sha256,events_sha256,source_manifest,
                    content_hash,code_version
                ) VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10)
                ON CONFLICT (reconstruction_id) DO NOTHING
                """,
                reconstruction_id,
                owner,
                pd.Timestamp(report["period"]["start"]).date(),
                pd.Timestamp(report["period"]["end"]).date(),
                len(canonical_rows),
                report["source_manifest"]["canonical_sha256"],
                report["source_manifest"]["events_sha256"],
                _json(report["source_manifest"]),
                report["reconstruction_content_hash"],
                report["code_version"],
            )
            for row in canonical_rows:
                payload = _clean(row)
                await conn.execute(
                    """
                    INSERT INTO historical_portfolio_reconstruction_positions(
                        reconstruction_id,observed_date,ticker,asset_type,currency,
                        quantity_observed,price_observed,market_value_observed,
                        weight_observed,invested_total_ars,cash_observed_ars,
                        account_total_ars,portfolio_snapshot_id,snapshot_scraped_at,
                        observed_owner_chat_id,confidence,quality_flags,
                        observed_payload,row_hash
                    ) VALUES(
                        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,
                        $16,$17,$18::jsonb,$19
                    )
                    ON CONFLICT (reconstruction_id,observed_date,ticker) DO NOTHING
                    """,
                    reconstruction_id,
                    pd.Timestamp(row["date"]).date(),
                    str(row["ticker"]).upper(),
                    row.get("asset_type"),
                    row.get("currency"),
                    _float_or_none(row.get("quantity_observed")),
                    _float_or_none(row.get("price_observed")),
                    _float_or_none(row.get("market_value_observed")),
                    _float_or_none(row.get("weight_final")),
                    _float_or_none(row.get("invested_total_observed_ars")),
                    _float_or_none(row.get("cash_observed_ars")),
                    _float_or_none(row.get("account_total_recalc_ars")),
                    str(row.get("snapshot_id") or ""),
                    pd.Timestamp(row["snapshot_scraped_at"]).to_pydatetime(),
                    _int_or_none(row.get("owner_chat_id")),
                    str(row.get("confidence") or "LOW"),
                    str(row.get("quality_flags") or "UNKNOWN"),
                    _json(payload),
                    digest(payload),
                )
            for row in event_rows:
                payload = _clean(row)
                await conn.execute(
                    """
                    INSERT INTO historical_portfolio_reconstruction_events(
                        reconstruction_id,event_date,ticker,classification,
                        confidence,event_payload,row_hash
                    ) VALUES($1,$2,$3,$4,$5,$6::jsonb,$7)
                    ON CONFLICT (reconstruction_id,event_date,ticker) DO NOTHING
                    """,
                    reconstruction_id,
                    pd.Timestamp(row["event_date"]).date(),
                    str(row["ticker"]).upper(),
                    str(row.get("classification") or "OBSERVED_CHANGE_INSUFFICIENT_EVIDENCE"),
                    str(row.get("confidence") or "LOW"),
                    _json(payload),
                    digest(payload),
                )

            await conn.execute(
                """
                INSERT INTO historical_contextual_replay_runs(
                    run_id,reconstruction_id,owner_chat_id,evaluated_at,
                    window_start,window_end,code_version,method_version,
                    data_status,mode,affects_analysis,affects_execution,
                    summary,source_manifest,content_hash
                ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'SHADOW_ONLY',FALSE,FALSE,
                         $10::jsonb,$11::jsonb,$12)
                """,
                run_id,
                reconstruction_id,
                owner,
                pd.Timestamp(report["evaluated_at"]).to_pydatetime(),
                pd.Timestamp(report["period"]["start"]).date(),
                pd.Timestamp(report["period"]["end"]).date(),
                report["code_version"],
                report["method_version"],
                report["data_status"],
                _json(report["summary"]),
                _json(report["source_manifest"]),
                report["content_hash"],
            )
            await conn.execute(
                """
                INSERT INTO historical_contextual_replay_run_events(
                    event_id,run_id,owner_chat_id,status,occurred_at,details
                ) VALUES($1,$2,$3,'STARTED',$4,$5::jsonb)
                """,
                _event_id(run_id, "STARTED"),
                run_id,
                owner,
                pd.Timestamp(report["evaluated_at"]).to_pydatetime(),
                _json({
                    "canonical_rows": len(canonical_rows),
                    "method_version": report["method_version"],
                    "data_status": report["data_status"],
                }),
            )

        try:
            async with conn.transaction():
                for row in report["results"]:
                    payload = _clean(row)
                    await conn.execute(
                        """
                        INSERT INTO historical_contextual_replay_results(
                            run_id,observed_date,ticker,market_session,
                            portfolio_snapshot_id,analysis_status,metric_eligible,
                            metric_exclusion_reason,portfolio_confidence,
                            old_signal,old_score_raw,old_strength,new_signal,
                            context_severity,context_confidence,context_confidence_value,
                            asset_return_5d,directional_or_hold_return_5d,outcome_5d_status,
                            asset_return_10d,directional_or_hold_return_10d,outcome_10d_status,
                            asset_return_20d,directional_or_hold_return_20d,outcome_20d_status,
                            result_payload,row_hash
                        ) VALUES(
                            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,
                            $16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26::jsonb,$27
                        )
                        """,
                        run_id,
                        pd.Timestamp(row["date"]).date(),
                        str(row["ticker"]),
                        pd.Timestamp(row["market_session"]).date() if row.get("market_session") else None,
                        str(row.get("portfolio_snapshot_id") or ""),
                        str(row.get("analysis_status") or "UNKNOWN"),
                        bool(row.get("metric_eligible")),
                        row.get("metric_exclusion_reason"),
                        str(row.get("portfolio_confidence") or "LOW"),
                        row.get("old_signal"),
                        _float_or_none(row.get("old_score_raw")),
                        _float_or_none(row.get("old_strength")),
                        row.get("new_signal"),
                        row.get("context_severity"),
                        row.get("context_confidence"),
                        _float_or_none(row.get("context_confidence_value")),
                        _float_or_none(row.get("asset_return_5d")),
                        _float_or_none(row.get("directional_or_hold_return_5d")),
                        row.get("outcome_5d_status"),
                        _float_or_none(row.get("asset_return_10d")),
                        _float_or_none(row.get("directional_or_hold_return_10d")),
                        row.get("outcome_10d_status"),
                        _float_or_none(row.get("asset_return_20d")),
                        _float_or_none(row.get("directional_or_hold_return_20d")),
                        row.get("outcome_20d_status"),
                        _json(payload),
                        digest(payload),
                    )
                await conn.execute(
                    """
                    INSERT INTO historical_contextual_replay_run_events(
                        event_id,run_id,owner_chat_id,status,occurred_at,details
                    ) VALUES($1,$2,$3,'COMPLETE',$4,$5::jsonb)
                    """,
                    _event_id(run_id, "COMPLETE"),
                    run_id,
                    owner,
                    datetime.now(UTC),
                    _json({
                        "result_rows": len(report["results"]),
                        "metric_eligible_rows": report["summary"]["population"]["metric_eligible_rows"],
                        "content_hash": report["content_hash"],
                    }),
                )
        except Exception as exc:
            await conn.execute(
                """
                INSERT INTO historical_contextual_replay_run_events(
                    event_id,run_id,owner_chat_id,status,occurred_at,reason_code,details
                ) VALUES($1,$2,$3,'FAILED',$4,$5,$6::jsonb)
                """,
                _event_id(run_id, "FAILED"),
                run_id,
                owner,
                datetime.now(UTC),
                type(exc).__name__.upper(),
                _json({"phase": "persist_results"}),
            )
            raise

        state = await conn.fetchrow(
            """
            SELECT status,occurred_at
            FROM historical_contextual_replay_state
            WHERE run_id=$1
            """,
            run_id,
        )
        persisted = await conn.fetchval(
            "SELECT count(*) FROM historical_contextual_replay_results WHERE run_id=$1",
            run_id,
        )
        if not state or state["status"] != "COMPLETE" or int(persisted) != len(report["results"]):
            raise RuntimeError("persisted replay failed lifecycle or row-count verification")
        return {
            "run_id": str(run_id),
            "reconstruction_id": str(reconstruction_id),
            "status": state["status"],
            "persisted_rows": int(persisted),
            "occurred_at": state["occurred_at"].isoformat(),
        }
    finally:
        await conn.close()


def _flatten_results(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    fields = [
        "date", "ticker", "asset_type", "currency", "quantity_observed",
        "price_observed", "market_value_observed", "weight_observed",
        "account_weight_observed", "cash_observed_ars", "portfolio_snapshot_id",
        "portfolio_confidence", "position_event_classification", "market_session",
        "market_source", "provider_symbol", "historical_cutoff", "analysis_status",
        "metric_eligible", "metric_exclusion_reason", "old_signal", "old_score_raw",
        "old_strength", "old_technical_regime", "new_signal", "signal_changed",
        "context_severity", "context_confidence", "context_confidence_value",
        "trend_daily", "trend_weekly", "rvol", "volume_state", "breakout_state",
        "rs20", "rs60", "rs120", "distance_support", "distance_resistance",
        "outcome_5d_status", "asset_return_5d", "directional_or_hold_return_5d",
        "outcome_10d_status", "asset_return_10d", "directional_or_hold_return_10d",
        "outcome_20d_status", "asset_return_20d", "directional_or_hold_return_20d",
        "contextual_snapshot_id", "contextual_snapshot_hash", "late_ingested_input_rows",
        "data_status", "mode", "affects_analysis", "affects_execution",
    ]
    flattened = []
    for row in rows:
        item = {field: _clean(row.get(field)) for field in fields}
        item["invalidators"] = _json(row.get("invalidators", []))
        flattened.append(item)
    return flattened


def write_artifacts(output_dir: Path, report: dict[str, Any]) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    files: dict[str, bytes] = {}
    files["historical_contextual_replay.json"] = (
        json.dumps(
            _clean(report),
            ensure_ascii=False,
            allow_nan=False,
            indent=2,
            default=str,
        ) + "\n"
    ).encode("utf-8")
    files["historical_contextual_replay_report.md"] = report_markdown(report).encode("utf-8")

    rows = _flatten_results(report["results"])
    from io import StringIO

    stream = StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=list(rows[0]) if rows else [])
    if rows:
        writer.writeheader()
        writer.writerows(rows)
    files["historical_contextual_replay_results.csv"] = stream.getvalue().encode("utf-8")

    manifest = {
        "schema": "historical-contextual-replay-manifest-v1",
        "run_id": report["run_id"],
        "reconstruction_id": report["reconstruction_id"],
        "code_version": report["code_version"],
        "data_status": report["data_status"],
        "content_hash": report["content_hash"],
        "source_manifest": report["source_manifest"],
        "row_counts": {
            "canonical": report["summary"]["population"]["canonical_rows"],
            "results": len(report["results"]),
            "metric_eligible": report["summary"]["population"]["metric_eligible_rows"],
            "evaluated_5d": report["summary"]["population"]["evaluated_5d_rows"],
        },
        "files": {
            name: hashlib.sha256(content).hexdigest()
            for name, content in sorted(files.items())
        },
    }
    files["manifest.json"] = (
        json.dumps(manifest, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
    ).encode("utf-8")
    for name, content in files.items():
        path = output_dir / name
        if path.exists() and path.read_bytes() != content:
            path.write_bytes(content)
        elif not path.exists():
            path.write_bytes(content)
    return {
        name: str(output_dir / name)
        for name in files
    }


async def run(args: argparse.Namespace) -> dict[str, Any]:
    canonical_path = args.canonical_csv.resolve()
    events_path = args.events_csv.resolve()
    canonical_frame = pd.read_csv(canonical_path, low_memory=False)
    events_frame = pd.read_csv(events_path, low_memory=False)
    required = {
        "date", "ticker", "quantity_observed", "price_observed",
        "market_value_observed", "snapshot_id", "snapshot_scraped_at", "confidence",
    }
    missing = sorted(required - set(canonical_frame.columns))
    if missing:
        raise ValueError("canonical CSV missing columns: " + ", ".join(missing))
    if canonical_frame.duplicated(["date", "ticker"]).any():
        raise ValueError("canonical CSV has duplicate date/ticker rows")
    if events_frame.duplicated(["event_date", "ticker"]).any():
        raise ValueError("events CSV has duplicate event_date/ticker rows")
    canonical_rows = canonical_frame.to_dict("records")
    event_rows = events_frame.to_dict("records")
    tickers = sorted({str(value).upper() for value in canonical_frame["ticker"]} | {"SPY"})
    start_date = pd.to_datetime(canonical_frame["date"], errors="raise").min().date()
    end_date = pd.to_datetime(canonical_frame["date"], errors="raise").max().date()
    market_start = pd.Timestamp(start_date, tz="UTC") - pd.Timedelta(days=550)
    market_end = pd.Timestamp(end_date, tz="UTC") + pd.Timedelta(days=45)
    market_rows = await load_market_rows(
        args.database_url,
        tickers=tickers,
        start=market_start.to_pydatetime(),
        end=market_end.to_pydatetime(),
    )
    replay = replay_holdings(canonical_rows, market_rows, event_rows)
    summary = summarize_replay(replay)
    canonical_hash = _sha256(canonical_path)
    events_hash = _sha256(events_path)
    reconstruction_id = _reconstruction_identity(
        args.owner_chat_id,
        canonical_hash,
        events_hash,
    )
    code_version = args.code_version or _code_version()
    evaluated_at = datetime.now(UTC)
    source_manifest = {
        "canonical_file": canonical_path.name,
        "canonical_sha256": canonical_hash,
        "events_file": events_path.name,
        "events_sha256": events_hash,
        "market_source_table": "market_candles",
        "market_rows_read": len(market_rows),
        "market_query_readonly": True,
        "market_source_selections": replay["source_selections"],
    }
    reconstruction_content_hash = digest(
        {
            "owner_chat_id": args.owner_chat_id,
            "canonical_sha256": canonical_hash,
            "events_sha256": events_hash,
            "rows": len(canonical_rows),
        }
    )
    results = replay["results"]
    content_hash = digest(
        {
            "schema": REPLAY_SCHEMA,
            "method_version": REPLAY_METHOD_VERSION,
            "reconstruction_id": str(reconstruction_id),
            "code_version": code_version,
            "source_manifest": source_manifest,
            "results": results,
        }
    )
    report = {
        "schema": REPLAY_SCHEMA,
        "run_id": str(uuid4()),
        "reconstruction_id": str(reconstruction_id),
        "owner_chat_id": args.owner_chat_id,
        "evaluated_at": evaluated_at.isoformat(),
        "period": {"start": start_date.isoformat(), "end": end_date.isoformat()},
        "code_version": code_version,
        "method_version": REPLAY_METHOD_VERSION,
        "data_status": DATA_STATUS,
        "mode": AUTHORITY_MODE,
        "affects_analysis": False,
        "affects_execution": False,
        "reconstruction_content_hash": reconstruction_content_hash,
        "source_manifest": source_manifest,
        "summary": summary,
        "results": results,
        "content_hash": content_hash,
        "persistence_status": "PENDING" if args.persist else "NOT_REQUESTED",
    }
    artifacts = write_artifacts(args.output_dir.resolve(), report)
    persistence = None
    if args.persist:
        persistence = await persist_report(
            args.database_url,
            report=report,
            canonical_rows=canonical_rows,
            event_rows=event_rows,
        )
        report["persistence_status"] = "COMPLETE"
        report["persistence"] = persistence
        artifacts = write_artifacts(args.output_dir.resolve(), report)
    return {
        "run_id": report["run_id"],
        "reconstruction_id": report["reconstruction_id"],
        "data_status": report["data_status"],
        "summary": report["summary"],
        "artifacts": artifacts,
        "persistence": persistence,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--canonical-csv", required=True, type=Path)
    parser.add_argument("--events-csv", required=True, type=Path)
    parser.add_argument("--owner-chat-id", required=True, type=int)
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=ROOT / "output" / "historical-contextual-replay",
    )
    parser.add_argument("--persist", action="store_true")
    parser.add_argument(
        "--code-version",
        help="Reviewed git SHA recorded in the replay; defaults to the current checkout HEAD.",
    )
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

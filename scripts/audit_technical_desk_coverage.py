"""Stage-0 coverage audit: bounded SELECTs in one read-only snapshot.

Uses the existing bot container's connection and owner without exporting secrets.
No application bootstrap, schema creation, model calls or database writes.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from uuid import UUID


TABLES = (
    "portfolio_snapshots", "positions", "radar_discovery_runs",
    "radar_discovery_snapshots", "radar_discovery_outcomes",
    "market_candle_observations", "market_evidence_capture_events",
    "contextual_market_snapshots", "contextual_snapshot_candles",
    "market_candles", "radar_exploratory_candidates", "radar_setup_alerts",
)


def encode(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (Decimal, UUID)):
        return str(value)
    raise TypeError(type(value).__name__)


async def capture(schema_only=False, details_only=False):
    import asyncpg

    owner = int(os.environ["TELEGRAM_CHAT_ID"])
    if owner <= 0:
        raise ValueError("A positive configured private owner is required")
    dsn = os.environ["DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn, server_settings={
        "default_transaction_read_only": "on", "statement_timeout": "45000",
        "lock_timeout": "2000", "application_name": "technical_desk_stage0_readonly_v1",
    })
    try:
        async with conn.transaction(readonly=True, isolation="repeatable_read"):
            identity = dict(await conn.fetchrow("""
                SELECT current_database() AS database, current_timestamp AS captured_at,
                       current_setting('transaction_read_only') AS transaction_read_only,
                       current_setting('transaction_isolation') AS transaction_isolation,
                       version() AS database_version
            """))
            assert identity["transaction_read_only"] == "on"
            assert identity["transaction_isolation"] == "repeatable read"
            schema = {}
            rows = await conn.fetch("""
                SELECT table_name,column_name,data_type FROM information_schema.columns
                WHERE table_schema='public' AND table_name=ANY($1::text[])
                ORDER BY table_name,ordinal_position
            """, list(TABLES))
            for row in rows:
                schema.setdefault(row["table_name"], {})[row["column_name"]] = row["data_type"]
            result = dict(schema_version="technical-desk-coverage-v1", identity=identity,
                          owner_token=hashlib.sha256(str(owner).encode()).hexdigest()[:16],
                          schema=schema, queries={}, results={})
            if details_only:
                await capture_details(conn, owner, result)
            elif not schema_only:
                await capture_profiles(conn, owner, result)
            return result
    finally:
        await conn.close()


async def capture_details(conn, owner, result):
    queries = {
        "radar_latest_members": """
            SELECT r.run_id,r.captured_at,r.scoring_version,s.ticker,s.asset_type,
                   s.rejection_reason,s.v2_version,s.v3_version,
                   (s.v2_score IS NOT NULL) AS has_v2_score,
                   (s.v3_score IS NOT NULL) AS has_v3_score
            FROM radar_discovery_runs r JOIN radar_discovery_snapshots s USING(run_id)
            WHERE r.run_id=(SELECT run_id FROM radar_discovery_runs WHERE owner_chat_id=$1
                           ORDER BY captured_at DESC LIMIT 1)
            ORDER BY ticker
        """,
        "observation_metadata": """
            SELECT provenance->>'calendar' AS calendar,
                   provenance->>'calendar_validation' AS calendar_validation,
                   provenance->>'adjustment_policy' AS adjustment_policy,
                   provenance->>'depositary_ratio' AS depositary_ratio,
                   price_unit,volume_unit,COUNT(*) AS observations
            FROM market_candle_observations WHERE owner_chat_id=$1
            GROUP BY provenance->>'calendar',provenance->>'calendar_validation',
                     provenance->>'adjustment_policy',provenance->>'depositary_ratio',
                     price_unit,volume_unit
        """,
        "contextual_link_integrity": """
            SELECT COUNT(*) AS links,
                COUNT(*) FILTER(WHERE o.observation_id IS NULL) AS orphan_links,
                COUNT(*) FILTER(WHERE o.owner_chat_id<>s.owner_chat_id) AS owner_mismatch,
                COUNT(*) FILTER(WHERE o.available_at>s.cutoff OR o.scraped_at>s.cutoff
                                  OR o.bar_end>s.cutoff) AS evidence_after_cutoff,
                COUNT(*) FILTER(WHERE o.bar_end IS NULL OR o.is_closed IS DISTINCT FROM TRUE) AS not_closed
            FROM contextual_market_snapshots s JOIN contextual_snapshot_candles c USING(snapshot_id)
            LEFT JOIN market_candle_observations o USING(observation_id)
            WHERE s.owner_chat_id=$1
        """,
    }
    for name, sql in queries.items():
        result["queries"][name] = {"sql": sql, "parameters": ["configured_owner"]}
        result["results"][name] = [dict(r) for r in await conn.fetch(sql, owner)]


async def capture_profiles(conn, owner, result):
    async def query(name, sql, scoped=True):
        if not sql.lstrip().upper().startswith(("SELECT", "WITH")):
            raise ValueError("Only fixed SELECT queries are permitted")
        result["queries"][name] = {"sql": sql, "parameters": ["configured_owner"] if scoped else []}
        result["results"][name] = [dict(r) for r in await conn.fetch(sql, *([owner] if scoped else []))]

    await query("portfolio_latest", """
        WITH latest AS (
            SELECT snapshot_id,scraped_at FROM portfolio_snapshots
            WHERE owner_chat_id=$1 ORDER BY scraped_at DESC,created_at DESC LIMIT 1
        )
        SELECT p.ticker,p.asset_type,p.currency,l.scraped_at AS portfolio_at
        FROM latest l JOIN positions p USING(snapshot_id)
        WHERE p.quantity>0 ORDER BY ticker,asset_type,currency
    """)
    await query("followed_recent", """
        SELECT ticker,'RADAR_MANUAL' AS origin,MAX(user_action_at) AS followed_at
        FROM radar_exploratory_candidates
        WHERE owner_chat_id=$1 AND user_action='FOLLOW'
          AND user_action_at >= CURRENT_TIMESTAMP-INTERVAL '30 days'
        GROUP BY ticker
        UNION ALL
        SELECT ticker,'RADAR_SETUP',MAX(user_action_at)
        FROM radar_setup_alerts
        WHERE owner_chat_id=$1 AND user_action='FOLLOW'
          AND user_action_at >= CURRENT_TIMESTAMP-INTERVAL '30 days'
        GROUP BY ticker ORDER BY ticker,origin
    """)
    await query("radar_versions", """
        SELECT scoring_version,protocol_version,status,COUNT(*) AS runs,
               MIN(captured_session) AS first_session,MAX(captured_session) AS last_session,
               MIN(captured_at) AS first_capture,MAX(captured_at) AS last_capture
        FROM radar_discovery_runs WHERE owner_chat_id=$1
        GROUP BY scoring_version,protocol_version,status ORDER BY first_capture,scoring_version
    """)
    await query("radar_latest", """
        SELECT run_id,scoring_version,protocol_version,captured_session,captured_at,status,
               universe_count,evaluated_count FROM radar_discovery_runs
        WHERE owner_chat_id=$1 ORDER BY captured_at DESC LIMIT 1
    """)
    await query("radar_version_ticker_date", """
        SELECT r.scoring_version,r.protocol_version,r.captured_session,s.ticker,s.asset_type,
               s.v2_version,s.v3_version,COUNT(*) AS snapshots,
               COUNT(s.v2_score) AS with_v2_score,COUNT(s.v3_score) AS with_v3_score,
               COUNT(s.v2_bias) AS with_v2_bias,COUNT(s.v3_classification) AS with_v3_classification,
               MIN(s.created_at) AS first_stored_at,MAX(s.created_at) AS last_stored_at
        FROM radar_discovery_runs r JOIN radar_discovery_snapshots s USING(run_id)
        WHERE r.owner_chat_id=$1
        GROUP BY r.scoring_version,r.protocol_version,r.captured_session,s.ticker,s.asset_type,
                 s.v2_version,s.v3_version ORDER BY captured_session,scoring_version,ticker
    """)
    await query("radar_outcomes_by_version", """
        SELECT r.scoring_version,o.horizon_sessions,o.outcome_basis,COUNT(*) AS outcomes,
               COUNT(DISTINCT s.ticker) AS tickers,
               MIN(r.captured_session) AS first_capture_session,
               MAX(r.captured_session) AS last_capture_session,
               MIN(o.resolved_at) AS first_resolved_at,MAX(o.resolved_at) AS last_resolved_at,
               COUNT(*) FILTER (WHERE o.forward_return IS NOT NULL) AS with_return
        FROM radar_discovery_outcomes o JOIN radar_discovery_snapshots s ON s.id=o.snapshot_id
        JOIN radar_discovery_runs r USING(run_id) WHERE r.owner_chat_id=$1
        GROUP BY r.scoring_version,o.horizon_sessions,o.outcome_basis
        ORDER BY r.scoring_version,o.horizon_sessions,o.outcome_basis
    """)
    observations = """
        WITH observations AS (
            SELECT o.*,e.status AS lifecycle_status,
              (o.is_closed IS TRUE AND o.bar_end IS NOT NULL
               AND o.bar_end<=CURRENT_TIMESTAMP AND o.available_at<=CURRENT_TIMESTAMP
               AND o.scraped_at<=CURRENT_TIMESTAMP AND o.created_at<=CURRENT_TIMESTAMP
               AND o.candle_timestamp<=CURRENT_TIMESTAMP AND o.bar_start<=CURRENT_TIMESTAMP
               AND e.status='COMPLETE' AND o.observation_digest ~ '^[0-9a-f]{64}$') AS candidate_now,
              GREATEST(o.available_at,o.scraped_at,o.created_at,e.occurred_at,e.created_at) AS known_at
            FROM market_candle_observations o
            LEFT JOIN LATERAL (
                SELECT status,occurred_at,created_at FROM market_evidence_capture_events e
                WHERE e.capture_id=o.ingestion_run_id AND e.owner_chat_id=o.owner_chat_id
                  AND e.occurred_at<=CURRENT_TIMESTAMP AND e.created_at<=CURRENT_TIMESTAMP
                ORDER BY occurred_at DESC,created_at DESC,event_id DESC LIMIT 1
            ) e ON TRUE WHERE o.owner_chat_id=$1
        )
    """
    await query("observation_quality", observations + """
        SELECT COUNT(*) AS observations,COUNT(DISTINCT ticker) AS tickers,
            COUNT(*) FILTER(WHERE candidate_now) AS lifecycle_eligible_now,
            COUNT(*) FILTER(WHERE is_closed IS TRUE) AS closed,
            COUNT(*) FILTER(WHERE bar_end IS NOT NULL) AS with_bar_end,
            COUNT(*) FILTER(WHERE observation_digest ~ '^[0-9a-f]{64}$') AS with_digest_format,
            COUNT(*) FILTER(WHERE lifecycle_status='COMPLETE') AS complete_capture,
            COUNT(*) FILTER(WHERE available_at IS NOT NULL) AS with_available_at,
            MIN(candle_timestamp) AS earliest_candle,MAX(candle_timestamp) AS latest_candle,
            MIN(known_at) FILTER(WHERE candidate_now) AS earliest_candidate_known_at,
            COUNT(*) FILTER(WHERE high_price IS NULL OR low_price IS NULL) AS missing_high_low,
            COUNT(*) FILTER(WHERE volume IS NULL) AS missing_volume,
            COUNT(*) FILTER(WHERE volume=0) AS zero_volume
        FROM observations
    """)
    await query("observations_version_ticker", observations + """
        SELECT code_version,ticker,instrument_id,market,currency,interval,source,
            COUNT(*) AS observations,COUNT(DISTINCT bar_start) AS distinct_bars,
            COUNT(DISTINCT bar_start) FILTER(WHERE candidate_now) AS candidate_bars_now,
            MIN(candle_timestamp) AS first_candle,MAX(candle_timestamp) AS last_candle,
            MIN(available_at) AS first_available_at,MAX(available_at) AS last_available_at,
            MIN(known_at) FILTER(WHERE candidate_now) AS first_candidate_known_at,
            MIN(bar_end) FILTER(WHERE candidate_now) AS first_candidate_bar_end,
            MAX(bar_end) FILTER(WHERE candidate_now) AS last_candidate_bar_end,
            COUNT(*) FILTER(WHERE provenance->>'calendar' IS NOT NULL) AS with_calendar,
            COUNT(*) FILTER(WHERE provenance->>'calendar_validation' IS NOT NULL) AS with_calendar_validation,
            COUNT(*) FILTER(WHERE provenance->>'adjustment_policy' IS NOT NULL) AS with_adjustment,
            COUNT(*) FILTER(WHERE provenance->>'depositary_ratio' IS NOT NULL) AS with_ratio,
            COUNT(*) FILTER(WHERE volume IS NULL OR volume<=0) AS missing_or_nonpositive_volume
        FROM observations GROUP BY code_version,ticker,instrument_id,market,currency,interval,source
        ORDER BY ticker,instrument_id,source,code_version
    """)
    await query("observations_version_ticker_date", observations + """
        SELECT code_version,ticker,instrument_id,market,currency,interval,source,
            (candle_timestamp AT TIME ZONE 'UTC')::date AS candle_date_utc_label,
            COUNT(*) AS observations,COUNT(DISTINCT bar_start) AS distinct_bars,
            COUNT(*) FILTER(WHERE candidate_now) AS candidate_observations_now,
            MIN(available_at) AS first_available_at,MAX(available_at) AS last_available_at,
            MIN(known_at) FILTER(WHERE candidate_now) AS first_candidate_known_at
        FROM observations GROUP BY code_version,ticker,instrument_id,market,currency,interval,source,
            (candle_timestamp AT TIME ZONE 'UTC')::date
        ORDER BY candle_date_utc_label,ticker,instrument_id,source,code_version
    """)
    await query("capture_lifecycle", """
        SELECT status,code_version,COUNT(*) AS events,COUNT(DISTINCT capture_id) AS captures,
               MIN(occurred_at) AS first_event,MAX(occurred_at) AS last_event
        FROM market_evidence_capture_events WHERE owner_chat_id=$1
        GROUP BY status,code_version ORDER BY status,code_version
    """)
    await query("contextual_version_instrument_date", """
        WITH link_counts AS (SELECT snapshot_id,COUNT(*) AS links
                             FROM contextual_snapshot_candles GROUP BY snapshot_id)
        SELECT s.code_version,s.instrument_id,(s.cutoff AT TIME ZONE 'UTC')::date AS cutoff_date_utc,
               s.mode,s.affects_analysis,s.affects_execution,COUNT(*) AS snapshots,
               MIN(s.cutoff) AS first_cutoff,MAX(s.cutoff) AS last_cutoff,
               MIN(s.created_at) AS first_stored_at,SUM(COALESCE(l.links,0)) AS candle_links,
               COUNT(*) FILTER(WHERE COALESCE(l.links,0)=0) AS without_candle_links
        FROM contextual_market_snapshots s LEFT JOIN link_counts l USING(snapshot_id)
        WHERE s.owner_chat_id=$1
        GROUP BY s.code_version,s.instrument_id,(s.cutoff AT TIME ZONE 'UTC')::date,
                 s.mode,s.affects_analysis,s.affects_execution
        ORDER BY cutoff_date_utc,s.instrument_id,s.code_version
    """)
    await query("market_candles_current", """
        SELECT ticker,asset_type,currency,venue,interval,source,COUNT(*) AS bars,
            MIN(ts) AS first_candle,MAX(ts) AS last_candle,
            MIN(scraped_at) AS first_scraped_at,MAX(scraped_at) AS last_scraped_at,
            COUNT(*) FILTER(WHERE available_at IS NOT NULL) AS with_available_at,
            COUNT(*) FILTER(WHERE is_closed IS TRUE) AS closed,
            COUNT(*) FILTER(WHERE bar_end IS NOT NULL) AS with_bar_end,
            COUNT(*) FILTER(WHERE calendar_validation IS NOT NULL) AS with_calendar_validation,
            COUNT(*) FILTER(WHERE adjustment_policy IS NOT NULL) AS with_adjustment,
            COUNT(*) FILTER(WHERE volume IS NULL) AS missing_volume
        FROM market_candles GROUP BY ticker,asset_type,currency,venue,interval,source
        ORDER BY ticker,currency,venue,source
    """, scoped=False)
    result["limitations"] = [
        "Local configured database only; remote/backups/other checkouts were not queried.",
        "market_candles is shared market data, has no owner, and is not an immutable as-of archive.",
        "candidate_now checks chronology, lifecycle and digest format, not recomputed digest or full quality.",
        "UTC candle date is a grouping label, not a verified exchange session_date.",
        "Counts preserve source/code versions; overlapping bars across versions must not be summed as unique history.",
        "Stored V2/V3 outputs do not establish reconstructible OHLCV inputs or profitability.",
    ]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--schema-only", action="store_true")
    parser.add_argument("--details-only", action="store_true")
    parser.add_argument("--container", default="cocos_telegram_bot")
    parser.add_argument("--output", type=Path,
                        default=Path("output/technical-desk-stage0-20261114"))
    args = parser.parse_args()
    if args.capture:
        print(json.dumps(asyncio.run(capture(args.schema_only, args.details_only)), default=encode, allow_nan=False))
        return
    source = Path(__file__).read_bytes()
    command = ["docker", "exec", "-i", args.container, "python", "-", "--capture"]
    if args.schema_only:
        command.append("--schema-only")
    if args.details_only:
        command.append("--details-only")
    proc = subprocess.run(command, input=source, capture_output=True, check=False)
    if proc.returncode:
        # Do not echo exception text that might include connection details.
        print("Read-only capture failed; no application fallback or writes attempted.", file=sys.stderr)
        raise SystemExit(proc.returncode)
    payload = json.loads(proc.stdout)
    payload["audit_script_sha256"] = hashlib.sha256(source).hexdigest()
    args.output.mkdir(parents=True, exist_ok=True)
    filename = "details.json" if args.details_only else "schema_capture.json" if args.schema_only else "coverage.json"
    path = args.output / filename
    if path.exists():
        raise FileExistsError(f"Capture already exists: {path}; choose another output directory")
    path.write_text(json.dumps(payload, default=encode, ensure_ascii=False, indent=2,
                               allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps({"path": str(path.resolve()), "identity": payload["identity"],
                      "schema_tables": len(payload["schema"]),
                      "result_groups": {k: len(v) for k, v in payload["results"].items()}},
                     default=encode))


if __name__ == "__main__":
    main()

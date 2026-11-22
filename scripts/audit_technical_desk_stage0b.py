"""Freeze five candidate packets with bounded, owner-scoped, read-only SQL.

This does not run a collector, migrate a schema, or invoke a model. Packet
eligibility is separate from digest integrity. Captures are never overwritten.
"""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from uuid import UUID


QUERIES = {
    "portfolio": """
        SELECT DISTINCT p.ticker,p.asset_type,p.currency,s.snapshot_id,s.scraped_at
        FROM positions p JOIN portfolio_snapshots s USING(snapshot_id)
        WHERE s.snapshot_id=(SELECT snapshot_id FROM portfolio_snapshots
              WHERE owner_chat_id=$1 AND scraped_at<=CURRENT_TIMESTAMP
              ORDER BY scraped_at DESC,created_at DESC,snapshot_id DESC LIMIT 1)
          AND s.owner_chat_id=$1 AND p.quantity>0
          AND p.asset_type='CEDEAR' AND p.currency='ARS'
        ORDER BY p.ticker LIMIT 100
    """,
    "observations": """
        WITH eligible AS (
          SELECT o.*,e.status AS lifecycle_status,
            row_number() OVER(PARTITION BY instrument_id,market,provider_symbol,
                              interval,bar_start
              ORDER BY available_at DESC,scraped_at DESC,o.created_at DESC,
                       observation_id DESC) AS version_rank
          FROM market_candle_observations o
          JOIN LATERAL (
            SELECT status FROM market_evidence_capture_events e
            WHERE e.capture_id=o.ingestion_run_id AND e.owner_chat_id=o.owner_chat_id
              AND e.occurred_at<=CURRENT_TIMESTAMP AND e.created_at<=CURRENT_TIMESTAMP
            ORDER BY e.occurred_at DESC,e.created_at DESC,e.event_id DESC LIMIT 1
          ) e ON e.status='COMPLETE'
          WHERE o.owner_chat_id=$1 AND o.ticker=ANY($2::text[])
            AND o.market='BYMA' AND o.asset_type='CEDEAR' AND o.currency='ARS'
            AND o.source='TRADINGVIEW_BYMA' AND o.interval='1d'
            AND o.candle_timestamp<=CURRENT_TIMESTAMP AND o.bar_start<=CURRENT_TIMESTAMP
            AND o.bar_end<=CURRENT_TIMESTAMP AND o.available_at<=CURRENT_TIMESTAMP
            AND o.scraped_at<=CURRENT_TIMESTAMP AND o.created_at<=CURRENT_TIMESTAMP
            AND o.is_closed IS TRUE
        ) SELECT * FROM eligible WHERE version_rank=1
          ORDER BY ticker,bar_start,observation_id LIMIT 5001
    """,
    "observation_freshness": """
        SELECT ticker,source,interval,COUNT(*) AS observations,
               MAX(candle_timestamp) AS latest_raw_bar,MAX(scraped_at) AS latest_scraped_at,
               MAX(available_at) AS latest_available_at,MAX(created_at) AS latest_created_at,
               MAX(candle_timestamp) FILTER(WHERE is_closed IS TRUE) AS latest_closed_bar,
               COUNT(*) FILTER(WHERE volume_unit IS NULL) AS unknown_volume_unit
        FROM market_candle_observations
        WHERE owner_chat_id=$1 AND ticker=ANY($2::text[])
          AND currency='ARS' AND market='BYMA' AND asset_type='CEDEAR'
          AND scraped_at<=CURRENT_TIMESTAMP AND created_at<=CURRENT_TIMESTAMP
        GROUP BY ticker,source,interval ORDER BY ticker,source,interval
    """,
    "capture_lifecycle": """
        SELECT status,reason_code,COUNT(*) AS events,COUNT(DISTINCT capture_id) AS captures,
               MIN(occurred_at) AS first_at,MAX(occurred_at) AS last_at
        FROM market_evidence_capture_events WHERE owner_chat_id=$1
          AND occurred_at>=CURRENT_TIMESTAMP-INTERVAL '45 days'
          AND occurred_at<=CURRENT_TIMESTAMP AND created_at<=CURRENT_TIMESTAMP
        GROUP BY status,reason_code ORDER BY status,reason_code
    """,
    "mutable_freshness": """
        SELECT ticker,source,interval,COUNT(*) AS bars,MAX(ts) AS latest_bar,
               MAX(scraped_at) AS latest_scraped_at,
               COUNT(*) FILTER(WHERE volume_unit IS NOT NULL) AS with_volume_unit
        FROM market_candles WHERE ticker=ANY($1::text[]) AND currency='ARS'
          AND asset_type='CEDEAR' AND venue='BYMA' AND interval='1d'
          AND ts<=CURRENT_TIMESTAMP AND scraped_at<=CURRENT_TIMESTAMP
        GROUP BY ticker,source,interval ORDER BY ticker,source,interval
    """,
}


def encode(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (Decimal, UUID)):
        return str(value)
    raise TypeError(type(value).__name__)


def canonical(value):
    return json.dumps(value, default=encode, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


async def capture(requested_owner=None):
    if __file__ != "<stdin>":
        sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from src.technical_desk.owner_scope import resolve_owner_chat_id

    owner = resolve_owner_chat_id(os.environ, requested_owner)
    import asyncpg

    conn = await asyncpg.connect(
        os.environ["DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://"),
        server_settings={"default_transaction_read_only": "on", "statement_timeout": "45000",
                         "lock_timeout": "2000", "application_name": "technical_desk_stage0b_v1"})
    try:
        async with conn.transaction(readonly=True, isolation="repeatable_read"):
            identity = dict(await conn.fetchrow("""
                SELECT current_database() AS database,current_timestamp AS captured_at,
                  current_setting('transaction_read_only') AS transaction_read_only,
                  current_setting('transaction_isolation') AS transaction_isolation
            """))
            assert identity["transaction_read_only"] == "on"
            assert identity["transaction_isolation"] == "repeatable read"
            portfolio = [dict(r) for r in await conn.fetch(QUERIES["portfolio"], owner)]
            tickers = sorted({r["ticker"] for r in portfolio})[:5]
            if len(tickers) != 5:
                raise ValueError("five distinct CEDEAR ARS holdings required")
            results = {"portfolio": portfolio}
            for name, query in QUERIES.items():
                if name == "portfolio":
                    continue
                params = ([tickers] if name == "mutable_freshness" else [owner]
                          if name == "capture_lifecycle" else [owner, tickers])
                results[name] = [dict(r) for r in await conn.fetch(query, *params)]
            if len(results["observations"]) >= 5001:
                raise ValueError("bounded export exceeded; increase only after inspecting scope")
            for row in results["observations"]:
                row.pop("owner_chat_id")  # not a field in the market fact digest
                row.pop("version_rank")
                for key in ("provenance", "quality", "missingness"):
                    if isinstance(row[key], str):
                        row[key] = json.loads(row[key])
            return dict(schema="technical-desk-stage0b-capture-v1", identity=identity,
                        owner_token=hashlib.sha256(str(owner).encode()).hexdigest()[:16],
                        selection_rule="first_five_sorted_positive_CEDEAR_ARS_tickers_latest_owner_snapshot",
                        selected_tickers=tickers, queries=QUERIES, results=results)
    finally:
        await conn.close()


def freeze(payload, output):
    # Rehash with the exact frozen source implementation, not the container's import.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from src.collector.contextual_g2 import observation_digest

    source = Path(__file__).resolve().parents[1] / "src/collector/contextual_g2.py"
    all_rows = payload["results"]["observations"]
    summaries = []
    for ticker in payload["selected_tickers"]:
        rows = [r for r in all_rows if r["ticker"] == ticker]
        identities = sorted({r["instrument_id"] for r in rows})
        providers = sorted({r["provider_symbol"] for r in rows})
        checks = []
        for row in rows:
            recomputed = observation_digest(row)
            checks.append({"observation_id": row["observation_id"],
                           "stored_digest": row["observation_digest"],
                           "computed_digest": recomputed,
                           "matches": recomputed == row["observation_digest"]})
        packet = dict(schema="technical-desk-stage0b-candidate-v1", ticker=ticker,
                      owner_token=payload["owner_token"], cutoff=payload["identity"]["captured_at"],
                      selection_rule=payload["selection_rule"],
                      source_capture_hash=digest(payload), observations=rows,
                      rehash=checks, eligibility="NOT_YET_VALIDATED")
        packet_id = digest(packet)
        packet_path = output / f"{ticker}-{packet_id[:16]}.json"
        packet_path.write_bytes(canonical(packet) + b"\n")
        summaries.append(dict(ticker=ticker, packet_id=packet_id, file=packet_path.name,
                              observations=len(rows), identities=identities, providers=providers,
                              first_bar=rows[0]["bar_start"] if rows else None,
                              last_bar=rows[-1]["bar_start"] if rows else None,
                              digest_matches=sum(c["matches"] for c in checks),
                              digest_mismatches=sum(not c["matches"] for c in checks),
                              missing_volume_units=sum(r["volume_unit"] is None for r in rows),
                              calendar_values=dict(Counter(str(r["provenance"].get("calendar")) for r in rows)),
                              adjustment_policies=dict(Counter(str(r["provenance"].get("adjustment_policy")) for r in rows))))
    return dict(schema="technical-desk-stage0b-rehash-v1", identity=payload["identity"],
                selection_rule=payload["selection_rule"], capture_hash=digest(payload),
                digest_source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
                packets=summaries, database_writes=False, models_invoked=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--from-capture", type=Path)
    parser.add_argument("--container", default="cocos_telegram_bot")
    parser.add_argument("--owner-chat-id", help="Required when ADMIN_CHAT_IDS contains multiple owners")
    parser.add_argument("--output", type=Path, default=Path("output/technical-desk-stage0b/packets"))
    args = parser.parse_args()
    if args.capture:
        print(json.dumps(asyncio.run(capture(args.owner_chat_id)), default=encode, allow_nan=False))
        return
    if args.output.exists():
        raise FileExistsError("Use a new output directory; evidence is not overwritten")
    if args.from_capture:
        payload = json.loads(args.from_capture.read_text(encoding="utf-8"))
    else:
        command = ["docker", "exec", "-i", args.container, "python", "-", "--capture"]
        if args.owner_chat_id is not None:
            command.extend(["--owner-chat-id", args.owner_chat_id])
        proc = subprocess.run(command, input=container_capture_source(),
                              capture_output=True, check=False)
        if proc.returncode:
            # Keep connection details and exception context out of stdout.
            print("Read-only capture failed; no mutation or fallback attempted.", file=sys.stderr)
            raise SystemExit(proc.returncode)
        payload = json.loads(proc.stdout)
    args.output.mkdir(parents=True)
    (args.output / "capture.json").write_bytes(canonical(payload) + b"\n")
    summary = freeze(payload, args.output)
    (args.output / "rehash.json").write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))


def container_capture_source():
    """Ship the exact pure selector with stdin; do not alter the container image."""
    path = Path(__file__).resolve()
    selector = (path.parents[1] / "src/technical_desk/owner_scope.py").read_text(encoding="utf-8")
    source = path.read_text(encoding="utf-8")
    return (
        "import sys, types\n"
        "selector = types.ModuleType('src.technical_desk.owner_scope')\n"
        f"exec(compile({selector!r}, '<owner_scope>', 'exec'), selector.__dict__)\n"
        "sys.modules[selector.__name__] = selector\n"
        f"exec(compile({source!r}, '<capture>', 'exec'), "
        "{'__name__': '__main__', '__file__': '<stdin>'})\n"
    ).encode("utf-8")


if __name__ == "__main__":
    main()

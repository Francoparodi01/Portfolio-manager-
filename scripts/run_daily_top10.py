"""Existing Radar daily capture. Files only unless --publish is explicit.

No DDL, decisions, order execution, Telegram sends or shadow activation.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.collect_radar_market import collect, connection_config, write_json
from scripts.build_radar_market_snapshot import build, publish, read_ratios, COMAFI_URL
from src.collector.radar_close_evidence import ART, eligible_session, session_end


def download_ratios(output):
    import requests
    response = requests.get(COMAFI_URL, timeout=30)
    response.raise_for_status()
    path = output / "comafi-ratios.xlsx"
    with path.open("xb") as stream:
        stream.write(response.content)
    if not read_ratios(path):
        raise ValueError("Depositary ratio list empty or schema changed")
    write_json(output / "ratios-source.json", dict(source=response.url,
        retrieved_at=datetime.now(timezone.utc).isoformat(), sha256=hashlib.sha256(response.content).hexdigest()))
    return path


async def run_daily(args, *, now=None, collector=collect, builder=build, publisher=publish,
                    ratios_loader=download_ratios, connector=None):
    now = now or datetime.now(timezone.utc)
    due = eligible_session(now)
    session = getattr(args, "session", None) or due
    if session is None:
        return {"status": "NOT_DUE"}
    if now < session_end(session):
        return {"status": "NOT_DUE"}
    if args.publish and (os.getenv("RADAR_TOP10_DAILY_ENABLED", "").lower() != "true" or session != due):
        raise ValueError("Daily publication disabled or wrong session")
    conn = None
    output = None
    try:
        if args.publish:
            import asyncpg
            dsn, owner = connection_config(args.env_file, args.host, args.port)
            conn = await (connector or asyncpg.connect)(dsn, command_timeout=30)
            locked = await conn.fetchval("SELECT pg_try_advisory_lock(hashtextextended($1, 0))",
                                         f"quantia:top10-daily-v1:{owner}:{session}")
            if not locked:
                return {"status": "BUSY"}
            prior = await conn.fetchval("""SELECT run_id FROM radar_discovery_runs
                WHERE owner_chat_id=$1 AND captured_session=$2 AND status='COMPLETE'
                AND parameters->>'daily_job'='top10-daily-v1'
                ORDER BY captured_at DESC LIMIT 1""", owner, session)
            if prior:
                return {"status": "ALREADY_PUBLISHED", "run_id": str(prior)}
        output = args.output / session.isoformat() / uuid4().hex
        output.mkdir(parents=True, exist_ok=False)
        inputs = output / "inputs"
        await collector(SimpleNamespace(output=inputs, env_file=args.env_file, host=args.host,
            port=args.port, bars=260, close_session=session))
        ratios = await asyncio.to_thread(ratios_loader, output)
        build_args = SimpleNamespace(inputs=inputs, ratios=ratios, output=output/"replay",
            env_file=args.env_file, host=args.host, port=args.port)
        context, run, observations = await asyncio.to_thread(builder, build_args)
        if not run["parameters"].get("independent_close_count"):
            result = {"status": "SOURCE_BLOCKED", "reason": "No dated independent close", "output": str(output)}
        elif args.publish:
            await publisher(build_args, context, run, observations)
            result = {"status": "PUBLISHED", "run_id": run["run_id"], "output": str(output)}
        else:
            result = {"status": "VALIDATED_FILES_ONLY", "run_id": run["run_id"], "output": str(output)}
        write_json(output / "job-result.json", result)
        return result
    except Exception as exc:
        if output:
            write_json(output / "job-result.json", {"status": "FAILED", "error_type": type(exc).__name__})
        raise
    finally:
        if conn:
            await conn.close()  # session advisory lock also released on failure/crash


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--host")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--output", type=Path, default=Path("outputs/radar-daily"))
    parser.add_argument("--session", type=date.fromisoformat, help="Dated file-only validation; never back-publish")
    parser.add_argument("--publish", action="store_true")
    args = parser.parse_args()
    result = await run_daily(args)
    print(json.dumps(result))
    if result["status"] == "SOURCE_BLOCKED":
        raise SystemExit(2)


if __name__ == "__main__":
    asyncio.run(main())

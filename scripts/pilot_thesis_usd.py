"""Run/replay the frozen USD thesis-selection pilot without production state."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import httpx

from src.thesis_desk.usd_pilot import BASE, MODEL, decide, prepare, read, replay, reuse_decisions, status
from src.thesis_desk.usd_pilot_market import capture_market, evaluate


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["prepare", "capture", "decide", "evaluate", "replay", "status", "run"])
    parser.add_argument("--output", type=Path)
    parser.add_argument("--cohort-size", type=int, help="Number of stocks selected by the frozen Aug31 IVV ranking")
    parser.add_argument("--reuse-run", type=Path, help="Reuse unchanged earlier decisions and prices at the same cutoff")
    parser.add_argument("--archive", type=Path, default=Path("outputs/thesis-history-sep-oct-2026-v2"))
    parser.add_argument("--catalog", type=Path, default=Path("outputs/thesis-desk-all-local/runs/20261107T210015-4d9db4d3/result.json"))
    parser.add_argument("--max-events", type=int)
    parser.add_argument("--retry-errors", action="store_true")
    parser.add_argument("--refresh-prices", action="store_true",
                        help="Capture a new dated price snapshot, preserving the earlier capture and evaluations")
    args = parser.parse_args(argv)
    if args.cohort_size is not None and not 1 <= args.cohort_size <= 500:
        parser.error("--cohort-size must be between 1 and 500")
    if args.output is None:
        suffix = "" if args.cohort_size in (None, 25) else f"-{args.cohort_size}"
        args.output = Path(f"outputs/thesis-usd-pilot-sep-oct-2026{suffix}-v1")
    existing = read(args.output / "protocol.json") if (args.output / "protocol.json").exists() else None
    cohort_size = args.cohort_size if args.cohort_size is not None else len(existing["cohort"]) if existing else 25
    if existing and len(existing["cohort"]) != cohort_size:
        parser.error("Frozen cohort size differs; use another --output directory")
    if args.max_events is not None and args.max_events < 1:
        parser.error("--max-events must be positive")
    if args.refresh_prices and args.command not in {"capture", "run"}:
        parser.error("--refresh-prices applies only to capture/run")
    if args.reuse_run and (args.command not in {"prepare", "capture", "run"} or args.refresh_prices):
        parser.error("--reuse-run applies to prepare/capture/run and cannot be combined with --refresh-prices")
    if args.command in {"prepare", "run"} and not (args.output / "protocol.json").exists():
        response = httpx.get(BASE + "/api/tags", timeout=10)
        response.raise_for_status()
        model = next(m for m in response.json()["models"] if m["name"] == MODEL)
        prepare(args.archive, args.catalog, args.output, model["digest"], cohort_size=cohort_size)
    if args.command == "prepare":
        # prepare() verifies a repeated request, including its source files and cohort.
        protocol = read(args.output / "protocol.json")
        prepare(args.archive, args.catalog, args.output, protocol["model_digest"], cohort_size=cohort_size)
        if args.reuse_run:
            reuse_decisions(args.reuse_run, args.output)
        result = status(args.output)
    elif args.command in {"capture", "run"}:
        if args.reuse_run and args.command == "run":
            reuse_decisions(args.reuse_run, args.output)
        market = capture_market(args.output, refresh=args.refresh_prices, reuse_run=args.reuse_run)
        result = dict(symbols=len(market["symbols"]), failures=market["failures"], asof=market["asof"])
    if args.command in {"decide", "run"}:
        result = decide(args.output, max_events=args.max_events, retry_errors=args.retry_errors)
    if args.command in {"evaluate", "run"}:
        result = evaluate(args.output)
        result = {k: result[k] for k in ("status", "decision_statuses", "summaries", "market_asof")}
    elif args.command == "replay":
        result = replay(args.output)
    elif args.command == "status":
        result = status(args.output)
    print(json.dumps(result, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

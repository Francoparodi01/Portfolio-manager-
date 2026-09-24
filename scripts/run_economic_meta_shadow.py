#!/usr/bin/env python3
"""Run Economic Meta Policy v1 in research-only shadow mode.

Input can be a JSON object, a JSON array, or JSONL. The runner only writes
shadow decision records; it does not import broker/execution modules and cannot
change production recommendations, sizing, or orders.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from src.analysis.economic_meta_policy import evaluate_all_preregistered
from src.analysis.economic_meta_store import (
    DEFAULT_SHADOW_PATH,
    EconomicMetaShadowStore,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=None,
        help="JSON/JSONL candidate file. If omitted, reads one JSON payload from stdin.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_SHADOW_PATH,
        help="Append-only JSONL shadow output path.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional shared run id for audit/reproducibility.",
    )
    return parser.parse_args()


def load_candidates(path: Path | None) -> list[dict[str, Any]]:
    if path is None:
        raw = sys.stdin.read().strip()
        if not raw:
            raise SystemExit("stdin is empty; provide a JSON candidate or --input")
    else:
        raw = path.read_text(encoding="utf-8").strip()
        if not raw:
            return []

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        rows: list[dict[str, Any]] = []
        for line_no, line in enumerate(raw.splitlines(), start=1):
            text = line.strip()
            if not text:
                continue
            try:
                item = json.loads(text)
            except json.JSONDecodeError as exc:
                raise SystemExit(f"invalid JSONL at line {line_no}: {exc}") from exc
            if not isinstance(item, dict):
                raise SystemExit(f"JSONL line {line_no} must be an object")
            rows.append(item)
        return rows

    if isinstance(payload, dict):
        return [payload]
    if isinstance(payload, list) and all(isinstance(item, dict) for item in payload):
        return list(payload)
    raise SystemExit("input must be a JSON object, array of objects, or JSONL")


def main() -> int:
    args = parse_args()
    candidates = load_candidates(args.input)
    store = EconomicMetaShadowStore(args.output)
    written = 0
    for candidate in candidates:
        records = evaluate_all_preregistered(candidate, run_id=args.run_id)
        written += store.append_many(records)
        for record in records:
            print(json.dumps(record.to_dict(), ensure_ascii=False, sort_keys=True))

    print(
        json.dumps(
            {
                "status": "SHADOW_ONLY",
                "capital_effect": False,
                "candidate_count": len(candidates),
                "records_written": written,
                "output": str(args.output),
            },
            sort_keys=True,
        ),
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

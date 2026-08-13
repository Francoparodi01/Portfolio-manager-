from __future__ import annotations

import argparse
import csv
from datetime import date
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.technical_buy_shadow_v3_audit import (
    build_technical_buy_shadow_v3_audit,
    render_technical_buy_shadow_v3_audit,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Audita Technical BUY Shadow V3 sobre un replay CSV")
    parser.add_argument("--csv", required=True, help="technical_replay.csv generado read-only")
    parser.add_argument("--cost-bps", type=float, default=75.0)
    parser.add_argument("--split-date", default="2026-07-01")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    with open(args.csv, newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    report = build_technical_buy_shadow_v3_audit(
        rows,
        cost_rate=max(0.0, args.cost_bps) / 10_000.0,
        split_date=date.fromisoformat(args.split_date),
    )
    if args.json:
        print(json.dumps(report.to_dict(), ensure_ascii=False, default=str, indent=2))
    else:
        print(render_technical_buy_shadow_v3_audit(report))


if __name__ == "__main__":
    main()

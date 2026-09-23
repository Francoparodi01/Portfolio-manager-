#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.training.pipeline import run_pipeline_from_files
from src.training.preflight import run_preflight


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fail-closed Quantia agent/JEV training dataset and promotion harness."
    )
    parser.add_argument("--check", action="store_true", help="Only run prerequisite checks.")
    parser.add_argument("--jev-module", default=None, help="Import path for the JEV adapter module.")
    parser.add_argument("--agent-runs")
    parser.add_argument("--agent-steps")
    parser.add_argument("--jev-assessments")
    parser.add_argument("--outcomes")
    parser.add_argument("--output-dir", default="outputs/agent_training/latest")
    parser.add_argument("--validation-fraction", type=float, default=0.20)
    parser.add_argument("--champion-metrics")
    parser.add_argument("--challenger-metrics")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.check:
        result = run_preflight(jev_module=args.jev_module).to_dict()
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return 0 if result["ready"] else 2

    required = {
        "--agent-runs": args.agent_runs,
        "--agent-steps": args.agent_steps,
        "--jev-assessments": args.jev_assessments,
        "--outcomes": args.outcomes,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        print("Missing required inputs: " + ", ".join(missing), file=sys.stderr)
        return 2

    receipt = run_pipeline_from_files(
        agent_runs_path=args.agent_runs,
        agent_steps_path=args.agent_steps,
        jev_assessments_path=args.jev_assessments,
        outcomes_path=args.outcomes,
        output_dir=args.output_dir,
        jev_module=args.jev_module,
        validation_fraction=args.validation_fraction,
        champion_metrics_path=args.champion_metrics,
        challenger_metrics_path=args.challenger_metrics,
    )
    print(json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if receipt["status"] not in {"BLOCKED", "CHALLENGER_BLOCKED"} else 2


if __name__ == "__main__":
    raise SystemExit(main())

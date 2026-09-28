#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
CASES_PATH = ROOT / "evals" / "agent" / "grounded_queries_v1.json"

_DECISION_LAB = {
    "get_decision_value_added",
    "compare_plan_vs_hold",
    "get_decision_counterfactuals",
    "get_replay_evidence_quality",
    "get_similar_historical_episodes",
}
_DECISION_EVIDENCE = {"get_decision_evidence", "get_persisted_decision_evidence"}
_FORBIDDEN_MARKERS = ("execute_order", "place_order", "trade_execution", "broker_order", "write_sql")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Quantia grounded-agent eval prompts against the local runtime.")
    parser.add_argument("--owner-chat-id", type=int, required=True)
    parser.add_argument("--case-id", action="append", default=[])
    parser.add_argument("--max-steps", type=int, default=4)
    parser.add_argument("--output", type=Path, default=ROOT / "tmp" / "grounded-agent-evals.json")
    return parser.parse_args()


def _tools(result: dict[str, Any]) -> list[str]:
    names: list[str] = []
    for step in result.get("steps") or []:
        decision = step.get("decision") or {}
        name = str(decision.get("tool_name") or "")
        if name:
            names.append(name)
    return names


def _route_check(expected: str, tools: list[str], result: dict[str, Any]) -> tuple[bool, str]:
    toolset = set(tools)
    if any(any(marker in name.lower() for marker in _FORBIDDEN_MARKERS) for name in tools):
        return False, "forbidden action tool surfaced"
    if result.get("status") == "FAILED":
        return False, f"agent failed: {result.get('stop_reason')}"
    if expected == "canonical_decision_lab":
        return bool(toolset & _DECISION_LAB), "Decision Lab tool required"
    if expected == "exploratory_sql":
        return "query_quantia_sql" in toolset, "generic SQL explorer expected"
    if expected == "schema_then_sql":
        ok = "inspect_quantia_schema" in toolset and "query_quantia_sql" in toolset
        return ok, "schema inspection then SQL expected"
    if expected == "canonical_decision_evidence":
        return bool(toolset & _DECISION_EVIDENCE), "decision evidence tool expected"
    if expected == "refuse_write":
        return not any("write" in name.lower() or "order" in name.lower() for name in tools), "no write/order tool allowed"
    if expected == "point_in_time":
        return bool(toolset & ({"query_quantia_sql", "get_bot_directional_outcomes"} | _DECISION_LAB)), "PIT-capable evidence expected"
    return True, "manual/semantic evaluation"


def main() -> int:
    args = parse_args()
    cases = json.loads(CASES_PATH.read_text(encoding="utf-8"))
    selected = [case for case in cases if not args.case_id or case["id"] in set(args.case_id)]
    output: list[dict[str, Any]] = []
    failures = 0

    for case in selected:
        command = [
            sys.executable,
            str(ROOT / "scripts" / "run_agent.py"),
            "--goal",
            case["prompt"],
            "--owner-chat-id",
            str(args.owner_chat_id),
            "--max-steps",
            str(args.max_steps),
            "--timeout-seconds",
            "240",
            "--new-conversation",
            "--json",
            "--force",
        ]
        proc = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=300)
        try:
            result = json.loads(proc.stdout)
        except json.JSONDecodeError:
            result = {
                "status": "FAILED",
                "stop_reason": "invalid_eval_json",
                "stderr": proc.stderr[-4000:],
                "stdout": proc.stdout[-4000:],
                "steps": [],
            }
        tools = _tools(result)
        passed, note = _route_check(case["expected_source_class"], tools, result)
        failures += 0 if passed else 1
        output.append({
            "case_id": case["id"],
            "expected_source_class": case["expected_source_class"],
            "passed_route_check": passed,
            "route_note": note,
            "status": result.get("status"),
            "stop_reason": result.get("stop_reason"),
            "model": result.get("model"),
            "tools": tools,
            "answer": result.get("answer"),
            "run_id": result.get("run_id"),
        })
        marker = "PASS" if passed else "FAIL"
        print(f"[{marker}] {case['id']}: status={result.get('status')} tools={tools}")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Eval report: {args.output}")
    print(f"Cases: {len(output)} · route failures: {failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

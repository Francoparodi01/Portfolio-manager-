#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path

from src.agentic.harness.contracts import ConversationState
from src.agentic.harness.tasking import parse_task


ROOT = Path(__file__).resolve().parents[1]
DATASET = ROOT / "evals" / "conversations" / "conversational_harness.jsonl"


def main() -> int:
    total = 0
    failures: list[str] = []
    for line_no, raw in enumerate(DATASET.read_text(encoding="utf-8").splitlines(), start=1):
        if not raw.strip():
            continue
        total += 1
        case = json.loads(raw)
        state_data = case.get("state") or {}
        state = ConversationState(
            conversation_id=f"eval-{line_no}",
            owner_chat_id=123,
            active_symbols=state_data.get("active_symbols") or [],
            last_intent=state_data.get("last_intent"),
        )
        task = parse_task(case["message"], state, [])
        expected_intent = case.get("expected_intent")
        if expected_intent and task.intent != expected_intent:
            failures.append(f"line {line_no}: intent {task.intent!r} != {expected_intent!r}")
        for tool in case.get("required_tools") or []:
            if tool not in task.required_tools:
                failures.append(f"line {line_no}: missing required tool {tool!r} in {task.required_tools!r}")
        if "entities" in case and task.entities != case["entities"]:
            failures.append(f"line {line_no}: entities {task.entities!r} != {case['entities']!r}")
        if "horizons" in case and task.horizons != case["horizons"]:
            failures.append(f"line {line_no}: horizons {task.horizons!r} != {case['horizons']!r}")

    if failures:
        print(f"Conversational evals: {total} cases, {len(failures)} failures")
        for failure in failures:
            print("FAIL", failure)
        return 1
    print(f"Conversational evals: {total} cases, all passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import json

from src.agentic.answer import evidence_decision
from src.agentic.harness.context import ContextSelector
from src.agentic.harness.task import TaskParser


def test_portfolio_review_requires_full_analysis():
    task = TaskParser().parse("¿Cómo está mi cartera?")
    plan = ContextSelector().select(
        task,
        {"get_portfolio_snapshot", "get_decision_evidence", "analyze_portfolio"},
    )

    assert task.intent == "portfolio_review"
    assert plan.required_tools == [
        "get_portfolio_snapshot",
        "get_decision_evidence",
        "analyze_portfolio",
    ]


def test_decision_evidence_fallback_is_compact_not_raw_json():
    payload = {
        "schema_version": "agent-decision-evidence-v1",
        "analysis_run_id": "run-123",
        "evaluated_at": "2026-09-26T05:58:19+00:00",
        "snapshot_as_of": "2026-09-26T05:57:18+00:00",
        "signals": [
            {"ticker": "NVDA", "decision": "ACCUMULATE", "final_score": 0.1814},
            {"ticker": "AMD", "decision": "HOLD", "final_score": 0.0835},
        ],
    }
    history = [
        {
            "decision": {"tool": "get_decision_evidence"},
            "observation": {
                "tool": "get_decision_evidence",
                "ok": True,
                "content": json.dumps(payload),
            },
        }
    ]

    decision = evidence_decision("¿Cómo está mi cartera?", history)

    assert "NVDA ACCUMULATE (score 0,181)" in decision.answer
    assert "AMD HOLD (score 0,084)" in decision.answer
    assert '"schema_version"' not in decision.answer
    assert '"signals"' not in decision.answer
    assert "Extracto literal de la herramienta" not in decision.answer

from __future__ import annotations

import json

from src.agentic.answer import evidence_decision
from src.agentic.harness.context import ContextSelector
from src.agentic.harness.synthesis import GroundedSynthesizer
from src.agentic.harness.task import TaskParser


def test_portfolio_review_requires_snapshot_and_persisted_decisions_only():
    task = TaskParser().parse("¿Cómo está mi cartera?")
    plan = ContextSelector().select(
        task,
        {
            "get_portfolio_snapshot",
            "get_persisted_decision_evidence",
            "get_decision_evidence",
            "analyze_portfolio",
        },
    )

    assert task.intent == "portfolio_review"
    assert plan.required_tools == [
        "get_portfolio_snapshot",
        "get_persisted_decision_evidence",
    ]
    assert "get_decision_evidence" not in plan.allowed_tools
    assert "analyze_portfolio" not in plan.allowed_tools


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


def test_portfolio_fallback_uses_persisted_decisions_without_recompute():
    snapshot = {
        "scraped_at": "2026-09-26T15:02:00+00:00",
        "total_value_ars": 2895125.0,
        "cash_ars": 3842.34,
        "positions": [
            {"ticker": "NVDA", "weight": 0.168},
            {"ticker": "AMD", "weight": 0.141},
            {"ticker": "GDX", "weight": 0.130},
        ],
    }
    decisions = {
        "schema_version": "persisted-decision-evidence-v1",
        "evidence_source": "persisted_latest_run",
        "analysis_run_id": "run-persisted",
        "evaluated_at": "2026-09-26T15:00:00+00:00",
        "snapshot_as_of": "2026-09-26T14:59:00+00:00",
        "signals": [
            {"ticker": "NVDA", "decision": "ACCUMULATE", "status": "APPROVED", "final_score": 0.1814},
            {"ticker": "AMD", "decision": "HOLD", "status": "OBSERVED", "final_score": 0.0835},
            {"ticker": "GDX", "decision": "WATCH", "status": "BLOCKED", "final_score": 0.152},
        ],
    }
    history = []
    for tool, content in (
        ("get_portfolio_snapshot", json.dumps(snapshot)),
        ("get_persisted_decision_evidence", json.dumps(decisions)),
    ):
        history.append({
            "decision": {"tool": tool},
            "observation": {"tool": tool, "ok": True, "content": content},
        })

    decision = evidence_decision("¿Cómo está mi cartera?", history)

    assert decision.answer_origin == "portfolio_renderer_v5"
    assert "Tu cartera tiene" in decision.answer
    assert "NVDA 16,8%" in decision.answer
    assert "NVDA ACCUMULATE" in decision.answer
    assert "GDX WATCH (score 0,152) · bloqueada" in decision.answer
    assert "Último análisis formal" in decision.answer
    assert "no se recalcularon con ese snapshot nuevo" in decision.answer
    assert "no volvió a ejecutar el análisis completo" in decision.answer
    assert "no son fills" in decision.answer.lower()
    assert "Resumen: Revisé la evidencia" not in decision.answer
    assert "Extracto literal" not in decision.answer
    assert len(decision.answer) < 1800


def test_synthesis_accepts_json_and_plain_text_answers():
    assert GroundedSynthesizer._extract_answer('{"answer":"Respuesta breve"}') == "Respuesta breve"
    assert GroundedSynthesizer._extract_answer("Respuesta breve basada sólo en evidencia disponible.") == (
        "Respuesta breve basada sólo en evidencia disponible."
    )

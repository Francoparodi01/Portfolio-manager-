from __future__ import annotations

import json

from src.agentic.answer import evidence_decision
from src.agentic.harness.context import ContextSelector
from src.agentic.harness.synthesis import GroundedSynthesizer
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


def test_full_portfolio_fallback_is_conversational_and_short():
    snapshot = {
        "total_value_ars": 2895125.0,
        "cash_ars": 3842.34,
        "positions": [
            {"ticker": "NVDA", "weight": 0.168},
            {"ticker": "AMD", "weight": 0.141},
            {"ticker": "GDX", "weight": 0.130},
        ],
    }
    decisions = {
        "signals": [
            {"ticker": "NVDA", "decision": "ACCUMULATE", "final_score": 0.1814},
            {"ticker": "AMD", "decision": "HOLD", "final_score": 0.0835},
            {"ticker": "GDX", "decision": "ACCUMULATE", "final_score": 0.152},
        ],
    }
    analysis = """🧠 ANÁLISIS — 26/09 03:12 ART
⚠️ Fuera de rueda — validar apertura, no perseguir gaps.
━━━ SIMULACIÓN CONTEXTUAL ━━━
🔴 REVALIDAR SELL YPFD -$67.520 ARS score -0.092
🟢 REVALIDAR BUY NVDA +$60.800 ARS score +0.181
"""
    history = []
    for tool, content in (
        ("get_portfolio_snapshot", json.dumps(snapshot)),
        ("get_decision_evidence", json.dumps(decisions)),
        ("analyze_portfolio", analysis),
    ):
        history.append({
            "decision": {"tool": tool},
            "observation": {"tool": tool, "ok": True, "content": content},
        })

    decision = evidence_decision("¿Cómo está mi cartera?", history)

    assert decision.answer_origin == "portfolio_renderer_v2"
    assert "Tu cartera tiene" in decision.answer
    assert "NVDA 16,8%" in decision.answer
    assert "NVDA ACCUMULATE" in decision.answer
    assert "REVALIDAR SELL YPFD" in decision.answer
    assert "fuera de rueda" in decision.answer.lower()
    assert "Resumen: Revisé la evidencia" not in decision.answer
    assert "Extracto literal" not in decision.answer
    assert len(decision.answer) < 1500


def test_synthesis_accepts_json_and_plain_text_answers():
    assert GroundedSynthesizer._extract_answer('{"answer":"Respuesta breve"}') == "Respuesta breve"
    assert GroundedSynthesizer._extract_answer("Respuesta breve basada sólo en evidencia disponible.") == (
        "Respuesta breve basada sólo en evidencia disponible."
    )

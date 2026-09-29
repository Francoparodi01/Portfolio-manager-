from __future__ import annotations

import asyncio
import json

from src.agentic.contracts import ToolSpec
from src.agentic.grounded_model import GroundedQuantiaAgentModel


def _tool(name: str) -> ToolSpec:
    return ToolSpec(
        name=name,
        description="fixture",
        input_schema={"type": "object", "properties": {}, "additionalProperties": False},
    )


def test_portfolio_question_bootstraps_canonical_evidence_before_llm(monkeypatch):
    model = GroundedQuantiaAgentModel(model="fixture", project_context="fixture")

    async def should_not_call_model(_payload):
        raise AssertionError("LLM must not run before canonical portfolio evidence is gathered")

    monkeypatch.setattr(model, "_call", should_not_call_model)
    tools = [_tool("get_portfolio_snapshot"), _tool("get_decision_evidence")]
    goal = "¿Cuál es hoy la decisión más importante de mi cartera y por qué?"

    first = asyncio.run(model.decide(
        goal=goal,
        tools=tools,
        history=[],
        step_no=1,
        max_steps=8,
    ))
    assert first.kind == "tool"
    assert first.tool_name == "get_portfolio_snapshot"

    history = [{
        "decision": {"kind": "tool", "tool": "get_portfolio_snapshot", "arguments": {}},
        "observation": {"tool_name": "get_portfolio_snapshot", "ok": True, "content": "{}"},
    }]
    second = asyncio.run(model.decide(
        goal=goal,
        tools=tools,
        history=history,
        step_no=2,
        max_steps=8,
    ))
    assert second.kind == "tool"
    assert second.tool_name == "get_decision_evidence"


def test_meta_policy_explanation_bootstraps_decision_evidence(monkeypatch):
    model = GroundedQuantiaAgentModel(model="fixture", project_context="fixture")

    async def should_not_call_model(_payload):
        raise AssertionError("LLM must not run before decision evidence is gathered")

    monkeypatch.setattr(model, "_call", should_not_call_model)
    decision = asyncio.run(model.decide(
        goal="¿Por qué Meta Policy está bloqueando esta decisión?",
        tools=[_tool("get_decision_evidence")],
        history=[],
        step_no=1,
        max_steps=8,
    ))
    assert decision.kind == "tool"
    assert decision.tool_name == "get_decision_evidence"


def test_grounded_controller_disables_thinking_for_json_control_output(monkeypatch):
    model = GroundedQuantiaAgentModel(model="fixture", project_context="fixture")
    captured = {}

    async def fake_call(payload):
        captured.update(payload)
        return json.dumps({
            "kind": "final",
            "answer": "La evidencia observada alcanza para una síntesis breve.",
            "rationale": "Hay una fuente auditada.",
        })

    monkeypatch.setattr(model, "_call", fake_call)
    history = [{
        "decision": {"kind": "tool", "tool": "query_quantia_sql", "arguments": {}},
        "observation": {"tool_name": "query_quantia_sql", "ok": True, "content": "{}"},
    }]
    decision = asyncio.run(model.decide(
        goal="Resumí la evidencia observada.",
        tools=[],
        history=history,
        step_no=2,
        max_steps=8,
    ))
    assert decision.kind == "final"
    assert captured["think"] is False

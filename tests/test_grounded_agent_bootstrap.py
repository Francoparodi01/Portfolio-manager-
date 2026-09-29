from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

from src.agentic.contracts import AgentDecision, AgentTraceStep, ToolObservation, ToolSpec
from src.agentic.grounded_model import GroundedQuantiaAgentModel
from src.agentic.orchestrator import AgentOrchestrator, _resolved_subject_from_steps
from src.agentic.tools import ToolRegistry


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


def test_portfolio_question_closes_after_two_canonical_tools_without_llm(monkeypatch):
    model = GroundedQuantiaAgentModel(model="fixture", project_context="fixture")

    async def should_not_call_model(_payload):
        raise AssertionError("canonical portfolio close must not invoke the LLM")

    monkeypatch.setattr(model, "_call", should_not_call_model)
    tools = [_tool("get_portfolio_snapshot"), _tool("get_decision_evidence")]
    goal = "¿Cuál es hoy la decisión más importante de mi cartera y por qué?"
    history = [
        {
            "decision": {"kind": "tool", "tool": "get_portfolio_snapshot", "arguments": {}},
            "observation": {
                "tool_name": "get_portfolio_snapshot",
                "ok": True,
                "content": json.dumps({
                    "total_value_ars": 2_820_635,
                    "cash_ars": 3_845.79,
                    "positions": [{"ticker": "GDX", "weight": 0.126}],
                }),
            },
        },
        {
            "decision": {"kind": "tool", "tool": "get_decision_evidence", "arguments": {}},
            "observation": {
                "tool_name": "get_decision_evidence",
                "ok": True,
                "content": json.dumps({
                    "plan": {
                        "decisions": [
                            {
                                "ticker": "GDX",
                                "action": "BUY",
                                "current_weight": 0.126,
                                "target_weight": 0.35,
                                "reason_primary": "Aumentar posición",
                                "reason_secondary": "score +0.085",
                            }
                        ],
                        "buy_orders": [
                            {
                                "ticker": "GDX",
                                "action": "BUY",
                                "amount_ars": 399_000,
                                "theoretical_ars": 632_536,
                            }
                        ],
                        "sell_orders": [
                            {
                                "ticker": "IREN",
                                "action": "SELL_PARTIAL",
                                "amount_ars": 260_360,
                                "theoretical_ars": 260_472,
                            }
                        ],
                        "blocked_orders": [
                            {"ticker": "NVDA", "reason": "BUY_SCORE_GUARD"}
                        ],
                    }
                }),
            },
        },
    ]

    final = asyncio.run(model.decide(
        goal=goal,
        tools=tools,
        history=history,
        step_no=3,
        max_steps=8,
    ))
    assert final.kind == "final"
    assert final.answer_origin == "portfolio_priority_renderer_v1"
    assert final.objective_status == "EXPLAINED"
    assert "GDX BUY" in final.answer
    assert "$399.000" in final.answer


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


def test_referential_meta_policy_uses_prior_resolved_subject_and_closes(monkeypatch):
    model = GroundedQuantiaAgentModel(
        model="fixture",
        project_context="fixture",
        conversation_context=[{
            "run_id": "prior",
            "goal": "¿Cuál es hoy la decisión más importante de mi cartera y por qué?",
            "resolved_subject": "GDX",
        }],
    )

    async def should_not_call_model(_payload):
        raise AssertionError("referential Meta Policy follow-up must stay deterministic")

    monkeypatch.setattr(model, "_call", should_not_call_model)
    tools = [_tool("get_decision_evidence"), _tool("get_meta_policy")]
    goal = "¿Por qué Meta Policy está bloqueando esta decisión?"

    first = asyncio.run(model.decide(
        goal=goal,
        tools=tools,
        history=[],
        step_no=1,
        max_steps=8,
    ))
    assert first.tool_name == "get_decision_evidence"

    history = [{
        "decision": {"kind": "tool", "tool": "get_decision_evidence", "arguments": {}},
        "observation": {
            "tool_name": "get_decision_evidence",
            "ok": True,
            "content": json.dumps({
                "plan": {"decisions": [{
                    "ticker": "GDX",
                    "action": "BUY",
                    "reason_primary": "Aumentar posición",
                    "reason_secondary": "score +0.088",
                }]}
            }),
        },
    }]
    second = asyncio.run(model.decide(
        goal=goal,
        tools=tools,
        history=history,
        step_no=2,
        max_steps=8,
    ))
    assert second.kind == "tool"
    assert second.tool_name == "get_meta_policy"
    assert second.arguments == {"ticker": "GDX"}

    history.append({
        "decision": {"kind": "tool", "tool": "get_meta_policy", "arguments": {"ticker": "GDX"}},
        "observation": {
            "tool_name": "get_meta_policy",
            "ok": True,
            "content": json.dumps({
                "source": "economic_meta_policy",
                "mode": "SHADOW",
                "capital_effect": "NO",
                "ticker": "GDX",
                "report": "Economic Meta Policy v1 · SHADOW_ONLY · Capital effect: NO",
            }),
        },
    })
    final = asyncio.run(model.decide(
        goal=goal,
        tools=tools,
        history=history,
        step_no=3,
        max_steps=8,
    ))
    assert final.kind == "final"
    assert final.answer_origin == "meta_policy_followup_renderer_v1"
    assert "Retomo GDX" in final.answer
    assert "capital_effect=NO" in final.answer
    assert "no modifica capital" in final.answer


def test_priority_run_resolves_subject_from_structured_observation():
    evidence = json.dumps({
        "plan": {
            "buy_orders": [{"ticker": "GDX", "amount_ars": 627_000}],
            "sell_orders": [{"ticker": "MU", "amount_ars": 341_250}],
        }
    })
    steps = [
        AgentTraceStep(
            step_no=1,
            decision=AgentDecision(kind="tool", tool_name="get_decision_evidence"),
            observation=ToolObservation("get_decision_evidence", {}, True, evidence),
        ),
        AgentTraceStep(
            step_no=2,
            decision=AgentDecision(
                kind="final",
                answer="GDX es la principal decisión operativa.",
                answer_origin="portfolio_priority_renderer_v1",
                objective_status="EXPLAINED",
            ),
        ),
    ]
    assert _resolved_subject_from_steps(steps) == "GDX"


def test_recent_context_exposes_only_structured_subject_not_assistant_answer(monkeypatch):
    from src.agentic import persistence

    now = datetime(2026, 9, 29, tzinfo=timezone.utc)
    conn = SimpleNamespace(
        fetch=AsyncMock(return_value=[{
            "id": "prior",
            "goal": "¿Cuál es la decisión más importante?",
            "started_at": now,
            "conversation_id": "session",
            "resolved_subject": "GDX",
        }]),
        close=AsyncMock(),
    )
    monkeypatch.setattr(persistence, "connect_read_only", AsyncMock(return_value=conn))
    context = asyncio.run(
        persistence.AgentRunStore("postgresql://fixture").recent_context(123, as_of=now)
    )
    sql = conn.fetch.call_args.args[0]
    assert context[0]["resolved_subject"] == "GDX"
    assert "resolved_subject" in sql
    assert "final_answer" not in sql
    conn.close.assert_awaited_once()


def test_capability_question_is_runtime_metadata_not_market_evidence(monkeypatch):
    model = GroundedQuantiaAgentModel(model="fixture", project_context="fixture")

    async def should_not_call_model(_payload):
        raise AssertionError("capability inventory must come from the runtime registry")

    monkeypatch.setattr(model, "_call", should_not_call_model)
    decision = asyncio.run(model.decide(
        goal="Decime qué herramientas tenés disponibles.",
        tools=[_tool("get_portfolio_snapshot"), _tool("get_decision_evidence")],
        history=[],
        step_no=1,
        max_steps=8,
    ))
    assert decision.kind == "final"
    assert decision.answer_origin == "runtime_capabilities_v1"
    assert "get_portfolio_snapshot" in decision.answer
    assert "get_decision_evidence" in decision.answer


def test_orchestrator_allows_runtime_capability_final_without_tool_observation(monkeypatch):
    model = GroundedQuantiaAgentModel(model="fixture", project_context="fixture")

    async def should_not_call_model(_payload):
        raise AssertionError("capability inventory must not invoke the LLM")

    monkeypatch.setattr(model, "_call", should_not_call_model)
    registry = ToolRegistry()

    async def should_not_execute(_arguments):
        raise AssertionError("capability inventory must not execute a data tool")

    registry.register(_tool("get_portfolio_snapshot"), should_not_execute)
    result = asyncio.run(AgentOrchestrator(
        model=model,
        registry=registry,
        store=None,
        require_audit=False,
    ).run(goal="Decime qué herramientas tenés disponibles."))

    assert result.status == "COMPLETE"
    assert result.stop_reason == "model_final"
    assert len(result.steps) == 1
    assert result.steps[0].observation is None
    assert result.steps[0].decision.answer_origin == "runtime_capabilities_v1"


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

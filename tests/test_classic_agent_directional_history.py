import json

import pytest

from src.agentic.classic_model import ClassicAgentModel
from src.agentic.contracts import ToolSpec


GOAL = "¿Qué decisiones del bot tuvieron mejor resultado a 20D en los últimos 180 días? Separá BUY y SELL."


def _tool_spec() -> ToolSpec:
    return ToolSpec(
        name="get_bot_directional_outcomes",
        description="fixture",
        input_schema={
            "type": "object",
            "properties": {
                "days": {"type": "integer"},
                "horizon": {"type": "integer"},
                "cost_bps": {"type": "integer"},
            },
            "additionalProperties": False,
        },
    )


@pytest.mark.asyncio
async def test_directional_history_routes_without_calling_llm(monkeypatch):
    model = ClassicAgentModel(model="fixture-model")

    async def fail_call(_payload):
        raise AssertionError("deterministic directional history must not call Ollama")

    monkeypatch.setattr(model, "_call", fail_call)
    decision = await model.decide(
        goal=GOAL,
        tools=[_tool_spec()],
        history=[],
        step_no=1,
        max_steps=4,
    )

    assert decision.kind == "tool"
    assert decision.tool_name == "get_bot_directional_outcomes"
    assert decision.arguments == {"days": 180, "horizon": 20, "cost_bps": 150}


@pytest.mark.asyncio
async def test_directional_history_closes_from_tool_evidence_without_llm(monkeypatch):
    model = ClassicAgentModel(model="fixture-model")

    async def fail_call(_payload):
        raise AssertionError("deterministic close must not call Ollama")

    monkeypatch.setattr(model, "_call", fail_call)
    payload = {
        "schema_version": "bot-directional-history-v1",
        "status": "observed",
        "lookback_days": 180,
        "horizon_days": 20,
        "cost_bps": 150,
        "sides": {
            "BUY": {
                "n_episodes": 12,
                "win_rate_net": 0.40,
                "mean_gross_return": -0.02,
                "mean_net_return": -0.035,
                "median_net_return": -0.03,
                "profit_factor_net": 0.70,
                "quality": "LOW",
            },
            "SELL": {
                "n_episodes": 18,
                "win_rate_net": 0.72,
                "mean_gross_return": 0.08,
                "mean_net_return": 0.065,
                "median_net_return": 0.05,
                "profit_factor_net": 2.1,
                "quality": "MEDIUM",
            },
        },
    }
    history = [
        {
            "decision": {"tool": "get_bot_directional_outcomes"},
            "observation": {
                "tool_name": "get_bot_directional_outcomes",
                "ok": True,
                "content": json.dumps(payload),
            },
        }
    ]

    decision = await model.decide(
        goal=GOAL,
        tools=[_tool_spec()],
        history=history,
        step_no=2,
        max_steps=4,
    )

    assert decision.kind == "final"
    assert decision.answer_origin == "classic_deterministic"
    assert "BUY: n=12" in decision.answer
    assert "SELL: n=18" in decision.answer
    assert "EV neto +6.50%" in decision.answer
    assert "no PnL realizado" in decision.answer
    assert "no es DVA contra HOLD" in decision.answer


@pytest.mark.asyncio
async def test_unrelated_goal_still_delegates_to_parent_model(monkeypatch):
    model = ClassicAgentModel(model="fixture-model")
    called = False

    async def fake_call(_payload):
        nonlocal called
        called = True
        return json.dumps({"kind": "tool", "tool": "some_tool", "arguments": {}, "rationale": "fixture"})

    monkeypatch.setattr(model, "_call", fake_call)
    tool = ToolSpec(
        name="some_tool",
        description="fixture",
        input_schema={"type": "object", "properties": {}, "additionalProperties": False},
    )
    decision = await model.decide(
        goal="Dame contexto macro actual",
        tools=[tool],
        history=[],
        step_no=1,
        max_steps=4,
    )

    assert called
    assert decision.kind == "tool"
    assert decision.tool_name == "some_tool"

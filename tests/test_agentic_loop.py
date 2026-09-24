from __future__ import annotations

import asyncio

from src.agentic.contracts import AgentDecision, ToolObservation, ToolSpec
from src.agentic.orchestrator import AgentOrchestrator
from src.agentic.tools import ToolRegistry


class FakeModel:
    name = "fake-model"

    def __init__(self, decisions):
        self.decisions = list(decisions)
        self.calls = []

    async def decide(self, **kwargs):
        self.calls.append(kwargs)
        return self.decisions.pop(0)


def run(coro):
    return asyncio.run(coro)


def test_agent_loop_observes_tool_then_finishes():
    registry = ToolRegistry()
    calls = []

    async def handler(arguments):
        calls.append(arguments)
        return ToolObservation(
            tool_name="portfolio",
            arguments=arguments,
            ok=True,
            content='{"cash_ars": 1000}',
        )

    registry.register(
        ToolSpec(
            name="portfolio",
            description="read portfolio",
            input_schema={
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
        ),
        handler,
    )

    model = FakeModel(
        [
            AgentDecision(kind="tool", tool_name="portfolio", arguments={}, rationale="need state"),
            AgentDecision(kind="final", answer="Sin acción material.", rationale="enough evidence"),
        ]
    )
    agent = AgentOrchestrator(
        model=model,
        registry=registry,
        store=None,
        max_steps=4,
        require_audit=False,
    )

    result = run(agent.run(goal="Evaluar cartera"))

    assert result.status == "COMPLETE"
    assert result.stop_reason == "model_final"
    assert result.answer == "Sin acción material."
    assert len(result.steps) == 2
    assert calls == [{}]
    assert model.calls[1]["history"][0]["observation"]["content"] == '{"cash_ars": 1000}'


def test_agent_blocks_identical_tool_loop():
    registry = ToolRegistry()
    executed = 0

    async def handler(arguments):
        nonlocal executed
        executed += 1
        return ToolObservation(
            tool_name="portfolio",
            arguments=arguments,
            ok=True,
            content="ok",
        )

    registry.register(
        ToolSpec(
            name="portfolio",
            description="read portfolio",
            input_schema={
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
        ),
        handler,
    )

    model = FakeModel(
        [
            AgentDecision(kind="tool", tool_name="portfolio", arguments={}),
            AgentDecision(kind="tool", tool_name="portfolio", arguments={}),
            AgentDecision(kind="final", answer="done"),
        ]
    )
    agent = AgentOrchestrator(
        model=model,
        registry=registry,
        store=None,
        max_steps=4,
        max_identical_calls=1,
        require_audit=False,
    )

    result = run(agent.run(goal="test loop guard"))

    assert result.status == "COMPLETE"
    assert executed == 1
    assert result.steps[1].observation is not None
    assert "repeated identical tool call blocked" in (result.steps[1].observation.error or "")


def test_registry_rejects_write_capable_tool():
    registry = ToolRegistry()

    async def handler(arguments):
        return ToolObservation(tool_name="trade", arguments=arguments, ok=True, content="")

    try:
        registry.register(
            ToolSpec(
                name="trade",
                description="unsafe",
                input_schema={"type": "object", "properties": {}},
                read_only=False,
            ),
            handler,
        )
    except ValueError as exc:
        assert "read-only" in str(exc)
    else:
        raise AssertionError("write-capable tool should have been rejected")


def test_max_steps_forces_final_answer():
    registry = ToolRegistry()

    async def handler(arguments):
        return ToolObservation(
            tool_name="portfolio",
            arguments=arguments,
            ok=True,
            content="same evidence",
        )

    registry.register(
        ToolSpec(
            name="portfolio",
            description="read portfolio",
            input_schema={
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
        ),
        handler,
    )

    model = FakeModel(
        [
            AgentDecision(kind="tool", tool_name="portfolio", arguments={}),
            AgentDecision(kind="final", answer="forced summary"),
        ]
    )
    agent = AgentOrchestrator(
        model=model,
        registry=registry,
        store=None,
        max_steps=1,
        require_audit=False,
    )

    result = run(agent.run(goal="bounded goal"))

    assert result.status == "LIMIT_REACHED"
    assert result.stop_reason == "max_steps"
    assert result.answer == "forced summary"
    assert model.calls[-1]["force_final"] is True
    assert model.calls[-1]["step_no"] == 2  # one consultation, then the separate close

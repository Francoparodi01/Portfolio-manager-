from __future__ import annotations

import asyncio

import requests

from src.agentic.contracts import AgentDecision, ToolSpec
from src.agentic.jev import (
    JevConfig,
    JevConfigError,
    JevRouter,
    JevRoutingResult,
    RoutedAgentModel,
)


def run(coro):
    return asyncio.run(coro)


class FakeBaseModel:
    name = "base"

    def __init__(self, decisions=None):
        self.decisions = list(decisions or [AgentDecision(kind="final", answer="base")])
        self.calls = []

    async def decide(self, **kwargs):
        self.calls.append(kwargs)
        return self.decisions.pop(0)


class FakeRouter:
    def __init__(self, result):
        self.result = result
        self.calls = []

    async def route(self, **kwargs):
        self.calls.append(kwargs)
        return self.result


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def json(self):
        return self.payload


def noarg_tool(name="portfolio"):
    return ToolSpec(
        name=name,
        description="read-only evidence",
        input_schema={"type": "object", "properties": {}, "additionalProperties": False},
    )


def arg_tool():
    return ToolSpec(
        name="ticker",
        description="ticker evidence",
        input_schema={
            "type": "object",
            "properties": {"ticker": {"type": "string"}},
            "required": ["ticker"],
            "additionalProperties": False,
        },
    )


def kwargs(tools):
    return {
        "goal": "evaluate",
        "tools": tools,
        "history": [],
        "step_no": 1,
        "max_steps": 4,
        "force_final": False,
    }


def test_off_uses_baseline_only():
    base = FakeBaseModel([AgentDecision(kind="final", answer="base")])
    model = RoutedAgentModel(base_model=base, mode="off")
    result = run(model.decide(**kwargs([noarg_tool()])))
    assert result.answer == "base"
    assert len(base.calls) == 1


def test_shadow_never_controls_execution():
    base = FakeBaseModel([AgentDecision(kind="tool", tool_name="other", arguments={})])
    router = FakeRouter(JevRoutingResult(choice="tool__portfolio", confidence=0.99))
    model = RoutedAgentModel(base_model=base, mode="shadow", router=router)
    result = run(model.decide(**kwargs([noarg_tool(), noarg_tool("other")])))
    assert result.tool_name == "other"
    assert result.routing["applied"] is False
    assert result.routing["fallback_reason"] == "shadow_mode"


def test_active_high_confidence_zero_arg_tool_bypasses_llm():
    base = FakeBaseModel()
    router = FakeRouter(
        JevRoutingResult(
            choice="tool__portfolio",
            confidence=0.94,
            probabilities={"tool__portfolio": 0.94, "final": 0.06},
            model="jev-test",
        )
    )
    model = RoutedAgentModel(
        base_model=base,
        mode="active",
        router=router,
        confidence_threshold=0.85,
    )
    result = run(model.decide(**kwargs([noarg_tool()])))
    assert result.kind == "tool"
    assert result.tool_name == "portfolio"
    assert result.arguments == {}
    assert result.routing["action"] == "direct_read_only_tool"
    assert base.calls == []


def test_active_low_confidence_falls_back_to_llm():
    base = FakeBaseModel([AgentDecision(kind="final", answer="fallback")])
    router = FakeRouter(JevRoutingResult(choice="tool__portfolio", confidence=0.60))
    model = RoutedAgentModel(
        base_model=base,
        mode="active",
        router=router,
        confidence_threshold=0.85,
    )
    result = run(model.decide(**kwargs([noarg_tool()])))
    assert result.answer == "fallback"
    assert result.routing["fallback_reason"] == "low_confidence"
    assert len(base.calls) == 1


def test_active_dynamic_arguments_delegate_to_llm():
    base = FakeBaseModel([AgentDecision(kind="tool", tool_name="ticker", arguments={"ticker": "AAPL"})])
    router = FakeRouter(JevRoutingResult(choice="tool__ticker", confidence=0.98))
    model = RoutedAgentModel(
        base_model=base,
        mode="active",
        router=router,
        confidence_threshold=0.85,
    )
    result = run(model.decide(**kwargs([arg_tool()])))
    assert result.arguments == {"ticker": "AAPL"}
    assert result.routing["fallback_reason"] == "dynamic_arguments_required"


def test_active_final_forces_llm_synthesis():
    base = FakeBaseModel([AgentDecision(kind="final", answer="synthesized")])
    router = FakeRouter(JevRoutingResult(choice="final", confidence=0.97))
    model = RoutedAgentModel(
        base_model=base,
        mode="active",
        router=router,
        confidence_threshold=0.85,
    )
    result = run(model.decide(**kwargs([noarg_tool()])))
    assert result.answer == "synthesized"
    assert base.calls[0]["force_final"] is True
    assert result.routing["action"] == "force_final"


def test_provider_error_falls_back_to_llm():
    base = FakeBaseModel([AgentDecision(kind="final", answer="safe fallback")])
    router = FakeRouter(JevRoutingResult(error="Timeout: provider unavailable"))
    model = RoutedAgentModel(
        base_model=base,
        mode="active",
        router=router,
        confidence_threshold=0.85,
    )
    result = run(model.decide(**kwargs([noarg_tool()])))
    assert result.answer == "safe fallback"
    assert result.routing["fallback_reason"] == "provider_error"


def test_router_parses_typesafe_choice_response():
    def request_fn(*args, **kwargs):
        return FakeResponse(
            {
                "model": "jev-2026-09-15",
                "answers": {
                    "next_action": {
                        "type": "choice",
                        "choice": "tool__portfolio",
                        "confidence": 0.91,
                        "probabilities": {
                            "tool__portfolio": 0.91,
                            "final": 0.05,
                            "delegate_llm": 0.04,
                        },
                    }
                },
                "usage": {"input_tokens": 10, "output_tokens": 3},
            }
        )

    router = JevRouter(JevConfig(api_key="test"), request_fn=request_fn)
    result = run(router.route(goal="g", tools=[noarg_tool()], history=[], step_no=1, max_steps=4))
    assert result.error is None
    assert result.choice == "tool__portfolio"
    assert result.confidence == 0.91
    assert result.usage["input_tokens"] == 10


def test_router_malformed_response_returns_error():
    router = JevRouter(
        JevConfig(api_key="test", max_retries=0),
        request_fn=lambda *a, **k: FakeResponse({"answers": {}}),
    )
    result = run(router.route(goal="g", tools=[noarg_tool()], history=[], step_no=1, max_steps=4))
    assert result.choice is None
    assert "JevResponseError" in (result.error or "")


def test_router_retries_timeout_then_succeeds():
    calls = {"n": 0}

    def request_fn(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise requests.Timeout("slow")
        return FakeResponse(
            {
                "model": "jev-latest",
                "answers": {
                    "next_action": {
                        "type": "choice",
                        "choice": "final",
                        "confidence": 0.9,
                        "probabilities": {"final": 0.9, "delegate_llm": 0.1},
                    }
                },
                "usage": {"input_tokens": 1, "output_tokens": 1},
            }
        )

    router = JevRouter(
        JevConfig(api_key="test", max_retries=1, retry_backoff_ms=0),
        request_fn=request_fn,
    )
    result = run(router.route(goal="g", tools=[], history=[], step_no=1, max_steps=4))
    assert result.error is None
    assert result.attempts == 2
    assert calls["n"] == 2


def test_config_requires_api_key_when_enabled(monkeypatch):
    monkeypatch.delenv("QUANTIA_JEV_API_KEY", raising=False)
    try:
        JevConfig.from_env(mode="active")
    except JevConfigError as exc:
        assert "QUANTIA_JEV_API_KEY" in str(exc)
    else:
        raise AssertionError("active Jev router must require an API key")

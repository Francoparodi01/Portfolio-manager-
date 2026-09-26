from __future__ import annotations

import asyncio
import json

from src.agentic.contracts import ToolSpec
from src.agentic.conversation.gateway import (
    _BACKGROUND_REFRESH_TASKS,
    _explicit_refresh_request,
    _schedule_background_refresh,
)
from src.agentic.harness.context import ContextSelector
from src.agentic.harness.schemas import TaskSpec
from src.agentic.harness.synthesis import GroundedSynthesizer
from src.agentic.harness.task import TaskParser
from src.agentic.model import OllamaAgentModel


def test_portfolio_review_runs_db_bound_evidence_in_parallel():
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
    assert [
        "get_portfolio_snapshot",
        "get_persisted_decision_evidence",
    ] in plan.parallel_groups
    assert "get_decision_evidence" not in plan.allowed_tools
    assert "analyze_portfolio" not in plan.allowed_tools


def test_explicit_refresh_language_bypasses_snapshot_fast_path():
    assert _explicit_refresh_request("Actualizá mi cartera ahora mismo")
    assert _explicit_refresh_request("Refresca los datos")
    assert not _explicit_refresh_request("¿Cómo está mi cartera?")


def test_background_refresh_is_deduplicated_per_owner():
    async def scenario():
        _BACKGROUND_REFRESH_TASKS.clear()
        entered = asyncio.Event()
        release = asyncio.Event()
        calls = 0

        async def refresh() -> str:
            nonlocal calls
            calls += 1
            entered.set()
            await release.wait()
            return ""

        assert _schedule_background_refresh(123, refresh)
        await entered.wait()
        assert not _schedule_background_refresh(123, refresh)
        assert calls == 1

        task = _BACKGROUND_REFRESH_TASKS[123]
        release.set()
        await task
        await asyncio.sleep(0)
        assert 123 not in _BACKGROUND_REFRESH_TASKS

    asyncio.run(scenario())


def test_known_bounded_intent_bypasses_ollama_planner(monkeypatch):
    model = OllamaAgentModel(model="fixture-model")

    async def should_not_run(_payload):
        raise AssertionError("Ollama planner should be bypassed for portfolio_review")

    monkeypatch.setattr(model, "_call", should_not_run)
    tools = [
        ToolSpec(
            name="get_portfolio_snapshot",
            description="fixture",
            input_schema={"type": "object", "properties": {}, "additionalProperties": False},
            capability="READ",
        )
    ]
    history = [
        {
            "decision": {"kind": "tool", "tool": "get_portfolio_snapshot", "arguments": {}},
            "observation": {
                "tool": "get_portfolio_snapshot",
                "ok": True,
                "content": json.dumps(
                    {
                        "scraped_at": "2026-09-26T12:00:00+00:00",
                        "total_value_ars": 1000,
                        "cash_ars": 100,
                        "positions": [],
                    }
                ),
            },
        }
    ]

    decision = asyncio.run(
        model.decide(
            goal="¿Cómo está mi cartera?",
            tools=tools,
            history=history,
            step_no=1,
            max_steps=5,
            force_final=False,
        )
    )

    assert decision.kind == "final"
    assert "1.000,00 ARS" in decision.answer


def test_synthesis_defaults_are_bounded_for_chat_latency(monkeypatch):
    monkeypatch.delenv("QUANTIA_HARNESS_SYNTHESIS_EVIDENCE_CHARS", raising=False)
    monkeypatch.delenv("QUANTIA_SYNTHESIS_CONTEXT_TOKENS", raising=False)
    monkeypatch.delenv("QUANTIA_SYNTHESIS_NUM_PREDICT", raising=False)
    monkeypatch.delenv("QUANTIA_OLLAMA_KEEP_ALIVE", raising=False)

    synthesizer = GroundedSynthesizer(model="fixture-model")

    assert synthesizer.max_chars == 8000
    assert synthesizer.context_tokens == 8192
    assert synthesizer.num_predict == 400
    assert synthesizer.keep_alive == "30m"


def test_portfolio_review_bypasses_llm_synthesis(monkeypatch):
    monkeypatch.delenv("QUANTIA_HARNESS_SYNTHESIS_BYPASS_INTENTS", raising=False)
    synthesizer = GroundedSynthesizer(model="fixture-model")

    async def should_not_post(*_args, **_kwargs):
        raise AssertionError("portfolio_review must not call Ollama synthesis")

    monkeypatch.setattr("httpx.AsyncClient.post", should_not_post)
    task = TaskSpec(intent="portfolio_review", raw_message="¿Cómo está mi cartera?")
    answer = asyncio.run(
        synthesizer.synthesize(task=task, evidence=[], fallback="respuesta determinística")
    )

    assert answer == "respuesta determinística"

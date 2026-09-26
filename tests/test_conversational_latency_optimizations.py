from __future__ import annotations

import asyncio
import json

from src.agentic.conversation.gateway import (
    _BACKGROUND_REFRESH_TASKS,
    _explicit_refresh_request,
    _schedule_background_refresh,
)
from src.agentic.harness.context import ContextSelector
from src.agentic.harness.runtime import _DIRECT_AFTER_REQUIRED
from src.agentic.harness.schemas import TaskSpec
from src.agentic.harness.synthesis import GroundedSynthesizer
from src.agentic.harness.task import TaskParser


class _SynthesisResponse:
    def raise_for_status(self):
        return None

    def json(self):
        return {"message": {"content": json.dumps({"answer": "Respuesta conversacional compacta."})}}


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


def test_known_bounded_intents_bypass_planner_only_inside_conversational_harness():
    # The latency optimization belongs at the conversational orchestration
    # boundary. OllamaAgentModel remains generic so legacy/research orchestrators
    # keep their original planner semantics.
    assert "portfolio_review" in _DIRECT_AFTER_REQUIRED
    assert "bot_follow_pnl" in _DIRECT_AFTER_REQUIRED
    assert "performance" in _DIRECT_AFTER_REQUIRED
    assert "system_status" in _DIRECT_AFTER_REQUIRED


def test_synthesis_defaults_are_bounded_for_chat_latency(monkeypatch):
    monkeypatch.delenv("QUANTIA_HARNESS_SYNTHESIS_EVIDENCE_CHARS", raising=False)
    monkeypatch.delenv("QUANTIA_SYNTHESIS_CONTEXT_TOKENS", raising=False)
    monkeypatch.delenv("QUANTIA_SYNTHESIS_NUM_PREDICT", raising=False)
    monkeypatch.delenv("QUANTIA_OLLAMA_KEEP_ALIVE", raising=False)

    synthesizer = GroundedSynthesizer(model="fixture-model")

    assert synthesizer.max_chars == 3500
    assert synthesizer.context_tokens == 4096
    assert synthesizer.num_predict == 220
    assert synthesizer.keep_alive == "30m"


def test_portfolio_review_uses_compact_llm_synthesis_by_default(monkeypatch):
    monkeypatch.delenv("QUANTIA_HARNESS_SYNTHESIS_BYPASS_INTENTS", raising=False)
    synthesizer = GroundedSynthesizer(model="fixture-model")
    called = 0

    async def fake_post(*_args, **_kwargs):
        nonlocal called
        called += 1
        return _SynthesisResponse()

    monkeypatch.setattr("httpx.AsyncClient.post", fake_post)
    task = TaskSpec(intent="portfolio_review", raw_message="¿Cómo está mi cartera?")
    answer = asyncio.run(
        synthesizer.synthesize(task=task, evidence=[], fallback="respuesta determinística")
    )

    assert called == 1
    assert answer == "Respuesta conversacional compacta."

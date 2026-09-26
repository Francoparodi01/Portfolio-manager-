from __future__ import annotations

import asyncio
import json

from src.agentic.answer import evidence_decision
from src.agentic.harness.context import ContextSelector
from src.agentic.harness.runtime import ConversationalHarness
from src.agentic.harness.schemas import ConversationState, TaskSpec
from src.agentic.harness.synthesis import GroundedSynthesizer
from src.agentic.harness.task import TaskParser


QUERY = "cuanto pnl hubiese ganado siguiendo las decisiones del bot los ultimos 25 días"


class _SynthesisResponse:
    def raise_for_status(self):
        return None

    def json(self):
        return {"message": {"content": json.dumps({"answer": "Respuesta natural basada en la evidencia."})}}


def _state_after_bot_pnl() -> ConversationState:
    return ConversationState(
        owner_chat_id=123,
        last_intent="bot_follow_pnl",
        recent_user_messages=[QUERY],
        last_task={
            "intent": "bot_follow_pnl",
            "lookback_days": 25,
            "aggregation": "plan_level",
            "entities": [],
        },
    )


def test_bot_follow_pnl_routes_to_single_bounded_tool():
    task = TaskParser().parse(QUERY)
    assert task.intent == "bot_follow_pnl"
    plan = ContextSelector().select(
        task,
        {"get_bot_follow_pnl", "get_normalized_bot_follow_pnl", "get_decision_ledger", "get_net_decision_report"},
    )
    assert plan.allowed_tools == ["get_bot_follow_pnl"]
    assert plan.required_tools == ["get_bot_follow_pnl"]


def test_bot_follow_pnl_preserves_requested_lookback():
    task = TaskParser().parse(QUERY)
    harness = ConversationalHarness(
        database_url="postgresql://example.invalid/db",
        owner_chat_id=123,
        repo_root=".",
        legacy_single_owner=True,
        require_audit=False,
    )
    assert task.lookback_days == 25
    assert harness._arguments("get_bot_follow_pnl", task) == {"days": 25}


def test_bot_follow_pnl_renderer_is_concise():
    payload = {
        "schema_version": "bot-follow-pnl-v1",
        "lookback_days": 25,
        "plans_total": 12,
        "plans_closed_5d": 10,
        "plans_closed_10d": 8,
        "plans_closed_20d": 3,
        "bot_pnl_5d_ars": 25000.0,
        "bot_pnl_10d_ars": 41000.0,
        "bot_pnl_20d_ars": -5000.0,
    }
    history = [{"decision": {"tool": "get_bot_follow_pnl"}, "observation": {
        "tool": "get_bot_follow_pnl", "ok": True, "content": json.dumps(payload)}}]
    decision = evidence_decision(QUERY, history)
    assert decision.answer_origin == "bot_follow_pnl_renderer_v3"
    assert "últimos 25 días" in decision.answer
    assert "A 5D: +$25.000 ARS" in decision.answer
    assert "A 10D: +$41.000 ARS" in decision.answer
    assert "A 20D: -$5.000 ARS" in decision.answer
    assert "no deben sumarse" in decision.answer


def test_pnl_provenance_followup_routes_only_to_previous_run_trace():
    task = TaskParser().parse("que datos usaste?", _state_after_bot_pnl())
    assert task.intent == "evidence_provenance"
    plan = ContextSelector().select(task, {
        "get_run_evidence_provenance", "get_bot_follow_pnl", "get_normalized_bot_follow_pnl",
        "get_decision_ledger", "get_system_status", "get_macro_context", "get_portfolio_snapshot",
    })
    assert plan.allowed_tools == ["get_run_evidence_provenance"]
    assert plan.required_tools == ["get_run_evidence_provenance"]


def test_pnl_provenance_renderer_uses_exact_audited_sources():
    payload = {
        "schema_version": "run-evidence-provenance-v1",
        "status": "observed",
        "referenced_run_id": "run-normalized-25d",
        "sources": [{
            "tool": "get_normalized_bot_follow_pnl",
            "source": "decision_log_formal_plan_episodes",
            "lookback_days": 25,
            "normalized_component": {
                "source": "decision_log_formal_plan_episodes",
                "scope": "FORMAL_PLAN_DIRECTIONAL_GROSS_EPISODE_DEDUPLICATED",
                "lookback_days": 25,
                "raw_plans_total": 52,
                "episodes_total": 17,
                "duplicates_removed": 35,
            },
        }],
    }
    history = [{"decision": {"tool": "get_run_evidence_provenance"}, "observation": {
        "tool": "get_run_evidence_provenance", "ok": True, "content": json.dumps(payload)}}]
    decision = evidence_decision("que datos usaste?", history)
    assert decision.answer_origin == "provenance_renderer_v1"
    assert "get_normalized_bot_follow_pnl" in decision.answer
    assert "ventana 25 días" in decision.answer
    assert "52 planes formales → 17 episodios independientes" in decision.answer
    assert "35 reiteraciones" in decision.answer
    assert "run-normalized-25d" in decision.answer


def test_normalized_bot_followup_inherits_25d_and_uses_dedicated_tool():
    state = _state_after_bot_pnl()
    query = "¿Cuánto hubiese ganado realmente siguiendo al bot, normalizando decisiones repetidas?"
    task = TaskParser().parse(query, state)
    assert task.intent == "bot_follow_pnl"
    assert task.objective == "explain_normalized_follow_pnl"
    assert task.lookback_days == 25
    assert task.aggregation == "normalized"

    plan = ContextSelector().select(task, {
        "get_bot_follow_pnl", "get_normalized_bot_follow_pnl", "get_decision_ledger", "get_net_decision_report"
    })
    assert plan.allowed_tools == ["get_normalized_bot_follow_pnl"]
    assert plan.required_tools == ["get_normalized_bot_follow_pnl"]

    harness = ConversationalHarness(
        database_url="postgresql://example.invalid/db",
        owner_chat_id=123,
        repo_root=".",
        legacy_single_owner=True,
        require_audit=False,
    )
    assert harness._arguments("get_normalized_bot_follow_pnl", task) == {"days": 25}


def test_normalized_bot_followup_renderer_uses_episode_counterfactual():
    payload = {
        "schema_version": "bot-follow-pnl-normalized-v1",
        "source": "decision_log_formal_plan_episodes",
        "lookback_days": 25,
        "raw_plans_total": 52,
        "episodes_total": 17,
        "duplicates_removed": 35,
        "episodes_closed_5d": 15,
        "episodes_closed_10d": 12,
        "episodes_closed_20d": 5,
        "pnl_5d_ars": -32000.0,
        "pnl_10d_ars": 18000.0,
        "pnl_20d_ars": 9000.0,
    }
    history = [{"decision": {"tool": "get_normalized_bot_follow_pnl"}, "observation": {
        "tool": "get_normalized_bot_follow_pnl", "ok": True, "content": json.dumps(payload)}}]
    decision = evidence_decision("normalizando decisiones repetidas", history)
    assert decision.answer_origin == "bot_follow_pnl_normalized_renderer_v1"
    assert "52 planes del bot quedan en 17 episodios independientes" in decision.answer
    assert "35 reiteraciones removidas" in decision.answer
    assert "A 5D: -$32.000 ARS" in decision.answer
    assert "A 10D: +$18.000 ARS" in decision.answer
    assert "A 20D: +$9.000 ARS" in decision.answer
    assert "0/0" not in decision.answer
    assert "Planes seguidos" not in decision.answer
    assert "no deben sumarse" in decision.answer


def test_persisted_portfolio_renderer_separates_current_snapshot_from_old_decision_run():
    snapshot = {
        "scraped_at": "2026-09-26T15:37:41.828492+00:00",
        "total_value_ars": 2895125.0,
        "cash_ars": 3842.34,
        "positions": [{"ticker": "NVDA", "weight": 0.168}, {"ticker": "GDX", "weight": 0.130}],
    }
    decisions = {
        "schema_version": "persisted-decision-evidence-v1",
        "evaluated_at": "2026-09-25T18:16:39.500060+00:00",
        "snapshot_as_of": "2026-09-25T18:16:04.392299+00:00",
        "signals": [
            {"ticker": "GDX", "decision": "WATCH", "status": "BLOCKED", "final_score": 0.1581},
            {"ticker": "NVDA", "decision": "HOLD", "status": "OBSERVED", "final_score": 0.0904},
        ],
    }
    history = [
        {"decision": {"tool": "get_portfolio_snapshot"}, "observation": {
            "tool": "get_portfolio_snapshot", "ok": True, "content": json.dumps(snapshot)}},
        {"decision": {"tool": "get_persisted_decision_evidence"}, "observation": {
            "tool": "get_persisted_decision_evidence", "ok": True, "content": json.dumps(decisions)}},
    ]
    decision = evidence_decision("¿Cómo está mi cartera?", history)
    assert decision.answer_origin == "portfolio_renderer_v5"
    assert "GDX WATCH (score 0,158) · bloqueada" in decision.answer
    assert "25/09 15:16 ART" in decision.answer
    assert "26/09 12:37 ART" in decision.answer
    assert "no se recalcularon con ese snapshot nuevo" in decision.answer


def test_bot_follow_pnl_uses_llm_synthesis_by_default(monkeypatch):
    monkeypatch.delenv("QUANTIA_HARNESS_SYNTHESIS_BYPASS_INTENTS", raising=False)
    synthesizer = GroundedSynthesizer(model="fixture-model")
    called = 0

    async def fake_post(*_args, **_kwargs):
        nonlocal called
        called += 1
        return _SynthesisResponse()

    monkeypatch.setattr("httpx.AsyncClient.post", fake_post)
    answer = asyncio.run(synthesizer.synthesize(
        task=TaskSpec(intent="bot_follow_pnl", raw_message=QUERY),
        evidence=[],
        fallback="respuesta determinística",
    ))
    assert called == 1
    assert answer == "Respuesta natural basada en la evidencia."


def test_provenance_bypasses_llm_synthesis(monkeypatch):
    monkeypatch.delenv("QUANTIA_HARNESS_SYNTHESIS_BYPASS_INTENTS", raising=False)
    synthesizer = GroundedSynthesizer(model="fixture-model")
    async def should_not_post(*_args, **_kwargs):
        raise AssertionError("evidence_provenance must not call Ollama synthesis")
    monkeypatch.setattr("httpx.AsyncClient.post", should_not_post)
    answer = asyncio.run(synthesizer.synthesize(
        task=TaskSpec(intent="evidence_provenance", raw_message="que datos usaste?"),
        evidence=[], fallback="procedencia determinística"))
    assert answer == "procedencia determinística"

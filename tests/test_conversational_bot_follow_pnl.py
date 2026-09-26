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


def _state_after_bot_pnl() -> ConversationState:
    return ConversationState(
        owner_chat_id=123,
        last_intent="bot_follow_pnl",
        recent_user_messages=[QUERY],
    )


def test_bot_follow_pnl_routes_to_single_bounded_tool():
    task = TaskParser().parse(QUERY)
    assert task.intent == "bot_follow_pnl"

    plan = ContextSelector().select(
        task,
        {"get_bot_follow_pnl", "get_decision_ledger", "get_net_decision_report", "get_performance"},
    )
    assert plan.allowed_tools == ["get_bot_follow_pnl"]
    assert plan.required_tools == ["get_bot_follow_pnl"]
    assert plan.parallel_groups == []


def test_bot_follow_pnl_preserves_requested_lookback():
    task = TaskParser().parse(QUERY)
    harness = ConversationalHarness(
        database_url="postgresql://example.invalid/db",
        owner_chat_id=123,
        repo_root=".",
        legacy_single_owner=True,
        require_audit=False,
    )
    assert harness._arguments("get_bot_follow_pnl", task) == {"days": 25}


def test_bot_follow_pnl_renderer_is_concise_and_does_not_add_horizons():
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
    history = [{
        "decision": {"tool": "get_bot_follow_pnl"},
        "observation": {
            "tool": "get_bot_follow_pnl",
            "ok": True,
            "content": json.dumps(payload),
        },
    }]

    decision = evidence_decision(QUERY, history)

    assert decision.answer_origin == "bot_follow_pnl_renderer_v2"
    assert "últimos 25 días" in decision.answer
    assert "A 5D: +$25.000 ARS" in decision.answer
    assert "A 10D: +$41.000 ARS" in decision.answer
    assert "A 20D: -$5.000 ARS" in decision.answer
    assert "no es PnL realizado" in decision.answer
    assert "no deben sumarse" in decision.answer
    assert '"schema_version"' not in decision.answer


def test_pnl_provenance_followup_reuses_previous_window_and_only_real_source():
    state = _state_after_bot_pnl()
    task = TaskParser().parse("que datos usaste?", state)
    assert task.intent == "bot_follow_pnl"
    assert task.objective == "explain_previous_sources"
    assert "25 días" in task.raw_message

    plan = ContextSelector().select(
        task,
        {"get_bot_follow_pnl", "get_decision_ledger", "get_system_status", "get_macro_context"},
    )
    assert plan.allowed_tools == ["get_bot_follow_pnl"]

    payload = {
        "schema_version": "bot-follow-pnl-v1",
        "source": "decision_log_formal_plans",
        "scope": "FORMAL_PLAN_DIRECTIONAL_GROSS_PLAN_LEVEL_NOT_DEDUPLICATED",
        "lookback_days": 25,
        "plans_total": 52,
        "plans_closed_5d": 35,
        "plans_closed_10d": 29,
        "plans_closed_20d": 11,
        "bot_pnl_5d_ars": -115960,
        "bot_pnl_10d_ars": -12320,
        "bot_pnl_20d_ars": -41805,
    }
    history = [{
        "decision": {"tool": "get_bot_follow_pnl"},
        "observation": {"tool": "get_bot_follow_pnl", "ok": True, "content": json.dumps(payload)},
    }]
    decision = evidence_decision(task.raw_message, history)

    assert "usé una sola fuente" in decision.answer
    assert "decision_log_formal_plans" in decision.answer
    assert "ventana de 25 días" in decision.answer
    assert "No usé snapshot de cartera" in decision.answer
    assert "contexto macro" in decision.answer
    assert "estado del sistema" in decision.answer
    assert "Redis" in decision.answer
    assert "1559" not in decision.answer


def test_normalized_bot_followup_inherits_25d_and_uses_ledger_not_plan_level_tool():
    state = _state_after_bot_pnl()
    query = "¿Cuánto hubiese ganado realmente siguiendo al bot, normalizando decisiones repetidas?"
    task = TaskParser().parse(query, state)

    assert task.intent == "bot_follow_pnl"
    assert task.objective == "explain_normalized_follow_pnl"
    assert "25 días" in task.raw_message

    plan = ContextSelector().select(
        task,
        {"get_bot_follow_pnl", "get_decision_ledger", "get_net_decision_report"},
    )
    assert plan.allowed_tools == ["get_decision_ledger"]
    assert plan.required_tools == ["get_decision_ledger"]

    harness = ConversationalHarness(
        database_url="postgresql://example.invalid/db",
        owner_chat_id=123,
        repo_root=".",
        legacy_single_owner=True,
        require_audit=False,
    )
    assert harness._arguments("get_decision_ledger", task) == {"days": 25}


def test_normalized_bot_followup_renderer_uses_normalized_ledger_and_does_not_invent_10d_20d():
    report = "\n".join([
        "📒 Decision Ledger",
        "25d · resultado 5D · lectura económica",
        "🧭 <b>Planes seguidos</b> <code>NORMALIZADO</code>",
        "   14/18 maduros · 64,3% positivos",
        "   Retorno bruto +2,1% · PnL direccional bruto 🟢 <b>+$82.500</b>",
        "🤖 <b>Bot vs ejecución humana</b> <code>PLAN-LEVEL</code>",
    ])
    payload = {
        "source": "decision_ledger",
        "lookback_days": 25,
        "report": report,
    }
    history = [{
        "decision": {"tool": "get_decision_ledger"},
        "observation": {"tool": "get_decision_ledger", "ok": True, "content": json.dumps(payload)},
    }]
    goal = "¿Cuánto hubiese ganado siguiendo al bot, normalizando decisiones repetidas? [ventana heredada: 25 días]"
    decision = evidence_decision(goal, history)

    assert decision.answer_origin == "bot_follow_pnl_renderer_v2"
    assert "NORMALIZADA" in decision.answer
    assert "últimos 25 días" in decision.answer
    assert "14/18 maduros" in decision.answer
    assert "PnL direccional bruto" in decision.answer
    assert "+$82.500" in decision.answer
    assert "5D" in decision.answer
    assert "no invento 10D/20D" in decision.answer
    assert "148 planes" not in decision.answer


def test_persisted_portfolio_renderer_separates_current_snapshot_from_old_decision_run():
    snapshot = {
        "scraped_at": "2026-09-26T15:37:41.828492+00:00",
        "total_value_ars": 2895125.0,
        "cash_ars": 3842.34,
        "positions": [
            {"ticker": "NVDA", "weight": 0.168},
            {"ticker": "GDX", "weight": 0.130},
        ],
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
        {
            "decision": {"tool": "get_portfolio_snapshot"},
            "observation": {"tool": "get_portfolio_snapshot", "ok": True, "content": json.dumps(snapshot)},
        },
        {
            "decision": {"tool": "get_persisted_decision_evidence"},
            "observation": {
                "tool": "get_persisted_decision_evidence",
                "ok": True,
                "content": json.dumps(decisions),
            },
        },
    ]

    decision = evidence_decision("¿Cómo está mi cartera?", history)

    assert decision.answer_origin == "portfolio_renderer_v5"
    assert "GDX WATCH (score 0,158) · bloqueada" in decision.answer
    assert "25/09 15:16 ART" in decision.answer
    assert "26/09 12:37 ART" in decision.answer
    assert "no se recalcularon con ese snapshot nuevo" in decision.answer
    assert "Extracto literal" not in decision.answer
    assert '"signals"' not in decision.answer


def test_bot_follow_pnl_bypasses_llm_synthesis(monkeypatch):
    monkeypatch.delenv("QUANTIA_HARNESS_SYNTHESIS_BYPASS_INTENTS", raising=False)
    synthesizer = GroundedSynthesizer(model="fixture-model")

    async def should_not_post(*_args, **_kwargs):
        raise AssertionError("bot_follow_pnl must not call Ollama synthesis")

    monkeypatch.setattr("httpx.AsyncClient.post", should_not_post)
    task = TaskSpec(intent="bot_follow_pnl", raw_message=QUERY)
    answer = asyncio.run(
        synthesizer.synthesize(task=task, evidence=[], fallback="respuesta determinística")
    )
    assert answer == "respuesta determinística"

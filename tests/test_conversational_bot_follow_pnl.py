from __future__ import annotations

import asyncio
import json

from src.agentic.answer import evidence_decision
from src.agentic.harness.context import ContextSelector
from src.agentic.harness.runtime import ConversationalHarness
from src.agentic.harness.schemas import TaskSpec
from src.agentic.harness.synthesis import GroundedSynthesizer
from src.agentic.harness.task import TaskParser


QUERY = "cuanto pnl hubiese ganado siguiendo las decisiones del bot los ultimos 25 días"


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

    assert decision.answer_origin == "bot_follow_pnl_renderer_v1"
    assert "últimos 25 días" in decision.answer
    assert "A 5D: +$25.000 ARS" in decision.answer
    assert "A 10D: +$41.000 ARS" in decision.answer
    assert "A 20D: -$5.000 ARS" in decision.answer
    assert "no es PnL realizado" in decision.answer
    assert "no deben sumarse" in decision.answer
    assert '"schema_version"' not in decision.answer


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

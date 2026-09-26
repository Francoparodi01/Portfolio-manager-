from __future__ import annotations

import asyncio

import httpx

from src.agentic.harness.schemas import ConversationState
from src.agentic.harness.semantic_router import SemanticTaskRouter
from src.agentic.harness.task import TaskParser


class _Response:
    def __init__(self, value):
        self._value = value

    def raise_for_status(self):
        return None

    def json(self):
        return {"message": {"content": __import__("json").dumps(self._value)}}


def _state() -> ConversationState:
    # Deliberately omit the numeric window from recent prose. The router must
    # inherit 25d from last_task, not scrape it out of old chat text.
    return ConversationState(
        owner_chat_id=123,
        last_intent="bot_follow_pnl",
        recent_user_messages=["consulta anterior sobre rendimiento del bot"],
        last_task={
            "intent": "bot_follow_pnl",
            "lookback_days": 25,
            "horizon_days": None,
            "aggregation": "plan_level",
            "entities": [],
        },
    )


def test_semantic_router_understands_normalized_followup_without_keyword_match(monkeypatch):
    message = "Hacé de cuenta que cada insistencia sobre la misma compra es una sola y decime cómo habría quedado en el período anterior"
    assert TaskParser().parse(message, _state()).objective != "explain_normalized_follow_pnl"

    async def fake_post(self, url, json):
        return _Response({
            "intent": "bot_follow_pnl",
            "entities": [],
            "lookback_days": None,
            "horizon_days": None,
            "aggregation": "normalized",
            "reference": "previous_turn",
            "confidence": 0.96,
        })

    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)
    task = asyncio.run(SemanticTaskRouter(model="fixture-model").route(message, _state()))

    assert task.routing_source == "semantic"
    assert task.intent == "bot_follow_pnl"
    assert task.objective == "explain_normalized_follow_pnl"
    assert task.aggregation == "normalized"
    assert task.lookback_days == 25
    assert task.reference == "previous_turn"


def test_semantic_router_inherits_structured_window_even_if_model_marks_same_thread_current(monkeypatch):
    message = "Ahora mostrame el mismo cálculo pero sin abrir una consulta nueva"

    async def fake_post(self, url, json):
        return _Response({
            "intent": "bot_follow_pnl",
            "entities": [],
            "lookback_days": None,
            "horizon_days": None,
            "aggregation": None,
            "reference": "current_turn",
            "confidence": 0.86,
        })

    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)
    task = asyncio.run(SemanticTaskRouter(model="fixture-model").route(message, _state()))

    assert task.lookback_days == 25
    assert task.aggregation == "plan_level"


def test_fallback_inherits_window_from_structured_task_not_old_text():
    task = TaskParser().parse("y normalizando las repetidas?", _state())
    assert task.intent == "bot_follow_pnl"
    assert task.lookback_days == 25
    assert task.aggregation == "normalized"


def test_semantic_router_understands_provenance_paraphrase(monkeypatch):
    message = "¿En qué evidencia te apoyaste para decir eso?"
    assert TaskParser().parse(message, _state()).intent != "evidence_provenance"

    async def fake_post(self, url, json):
        return _Response({
            "intent": "evidence_provenance",
            "entities": [],
            "lookback_days": None,
            "horizon_days": None,
            "aggregation": None,
            "reference": "previous_turn",
            "confidence": 0.94,
        })

    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)
    task = asyncio.run(SemanticTaskRouter(model="fixture-model").route(message, _state()))

    assert task.routing_source == "semantic"
    assert task.intent == "evidence_provenance"
    assert task.objective == "explain_previous_sources"
    assert task.reference == "previous_turn"


def test_semantic_router_understands_natural_opportunity_request(monkeypatch):
    message = "Si sacara lo más flojo que tengo, ¿qué alternativa tendría sentido mirar?"
    assert TaskParser().parse(message, ConversationState(owner_chat_id=123)).intent == "general"

    async def fake_post(self, url, json):
        return _Response({
            "intent": "opportunities",
            "entities": [],
            "lookback_days": None,
            "horizon_days": None,
            "aggregation": None,
            "reference": "current_turn",
            "confidence": 0.9,
        })

    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)
    task = asyncio.run(
        SemanticTaskRouter(model="fixture-model").route(
            message, ConversationState(owner_chat_id=123)
        )
    )
    assert task.routing_source == "semantic"
    assert task.intent == "opportunities"
    assert task.required_evidence == ["portfolio", "radar", "risk", "decision_lab"]


def test_semantic_router_falls_back_safely_when_model_fails(monkeypatch):
    async def failing_post(self, url, json):
        raise httpx.ConnectError("fixture failure")

    monkeypatch.setattr(httpx.AsyncClient, "post", failing_post)
    task = asyncio.run(
        SemanticTaskRouter(model="fixture-model").route(
            "¿Cómo está mi cartera?", ConversationState(owner_chat_id=123)
        )
    )
    assert task.routing_source == "fallback"
    assert task.intent == "portfolio_review"

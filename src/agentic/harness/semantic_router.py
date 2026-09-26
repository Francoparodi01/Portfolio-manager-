from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from typing import Any

import httpx

from .models import ModelRoles
from .schemas import ConversationState, TaskSpec
from .task import TaskParser

logger = logging.getLogger(__name__)


_ALLOWED_INTENTS = {
    "portfolio_review",
    "position_analysis",
    "decision_explanation",
    "position_comparison",
    "opportunities",
    "performance",
    "bot_follow_pnl",
    "net_performance",
    "analytics_v2",
    "viability",
    "regression_audit",
    "calibration_audit",
    "decision_history",
    "decision_lab",
    "meta_policy",
    "market_context",
    "system_status",
    "evidence_provenance",
    "general",
}

_POLICY: dict[str, tuple[str, list[str]]] = {
    "portfolio_review": ("review_portfolio", ["portfolio", "decision", "risk"]),
    "position_analysis": ("evaluate_position", ["portfolio", "decision", "technical", "macro", "risk", "decision_lab"]),
    "decision_explanation": ("explain_current_decision", ["portfolio", "decision", "technical", "macro", "risk", "decision_lab"]),
    "position_comparison": ("compare_positions", ["portfolio", "decision", "risk", "decision_lab"]),
    "opportunities": ("find_portfolio_alternatives", ["portfolio", "radar", "risk", "decision_lab"]),
    "performance": ("explain_economic_results", ["ledger", "performance"]),
    "net_performance": ("explain_net_decision_results", ["net_performance"]),
    "analytics_v2": ("explain_observational_analytics", ["analytics_v2"]),
    "viability": ("explain_viability_audit", ["viability"]),
    "regression_audit": ("explain_regression_audit", ["regression_audit"]),
    "calibration_audit": ("explain_calibration_audit", ["calibration_audit"]),
    "decision_history": ("explain_matured_decisions", ["ledger", "outcomes"]),
    "decision_lab": ("compare_recorded_decision_evidence", ["decision_lab"]),
    "meta_policy": ("explain_shadow_meta_policy", ["meta_policy", "decision_lab"]),
    "market_context": ("explain_market_context", ["macro"]),
    "system_status": ("explain_system_health", ["system_status"]),
    "evidence_provenance": ("explain_previous_sources", ["previous_run_trace"]),
    "general": ("answer_user", []),
}


def _explicit_days(text: str) -> int | None:
    match = re.search(r"\b(\d{1,3})\s*(?:d[ií]as|days)\b", str(text or "").lower())
    if not match:
        return None
    return max(1, min(365, int(match.group(1))))


def _structured_previous(state: ConversationState, key: str) -> Any:
    task = state.last_task
    return task.get(key) if isinstance(task, dict) else None


def _previous_days(state: ConversationState) -> int | None:
    value = _structured_previous(state, "lookback_days")
    try:
        if value is not None:
            return max(1, min(730, int(value)))
    except (TypeError, ValueError):
        pass
    # Backward compatibility for sessions created before structured state was
    # introduced. New turns persist last_task and no longer depend on this scan.
    for text in reversed(state.recent_user_messages):
        days = _explicit_days(text)
        if days is not None:
            return days
    return None


class SemanticTaskRouter:
    """Map natural conversation to a validated TaskSpec, never directly to tools.

    The router is deliberately narrow: the LLM only labels semantics. Tool
    selection, permissions, financial computation and verification remain
    deterministic. TaskParser is retained solely as a conservative fallback.
    """

    def __init__(self, *, model: str | None = None, base_url: str | None = None) -> None:
        roles = ModelRoles.from_env()
        self.model = model or roles.router
        self.base_url = (
            base_url
            or os.getenv("QUANTIA_AGENT_OLLAMA_URL")
            or os.getenv("OLLAMA_URL")
            or "http://host.docker.internal:11434"
        ).rstrip("/")
        self.timeout_seconds = max(2.0, min(45.0, float(os.getenv("QUANTIA_ROUTER_TIMEOUT_SECONDS", "15"))))
        self.keep_alive = os.getenv("QUANTIA_OLLAMA_KEEP_ALIVE", "30m")
        self.fallback = TaskParser()

    @staticmethod
    def _system_prompt() -> str:
        intents = sorted(_ALLOWED_INTENTS)
        return (
            "Sos el router semántico de Quantia. Tu única tarea es entender la intención conversacional; "
            "NO calculás finanzas, NO recomendás operaciones y NO elegís herramientas. "
            "Usá el contexto estructurado para resolver referencias como el turno anterior, el mismo activo, "
            "la misma ventana temporal o una modificación del análisis previo.\n\n"
            "Intenciones disponibles y significado:\n"
            "- portfolio_review: estado/composición general de la cartera.\n"
            "- position_analysis: evaluar una posición o ticker concreto.\n"
            "- decision_explanation: explicar por qué Quantia propone/propuso algo sobre un ticker.\n"
            "- position_comparison: comparar dos posiciones/tickers.\n"
            "- opportunities: buscar alternativas u oportunidades fuera/dentro de cartera.\n"
            "- performance: resultado económico general/ledger de Quantia.\n"
            "- bot_follow_pnl: contrafactual de cuánto habría pasado siguiendo planes del bot.\n"
            "- net_performance: resultado neto por decisión/run.\n"
            "- decision_history: outcomes de decisiones históricas/maduras.\n"
            "- decision_lab: PLAN vs HOLD, DVA, counterfactuals, replay/comparables.\n"
            "- analytics_v2, viability, regression_audit, calibration_audit: esos reportes específicos.\n"
            "- meta_policy: Economic Meta Policy / A-B-C shadow.\n"
            "- market_context: contexto macro/mercado.\n"
            "- system_status: salud técnica del sistema.\n"
            "- evidence_provenance: el usuario pregunta qué datos/fuentes/evidencia se usaron en la respuesta anterior.\n"
            "- general: sólo si ninguna categoría anterior describe el objetivo.\n\n"
            "Para bot_follow_pnl, aggregation='normalized' significa deduplicar recomendaciones repetidas; "
            "aggregation='plan_level' significa contar cada plan formal. No confundas normalizado con operaciones que el humano realmente siguió.\n"
            "Si el mensaje modifica o continúa el análisis previo, reference='previous_turn' y no inventes una nueva ventana. "
            "Si pregunta por fuentes de la respuesta anterior, intent=evidence_provenance y reference='previous_turn'.\n\n"
            "Devolvé únicamente JSON válido con este esquema:\n"
            "{\"intent\":\"...\",\"entities\":[\"TICKER\"],\"lookback_days\":null,"
            "\"horizon_days\":null,\"aggregation\":null,\"reference\":\"current_turn\",\"confidence\":0.0}\n"
            f"intent debe ser uno de: {json.dumps(intents, ensure_ascii=False)}. "
            "aggregation sólo puede ser plan_level, normalized o null; reference sólo current_turn o previous_turn."
        )

    @staticmethod
    def _parse_json(content: str) -> dict[str, Any]:
        text = str(content or "").strip()
        if text.startswith("```"):
            text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
            text = re.sub(r"\s*```$", "", text).strip()
        try:
            value = json.loads(text)
            if isinstance(value, dict):
                return value
        except (TypeError, ValueError):
            pass
        start, end = text.find("{"), text.rfind("}")
        if start >= 0 and end > start:
            value = json.loads(text[start:end + 1])
            if isinstance(value, dict):
                return value
        raise ValueError("semantic router returned invalid JSON")

    @staticmethod
    def _bounded_int(value: Any, minimum: int, maximum: int) -> int | None:
        if value in (None, "", "null"):
            return None
        try:
            return max(minimum, min(maximum, int(value)))
        except (TypeError, ValueError):
            return None

    def _to_task(self, raw: str, value: dict[str, Any], state: ConversationState) -> TaskSpec:
        intent = str(value.get("intent") or "general").strip()
        if intent not in _ALLOWED_INTENTS:
            raise ValueError(f"unsupported semantic intent: {intent}")

        entities_raw = value.get("entities")
        entities = [str(item).upper().strip() for item in entities_raw] if isinstance(entities_raw, list) else []
        entities = [item for item in entities if re.fullmatch(r"[A-Z][A-Z0-9.=-]{0,14}", item)]

        reference = str(value.get("reference") or "current_turn")
        if reference not in {"current_turn", "previous_turn"}:
            reference = "current_turn"
        if intent == "evidence_provenance":
            reference = "previous_turn"

        prior_intent = str(_structured_previous(state, "intent") or state.last_intent or "")
        same_thread = bool(prior_intent and intent == prior_intent)

        explicit_days = _explicit_days(raw)
        routed_days = self._bounded_int(value.get("lookback_days"), 1, 730)
        lookback_days = explicit_days if explicit_days is not None else routed_days
        if lookback_days is None and (reference == "previous_turn" or same_thread):
            lookback_days = _previous_days(state)

        horizon_days = self._bounded_int(value.get("horizon_days"), 1, 365)
        if horizon_days is None and reference == "previous_turn":
            horizon_days = self._bounded_int(_structured_previous(state, "horizon_days"), 1, 365)

        aggregation = value.get("aggregation")
        aggregation = str(aggregation) if aggregation in {"plan_level", "normalized"} else None
        if intent == "bot_follow_pnl" and aggregation is None:
            inherited_aggregation = _structured_previous(state, "aggregation")
            if (reference == "previous_turn" or same_thread) and inherited_aggregation in {"plan_level", "normalized"}:
                aggregation = str(inherited_aggregation)
            else:
                aggregation = "plan_level"

        if not entities and (reference == "previous_turn" or same_thread) and intent in {
            "position_analysis", "decision_explanation", "position_comparison", "meta_policy"
        }:
            previous_entities = _structured_previous(state, "entities")
            if isinstance(previous_entities, list):
                entities = [str(item).upper().strip() for item in previous_entities if str(item).strip()]
            elif state.active_symbols:
                entities = list(state.active_symbols)

        if intent == "bot_follow_pnl":
            objective = "explain_normalized_follow_pnl" if aggregation == "normalized" else "explain_hypothetical_bot_plan_pnl"
            evidence = ["bot_counterfactual_normalized"] if aggregation == "normalized" else ["bot_follow_pnl"]
        else:
            objective, evidence = _POLICY[intent]

        ambiguity: list[str] = []
        if intent in {"position_analysis", "decision_explanation"} and not entities:
            ambiguity.append("symbol_missing")
        if intent == "position_comparison" and len(entities) < 2:
            ambiguity.append("comparison_target_missing")

        try:
            confidence = max(0.0, min(1.0, float(value.get("confidence"))))
        except (TypeError, ValueError):
            confidence = None

        return TaskSpec(
            intent=intent,
            entities=entities,
            objective=objective,
            required_evidence=evidence,
            inherited_subject=state.conversation_subject if reference == "previous_turn" else None,
            ambiguity=ambiguity,
            raw_message=raw,
            lookback_days=lookback_days,
            horizon_days=horizon_days,
            aggregation=aggregation,
            reference=reference,
            routing_source="semantic",
            routing_confidence=confidence,
        )

    async def route(self, message: str, state: ConversationState) -> TaskSpec:
        raw = " ".join(str(message or "").split())
        if not raw:
            raise ValueError("message cannot be empty")
        if os.getenv("QUANTIA_SEMANTIC_ROUTER_ENABLED", "true").lower() not in {"1", "true", "yes", "on"}:
            return self.fallback.parse(raw, state)

        context = {
            "last_intent": state.last_intent,
            "last_task": state.last_task,
            "active_symbols": state.active_symbols,
            "conversation_subject": state.conversation_subject,
            "recent_user_messages": state.recent_user_messages[-2:],
        }
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": self._system_prompt()},
                {"role": "user", "content": json.dumps({"message": raw, "context": context}, ensure_ascii=False)},
            ],
            "stream": False,
            "format": "json",
            "keep_alive": self.keep_alive,
            "options": {
                "temperature": 0.0,
                "num_ctx": max(1024, min(4096, int(os.getenv("QUANTIA_ROUTER_CONTEXT_TOKENS", "2048")))),
                "num_predict": max(96, min(384, int(os.getenv("QUANTIA_ROUTER_NUM_PREDICT", "160")))),
            },
        }
        started = time.monotonic()
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await asyncio.wait_for(
                    client.post(f"{self.base_url}/api/chat", json=payload),
                    timeout=self.timeout_seconds,
                )
            response.raise_for_status()
            body = response.json()
            value = self._parse_json(str((body.get("message") or {}).get("content") or ""))
            task = self._to_task(raw, value, state)
            logger.info(
                "[CHAT][ROUTER] source=semantic model=%s elapsed_ms=%s intent=%s confidence=%s",
                self.model,
                int((time.monotonic() - started) * 1000),
                task.intent,
                task.routing_confidence,
            )
            return task
        except Exception as exc:
            task = self.fallback.parse(raw, state)
            logger.warning(
                "[CHAT][ROUTER] source=fallback model=%s elapsed_ms=%s error=%s intent=%s",
                self.model,
                int((time.monotonic() - started) * 1000),
                type(exc).__name__,
                task.intent,
            )
            return task


__all__ = ["SemanticTaskRouter"]

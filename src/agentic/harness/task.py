from __future__ import annotations

import re
import unicodedata

from .schemas import ConversationState, TaskSpec


_TICKER_RE = re.compile(r"\b[A-Z][A-Z0-9.=-]{1,9}\b")
_STOP_SYMBOLS = {
    "PLAN", "HOLD", "BUY", "SELL", "REDUCE", "DVA", "EV", "IC", "CI",
    "ARS", "USD", "CCL", "MEP", "PIT", "META", "SHADOW", "PRODUCTION",
}
_ALIASES = {
    "microsoft": "MSFT", "msft": "MSFT", "nvidia": "NVDA", "nvda": "NVDA",
    "apple": "AAPL", "aapl": "AAPL", "amazon": "AMZN", "amzn": "AMZN",
    "google": "GOOGL", "googl": "GOOGL", "tesla": "TSLA", "tsla": "TSLA",
    "amd": "AMD", "mu": "MU", "gdx": "GDX", "iren": "IREN",
}


def _plain(text: str) -> str:
    return "".join(
        char for char in unicodedata.normalize("NFKD", str(text).lower())
        if not unicodedata.combining(char)
    )


def _extract_days(text: str) -> int | None:
    match = re.search(r"\b(\d{1,3})\s*(?:dias|days)\b", _plain(text))
    if not match:
        return None
    return max(1, min(365, int(match.group(1))))


class TaskParser:
    """Turn a user message into a bounded task without making financial claims."""

    def extract_entities(self, text: str) -> list[str]:
        raw = str(text or "")
        return self._entities(raw, _plain(raw))

    def parse(self, message: str, state: ConversationState | None = None) -> TaskSpec:
        raw = " ".join(str(message or "").split())
        if not raw:
            raise ValueError("message cannot be empty")
        if len(raw) > 4000:
            raise ValueError("message exceeds 4000 characters")
        text = _plain(raw)
        entities = self._entities(raw, text)
        inherited_subject = None

        follow_up = self._looks_like_follow_up(text)
        if state and follow_up:
            comparison_follow_up = any(term in text for term in ("comparalo", "comparala", "comparar", " vs "))
            if comparison_follow_up and entities and state.active_symbols:
                previous = [symbol for symbol in state.active_symbols if symbol not in entities]
                if previous:
                    entities = [previous[0], *entities]
                    inherited_subject = state.conversation_subject or previous[0]
            elif not entities and state.active_symbols:
                entities = list(state.active_symbols)
                inherited_subject = state.conversation_subject or entities[0]
            elif state.conversation_subject:
                inherited_subject = state.conversation_subject

        intent, objective, evidence = self._classify(text, entities, state, follow_up)

        # Follow-ups such as "¿qué datos usaste?" or "normalizando repetidas"
        # inherit the previous bounded lookback instead of silently falling back
        # to the tool default (90d). Provenance also carries the referenced user
        # turn so the model-side deterministic router can stay on the same intent
        # without opening the general tool surface.
        task_raw = raw
        if state and objective == "explain_previous_sources" and state.recent_user_messages:
            task_raw = f"{raw} [turno referido: {state.recent_user_messages[-1]}]"
        if state and follow_up and _extract_days(task_raw) is None and intent in {
            "bot_follow_pnl", "performance", "decision_history"
        }:
            inherited_days = next(
                (
                    days for days in (
                        _extract_days(item) for item in reversed(state.recent_user_messages)
                    )
                    if days is not None
                ),
                None,
            )
            if inherited_days is not None:
                task_raw = f"{task_raw} [ventana heredada: {inherited_days} días]"

        ambiguity: list[str] = []
        if intent in {"position_analysis", "decision_explanation"} and not entities:
            ambiguity.append("symbol_missing")
        if intent == "position_comparison" and len(entities) < 2:
            ambiguity.append("comparison_target_missing")

        return TaskSpec(
            intent=intent,
            entities=entities,
            objective=objective,
            required_evidence=evidence,
            inherited_subject=inherited_subject,
            ambiguity=ambiguity,
            raw_message=task_raw,
        )

    @staticmethod
    def _looks_like_follow_up(text: str) -> bool:
        short = len(text.split()) <= 14
        markers = (
            "por que", "porque", "y si", "comparalo", "comparala", "y ahora",
            "eso", "esa", "ese", "entonces", "en su lugar", "y cual", "y que",
            "que te preocupa", "cual te preocupa", "esta senal", "esa senal",
            "que datos usaste", "que dato usaste", "que fuentes usaste", "que fuente usaste",
            "de donde sale", "de donde salio", "normaliz", "deduplic", "sin repetir",
        )
        return short and any(marker in text for marker in markers)

    @staticmethod
    def _entities(raw: str, text: str) -> list[str]:
        found = [item for item in _TICKER_RE.findall(raw) if item not in _STOP_SYMBOLS]
        for name, ticker in _ALIASES.items():
            if re.search(rf"\b{re.escape(name)}\b", text):
                found.append(ticker)
        return list(dict.fromkeys(found))

    @staticmethod
    def _is_provenance_question(text: str) -> bool:
        return any(term in text for term in (
            "que datos usaste", "que dato usaste", "que fuentes usaste", "que fuente usaste",
            "de donde sale", "de donde salio", "que evidencia usaste", "con que datos",
        ))

    def _classify(
        self,
        text: str,
        entities: list[str],
        state: ConversationState | None,
        follow_up: bool,
    ) -> tuple[str, str, list[str]]:
        # Provenance is a follow-up over the previous bounded workflow. Reuse
        # that workflow's source surface; do not open the general tool registry.
        if state and state.last_intent and self._is_provenance_question(text):
            return state.last_intent, "explain_previous_sources", ["referenced_evidence"]

        if any(term in text for term in ("a y b", "meta-a", "meta-b", "meta-c", "economic meta", "politica meta")):
            return "meta_policy", "explain_shadow_meta_policy", ["meta_policy", "decision_lab"]
        if any(term in text for term in ("decision lab", "plan vs hold", "plan contra hold", "dva", "contrafactual", "counterfactual")):
            return "decision_lab", "compare_recorded_decision_evidence", ["decision_lab"]
        if any(term in text for term in ("regression audit", "regression", "regresion", "auditoria de regresion", "auditoria estadistica")):
            return "regression_audit", "explain_regression_audit", ["regression_audit"]
        if any(term in text for term in ("decision calibration", "calibration", "calibracion", "dcl")):
            return "calibration_audit", "explain_calibration_audit", ["calibration_audit"]
        if any(term in text for term in ("analytics v2", "analytics", "analitica v2")):
            return "analytics_v2", "explain_observational_analytics", ["analytics_v2"]
        if any(term in text for term in ("viability", "viabilidad", "es viable", "sigue siendo viable")):
            return "viability", "explain_viability_audit", ["viability"]
        if any(term in text for term in ("resultado neto", "reporte neto", "neto por decision")):
            return "net_performance", "explain_net_decision_results", ["net_performance"]

        normalized_follow = any(term in text for term in (
            "normaliz", "deduplic", "sin repetir", "decisiones repetidas", "recomendaciones repetidas"
        ))
        prior_bot_follow = bool(state and state.last_intent == "bot_follow_pnl")
        if normalized_follow and ("bot" in text or prior_bot_follow):
            # Keep the same deterministic intent so planner/synthesis remain
            # bypassed, but switch the evidence surface in ContextSelector.
            return "bot_follow_pnl", "explain_normalized_follow_pnl", ["ledger_normalized"]

        bot_counterfactual = (
            "bot" in text
            and any(term in text for term in (
                "hubiese", "habria", "si seguia", "si hubiera seguido", "siguiendo",
                "seguir las decisiones", "seguido las decisiones", "seguir al bot",
            ))
            and any(term in text for term in ("pnl", "gana", "perd", "resultado", "decision"))
        )
        if bot_counterfactual:
            return "bot_follow_pnl", "explain_hypothetical_bot_plan_pnl", ["bot_follow_pnl"]

        if any(term in text for term in ("cuanto gano", "pnl", "ganancia", "perdio", "ledger")):
            return "performance", "explain_economic_results", ["ledger", "performance"]
        horizon_decisions = "decision" in text and bool(re.search(r"\b\d{1,3}\s*(?:dias|days)\b", text))
        if horizon_decisions or any(term in text for term in ("hace 20 dias", "decisiones que tomaste", "outcomes", "resultado de decisiones")):
            return "decision_history", "explain_matured_decisions", ["ledger", "outcomes"]

        # Natural-language discovery/buy requests should use the bounded radar
        # workflow rather than falling through to the unrestricted general intent.
        # Keep named-ticker questions out of this branch so "¿conviene comprar NVDA?"
        # remains a position analysis instead of becoming a portfolio-wide scan.
        purchase_recommendation = not entities and (
            any(term in text for term in (
                "que me recomendas comprar", "que me recomiendas comprar",
                "que recomendas comprar", "que recomiendas comprar",
                "que puedo comprar", "que deberia comprar", "que compro",
                "que comprarias", "opciones para comprar", "opciones de compra",
                "donde pondrias el cash", "donde pondrias el efectivo",
            ))
            or (
                any(term in text for term in ("comprar", "compro", "compraria", "comprarias"))
                and any(term in text for term in ("recomend", "suger", "opcion", "opciones"))
            )
        )
        if purchase_recommendation or any(
            term in text for term in ("oportunidad", "reemplazar", "en su lugar", "que compraria")
        ):
            return "opportunities", "find_portfolio_alternatives", ["portfolio", "radar", "risk", "decision_lab"]
        if any(term in text for term in ("status", "estado del sistema", "esta funcionando", "salud del sistema")):
            return "system_status", "explain_system_health", ["system_status"]
        if any(term in text for term in ("mercado", "macro", "vix", "dolar", "riesgo pais")):
            return "market_context", "explain_market_context", ["macro"]
        is_why = any(term in text for term in ("por que", "porque", "explic", "motivo", "razon"))
        if entities and is_why:
            return "decision_explanation", "explain_current_decision", ["portfolio", "decision", "technical", "macro", "risk", "decision_lab"]
        if len(entities) >= 2 and any(term in text for term in ("compar", "cambiar", " vs ")):
            return "position_comparison", "compare_positions", ["portfolio", "decision", "risk", "decision_lab"]
        if entities and any(term in text for term in ("que hago", "conviene", "revis", "analiz", "decision", "comprar")):
            return "position_analysis", "evaluate_position", ["portfolio", "decision", "technical", "macro", "risk", "decision_lab"]
        if any(term in text for term in ("cartera", "portfolio", "como viene todo", "como esta todo")):
            return "portfolio_review", "review_portfolio", ["portfolio", "decision", "risk"]
        if follow_up and state and state.last_intent:
            return state.last_intent, "continue_previous_task", ["referenced_evidence"]
        return "general", "answer_user", []

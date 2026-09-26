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
_REFERENTIAL_INTENTS = {
    "position_analysis", "decision_explanation", "position_comparison", "decision_lab", "meta_policy"
}


def _plain(text: str) -> str:
    return "".join(
        char for char in unicodedata.normalize("NFKD", str(text).lower())
        if not unicodedata.combining(char)
    )


class TaskParser:
    """Turn a user message into a bounded task without making financial claims.

    Known intents improve routing, but unknown/composed requests remain `general`
    and are delegated to the dynamic planner. Follow-ups inherit only structural
    references (subject/symbol), never an earlier assistant conclusion as evidence.
    """

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
        may_inherit_symbol = bool(state and state.last_intent in _REFERENTIAL_INTENTS)
        if state and follow_up and may_inherit_symbol:
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
            raw_message=raw,
        )

    @staticmethod
    def _looks_like_follow_up(text: str) -> bool:
        short = len(text.split()) <= 12
        markers = (
            "por que", "porque", "y si", "comparalo", "comparala", "y ahora",
            "eso", "esa", "ese", "entonces", "en su lugar", "y cual", "y que",
            "que te preocupa", "cual te preocupa",
        )
        return short and any(marker in text for marker in markers)

    @staticmethod
    def _entities(raw: str, text: str) -> list[str]:
        # Only accept ticker-shaped tokens that the user actually wrote in
        # uppercase. Converting the whole sentence to uppercase would turn
        # ordinary Spanish words into fake symbols.
        found = [item for item in _TICKER_RE.findall(raw) if item not in _STOP_SYMBOLS]
        for name, ticker in _ALIASES.items():
            if re.search(rf"\b{re.escape(name)}\b", text):
                found.append(ticker)
        return list(dict.fromkeys(found))

    def _classify(
        self,
        text: str,
        entities: list[str],
        state: ConversationState | None,
        follow_up: bool,
    ) -> tuple[str, str, list[str]]:
        if any(term in text for term in ("a y b", "meta-a", "meta-b", "meta-c", "economic meta", "politica meta")):
            return "meta_policy", "explain_shadow_meta_policy", ["meta_policy", "decision_lab"]
        if any(term in text for term in ("decision lab", "plan vs hold", "plan contra hold", "dva", "contrafactual", "counterfactual")):
            return "decision_lab", "compare_recorded_decision_evidence", ["decision_lab"]
        if any(term in text for term in ("cuanto gano", "pnl", "ganancia", "perdio", "ledger")):
            return "performance", "explain_economic_results", ["ledger", "performance"]
        if any(term in text for term in ("hace 20 dias", "decisiones que tomaste", "outcomes", "resultado de decisiones")):
            return "decision_history", "explain_matured_decisions", ["ledger", "outcomes"]
        if any(term in text for term in ("oportunidad", "reemplazar", "en su lugar", "que compraria")):
            return "opportunities", "find_portfolio_alternatives", ["portfolio", "radar", "risk", "decision_lab"]
        if any(term in text for term in ("status", "estado del sistema", "esta funcionando", "salud del sistema")):
            return "system_status", "explain_system_health", ["system_status"]
        if any(term in text for term in ("mercado", "macro", "vix", "dolar", "riesgo pais")):
            return "market_context", "explain_market_context", ["macro"]
        if len(entities) >= 2 and any(term in text for term in ("compar", "cambiar", "vs", "por")):
            return "position_comparison", "compare_positions", ["portfolio", "decision", "risk", "decision_lab"]
        if entities and any(term in text for term in ("por que", "porque", "explic", "motivo", "razon")):
            return "decision_explanation", "explain_current_decision", ["portfolio", "decision", "technical", "macro", "risk", "decision_lab"]
        if entities and any(term in text for term in ("que hago", "conviene", "revis", "analiz", "decision")):
            return "position_analysis", "evaluate_position", ["portfolio", "decision", "technical", "macro", "risk", "decision_lab"]
        if any(term in text for term in ("cartera", "portfolio", "como viene todo", "como esta todo")):
            return "portfolio_review", "review_portfolio", ["portfolio", "decision", "risk"]
        if follow_up and state and state.last_intent:
            return state.last_intent, "continue_previous_task", ["referenced_evidence"]
        return "general", "answer_user", []

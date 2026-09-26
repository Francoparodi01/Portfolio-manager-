from __future__ import annotations

import re
import unicodedata

from src.agentic.diagnostics import question_plan

from .contracts import ConversationState, TaskSpec


_TICKER = re.compile(r"\b[A-Z][A-Z0-9.=-]{1,9}\b")
_HORIZON = re.compile(r"\b(5|10|20|40)\s*(?:D|DIAS|DÍAS|SESIONES)?\b", re.IGNORECASE)
_STOP = {
    "PLAN", "HOLD", "BUY", "SELL", "REDUCE", "DVA", "CI", "IC", "ARS", "USD",
    "PNL", "EV", "SHADOW", "RESEARCH", "PRODUCTION", "META", "TODO", "HOY",
}
_ALIASES = {
    "microsoft": "MSFT", "nvidia": "NVDA", "apple": "AAPL", "amazon": "AMZN",
    "google": "GOOGL", "tesla": "TSLA", "amd": "AMD", "gdx": "GDX",
    "micron": "MU", "iren": "IREN",
}


def _plain(value: str) -> str:
    value = unicodedata.normalize("NFKD", value.lower())
    return "".join(char for char in value if not unicodedata.combining(char))


def _symbols(message: str) -> list[str]:
    found = [token for token in _TICKER.findall(message) if token not in _STOP]
    lowered = _plain(message)
    for alias, ticker in _ALIASES.items():
        if re.search(rf"\b{re.escape(alias)}\b", lowered):
            found.append(ticker)
    return list(dict.fromkeys(found))


def parse_task(message: str, state: ConversationState, recent_context: list[dict] | None = None) -> TaskSpec:
    raw = " ".join(str(message or "").split())
    if not raw:
        raise ValueError("message cannot be empty")
    if len(raw) > 4000:
        raise ValueError("message exceeds 4000 characters")

    lower = _plain(raw)
    entities = _symbols(raw)
    horizons = [int(match) for match in _HORIZON.findall(raw)]
    required: list[str] = []
    optional: list[str] = []
    objective = "answer_user"
    intent = "general"

    # Preserve the existing diagnostic policy as one source of routing hints,
    # but do not make it an exhaustive intent whitelist.
    prior = recent_context or []
    plan = question_plan(raw, prior)
    if plan.intent != "general":
        intent = plan.intent
        required.extend(plan.required_tools)

    if any(term in lower for term in ("cuanto gano", "cuanto gano quantia", "pnl", "performance", "rendimiento", "resultado")):
        intent = "performance" if intent == "general" else intent
        objective = "explain_economic_result"
        required.append("get_performance")
        optional.append("get_ledger_outcomes")

    if any(term in lower for term in ("oportunidad", "reemplazar", "alternativa", "que pondrias", "que comprarias")):
        intent = "opportunity_search" if intent == "general" else intent
        objective = "find_portfolio_alternatives"
        required.extend(["get_portfolio_snapshot", "scan_opportunities"])
        optional.extend(["get_decision_evidence", "compare_plan_vs_hold"])

    if any(term in lower for term in ("como esta mi cartera", "como viene todo", "mi cartera hoy", "mi portfolio")):
        intent = "portfolio_review"
        objective = "summarize_current_portfolio"
        required.extend(["get_portfolio_snapshot", "get_decision_evidence"])
        optional.extend(["get_macro_context", "get_performance"])

    if any(term in lower for term in ("estado del sistema", "funcionando", "anda quantia", "status", "salud del sistema")):
        intent = "system_status"
        objective = "explain_system_health"
        required.append("get_system_status")

    if any(term in lower for term in ("ledger", "hace 20 dias", "hace 10 dias", "hace 5 dias", "hace 40 dias", "decisiones que tomaste")):
        intent = "historical_outcomes" if intent == "general" else intent
        objective = "explain_historical_decision_outcomes"
        required.append("get_ledger_outcomes")

    if entities and any(term in lower for term in ("que hago", "que haria", "por que", "porque", "decision", "vender", "comprar", "holdear", "mantener", "reducir")):
        if intent == "general":
            intent = "position_analysis"
        objective = "explain_position_decision"
        required.extend(["get_portfolio_snapshot", "get_decision_evidence"])
        optional.extend(["analyze_ticker", "compare_plan_vs_hold", "get_decision_value_added"])

    # Short follow-ups inherit structured subjects without replaying the full chat.
    referential = (
        len(raw.split()) <= 8
        or any(term in lower for term in ("por que", "porque", "y si", "comparalo", "comparala", "eso", "esa", "ese", "entonces"))
    )
    inherited: list[str] = []
    if referential and state.active_symbols:
        inherited = list(state.active_symbols)
        for ticker in inherited:
            if ticker not in entities:
                entities.append(ticker)
        if intent == "general" and state.last_intent:
            intent = state.last_intent
            objective = "continue_conversation"

    if "compar" in lower and len(entities) >= 2:
        intent = "position_comparison"
        objective = "compare_positions"
        required.extend(["get_portfolio_snapshot", "get_decision_evidence"])
        optional.extend(["analyze_ticker", "compare_plan_vs_hold"])

    required = list(dict.fromkeys(required))
    optional = [name for name in dict.fromkeys(optional) if name not in required]
    compound = len(required) >= 3 or len(entities) >= 2

    return TaskSpec(
        raw_message=raw,
        intent=intent,
        objective=objective,
        entities=entities,
        horizons=horizons,
        required_tools=required,
        optional_tools=optional,
        inherited_subjects=inherited,
        inherited_from_run_id=state.last_run_id if inherited else None,
        compound=compound,
        ambiguous=not entities and referential and not state.active_symbols and intent == "general",
        verification_required=True,
    )

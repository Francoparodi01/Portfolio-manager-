from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class EvidenceRequirement:
    required_tools: tuple[str, ...]
    required_fields: tuple[str, ...]


@dataclass(frozen=True)
class EvidenceGateResult:
    intent: str
    complete: bool
    required_tools: tuple[str, ...]
    executed_tools: tuple[str, ...]
    missing_tools: tuple[str, ...] = ()
    missing_fields: tuple[str, ...] = ()
    normalized: dict[str, Any] | None = None
    reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "intent": self.intent,
            "complete": self.complete,
            "required_tools": list(self.required_tools),
            "executed_tools": list(self.executed_tools),
            "missing_tools": list(self.missing_tools),
            "missing_fields": list(self.missing_fields),
            "reason": self.reason,
        }


PORTFOLIO_REVIEW_REQUIREMENT = EvidenceRequirement(
    required_tools=("get_portfolio_snapshot", "get_decision_evidence"),
    required_fields=(
        "current_weight",
        "theoretical_target_weight",
        "executable_target_weight",
        "signal_class",
        "portfolio_intent",
        "risk",
        "technical_regime",
        "trend_score",
    ),
)


INTENT_REQUIREMENTS: dict[str, EvidenceRequirement] = {
    "portfolio_review": PORTFOLIO_REVIEW_REQUIREMENT,
}


_CANONICAL_DECISION_FIELDS = set(PORTFOLIO_REVIEW_REQUIREMENT.required_fields)


def _tool_name(item: dict[str, Any]) -> str:
    observation = item.get("observation") or {}
    decision = item.get("decision") or {}
    return str(
        observation.get("tool")
        or observation.get("tool_name")
        or decision.get("tool")
        or decision.get("tool_name")
        or ""
    )


def successful_payloads(history: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    payloads: dict[str, dict[str, Any]] = {}
    for item in history:
        observation = item.get("observation") or {}
        if not observation.get("ok"):
            continue
        name = _tool_name(item)
        if not name:
            continue
        raw = observation.get("content")
        if isinstance(raw, dict):
            payload = raw
        else:
            try:
                payload = json.loads(str(raw or ""))
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
        if isinstance(payload, dict):
            payloads[name] = payload
    return payloads


def _risk_from_signal(signal: dict[str, Any]) -> Any:
    direct = signal.get("risk")
    if direct is not None:
        return direct
    layers = signal.get("layers")
    if not isinstance(layers, list):
        return None
    for layer in layers:
        if not isinstance(layer, dict) or str(layer.get("name") or "").lower() != "risk":
            continue
        if layer.get("raw_score") is not None:
            return layer.get("raw_score")
        if layer.get("score") is not None:
            return layer.get("score")
        if layer.get("weighted") is not None:
            return layer.get("weighted")
    return None


def _ticker(value: Any) -> str:
    return str(value or "").strip().upper()


def normalize_portfolio_review(history: list[dict[str, Any]]) -> dict[str, Any]:
    payloads = successful_payloads(history)
    snapshot = payloads.get("get_portfolio_snapshot") or {}
    evidence = payloads.get("get_decision_evidence") or {}

    signals = evidence.get("signals") if isinstance(evidence.get("signals"), list) else []
    signal_by_ticker = {
        _ticker(row.get("ticker")): row
        for row in signals
        if isinstance(row, dict) and _ticker(row.get("ticker"))
    }

    plan = evidence.get("plan") if isinstance(evidence.get("plan"), dict) else {}
    decisions = plan.get("decisions") if isinstance(plan.get("decisions"), list) else []
    normalized_rows: list[dict[str, Any]] = []
    for decision in decisions:
        if not isinstance(decision, dict):
            continue
        ticker = _ticker(decision.get("ticker"))
        if not ticker:
            continue
        signal = signal_by_ticker.get(ticker, {})
        normalized_rows.append(
            {
                "ticker": ticker,
                "action": decision.get("action"),
                "current_weight": decision.get("current_weight"),
                "theoretical_target_weight": decision.get("theoretical_target_weight"),
                "executable_target_weight": decision.get("executable_target_weight"),
                "signal_class": decision.get("signal_class"),
                "portfolio_intent": decision.get("portfolio_intent"),
                "risk": _risk_from_signal(signal),
                "technical_regime": signal.get("technical_regime"),
                "trend_score": signal.get("trend_score"),
                "reason_primary": decision.get("reason_primary"),
                "reason_secondary": decision.get("reason_secondary"),
            }
        )

    positions = snapshot.get("positions") if isinstance(snapshot.get("positions"), list) else []
    portfolio_tickers = {
        _ticker(row.get("ticker"))
        for row in positions
        if isinstance(row, dict) and _ticker(row.get("ticker"))
    }
    evaluable_tickers = set(signal_by_ticker) | {
        row["ticker"] for row in normalized_rows if row.get("ticker")
    }
    non_evaluable = sorted(portfolio_tickers - evaluable_tickers)

    return {
        "snapshot": snapshot,
        "decision_evidence": evidence,
        "decisions": normalized_rows,
        "non_evaluable_positions": non_evaluable,
        "cash_ars": snapshot.get("cash_ars", evidence.get("cash_ars")),
        "total_value_ars": snapshot.get("total_value_ars", evidence.get("total_value_ars")),
        "snapshot_as_of": snapshot.get("scraped_at") or evidence.get("snapshot_as_of"),
        "evaluated_at": evidence.get("evaluated_at"),
        "snapshot_stale_reason": evidence.get("snapshot_stale_reason"),
    }


def evaluate_evidence(intent: str, goal: str, history: list[dict[str, Any]]) -> EvidenceGateResult:
    del goal  # reserved for intent-specific evidence contracts that depend on requested fields
    requirement = INTENT_REQUIREMENTS.get(intent)
    executed = tuple(dict.fromkeys(_tool_name(item) for item in history if _tool_name(item)))
    if requirement is None:
        return EvidenceGateResult(
            intent=intent,
            complete=False,
            required_tools=(),
            executed_tools=executed,
            reason="No deterministic evidence contract is registered for this intent.",
        )

    payloads = successful_payloads(history)
    missing_tools = tuple(name for name in requirement.required_tools if name not in payloads)
    if missing_tools:
        return EvidenceGateResult(
            intent=intent,
            complete=False,
            required_tools=requirement.required_tools,
            executed_tools=executed,
            missing_tools=missing_tools,
            reason="Required canonical tools have not all completed successfully.",
        )

    if intent == "portfolio_review":
        normalized = normalize_portfolio_review(history)
        decisions = normalized["decisions"]
        complete_rows = [
            row for row in decisions
            if all(row.get(field) is not None for field in requirement.required_fields)
        ]
        missing_fields: list[str] = []
        if not complete_rows:
            for field in requirement.required_fields:
                if not any(row.get(field) is not None for row in decisions):
                    missing_fields.append(field)
            if decisions and not missing_fields:
                missing_fields = [
                    field for field in requirement.required_fields
                    if any(row.get(field) is None for row in decisions)
                ]
        complete = bool(complete_rows)
        reason = (
            "Canonical portfolio snapshot and current decision evidence contain the required decision fields."
            if complete
            else "Canonical tools succeeded but the decision evidence is missing required normalized fields."
        )
        return EvidenceGateResult(
            intent=intent,
            complete=complete,
            required_tools=requirement.required_tools,
            executed_tools=executed,
            missing_fields=tuple(dict.fromkeys(missing_fields)),
            normalized=normalized,
            reason=reason,
        )

    return EvidenceGateResult(
        intent=intent,
        complete=False,
        required_tools=requirement.required_tools,
        executed_tools=executed,
        reason="Evidence contract is registered but has no evaluator.",
    )


def evidence_complete(intent: str, goal: str, history: list[dict[str, Any]]) -> bool:
    return evaluate_evidence(intent, goal, history).complete


def canonical_sql_is_redundant(intent: str, history: list[dict[str, Any]]) -> bool:
    """Block exploratory SQL from reconstructing canonical fields already supplied by canonical tools."""
    requirement = INTENT_REQUIREMENTS.get(intent)
    if requirement is None:
        return False
    payloads = successful_payloads(history)
    if "get_decision_evidence" not in payloads:
        return False
    if intent == "portfolio_review":
        normalized = normalize_portfolio_review(history)
        return any(
            any(row.get(field) is not None for field in _CANONICAL_DECISION_FIELDS)
            for row in normalized.get("decisions", [])
        )
    return False


def _pct(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "N/D"
    return f"{float(value) * 100:.2f}%"


def _number(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "N/D"
    return f"{float(value):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def render_portfolio_review(gate: EvidenceGateResult) -> str:
    """Deterministic safe renderer for current portfolio evidence.

    It intentionally does not derive a frozen target or reinterpret planner actions.
    """
    data = gate.normalized or {}
    decisions = [row for row in data.get("decisions", []) if isinstance(row, dict)]
    complete_rows = [
        row for row in decisions
        if all(row.get(field) is not None for field in PORTFOLIO_REVIEW_REQUIREMENT.required_fields)
    ]
    candidates = complete_rows or decisions
    chosen = None
    if candidates:
        chosen = max(
            candidates,
            key=lambda row: abs(
                float(row.get("theoretical_target_weight") or row.get("current_weight") or 0.0)
                - float(row.get("current_weight") or 0.0)
            ),
        )

    parts: list[str] = []
    if chosen:
        action = str(chosen.get("action") or "N/D")
        parts.append(
            f"{chosen.get('ticker', 'N/D')}: peso actual {_pct(chosen.get('current_weight'))}; "
            f"target teórico del optimizer {_pct(chosen.get('theoretical_target_weight'))}; "
            f"target ejecutable del planner {_pct(chosen.get('executable_target_weight'))}."
        )
        parts.append(
            "SignalClass " + str(chosen.get("signal_class") or "N/D")
            + " · PortfolioIntent " + str(chosen.get("portfolio_intent") or "N/D")
            + " · Risk " + str(chosen.get("risk") if chosen.get("risk") is not None else "N/D")
            + " · Regime " + str(chosen.get("technical_regime") or "N/D")
            + " · Trend " + str(chosen.get("trend_score") if chosen.get("trend_score") is not None else "N/D")
            + f" · acción {action}."
        )
        if action.upper() in {"WATCH", "BLOCKED"}:
            parts.append(
                f"{action.upper()} no es una orden: el target teórico informa la preferencia del optimizer, "
                "pero el target ejecutable conserva lo que el planner permite hacer ahora."
            )
        else:
            parts.append(
                "El target teórico pertenece al optimizer; el ejecutable incorpora las restricciones del planner y es la referencia operativa del plan."
            )
    else:
        parts.append("La evidencia actual no contiene una decisión con todos los campos necesarios para comparar optimizer y planner.")

    frozen = [str(item) for item in data.get("non_evaluable_positions", []) if str(item)]
    if frozen:
        parts.append(
            "Posiciones frozen/no evaluables detectadas por presencia en el snapshot y ausencia en la evidencia evaluable: "
            + ", ".join(frozen)
            + ". No les asigno target 0: la evidencia no informa un frozen_weight explícito del optimizer."
        )
        cash = data.get("cash_ars")
        parts.append(
            "El presupuesto total debe leerse como posiciones optimizables + posiciones frozen/no evaluables + cash"
            + (f" ({_number(cash)} ARS informado)." if isinstance(cash, (int, float)) else ".")
        )
    else:
        parts.append("No detecté posiciones del snapshot ausentes de la evidencia evaluable en este run.")

    stale = data.get("snapshot_stale_reason")
    if stale:
        parts.append(f"Freshness: el snapshot está marcado como desactualizado: {stale}.")
    elif data.get("snapshot_as_of"):
        parts.append(f"Snapshot usado: {data.get('snapshot_as_of')}; análisis: {data.get('evaluated_at') or 'N/D'}.")

    if gate.missing_fields:
        parts.append("Campos faltantes para completar el contrato: " + ", ".join(gate.missing_fields) + ".")
    if gate.missing_tools:
        parts.append("Fuentes canónicas faltantes: " + ", ".join(gate.missing_tools) + ".")
    return "\n".join(parts)


__all__ = [
    "EvidenceRequirement",
    "EvidenceGateResult",
    "INTENT_REQUIREMENTS",
    "PORTFOLIO_REVIEW_REQUIREMENT",
    "canonical_sql_is_redundant",
    "evaluate_evidence",
    "evidence_complete",
    "normalize_portfolio_review",
    "render_portfolio_review",
    "successful_payloads",
]

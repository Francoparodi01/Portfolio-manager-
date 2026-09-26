from __future__ import annotations

import json
import math
from typing import Any

from src.decision_lab.queries import explain_evidence

from .contracts import Evidence, EvidenceQuality, RunState


def _payload(evidence: list[Evidence], tool: str) -> dict[str, Any] | None:
    for item in reversed(evidence):
        if item.tool == tool and item.ok and isinstance(item.data, dict):
            return item.data
    return None


def _num(value: Any, digits: int = 2) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "N/D"
    if not math.isfinite(number):
        return "N/D"
    return f"{number:,.{digits}f}".translate(str.maketrans({",": ".", ".": ","}))


def _money(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "N/D"
    if not math.isfinite(number):
        return "N/D"
    return "$" + f"{number:,.0f}".replace(",", ".")


def _pct(value: Any, digits: int = 1) -> str:
    try:
        number = float(value) * 100
    except (TypeError, ValueError):
        return "N/D"
    if not math.isfinite(number):
        return "N/D"
    return f"{number:+.{digits}f}%"


def _decision_row(evidence: dict[str, Any], ticker: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    plan = evidence.get("plan") or {}
    decisions = plan.get("decisions") or []
    signals = evidence.get("signals") or []
    decision = next((row for row in decisions if str(row.get("ticker") or "").upper() == ticker), None)
    signal = next((row for row in signals if str(row.get("ticker") or "").upper() == ticker), None)
    return decision, signal


def _portfolio_answer(state: RunState) -> str:
    snapshot = _payload(state.evidence, "get_portfolio_snapshot") or {}
    decisions = _payload(state.evidence, "get_decision_evidence") or {}
    positions = snapshot.get("positions") or []
    plan = decisions.get("plan") or {}
    rows = plan.get("decisions") or []
    actionable = [row for row in rows if str(row.get("action") or "").upper() not in {"HOLD", "NONE", ""}]

    first = (
        f"Tu cartera tiene {len(positions)} posiciones y un valor informado de {_money(snapshot.get('total_value_ars'))}; "
        f"cash {_money(snapshot.get('cash_ars'))}."
    )
    if actionable:
        top = actionable[:3]
        first += " El plan actual marca " + ", ".join(
            f"{row.get('ticker')} {row.get('action')}" for row in top
        ) + ("." if len(actionable) <= 3 else f" y {len(actionable)-3} cambios más.")
    else:
        first += " No veo cambios operativos explícitos en el plan consultado."

    detail: list[str] = []
    for row in actionable[:5]:
        detail.append(f"• {row.get('ticker')}: {row.get('reason_primary') or 'sin motivo primario registrado'}")
    if decisions.get("snapshot_stale_reason"):
        detail.append(f"• Frescura: {decisions['snapshot_stale_reason']}")
    if not detail:
        detail.append("• El snapshot por sí solo no alcanza para inferir ganancia, pérdida o necesidad de operar.")
    return first + "\n\n" + "\n".join(detail)


def _position_answer(state: RunState) -> str:
    ticker = state.task.entities[0] if state.task.entities else ""
    current = _payload(state.evidence, "get_decision_evidence") or {}
    decision, signal = _decision_row(current, ticker) if ticker else (None, None)
    if decision:
        action = str(decision.get("action") or "N/D")
        answer = f"Para {ticker}, Quantia está proponiendo {action} en el plan consultado."
        reason = decision.get("reason_primary") or decision.get("reason_secondary")
        if reason:
            answer += f" La razón registrada es: {reason}."
        if signal and signal.get("final_score") is not None:
            answer += f" Score actual: {_num(signal.get('final_score'), 3)}; es una señal, no un retorno esperado."
        current_weight = decision.get("current_weight")
        target_weight = decision.get("target_weight")
        if current_weight is not None or target_weight is not None:
            answer += f" Peso actual {_pct(current_weight)} → objetivo {_pct(target_weight)}."
    else:
        answer = f"No encontré una decisión estructurada vigente para {ticker or 'ese activo'} en la evidencia consultada."

    for tool in ("compare_plan_vs_hold", "get_decision_value_added", "get_decision_counterfactuals"):
        lab = _payload(state.evidence, tool)
        if lab:
            explanation, _ = explain_evidence(lab)
            answer += "\n\nDecision Lab:\n" + explanation
            break
    return answer


def _comparison_answer(state: RunState) -> str:
    current = _payload(state.evidence, "get_decision_evidence") or {}
    if len(state.task.entities) < 2:
        return _position_answer(state)
    rows: list[str] = []
    for ticker in state.task.entities[:4]:
        decision, signal = _decision_row(current, ticker)
        if not decision:
            rows.append(f"• {ticker}: no encontré decisión vigente estructurada.")
            continue
        score = signal.get("final_score") if signal else None
        score_text = _num(score, 3) if score is not None else "N/D"
        rows.append(
            f"• {ticker}: {decision.get('action') or 'N/D'} · score {score_text} · "
            f"peso {_pct(decision.get('current_weight'))} → {_pct(decision.get('target_weight'))}. "
            f"{decision.get('reason_primary') or 'Sin motivo primario registrado.'}"
        )
    return (
        "Los comparo con la misma evidencia vigente de Quantia; no elijo un ganador sólo por score.\n\n"
        + "\n".join(rows)
        + "\n\nPara decidir un reemplazo real todavía hay que comparar sizing, riesgo y evidencia económica bajo una ventana común."
    )


def _performance_answer(state: RunState) -> str:
    analytics = _payload(state.evidence, "get_analytics_v2")
    if analytics:
        economic = analytics.get("economic_pnl") or {}
        if economic.get("economic_pnl_net") is None:
            answer = "PnL neto económico real: N/D. Analytics v2 todavía no puede reconciliar NAV, flujos externos y costos observados."
        else:
            answer = f"PnL neto económico observado: {_money(economic.get('economic_pnl_net'))}."
        metrics = analytics.get("metrics") or []
        preferred = [m for m in metrics if m.get("cost_scenario") == "RESEARCH_BASE"] or metrics
        if preferred:
            lines = []
            for metric in preferred[:8]:
                mean = metric.get("mean") if "mean" in metric else metric.get("ev_mean")
                lines.append(
                    f"• {metric.get('cohort')} {metric.get('horizon_days')}D: n={metric.get('n_observed')}; "
                    f"EV/retorno medio neto observado {_pct(mean) if mean is not None else 'N/D'}"
                )
            answer += "\n\n" + "\n".join(lines)
        return answer

    legacy = _payload(state.evidence, "get_performance")
    if legacy:
        return (
            "Tengo resultados registrados, pero esa fuente es legacy y no equivale a PnL económico neto. "
            "No los sumo como si fueran ganancia real.\n\n"
            + json.dumps(legacy.get("cohorts", [])[:6], ensure_ascii=False)
        )
    return "No obtuve una fuente válida para cuantificar el resultado de Quantia."


def _ledger_answer(state: RunState) -> str:
    payload = _payload(state.evidence, "get_ledger_outcomes") or {}
    metrics = payload.get("metrics") or []
    if not metrics:
        return "No hay outcomes maduros suficientes en el ledger para esa consulta."
    scope = f" de {payload.get('ticker')}" if payload.get("ticker") else ""
    answer = f"Outcomes registrados{scope} en los últimos {payload.get('days')} días:"
    for metric in metrics:
        answer += (
            f"\n• {metric.get('horizon')}D: n={metric.get('n')}, retorno direccional bruto medio "
            f"{_pct(metric.get('mean_recorded_gross_return'))}, positivos {_pct(metric.get('positive_rate'))}."
        )
    answer += "\n\nEsto no es PnL económico reconciliado y no debe sumarse entre decisiones solapadas."
    return answer


def _status_answer(state: RunState) -> str:
    payload = _payload(state.evidence, "get_system_status") or {}
    if not payload:
        return "No pude verificar el estado del sistema."
    return (
        f"Quantia: DB {payload.get('database', 'N/D')} · Redis {payload.get('redis', 'N/D')}.\n"
        f"Último snapshot: {payload.get('latest_portfolio_snapshot') or 'N/D'}\n"
        f"Última decisión: {payload.get('latest_decision') or 'N/D'}\n"
        f"Último precio de mercado: {payload.get('latest_market_price') or 'N/D'}"
    )


def _macro_answer(state: RunState) -> str:
    payload = _payload(state.evidence, "get_macro_context") or {}
    if not payload:
        return "No pude obtener el contexto macro actual."
    labels = (
        ("sp500", "SP500"), ("vix", "VIX"), ("wti", "WTI"), ("ccl", "CCL"),
        ("mep", "MEP"), ("riesgo_pais", "riesgo país"), ("merval", "Merval"),
    )
    values = [f"{label} {_num(payload.get(key))}" for key, label in labels if payload.get(key) is not None]
    answer = "Contexto macro observado: " + (" · ".join(values) if values else "sin indicadores numéricos disponibles") + "."
    missing = payload.get("missing_indicators") or []
    if missing:
        answer += " Faltan: " + ", ".join(map(str, missing)) + "."
    return answer


def _meta_policy_answer(state: RunState) -> str:
    payload = _payload(state.evidence, "get_meta_policy_shadow") or {}
    rows = payload.get("records") or []
    if not rows:
        return "No encontré registros actuales de Economic Meta Policy para esa consulta."
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        ticker = str(row.get("ticker") or "?").upper()
        grouped.setdefault(ticker, []).append(row)
    lines = ["Economic Meta Policy está en SHADOW_ONLY: lo que aprueba o rechaza acá no cambia capital ni la decisión de producción."]
    for ticker, records in sorted(grouped.items()):
        first = records[0]
        parts = []
        for row in sorted(records, key=lambda item: str(item.get("policy_name") or "")):
            name = str(row.get("policy_name") or "META-?")
            decision = str(row.get("decision") or "")
            reason = str(row.get("rejection_reason") or "").replace("|", ", ")
            parts.append(f"{name}: {'pasa' if decision == 'ALLOW_SHADOW' else 'no pasa'}" + (f" ({reason})" if reason else ""))
        lines.append(
            f"• {ticker}: candidato {first.get('candidate_action') or 'N/D'} · score {_num(first.get('candidate_score'), 3)} · "
            + " · ".join(parts)
        )
    lines.append("Que una META pase significa que el candidato supera ese filtro experimental; no significa comprar o vender en producción.")
    return "\n".join(lines)


def _opportunity_answer(state: RunState) -> str:
    radar = next((item for item in state.evidence if item.tool == "scan_opportunities" and item.ok), None)
    if not radar:
        return "No pude obtener oportunidades del radar en esta corrida."
    current = _payload(state.evidence, "get_portfolio_snapshot") or {}
    count = len(current.get("positions") or [])
    return (
        f"Busqué alternativas contra tu cartera actual ({count} posiciones). El radar devolvió esta evidencia:\n\n"
        + radar.excerpt[:2200]
        + "\n\nLa lista es candidata/research; antes de reemplazar una posición hay que comparar riesgo, sizing y evidencia PLAN vs HOLD bajo la misma ventana."
    )


def synthesize(state: RunState) -> str:
    """Format verified Quantia facts without inventing market values or policy math."""
    intent = state.task.intent
    if intent == "portfolio_review":
        return _portfolio_answer(state)
    if intent in {"position_analysis", "decision_lab_mechanism", "explain_plan"}:
        return _position_answer(state)
    if intent == "position_comparison":
        return _comparison_answer(state)
    if intent == "performance":
        return _performance_answer(state)
    if intent == "historical_outcomes":
        return _ledger_answer(state)
    if intent == "system_status":
        return _status_answer(state)
    if intent == "macro_context":
        return _macro_answer(state)
    if intent == "meta_policy":
        return _meta_policy_answer(state)
    if intent == "opportunity_search":
        return _opportunity_answer(state)

    for tool in (
        "compare_plan_vs_hold", "get_decision_value_added", "get_decision_counterfactuals",
        "get_similar_historical_episodes", "get_replay_evidence_quality", "compare_strategy_versions",
    ):
        payload = _payload(state.evidence, tool)
        if payload:
            explanation, _ = explain_evidence(payload)
            return explanation

    successful = [item for item in state.evidence if item.ok]
    if not successful:
        return "No tengo evidencia suficiente para responder esa consulta sin inventar datos."
    lines = ["Esto es lo que pude verificar en Quantia:"]
    for item in successful[:5]:
        lines.append(f"\n[{item.tool}]\n{item.excerpt[:1200]}")
    if any(item.quality in {EvidenceQuality.LOW, EvidenceQuality.INSUFFICIENT} for item in successful):
        lines.append("\nParte de la evidencia tiene calidad baja/insuficiente; no la uso como prueba de edge.")
    return "\n".join(lines)

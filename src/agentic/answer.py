"""Source-bound closing report; the controller chooses tools, not financial facts.

The small local controller is not a reliable free-form financial summarizer.
Render observed values and labelled excerpts instead of publishing its prose.
No network, new valuation, economic inference, or tool execution happens here.
"""
from __future__ import annotations

import json
import math
from typing import Any

from .contracts import AgentDecision, AgentModelError


def _number(value: Any, digits: int = 2) -> str:
    if value is None or isinstance(value, bool):
        return "N/D"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "N/D"
    if not math.isfinite(number):
        return "N/D"
    return f"{number:,.{digits}f}".translate(str.maketrans({",": ".", ".": ","}))


def _section(content: str, label: str) -> str:
    marker = f"━━━ {label} ━━━"
    if marker not in content:
        return ""
    return content.split(marker, 1)[1].split("━━━", 1)[0].strip()


def _excerpt(content: str, limit: int = 1800) -> str:
    if len(content) <= limit:
        return content
    return content[:limit] + "\n[Extracto; fuente completa en la traza.]"


def _source_card(tool: str, content: str) -> tuple[str, list[str]]:
    try:
        data = json.loads(content)
    except (ValueError, TypeError):
        data = None
    if not isinstance(data, dict):
        data = {}

    if data.get("schema_version") == "decision-lab-agent-evidence-v1":
        from src.decision_lab.queries import explain_evidence
        answer, _status = explain_evidence(data)
        return answer, ["El mecanismo interno no prueba valor económico; la evidencia del Lab conserva su calidad y cohorte."]

    if tool == "get_portfolio_snapshot" and data:
        positions = data.get("positions")
        position_count = len(positions) if isinstance(positions, list) else "N/D"
        positions = positions if isinstance(positions, list) else []
        positions = [p for p in positions if isinstance(p, dict)]
        rows = [
            f"Snapshot de cuenta: {data.get('scraped_at') or 'fecha no informada'}.",
            f"Valor informado: {_number(data.get('total_value_ars'))} ARS; "
            f"cash: {_number(data.get('cash_ars'))} ARS; posiciones informadas: {position_count}.",
        ]
        holdings = []
        for position in positions[:12]:
            weight = position.get("weight")
            try:
                weight = float(weight) * 100 if weight is not None and not isinstance(weight, bool) else None
            except (ValueError, TypeError):
                weight = None
            holdings.append(f"{position.get('ticker') or 'N/D'} {_number(weight, 1)}%")
        if holdings:
            rows.append("Pesos informados: " + "; ".join(holdings) + ".")
        if len(positions) > 12:
            rows.append("Primeras 12 posiciones; resto en la traza.")
        limits = ["El snapshot no reconcilia NAV, flujos externos y costos: no establece Economic PnL Net."]
        if positions and any(p.get("pnl_pct") is None for p in positions):
            limits.append("Hay posiciones sin PnL porcentual informado; no se las trata como retorno cero.")
        return "\n".join(rows), limits

    if tool == "get_decision_evidence" and data:
        signals = data.get("signals")
        signals = signals if isinstance(signals, list) else []
        signals = [item for item in signals if isinstance(item, dict)]
        rows = [
            f"Decisiones evaluadas: {data.get('evaluated_at') or 'fecha no informada'}.",
            f"Snapshot de referencia: {data.get('snapshot_as_of') or 'fecha no informada'}.",
        ]
        if data.get("analysis_run_id"):
            rows.append(f"Run de análisis: {data.get('analysis_run_id')}.")
        compact = []
        for signal in signals[:12]:
            ticker = str(signal.get("ticker") or "N/D")
            decision = str(signal.get("decision") or "N/D")
            score = _number(signal.get("final_score"), 3)
            compact.append(f"{ticker} {decision} (score {score})")
        if compact:
            rows.append("Señales actuales: " + "; ".join(compact) + ".")
        if len(signals) > 12:
            rows.append("Primeras 12 señales; resto en la traza.")
        limits = [
            "Las decisiones y scores describen la señal actual del motor; no son fills, retornos ni PnL.",
            "Esta evidencia por sí sola no prueba edge económico frente a HOLD.",
        ]
        return "\n".join(rows), limits

    if tool == "get_macro_context" and data:
        labels = {"sp500": "SP500", "vix": "VIX", "wti": "WTI", "ccl": "CCL", "mep": "MEP",
                  "riesgo_pais": "riesgo país (pb)", "merval": "Merval", "reservas": "reservas"}
        values = [f"{label}: {_number(data.get(key))}" for key, label in labels.items() if data.get(key) is not None]
        missing = data.get("missing_indicators") or []
        limits = ["Indicadores no informados por esta consulta macro: " + ", ".join(map(str, missing)) + "."] if missing else []
        return (f"Contexto consultado: {data.get('fetched_at') or 'fecha no informada'}.\n"
                + ("; ".join(values) + "." if values else "Sin indicadores numéricos reconocibles; ver fuente completa.")), limits

    if tool in {"analyze_portfolio", "analyze_ticker"}:
        date_line = content.splitlines()[0] if content else "Fecha no informada."
        if tool == "analyze_portfolio":
            plan = _section(content, "PLAN AHORA")
            if plan:
                return (f"{date_line}\nPropuesta del motor en modo consulta (no son fills):\n{_excerpt(plan)}",
                        ["No se verificó la ejecución de este plan. Scores y montos propuestos no son retornos ni PnL."])
        else:
            instruments = _section(content, "CARTERA")
            if instruments:
                return (f"{date_line}\nAnálisis de instrumento aislado; sus ceros de cartera/cash no describen tu cuenta.\n"
                        + _excerpt(instruments, 900),
                        ["Un score aislado no prueba rentabilidad ni confirma edge."])

    return ("Extracto literal de la herramienta:\n" + _excerpt(content),
            [f"{tool}: el extracto no agrega validaciones que la propia fuente no haya realizado."])


def _successful_tool_content(history: list[dict[str, Any]]) -> dict[str, str]:
    result: dict[str, str] = {}
    for item in history:
        observation = item.get("observation") or {}
        if not observation.get("ok"):
            continue
        decision = item.get("decision") or {}
        tool = str(observation.get("tool") or observation.get("tool_name") or decision.get("tool") or "")
        if tool:
            result[tool] = str(observation.get("content") or "")
    return result


def _portfolio_review_fallback(goal: str, history: list[dict[str, Any]]) -> str | None:
    normalized_goal = str(goal or "").lower()
    if "cartera" not in normalized_goal and "portfolio" not in normalized_goal:
        return None

    tools = _successful_tool_content(history)
    required = {"get_portfolio_snapshot", "get_decision_evidence"}
    if not required.issubset(tools):
        return None

    try:
        snapshot = json.loads(tools["get_portfolio_snapshot"])
        decisions = json.loads(tools["get_decision_evidence"])
    except (ValueError, TypeError):
        return None
    if not isinstance(snapshot, dict) or not isinstance(decisions, dict):
        return None

    positions = snapshot.get("positions")
    positions = positions if isinstance(positions, list) else []
    positions = [item for item in positions if isinstance(item, dict)]
    ranked = sorted(
        positions,
        key=lambda item: float(item.get("weight") or 0.0) if not isinstance(item.get("weight"), bool) else 0.0,
        reverse=True,
    )
    holdings = []
    for position in ranked[:6]:
        try:
            weight = float(position.get("weight")) * 100
        except (TypeError, ValueError):
            continue
        holdings.append(f"{position.get('ticker') or 'N/D'} {_number(weight, 1)}%")

    signals = decisions.get("signals")
    signals = signals if isinstance(signals, list) else []
    signals = [item for item in signals if isinstance(item, dict)]
    active = [item for item in signals if str(item.get("decision") or "").upper() != "HOLD"]
    hold_count = sum(1 for item in signals if str(item.get("decision") or "").upper() == "HOLD")
    signal_bits = [
        f"{item.get('ticker') or 'N/D'} {str(item.get('decision') or 'N/D')} (score {_number(item.get('final_score'), 3)})"
        for item in active[:4]
    ]

    lines = [
        f"Tu cartera tiene {_number(snapshot.get('total_value_ars'))} ARS, "
        f"{_number(snapshot.get('cash_ars'))} ARS de cash y {len(positions)} posiciones.",
    ]
    if holdings:
        lines.append("La mayor concentración está en " + ", ".join(holdings) + ".")
    if signal_bits:
        suffix = f"; {hold_count} posiciones siguen en HOLD" if hold_count else ""
        lines.append("Las señales no-HOLD actuales son " + "; ".join(signal_bits) + suffix + ".")
    elif signals:
        lines.append(f"Las {len(signals)} señales actuales están en HOLD.")

    evaluated_at = decisions.get("evaluated_at")
    snapshot_as_of = decisions.get("snapshot_as_of")
    if evaluated_at:
        reference = f" sobre snapshot {snapshot_as_of}" if snapshot_as_of else ""
        lines.append(f"Señales evaluadas {evaluated_at}{reference}.")
    lines.append("Las señales son una lectura del motor en modo consulta: no son fills, operaciones ejecutadas ni rentabilidad realizada.")
    return "\n".join(lines)


def evidence_decision(goal: str, history: list[dict[str, Any]]) -> AgentDecision:
    portfolio_answer = _portfolio_review_fallback(goal, history)
    if portfolio_answer:
        return AgentDecision(
            kind="final",
            answer=portfolio_answer,
            rationale="Resumen determinístico de cartera desde snapshot y decisiones estructuradas observadas.",
            answer_origin="portfolio_renderer_v3",
        )

    cards, limits = [], []
    observed_tools = set()
    successful = 0
    for item in history:
        observation = item.get("observation")
        if observation is None:
            continue
        decision = item.get("decision") or {}
        tool = str(observation.get("tool") or observation.get("tool_name") or decision.get("tool") or "fuente")
        if not observation.get("ok"):
            limits.append(f"{tool}: consulta sin resultado válido; detalle técnico en la traza.")
            continue
        successful += 1
        observed_tools.add(tool)
        body, source_limits = _source_card(tool, str(observation.get("content") or ""))
        cards.append(f"[{tool}]\n{body}")
        limits.extend(source_limits)
    if not successful:
        raise AgentModelError("no successful tool evidence for the closing report")
    if (
        "get_portfolio_snapshot" in observed_tools
        and "analyze_portfolio" not in observed_tools
        and "get_decision_evidence" not in observed_tools
    ):
        limits.insert(0, "En esta corrida no se obtuvo evidencia de decisiones: no se verificó la lectura actual del motor.")

    # Reserve space for all sources and limitations even in a 20-step CLI run.
    card_budget = min(2600, 7600 // len(cards))
    cards = [_excerpt(card, card_budget) for card in cards]
    limits = list(dict.fromkeys(limits))
    answer = (
        "Resumen: Revisé la evidencia disponible para tu consulta: «" + _excerpt(goal, 400) + "». "
        f"Consultas con resultado: {successful}. Este cierre describe sus datos y límites; "
        "no establece una operación ni una rentabilidad validada.\n\n"
        "Evidencia:\n" + "\n\n".join(cards)
        + "\n\nFaltantes y límites:\n- " + "\n- ".join(limits)
        + "\n- Sólo se verificó lo consultado. La traza conserva las fuentes; no certifica la calidad económica de sus señales."
    )
    return AgentDecision(kind="final", answer=answer,
                         rationale="Cierre determinístico con valores y extractos de fuentes observadas; sin inferencias financieras del LLM.",
                         answer_origin="evidence_renderer_v1")

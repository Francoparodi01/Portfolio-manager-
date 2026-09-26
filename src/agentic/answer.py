"""Source-bound closing report; the controller chooses tools, not financial facts.

The small local controller is not a reliable free-form financial summarizer.
Render observed values and labelled excerpts instead of publishing its prose.
No network, new valuation, economic inference, or tool execution happens here.
"""
from __future__ import annotations

import json
import math
import re
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from .contracts import AgentDecision, AgentModelError


_ART = ZoneInfo("America/Argentina/Buenos_Aires")


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


def _signed_money(value: Any) -> str:
    if value is None or isinstance(value, bool):
        return "N/D"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "N/D"
    if not math.isfinite(number):
        return "N/D"
    sign = "+" if number > 0 else "-" if number < 0 else ""
    return f"{sign}${_number(abs(number), 0)}"


def _fmt_art(value: Any) -> str:
    if not value:
        return "fecha no informada"
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.strftime("%d/%m %H:%M")
        return parsed.astimezone(_ART).strftime("%d/%m %H:%M ART")
    except (TypeError, ValueError):
        return str(value)


def _section(content: str, label: str) -> str:
    marker = f"━━━ {label} ━━━"
    if marker not in content:
        return ""
    return content.split(marker, 1)[1].split("━━━", 1)[0].strip()


def _excerpt(content: str, limit: int = 1800) -> str:
    if len(content) <= limit:
        return content
    return content[:limit] + "\n[Extracto; fuente completa en la traza.]"


def _strip_html(text: str) -> str:
    clean = re.sub(r"<[^>]+>", "", str(text or ""))
    return re.sub(r"[ \t]+", " ", clean).strip()


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

    if tool == "get_run_evidence_provenance" and data:
        sources = data.get("sources") if isinstance(data.get("sources"), list) else []
        return (
            f"Run referido: {data.get('referenced_run_id') or 'N/D'} · fuentes auditadas: {len(sources)}.",
            ["La procedencia describe el turno anterior; no vuelve a consultar fuentes de mercado."],
        )

    if tool == "get_bot_follow_pnl" and data:
        rows = [f"Ventana observada: {int(data.get('lookback_days') or 0)} días."]
        for horizon in (5, 10, 20):
            n = int(data.get(f"plans_closed_{horizon}d") or 0)
            pnl = data.get(f"bot_pnl_{horizon}d_ars")
            if n and pnl is not None:
                rows.append(f"{horizon}D: {_signed_money(pnl)} ARS · {n} planes maduros.")
        if len(rows) == 1:
            rows.append("No hay outcomes maduros suficientes en esa ventana.")
        return "\n".join(rows), [
            "PnL hipotético del bot, no PnL realizado de la cuenta.",
            "Resultado direccional bruto antes de fees/slippage y plan-level no deduplicado.",
            "5D/10D/20D son cortes alternativos: no se suman entre sí.",
        ]

    if tool == "get_normalized_bot_follow_pnl" and data:
        rows = [
            f"Ventana observada: {int(data.get('lookback_days') or 0)} días; "
            f"{int(data.get('raw_plans_total') or 0)} planes → {int(data.get('episodes_total') or 0)} episodios."
        ]
        excluded = int(data.get("excluded_missing_notional") or 0)
        if excluded:
            rows.append(f"Cobertura: {excluded} planes quedaron fuera por no tener notional positivo persistido.")
        for horizon in (5, 10, 20):
            n = int(data.get(f"episodes_closed_{horizon}d") or 0)
            pnl = data.get(f"pnl_{horizon}d_ars")
            if n and pnl is not None:
                rows.append(f"{horizon}D: {_signed_money(pnl)} ARS · {n} episodios maduros.")
        return "\n".join(rows), [
            "Contrafactual deduplicado por episodio de recomendación; no representa fills humanos.",
            "Resultado bruto antes de fees/slippage; 5D/10D/20D no se suman.",
        ]

    if tool == "get_portfolio_snapshot" and data:
        positions = data.get("positions")
        position_count = len(positions) if isinstance(positions, list) else "N/D"
        positions = positions if isinstance(positions, list) else []
        positions = [p for p in positions if isinstance(p, dict)]
        rows = [
            f"Snapshot de cuenta: {_fmt_art(data.get('scraped_at'))}.",
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

    if tool in {"get_decision_evidence", "get_persisted_decision_evidence"} and data:
        signals = data.get("signals")
        signals = signals if isinstance(signals, list) else []
        signals = [item for item in signals if isinstance(item, dict)]
        rows = [
            f"Decisiones evaluadas: {_fmt_art(data.get('evaluated_at'))}.",
            f"Snapshot usado por esa corrida: {_fmt_art(data.get('snapshot_as_of'))}.",
        ]
        if data.get("analysis_run_id"):
            rows.append(f"Run de análisis: {data.get('analysis_run_id')}.")
        compact = []
        for signal in signals[:12]:
            ticker = str(signal.get("ticker") or "N/D")
            decision = str(signal.get("decision") or "N/D")
            status = str(signal.get("status") or "").upper()
            score = _number(signal.get("final_score"), 3)
            status_suffix = ""
            if status in {"BLOCKED", "REJECTED"}:
                status_suffix = " · bloqueada"
            elif status and status not in {"OBSERVED", "APPROVED", "EXECUTED"}:
                status_suffix = f" · {status.lower()}"
            compact.append(f"{ticker} {decision} (score {score}){status_suffix}")
        if compact:
            rows.append("Señales observadas: " + "; ".join(compact) + ".")
        if len(signals) > 12:
            rows.append("Primeras 12 señales; resto en la traza.")
        limits = [
            "Las decisiones y scores describen la señal observada del motor; no son fills, retornos ni PnL.",
            "Esta evidencia por sí sola no prueba edge económico frente a HOLD.",
        ]
        if tool == "get_persisted_decision_evidence":
            limits.insert(0, "Lectura rápida de la última corrida formal persistida; el chat no recalculó el análisis completo en este turno.")
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


def _provenance_fallback(history: list[dict[str, Any]]) -> str | None:
    raw = _successful_tool_content(history).get("get_run_evidence_provenance")
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return None
    if not isinstance(data, dict) or data.get("schema_version") != "run-evidence-provenance-v1":
        return None
    if data.get("status") != "observed":
        return "No encuentro una respuesta anterior auditada dentro de esta conversación para listar sus fuentes."

    sources = data.get("sources") if isinstance(data.get("sources"), list) else []
    lines = ["Para la respuesta inmediatamente anterior usé exactamente estas fuentes auditadas:"]
    for source in sources:
        if not isinstance(source, dict):
            continue
        tool = str(source.get("tool") or "fuente")
        origin = str(source.get("source") or "fuente interna no etiquetada")
        lookback = source.get("lookback_days")
        suffix = f" · ventana {int(lookback)} días" if isinstance(lookback, (int, float)) else ""
        lines.append(f"• {tool}: {origin}{suffix}.")
        normalized = source.get("normalized_component")
        if isinstance(normalized, dict):
            raw_total = int(normalized.get("raw_plans_total") or 0)
            episodes = int(normalized.get("episodes_total") or 0)
            removed = int(normalized.get("duplicates_removed") or 0)
            lines.append(
                f"  Para el normalizado usé {raw_total} planes formales → {episodes} episodios independientes; {removed} reiteraciones quedaron deduplicadas."
            )
    if not sources:
        lines.append("• El turno anterior no registró una herramienta de evidencia exitosa.")
    lines.append(f"Run auditado: {data.get('referenced_run_id') or 'N/D'}.")
    lines.append("No vuelvo a consultar macro, cartera o estado del sistema para contestar esta pregunta de procedencia.")
    return "\n".join(lines)


def _normalized_bot_pnl_fallback(history: list[dict[str, Any]]) -> str | None:
    raw = _successful_tool_content(history).get("get_normalized_bot_follow_pnl")
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return None
    if not isinstance(data, dict) or data.get("schema_version") != "bot-follow-pnl-normalized-v1":
        return None
    if data.get("status") == "unavailable":
        return (
            f"No pude calcular el contrafactual normalizado de los últimos {int(data.get('lookback_days') or 0)} días "
            "con evidencia suficiente. No reemplazo ese dato con el plan-level ni con operaciones realmente seguidas."
        )

    days = int(data.get("lookback_days") or 0)
    raw_total = int(data.get("raw_plans_total") or 0)
    episodes_total = int(data.get("episodes_total") or 0)
    removed = int(data.get("duplicates_removed") or 0)
    excluded = int(data.get("excluded_missing_notional") or 0)
    lines = [
        f"Tomando los últimos {days} días y deduplicando recomendaciones repetidas, {raw_total} planes del bot quedan en {episodes_total} episodios independientes ({removed} reiteraciones removidas)."
    ]
    if excluded:
        lines.append(f"Cobertura: {excluded} planes no entran al PnL porque no tienen un notional positivo persistido; no les asigno capital ficticio.")
    mature = 0
    for horizon in (5, 10, 20):
        n = int(data.get(f"episodes_closed_{horizon}d") or 0)
        pnl = data.get(f"pnl_{horizon}d_ars")
        if n and pnl is not None:
            mature += 1
            lines.append(f"• A {horizon}D: {_signed_money(pnl)} ARS sobre {n} episodios maduros.")
    if not mature:
        lines.append("Todavía no hay episodios maduros suficientes para estimar ese contrafactual en esta ventana.")
    lines.append(
        "Normalización: una misma acción BUY/SELL sobre el mismo ticker en corridas formales consecutivas cuenta una sola vez; un cambio de lado o una corrida intermedia sin esa recomendación abre un episodio nuevo."
    )
    lines.append(
        "Es un PnL contrafactual bruto y deduplicado, no PnL realizado de la cuenta; no descuenta costos ni slippage."
    )
    if mature > 1:
        lines.append("5D/10D/20D son escenarios alternativos y no deben sumarse.")
    return "\n".join(lines)


def _bot_follow_pnl_fallback(goal: str, history: list[dict[str, Any]]) -> str | None:
    tools = _successful_tool_content(history)
    raw = tools.get("get_bot_follow_pnl")
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return None
    if not isinstance(data, dict) or data.get("schema_version") != "bot-follow-pnl-v1":
        return None

    days = int(data.get("lookback_days") or 0)
    total = int(data.get("plans_total") or 0)
    lines = [f"Tomando los últimos {days} días, Quantia registró {total} planes ejecutables del bot."]
    mature = 0
    for horizon in (5, 10, 20):
        n = int(data.get(f"plans_closed_{horizon}d") or 0)
        pnl = data.get(f"bot_pnl_{horizon}d_ars")
        if n and pnl is not None:
            mature += 1
            lines.append(f"• A {horizon}D: {_signed_money(pnl)} ARS sobre {n} planes maduros.")
    if not mature:
        lines.append("Todavía no hay outcomes maduros suficientes para estimar ese PnL en esta ventana.")
    lines.append(
        "Es PnL direccional bruto hipotético a nivel plan: no es PnL realizado, no descuenta costos y puede repetir recomendaciones entre corridas."
    )
    if mature > 1:
        lines.append("Los horizontes 5D/10D/20D son escenarios alternativos y no deben sumarse.")
    return "\n".join(lines)


def _portfolio_review_fallback(goal: str, history: list[dict[str, Any]]) -> str | None:
    normalized_goal = str(goal or "").lower()
    if "cartera" not in normalized_goal and "portfolio" not in normalized_goal:
        return None

    tools = _successful_tool_content(history)
    if "get_portfolio_snapshot" not in tools:
        return None
    decision_tool = (
        "get_persisted_decision_evidence"
        if "get_persisted_decision_evidence" in tools
        else "get_decision_evidence"
        if "get_decision_evidence" in tools
        else None
    )
    if decision_tool is None:
        return None

    try:
        snapshot = json.loads(tools["get_portfolio_snapshot"])
        decisions = json.loads(tools[decision_tool])
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
    signal_bits = []
    for item in active[:4]:
        ticker = item.get("ticker") or "N/D"
        decision = str(item.get("decision") or "N/D")
        score = _number(item.get("final_score"), 3)
        status = str(item.get("status") or "").upper()
        suffix = " · bloqueada" if status in {"BLOCKED", "REJECTED"} else ""
        signal_bits.append(f"{ticker} {decision} (score {score}){suffix}")

    lines = [
        f"Tu cartera tiene {_number(snapshot.get('total_value_ars'))} ARS, "
        f"{_number(snapshot.get('cash_ars'))} ARS de cash y {len(positions)} posiciones.",
    ]
    if holdings:
        lines.append("La mayor concentración está en " + ", ".join(holdings) + ".")
    if signal_bits:
        suffix = f"; {hold_count} posiciones siguen en HOLD" if hold_count else ""
        lines.append("Las señales no-HOLD del último análisis son " + "; ".join(signal_bits) + suffix + ".")
    elif signals:
        lines.append(f"Las {len(signals)} señales del último análisis están en HOLD.")
    else:
        lines.append("No hay una corrida formal de decisiones persistida con señales disponibles para mostrar.")

    evaluated_at = decisions.get("evaluated_at")
    decision_snapshot_as_of = decisions.get("snapshot_as_of")
    current_snapshot_as_of = snapshot.get("scraped_at")
    if evaluated_at:
        reference = f", sobre snapshot {_fmt_art(decision_snapshot_as_of)}" if decision_snapshot_as_of else ""
        prefix = "Último análisis formal" if decision_tool == "get_persisted_decision_evidence" else "Señales evaluadas"
        lines.append(f"{prefix}: {_fmt_art(evaluated_at)}{reference}.")
    if current_snapshot_as_of and decision_snapshot_as_of and str(current_snapshot_as_of) != str(decision_snapshot_as_of):
        lines.append(
            f"La cartera actual fue refrescada {_fmt_art(current_snapshot_as_of)}; las señales anteriores no se recalcularon con ese snapshot nuevo."
        )
    if decision_tool == "get_persisted_decision_evidence":
        lines.append("Respuesta rápida: usa la última corrida formal guardada; no volvió a ejecutar el análisis completo en este turno.")
    lines.append("Las señales son evidencia del motor: no son fills, operaciones ejecutadas ni rentabilidad realizada.")
    return "\n".join(lines)


def evidence_decision(goal: str, history: list[dict[str, Any]]) -> AgentDecision:
    provenance_answer = _provenance_fallback(history)
    if provenance_answer:
        return AgentDecision(
            kind="final",
            answer=provenance_answer,
            rationale="Procedencia reconstruida desde la traza auditada del turno anterior.",
            answer_origin="provenance_renderer_v1",
        )

    normalized_bot_answer = _normalized_bot_pnl_fallback(history)
    if normalized_bot_answer:
        return AgentDecision(
            kind="final",
            answer=normalized_bot_answer,
            rationale="Contrafactual del bot deduplicado por episodios de recomendación.",
            answer_origin="bot_follow_pnl_normalized_renderer_v1",
        )

    bot_pnl_answer = _bot_follow_pnl_fallback(goal, history)
    if bot_pnl_answer:
        return AgentDecision(
            kind="final",
            answer=bot_pnl_answer,
            rationale="Resumen determinístico del contrafactual plan-level del bot desde evidencia persistida.",
            answer_origin="bot_follow_pnl_renderer_v3",
        )

    portfolio_answer = _portfolio_review_fallback(goal, history)
    if portfolio_answer:
        return AgentDecision(
            kind="final",
            answer=portfolio_answer,
            rationale="Resumen determinístico de cartera desde snapshot y decisiones observadas.",
            answer_origin="portfolio_renderer_v5",
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
    if "get_portfolio_snapshot" in observed_tools and "analyze_portfolio" not in observed_tools:
        limits.insert(0, "En esta corrida no se obtuvo el análisis de cartera: no se verificaron el plan actual ni sus controles.")

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

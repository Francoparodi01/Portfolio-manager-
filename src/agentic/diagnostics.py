"""Question-specific evidence requirements and operational explanations.

This policy explains Quantia's mechanics. It never promotes a proposed trade
into evidence that buying, selling or holding is economically preferable.
"""
from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass

from .answer import evidence_decision, _number
from .contracts import AgentDecision


@dataclass(frozen=True)
class QuestionPlan:
    intent: str = "general"
    required_tools: tuple[str, ...] = ()
    user_hypotheses: tuple[str, ...] = ()
    inherited_from: str | None = None


def _plain(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", text.lower()) if not unicodedata.combining(c))


def question_plan(goal: str, context: list[dict] | None = None) -> QuestionPlan:
    text = _plain(goal)
    if any(word in text for word in ("decision lab", "contrafactual", "counterfactual", "dva", "plan vs hold", "plan contra hold", "replay", "versiones", "valor agregado", "hubiera mantenido")):
        intent, required = "decision_lab", ("get_decision_value_added",)
    elif any(word in text for word in ("reduc", "vend", "microsoft", "msft")) and any(word in text for word in ("por que", "porque", "explica", "quiere")):
        intent, required = "decision_lab_mechanism", ("get_decision_evidence", "get_decision_value_added")
    elif any(word in text for word in ("cedear", "cdeear")) and any(word in text for word in ("riesgo", "argentin", "wall street")):
        intent, required = "cedear_risk", ("get_macro_exposure",)
    elif any(word in text for word in ("recesion", "boom", "tendencia alcista", "compra/hold", "recuperacion")):
        intent, required = "market_thesis", ("get_decision_evidence",)
    elif any(word in text for word in ("compr", "bloque", "optimizador", "guard")) and any(word in text for word in ("por que", "porque", "explic", "no ", "bloque")):
        intent, required = "explain_plan", ("get_decision_evidence",)
    elif any(word in text for word in ("cartera", "portfolio", "portafolio")) and any(word in text for word in ("revis", "analiz", "evalu", "decid")):
        intent, required = "portfolio_review", ("get_portfolio_snapshot", "get_decision_evidence")
    else:
        intent, required = "general", ()
    prior = context or []
    inherited = None
    if intent == "general" and any(word in text for word in ("por que", "porque", "eso", "entonces", "y ahora")):
        for turn in reversed(prior):
            previous = question_plan(str(turn.get("goal", "")))
            if previous.intent != "general":
                intent, required, inherited = previous.intent, previous.required_tools, str(turn.get("run_id"))
                break
    hypotheses = []
    for message in [*(str(turn.get("goal", "")) for turn in prior), goal]:
        plain = _plain(message)
        if any(term in plain for term in ("recesion", "boom", "tendencia alcista", "recuperacion")):
            hypotheses.append(message[:500])
    return QuestionPlan(intent, required, tuple(dict.fromkeys(hypotheses)), inherited)


def observed_payloads(history: list[dict]) -> dict[str, dict]:
    found = {}
    for step in history:
        obs = step.get("observation") or {}
        if not obs.get("ok"):
            continue
        try:
            payload = json.loads(obs.get("content", ""))
        except (ValueError, TypeError):
            continue
        if isinstance(payload, dict):
            found[obs.get("tool") or obs.get("tool_name") or (step.get("decision") or {}).get("tool")] = payload
    return found


def diagnostic_decision(goal: str, history: list[dict], plan: QuestionPlan,
                        context: list[dict] | None = None) -> AgentDecision:
    if plan.intent in {"decision_lab", "decision_lab_mechanism"}:
        from src.decision_lab.queries import explain_evidence
        payloads = observed_payloads(history)
        explanation, status = explain_evidence(payloads.get("get_decision_value_added"))
        if plan.intent == "decision_lab_mechanism":
            mechanism = diagnostic_decision(goal, history, QuestionPlan("explain_plan", ("get_decision_evidence",)), context)
            explanation = "Mecanismo actual\n" + mechanism.answer + "\n\n" + explanation
        return AgentDecision(kind="final", answer=explanation, rationale="Stored quantitative evidence; no LLM return calculation.",
                             answer_origin="diagnostics_v2", objective_status=status)
    if plan.intent == "general":
        return evidence_decision(goal, history)
    payloads = observed_payloads(history)
    parts, gaps = [], []
    status = "EXPLAINED"
    if context:
        previous = context[-1]
        parts.append(f"Contexto: retomo tus consultas recientes (último run {previous['run_id'][:8]}). "
                     "Tus planteos previos son contexto, no hechos de mercado verificados.")

    if plan.intent == "cedear_risk":
        data = payloads.get("get_macro_exposure")
        parts.append("Un CEDEAR representa un activo extranjero y se negocia localmente. "
                     "Hay que separar el riesgo del subyacente, el efecto cambiario y las condiciones locales; "
                     "no asumir que todos son empresas estadounidenses ni aplicarles automáticamente una lectura de deuda soberana. "
                     "Fuente: https://www.byma.com.ar/productos/productos-financieros/cedears")
        if data:
            macro, probe = data["macro"], data["argentina_label_probe"]
            if data.get("data_status") == "PARTIAL":
                status = "PARTIAL"
                gaps.append("Faltan datos macro: no convierto sus ausencias en valores observados.")
            parts.append(f"En la consulta {macro.get('fetched_at')}: riesgo país {_number(macro.get('riesgo_pais'), 0)} pb; "
                         f"CCL {_number(macro.get('ccl'))}. La etiqueta de Quantia es «{probe['observed']}».")
            if probe["country_risk_only"] == "estable" and probe["ccl_only"] == "crítico":
                parts.append("Al evaluar la misma regla por separado, el riesgo país observado no activa «crítico»; "
                             "el CCL sí. Atribuir esa etiqueta al riesgo país sería incorrecto. "
                             "Esta es una regla interna, no una evaluación económica validada de Argentina.")
            rules = data.get("direct_macro_rules", {})
            for ticker in ("NVDA", "AMD", "_default"):
                if ticker in rules:
                    factors = ", ".join(str(rule[0]) for rule in rules[ticker])
                    parts.append(f"Mapa macro directo de {ticker}: {factors}.")
            gaps.append("Esto explica el mapa macro directo y su etiqueta; no demuestra ausencia de todos los canales de riesgo local.")
        else:
            status = "INSUFFICIENT"
            gaps.append("No pude obtener la política macro y sus valores; no atribuyo causas a la etiqueta.")
    else:
        evidence = payloads.get("get_decision_evidence")
        if not evidence or evidence.get("schema_version") != "agent-decision-evidence-v1":
            status = "INSUFFICIENT"
            parts.append("No obtuve evidencia estructurada del plan. No puedo explicar sus compras o bloqueos a partir de un score aislado.")
        else:
            snapshot = evidence.get("snapshot_as_of") or "fecha de snapshot no informada"
            parts.append(f"Consulté el plan de Quantia generado {evidence['evaluated_at']}, con snapshot {snapshot}. "
                         "Es una evaluación nueva en modo consulta; no representa órdenes enviadas ni fills.")
            operational = evidence.get("plan")
            if evidence.get("snapshot_stale_reason"):
                status = "PARTIAL"
                gaps.append(f"Snapshot desactualizado: {evidence['snapshot_stale_reason']}.")
            if operational is None:
                status = "INSUFFICIENT"
                parts.append("El motor no produjo un plan evaluable. Eso no equivale a una recomendación de mantener.")
            else:
                buys = operational.get("buy_orders", [])
                parts.append(f"El plan contiene {len(buys)} compras propuestas. Efectivo disponible informado: "
                             f"{_number(evidence.get('cash_ars'))} ARS. "
                             "La existencia de ventas propuestas no demuestra por sí sola que comprar sea inconveniente.")
                decisions = operational.get("decisions", [])
                requested = set(re.findall(r"[A-Z0-9.=-]+", goal.upper()))
                focused = [d for d in decisions if d.get("ticker") in requested]
                # Prefer buying restrictions and target disagreements, then the
                # remaining decisions. Never recompute the planner's decision.
                ordered = sorted(focused or decisions, key=lambda d: (not ((d.get('target_weight') or 0) > (d.get('current_weight') or 0)), d.get('ticker', '')))
                for decision in ordered[:8]:
                    ticker = decision.get("ticker", "N/D")
                    row = next((r for r in evidence.get("signals", []) if r.get("ticker") == ticker), {})
                    current, target = decision.get('current_weight'), decision.get('target_weight')
                    parts.append(f"• {ticker}: peso actual {_number(current*100 if current is not None else None, 1)}% → "
                                 f"objetivo {_number(target*100 if target is not None else None, 1)}%; decisión {decision.get('action')}. "
                                 f"{decision.get('reason_primary') or 'Motivo no informado'}. "
                                 f"{decision.get('reason_secondary') or ''}")
                    layers = row.get("layers", [])
                    if layers:
                        contributions = "; ".join(f"{l['name']} {l['weighted']:+.3f}" for l in layers if isinstance(l.get('weighted'), (int, float)))
                        parts.append(f"  Aportes al score: {contributions}. Total {row.get('final_score')}; no es un retorno.")
                        drivers = sorted((l for l in layers if isinstance(l.get('weighted'), (int, float))),
                                         key=lambda l: -abs(l['weighted']))
                        for layer in drivers[:2]:
                            if layer.get("reasons"):
                                parts.append(f"  Motivo registrado en {layer['name']}: {str(layer['reasons'][0])[:300]}.")
                if len(ordered) > 8:
                    gaps.append("Se muestran ocho decisiones; todas están en la traza estructurada.")
                for blocked in operational.get("blocked_orders", [])[:6]:
                    if focused and blocked.get("ticker") not in requested:
                        continue
                    parts.append(f"• Restricción de orden {blocked.get('ticker')}: {blocked.get('reason')} ({blocked.get('block_code') or 'sin código'}).")
                policy = evidence.get("buy_policy", {})
                parts.append(f"Regla de score para compras: bloqueo por debajo de {policy.get('negative_block')}; "
                             f"señal mínima {policy.get('minimum')}. Superarla no garantiza una compra: siguen aplicando fondos y demás controles.")
                if not decisions:
                    status = "PARTIAL"
                    gaps.append("El plan no contiene motivos individuales de decisión; no invento una causa para cada activo.")
            gaps.append("Esta explicación describe por qué actuó el sistema. No prueba que sus reglas superen a comprar o mantener bajo la misma ventana y costos.")
        if plan.intent == "portfolio_review":
            if status != "INSUFFICIENT":
                status = "PARTIAL"
            snapshot = payloads.get("get_portfolio_snapshot")
            if snapshot:
                parts.insert(0, f"Snapshot de cuenta {snapshot.get('scraped_at')}: "
                             f"{_number(snapshot.get('total_value_ars'))} ARS; efectivo {_number(snapshot.get('cash_ars'))} ARS. "
                             "La diferencia con otra valuación no se interpreta como ganancia o pérdida.")
            else:
                status = "INSUFFICIENT"
            gaps.append("Faltan una reconciliación completa de NAV, flujos y costos y una comparación de alternativas para concluir sobre resultado económico.")
        if plan.intent == "market_thesis" or plan.user_hypotheses:
            if status != "INSUFFICIENT":
                status = "PARTIAL"
            parts.insert(0, "Tu tesis de mercado sigue sin verificar. Una corrección bursátil y una recesión económica no son equivalentes; "
                         "una tendencia positiva tampoco determina por sí sola si conviene comprar o mantener.")
            for hypothesis in plan.user_hypotheses:
                parts.append(f"Planteo del usuario, no verificado: «{hypothesis}».")
            gaps.append("Para evaluar esa tesis faltan series fechadas del subyacente y del tipo de cambio, un horizonte explícito y comparación de comprar/mantener/reducir con sizing y costos comunes. No infiero caídas de dos meses a partir del score actual.")
    answer = "\n\n".join(parts) + "\n\nPendiente:\n- " + "\n- ".join(gaps)
    return AgentDecision(kind="final", answer=answer, rationale="Explicación de fuentes estructuradas bajo política de preguntas v2.",
                         answer_origin="diagnostics_v2", objective_status=status)


def decision_lab_arguments(goal):
    """Conservative routing. Unknown names do not silently select another ticker."""
    text = _plain(goal)
    aliases = {"microsoft":"MSFT", "nvidia":"NVDA", "apple":"AAPL", "amazon":"AMZN", "google":"GOOGL", "tesla":"TSLA", "meta":"META"}
    tickers = {ticker for name,ticker in aliases.items() if re.search(r"\b"+name+r"\b",text)}
    stop = {"DVA","PLAN","HOLD","CASH","ARS","USD","REDUCE","BUY","SELL","PARTIAL","IC","CI"}
    tickers.update(t for t in re.findall(r"\b[A-Z][A-Z0-9.=-]{1,9}\b",goal) if t not in stop)
    args = {}
    if len(tickers)==1:args["ticker"]=next(iter(tickers))
    horizons = re.findall(r"\b(5|10|20|40)\s*(?:d|dias|sesiones)\b",text)
    if horizons:args["horizon"]=int(horizons[0])
    return args

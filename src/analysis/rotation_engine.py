"""
src/analysis/rotation_engine.py
────────────────────────────────
Rotation Engine: decide qué hacer con el capital liberado de reducciones.

Responde: "La plata que sale de CVX, ¿va a MU, a cash, o a un candidato nuevo?"

Inputs:
  - portfolio_results:   lista de SynthesisResult del análisis de cartera
  - opportunity_report:  OpportunityReport del radar de oportunidades
  - rebalance_report:    RebalanceReport del optimizer (ventas planeadas)
  - portfolio_value_ars: valor total del portfolio
  - cash_ars:            cash disponible
  - gate_state:          NORMAL | CAUTIOUS | BLOCKED

Output:
  - RotationPlan con decisiones concretas en ARS

Filosofía:
  El engine compara directamente:
    "¿Tiene más sentido aumentar MU (ya en cartera, score conocido)
     o abrir AVGO (candidato nuevo, score X, asimetría Y)?"

  La respuesta considera:
    - Score relativo entre las opciones
    - Diversificación sectorial
    - Asimetría de la oportunidad nueva
    - Concentración actual del portfolio
    - Estado del gate de riesgo
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


# ─── Dataclasses ──────────────────────────────────────────────────────────────

@dataclass
class RotationTarget:
    """Un destino de capital dentro del plan de rotación."""
    ticker:         str
    action:         str        # AUMENTAR_EXISTENTE | NUEVO_CANDIDATO | CASH
    amount_ars:     float
    weight_delta:   float      # cambio en el peso del portfolio
    score:          float
    conviction:     float
    rationale:      str
    is_new:         bool = False
    sector:         str  = ""


@dataclass
class RotationPlan:
    """Plan completo de rotación de capital."""
    total_capital_ars:   float   # capital disponible para rotar
    gate_state:          str     # NORMAL | CAUTIOUS | BLOCKED
    # Fuentes (ventas)
    sources:             list[dict] = field(default_factory=list)  # [{ticker, amount_ars}]
    # Destinos
    targets:             list[RotationTarget] = field(default_factory=list)
    # Resumen
    to_existing:         float = 0.0   # ARS que van a posiciones existentes
    to_new:              float = 0.0   # ARS que van a candidatos nuevos
    to_cash:             float = 0.0   # ARS que quedan en cash
    # Decisión principal
    primary_decision:    str   = ""
    primary_rationale:   str   = ""
    # Contexto
    skipped_reason:      str   = ""    # por qué no hay rotación


# ─── Engine ───────────────────────────────────────────────────────────────────

def build_rotation_plan(
    portfolio_results,       # list[SynthesisResult]
    opportunity_report,      # OpportunityReport
    rebalance_report,        # RebalanceReport (del optimizer)
    portfolio_positions:     list[dict],
    portfolio_value_ars:     float,
    cash_ars:                float,
    gate_state:              str = "NORMAL",
) -> Optional[RotationPlan]:
    """
    Construye el plan de rotación completo.

    Lógica:
      1. ¿Cuánto capital se libera de las ventas planificadas?
      2. ¿Hay candidatos nuevos con mejor score + asimetría que las opciones internas?
      3. ¿Cuánto va a cada destino? ¿Cuánto queda en cash?
    """
    if gate_state == "BLOCKED":
        return RotationPlan(
            total_capital_ars = 0.0,
            gate_state        = gate_state,
            skipped_reason    = "Gate BLOCKED — capital liberado va a cash",
        )

    # ── Capital disponible ────────────────────────────────────────────────────
    sells_ars     = float(getattr(rebalance_report, "total_sells_ars", 0.0) or 0.0)
    available_ars = sells_ars + max(0.0, cash_ars - portfolio_value_ars * 0.05)
    available_ars = max(0.0, available_ars)

    if available_ars < 10_000:
        return RotationPlan(
            total_capital_ars = available_ars,
            gate_state        = gate_state,
            skipped_reason    = f"Capital insuficiente para rotar (${available_ars:,.0f} ARS)",
        )

    plan = RotationPlan(
        total_capital_ars = available_ars,
        gate_state        = gate_state,
    )

    # Fuentes (qué se vende)
    trades = getattr(rebalance_report, "trades", []) or []
    for tr in trades:
        action = str(getattr(tr, "action", "") or "").upper()
        if "SELL" in action or "REDUCIR" in action or "RECORTAR" in action:
            plan.sources.append({
                "ticker":     getattr(tr, "ticker", "?"),
                "amount_ars": abs(float(getattr(tr, "amount_ars", 0.0) or 0.0)),
            })

    # ── Opciones internas: posiciones existentes con señal de AUMENTAR ────────
    internal_options = []
    for r in (portfolio_results or []):
        decision = str(getattr(r, "decision", "HOLD")).upper()
        if decision in ("BUY", "ACCUMULATE"):
            ticker = str(getattr(r, "ticker", "")).upper()
            score  = float(getattr(r, "final_score", 0.0) or 0.0)
            conv   = float(getattr(r, "conviction", getattr(r, "confidence", 0.0)) or 0.0)
            if conv > 1.0:
                conv /= 100.0
            internal_options.append({
                "ticker":     ticker,
                "score":      score,
                "conviction": conv,
                "is_new":     False,
            })

    internal_options.sort(key=lambda x: x["score"] * x["conviction"], reverse=True)

    # ── Opciones externas: candidatos del radar ────────────────────────────────
    external_options = []
    if opportunity_report:
        for c in (opportunity_report.comprable_ahora or []):
            asym_bonus = (c.asymmetry.asymmetry_ratio / 3.0) if c.asymmetry else 0.0
            external_options.append({
                "ticker":     c.ticker,
                "score":      c.final_score,
                "conviction": c.conviction,
                "asym_ratio": c.asymmetry.asymmetry_ratio if c.asymmetry else 0.0,
                "asym_label": c.asymmetry_label,
                "sizing":     c.sizing_suggested,
                "competes":   c.competes_with,
                "is_new":     True,
                "combined":   c.final_score * c.conviction * (1 + asym_bonus),
            })

    external_options.sort(key=lambda x: x["combined"], reverse=True)

    # ── Decisión de asignación ────────────────────────────────────────────────
    # Combinar internas y externas, ordenar por valor ajustado
    all_options = []
    for opt in internal_options[:3]:
        all_options.append({**opt, "combined": opt["score"] * opt["conviction"]})
    for opt in external_options[:3]:
        all_options.append(opt)

    all_options.sort(key=lambda x: x.get("combined", 0.0), reverse=True)

    if not all_options:
        plan.to_cash    = available_ars
        plan.skipped_reason = "Sin candidatos claros — capital va a cash"
        return plan

    # Distribuir capital entre los mejores candidatos (máximo 3 destinos)
    top = all_options[:3]
    weights = [x.get("combined", 0.01) for x in top]
    total_w = sum(weights) or 1.0

    # En gate CAUTIOUS: limitar compras nuevas
    if gate_state == "CAUTIOUS":
        top = [x for x in top if not x.get("is_new", False)][:2]
        if not top:
            plan.to_cash = available_ars
            plan.skipped_reason = "Gate CAUTIOUS — sin aumentos internos claros, capital a cash"
            return plan
        weights = [x.get("combined", 0.01) for x in top]
        total_w = sum(weights) or 1.0

    remaining = available_ars
    for opt, w in zip(top, weights):
        # Límite por posición: 15% del portfolio o 50% del capital disponible
        max_amount = min(
            portfolio_value_ars * 0.15,
            available_ars * 0.50,
        )
        amount = min(available_ars * (w / total_w), max_amount, remaining)

        if amount < 5_000:
            continue

        ticker     = opt["ticker"]
        is_new     = opt.get("is_new", False)
        score      = opt.get("score", 0.0)
        conviction = opt.get("conviction", 0.0)

        # Rationale
        if is_new:
            asym_label = opt.get("asym_label", "")
            asym_ratio = opt.get("asym_ratio", 0.0)
            competes   = opt.get("competes", [])
            rationale  = (
                f"Candidato nuevo con asimetría {asym_label} (R/R {asym_ratio:.1f}x), "
                f"score {score:+.3f}, convicción {round(conviction*100)}%"
            )
            if competes:
                rationale += f". Compite con {', '.join(competes)} — abre diversificación sectorial"
        else:
            rationale = (
                f"Aumentar posición existente: score {score:+.3f}, "
                f"convicción {round(conviction*100)}%"
            )

        target = RotationTarget(
            ticker       = ticker,
            action       = "NUEVO_CANDIDATO" if is_new else "AUMENTAR_EXISTENTE",
            amount_ars   = round(amount),
            weight_delta = amount / portfolio_value_ars if portfolio_value_ars > 0 else 0.0,
            score        = score,
            conviction   = conviction,
            rationale    = rationale,
            is_new       = is_new,
        )
        plan.targets.append(target)
        remaining -= amount

        if is_new:
            plan.to_new += amount
        else:
            plan.to_existing += amount

    # Resto va a cash
    plan.to_cash = max(0.0, remaining)

    # Decisión principal
    if plan.targets:
        primary = plan.targets[0]
        verb    = "Abrir" if primary.is_new else "Aumentar"
        plan.primary_decision  = f"{verb} {primary.ticker}: +${primary.amount_ars:,.0f} ARS"
        plan.primary_rationale = primary.rationale

        # Comparar mejor candidato nuevo vs mejor opción interna
        best_new      = next((x for x in plan.targets if x.is_new), None)
        best_internal = next((x for x in plan.targets if not x.is_new), None)
        if best_new and best_internal:
            if best_new.score > best_internal.score + 0.05:
                plan.primary_rationale += (
                    f" — Candidato nuevo ({best_new.ticker}) "
                    f"supera en score a opción interna ({best_internal.ticker})"
                )
            else:
                plan.primary_rationale += (
                    f" — Señal similar a {best_internal.ticker}: "
                    f"balance entre concentración y diversificación"
                )
    else:
        plan.primary_decision  = "Capital a cash"
        plan.primary_rationale = "Sin opciones con suficiente calidad — esperar mejor señal"
        plan.to_cash = available_ars

    return plan


def render_rotation_plan(plan: RotationPlan) -> str:
    """Bloque HTML para insertar en el reporte combinado."""
    if not plan:
        return ""

    from html import escape

    def _money(x):
        try:
            return f"${float(x):,.0f} ARS".replace(",", ".")
        except Exception:
            return "$0 ARS"

    h = []
    h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    h.append("🔄 <b>ROTATION ENGINE</b>")
    h.append(f"Capital disponible: <b>{_money(plan.total_capital_ars)}</b>")
    h.append("")

    if plan.skipped_reason:
        h.append(f"⚠️ {escape(plan.skipped_reason)}")
        return "\n".join(h)

    if not plan.targets:
        h.append("Sin destinos de rotación claros.")
        h.append(f"Capital queda en cash: {_money(plan.to_cash)}")
        return "\n".join(h)

    h.append(f"<b>Decisión principal:</b> {escape(plan.primary_decision)}")
    h.append(f"   {escape(plan.primary_rationale)}")
    h.append("")

    h.append("<b>Distribución del capital:</b>")
    step = 1
    for t in plan.targets:
        icon = "🌍" if t.is_new else "📈"
        verb = "Nuevo" if t.is_new else "Aumentar"
        h.append(
            f"  {step}. {icon} {verb} <b>{t.ticker}</b>: "
            f"+{_money(t.amount_ars)} (+{t.weight_delta:.1%} portfolio)"
        )
        h.append(f"     → {escape(t.rationale)}")
        step += 1

    if plan.to_cash > 5_000:
        h.append(f"  {step}. 💵 Cash: {_money(plan.to_cash)}")

    h.append("")
    summary_parts = []
    if plan.to_existing > 0:  summary_parts.append(f"existentes: {_money(plan.to_existing)}")
    if plan.to_new      > 0:  summary_parts.append(f"nuevos: {_money(plan.to_new)}")
    if plan.to_cash     > 0:  summary_parts.append(f"cash: {_money(plan.to_cash)}")
    if summary_parts:
        h.append("   " + " | ".join(summary_parts))

    return "\n".join(h)

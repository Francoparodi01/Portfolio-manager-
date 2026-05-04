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

Fix P0:
  - Bloquea destinos internos con score negativo.
  - Bloquea candidatos externos con score negativo.
  - Evita que el capital liberado termine en compras incoherentes.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


# ─── Constantes ───────────────────────────────────────────────────────────────

BUY_SCORE_MIN = 0.0


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
    sector:         str = ""


@dataclass
class RotationPlan:
    """Plan completo de rotación de capital."""
    total_capital_ars:   float   # capital disponible para rotar
    gate_state:          str     # NORMAL | CAUTIOUS | BLOCKED

    # Fuentes: ventas / reducciones
    sources:             list[dict] = field(default_factory=list)

    # Destinos
    targets:             list[RotationTarget] = field(default_factory=list)

    # Resumen
    to_existing:         float = 0.0
    to_new:              float = 0.0
    to_cash:             float = 0.0

    # Decisión principal
    primary_decision:    str = ""
    primary_rationale:   str = ""

    # Contexto
    skipped_reason:      str = ""


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _money(x: float) -> str:
    try:
        value = float(x)
        sign = "-" if value < 0 else ""
        return f"{sign}${abs(value):,.0f} ARS".replace(",", ".")
    except Exception:
        return "$0 ARS"


def _normalize_conviction(value: float) -> float:
    """
    Normaliza conviction/confidence:
    - Si viene como 75, lo pasa a 0.75.
    - Si viene como 0.75, lo deja igual.
    """
    try:
        conv = float(value or 0.0)
    except Exception:
        return 0.0

    if conv > 1.0:
        conv /= 100.0

    return max(0.0, min(conv, 1.0))


def _is_sell_action(action: str) -> bool:
    action = str(action or "").upper()
    return (
        "SELL" in action
        or "VENDER" in action
        or "REDUCIR" in action
        or "RECORTAR" in action
    )


def _is_buy_decision(decision: str) -> bool:
    decision = str(decision or "").upper()
    return decision in ("BUY", "ACCUMULATE", "COMPRAR", "AUMENTAR")


# ─── Engine ──────────────────────────────────────────────────────────────────

def build_rotation_plan(
    portfolio_results,       # list[SynthesisResult]
    opportunity_report,      # OpportunityReport
    rebalance_report,        # RebalanceReport del optimizer
    portfolio_positions:     list[dict],
    portfolio_value_ars:     float,
    cash_ars:                float,
    gate_state:              str = "NORMAL",
) -> Optional[RotationPlan]:
    """
    Construye el plan de rotación completo.

    Lógica:
      1. Calcula capital disponible: ventas + cash excedente sobre buffer.
      2. Evalúa destinos internos con señal BUY/ACCUMULATE.
      3. Evalúa candidatos externos del radar.
      4. Bloquea destinos con score negativo.
      5. Distribuye capital entre los mejores destinos restantes.
    """
    gate_state = str(gate_state or "NORMAL").upper()

    if gate_state == "BLOCKED":
        return RotationPlan(
            total_capital_ars=0.0,
            gate_state=gate_state,
            skipped_reason="Gate BLOCKED — capital liberado va a cash",
        )

    portfolio_value_ars = float(portfolio_value_ars or 0.0)
    cash_ars = float(cash_ars or 0.0)

    # ── Capital disponible ────────────────────────────────────────────────────
    sells_ars = float(getattr(rebalance_report, "total_sells_ars", 0.0) or 0.0)

    # Mantener 5% mínimo de cash como buffer.
    cash_buffer = portfolio_value_ars * 0.05
    excess_cash = max(0.0, cash_ars - cash_buffer)

    available_ars = max(0.0, sells_ars + excess_cash)

    if available_ars < 10_000:
        return RotationPlan(
            total_capital_ars=available_ars,
            gate_state=gate_state,
            skipped_reason=f"Capital insuficiente para rotar ({_money(available_ars)})",
        )

    plan = RotationPlan(
        total_capital_ars=available_ars,
        gate_state=gate_state,
    )

    # ── Fuentes: qué se vende/reduce ──────────────────────────────────────────
    trades = getattr(rebalance_report, "trades", []) or []

    for tr in trades:
        action = str(getattr(tr, "action", "") or "").upper()

        if _is_sell_action(action):
            plan.sources.append({
                "ticker": getattr(tr, "ticker", "?"),
                "amount_ars": abs(float(getattr(tr, "amount_ars", 0.0) or 0.0)),
            })

    # ── Opciones internas: posiciones existentes con señal de aumentar ────────
    internal_options = []

    for r in (portfolio_results or []):
        decision = str(getattr(r, "decision", "HOLD") or "HOLD").upper()

        if not _is_buy_decision(decision):
            continue

        ticker = str(getattr(r, "ticker", "") or "").upper().strip()
        if not ticker:
            continue

        score = float(getattr(r, "final_score", 0.0) or 0.0)
        conv = _normalize_conviction(
            getattr(r, "conviction", getattr(r, "confidence", 0.0))
        )

        # Guardrail P0:
        # Una posición existente no puede recibir más capital si el score final es negativo.
        # El optimizer puede sugerir aumentar por diversificación/riesgo, pero el rotation
        # engine no debe convertir eso en compra operativa.
        if score < BUY_SCORE_MIN:
            logger.info(
                f"Rotation engine: {ticker} excluido como destino interno "
                f"por score negativo ({score:+.3f})"
            )
            continue

        internal_options.append({
            "ticker": ticker,
            "score": score,
            "conviction": conv,
            "is_new": False,
            "combined": score * conv,
        })

    internal_options.sort(key=lambda x: x["combined"], reverse=True)

    # ── Opciones externas: candidatos del radar ────────────────────────────────
    external_options = []

    if opportunity_report:
        # Usamos principalmente comprable_ahora.
        # Si querés hacerlo más agresivo después, se puede sumar compra_habilitada,
        # pero por ahora conviene mantenerlo conservador.
        for c in (getattr(opportunity_report, "comprable_ahora", []) or []):
            ticker = str(getattr(c, "ticker", "") or "").upper().strip()
            if not ticker:
                continue

            score = float(getattr(c, "final_score", 0.0) or 0.0)
            conviction = _normalize_conviction(getattr(c, "conviction", 0.0))

            # Guardrail P0:
            # Un candidato externo con score negativo no puede ser destino de capital.
            if score < BUY_SCORE_MIN:
                logger.info(
                    f"Rotation engine: {ticker} excluido como candidato externo "
                    f"por score negativo ({score:+.3f})"
                )
                continue

            asym = getattr(c, "asymmetry", None)
            asym_ratio = float(getattr(asym, "asymmetry_ratio", 0.0) or 0.0) if asym else 0.0
            asym_bonus = asym_ratio / 3.0 if asym_ratio > 0 else 0.0

            external_options.append({
                "ticker": ticker,
                "score": score,
                "conviction": conviction,
                "asym_ratio": asym_ratio,
                "asym_label": getattr(c, "asymmetry_label", "") or "",
                "sizing": float(getattr(c, "sizing_suggested", 0.05) or 0.05),
                "competes": getattr(c, "competes_with", []) or [],
                "is_new": True,
                "combined": score * conviction * (1 + asym_bonus),
            })

    external_options.sort(key=lambda x: x["combined"], reverse=True)

    # ── Combinar destinos internos y externos ─────────────────────────────────
    all_options = []

    for opt in internal_options[:3]:
        all_options.append(opt)

    for opt in external_options[:3]:
        all_options.append(opt)

    # Evitar cualquier opción con combined <= 0.
    # Esto previene pesos negativos o asignaciones raras si score/conviction vienen mal.
    all_options = [
        opt for opt in all_options
        if float(opt.get("combined", 0.0) or 0.0) > 0
    ]

    all_options.sort(key=lambda x: x.get("combined", 0.0), reverse=True)

    if not all_options:
        plan.to_cash = available_ars
        plan.skipped_reason = "Sin candidatos claros con score positivo — capital va a cash"
        return plan

    # ── Gate CAUTIOUS: limitar compras nuevas ─────────────────────────────────
    top = all_options[:3]

    if gate_state == "CAUTIOUS":
        top = [x for x in top if not x.get("is_new", False)][:2]

        if not top:
            plan.to_cash = available_ars
            plan.skipped_reason = "Gate CAUTIOUS — sin aumentos internos claros, capital a cash"
            return plan

    weights = [max(float(x.get("combined", 0.0) or 0.0), 0.0) for x in top]
    total_w = sum(weights)

    if total_w <= 0:
        plan.to_cash = available_ars
        plan.skipped_reason = "Opciones sin peso positivo — capital va a cash"
        return plan

    # ── Distribuir capital ────────────────────────────────────────────────────
    remaining = available_ars

    for opt, w in zip(top, weights):
        max_amount = min(
            portfolio_value_ars * 0.15,  # límite por posición/destino
            available_ars * 0.50,        # límite por concentración del capital rotado
        )

        amount = min(
            available_ars * (w / total_w),
            max_amount,
            remaining,
        )

        if amount < 5_000:
            continue

        ticker = opt["ticker"]
        is_new = bool(opt.get("is_new", False))
        score = float(opt.get("score", 0.0) or 0.0)
        conviction = _normalize_conviction(opt.get("conviction", 0.0))

        # Segundo guard defensivo:
        # Por si alguna opción negativa se filtró por error.
        if score < BUY_SCORE_MIN:
            logger.warning(
                f"Rotation engine: {ticker} bloqueado en asignación final "
                f"por score negativo ({score:+.3f})"
            )
            continue

        if is_new:
            asym_label = opt.get("asym_label", "")
            asym_ratio = float(opt.get("asym_ratio", 0.0) or 0.0)
            competes = opt.get("competes", []) or []

            rationale = (
                f"Candidato nuevo con asimetría {asym_label} "
                f"(R/R {asym_ratio:.1f}x), score {score:+.3f}, "
                f"convicción {round(conviction * 100)}%"
            )

            if competes:
                rationale += (
                    f". Compite con {', '.join(competes)} — "
                    "requiere reducir antes de comprar"
                )
        else:
            rationale = (
                f"Aumentar posición existente: score {score:+.3f}, "
                f"convicción {round(conviction * 100)}%"
            )

        target = RotationTarget(
            ticker=ticker,
            action="NUEVO_CANDIDATO" if is_new else "AUMENTAR_EXISTENTE",
            amount_ars=round(amount),
            weight_delta=amount / portfolio_value_ars if portfolio_value_ars > 0 else 0.0,
            score=score,
            conviction=conviction,
            rationale=rationale,
            is_new=is_new,
        )

        plan.targets.append(target)
        remaining -= amount

        if is_new:
            plan.to_new += amount
        else:
            plan.to_existing += amount

    # Resto va a cash.
    plan.to_cash = max(0.0, remaining)

    # ── Decisión principal ────────────────────────────────────────────────────
    if plan.targets:
        primary = plan.targets[0]
        verb = "Abrir" if primary.is_new else "Aumentar"

        plan.primary_decision = f"{verb} {primary.ticker}: +{_money(primary.amount_ars)}"
        plan.primary_rationale = primary.rationale

        best_new = next((x for x in plan.targets if x.is_new), None)
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
                    "balance entre concentración y diversificación"
                )
    else:
        plan.primary_decision = "Capital a cash"
        plan.primary_rationale = "Sin opciones con suficiente calidad — esperar mejor señal"
        plan.to_cash = available_ars

    return plan


def render_rotation_plan(plan: RotationPlan) -> str:
    """Bloque HTML para insertar en el reporte combinado."""
    if not plan:
        return ""

    from html import escape

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
            f"  {step}. {icon} {verb} <b>{escape(t.ticker)}</b>: "
            f"+{_money(t.amount_ars)} (+{t.weight_delta:.1%} portfolio)"
        )
        h.append(f"     → {escape(t.rationale)}")
        step += 1

    if plan.to_cash > 5_000:
        h.append(f"  {step}. 💵 Cash: {_money(plan.to_cash)}")

    h.append("")

    summary_parts = []

    if plan.to_existing > 0:
        summary_parts.append(f"existentes: {_money(plan.to_existing)}")

    if plan.to_new > 0:
        summary_parts.append(f"nuevos: {_money(plan.to_new)}")

    if plan.to_cash > 0:
        summary_parts.append(f"cash: {_money(plan.to_cash)}")

    if summary_parts:
        h.append("   " + " | ".join(summary_parts))

    return "\n".join(h)
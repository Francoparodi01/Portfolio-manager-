"""
src/analysis/execution_planner.py
──────────────────────────────────
Capa de ejecución: traduce el target teórico del optimizer en un plan
ejecutable real, con cash reconciliado y órdenes priorizadas.

Flujo:
    RebalanceReport (optimizer)
        → derive_decision_intents()   →  list[DecisionIntent]
        → reconcile_funding()         →  ExecutionPlan
        → validate_execution_plan()   →  raises AssertionError si no cierra
        → render usa ExecutionPlan como única fuente de verdad operativa

Regla de oro:
    El renderer NUNCA lee de PortfolioTarget para construir:
      - acción principal
      - montos de compra/venta
      - plan de rotación
      - veredicto final
    Todo eso sale de ExecutionPlan.

Principio MVP:
    El optimizer puede sugerir, pero una señal neutral no justifica operar.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from src.analysis.enums import DecisionType, DeprecatedEnumMeta

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# PARÁMETROS OPERATIVOS
# ══════════════════════════════════════════════════════════════════════════════

MIN_WEIGHT_DELTA = 0.015      # 1.5 pp — diferencias menores → HOLD
MIN_TRADE_ARS = 25_000       # monto mínimo para generar una orden
FEE_PCT = 0.006              # 0.6% fee total estimado
SLIPPAGE_PCT = 0.0015        # 0.15% slippage estimado
SELL_FULL_THRESH = 0.005     # target < 0.5% → SELL_FULL


# ══════════════════════════════════════════════════════════════════════════════
# GUARDS DE CALIDAD OPERATIVA
# ══════════════════════════════════════════════════════════════════════════════

# BUY:
# - score < -0.01   → BLOCKED por BUY_SCORE_GUARD
# - score < +0.08   → WATCH por TRADE_QUALITY_GUARD
# - score >= +0.08  → BUY permitido

SCORE_BUY_STRONG = +0.12
SCORE_BUY_MIN = +0.08
SCORE_BUY_BLOCK_NEG = -0.01

# Clasificación general de señal

SCORE_NEU_HIGH = +0.05
SCORE_NEU_LOW = -0.05
SCORE_NEG_DEBIL_LOW = -0.08

# SELL:
# - score <= -0.08              → venta permitida por señal negativa
# - -0.08 < score < -0.05       → venta solo si delta relevante o concentración
# - -0.05 <= score <= +0.05     → HOLD salvo concentración
# - score > +0.05               → HOLD salvo concentración fuerte

MAX_WEIGHT_CONC = 0.25        # concentración media: permite rebalanceo con neutral
MAX_WEIGHT_HARD_CONC = 0.30   # concentración alta: permite vender aunque score sea positivo


# ══════════════════════════════════════════════════════════════════════════════
# TIPOS
# ══════════════════════════════════════════════════════════════════════════════

class Action(str, Enum, metaclass=DeprecatedEnumMeta):
    BUY = "BUY"
    SELL_FULL = "SELL_FULL"
    SELL_PARTIAL = "SELL_PARTIAL"
    HOLD = "HOLD"
    WATCH = "WATCH"
    BLOCKED = "BLOCKED"

    def to_decision_type(self) -> DecisionType:
        return {
            Action.BUY: DecisionType.BUY,
            Action.SELL_FULL: DecisionType.SELL_FULL,
            Action.SELL_PARTIAL: DecisionType.SELL_PARTIAL,
            Action.HOLD: DecisionType.HOLD,
            Action.WATCH: DecisionType.WATCH,
            Action.BLOCKED: DecisionType.BLOCKED,
        }[self]


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(str, Enum):
    PLANNED = "PLANNED"
    SUBMITTED = "SUBMITTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"


class ScoreRange(str, Enum):
    POS_FUERTE = "POS_FUERTE"
    POS_OPERABLE = "POS_OPERABLE"
    POS_DEBIL = "POS_DEBIL"
    NEUTRAL = "NEUTRAL"
    NEG_DEBIL = "NEG_DEBIL"
    NEG_OPERABLE = "NEG_OPERABLE"


SCORE_RANGE_LABELS: dict[ScoreRange, str] = {
    ScoreRange.POS_FUERTE: "POSITIVA FUERTE",
    ScoreRange.POS_OPERABLE: "POSITIVA OPERABLE",
    ScoreRange.POS_DEBIL: "POSITIVA DÉBIL",
    ScoreRange.NEUTRAL: "NEUTRAL / RUIDO",
    ScoreRange.NEG_DEBIL: "NEGATIVA DÉBIL",
    ScoreRange.NEG_OPERABLE: "NEGATIVA OPERABLE",
}


@dataclass
class AssetSignal:
    """Calidad de señal del activo, independiente de la decisión de cartera."""
    ticker: str
    score: float
    conviction: float
    technical: float
    macro: float
    sentiment: float
    explanation: Optional[str] = None


@dataclass
class PositionSnapshot:
    """Posición actual en el portfolio."""
    ticker: str
    quantity: float
    price: float
    market_value_ars: float
    current_weight: float


@dataclass
class TargetWeight:
    """Un peso objetivo del optimizer para un ticker."""
    ticker: str
    current_weight: float
    target_weight: float
    delta_weight: float


@dataclass
class PortfolioTarget:
    """
    Output teórico del optimizer.
    Aparece SOLO en la sección informativa del reporte, nunca como fuente
    de la acción principal ni del plan de rotación.
    """
    method: str
    expected_return: float
    volatility: float
    sharpe: float
    targets: list[TargetWeight]


@dataclass
class DecisionIntent:
    """
    Decisión conceptual para un ticker.

    Combina:
      - señal del activo
      - target del optimizer
      - guards operativas
      - restricciones del gate
    """
    ticker: str
    action: DecisionType
    reason_primary: str
    reason_secondary: Optional[str]
    current_weight: float
    target_weight: float
    delta_weight: float
    score: Optional[float] = None
    conviction: Optional[float] = None
    theoretical_ars: float = 0.0


@dataclass
class OrderIntent:
    """
    Orden ejecutable concreta con monto real disponible.
    amount_ars puede ser menor a theoretical_ars si el cash no alcanza.
    """
    ticker: str
    side: OrderSide
    action: DecisionType
    amount_ars: float
    theoretical_ars: float
    quantity_est: float
    reference_price: float
    reason: str
    priority: int
    funded_by: list[str] = field(default_factory=list)
    partial: bool = False
    status: OrderStatus = OrderStatus.PLANNED

    planned_qty: Optional[float] = None
    filled_qty: Optional[float] = None
    avg_fill_price: Optional[float] = None
    submitted_at: Optional[str] = None
    filled_at: Optional[str] = None


@dataclass
class ExecutionPlan:
    """
    Plan ejecutable completo. Fuente única de verdad para el reporte operativo.

    cash_after = cash_before + net_sell_ars - gross_buy_ars - fee_buy_ars
    """
    decisions: list[DecisionIntent]

    sell_orders: list[OrderIntent]
    buy_orders: list[OrderIntent]
    blocked_orders: list[OrderIntent]

    cash_before: float
    gross_sell_ars: float
    fee_sell_ars: float
    net_sell_ars: float
    gross_buy_ars: float
    fee_buy_ars: float
    cash_after: float

    feasible: bool
    gate: str
    summary: str
    warnings: list[str] = field(default_factory=list)

    pending_buys: list[str] = field(default_factory=list)

    @property
    def main_action(self) -> Optional[OrderIntent]:
        if self.sell_orders:
            return self.sell_orders[0]
        if self.buy_orders:
            return self.buy_orders[0]
        return None

    @property
    def has_orders(self) -> bool:
        return bool(self.sell_orders or self.buy_orders)

    def sell_total(self) -> float:
        return sum(o.amount_ars for o in self.sell_orders)

    def buy_total(self) -> float:
        return sum(o.amount_ars for o in self.buy_orders)

    def verdict(self) -> str:
        if not self.feasible:
            return "Sin plan ejecutable — revisar restricciones del sistema."

        if self.gate == "BLOCKED":
            return "Sistema bloqueado por gate de riesgo — solo stops de emergencia."

        if not self.has_orders:
            if self.pending_buys:
                return (
                    "Hay señales de compra, pero no hay cash ni ventas "
                    "suficientes para financiarlas hoy — mantener o evaluar swaps."
                )
            return (
                "Mantener y observar — el optimizer sugirió cambios, "
                "pero la calidad de señal no justifica operar."
            )

        sells = [o for o in self.sell_orders]
        buys = [o for o in self.buy_orders]

        if sells and buys:
            return (
                "Plan de rotación definido — ejecutar ventas primero, "
                "luego reasignar capital en el orden indicado."
            )

        if sells:
            return (
                "Reducir exposición — ejecutar ventas. "
                "Sin compras habilitadas por calidad de señal."
            )

        if buys:
            return "Aumentar exposición selectiva — ejecutar compras en el orden indicado."

        return "Mantener y observar — sin ventaja operativa para actuar hoy."


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS DE SCORE / SEÑAL
# ══════════════════════════════════════════════════════════════════════════════

def _fmt_score(score: Optional[float]) -> str:
    return "N/A" if score is None else f"{score:+.3f}"


def classify_score(score: Optional[float]) -> tuple[ScoreRange, str]:
    """
    Clasifica el score en una señal operativa.

    Esto separa:
      - score: magnitud/dirección
      - señal: interpretación operativa
      - conviction: acuerdo entre capas
    """
    if score is None:
        return ScoreRange.NEUTRAL, SCORE_RANGE_LABELS[ScoreRange.NEUTRAL]

    if score >= SCORE_BUY_STRONG:
        rango = ScoreRange.POS_FUERTE
    elif score >= SCORE_BUY_MIN:
        rango = ScoreRange.POS_OPERABLE
    elif score >= SCORE_NEU_HIGH:
        rango = ScoreRange.POS_DEBIL
    elif score >= SCORE_NEU_LOW:
        rango = ScoreRange.NEUTRAL
    elif score >= SCORE_NEG_DEBIL_LOW:
        rango = ScoreRange.NEG_DEBIL
    else:
        rango = ScoreRange.NEG_OPERABLE

    return rango, SCORE_RANGE_LABELS[rango]


def signal_label_for_render(score: Optional[float]) -> str:
    """Helper público para el renderer."""
    _, label = classify_score(score)
    return label


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS DE GUARDIAS
# ══════════════════════════════════════════════════════════════════════════════

def _buy_guard(
    score: Optional[float],
    w_cur: float,
    w_opt: float,
    theoretical_ars: float,
) -> tuple[DecisionType, str, str]:
    """
    Devuelve:
      action, reason_primary, reason_secondary
    """
    if score is None:
        return (
            DecisionType.BLOCKED,
            "Compra bloqueada: score no disponible",
            f"Optimizer sugería aumentar {w_cur:.1%} → {w_opt:.1%} "
            f"({theoretical_ars:,.0f} ARS), pero falta señal cuantitativa",
        )

    if score < SCORE_BUY_BLOCK_NEG:
        return (
            DecisionType.BLOCKED,
            f"Compra bloqueada por scorer negativo: {score:+.3f}",
            f"Optimizer sugería aumentar {w_cur:.1%} → {w_opt:.1%} "
            f"({theoretical_ars:,.0f} ARS), pero no pasa BUY_SCORE_GUARD",
        )

    if score < SCORE_BUY_MIN:
        return (
            DecisionType.WATCH,
            f"Compra en WATCH: señal insuficiente {score:+.3f}",
            f"Optimizer sugería aumentar {w_cur:.1%} → {w_opt:.1%} "
            f"({theoretical_ars:,.0f} ARS), pero BUY requiere score >= {SCORE_BUY_MIN:+.2f}",
        )

    return (
        DecisionType.BUY,
        f"Aumentar posición: {w_cur:.1%} → {w_opt:.1%} ({(w_opt - w_cur):+.1%})",
        f"score {score:+.3f}",
    )


def _sell_guard(
    score: Optional[float],
    conv: Optional[float],
    w_cur: float,
    w_opt: float,
    delta: float,
) -> tuple[DecisionType, str, str]:
    """
    Devuelve:
      action, reason_primary, reason_secondary

    Regla MVP:
    - Score neutral sin concentración NO opera.
    - Score positivo sin concentración NO se vende.
    - Score negativo operable sí permite venta.
    """
    rango, label = classify_score(score)
    high_concentration = w_cur >= MAX_WEIGHT_CONC
    hard_concentration = w_cur >= MAX_WEIGHT_HARD_CONC

    # Score no disponible: no vender salvo concentración.
    if score is None:
        if high_concentration:
            return (
                DecisionType.SELL_PARTIAL,
                f"Reducir exposición: {w_cur:.1%} → {w_opt:.1%} ({delta:+.1%})",
                "rebalanceo por concentración; score no disponible",
            )

        return (
            DecisionType.HOLD,
            "Venta bloqueada: score no disponible",
            f"Optimizer sugería reducir {w_cur:.1%} → {w_opt:.1%}, "
            "pero no hay señal suficiente ni concentración",
        )

    # Score positivo: bloquear salvo concentración fuerte.
    if score >= SCORE_NEU_HIGH:
        if hard_concentration:
            return (
                DecisionType.SELL_PARTIAL,
                f"Reducir exposición: {w_cur:.1%} → {w_opt:.1%} ({delta:+.1%})",
                f"rebalanceo por concentración ({w_cur:.1%}); score positivo {score:+.3f}",
            )

        return (
            DecisionType.HOLD,
            f"Venta bloqueada: score positivo {score:+.3f}",
            f"Optimizer sugería reducir {w_cur:.1%} → {w_opt:.1%}, "
            "pero la señal del activo es positiva y no hay concentración excesiva",
        )

    # Score neutral: bloquear salvo concentración media.
    if rango == ScoreRange.NEUTRAL:
        if high_concentration:
            return (
                DecisionType.SELL_PARTIAL,
                f"Reducir exposición: {w_cur:.1%} → {w_opt:.1%} ({delta:+.1%})",
                f"rebalanceo por concentración ({w_cur:.1%}); score neutral/ruido {score:+.3f}",
            )

        return (
            DecisionType.HOLD,
            f"Venta bloqueada: score neutral/ruido {score:+.3f}",
            f"Optimizer sugería reducir {w_cur:.1%} → {w_opt:.1%}, "
            "pero una señal neutral no justifica operar",
        )

    # Score negativo débil: vender solo si delta relevante o concentración.
    if rango == ScoreRange.NEG_DEBIL:
        if abs(delta) >= 0.05 or high_concentration:
            return (
                DecisionType.SELL_PARTIAL,
                f"Reducir exposición: {w_cur:.1%} → {w_opt:.1%} ({delta:+.1%})",
                f"rebalanceo por señal negativa débil ({score:+.3f})",
            )

        return (
            DecisionType.HOLD,
            f"Venta bloqueada: señal negativa débil {score:+.3f}",
            f"Optimizer sugería reducir {w_cur:.1%} → {w_opt:.1%}, "
            "pero el delta no justifica costos/slippage",
        )

    # NEG_OPERABLE
    return (
        DecisionType.SELL_PARTIAL,
        f"Reducir exposición: {w_cur:.1%} → {w_opt:.1%} ({delta:+.1%})",
        f"rebalanceo por score negativo ({score:+.3f})",
    )


# ══════════════════════════════════════════════════════════════════════════════
# DERIVAR DECISIONES DESDE EL OPTIMIZER
# ══════════════════════════════════════════════════════════════════════════════

def derive_decision_intents(
    rebalance_report,
    signals_by_ticker: dict[str, AssetSignal],
    current_positions: dict[str, PositionSnapshot],
    portfolio_value_ars: float,
    gate: str,
    min_weight_delta: float = MIN_WEIGHT_DELTA,
    sell_full_thresh: float = SELL_FULL_THRESH,
) -> list[DecisionIntent]:
    """
    Traduce trades del optimizer en DecisionIntent.

    El optimizer propone targets.
    El execution planner decide si son operables.

    Guards MVP:
      - BUY con score negativo → BLOCKED
      - BUY con score débil → WATCH
      - SELL con score neutral sin concentración → HOLD
      - SELL con score positivo sin concentración → HOLD
    """
    intents: list[DecisionIntent] = []

    trades = getattr(rebalance_report, "trades", []) or []

    for trade in trades:
        ticker = str(getattr(trade, "ticker", "") or "").upper()
        if not ticker:
            continue

        w_cur = float(getattr(trade, "weight_current", 0.0) or 0.0)
        w_opt = float(getattr(trade, "weight_optimal", 0.0) or 0.0)
        delta = w_opt - w_cur

        sig = signals_by_ticker.get(ticker)
        score = sig.score if sig else None
        conv = sig.conviction if sig else None

        theoretical_ars = abs(delta) * portfolio_value_ars

        pos = current_positions.get(ticker)
        has_position = (pos is not None and pos.market_value_ars > 0) or w_cur > 0.001

        # ── SELL_FULL ───────────────────────────────────────────────────────
        if w_opt <= sell_full_thresh and has_position:
            action, reason_primary, reason_secondary = _sell_guard(
                score=score,
                conv=conv,
                w_cur=w_cur,
                w_opt=w_opt,
                delta=delta,
            )

            # Si el guard permitió vender, respetar liquidación completa.
            if action == DecisionType.SELL_PARTIAL:
                action = DecisionType.SELL_FULL
                reason_primary = f"Target {w_opt:.1%} — liquidar posición completa"

        # ── SELL_PARTIAL ────────────────────────────────────────────────────
        elif delta < -min_weight_delta:
            if gate == "BLOCKED":
                action = DecisionType.BLOCKED
                reason_primary = f"Gate {gate} — venta parcial bloqueada"
                reason_secondary = f"Delta objetivo: {delta:+.1%}"
            else:
                action, reason_primary, reason_secondary = _sell_guard(
                    score=score,
                    conv=conv,
                    w_cur=w_cur,
                    w_opt=w_opt,
                    delta=delta,
                )

        # ── BUY ─────────────────────────────────────────────────────────────
        elif delta > min_weight_delta:
            if gate in ("BLOCKED", "CAUTIOUS"):
                action = DecisionType.BLOCKED
                reason_primary = f"Gate {gate} — compra bloqueada"
                reason_secondary = f"Delta objetivo: {delta:+.1%} ({theoretical_ars:,.0f} ARS)"
            else:
                action, reason_primary, reason_secondary = _buy_guard(
                    score=score,
                    w_cur=w_cur,
                    w_opt=w_opt,
                    theoretical_ars=theoretical_ars,
                )

        # ── HOLD / WATCH por delta chico ────────────────────────────────────
        else:
            if sig and sig.score >= SCORE_BUY_MIN and sig.conviction >= 0.40:
                action = DecisionType.WATCH
                reason_primary = "Señal positiva — delta insuficiente para operar"
                reason_secondary = f"score {sig.score:+.3f}, delta {delta:+.1%} < umbral"
            else:
                action = DecisionType.HOLD
                reason_primary = f"Sin ventaja operativa clara (delta {delta:+.1%})"
                reason_secondary = None

        intents.append(DecisionIntent(
            ticker=ticker,
            action=action,
            reason_primary=reason_primary,
            reason_secondary=reason_secondary,
            current_weight=round(w_cur, 4),
            target_weight=round(w_opt, 4),
            delta_weight=round(delta, 4),
            score=round(score, 4) if score is not None else None,
            conviction=round(conv, 4) if conv is not None else None,
            theoretical_ars=round(theoretical_ars, 0),
        ))

    priority_order = {
        DecisionType.SELL_FULL: 0,
        DecisionType.SELL_PARTIAL: 1,
        DecisionType.BUY: 2,
        DecisionType.BLOCKED: 3,
        DecisionType.WATCH: 4,
        DecisionType.HOLD: 5,
    }

    intents.sort(key=lambda x: priority_order.get(x.action, 9))
    return intents


# ══════════════════════════════════════════════════════════════════════════════
# RECONCILIAR FONDOS → EXECUTION PLAN
# ══════════════════════════════════════════════════════════════════════════════

def reconcile_funding(
    decisions: list[DecisionIntent],
    current_positions: dict[str, PositionSnapshot],
    cash_before: float,
    portfolio_value_ars: float,
    gate: str,
    min_trade_ars: float = MIN_TRADE_ARS,
    fee_pct: float = FEE_PCT,
    slippage_pct: float = SLIPPAGE_PCT,
    external_buys: Optional[list[dict]] = None,
    allow_new_entries: bool = True,
) -> ExecutionPlan:
    """
    Convierte decisiones conceptuales en órdenes ejecutables con cash real.

    Orden:
      1. Ventas ejecutables
      2. Cash disponible
      3. Compras core ejecutables
      4. Compras externas del radar
      5. Bloqueadas / WATCH
      6. Cash accounting
    """
    warnings: list[str] = []
    sell_orders: list[OrderIntent] = []
    buy_orders: list[OrderIntent] = []
    blocked_orders: list[OrderIntent] = []
    pending_buys: list[str] = []

    cost_rate = fee_pct + slippage_pct

    # ── PASO 1: Ventas ejecutables ──────────────────────────────────────────
    sell_decisions = [
        d for d in decisions
        if d.action in (DecisionType.SELL_FULL, DecisionType.SELL_PARTIAL)
    ]

    for d in sell_decisions:
        amount = d.theoretical_ars

        if amount < min_trade_ars:
            warnings.append(
                f"{d.ticker}: venta ignorada (${amount:,.0f} < mínimo ${min_trade_ars:,.0f})"
            )
            continue

        pos = current_positions.get(d.ticker)
        ref_price = pos.price if pos and pos.price > 0 else 0.0
        qty_est = amount / ref_price if ref_price > 0 else 0.0

        sell_orders.append(OrderIntent(
            ticker=d.ticker,
            side=OrderSide.SELL,
            action=d.action,
            amount_ars=round(amount, 0),
            theoretical_ars=round(d.theoretical_ars, 0),
            quantity_est=round(qty_est, 4),
            reference_price=ref_price,
            reason=d.reason_primary,
            priority=0 if d.action == DecisionType.SELL_FULL else 1,
            funded_by=[],
            partial=False,
        ))

    # ── PASO 2: Cash disponible ─────────────────────────────────────────────
    gross_sell_ars = sum(o.amount_ars for o in sell_orders)
    fee_sell_ars = round(gross_sell_ars * cost_rate, 0)
    net_sell_ars = round(gross_sell_ars - fee_sell_ars, 0)
    available = cash_before + net_sell_ars

    logger.info(
        f"[reconcile] cash_before={cash_before:,.0f} + ventas_netas={net_sell_ars:,.0f} "
        f"= disponible={available:,.0f}"
    )

    # ── PASO 3: Compras core ejecutables ────────────────────────────────────
    buy_decisions = [d for d in decisions if d.action == DecisionType.BUY]
    buy_decisions.sort(
        key=lambda d: (
            -(d.conviction or 0.0),
            -(d.score or 0.0),
        )
    )

    sell_tickers = {o.ticker for o in sell_orders}

    for d in buy_decisions:
        wanted = d.theoretical_ars
        max_affordable = available / (1 + cost_rate) if cost_rate >= 0 else available
        executable = min(wanted, max_affordable)

        if executable < min_trade_ars:
            pending_buys.append(d.ticker)
            d.action = DecisionType.WATCH

            if executable > 0:
                d.reason_secondary = (
                    f"Señal positiva, pero solo hay ${executable:,.0f} ejecutables "
                    f"(< mínimo ${min_trade_ars:,.0f}); requiere más funding"
                )
                warnings.append(
                    f"{d.ticker}: compra reducida a ${executable:,.0f} "
                    f"(< mínimo ${min_trade_ars:,.0f}) — queda pendiente"
                )
            else:
                d.reason_secondary = (
                    f"Señal positiva sin cash disponible; requiere venta financiadora o swap "
                    f"para habilitar los ${wanted:,.0f} teóricos"
                )
                warnings.append(
                    f"{d.ticker}: sin cash para ejecutar compra "
                    f"(quería ${wanted:,.0f}) — queda pendiente"
                )
            continue

        is_partial = executable < wanted - 1
        fee_buy = round(executable * cost_rate, 0)

        buy_orders.append(OrderIntent(
            ticker=d.ticker,
            side=OrderSide.BUY,
            action=d.action,
            amount_ars=round(executable, 0),
            theoretical_ars=round(wanted, 0),
            quantity_est=0.0,
            reference_price=0.0,
            reason=d.reason_primary
                   + (
                       f" (parcial: ${executable:,.0f} de ${wanted:,.0f})"
                       if is_partial else ""
                   ),
            priority=2,
            funded_by=list(sell_tickers),
            partial=is_partial,
        ))

        if is_partial:
            warnings.append(
                f"{d.ticker}: compra parcial ${executable:,.0f} de ${wanted:,.0f} "
                f"({executable/wanted:.0%}) — funding insuficiente"
            )

        available -= (executable + fee_buy)

    # ── PASO 4: Entradas externas del radar ─────────────────────────────────
    if allow_new_entries and external_buys and gate == "NORMAL":
        for ext in external_buys:
            ticker = str(ext.get("ticker", "")).upper()
            wanted = float(ext.get("amount_ars", 0.0) or 0.0)
            score = float(ext.get("score", 0.0) or 0.0)
            reason = ext.get("reason", "Candidato radar externo")

            if score < SCORE_BUY_MIN:
                pending_buys.append(ticker)
                warnings.append(
                    f"{ticker} (radar): compra bloqueada por score insuficiente {score:+.3f}"
                )
                continue

            max_affordable = available / (1 + cost_rate) if cost_rate >= 0 else available
            executable = min(wanted, max_affordable)

            if executable < min_trade_ars:
                pending_buys.append(ticker)
                blocked_orders.append(OrderIntent(
                    ticker=ticker,
                    side=OrderSide.BUY,
                    action=DecisionType.WATCH,
                    amount_ars=0.0,
                    theoretical_ars=round(wanted, 0),
                    quantity_est=0.0,
                    reference_price=0.0,
                    reason="Sin cash disponible; requiere venta financiadora o swap",
                    priority=3,
                    funded_by=list(sell_tickers),
                ))
                continue

            is_partial = executable < wanted - 1
            fee_buy = round(executable * cost_rate, 0)

            buy_orders.append(OrderIntent(
                ticker=ticker,
                side=OrderSide.BUY,
                action=DecisionType.BUY,
                amount_ars=round(executable, 0),
                theoretical_ars=round(wanted, 0),
                quantity_est=0.0,
                reference_price=0.0,
                reason=reason + (" (parcial)" if is_partial else ""),
                priority=3,
                funded_by=list(sell_tickers),
                partial=is_partial,
            ))

            if is_partial:
                warnings.append(
                    f"{ticker} (radar): compra parcial ${executable:,.0f} de ${wanted:,.0f}"
                )

            available -= (executable + fee_buy)

    # ── PASO 5: Bloqueadas / WATCH ──────────────────────────────────────────
    for d in decisions:
        if d.action in (DecisionType.BLOCKED, DecisionType.WATCH):
            blocked_orders.append(OrderIntent(
                ticker=d.ticker,
                side=OrderSide.BUY if d.delta_weight > 0 else OrderSide.SELL,
                action=d.action,
                amount_ars=0.0,
                theoretical_ars=d.theoretical_ars,
                quantity_est=0.0,
                reference_price=0.0,
                reason=d.reason_secondary or d.reason_primary,
                priority=9,
            ))

    # ── PASO 6: Cash accounting ─────────────────────────────────────────────
    gross_buy_ars = sum(o.amount_ars for o in buy_orders)
    fee_buy_ars = round(gross_buy_ars * cost_rate, 0)
    cash_after = round(cash_before + net_sell_ars - gross_buy_ars - fee_buy_ars, 0)

    if cash_after < -100:
        warnings.append(
            f"ALERTA: cash_after={cash_after:,.0f} es negativo — revisar funding"
        )

    warnings = list(dict.fromkeys(warnings))

    # ── PASO 7: Resumen ─────────────────────────────────────────────────────
    n_sells = len(sell_orders)
    n_buys = len(buy_orders)
    n_blocked = len([o for o in blocked_orders if o.action == DecisionType.BLOCKED])
    n_watch = len([o for o in blocked_orders if o.action == DecisionType.WATCH])

    summary_parts = []

    if n_sells:
        summary_parts.append(
            f"{n_sells} venta{'s' if n_sells > 1 else ''} por ${gross_sell_ars:,.0f}"
        )

    if n_buys:
        summary_parts.append(
            f"{n_buys} compra{'s' if n_buys > 1 else ''} por ${gross_buy_ars:,.0f}"
        )

    if n_blocked:
        summary_parts.append(
            f"{n_blocked} bloqueada{'s' if n_blocked > 1 else ''} por guardias"
        )

    if n_watch:
        summary_parts.append(
            f"{n_watch} en WATCH por señal insuficiente"
        )

    if pending_buys:
        summary_parts.append(f"{len(pending_buys)} pendiente(s) por funding/señal")

    summary = " | ".join(summary_parts) if summary_parts else "Sin órdenes — mantener"

    logger.info(
        f"[reconcile] ventas=${gross_sell_ars:,.0f} compras=${gross_buy_ars:,.0f} "
        f"bloqueadas={n_blocked} watch={n_watch} cash_after=${cash_after:,.0f} "
        f"warnings={len(warnings)}"
    )

    return ExecutionPlan(
        decisions=decisions,
        sell_orders=sell_orders,
        buy_orders=buy_orders,
        blocked_orders=blocked_orders,
        cash_before=round(cash_before, 0),
        gross_sell_ars=round(gross_sell_ars, 0),
        fee_sell_ars=round(fee_sell_ars, 0),
        net_sell_ars=round(net_sell_ars, 0),
        gross_buy_ars=round(gross_buy_ars, 0),
        fee_buy_ars=round(fee_buy_ars, 0),
        cash_after=round(cash_after, 0),
        feasible=cash_after >= 0,
        gate=gate,
        summary=summary,
        warnings=warnings,
        pending_buys=pending_buys,
    )


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS DE CONVERSIÓN DESDE TIPOS ACTUALES DEL SISTEMA
# ══════════════════════════════════════════════════════════════════════════════

def build_signals_from_synthesis(results: list) -> dict[str, AssetSignal]:
    """
    Construye dict ticker → AssetSignal desde SynthesisResult actuales.
    Bridge para no romper el pipeline existente.
    """
    out: dict[str, AssetSignal] = {}

    for r in results or []:
        ticker = str(getattr(r, "ticker", "") or "").upper()
        if not ticker:
            continue

        score = float(getattr(r, "final_score", getattr(r, "score", 0.0)) or 0.0)
        conv = getattr(r, "conviction", getattr(r, "confidence", 0.0)) or 0.0
        conv = float(conv)

        if conv > 1.0:
            conv /= 100.0

        layers = {}
        for layer in getattr(r, "layers", []) or []:
            name = getattr(layer, "name", None)
            if name:
                layers[name] = float(getattr(layer, "weighted", 0.0))

        out[ticker] = AssetSignal(
            ticker=ticker,
            score=round(score, 4),
            conviction=round(conv, 4),
            technical=round(layers.get("technical", 0.0), 4),
            macro=round(layers.get("macro", 0.0), 4),
            sentiment=round(layers.get("sentiment", 0.0), 4),
        )

    return out


def build_positions_from_snapshot(
    positions_raw: list[dict],
    portfolio_value: float,
) -> dict[str, PositionSnapshot]:
    """Construye dict ticker → PositionSnapshot desde posiciones del DB."""
    out: dict[str, PositionSnapshot] = {}
    denom = portfolio_value if portfolio_value > 0 else 1.0

    for p in positions_raw or []:
        ticker = str(p.get("ticker", "") or "").upper()
        if not ticker:
            continue

        mv = float(p.get("market_value", 0.0) or 0.0)
        price = float(p.get("current_price", 0.0) or 0.0)
        qty = float(p.get("quantity", 0.0) or 0.0)

        out[ticker] = PositionSnapshot(
            ticker=ticker,
            quantity=qty,
            price=price,
            market_value_ars=mv,
            current_weight=round(mv / denom, 4),
        )

    return out

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
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)

# ── Parámetros operativos (todos sobreescribibles en llamada) ─────────────────
MIN_WEIGHT_DELTA  = 0.015    # 1.5 pp — diferencias menores → HOLD
MIN_TRADE_ARS     = 25_000   # monto mínimo para generar una orden
FEE_PCT           = 0.006    # 0.6% fee total (comisión + spread)
SLIPPAGE_PCT      = 0.0015   # 0.15% slippage estimado
SELL_FULL_THRESH  = 0.005    # target < 0.5% → SELL_FULL


# ══════════════════════════════════════════════════════════════════════════════
# TIPOS
# ══════════════════════════════════════════════════════════════════════════════

class Action(str, Enum):
    BUY          = "BUY"
    SELL_FULL    = "SELL_FULL"
    SELL_PARTIAL = "SELL_PARTIAL"
    HOLD         = "HOLD"
    WATCH        = "WATCH"
    BLOCKED      = "BLOCKED"


class OrderSide(str, Enum):
    BUY  = "BUY"
    SELL = "SELL"


class OrderStatus(str, Enum):
    PLANNED          = "PLANNED"
    SUBMITTED        = "SUBMITTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED           = "FILLED"
    CANCELLED        = "CANCELLED"
    EXPIRED          = "EXPIRED"


@dataclass
class AssetSignal:
    """Calidad de señal del activo, independiente de la decisión de cartera."""
    ticker:     str
    score:      float
    conviction: float
    technical:  float
    macro:      float
    sentiment:  float
    explanation: Optional[str] = None


@dataclass
class PositionSnapshot:
    """Posición actual en el portfolio."""
    ticker:           str
    quantity:         float
    price:            float
    market_value_ars: float
    current_weight:   float


@dataclass
class TargetWeight:
    """Un peso objetivo del optimizer para un ticker."""
    ticker:        str
    current_weight: float
    target_weight:  float
    delta_weight:   float   # target - current (puede ser negativo)


@dataclass
class PortfolioTarget:
    """
    Output teórico del optimizer.
    Aparece SOLO en la sección informativa del reporte, nunca como fuente
    de la acción principal ni del plan de rotación.
    """
    method:          str
    expected_return: float
    volatility:      float
    sharpe:          float
    targets:         list[TargetWeight]


@dataclass
class DecisionIntent:
    """
    Decisión de cartera para un ticker.
    Combina la señal del activo con la lógica de rebalanceo y las restricciones
    del gate. Separa explícitamente señal vs. decisión de cartera.
    """
    ticker:           str
    action:           Action
    reason_primary:   str
    reason_secondary: Optional[str]
    current_weight:   float
    target_weight:    float
    delta_weight:     float
    score:            Optional[float] = None
    conviction:       Optional[float] = None
    # Teórico (antes de restricciones de funding)
    theoretical_ars:  float = 0.0


@dataclass
class OrderIntent:
    """
    Una orden ejecutable concreta con monto real disponible.
    El amount_ars puede ser menor al theoretical_ars si el cash no alcanza.
    """
    ticker:          str
    side:            OrderSide
    action:          Action
    amount_ars:      float          # monto ejecutable REAL (reconciliado)
    theoretical_ars: float          # lo que el optimizer quería
    quantity_est:    float          # cantidad estimada de títulos
    reference_price: float          # precio de referencia en ARS
    reason:          str
    priority:        int            # menor = más urgente
    funded_by:       list[str]      = field(default_factory=list)
    partial:         bool           = False   # True si amount < theoretical
    status:          OrderStatus    = OrderStatus.PLANNED
    # Campos para lifecycle futuro
    planned_qty:     Optional[float] = None
    filled_qty:      Optional[float] = None
    avg_fill_price:  Optional[float] = None
    submitted_at:    Optional[str]   = None
    filled_at:       Optional[str]   = None


@dataclass
class ExecutionPlan:
    """
    Plan ejecutable completo. Fuente única de verdad para el reporte operativo.

    La consistencia matemática garantizada:
        cash_after = cash_before + gross_sell_net - gross_buy_ars
        gross_sell_net = gross_sell_ars * (1 - fee_pct - slippage_pct)
        gross_buy_ars  = sum(o.amount_ars for o in buy_orders)
        gross_sell_ars = sum(o.amount_ars for o in sell_orders)
    """
    # Decisiones por ticker (nivel conceptual)
    decisions:       list[DecisionIntent]

    # Órdenes concretas (nivel operativo)
    sell_orders:     list[OrderIntent]
    buy_orders:      list[OrderIntent]
    blocked_orders:  list[OrderIntent]   # gate impidió la acción

    # Cash accounting (siempre cuadra)
    cash_before:     float
    gross_sell_ars:  float    # suma raw de ventas
    fee_sell_ars:    float    # costo de las ventas
    net_sell_ars:    float    # gross_sell_ars - fee_sell_ars
    gross_buy_ars:   float    # suma de compras ejecutables
    fee_buy_ars:     float    # costo de las compras
    cash_after:      float    # cash_before + net_sell_ars - gross_buy_ars - fee_buy_ars

    # Estado
    feasible:        bool
    gate:            str      # NORMAL | CAUTIOUS | BLOCKED
    summary:         str
    warnings:        list[str] = field(default_factory=list)

    # Acciones pendientes por falta de funding
    pending_buys:    list[str] = field(default_factory=list)

    @property
    def main_action(self) -> Optional[OrderIntent]:
        """La orden más importante del plan (primera venta, o primera compra)."""
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
        """Veredicto final derivado del plan — nunca de otra lógica."""
        if not self.feasible:
            return "Sin plan ejecutable — revisar restricciones del sistema."
        if self.gate == "BLOCKED":
            return "Sistema bloqueado por gate de riesgo — solo stops de emergencia."
        if not self.has_orders:
            return "Mantener y observar — sin ventaja operativa para actuar hoy."
        sells = [o for o in self.sell_orders]
        buys  = [o for o in self.buy_orders]
        if sells and buys:
            return (
                "Plan de rotación definido — ejecutar ventas primero, "
                "luego reasignar capital en el orden indicado."
            )
        if sells:
            return "Reducir exposición — ejecutar ventas. Sin compras habilitadas por el régimen."
        if buys:
            return "Aumentar exposición selectiva — ejecutar compras en el orden indicado."
        return "Mantener y observar — sin ventaja operativa para actuar hoy."


# ══════════════════════════════════════════════════════════════════════════════
# DERIVAR DECISIONES DESDE EL OPTIMIZER
# ══════════════════════════════════════════════════════════════════════════════

def derive_decision_intents(
    rebalance_report,
    signals_by_ticker:   dict[str, AssetSignal],
    current_positions:   dict[str, PositionSnapshot],
    portfolio_value_ars: float,
    gate:                str,
    min_weight_delta:    float = MIN_WEIGHT_DELTA,
    sell_full_thresh:    float = SELL_FULL_THRESH,
) -> list[DecisionIntent]:
    """
    Traduce los trades del RebalanceReport en DecisionIntents tipados.

    Separa explícitamente:
      - señal del activo (AssetSignal)
      - decisión de cartera (Action derivada del delta y las restricciones)

    Reglas de clasificación:
      target < sell_full_thresh y hay posición → SELL_FULL
      delta < -min_weight_delta               → SELL_PARTIAL
      delta >  min_weight_delta               → BUY
      |delta| <= min_weight_delta             → HOLD
      buena señal pero gate bloquea           → BLOCKED
      señal positiva sin entry clara          → WATCH
    """
    intents: list[DecisionIntent] = []

    trades = getattr(rebalance_report, "trades", []) or []

    for trade in trades:
        ticker = str(getattr(trade, "ticker", "") or "").upper()
        if not ticker:
            continue

        w_cur  = float(getattr(trade, "weight_current", 0.0) or 0.0)
        w_opt  = float(getattr(trade, "weight_optimal", 0.0) or 0.0)
        delta  = w_opt - w_cur
        sig    = signals_by_ticker.get(ticker)
        score  = sig.score      if sig else None
        conv   = sig.conviction if sig else None

        theoretical_ars = abs(delta) * portfolio_value_ars

        # ── Clasificar acción ──────────────────────────────────────────────
        pos = current_positions.get(ticker)
        has_position = (pos is not None and pos.market_value_ars > 0) or w_cur > 0.001

        if w_opt <= sell_full_thresh and has_position:
            action           = Action.SELL_FULL
            reason_primary   = f"Target {w_opt:.1%} — liquidar posición completa"
            reason_secondary = f"Score: {score:+.3f}" if score is not None else None

        elif delta < -min_weight_delta:
            if gate in ("BLOCKED",):
                action         = Action.BLOCKED
                reason_primary = f"Gate {gate} — venta parcial bloqueada"
                reason_secondary = f"Delta objetivo: {delta:+.1%}"
            else:
                action           = Action.SELL_PARTIAL
                reason_primary   = f"Reducir exposición: {w_cur:.1%} → {w_opt:.1%} ({delta:+.1%})"
                reason_secondary = (
                    "concentración excesiva" if w_cur > 0.30
                    else "rebalanceo por score"
                )

        elif delta > min_weight_delta:
            if gate in ("BLOCKED", "CAUTIOUS"):
                action           = Action.BLOCKED
                reason_primary   = f"Gate {gate} — compra bloqueada"
                reason_secondary = f"Delta objetivo: {delta:+.1%} ({theoretical_ars:,.0f} ARS)"
            else:
                action           = Action.BUY
                reason_primary   = f"Aumentar posición: {w_cur:.1%} → {w_opt:.1%} ({delta:+.1%})"
                reason_secondary = (
                    f"score {score:+.3f}" if score is not None else None
                )

        else:
            # Delta pequeño — HOLD o WATCH según la señal
            if sig and sig.score >= 0.08 and sig.conviction >= 0.40:
                action           = Action.WATCH
                reason_primary   = "Señal positiva — delta insuficiente para operar"
                reason_secondary = f"score {sig.score:+.3f}, delta {delta:+.1%} < umbral"
            else:
                action           = Action.HOLD
                reason_primary   = f"Sin ventaja operativa clara (delta {delta:+.1%})"
                reason_secondary = None

        intents.append(DecisionIntent(
            ticker           = ticker,
            action           = action,
            reason_primary   = reason_primary,
            reason_secondary = reason_secondary,
            current_weight   = round(w_cur, 4),
            target_weight    = round(w_opt, 4),
            delta_weight     = round(delta, 4),
            score            = round(score, 4) if score is not None else None,
            conviction       = round(conv, 4)  if conv  is not None else None,
            theoretical_ars  = round(theoretical_ars, 0),
        ))

    # Ordenar: SELL_FULL > SELL_PARTIAL > BUY > BLOCKED > WATCH > HOLD
    priority_order = {
        Action.SELL_FULL:    0,
        Action.SELL_PARTIAL: 1,
        Action.BUY:          2,
        Action.BLOCKED:      3,
        Action.WATCH:        4,
        Action.HOLD:         5,
    }
    intents.sort(key=lambda x: priority_order.get(x.action, 9))
    return intents


# ══════════════════════════════════════════════════════════════════════════════
# RECONCILIAR FONDOS → EXECUTION PLAN
# ══════════════════════════════════════════════════════════════════════════════

def reconcile_funding(
    decisions:           list[DecisionIntent],
    current_positions:   dict[str, PositionSnapshot],
    cash_before:         float,
    portfolio_value_ars: float,
    gate:                str,
    min_trade_ars:       float = MIN_TRADE_ARS,
    fee_pct:             float = FEE_PCT,
    slippage_pct:        float = SLIPPAGE_PCT,
    # Candidatos externos del radar (adicionales al rebalanceo)
    external_buys:       Optional[list[dict]] = None,
    allow_new_entries:   bool = True,
) -> ExecutionPlan:
    """
    Convierte decisiones conceptuales en órdenes ejecutables con cash real.

    Cash accounting (determinístico):
        net_cash_from_sells = gross_sell_ars × (1 - fee_pct - slippage_pct)
        available           = cash_before + net_cash_from_sells
        gross_buy_ars       = sum de compras ejecutadas (hasta agotar available)
        fee_buy_ars         = gross_buy_ars × (fee_pct + slippage_pct)
        cash_after          = available - gross_buy_ars - fee_buy_ars

    Orden de construcción del plan:
        1. SELL_FULL  (liquidaciones totales — obligatorias)
        2. SELL_PARTIAL (recortes por rebalanceo/riesgo)
        3. BUY core  (posiciones existentes con señal de aumento)
        4. BUY nuevo del radar externo
        5. Cash remanente

    Reglas de funding:
        - Si no hay cash suficiente para una BUY completa → ejecutar parcial
        - Si monto parcial < min_trade_ars → no generar la orden (pending)
        - Nunca generar orden BUY que deje cash_after < 0
    """
    warnings: list[str] = []
    sell_orders:    list[OrderIntent] = []
    buy_orders:     list[OrderIntent] = []
    blocked_orders: list[OrderIntent] = []
    pending_buys:   list[str]         = []

    cost_rate = fee_pct + slippage_pct  # tasa total por operación

    # ── PASO 1: Construir ventas ──────────────────────────────────────────────
    # Ventas primero (siempre ejecutables — no dependen de cash)
    sell_decisions = [
        d for d in decisions
        if d.action in (Action.SELL_FULL, Action.SELL_PARTIAL)
    ]

    for d in sell_decisions:
        amount = d.theoretical_ars
        if amount < min_trade_ars:
            warnings.append(
                f"{d.ticker}: venta ignorada (${amount:,.0f} < mínimo ${min_trade_ars:,.0f})"
            )
            continue

        ref_price = 0.0
        pos = current_positions.get(d.ticker)
        if pos and pos.price > 0:
            ref_price = pos.price

        qty_est = amount / ref_price if ref_price > 0 else 0.0

        sell_orders.append(OrderIntent(
            ticker          = d.ticker,
            side            = OrderSide.SELL,
            action          = d.action,
            amount_ars      = round(amount, 0),
            theoretical_ars = round(d.theoretical_ars, 0),
            quantity_est    = round(qty_est, 4),
            reference_price = ref_price,
            reason          = d.reason_primary,
            priority        = 0 if d.action == Action.SELL_FULL else 1,
            funded_by       = [],
            partial         = False,
        ))

    # ── PASO 2: Cash disponible post-ventas ───────────────────────────────────
    gross_sell_ars = sum(o.amount_ars for o in sell_orders)
    fee_sell_ars   = round(gross_sell_ars * cost_rate, 0)
    net_sell_ars   = round(gross_sell_ars - fee_sell_ars, 0)
    available      = cash_before + net_sell_ars

    logger.info(
        f"[reconcile] cash_before={cash_before:,.0f} + ventas_netas={net_sell_ars:,.0f} "
        f"= disponible={available:,.0f}"
    )

    # ── PASO 3: Construir compras en orden de prioridad ───────────────────────
    buy_decisions = [d for d in decisions if d.action == Action.BUY]
    # Prioridad: mayor convicción primero, luego mayor score
    buy_decisions.sort(
        key=lambda d: (
            -(d.conviction or 0.0),
            -(d.score      or 0.0),
        )
    )

    sell_tickers = {o.ticker for o in sell_orders}

    for d in buy_decisions:
        wanted = d.theoretical_ars

        # Limitar por cash disponible
        executable = min(wanted, available)

        if executable < min_trade_ars:
            pending_buys.append(d.ticker)
            if executable > 0:
                warnings.append(
                    f"{d.ticker}: compra reducida a ${executable:,.0f} "
                    f"(< mínimo ${min_trade_ars:,.0f}) — queda pendiente"
                )
            else:
                warnings.append(
                    f"{d.ticker}: sin cash para ejecutar compra "
                    f"(quería ${wanted:,.0f}) — queda pendiente"
                )
            continue

        is_partial = executable < wanted - 1  # tolerancia $1
        fee_buy    = round(executable * cost_rate, 0)

        buy_orders.append(OrderIntent(
            ticker          = d.ticker,
            side            = OrderSide.BUY,
            action          = d.action,
            amount_ars      = round(executable, 0),
            theoretical_ars = round(wanted, 0),
            quantity_est    = 0.0,   # se completa con precios reales
            reference_price = 0.0,
            reason          = d.reason_primary
                              + (f" (parcial: ${executable:,.0f} de ${wanted:,.0f})" if is_partial else ""),
            priority        = 2,
            funded_by       = list(sell_tickers),
            partial         = is_partial,
        ))

        if is_partial:
            warnings.append(
                f"{d.ticker}: compra parcial ${executable:,.0f} de ${wanted:,.0f} "
                f"({executable/wanted:.0%}) — funding insuficiente"
            )

        available -= (executable + fee_buy)

    # ── PASO 4: Entradas externas del radar (si hay cash disponible) ──────────
    if allow_new_entries and external_buys and gate == "NORMAL":
        for ext in external_buys:
            ticker  = str(ext.get("ticker", "")).upper()
            wanted  = float(ext.get("amount_ars", 0.0) or 0.0)
            score   = float(ext.get("score", 0.0) or 0.0)
            reason  = ext.get("reason", "Candidato radar externo")

            executable = min(wanted, available)
            if executable < min_trade_ars:
                pending_buys.append(ticker)
                continue

            is_partial = executable < wanted - 1
            fee_buy    = round(executable * cost_rate, 0)

            buy_orders.append(OrderIntent(
                ticker          = ticker,
                side            = OrderSide.BUY,
                action          = Action.BUY,
                amount_ars      = round(executable, 0),
                theoretical_ars = round(wanted, 0),
                quantity_est    = 0.0,
                reference_price = 0.0,
                reason          = reason + (" (parcial)" if is_partial else ""),
                priority        = 3,
                funded_by       = list(sell_tickers),
                partial         = is_partial,
            ))

            if is_partial:
                warnings.append(
                    f"{ticker} (radar): compra parcial ${executable:,.0f} de ${wanted:,.0f}"
                )

            available -= (executable + fee_buy)

    # ── PASO 5: Órdenes bloqueadas por gate ───────────────────────────────────
    for d in decisions:
        if d.action == Action.BLOCKED:
            blocked_orders.append(OrderIntent(
                ticker          = d.ticker,
                side            = OrderSide.BUY if d.delta_weight > 0 else OrderSide.SELL,
                action          = Action.BLOCKED,
                amount_ars      = 0.0,
                theoretical_ars = d.theoretical_ars,
                quantity_est    = 0.0,
                reference_price = 0.0,
                reason          = d.reason_primary,
                priority        = 9,
            ))

    # ── PASO 6: Cash accounting final ─────────────────────────────────────────
    gross_buy_ars = sum(o.amount_ars for o in buy_orders)
    fee_buy_ars   = round(gross_buy_ars * cost_rate, 0)
    cash_after    = round(cash_before + net_sell_ars - gross_buy_ars - fee_buy_ars, 0)

    if cash_after < -100:  # tolerancia $100 por redondeos
        warnings.append(
            f"ALERTA: cash_after={cash_after:,.0f} es negativo — "
            f"revisar cálculo de funding"
        )

    # ── PASO 7: Resumen ────────────────────────────────────────────────────────
    n_sells = len(sell_orders)
    n_buys  = len(buy_orders)
    summary_parts = []
    if n_sells:
        summary_parts.append(
            f"{n_sells} venta{'s' if n_sells>1 else ''} por ${gross_sell_ars:,.0f}"
        )
    if n_buys:
        summary_parts.append(
            f"{n_buys} compra{'s' if n_buys>1 else ''} por ${gross_buy_ars:,.0f}"
        )
    if pending_buys:
        summary_parts.append(f"{len(pending_buys)} compra(s) pendiente(s) por funding")
    summary = " | ".join(summary_parts) if summary_parts else "Sin órdenes — mantener"

    logger.info(
        f"[reconcile] ventas=${gross_sell_ars:,.0f} compras=${gross_buy_ars:,.0f} "
        f"cash_after=${cash_after:,.0f} warnings={len(warnings)}"
    )

    return ExecutionPlan(
        decisions       = decisions,
        sell_orders     = sell_orders,
        buy_orders      = buy_orders,
        blocked_orders  = blocked_orders,
        cash_before     = round(cash_before, 0),
        gross_sell_ars  = round(gross_sell_ars, 0),
        fee_sell_ars    = round(fee_sell_ars, 0),
        net_sell_ars    = round(net_sell_ars, 0),
        gross_buy_ars   = round(gross_buy_ars, 0),
        fee_buy_ars     = round(fee_buy_ars, 0),
        cash_after      = round(cash_after, 0),
        feasible        = cash_after >= 0,
        gate            = gate,
        summary         = summary,
        warnings        = warnings,
        pending_buys    = pending_buys,
    )


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS DE CONVERSIÓN DESDE TIPOS ACTUALES DEL SISTEMA
# ══════════════════════════════════════════════════════════════════════════════

def build_signals_from_synthesis(results: list) -> dict[str, AssetSignal]:
    """
    Construye el dict ticker → AssetSignal desde los SynthesisResult actuales.
    Bridge para no romper el pipeline existente.
    """
    out: dict[str, AssetSignal] = {}
    for r in results or []:
        ticker = str(getattr(r, "ticker", "") or "").upper()
        if not ticker:
            continue

        score = float(getattr(r, "final_score", getattr(r, "score", 0.0)) or 0.0)
        conv  = getattr(r, "conviction", getattr(r, "confidence", 0.0)) or 0.0
        conv  = float(conv)
        if conv > 1.0:
            conv /= 100.0

        # Extraer capas individuales
        layers = {}
        for layer in getattr(r, "layers", []) or []:
            name = getattr(layer, "name", None)
            if name:
                layers[name] = float(getattr(layer, "weighted", 0.0))

        out[ticker] = AssetSignal(
            ticker     = ticker,
            score      = round(score, 4),
            conviction = round(conv, 4),
            technical  = round(layers.get("technical", 0.0), 4),
            macro      = round(layers.get("macro", 0.0), 4),
            sentiment  = round(layers.get("sentiment", 0.0), 4),
        )
    return out


def build_positions_from_snapshot(
    positions_raw:    list[dict],
    portfolio_value:  float,
) -> dict[str, PositionSnapshot]:
    """Construye dict ticker → PositionSnapshot desde las posiciones del DB."""
    out: dict[str, PositionSnapshot] = {}
    denom = portfolio_value if portfolio_value > 0 else 1.0
    for p in positions_raw or []:
        ticker = str(p.get("ticker", "") or "").upper()
        if not ticker:
            continue
        mv     = float(p.get("market_value", 0.0) or 0.0)
        price  = float(p.get("current_price", 0.0) or 0.0)
        qty    = float(p.get("quantity", 0.0) or 0.0)
        out[ticker] = PositionSnapshot(
            ticker           = ticker,
            quantity         = qty,
            price            = price,
            market_value_ars = mv,
            current_weight   = round(mv / denom, 4),
        )
    return out

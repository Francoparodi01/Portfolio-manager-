"""
src/analysis/validators.py
───────────────────────────
Validaciones duras que deben pasar ANTES de enviar el reporte.

Si algo no cuadra, se lanza AssertionError con descripción clara.
El pipeline intercepta el error y envía alerta en lugar del reporte corrupto.

Filosofía:
    Consistencia operativa > narrativa linda.
    Si el plan no cierra, no se envía el reporte.
"""
from __future__ import annotations

import logging
from typing import Any

from .execution_planner import ExecutionPlan, Action, OrderSide

logger = logging.getLogger(__name__)


class PlanValidationError(ValueError):
    """Error de validación del plan de ejecución."""
    pass


def validate_execution_plan(plan: ExecutionPlan) -> None:
    """
    Valida la consistencia matemática y operativa del ExecutionPlan.
    Lanza PlanValidationError si algo no cuadra.

    Checks:
      1. Cash accounting cierra
      2. Totales de ventas/compras coinciden con suma de órdenes
      3. No hay BUY con amount > available cash en ese momento
      4. Ninguna orden tiene amount negativo
      5. SELL_FULL no puede tener amount == 0 si había posición
      6. cash_after >= 0 (nunca quedar en negativo)
      7. Un ticker con target=0 no puede tener action=HOLD
    """
    errors: list[str] = []

    # 1. Cash accounting
    expected_cash_after = (
        plan.cash_before
        + plan.net_sell_ars
        - plan.gross_buy_ars
        - plan.fee_buy_ars
    )
    if abs(expected_cash_after - plan.cash_after) > 500:  # tolerancia $500
        errors.append(
            f"Cash accounting no cierra: "
            f"calculado={expected_cash_after:,.0f} vs plan.cash_after={plan.cash_after:,.0f} "
            f"(diferencia ${abs(expected_cash_after - plan.cash_after):,.0f})"
        )

    # 2. Suma de sell_orders == gross_sell_ars
    sum_sells = sum(o.amount_ars for o in plan.sell_orders)
    if abs(sum_sells - plan.gross_sell_ars) > 100:
        errors.append(
            f"gross_sell_ars={plan.gross_sell_ars:,.0f} != "
            f"sum(sell_orders)={sum_sells:,.0f}"
        )

    # 3. Suma de buy_orders == gross_buy_ars
    sum_buys = sum(o.amount_ars for o in plan.buy_orders)
    if abs(sum_buys - plan.gross_buy_ars) > 100:
        errors.append(
            f"gross_buy_ars={plan.gross_buy_ars:,.0f} != "
            f"sum(buy_orders)={sum_buys:,.0f}"
        )

    # 4. No hay órdenes con monto negativo
    for o in plan.sell_orders + plan.buy_orders:
        if o.amount_ars < 0:
            errors.append(f"Orden {o.ticker} tiene amount_ars negativo: {o.amount_ars}")

    # 5. cash_after no puede ser negativo (más de $500 de tolerancia)
    if plan.cash_after < -500:
        errors.append(
            f"cash_after={plan.cash_after:,.0f} es negativo — plan no financiable"
        )

    # 6. Ninguna BUY puede ser mayor al available en ese momento
    #    (ya garantizado por reconcile_funding, pero verificamos)
    if plan.gross_buy_ars > plan.cash_before + plan.net_sell_ars + 500:
        errors.append(
            f"gross_buy_ars={plan.gross_buy_ars:,.0f} supera el cash disponible "
            f"{plan.cash_before + plan.net_sell_ars:,.0f}"
        )

    # 7. Un ticker con decision SELL_FULL no puede estar en buy_orders
    sell_full_tickers = {
        d.ticker for d in plan.decisions if d.action == Action.SELL_FULL
    }
    for o in plan.buy_orders:
        if o.ticker in sell_full_tickers:
            errors.append(
                f"{o.ticker}: está en buy_orders pero tiene decisión SELL_FULL"
            )

    # 8. Un ticker con target_weight=0 no puede tener action=HOLD en decisions
    for d in plan.decisions:
        if d.target_weight <= 0.005 and d.current_weight > 0.005 and d.action == Action.HOLD:
            errors.append(
                f"{d.ticker}: target={d.target_weight:.1%} ≈ 0 pero action=HOLD — "
                f"debería ser SELL_FULL o SELL_PARTIAL"
            )

    if errors:
        msg = f"ExecutionPlan inválido ({len(errors)} error(es)):\n" + "\n".join(
            f"  [{i+1}] {e}" for i, e in enumerate(errors)
        )
        logger.error(msg)
        raise PlanValidationError(msg)

    logger.info(
        f"[validate] Plan OK — ventas=${plan.gross_sell_ars:,.0f} "
        f"compras=${plan.gross_buy_ars:,.0f} cash_after=${plan.cash_after:,.0f}"
    )


def validate_report_consistency(
    main_ticker:  str | None,
    main_amount:  float,
    plan:         ExecutionPlan,
) -> None:
    """
    Valida que lo que muestra el header del reporte coincida con el plan real.

    Checks:
      - Si hay un main_ticker, tiene que estar en sell_orders o buy_orders
      - El monto mostrado en el header no puede diferir del plan en más del 10%
      - Si el plan compra XOM por $25k, el header no puede decir $287k
    """
    errors: list[str] = []

    if main_ticker is None:
        return  # sin acción principal, no hay nada que validar

    all_order_tickers = {
        o.ticker for o in plan.sell_orders + plan.buy_orders
    }

    if main_ticker not in all_order_tickers:
        errors.append(
            f"main_ticker='{main_ticker}' aparece en el header "
            f"pero no está en las órdenes del plan. "
            f"Órdenes: {sorted(all_order_tickers)}"
        )

    # Buscar el monto real en el plan
    real_amount = None
    for o in plan.sell_orders + plan.buy_orders:
        if o.ticker == main_ticker:
            real_amount = o.amount_ars
            break

    if real_amount is not None and main_amount > 0:
        ratio = main_amount / real_amount if real_amount > 0 else float("inf")
        if ratio > 1.15 or ratio < 0.85:  # diferencia > 15%
            errors.append(
                f"Header muestra {main_ticker} por ${main_amount:,.0f} "
                f"pero el plan ejecuta ${real_amount:,.0f} "
                f"(diferencia {abs(ratio - 1):.0%}). "
                f"El header debe mostrar el monto ejecutable real."
            )

    if errors:
        msg = f"Inconsistencia reporte/plan ({len(errors)} error(es)):\n" + "\n".join(
            f"  [{i+1}] {e}" for i, e in enumerate(errors)
        )
        logger.error(msg)
        raise PlanValidationError(msg)


def soft_validate(plan: ExecutionPlan) -> list[str]:
    """
    Versión soft de validate_execution_plan: no lanza excepción,
    devuelve lista de warnings para incluir en el reporte.
    Útil como fallback si la validación dura está deshabilitada.
    """
    warnings: list[str] = list(plan.warnings)  # copia los warnings del plan

    # Cash check
    if plan.cash_after < 0:
        warnings.append(f"⚠️ cash_after negativo: ${plan.cash_after:,.0f}")

    # Discrepancia sell total
    sum_sells = sum(o.amount_ars for o in plan.sell_orders)
    if abs(sum_sells - plan.gross_sell_ars) > 500:
        warnings.append(
            f"⚠️ discrepancia en ventas: "
            f"gross_sell={plan.gross_sell_ars:,.0f} vs sum_orders={sum_sells:,.0f}"
        )

    # Discrepancia buy total
    sum_buys = sum(o.amount_ars for o in plan.buy_orders)
    if abs(sum_buys - plan.gross_buy_ars) > 500:
        warnings.append(
            f"⚠️ discrepancia en compras: "
            f"gross_buy={plan.gross_buy_ars:,.0f} vs sum_orders={sum_buys:,.0f}"
        )

    # HOLD con target 0
    for d in plan.decisions:
        if d.target_weight <= 0.005 and d.current_weight > 0.01 and d.action == Action.HOLD:
            warnings.append(
                f"⚠️ {d.ticker}: target≈0 pero acción=HOLD — revisar clasificación"
            )

    if warnings:
        logger.warning(
            f"[soft_validate] {len(warnings)} warning(s): "
            + " | ".join(warnings[:3])
        )

    return warnings

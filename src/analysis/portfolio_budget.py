"""Portfolio budget contract for optimizer sleeves.

Frozen/non-evaluable holdings are capital, not missing data.  They remain in the
portfolio denominator and cannot be implicitly liquidated to fund the optimizer.
"""
from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Iterable, Mapping, Any


class PortfolioBudgetViolation(ValueError):
    pass


@dataclass(frozen=True)
class PortfolioBudget:
    portfolio_value_ars: float
    frozen_value_ars: float
    cash_reserved_ars: float
    optimizable_value_ars: float
    frozen_weight: float
    cash_reserved_weight: float
    optimizable_budget: float
    implicit_frozen_value_ars: float = 0.0
    excluded_candidate_value_ars: float = 0.0

    def invariant_total(self, *, optimizer_cash_weight: float = 0.0,
                        optimized_weight: float | None = None) -> float:
        optimized = self.optimizable_budget - float(optimizer_cash_weight) if optimized_weight is None else float(optimized_weight)
        return self.frozen_weight + self.cash_reserved_weight + optimized + float(optimizer_cash_weight)

    def validate(self, *, optimized_weight: float, optimizer_cash_weight: float = 0.0,
                 tolerance: float = 2e-3) -> None:
        validate_weight_invariant(
            frozen_weight=self.frozen_weight,
            optimized_weight=optimized_weight,
            cash_weight=self.cash_reserved_weight + optimizer_cash_weight,
            tolerance=tolerance,
        )
        if optimized_weight + optimizer_cash_weight > self.optimizable_budget + tolerance:
            raise PortfolioBudgetViolation(
                "optimized sleeve exceeds optimizable budget: "
                f"{optimized_weight + optimizer_cash_weight:.6f} > {self.optimizable_budget:.6f}"
            )


def _mv(position: Mapping[str, Any]) -> float:
    try:
        return max(0.0, float(position.get("market_value", 0.0) or 0.0))
    except (TypeError, ValueError):
        return 0.0


def build_portfolio_budget(
    current_positions: Iterable[Mapping[str, Any]],
    *,
    optimized_tickers: Iterable[str],
    portfolio_value_ars: float,
    cash_reserved_ars: float,
) -> PortfolioBudget:
    total = max(0.0, float(portfolio_value_ars or 0.0))
    if total <= 0:
        raise PortfolioBudgetViolation("portfolio_value_ars must be positive")

    positions = list(current_positions or [])
    optimized = {str(t or "").upper().strip() for t in optimized_tickers if str(t or "").strip()}
    candidate_value = sum(_mv(p) for p in positions)
    optimizable_value = sum(
        _mv(p)
        for p in positions
        if str(p.get("ticker", "") or "").upper().strip() in optimized
    )
    excluded_candidate = max(0.0, candidate_value - optimizable_value)
    cash = min(max(0.0, float(cash_reserved_ars or 0.0)), total)

    # Capital not passed to the optimizer is still part of the denominator.
    implicit_frozen = max(0.0, total - cash - candidate_value)
    frozen = min(total, implicit_frozen + excluded_candidate)
    frozen_weight = frozen / total
    cash_weight = cash / total
    budget = max(0.0, 1.0 - frozen_weight - cash_weight)

    # The dollar value implied by the budget is authoritative.  We deliberately
    # do not invent prices or assume excluded positions can be sold.
    optimizable_budget_value = budget * total
    return PortfolioBudget(
        portfolio_value_ars=total,
        frozen_value_ars=frozen,
        cash_reserved_ars=cash,
        optimizable_value_ars=optimizable_budget_value,
        frozen_weight=frozen_weight,
        cash_reserved_weight=cash_weight,
        optimizable_budget=budget,
        implicit_frozen_value_ars=implicit_frozen,
        excluded_candidate_value_ars=excluded_candidate,
    )


def scale_optimizer_sleeve(
    weights: Mapping[str, float],
    *,
    optimizer_cash_weight: float,
    budget: float,
) -> tuple[dict[str, float], float]:
    risky = {str(k): max(0.0, float(v or 0.0)) for k, v in (weights or {}).items()}
    cash = max(0.0, float(optimizer_cash_weight or 0.0))
    sleeve_total = sum(risky.values()) + cash
    budget = max(0.0, min(1.0, float(budget or 0.0)))
    if sleeve_total <= 1e-12 or budget <= 0:
        return {k: 0.0 for k in risky}, 0.0
    scale = budget / sleeve_total
    return ({k: v * scale for k, v in risky.items()}, cash * scale)


def validate_weight_invariant(*, frozen_weight: float, optimized_weight: float,
                              cash_weight: float, tolerance: float = 2e-3) -> None:
    values = (float(frozen_weight), float(optimized_weight), float(cash_weight))
    if not all(math.isfinite(v) and v >= -tolerance for v in values):
        raise PortfolioBudgetViolation(f"invalid portfolio weights: {values}")
    total = sum(values)
    if abs(total - 1.0) > tolerance:
        raise PortfolioBudgetViolation(
            "portfolio budget invariant violated: "
            f"frozen={values[0]:.6f} + optimized={values[1]:.6f} + "
            f"cash={values[2]:.6f} = {total:.6f}"
        )

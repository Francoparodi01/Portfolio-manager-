from __future__ import annotations

import warnings
from enum import Enum, EnumMeta


class DeprecatedEnumMeta(EnumMeta):
    """EnumMeta que avisa cuando se instancia un wrapper legacy."""

    def __call__(cls, value, *args, **kwargs):
        warnings.warn(
            f"{cls.__name__} esta deprecado; usar DecisionType",
            DeprecationWarning,
            stacklevel=2,
        )
        return super().__call__(value, *args, **kwargs)


class DecisionType(str, Enum):
    """Enum canonico de acciones/decisiones del sistema."""

    BUY = "BUY"
    BUY_REBALANCE = "BUY_REBALANCE"
    SELL = "SELL"  # coarse legacy: no distingue parcial vs total
    SELL_PARTIAL = "SELL_PARTIAL"
    SELL_FULL = "SELL_FULL"
    HOLD = "HOLD"
    WATCH = "WATCH"
    SWAP_CANDIDATE = "SWAP_CANDIDATE"
    NO_ACTION = "NO_ACTION"
    BLOCKED = "BLOCKED"

    def is_buy(self) -> bool:
        return self in (DecisionType.BUY, DecisionType.BUY_REBALANCE)

    def is_sell(self) -> bool:
        return self in (
            DecisionType.SELL,
            DecisionType.SELL_PARTIAL,
            DecisionType.SELL_FULL,
        )

    def to_db_decision(self) -> str:
        if self.is_buy():
            return "BUY"
        if self.is_sell():
            return "SELL"
        return "HOLD"

    def display_label(self) -> str:
        return {
            DecisionType.BUY: "COMPRA (señal real)",
            DecisionType.BUY_REBALANCE: "AUMENTO POR REBALANCEO",
            DecisionType.SELL: "VENTA",
            DecisionType.SELL_PARTIAL: "VENTA PARCIAL",
            DecisionType.SELL_FULL: "VENTA TOTAL",
            DecisionType.HOLD: "MANTENER",
            DecisionType.WATCH: "VIGILAR",
            DecisionType.SWAP_CANDIDATE: "SWAP CANDIDATO",
            DecisionType.NO_ACTION: "SIN ACCION",
            DecisionType.BLOCKED: "BLOQUEADO",
        }[self]

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


DEFAULT_STOP_PCT = 0.08
CAUTIOUS_STOP_PCT = 0.05
TARGET_RR_DEFAULT = 2.0


@dataclass(frozen=True)
class RiskLevels:
    stop: float
    target: float
    rr: float
    stop_pct: float
    target_pct: float
    stop_source: str = "FIXED"


def compute_risk_levels(
    entry_price: float,
    signal_class,
    action,
    *,
    regime: Optional[str] = None,
    vix: Optional[float] = None,
    atr_pct: Optional[float] = None,
    rr_override: Optional[float] = None,
    vol_annual: Optional[float] = None,
) -> RiskLevels:
    """
    Calcula stop/target/RR en un único punto del sistema.

    `stop` y `target` son precios absolutos.
    `stop_pct` y `target_pct` se expresan en espacio direccional:
    - stop_pct siempre es negativo.
    - target_pct siempre es positivo.
    """
    _ = signal_class  # reservado para futuras políticas por clase de señal

    entry = float(entry_price)
    if entry <= 0:
        raise ValueError("entry_price debe ser positivo")

    action_name = str(getattr(action, "value", action)).upper()
    regime_name = str(regime or "NORMAL").upper()
    is_defensive = regime_name in {"RISK_OFF", "DEFENSIVE", "BLOCKED", "CAUTIOUS"}
    vix_value = float(vix) if vix is not None else 0.0
    vol_value = float(vol_annual) if vol_annual is not None else 0.0

    if atr_pct is not None and float(atr_pct) > 0:
        distance = min(max(float(atr_pct) * 1.5, 0.04), 0.18)
        stop_source = "ATR"
    elif vix_value > 30:
        distance = CAUTIOUS_STOP_PCT
        stop_source = "VIX_DYNAMIC"
    elif vix_value > 25:
        distance = DEFAULT_STOP_PCT * 1.25
        stop_source = "VIX_DYNAMIC"
    elif is_defensive:
        distance = CAUTIOUS_STOP_PCT
        stop_source = "FIXED"
    elif vol_value > 0.60:
        distance = CAUTIOUS_STOP_PCT
        stop_source = "VOLATILITY"
    elif vol_value > 0.40:
        distance = 0.07
        stop_source = "VOLATILITY"
    else:
        distance = DEFAULT_STOP_PCT
        stop_source = "FIXED"

    rr = float(rr_override) if rr_override and float(rr_override) > 0 else TARGET_RR_DEFAULT
    directional_stop_pct = -distance
    directional_target_pct = distance * rr

    if action_name.startswith("SELL"):
        stop = entry * (1 + distance)
        target = entry * (1 - directional_target_pct)
    else:
        stop = entry * (1 - distance)
        target = entry * (1 + directional_target_pct)

    return RiskLevels(
        stop=round(stop, 4),
        target=round(target, 4),
        rr=round(rr, 2),
        stop_pct=round(directional_stop_pct, 4),
        target_pct=round(directional_target_pct, 4),
        stop_source=stop_source,
    )

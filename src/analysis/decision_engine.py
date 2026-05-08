"""
src/analysis/decision_engine.py
────────────────────────────────

Motor conservador de decisión.

Objetivo:
    Convertir score + contexto en una intención operativa clara,
    sin saltear al Execution Planner.

Antes:
    score + conviction -> BUY/SELL/HOLD definitivo

Ahora:
    score + contexto -> SIGNAL + INTENT + BLOCKERS

Principios:
    - El optimizer nunca genera órdenes directas.
    - El radar nunca ejecuta compra directa sin validación.
    - Un BUY ejecutable requiere score, convicción, delta útil, R/R válido y gate habilitado.
    - En IC_CAUTION / CAUTELA_ALTA se endurecen umbrales.
    - Si falta contexto operativo, se devuelve WATCH/HOLD, no BUY.
    - SELL significa reducir posición, no short.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Umbrales base
# ──────────────────────────────────────────────────────────────────────────────

SCORE_BUY_MIN = 0.08
SCORE_BUY_STRONG = 0.15
SCORE_BUY_EXCEPTIONAL = 0.18

SCORE_SELL_MIN = -0.08
SCORE_SELL_STRONG = -0.15

CONVICTION_MIN = 0.45
CONVICTION_STRONG = 0.60

MIN_DELTA_PCT = 0.015       # 1.5% del portfolio para operar
MIN_TRADE_SIZE_PCT = 0.015  # 1.5% del portfolio
MAX_POSITION_PCT = 0.25     # hard cap por activo
MAX_NEW_POSITION_PCT = 0.06 # posición inicial máxima si no existe

RR_MIN = 1.20
RR_GOOD = 1.50

STOP_NORMAL = -0.08
STOP_CAUTIOUS = -0.05
TARGET_RR = 2.0

HORIZON_SHORT = 5
HORIZON_MED = 10
HORIZON_LONG = 20


class SignalClass(str, Enum):
    NEG_STRONG = "NEG_FUERTE"
    NEG_OPERABLE = "NEG_OPERABLE"
    NEUTRAL = "NEUTRAL_RUIDO"
    POS_WEAK = "POS_DEBIL"
    POS_OPERABLE = "POS_OPERABLE"
    POS_STRONG = "POS_FUERTE"


class FinalAction(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"
    WATCH = "WATCH"
    SWAP_CANDIDATE = "SWAP_CANDIDATE"
    NO_ACTION = "NO_ACTION"


@dataclass
class DecisionOutput:
    ticker: str
    final_action: str
    executable: bool

    score: float
    conviction: float
    signal_class: str

    current_weight: Optional[float] = None
    target_weight: Optional[float] = None
    delta_weight: Optional[float] = None

    size_pct: float = 0.0
    entry_price: Optional[float] = None
    stop_loss_pct: Optional[float] = None
    target_pct: Optional[float] = None
    rr_ratio: Optional[float] = None
    horizon_days: int = HORIZON_MED

    regime: str = "NEUTRAL"
    ic_regime: str = "NORMAL"
    vix: Optional[float] = None

    reason: str = ""
    blockers: list[str] = field(default_factory=list)
    source: str = "decision_engine"
    decided_at: datetime = field(default_factory=datetime.utcnow)

    @property
    def direction(self) -> str:
        """
        Compatibilidad con código viejo.
        Devuelve BUY/SELL/HOLD aunque internamente exista WATCH/SWAP.
        """
        if self.final_action == FinalAction.BUY.value:
            return "BUY"
        if self.final_action == FinalAction.SELL.value:
            return "SELL"
        return "HOLD"

    def is_actionable(self) -> bool:
        return bool(self.executable and self.final_action in {"BUY", "SELL"})

    def stop_price(self) -> Optional[float]:
        if self.entry_price is None or self.stop_loss_pct is None:
            return None
        return round(self.entry_price * (1 + self.stop_loss_pct), 4)

    def target_price(self) -> Optional[float]:
        if self.entry_price is None or self.target_pct is None:
            return None
        return round(self.entry_price * (1 + self.target_pct), 4)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "final_action": self.final_action,
            "direction": self.direction,
            "executable": self.executable,
            "score": self.score,
            "conviction": self.conviction,
            "signal_class": self.signal_class,
            "current_weight": self.current_weight,
            "target_weight": self.target_weight,
            "delta_weight": self.delta_weight,
            "size_pct": self.size_pct,
            "entry_price": self.entry_price,
            "stop_loss_pct": self.stop_loss_pct,
            "target_pct": self.target_pct,
            "rr_ratio": self.rr_ratio,
            "horizon_days": self.horizon_days,
            "regime": self.regime,
            "ic_regime": self.ic_regime,
            "vix": self.vix,
            "reason": self.reason,
            "blockers": self.blockers,
            "source": self.source,
            "decided_at": self.decided_at.isoformat(),
        }

    def format_telegram(self) -> str:
        icon = {
            "BUY": "🟢",
            "SELL": "🔴",
            "HOLD": "🟡",
            "WATCH": "🔵",
            "SWAP_CANDIDATE": "🔄",
            "NO_ACTION": "⚪",
        }.get(self.final_action, "⚪")

        lines = [
            f"{icon} <b>{self.ticker} → {self.final_action}</b>",
            f"   Score: <code>{self.score:+.3f}</code> | Conv: <b>{self.conviction:.0%}</b>",
            f"   Señal: <b>{self.signal_class}</b>",
            f"   Ejecutable: <b>{'sí' if self.executable else 'no'}</b>",
        ]

        if self.current_weight is not None and self.target_weight is not None:
            lines.append(
                f"   Peso: {self.current_weight:.1%} → {self.target_weight:.1%}"
            )

        if self.size_pct:
            lines.append(f"   Size sugerido: <b>{self.size_pct:.1%}</b> del portfolio")

        if self.rr_ratio is not None:
            lines.append(f"   R/R: <b>{self.rr_ratio:.1f}x</b>")

        if self.stop_loss_pct is not None and self.target_pct is not None:
            lines.append(
                f"   Stop: <code>{self.stop_loss_pct:+.1%}</code> | "
                f"Target: <code>{self.target_pct:+.1%}</code>"
            )

        if self.reason:
            lines.append(f"   Motivo: {self.reason}")

        if self.blockers:
            lines.append("   Bloqueos:")
            for b in self.blockers:
                lines.append(f"      • {b}")

        return "\n".join(lines)


# ──────────────────────────────────────────────────────────────────────────────
# API principal
# ──────────────────────────────────────────────────────────────────────────────

def classify_signal(score: float, conviction: float = 0.0) -> SignalClass:
    score = float(score or 0.0)
    conviction = _normalize_conviction(conviction)

    if score >= SCORE_BUY_STRONG and conviction >= CONVICTION_MIN:
        return SignalClass.POS_STRONG

    if score >= SCORE_BUY_MIN and conviction >= CONVICTION_MIN:
        return SignalClass.POS_OPERABLE

    if score > 0.03:
        return SignalClass.POS_WEAK

    if score <= SCORE_SELL_STRONG and conviction >= CONVICTION_MIN:
        return SignalClass.NEG_STRONG

    if score <= SCORE_SELL_MIN and conviction >= CONVICTION_MIN:
        return SignalClass.NEG_OPERABLE

    return SignalClass.NEUTRAL


def make_decision(
    ticker: str,
    score: float,
    conviction: float,
    regime: str,
    vix: Optional[float] = None,
    entry_price: Optional[float] = None,
    layers: Optional[dict] = None,

    # Contexto operativo nuevo
    current_weight: Optional[float] = None,
    target_weight: Optional[float] = None,
    rr_ratio: Optional[float] = None,
    ic_regime: str = "NORMAL",
    has_position: Optional[bool] = None,
    source: str = "portfolio",
    allow_new_buy: bool = True,
) -> DecisionOutput:
    """
    Genera una decisión conservadora.

    Importante:
        Si no recibe current_weight/target_weight, NO emite BUY ejecutable.
        Solo clasifica señal y devuelve WATCH/HOLD.

    Esto evita que un score aislado se transforme en orden directa.
    """

    ticker = str(ticker or "").upper().strip()
    score = float(score or 0.0)
    conviction = _normalize_conviction(conviction)
    regime = _normalize_regime(regime)
    ic_regime = _normalize_ic_regime(ic_regime)
    vix = float(vix) if vix is not None else None

    signal = classify_signal(score, conviction)
    is_defensive = regime in {"RISK_OFF", "DEFENSIVE", "BLOCKED", "CAUTIOUS"}
    ic_caution = ic_regime in {"CAUTION", "CAUTELA", "CAUTELA_ALTA", "HIGH_CAUTION"}

    blockers: list[str] = []

    if current_weight is not None:
        current_weight = float(current_weight)

    if target_weight is not None:
        target_weight = float(target_weight)

    delta_weight: Optional[float] = None
    if current_weight is not None and target_weight is not None:
        delta_weight = target_weight - current_weight

    if has_position is None:
        has_position = bool(current_weight and current_weight > 0)

    # Stops y targets estándar
    stop_loss_pct, target_pct, default_rr = _risk_params(
        regime=regime,
        vix=vix,
        direction="BUY" if score >= 0 else "SELL",
    )

    effective_rr = float(rr_ratio) if rr_ratio is not None else default_rr

    # ── Sin contexto operativo: no ejecutar ───────────────────────────────────
    if delta_weight is None:
        return DecisionOutput(
            ticker=ticker,
            final_action=FinalAction.WATCH.value if signal != SignalClass.NEUTRAL else FinalAction.HOLD.value,
            executable=False,
            score=score,
            conviction=conviction,
            signal_class=signal.value,
            current_weight=current_weight,
            target_weight=target_weight,
            delta_weight=delta_weight,
            entry_price=entry_price,
            stop_loss_pct=stop_loss_pct,
            target_pct=target_pct,
            rr_ratio=effective_rr,
            horizon_days=_horizon(score, conviction),
            regime=regime,
            ic_regime=ic_regime,
            vix=vix,
            reason="Señal clasificada, pero falta contexto de peso/delta para ejecutar.",
            blockers=["Falta current_weight/target_weight"],
            source=source,
        )

    # ── Gates globales ────────────────────────────────────────────────────────
    if is_defensive and score > 0:
        blockers.append(f"Régimen defensivo {regime}: no habilita compras débiles")

    if ic_caution:
        if score > 0 and score < SCORE_BUY_EXCEPTIONAL:
            blockers.append(
                f"IC en cautela ({ic_regime}): BUY requiere score >= {SCORE_BUY_EXCEPTIONAL:+.2f}"
            )

    if effective_rr < RR_MIN:
        blockers.append(f"R/R insuficiente ({effective_rr:.1f}x < {RR_MIN:.1f}x)")

    if abs(delta_weight) < MIN_DELTA_PCT:
        blockers.append(
            f"Delta insuficiente ({delta_weight:+.1%} < {MIN_DELTA_PCT:.1%})"
        )

    # ── BUY intent ────────────────────────────────────────────────────────────
    if delta_weight > 0:
        if signal not in {SignalClass.POS_OPERABLE, SignalClass.POS_STRONG}:
            blockers.append(f"BUY requiere señal positiva operable; actual {signal.value}")

        if not allow_new_buy and not has_position:
            blockers.append("Compra nueva no habilitada por configuración")

        if target_weight > MAX_POSITION_PCT:
            blockers.append(
                f"Target excede cap por activo ({target_weight:.1%} > {MAX_POSITION_PCT:.1%})"
            )

        if not has_position and target_weight > MAX_NEW_POSITION_PCT:
            blockers.append(
                f"Posición nueva excede sizing inicial ({target_weight:.1%} > {MAX_NEW_POSITION_PCT:.1%})"
            )

        if blockers:
            return DecisionOutput(
                ticker=ticker,
                final_action=FinalAction.WATCH.value,
                executable=False,
                score=score,
                conviction=conviction,
                signal_class=signal.value,
                current_weight=current_weight,
                target_weight=target_weight,
                delta_weight=delta_weight,
                size_pct=max(0.0, delta_weight),
                entry_price=entry_price,
                stop_loss_pct=stop_loss_pct,
                target_pct=target_pct,
                rr_ratio=effective_rr,
                horizon_days=_horizon(score, conviction),
                regime=regime,
                ic_regime=ic_regime,
                vix=vix,
                reason="Compra bloqueada por guardias.",
                blockers=blockers,
                source=source,
            )

        return DecisionOutput(
            ticker=ticker,
            final_action=FinalAction.BUY.value,
            executable=True,
            score=score,
            conviction=conviction,
            signal_class=signal.value,
            current_weight=current_weight,
            target_weight=target_weight,
            delta_weight=delta_weight,
            size_pct=max(MIN_TRADE_SIZE_PCT, min(delta_weight, MAX_NEW_POSITION_PCT if not has_position else MAX_POSITION_PCT)),
            entry_price=entry_price,
            stop_loss_pct=stop_loss_pct,
            target_pct=target_pct,
            rr_ratio=effective_rr,
            horizon_days=_horizon(score, conviction),
            regime=regime,
            ic_regime=ic_regime,
            vix=vix,
            reason="BUY habilitado: señal, delta y guardias operativas OK.",
            blockers=[],
            source=source,
        )

    # ── SELL / reducción intent ───────────────────────────────────────────────
    if delta_weight < 0:
        reduction_size = abs(delta_weight)

        sell_allowed = signal in {SignalClass.NEG_OPERABLE, SignalClass.NEG_STRONG}

        # Permitir reducción por concentración, aunque la señal no sea negativa,
        # pero solo si el peso es alto.
        concentration_trim = (
            current_weight is not None
            and current_weight > MAX_POSITION_PCT
            and reduction_size >= MIN_DELTA_PCT
        )

        if not sell_allowed and not concentration_trim:
            blockers.append(
                f"SELL requiere señal negativa o concentración excesiva; actual {signal.value}"
            )

        if blockers:
            return DecisionOutput(
                ticker=ticker,
                final_action=FinalAction.HOLD.value,
                executable=False,
                score=score,
                conviction=conviction,
                signal_class=signal.value,
                current_weight=current_weight,
                target_weight=target_weight,
                delta_weight=delta_weight,
                size_pct=0.0,
                entry_price=entry_price,
                stop_loss_pct=None,
                target_pct=None,
                rr_ratio=effective_rr,
                horizon_days=_horizon(score, conviction),
                regime=regime,
                ic_regime=ic_regime,
                vix=vix,
                reason="Venta/reducción bloqueada por guardias.",
                blockers=blockers,
                source=source,
            )

        return DecisionOutput(
            ticker=ticker,
            final_action=FinalAction.SELL.value,
            executable=True,
            score=score,
            conviction=conviction,
            signal_class=signal.value,
            current_weight=current_weight,
            target_weight=target_weight,
            delta_weight=delta_weight,
            size_pct=reduction_size,
            entry_price=entry_price,
            stop_loss_pct=None,
            target_pct=None,
            rr_ratio=effective_rr,
            horizon_days=_horizon(score, conviction),
            regime=regime,
            ic_regime=ic_regime,
            vix=vix,
            reason="Reducción habilitada por señal negativa o concentración.",
            blockers=[],
            source=source,
        )

    # delta 0
    return DecisionOutput(
        ticker=ticker,
        final_action=FinalAction.HOLD.value,
        executable=False,
        score=score,
        conviction=conviction,
        signal_class=signal.value,
        current_weight=current_weight,
        target_weight=target_weight,
        delta_weight=delta_weight,
        size_pct=0.0,
        entry_price=entry_price,
        stop_loss_pct=None,
        target_pct=None,
        rr_ratio=effective_rr,
        horizon_days=_horizon(score, conviction),
        regime=regime,
        ic_regime=ic_regime,
        vix=vix,
        reason="Sin delta operativo.",
        blockers=[],
        source=source,
    )


def make_decisions_from_results(
    results: list,
    macro_snap,
    regime: str,
    *,
    weights_current: Optional[dict[str, float]] = None,
    weights_target: Optional[dict[str, float]] = None,
    rr_by_ticker: Optional[dict[str, float]] = None,
    ic_regime: str = "NORMAL",
    source: str = "portfolio",
) -> list[DecisionOutput]:
    """
    Wrapper para run_analysis.

    Acepta pesos actuales/objetivo si existen.
    Si no existen, clasifica señal pero NO genera orden ejecutable.
    """
    vix = getattr(macro_snap, "vix", None)
    outputs: list[DecisionOutput] = []

    weights_current = weights_current or {}
    weights_target = weights_target or {}
    rr_by_ticker = rr_by_ticker or {}

    for r in results:
        ticker = str(getattr(r, "ticker", "")).upper()
        score = float(getattr(r, "final_score", getattr(r, "score", 0.0)) or 0.0)
        conviction = _normalize_conviction(
            getattr(r, "conviction", getattr(r, "confidence", 0.0))
        )
        price = _extract_price(r)
        layers = _extract_layers(r)

        dec = make_decision(
            ticker=ticker,
            score=score,
            conviction=conviction,
            regime=regime,
            vix=vix,
            entry_price=price,
            layers=layers,
            current_weight=weights_current.get(ticker),
            target_weight=weights_target.get(ticker),
            rr_ratio=rr_by_ticker.get(ticker),
            ic_regime=ic_regime,
            source=source,
        )
        outputs.append(dec)

    return outputs


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _risk_params(
    regime: str,
    vix: Optional[float],
    direction: str,
) -> tuple[float, float, float]:
    regime = _normalize_regime(regime)
    is_defensive = regime in {"RISK_OFF", "DEFENSIVE", "BLOCKED", "CAUTIOUS"}

    if vix is not None and vix > 25:
        stop = STOP_NORMAL * 1.25
    elif is_defensive:
        stop = STOP_CAUTIOUS
    else:
        stop = STOP_NORMAL

    target = abs(stop) * TARGET_RR

    if direction == "SELL":
        # Para CEDEARs, SELL significa reducir, no short.
        # No usamos stop/target como short en este módulo.
        return stop, target, TARGET_RR

    return stop, target, TARGET_RR


def _horizon(score: float, conviction: float) -> int:
    score = abs(float(score or 0.0))
    conviction = _normalize_conviction(conviction)

    if conviction >= 0.70 and score >= SCORE_BUY_STRONG:
        return HORIZON_SHORT
    if conviction >= 0.45:
        return HORIZON_MED
    return HORIZON_LONG


def _normalize_regime(regime: Any) -> str:
    if isinstance(regime, dict):
        market = str(regime.get("market", "neutral")).lower()
        arg = str(regime.get("argentina", "estable")).lower()

        if market == "risk_off":
            return "RISK_OFF"
        if market in {"cautious", "caution"}:
            return "CAUTIOUS"
        if arg in {"crítico", "critico", "risk_off"}:
            return "DEFENSIVE"
        return "NEUTRAL"

    value = str(regime or "NEUTRAL").upper()
    aliases = {
        "NORMAL": "NEUTRAL",
        "RISKON": "RISK_ON",
        "RISK_ON": "RISK_ON",
        "RISKOFF": "RISK_OFF",
        "RISK_OFF": "RISK_OFF",
        "CAUTION": "CAUTIOUS",
        "CAUTELA": "CAUTIOUS",
        "CAUTELA_ALTA": "CAUTIOUS",
    }
    return aliases.get(value, value)


def _normalize_ic_regime(value: Any) -> str:
    v = str(value or "NORMAL").upper()
    aliases = {
        "NORMAL": "NORMAL",
        "OK": "NORMAL",
        "CAUTION": "CAUTION",
        "CAUTELA": "CAUTION",
        "CAUTELA_ALTA": "HIGH_CAUTION",
        "HIGH_CAUTION": "HIGH_CAUTION",
        "BLOCKED": "BLOCKED",
    }
    return aliases.get(v, v)


def _normalize_conviction(x: Any) -> float:
    try:
        if x is None:
            return 0.0
        x = float(x)
        if x > 1.0:
            x = x / 100.0
        return max(0.0, min(1.0, x))
    except Exception:
        return 0.0


def _extract_price(result: Any) -> Optional[float]:
    for key in ("price", "price_at_decision", "current_price", "last_price"):
        val = getattr(result, key, None)
        if val is not None:
            try:
                return float(val)
            except Exception:
                pass
    return None


def _extract_layers(result: Any) -> dict:
    out: dict[str, float] = {}

    layers = getattr(result, "layers", None)
    if isinstance(layers, dict):
        for k, v in layers.items():
            try:
                out[str(k)] = float(v)
            except Exception:
                pass
        return out

    for layer in layers or []:
        name = getattr(layer, "name", None)
        if not name:
            continue
        try:
            out[str(name)] = float(getattr(layer, "weighted", 0.0))
        except Exception:
            pass

    return out
"""
src/analysis/decision_engine.py
────────────────────────────────
Convierte un SynthesisResult en una DECISIÓN FORZADA con parámetros concretos.

Antes:  "NFLX score 0.2 | convicción MEDIA"
Ahora:
    DECISIÓN:   BUY NFLX
    SIZE:       12% del portfolio
    ENTRY:      ahora
    STOP:       -8%  (stop loss)
    TARGET:     +16% (take profit)
    HORIZONTE:  10 días
    R/R:        2.0x

Reglas de diseño:
  - Si score es ambiguo → HOLD (no se guarda en DB)
  - El stop/target se ajustan por régimen macro y VIX
  - El size se escala por convicción (nunca >20% en una sola posición)
  - La decisión es DEFINITIVA — el LLM no la modifica
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# ── Umbrales ──────────────────────────────────────────────────────────────────
SCORE_BUY_STRONG   =  0.20   # compra fuerte
SCORE_BUY_WEAK     =  0.08   # compra débil
SCORE_SELL_STRONG  = -0.20   # venta fuerte
SCORE_SELL_WEAK    = -0.08   # reducción
SCORE_HOLD_BAND    =  0.08   # zona muerta: abs(score) < esto → HOLD

# ── Sizing (% del portfolio) ──────────────────────────────────────────────────
SIZE_MAX       = 0.20   # máximo por posición
SIZE_MIN       = 0.03   # mínimo para que valga la pena
SIZE_BASE      = 0.20   # base sobre la que escala la convicción

# ── Risk defaults ─────────────────────────────────────────────────────────────
STOP_NORMAL    = -0.08   # -8%
STOP_CAUTIOUS  = -0.05   # -5% en régimen defensivo
TARGET_RR      =  2.0    # target = stop * RR (R/R 2:1 por defecto)

# ── Horizontes (días) ─────────────────────────────────────────────────────────
HORIZON_SHORT  =  5
HORIZON_MED    = 10
HORIZON_LONG   = 20


@dataclass
class DecisionOutput:
    """Decisión forzada con todos los parámetros operativos."""
    ticker:         str
    direction:      str          # 'BUY' | 'SELL' | 'HOLD'
    score:          float
    conviction:     float        # 0–1
    size_pct:       float        # fracción del portfolio (0.12 = 12%)
    entry_price:    Optional[float]
    stop_loss_pct:  float        # negativo  (ej: -0.08)
    target_pct:     float        # positivo  (ej: +0.16)
    horizon_days:   int
    rr_ratio:       float
    regime:         str
    vix:            Optional[float]
    decided_at:     datetime = field(default_factory=datetime.utcnow)

    # ── Helpers de display ────────────────────────────────────────────────────

    def is_actionable(self) -> bool:
        return self.direction != "HOLD"

    def stop_price(self) -> Optional[float]:
        if self.entry_price:
            return round(self.entry_price * (1 + self.stop_loss_pct), 4)
        return None

    def target_price(self) -> Optional[float]:
        if self.entry_price:
            return round(self.entry_price * (1 + self.target_pct), 4)
        return None

    def format_telegram(self) -> str:
        """Bloque de decisión listo para pegar en Telegram (HTML)."""
        if not self.is_actionable():
            return f"⚪ <b>HOLD {self.ticker}</b> — sin ventaja operativa clara (score {self.score:+.3f})"

        icon = "🟢" if self.direction == "BUY" else "🔴"
        lines = [
            f"{icon} <b>DECISIÓN: {self.direction} {self.ticker}</b>",
            f"   SIZE:       <b>{self.size_pct:.0%}</b> del portfolio",
            f"   ENTRY:      ahora (precio referencia: {self.entry_price or '?'})",
            f"   STOP:       <code>{self.stop_loss_pct:+.1%}</code>",
            f"   TARGET:     <code>{self.target_pct:+.1%}</code>",
            f"   HORIZONTE:  {self.horizon_days} días",
            f"   R/R:        <b>{self.rr_ratio:.1f}x</b>",
            f"   Score:      <code>{self.score:+.3f}</code> | Conv: <b>{self.conviction:.0%}</b>",
            f"   Régimen:    {self.regime}",
        ]
        if self.stop_price():
            lines.append(f"   Stop price: <code>${self.stop_price():,.2f}</code>  →  Target: <code>${self.target_price():,.2f}</code>")
        return "\n".join(lines)

    def to_dict(self) -> dict:
        return {
            "ticker":        self.ticker,
            "direction":     self.direction,
            "score":         self.score,
            "conviction":    self.conviction,
            "size_pct":      self.size_pct,
            "entry_price":   self.entry_price,
            "stop_loss_pct": self.stop_loss_pct,
            "target_pct":    self.target_pct,
            "horizon_days":  self.horizon_days,
            "rr_ratio":      self.rr_ratio,
            "regime":        self.regime,
            "vix":           self.vix,
            "decided_at":    self.decided_at.isoformat(),
        }


# ── Lógica principal ──────────────────────────────────────────────────────────

def make_decision(
    ticker:      str,
    score:       float,
    conviction:  float,
    regime:      str,
    vix:         Optional[float] = None,
    entry_price: Optional[float] = None,
    layers:      Optional[dict]  = None,
) -> DecisionOutput:
    """
    Convierte un score + contexto en una decisión forzada.

    Parámetros
    ----------
    ticker      : símbolo del activo
    score       : final_score del sistema (-1 a +1)
    conviction  : confidence normalizado (0 a 1)
    regime      : 'RISK_ON' | 'NEUTRAL' | 'RISK_OFF' | 'DEFENSIVE'
    vix         : VIX actual (ajusta stops)
    entry_price : precio actual (para calcular niveles absolutos)
    layers      : dict de layer scores para logging

    Retorna
    -------
    DecisionOutput con direction='HOLD' si no hay ventaja clara.
    """
    score      = float(score or 0.0)
    conviction = float(conviction or 0.0)
    vix        = float(vix) if vix else None
    regime     = _normalize_regime(regime)

    # ── 1. Dirección ──────────────────────────────────────────────────────────
    is_defensive = regime in ("RISK_OFF", "DEFENSIVE", "BLOCKED", "CAUTIOUS")

    if score >= SCORE_BUY_STRONG and conviction >= 0.45:
        direction = "BUY"
    elif score >= SCORE_BUY_WEAK and conviction >= 0.30 and not is_defensive:
        direction = "BUY"
    elif score <= SCORE_SELL_STRONG:
        direction = "SELL"
    elif score <= SCORE_SELL_WEAK and conviction >= 0.40:
        direction = "SELL"
    else:
        # Zona muerta — no hay ventaja suficiente para actuar
        return DecisionOutput(
            ticker=ticker, direction="HOLD", score=score, conviction=conviction,
            size_pct=0.0, entry_price=entry_price,
            stop_loss_pct=STOP_NORMAL, target_pct=abs(STOP_NORMAL) * TARGET_RR,
            horizon_days=HORIZON_MED, rr_ratio=TARGET_RR,
            regime=regime, vix=vix,
        )

    # ── 2. Sizing — escala por convicción ────────────────────────────────────
    # size = conviction * SIZE_BASE, ajustado por intensidad del score
    score_intensity = min(abs(score) / SCORE_BUY_STRONG, 1.0)  # 0–1
    raw_size = conviction * SIZE_BASE * (0.7 + 0.3 * score_intensity)
    size_pct = max(SIZE_MIN, min(SIZE_MAX, raw_size))

    # En régimen defensivo, reducir tamaño a la mitad
    if is_defensive and direction == "BUY":
        size_pct *= 0.5

    # ── 3. Stop loss — ajustado por VIX y régimen ────────────────────────────
    if vix and vix > 25:
        # Mercado volátil: stops más amplios para no ser sacado
        stop_loss = STOP_NORMAL * 1.25       # -10%
    elif is_defensive:
        stop_loss = STOP_CAUTIOUS            # -5%
    else:
        stop_loss = STOP_NORMAL              # -8%

    # En SELL, el stop es al alza
    if direction == "SELL":
        stop_loss = abs(stop_loss)           # stop positivo para posición corta

    # ── 4. Target — R/R 2:1 ──────────────────────────────────────────────────
    target_pct = abs(stop_loss) * TARGET_RR
    if direction == "SELL":
        target_pct = -target_pct            # target negativo para short

    rr_ratio = abs(target_pct) / abs(stop_loss) if stop_loss != 0 else TARGET_RR

    # ── 5. Horizonte ──────────────────────────────────────────────────────────
    if conviction >= 0.70 and abs(score) >= SCORE_BUY_STRONG:
        horizon_days = HORIZON_SHORT
    elif conviction >= 0.45:
        horizon_days = HORIZON_MED
    else:
        horizon_days = HORIZON_LONG

    logger.info(
        f"[decision_engine] {direction} {ticker} | score={score:+.3f} conv={conviction:.0%} "
        f"size={size_pct:.0%} stop={stop_loss:+.1%} target={target_pct:+.1%} "
        f"horizon={horizon_days}d regime={regime}"
    )

    return DecisionOutput(
        ticker=ticker, direction=direction, score=score, conviction=conviction,
        size_pct=size_pct, entry_price=entry_price,
        stop_loss_pct=stop_loss if direction == "BUY" else -abs(stop_loss),
        target_pct=target_pct,
        horizon_days=horizon_days, rr_ratio=rr_ratio,
        regime=regime, vix=vix,
    )


def make_decisions_from_results(results: list, macro_snap, regime: str) -> list[DecisionOutput]:
    """
    Wrapper conveniente: toma la lista de SynthesisResult de run_analysis
    y devuelve una lista de DecisionOutput (filtrando HOLDs si se desea).

    Uso en run_analysis.py:
        decisions = make_decisions_from_results(results, macro_snap, macro_regime)
    """
    vix = getattr(macro_snap, "vix", None)
    regime = _normalize_regime(regime)
    outputs = []
    for r in results:
        ticker     = str(getattr(r, "ticker", "")).upper()
        score      = float(getattr(r, "final_score", getattr(r, "score", 0.0)) or 0.0)
        conviction = _normalize_conviction(getattr(r, "conviction",
                                           getattr(r, "confidence", 0.0)))
        price      = _extract_price(r)
        layers     = _extract_layers(r)

        dec = make_decision(
            ticker=ticker, score=score, conviction=conviction,
            regime=regime, vix=vix, entry_price=price, layers=layers,
        )
        outputs.append(dec)
    return outputs


# ── Helpers privados ──────────────────────────────────────────────────────────

def _normalize_regime(regime) -> str:
    """
    Convierte régimen a string normalizado.
    Acepta str O dict (el formato que retorna get_macro_regime()).

    dict ejemplo: {'market': 'risk_off', 'oil': 'bull', 'rates': 'neutral',
                   'dollar': 'neutral', 'argentina': 'estable'}
    → prioriza 'market', que es el campo más relevante para decisiones.
    """
    if isinstance(regime, dict):
        market = str(regime.get("market", "neutral")).lower()
        arg    = str(regime.get("argentina", "estable")).lower()
        # Mapear a los valores que usa is_defensive
        if market == "risk_off":
            return "RISK_OFF"
        if arg == "crítico":
            return "DEFENSIVE"
        return "NEUTRAL"
    return (str(regime) if regime else "NEUTRAL").upper()

def _normalize_conviction(x) -> float:
    try:
        if x is None: return 0.0
        x = float(x)
        return max(0.0, min(1.0, x / 100.0 if x > 1.0 else x))
    except Exception:
        return 0.0


def _extract_price(result) -> Optional[float]:
    for key in ("price", "price_at_decision", "current_price", "last_price"):
        val = getattr(result, key, None)
        if val is not None:
            try: return float(val)
            except Exception: pass
    return None


def _extract_layers(result) -> dict:
    out = {}
    for layer in getattr(result, "layers", []) or []:
        name = getattr(layer, "name", None)
        if name:
            out[name] = float(getattr(layer, "weighted", 0.0))
    return out
"""
src/analysis/trade_lifecycle.py
─────────────────────────────────
Lifecycle completo de un trade: emisión → ejecución → cierre.

Resuelve la ambigüedad entre:
  - BUY:            compra por señal real del activo (score + conviction fuertes)
  - BUY_REBALANCE:  aumento por construcción de cartera / optimizer
                    (la señal del activo no alcanza el umbral de BUY)
  - SELL_PARTIAL:   recorte parcial por rebalanceo, concentración o riesgo
  - SELL_FULL:      salida total por stop, target a 0%, invalidación, horizonte
  - HOLD:           sin acción operativa

Cada compra deja persistido:
  entry_price, stop_loss_pct, stop_loss_price, target_pct, target_price,
  horizon_days, exit_scope, exit_reason_rule, stop_policy, signal_strength.

La salida por stop genera automáticamente un SELL_FULL con trazabilidad completa.

DB:
  Se extiende decision_log con columnas nuevas via _MIGRATION_SQL.
  No se crea tabla nueva — la migración es additive (IF NOT EXISTS).
  Los campos nuevos son NULL para decisiones antiguas, no rompe compatibilidad.

Integración:
  - run_analysis.py llama classify_decision_type() después de build_signals_from_synthesis()
  - execution_planner.py usa DecisionType en DecisionIntent
  - update_outcomes.py llama check_stop_activations() al final de cada run
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)

# ── Umbrales para BUY vs BUY_REBALANCE ────────────────────────────────────────
# Si score Y conviction superan estos umbrales → BUY (señal real)
# Si no → BUY_REBALANCE (sube por el optimizer, no por mérito propio)
BUY_SCORE_MIN      = 0.08   # score mínimo para BUY real
BUY_CONVICTION_MIN = 0.40   # conviction mínima para BUY real
BUY_RR_MIN         = 1.0    # R/R mínimo para BUY real (si se conoce)

# Stop defaults
STOP_DEFAULT_PCT   = 0.08   # -8% default
STOP_CAUTIOUS_PCT  = 0.05   # -5% en régimen defensivo
TARGET_RR_DEFAULT  = 2.0    # target = stop × RR

# Horizonte default
HORIZON_DEFAULT    = 10     # días


# ══════════════════════════════════════════════════════════════════════════════
# ENUMS
# ══════════════════════════════════════════════════════════════════════════════

class DecisionType(str, Enum):
    """
    Tipo semántico de la decisión.
    Reemplaza el campo `decision` libre (BUY/SELL) con categorías más precisas.
    El campo `decision` original se mantiene en DB para compatibilidad hacia atrás.
    """
    BUY            = "BUY"            # compra por señal real del activo
    BUY_REBALANCE  = "BUY_REBALANCE"  # aumento por optimizer, señal floja
    SELL_PARTIAL   = "SELL_PARTIAL"   # recorte parcial
    SELL_FULL      = "SELL_FULL"      # salida total
    HOLD           = "HOLD"           # sin acción

    def to_db_decision(self) -> str:
        """Valor que va al campo legacy `decision` en decision_log."""
        if self in (DecisionType.BUY, DecisionType.BUY_REBALANCE):
            return "BUY"
        if self in (DecisionType.SELL_PARTIAL, DecisionType.SELL_FULL):
            return "SELL"
        return "HOLD"

    def display_label(self) -> str:
        """Etiqueta para el reporte de Telegram."""
        return {
            DecisionType.BUY:           "COMPRA (señal real)",
            DecisionType.BUY_REBALANCE: "AUMENTO POR REBALANCEO",
            DecisionType.SELL_PARTIAL:  "VENTA PARCIAL",
            DecisionType.SELL_FULL:     "VENTA TOTAL",
            DecisionType.HOLD:          "MANTENER",
        }.get(self, self.value)


class SignalStrength(str, Enum):
    """Intensidad de la señal del activo."""
    FUERTE   = "FUERTE"    # score >= 0.18 + conviction >= 0.55
    MODERADA = "MODERADA"  # score >= 0.08 + conviction >= 0.40
    DÉBIL    = "DÉBIL"     # score o conviction por debajo de moderada
    NEGATIVA = "NEGATIVA"  # score < 0


class StopPolicy(str, Enum):
    """Política de ejecución del stop."""
    HARD       = "HARD"        # stop duro — cerrar al precio o mejor disponible
    CLOSE_ONLY = "CLOSE_ONLY"  # solo cerrar al cierre del día (menos slippage)
    TRAILING   = "TRAILING"    # trailing stop (futuro)


class StopSource(str, Enum):
    """Origen del nivel de stop."""
    FIXED       = "FIXED"        # porcentaje fijo
    ATR         = "ATR"          # basado en ATR
    VIX_DYNAMIC = "VIX_DYNAMIC"  # ajustado por VIX


class ExitScope(str, Enum):
    """Alcance de la salida."""
    FULL    = "FULL"     # cerrar posición completa
    PARTIAL = "PARTIAL"  # cerrar solo una fracción


class ExitReasonRule(str, Enum):
    """Regla que dispara la salida."""
    STOP_LOSS      = "STOP_LOSS"
    TARGET_HIT     = "TARGET_HIT"
    HORIZON_END    = "HORIZON_END"
    INVALIDATION   = "INVALIDATION"   # señal invertida o score muy negativo
    REBALANCE      = "REBALANCE"      # el optimizer redujo el target
    MANUAL         = "MANUAL"


# ══════════════════════════════════════════════════════════════════════════════
# DATACLASSES
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class StopLevel:
    """Niveles de stop y target para una compra."""
    entry_price:      float
    stop_loss_pct:    float           # negativo, ej: -0.08
    stop_loss_price:  float           # precio absoluto
    target_pct:       float           # positivo, ej: +0.16
    target_price:     float           # precio absoluto
    rr_ratio:         float           # target_pct / abs(stop_loss_pct)
    stop_policy:      StopPolicy      = StopPolicy.HARD
    stop_source:      StopSource      = StopSource.FIXED
    trailing_active:  bool            = False
    exit_scope:       ExitScope       = ExitScope.FULL
    exit_reason_rule: ExitReasonRule  = ExitReasonRule.STOP_LOSS


@dataclass
class TradeDecision:
    """
    Decisión completa con semántica, niveles y trazabilidad.
    Alimenta tanto el render como la persistencia en DB.
    """
    ticker:           str
    decision_type:    DecisionType
    signal_strength:  SignalStrength
    score:            float
    conviction:       float
    size_pct:         float
    regime:           str
    vix:              Optional[float]
    # Niveles (None para HOLD, SELL_PARTIAL, SELL_FULL por señal)
    stop:             Optional[StopLevel]   = None
    horizon_days:     int                   = HORIZON_DEFAULT
    # Contexto
    generated_at:     datetime              = field(default_factory=lambda: datetime.now(timezone.utc))
    entry_price:      Optional[float]       = None
    source:           str                   = "signal"   # "signal" | "optimizer"
    notes:            str                   = ""
    # Cierre (se rellena después)
    was_stopped:      bool                  = False
    exit_reason:      Optional[str]         = None
    closed_at:        Optional[datetime]    = None
    close_price:      Optional[float]       = None

    def is_buy(self) -> bool:
        return self.decision_type in (DecisionType.BUY, DecisionType.BUY_REBALANCE)

    def is_sell(self) -> bool:
        return self.decision_type in (DecisionType.SELL_PARTIAL, DecisionType.SELL_FULL)

    def to_db_decision(self) -> str:
        """Campo legacy `decision` en decision_log."""
        return self.decision_type.to_db_decision()

    def render_header(self) -> str:
        """Línea de cabecera para el reporte de Telegram."""
        icon = {
            DecisionType.BUY:           "🟢",
            DecisionType.BUY_REBALANCE: "📈",
            DecisionType.SELL_PARTIAL:  "🔴",
            DecisionType.SELL_FULL:     "🔴🔴",
            DecisionType.HOLD:          "🟡",
        }.get(self.decision_type, "⚪")
        label = self.decision_type.display_label()
        return f"{icon} <b>{label}: {self.ticker}</b>"

    def render_detail(self) -> str:
        """Bloque de detalle para Telegram."""
        lines = [self.render_header()]

        if self.decision_type == DecisionType.BUY_REBALANCE:
            lines.append(
                f"   <i>Aumento por target del optimizer — "
                f"la señal del activo no alcanza umbral de compra táctica "
                f"(score {self.score:+.3f}, conviction {self.conviction:.0%}).</i>"
            )
        elif self.decision_type == DecisionType.BUY:
            lines.append(
                f"   Compra por señal: score <code>{self.score:+.3f}</code> | "
                f"conviction <b>{self.conviction:.0%}</b> | "
                f"señal <b>{self.signal_strength.value}</b>"
            )

        if self.stop and self.is_buy():
            s = self.stop
            lines += [
                f"   📍 Entry: <b>${self.entry_price:.2f}</b>" if self.entry_price else "",
                f"   🛑 Stop: <code>{s.stop_loss_pct:+.1%}</code> "
                f"→ ${s.stop_loss_price:.2f} ({s.stop_policy.value})",
                f"   🎯 Target: <code>{s.target_pct:+.1%}</code> "
                f"→ ${s.target_price:.2f} | R/R <b>{s.rr_ratio:.1f}x</b>",
                f"   📅 Horizonte: {self.horizon_days} días",
                f"   📤 Salida si stop: {s.exit_scope.value} ({s.exit_reason_rule.value})",
            ]
            lines = [l for l in lines if l]  # filtrar vacíos

        lines.append(
            f"   Size: <b>{self.size_pct:.0%}</b> del portfolio | "
            f"Régimen: {self.regime}"
        )
        return "\n".join(lines)

    def to_db_dict(self) -> dict:
        """Dict para INSERT en decision_log con campos extendidos."""
        d: dict = {
            "ticker":           self.ticker,
            "decision":         self.to_db_decision(),        # campo legacy
            "decision_type":    self.decision_type.value,     # campo nuevo
            "final_score":      self.score,
            "confidence":       self.conviction,
            "size_pct":         self.size_pct,
            "regime":           self.regime,
            "vix_at_decision":  self.vix,
            "price_at_decision": self.entry_price,
            "signal_strength":  self.signal_strength.value,
            "decided_at":       self.generated_at,
            "source":           self.source,
        }
        if self.stop:
            s = self.stop
            d.update({
                "stop_loss_pct":    s.stop_loss_pct,
                "stop_loss_price":  s.stop_loss_price,
                "target_pct":       s.target_pct,
                "target_price":     s.target_price,
                "horizon_days":     self.horizon_days,
                "exit_scope":       s.exit_scope.value,
                "exit_reason_rule": s.exit_reason_rule.value,
                "stop_policy":      s.stop_policy.value,
                "stop_source":      s.stop_source.value,
                "rr_ratio":         s.rr_ratio,
            })
        return d


# ══════════════════════════════════════════════════════════════════════════════
# LÓGICA DE CLASIFICACIÓN BUY vs BUY_REBALANCE
# ══════════════════════════════════════════════════════════════════════════════

def _signal_strength(score: float, conviction: float) -> SignalStrength:
    if score < 0:
        return SignalStrength.NEGATIVA
    if score >= 0.18 and conviction >= 0.55:
        return SignalStrength.FUERTE
    if score >= BUY_SCORE_MIN and conviction >= BUY_CONVICTION_MIN:
        return SignalStrength.MODERADA
    return SignalStrength.DÉBIL


def classify_decision_type(
    ticker:         str,
    score:          float,
    conviction:     float,
    delta_weight:   float,         # target_weight - current_weight (del optimizer)
    regime:         str,
    gate_state:     str = "NORMAL",
    from_optimizer: bool = True,   # True si viene del optimizer, False si es puro signal
    rr:             float = 0.0,   # R/R disponible si lo hay
) -> DecisionType:
    """
    Clasifica la decisión según la señal del activo y la razón del trade.

    Reglas:
    ─────────────────────────────────────────────
    BUY:
      score >= BUY_SCORE_MIN
      conviction >= BUY_CONVICTION_MIN
      delta_weight > 0 (se quiere más de este activo)
      gate permite compras (NORMAL o CAUTIOUS con señal fuerte)
      R/R >= BUY_RR_MIN si se conoce

    BUY_REBALANCE:
      delta_weight > 0 (el optimizer lo quiere)
      PERO score o conviction están por debajo del umbral de BUY
      O el score es negativo (el optimizer lo sube solo por construcción de cartera)

    SELL_FULL:
      delta_weight <= 0 y target_weight ≈ 0 (sale completo)

    SELL_PARTIAL:
      delta_weight < 0 (recorte, no salida total)

    HOLD:
      todo lo demás
    """
    score      = float(score or 0.0)
    conviction = float(conviction or 0.0)
    if conviction > 1.0:
        conviction /= 100.0

    is_defensive = gate_state in ("BLOCKED", "CAUTIOUS")

    if delta_weight > 0.005:  # compra o aumento
        signal_ok = (
            score >= BUY_SCORE_MIN
            and conviction >= BUY_CONVICTION_MIN
            and (rr >= BUY_RR_MIN or rr == 0.0)   # si no hay R/R, no bloquear
            and not (is_defensive and score < 0.10)  # defensivo requiere señal mínima
        )

        if signal_ok and not (from_optimizer and score < 0):
            # Señal suficiente → BUY
            return DecisionType.BUY
        else:
            # Señal insuficiente → BUY_REBALANCE
            return DecisionType.BUY_REBALANCE

    if delta_weight < -0.005:  # venta o reducción
        # Heurística: si el delta es mayor al 5% absoluto del portfolio, es SELL_FULL
        # Si no, es SELL_PARTIAL. El caller puede overridear esto.
        if delta_weight <= -0.08 or score <= -0.20:
            return DecisionType.SELL_FULL
        return DecisionType.SELL_PARTIAL

    return DecisionType.HOLD


# ══════════════════════════════════════════════════════════════════════════════
# CÁLCULO DE STOP-LOSS Y TARGET
# ══════════════════════════════════════════════════════════════════════════════

def compute_stop_levels(
    entry_price:  float,
    score:        float,
    vix:          Optional[float],
    regime:       str,
    atr_pct:      Optional[float] = None,   # ATR del screener si está disponible
    rr_override:  Optional[float] = None,   # R/R explícito del radar
) -> StopLevel:
    """
    Calcula stop-loss y target para una compra.

    Prioridad de stop:
      1. ATR disponible → usar ATR × 1.5 (stop basado en volatilidad real)
      2. VIX > 30       → stop ajustado a -5% (mercado en pánico)
      3. Régimen defens.→ stop ajustado a -5%
      4. Default        → -8%

    Target: entry_price × (1 + abs(stop) × RR)
    """
    is_defensive = str(regime).upper() in (
        "RISK_OFF", "DEFENSIVE", "BLOCKED", "CAUTIOUS"
    )
    vix_f = float(vix) if vix else 0.0

    # Determinar stop_pct y stop_source
    if atr_pct and atr_pct > 0:
        stop_pct   = -(atr_pct * 1.5)
        stop_pct   = max(stop_pct, -0.18)   # cap -18%
        stop_pct   = min(stop_pct, -0.04)   # mínimo -4%
        stop_source = StopSource.ATR
    elif vix_f > 30:
        stop_pct   = -0.05
        stop_source = StopSource.VIX_DYNAMIC
    elif is_defensive:
        stop_pct   = -STOP_CAUTIOUS_PCT
        stop_source = StopSource.FIXED
    else:
        stop_pct   = -STOP_DEFAULT_PCT
        stop_source = StopSource.FIXED

    stop_price = round(entry_price * (1 + stop_pct), 4)

    rr        = rr_override if rr_override and rr_override > 0 else TARGET_RR_DEFAULT
    target_pct = abs(stop_pct) * rr
    target_price = round(entry_price * (1 + target_pct), 4)
    rr_ratio   = target_pct / abs(stop_pct) if stop_pct != 0 else rr

    return StopLevel(
        entry_price      = round(entry_price, 4),
        stop_loss_pct    = round(stop_pct, 4),
        stop_loss_price  = stop_price,
        target_pct       = round(target_pct, 4),
        target_price     = target_price,
        rr_ratio         = round(rr_ratio, 2),
        stop_policy      = StopPolicy.HARD,
        stop_source      = stop_source,
        trailing_active  = False,
        exit_scope       = ExitScope.FULL,
        exit_reason_rule = ExitReasonRule.STOP_LOSS,
    )


# ══════════════════════════════════════════════════════════════════════════════
# CONSTRUCCIÓN DE TradeDecision DESDE RESULTADOS DEL PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def build_trade_decision(
    ticker:           str,
    score:            float,
    conviction:       float,
    delta_weight:     float,
    regime:           str,
    vix:              Optional[float] = None,
    entry_price:      Optional[float] = None,
    size_pct:         float           = 0.05,
    gate_state:       str             = "NORMAL",
    from_optimizer:   bool            = True,
    atr_pct:          Optional[float] = None,
    rr_from_radar:    Optional[float] = None,
    horizon_days:     int             = HORIZON_DEFAULT,
) -> TradeDecision:
    """
    Construye un TradeDecision completo desde los inputs del pipeline.

    Uso en run_analysis.py (paso 9 / _save_optimizer_trades):
        from src.analysis.trade_lifecycle import build_trade_decision
        td = build_trade_decision(
            ticker=ticker, score=score, conviction=conv,
            delta_weight=delta, regime=regime, vix=vix,
            entry_price=price_usd, size_pct=size_pct,
            gate_state=gate, from_optimizer=True,
        )
        # td.to_db_dict() → para persistir en decision_log
        # td.render_detail() → para mostrar en Telegram
    """
    decision_type   = classify_decision_type(
        ticker=ticker, score=score, conviction=conviction,
        delta_weight=delta_weight, regime=regime,
        gate_state=gate_state, from_optimizer=from_optimizer,
        rr=rr_from_radar or 0.0,
    )
    signal_strength = _signal_strength(score, conviction)

    stop = None
    if decision_type in (DecisionType.BUY, DecisionType.BUY_REBALANCE) and entry_price:
        stop = compute_stop_levels(
            entry_price  = entry_price,
            score        = score,
            vix          = vix,
            regime       = regime,
            atr_pct      = atr_pct,
            rr_override  = rr_from_radar,
        )

    source = "optimizer" if from_optimizer else "signal"
    notes  = ""
    if decision_type == DecisionType.BUY_REBALANCE:
        notes = (
            f"Score {score:+.3f} y conviction {conviction:.0%} por debajo del umbral de BUY — "
            f"aumento motivado por target del optimizer, no por señal táctica."
        )

    td = TradeDecision(
        ticker          = ticker.upper(),
        decision_type   = decision_type,
        signal_strength = signal_strength,
        score           = round(float(score or 0.0), 4),
        conviction      = round(float(conviction or 0.0), 4),
        size_pct        = round(float(size_pct or 0.05), 4),
        regime          = str(regime),
        vix             = float(vix) if vix else None,
        stop            = stop,
        horizon_days    = horizon_days,
        entry_price     = round(float(entry_price), 4) if entry_price else None,
        source          = source,
        notes           = notes,
    )

    logger.info(
        f"[trade_lifecycle] {td.decision_type.value} {ticker} | "
        f"score={score:+.3f} conv={conviction:.0%} "
        f"signal={signal_strength.value} "
        + (f"stop={stop.stop_loss_pct:+.1%} target={stop.target_pct:+.1%}" if stop else "no-stop")
    )

    return td


# ══════════════════════════════════════════════════════════════════════════════
# DETECCIÓN DE STOP ACTIVADO
# ══════════════════════════════════════════════════════════════════════════════

async def check_stop_activations(pool, lookback_days: int = 60) -> list[dict]:
    """
    Revisa trades abiertos (BUY o BUY_REBALANCE) para detectar si el stop
    se activó. Retorna lista de trades donde el precio actual < stop_loss_price.

    Esto no ejecuta la venta — solo la detecta y la loguea.
    El operador (o un futuro job) convierte la detección en SELL_FULL.

    Uso en update_outcomes.py:
        triggered = await check_stop_activations(pool)
        for t in triggered:
            print(f"STOP activado: {t['ticker']} precio actual {t['current_price']:.2f} < stop {t['stop_loss_price']:.2f}")
    """
    if not pool:
        return []

    from datetime import timedelta
    cutoff = __import__("datetime").datetime.utcnow() - timedelta(days=lookback_days)

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                id, ticker, decision, decided_at,
                price_at_decision, stop_loss_price, stop_loss_pct,
                target_price, target_pct, horizon_days,
                exit_scope, exit_reason_rule, decision_type
            FROM decision_log
            WHERE decision IN ('BUY')
              AND stop_loss_price IS NOT NULL
              AND outcome_5d IS NULL
              AND was_correct IS NULL
              AND decided_at >= $1
            ORDER BY decided_at DESC
            """,
            cutoff,
        )

    if not rows:
        return []

    try:
        import yfinance as yf
        tickers = list({str(r["ticker"]).upper() for r in rows})
        prices_now = {}
        if tickers:
            raw = yf.download(tickers, period="5d", progress=False, auto_adjust=True)["Close"]
            if hasattr(raw, "squeeze"):
                raw = raw if hasattr(raw, "columns") else raw.to_frame(name=tickers[0])
            for t in tickers:
                col = raw.get(t) if hasattr(raw, "get") else (
                    raw[t] if t in getattr(raw, "columns", []) else None
                )
                if col is not None and not col.dropna().empty:
                    prices_now[t] = float(col.dropna().iloc[-1])
    except Exception as e:
        logger.warning(f"check_stop_activations: no se pudo descargar precios — {e}")
        return []

    triggered = []
    for r in rows:
        ticker     = str(r["ticker"]).upper()
        stop_price = r["stop_loss_price"]
        current    = prices_now.get(ticker)

        if current is None or stop_price is None:
            continue

        if float(current) <= float(stop_price):
            loss_pct = (float(current) - float(r["price_at_decision"])) / float(r["price_at_decision"]) \
                       if r["price_at_decision"] else None
            triggered.append({
                "id":                  r["id"],
                "ticker":              ticker,
                "decided_at":          r["decided_at"],
                "current_price":       current,
                "stop_loss_price":     float(stop_price),
                "entry_price":         float(r["price_at_decision"]) if r["price_at_decision"] else None,
                "loss_pct":            round(loss_pct, 4) if loss_pct else None,
                "exit_scope":          r["exit_scope"] or "FULL",
                "exit_reason_rule":    "STOP_LOSS",
                "decision_type":       r["decision_type"] or "BUY",
            })
            logger.warning(
                f"STOP ACTIVADO: {ticker} id={r['id']} "
                f"precio actual ${current:.2f} < stop ${float(stop_price):.2f} "
                + (f"(pérdida {loss_pct:+.1%})" if loss_pct else "")
            )

    if triggered:
        logger.info(
            f"check_stop_activations: {len(triggered)} stops activados — "
            f"{[t['ticker'] for t in triggered]}"
        )

    return triggered


async def register_stop_exit(
    pool,
    trade_id:    int,
    close_price: float,
    closed_at:   Optional[datetime] = None,
) -> bool:
    """
    Registra el cierre de un trade por stop-loss en decision_log.

    Marca:
      was_stopped = TRUE
      exit_reason = 'STOP_LOSS'
      closed_at   = timestamp del cierre
      close_price = precio de ejecución
      was_correct = FALSE (el stop se activó = trade perdedor)
    """
    if not pool:
        return False
    if closed_at is None:
        closed_at = datetime.now(timezone.utc)

    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE decision_log SET
                    was_stopped  = TRUE,
                    exit_reason  = 'STOP_LOSS',
                    closed_at    = $1,
                    close_price  = $2,
                    was_correct  = FALSE,
                    outcome_filled_at = NOW()
                WHERE id = $3
                """,
                closed_at, float(close_price), trade_id,
            )
        logger.info(
            f"register_stop_exit: trade id={trade_id} cerrado por STOP_LOSS "
            f"@ ${close_price:.2f}"
        )
        return True
    except Exception as e:
        logger.error(f"register_stop_exit error: {e}", exc_info=True)
        return False


# ══════════════════════════════════════════════════════════════════════════════
# MIGRACIÓN DE DB
# ══════════════════════════════════════════════════════════════════════════════

MIGRATION_SQL = """
-- trade_lifecycle migration — additive, no rompe filas existentes
-- Correr una vez: python scripts/init_db.py o psql manual

ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS decision_type    TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS signal_strength  TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS stop_loss_price  FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS target_price     FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS rr_ratio         FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS exit_scope       TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS exit_reason_rule TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS stop_policy      TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS stop_source      TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS trailing_active  BOOLEAN DEFAULT FALSE;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS was_stopped      BOOLEAN;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS exit_reason      TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS closed_at        TIMESTAMPTZ;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS close_price      FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS source           TEXT;

-- Rellenar decision_type para filas existentes basado en `decision`
UPDATE decision_log
SET decision_type = CASE
    WHEN decision = 'BUY'  THEN 'BUY'
    WHEN decision = 'SELL' THEN 'SELL_FULL'
    WHEN decision = 'HOLD' THEN 'HOLD'
    ELSE decision
END
WHERE decision_type IS NULL;

-- Índice para queries de stops abiertos
CREATE INDEX IF NOT EXISTS idx_decision_log_stops
    ON decision_log(decision, stop_loss_price, outcome_5d)
    WHERE stop_loss_price IS NOT NULL AND outcome_5d IS NULL;
"""


async def run_migration(pool) -> bool:
    """Corre la migración de decision_log. Idempotente."""
    if not pool:
        return False
    try:
        stmts = [s.strip() for s in MIGRATION_SQL.split(";") if s.strip()]
        async with pool.acquire() as conn:
            for stmt in stmts:
                if stmt.upper().startswith("--"):
                    continue
                try:
                    await conn.execute(stmt)
                except Exception as e:
                    logger.debug(f"Migration stmt ignorado: {e!r}")
        logger.info("trade_lifecycle migration completada")
        return True
    except Exception as e:
        logger.error(f"run_migration error: {e}", exc_info=True)
        return False

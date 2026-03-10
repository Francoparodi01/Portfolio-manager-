"""
src/analysis/technical.py — Análisis técnico multicapa profesional.

Indicadores implementados:
  Tendencia   : SMA(20,50,200), EMA(12,26), ADX(14)
  Momentum    : RSI(14), Estocástico(14,3), Williams %R(14)
  MACD        : (12,26,9) — línea, señal e histograma
  Volatilidad : Bollinger Bands(20,2), ATR(14)
  Volumen     : OBV, volumen relativo
  Estructura  : soporte/resistencia vía swing highs/lows

Sistema de scoring:
  Cada condición aporta puntos positivos (alcista) o negativos (bajista).
  Score > +3  → BUY   | Score < -3  → SELL  | Resto → HOLD
  La fuerza (strength) es la magnitud normalizada del score.

Principios:
  - Zero look-ahead bias: todos los indicadores usan solo datos del pasado.
  - Confirmación de volumen obligatoria para señales fuertes.
  - HOLD con dirección: score_raw preserva el sesgo aunque no sea BUY/SELL.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

try:
    import pandas as pd
    import numpy as np
    HAS_DEPS = True
except ImportError:
    HAS_DEPS = False

try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False

logger = logging.getLogger(__name__)


# ── Dataclasses ────────────────────────────────────────────────────────────────

@dataclass
class Signal:
    ticker: str
    signal: str           # "BUY" | "SELL" | "HOLD"
    strength: float       # 0.0 – 1.0
    score_raw: float      # score sin clampear (-9 a +9) para síntesis
    reasons: list[str]
    price_usd: float
    generated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_telegram(self) -> str:
        icon = {"BUY": "🟢", "SELL": "🔴", "HOLD": "🟡"}.get(self.signal, "⚪")
        bar  = "█" * int(self.strength * 5) + "░" * (5 - int(self.strength * 5))
        reasons_txt = "\n".join(f"  • {r}" for r in self.reasons)
        return (
            f"{icon} <b>{self.ticker}</b> — <b>{self.signal}</b>\n"
            f"Precio: <b>${self.price_usd:.2f}</b>  |  Fuerza: {bar} {self.strength:.0%}\n"
            f"{reasons_txt}"
        )


@dataclass
class IndicatorSnapshot:
    ticker: str
    close: float
    # Tendencia
    sma_20: float; sma_50: float; sma_200: float
    ema_12: float; ema_26: float
    # ADX — fuerza de tendencia (no dirección)
    adx_14: float; di_plus: float; di_minus: float
    # Momentum
    rsi_14: float
    stoch_k: float; stoch_d: float    # Estocástico
    williams_r: float                  # Williams %R
    # MACD
    macd_line: float; macd_signal: float; macd_hist: float
    macd_hist_prev: float              # histograma anterior (detectar aceleración)
    # Volatilidad
    bb_upper: float; bb_middle: float; bb_lower: float
    bb_width: float                    # (upper-lower)/middle — contracción BB
    atr_14: float
    # Volumen
    obv: float; obv_sma20: float       # OBV y su media (tendencia de volumen)
    vol_ratio: float                   # last_vol / sma_vol_20


# ── Descarga ───────────────────────────────────────────────────────────────────

def fetch_history(ticker: str, period: str = "6mo",
                  interval: str = "1d") -> Optional["pd.DataFrame"]:
    if not HAS_YFINANCE:
        raise ImportError("yfinance no instalado")
    if not HAS_DEPS:
        raise ImportError("pandas/numpy no instalados")
    try:
        data = yf.download(ticker, period=period, interval=interval,
                           progress=False, auto_adjust=True)
        if data.empty:
            logger.warning(f"yfinance vacío para {ticker}")
            return None
        # yfinance >= 1.0 retorna MultiIndex columns: ('Close', 'AAPL')
        # Aplanar a columnas simples: 'Close', 'Open', etc.
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)
        logger.info(f"{ticker}: {len(data)} velas descargadas ({period}/{interval})")
        return data
    except Exception as e:
        logger.error(f"Error descargando {ticker}: {e}")
        return None


# ── Helpers de series ──────────────────────────────────────────────────────────

def _sma(s: "pd.Series", w: int) -> "pd.Series":
    return s.rolling(w).mean()

def _ema(s: "pd.Series", span: int) -> "pd.Series":
    return s.ewm(span=span, adjust=False).mean()

def _rsi(s: "pd.Series", period: int = 14) -> "pd.Series":
    delta = s.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / loss.replace(0, float("nan"))
    return 100 - (100 / (1 + rs))

def _macd(s: "pd.Series"):
    ema12 = _ema(s, 12); ema26 = _ema(s, 26)
    line  = ema12 - ema26
    sig   = _ema(line, 9)
    return line, sig, line - sig

def _bollinger(s: "pd.Series", w: int = 20, n: float = 2.0):
    mid = _sma(s, w)
    std = s.rolling(w).std()
    return mid + n * std, mid, mid - n * std

def _atr(df: "pd.DataFrame", period: int = 14) -> "pd.Series":
    h, l, pc = df["High"], df["Low"], df["Close"].shift(1)
    tr = pd.concat([(h-l).abs(), (h-pc).abs(), (l-pc).abs()], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def _stochastic(df: "pd.DataFrame", k: int = 14, d: int = 3):
    lo = df["Low"].rolling(k).min()
    hi = df["High"].rolling(k).max()
    stoch_k = 100 * (df["Close"] - lo) / (hi - lo + 1e-9)
    stoch_d = stoch_k.rolling(d).mean()
    return stoch_k, stoch_d

def _williams_r(df: "pd.DataFrame", period: int = 14) -> "pd.Series":
    hi = df["High"].rolling(period).max()
    lo = df["Low"].rolling(period).min()
    return -100 * (hi - df["Close"]) / (hi - lo + 1e-9)

def _adx(df: "pd.DataFrame", period: int = 14):
    """ADX + DI+/DI- (Wilder smoothing)."""
    h  = df["High"]; l = df["Low"]; pc = df["Close"].shift(1)
    tr = pd.concat([(h-l).abs(), (h-pc).abs(), (l-pc).abs()], axis=1).max(axis=1)
    up = h - h.shift(1); dn = l.shift(1) - l
    dm_plus  = up.where((up > dn) & (up > 0), 0.0)
    dm_minus = dn.where((dn > up) & (dn > 0), 0.0)
    # Wilder smoothing
    atr_w  = tr.ewm(alpha=1/period, adjust=False).mean()
    di_p   = 100 * dm_plus.ewm(alpha=1/period, adjust=False).mean() / (atr_w + 1e-9)
    di_m   = 100 * dm_minus.ewm(alpha=1/period, adjust=False).mean() / (atr_w + 1e-9)
    dx     = 100 * (di_p - di_m).abs() / (di_p + di_m + 1e-9)
    adx    = dx.ewm(alpha=1/period, adjust=False).mean()
    return adx, di_p, di_m

def _obv(df: "pd.DataFrame") -> "pd.Series":
    direction = df["Close"].diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    return (direction * df["Volume"]).cumsum()

def _last(s: "pd.Series") -> float:
    v = s.dropna()
    return float(v.iloc[-1]) if not v.empty else 0.0

def _prev(s: "pd.Series") -> float:
    v = s.dropna()
    return float(v.iloc[-2]) if len(v) >= 2 else 0.0


# ── Cálculo de indicadores ─────────────────────────────────────────────────────

def compute_indicators(df: "pd.DataFrame", ticker: str) -> Optional[IndicatorSnapshot]:
    if df is None or len(df) < 60:
        logger.warning(f"{ticker}: datos insuficientes ({len(df) if df is not None else 0} velas)")
        return None
    try:
        close  = df["Close"].squeeze()
        volume = df["Volume"].squeeze()

        # Tendencia
        sma20  = _sma(close, 20);  sma50 = _sma(close, 50);  sma200 = _sma(close, 200)
        ema12  = _ema(close, 12);  ema26 = _ema(close, 26)

        # ADX
        adx, di_p, di_m = _adx(df, 14)

        # Momentum
        rsi    = _rsi(close, 14)
        sk, sd = _stochastic(df, 14, 3)
        wr     = _williams_r(df, 14)

        # MACD
        macd_l, macd_s, macd_h = _macd(close)

        # Bollinger
        bb_u, bb_m, bb_l = _bollinger(close, 20, 2.0)
        bb_w = (bb_u - bb_l) / (bb_m + 1e-9)

        # ATR
        atr = _atr(df, 14)

        # Volumen / OBV
        obv_s    = _obv(df)
        obv_ma   = _sma(obv_s, 20)
        vol_sma  = _sma(volume, 20)
        last_vol = _last(volume)
        v_sma    = _last(vol_sma)
        v_ratio  = last_vol / v_sma if v_sma > 0 else 1.0

        return IndicatorSnapshot(
            ticker=ticker, close=_last(close),
            sma_20=_last(sma20), sma_50=_last(sma50), sma_200=_last(sma200),
            ema_12=_last(ema12), ema_26=_last(ema26),
            adx_14=_last(adx), di_plus=_last(di_p), di_minus=_last(di_m),
            rsi_14=_last(rsi),
            stoch_k=_last(sk), stoch_d=_last(sd),
            williams_r=_last(wr),
            macd_line=_last(macd_l), macd_signal=_last(macd_s),
            macd_hist=_last(macd_h), macd_hist_prev=_prev(macd_h),
            bb_upper=_last(bb_u), bb_middle=_last(bb_m), bb_lower=_last(bb_l),
            bb_width=_last(bb_w),
            atr_14=_last(atr),
            obv=_last(obv_s), obv_sma20=_last(obv_ma),
            vol_ratio=v_ratio,
        )
    except Exception as e:
        logger.error(f"Error calculando indicadores {ticker}: {e}", exc_info=True)
        return None


# ── Generación de señales ──────────────────────────────────────────────────────

def generate_signals(ind: IndicatorSnapshot) -> Signal:
    """
    Sistema de scoring profesional multicapa.

    Cada condición suma puntos. El score final determina BUY/SELL/HOLD.
    Principios de trading aplicados:
      1. Tendencia primero (ADX, medias móviles) — operar a favor del trend.
      2. Momentum confirma (RSI, Estocástico, MACD).
      3. Precio vs estructura (Bollinger, soporte/resistencia).
      4. Volumen valida (OBV trend, vol ratio).
      5. Señales contradictorias → reducir puntuación, no eliminar.
    """
    score = 0.0
    reasons_buy:  list[str] = []
    reasons_sell: list[str] = []

    c   = ind.close
    rsi = ind.rsi_14

    # ── 1. TENDENCIA — ADX + Medias Móviles ─────────────────────────────────
    # ADX > 25: tendencia establecida. Sin ADX alto las señales son menos fiables.
    trend_factor = 1.0 if ind.adx_14 > 25 else 0.7  # penaliza señales en rango lateral

    if ind.di_plus > ind.di_minus and ind.adx_14 > 20:
        s = 1.5 * trend_factor
        score += s
        reasons_buy.append(f"ADX {ind.adx_14:.1f} — DI+ > DI− (tendencia alcista)")
    elif ind.di_minus > ind.di_plus and ind.adx_14 > 20:
        s = 1.5 * trend_factor
        score -= s
        reasons_sell.append(f"ADX {ind.adx_14:.1f} — DI− > DI+ (tendencia bajista)")

    # EMA 12/26 — cruce corto plazo
    if ind.ema_12 > ind.ema_26:
        score += 1.0 * trend_factor
        reasons_buy.append("EMA12 > EMA26 — tendencia alcista corto plazo")
    else:
        score -= 1.0 * trend_factor
        reasons_sell.append("EMA12 < EMA26 — tendencia bajista corto plazo")

    # SMA 20/50 — Golden/Death Cross (señal de mediano plazo más fuerte)
    if ind.sma_50 > 0:
        if ind.sma_20 > ind.sma_50:
            score += 1.2
            reasons_buy.append("SMA20 > SMA50 — Golden Cross activo")
        else:
            score -= 1.2
            reasons_sell.append("SMA20 < SMA50 — Death Cross activo")

    # SMA 200 — tendencia de largo plazo (los institucionales la respetan)
    if ind.sma_200 > 0:
        dist_200 = (c - ind.sma_200) / ind.sma_200
        if c > ind.sma_200:
            score += 0.8
            reasons_buy.append(f"Precio {dist_200:+.1%} sobre SMA200 — bull market")
        else:
            score -= 0.8
            reasons_sell.append(f"Precio {dist_200:+.1%} bajo SMA200 — bear market")

    # ── 2. MOMENTUM — RSI ────────────────────────────────────────────────────
    if rsi < 30:
        score += 2.5
        reasons_buy.append(f"RSI {rsi:.1f} — sobreventa severa (potencial reversión)")
    elif rsi < 40:
        score += 1.5
        reasons_buy.append(f"RSI {rsi:.1f} — zona de sobreventa")
    elif rsi > 70:
        score -= 2.5
        reasons_sell.append(f"RSI {rsi:.1f} — sobrecompra severa")
    elif rsi > 60:
        score -= 1.5
        reasons_sell.append(f"RSI {rsi:.1f} — zona de sobrecompra")

    # ── 3. MOMENTUM — Estocástico ─────────────────────────────────────────────
    # Cruces del estocástico son señales tempranas de reversión
    if ind.stoch_k < 20 and ind.stoch_k > ind.stoch_d:
        score += 1.5
        reasons_buy.append(f"Estocástico {ind.stoch_k:.1f} — cruce alcista en sobreventa")
    elif ind.stoch_k > 80 and ind.stoch_k < ind.stoch_d:
        score -= 1.5
        reasons_sell.append(f"Estocástico {ind.stoch_k:.1f} — cruce bajista en sobrecompra")
    elif ind.stoch_k < 30:
        score += 0.8
        reasons_buy.append(f"Estocástico {ind.stoch_k:.1f} — zona de sobreventa")
    elif ind.stoch_k > 70:
        score -= 0.8
        reasons_sell.append(f"Estocástico {ind.stoch_k:.1f} — zona de sobrecompra")

    # ── 4. MOMENTUM — Williams %R ────────────────────────────────────────────
    wr = ind.williams_r
    if wr < -80:
        score += 1.0
        reasons_buy.append(f"Williams %R {wr:.1f} — sobreventa extrema")
    elif wr > -20:
        score -= 1.0
        reasons_sell.append(f"Williams %R {wr:.1f} — sobrecompra")

    # ── 5. MACD ───────────────────────────────────────────────────────────────
    # Histograma: detectar aceleración/desaceleración del momentum
    macd_accel = ind.macd_hist - ind.macd_hist_prev   # positivo = momentum acelerando
    if ind.macd_hist > 0:
        pts = 1.5 + (0.5 if macd_accel > 0 else 0)   # +0.5 si acelera
        score += pts
        reasons_buy.append(
            f"MACD alcista (hist={ind.macd_hist:+.3f}, {'↑ acelerando' if macd_accel > 0 else '→ estable'})"
        )
    elif ind.macd_hist < 0:
        pts = 1.5 + (0.5 if macd_accel < 0 else 0)
        score -= pts
        reasons_sell.append(
            f"MACD bajista (hist={ind.macd_hist:+.3f}, {'↓ acelerando' if macd_accel < 0 else '→ estable'})"
        )

    # ── 6. BOLLINGER BANDS ───────────────────────────────────────────────────
    bb_range = ind.bb_upper - ind.bb_lower
    if bb_range > 0:
        bb_pos = (c - ind.bb_lower) / bb_range   # 0=lower, 1=upper
        if c <= ind.bb_lower * 1.005:
            # Precio toca/rompe banda inferior → rebote probable
            score += 2.0
            reasons_buy.append(f"Precio en banda inferior Bollinger (BB%={bb_pos:.1%})")
        elif c >= ind.bb_upper * 0.995:
            # Precio toca/rompe banda superior → sobreextensión
            score -= 2.0
            reasons_sell.append(f"Precio en banda superior Bollinger (BB%={bb_pos:.1%})")
        # Squeeze de Bollinger (compresión de volatilidad → explosión próxima)
        if ind.bb_width < 0.05:
            reasons_buy.append(f"BB Width {ind.bb_width:.3f} — squeeze: volumen importante próximo")

    # ── 7. VOLUMEN — OBV ─────────────────────────────────────────────────────
    # OBV tendencia confirma o diverge del precio
    if ind.obv_sma20 != 0:
        obv_trend = (ind.obv - ind.obv_sma20) / abs(ind.obv_sma20 + 1e-9)
        if obv_trend > 0.02:
            if score > 0:
                score += 0.5
                reasons_buy.append("OBV sobre su media — acumulación institucional")
        elif obv_trend < -0.02:
            if score < 0:
                score -= 0.5
                reasons_sell.append("OBV bajo su media — distribución institucional")

    # Volumen relativo — confirma los movimientos
    if ind.vol_ratio > 1.5:
        vol_note = f"Volumen {ind.vol_ratio:.1f}x su media — movimiento respaldado"
        if score > 0:
            score += 0.5
            reasons_buy.append(vol_note)
        elif score < 0:
            score -= 0.5
            reasons_sell.append(vol_note)

    # ── Clasificar señal final ────────────────────────────────────────────────
    MAX_SCORE = 12.0   # suma máxima posible con todos los indicadores
    if score >= 3.5:
        direction = "BUY"
        strength  = min(score / MAX_SCORE, 1.0)
        reasons   = reasons_buy[:5]
    elif score <= -3.5:
        direction = "SELL"
        strength  = min(abs(score) / MAX_SCORE, 1.0)
        reasons   = reasons_sell[:5]
    else:
        direction = "HOLD"
        strength  = 1.0 - abs(score) / MAX_SCORE
        reasons   = (reasons_buy + reasons_sell)[:4] or ["Sin señal clara — esperar confirmación"]

    return Signal(
        ticker=ind.ticker,
        signal=direction,
        strength=round(max(strength, 0.0), 3),
        score_raw=round(score, 4),
        reasons=reasons,
        price_usd=ind.close,
    )


# ── Pipeline público ───────────────────────────────────────────────────────────

def analyze_ticker(ticker: str, period: str = "6mo") -> Optional[Signal]:
    df = fetch_history(ticker, period=period)
    if df is None:
        return None
    ind = compute_indicators(df, ticker)
    if ind is None:
        return None
    signal = generate_signals(ind)
    logger.info(f"{ticker}: {signal.signal} (fuerza={signal.strength:.0%}, score_reasons={len(signal.reasons)})")
    return signal


def analyze_portfolio(tickers: list[str], period: str = "6mo") -> list[Signal]:
    signals = []
    for ticker in tickers:
        try:
            sig = analyze_ticker(ticker, period)
            if sig:
                signals.append(sig)
        except Exception as e:
            logger.error(f"Error analizando {ticker}: {e}")
    priority = {"BUY": 0, "SELL": 1, "HOLD": 2}
    return sorted(signals, key=lambda s: (priority.get(s.signal, 3), -s.strength))

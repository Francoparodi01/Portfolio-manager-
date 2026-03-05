"""
src/analysis/technical.py

Capa de análisis técnico sobre datos históricos reales (Yahoo Finance).

Flujo:
  1. fetch_history()     → descarga OHLCV del subyacente USD desde yfinance
  2. compute_indicators()→ calcula RSI, MACD, Bollinger, SMA, EMA, ATR
  3. generate_signals()  → evalúa condiciones y genera señales BUY/SELL/HOLD
  4. build_report()      → arma resumen completo para Telegram

Tickers usados (subyacentes NYSE/NASDAQ de tus CEDEARs):
  CVX  → CVX   (Chevron)
  NVDA → NVDA  (Nvidia)
  MU   → MU    (Micron)
  MELI → MELI  (MercadoLibre)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

try:
    import pandas as pd
    import numpy as np
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False

logger = logging.getLogger(__name__)


# ── Señales ────────────────────────────────────────────────────────────────────

@dataclass
class Signal:
    ticker: str
    signal: str           # "BUY" | "SELL" | "HOLD"
    strength: float       # 0.0 – 1.0
    score_raw: float      # score sin clasificar (-9 a +9) para la sintesis
    reasons: list[str]    # razones legibles
    price_usd: float
    generated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_telegram(self) -> str:
        icon = {"BUY": "🟢", "SELL": "🔴", "HOLD": "🟡"}.get(self.signal, "⚪")
        strength_bar = "█" * int(self.strength * 5) + "░" * (5 - int(self.strength * 5))
        reasons_txt = "\n".join(f"  • {r}" for r in self.reasons)
        return (
            f"{icon} <b>{self.ticker}</b> — <b>{self.signal}</b>\n"
            f"Precio: <b>${self.price_usd:.2f}</b>  |  Fuerza: {strength_bar} {self.strength:.0%}\n"
            f"{reasons_txt}"
        )


@dataclass
class IndicatorSnapshot:
    ticker: str
    close: float
    # Tendencia
    sma_20: float
    sma_50: float
    ema_12: float
    ema_26: float
    # Momentum
    rsi_14: float
    # MACD
    macd_line: float
    macd_signal: float
    macd_hist: float
    # Volatilidad
    bb_upper: float
    bb_middle: float
    bb_lower: float
    atr_14: float
    # Volumen
    vol_sma_20: float
    last_volume: float


# ── Descarga de datos ──────────────────────────────────────────────────────────

def fetch_history(ticker: str, period: str = "6mo", interval: str = "1d") -> Optional["pd.DataFrame"]:
    """
    Descarga OHLCV desde Yahoo Finance.

    Args:
        ticker:   símbolo NYSE/NASDAQ (ej: "CVX", "NVDA")
        period:   "1mo" | "3mo" | "6mo" | "1y" | "2y"
        interval: "1d" | "1wk"

    Returns:
        DataFrame con columnas: Open, High, Low, Close, Volume
        o None si falla la descarga.
    """
    if not HAS_YFINANCE:
        raise ImportError("yfinance no instalado: pip install yfinance")
    if not HAS_PANDAS:
        raise ImportError("pandas no instalado: pip install pandas")

    try:
        data = yf.download(ticker, period=period, interval=interval, progress=False, auto_adjust=True)
        if data.empty:
            logger.warning(f"yfinance retornó datos vacíos para {ticker}")
            return None
        logger.info(f"{ticker}: {len(data)} velas descargadas ({period}/{interval})")
        return data
    except Exception as e:
        logger.error(f"Error descargando {ticker}: {e}")
        return None


# ── Cálculo de indicadores ─────────────────────────────────────────────────────

def _sma(series: "pd.Series", window: int) -> "pd.Series":
    return series.rolling(window=window).mean()

def _ema(series: "pd.Series", span: int) -> "pd.Series":
    return series.ewm(span=span, adjust=False).mean()

def _rsi(series: "pd.Series", period: int = 14) -> "pd.Series":
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(window=period).mean()
    loss = (-delta.clip(upper=0)).rolling(window=period).mean()
    rs = gain / loss.replace(0, float("nan"))
    return 100 - (100 / (1 + rs))

def _macd(series: "pd.Series") -> tuple["pd.Series", "pd.Series", "pd.Series"]:
    ema12 = _ema(series, 12)
    ema26 = _ema(series, 26)
    macd_line = ema12 - ema26
    signal = _ema(macd_line, 9)
    hist = macd_line - signal
    return macd_line, signal, hist

def _bollinger(series: "pd.Series", window: int = 20, std: float = 2.0):
    middle = _sma(series, window)
    std_dev = series.rolling(window=window).std()
    upper = middle + std * std_dev
    lower = middle - std * std_dev
    return upper, middle, lower

def _atr(df: "pd.DataFrame", period: int = 14) -> "pd.Series":
    high = df["High"]
    low  = df["Low"]
    prev_close = df["Close"].shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(window=period).mean()


def compute_indicators(df: "pd.DataFrame", ticker: str) -> Optional[IndicatorSnapshot]:
    """
    Calcula todos los indicadores sobre el DataFrame OHLCV.
    Retorna un IndicatorSnapshot con los valores del último cierre.
    """
    if df is None or len(df) < 50:
        logger.warning(f"{ticker}: datos insuficientes para calcular indicadores (necesita >= 50 velas)")
        return None

    try:
        close  = df["Close"].squeeze()
        volume = df["Volume"].squeeze()

        sma20  = _sma(close, 20)
        sma50  = _sma(close, 50)
        ema12  = _ema(close, 12)
        ema26  = _ema(close, 26)
        rsi    = _rsi(close, 14)
        macd_l, macd_s, macd_h = _macd(close)
        bb_u, bb_m, bb_l = _bollinger(close, 20, 2.0)
        atr    = _atr(df, 14)
        vol_sma = _sma(volume, 20)

        # Último valor válido
        def last(s):
            v = s.dropna()
            return float(v.iloc[-1]) if not v.empty else 0.0

        return IndicatorSnapshot(
            ticker=ticker,
            close=last(close),
            sma_20=last(sma20),
            sma_50=last(sma50),
            ema_12=last(ema12),
            ema_26=last(ema26),
            rsi_14=last(rsi),
            macd_line=last(macd_l),
            macd_signal=last(macd_s),
            macd_hist=last(macd_h),
            bb_upper=last(bb_u),
            bb_middle=last(bb_m),
            bb_lower=last(bb_l),
            atr_14=last(atr),
            vol_sma_20=last(vol_sma),
            last_volume=last(volume),
        )
    except Exception as e:
        logger.error(f"Error calculando indicadores para {ticker}: {e}", exc_info=True)
        return None


# ── Generación de señales ──────────────────────────────────────────────────────

def generate_signals(ind: IndicatorSnapshot) -> Signal:
    """
    Evalúa los indicadores y genera una señal BUY / SELL / HOLD.

    Lógica multicritério — cada condición suma o resta puntos:

    ALCISTAS (+):
      • RSI < 35 (sobreventa)
      • Precio cruza SMA20 desde abajo (precio > SMA20 y ema12 > ema26)
      • MACD histogram positivo y creciente (cruce alcista)
      • Precio en/cerca banda inferior Bollinger (< bb_lower * 1.01)
      • EMA12 > EMA26 (tendencia alcista de corto plazo)
      • SMA20 > SMA50 (golden cross de mediano plazo)
      • Volumen > 1.5x su media (confirmación de movimiento)

    BAJISTAS (-):
      • RSI > 65 (sobrecompra)
      • Precio cerca de banda superior Bollinger (> bb_upper * 0.99)
      • MACD histogram negativo
      • EMA12 < EMA26 (tendencia bajista)
      • SMA20 < SMA50 (death cross)

    Score final:
      > +3  → BUY  (fuerza proporcional al score)
      < -3  → SELL
      entre → HOLD
    """
    reasons_buy  = []
    reasons_sell = []
    score = 0.0

    c     = ind.close
    rsi   = ind.rsi_14

    # ── RSI ───────────────────────────────────────────
    if rsi < 30:
        score += 2.5
        reasons_buy.append(f"RSI {rsi:.1f} — sobreventa fuerte")
    elif rsi < 40:
        score += 1.5
        reasons_buy.append(f"RSI {rsi:.1f} — zona de sobreventa")
    elif rsi > 70:
        score -= 2.5
        reasons_sell.append(f"RSI {rsi:.1f} — sobrecompra fuerte")
    elif rsi > 60:
        score -= 1.5
        reasons_sell.append(f"RSI {rsi:.1f} — zona de sobrecompra")

    # ── MACD ─────────────────────────────────────────
    if ind.macd_hist > 0 and ind.macd_line > ind.macd_signal:
        score += 1.5
        reasons_buy.append(f"MACD cruce alcista (hist={ind.macd_hist:.3f})")
    elif ind.macd_hist < 0 and ind.macd_line < ind.macd_signal:
        score -= 1.5
        reasons_sell.append(f"MACD cruce bajista (hist={ind.macd_hist:.3f})")

    # ── Bollinger Bands ───────────────────────────────
    bb_range = ind.bb_upper - ind.bb_lower
    if bb_range > 0:
        bb_pos = (c - ind.bb_lower) / bb_range  # 0=lower, 1=upper
        if c <= ind.bb_lower * 1.01:
            score += 2.0
            reasons_buy.append(f"Precio en banda inferior Bollinger (BB%={bb_pos:.1%})")
        elif c >= ind.bb_upper * 0.99:
            score -= 2.0
            reasons_sell.append(f"Precio en banda superior Bollinger (BB%={bb_pos:.1%})")

    # ── EMAs corto plazo ──────────────────────────────
    if ind.ema_12 > ind.ema_26:
        score += 1.0
        reasons_buy.append(f"EMA12 > EMA26 — tendencia alcista corto plazo")
    else:
        score -= 1.0
        reasons_sell.append(f"EMA12 < EMA26 — tendencia bajista corto plazo")

    # ── Golden / Death cross ──────────────────────────
    if ind.sma_20 > 0 and ind.sma_50 > 0:
        if ind.sma_20 > ind.sma_50:
            score += 1.0
            reasons_buy.append(f"SMA20 > SMA50 — Golden Cross activo")
        else:
            score -= 1.0
            reasons_sell.append(f"SMA20 < SMA50 — Death Cross activo")

    # ── Precio vs SMA20 ───────────────────────────────
    if ind.sma_20 > 0:
        dist_sma20 = (c - ind.sma_20) / ind.sma_20
        if dist_sma20 > 0.02:
            reasons_buy.append(f"Precio {dist_sma20:.1%} sobre SMA20")
        elif dist_sma20 < -0.03:
            score += 0.5
            reasons_buy.append(f"Precio {abs(dist_sma20):.1%} bajo SMA20 — posible rebote")

    # ── Volumen ───────────────────────────────────────
    if ind.vol_sma_20 > 0:
        vol_ratio = ind.last_volume / ind.vol_sma_20
        if vol_ratio > 1.5:
            vol_note = f"Volumen {vol_ratio:.1f}x su media — movimiento confirmado"
            if score > 0:
                score += 0.5
                reasons_buy.append(vol_note)
            elif score < 0:
                score -= 0.5
                reasons_sell.append(vol_note)

    # ── Clasificar señal ──────────────────────────────
    MAX_SCORE = 9.0
    if score >= 3.0:
        direction = "BUY"
        strength  = min(score / MAX_SCORE, 1.0)
        reasons   = reasons_buy[:5]
    elif score <= -3.0:
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
        strength=round(strength, 3),
        score_raw=round(score, 4),
        reasons=reasons,
        price_usd=ind.close,
    )


# ── Análisis completo de un ticker ─────────────────────────────────────────────

def analyze_ticker(ticker: str, period: str = "6mo") -> Optional[Signal]:
    """
    Pipeline completo: descarga → indicadores → señal.
    Retorna Signal o None si falla algún paso.
    """
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
    """
    Analiza una lista de tickers y retorna las señales ordenadas por fuerza.
    """
    signals = []
    for ticker in tickers:
        try:
            sig = analyze_ticker(ticker, period)
            if sig:
                signals.append(sig)
        except Exception as e:
            logger.error(f"Error analizando {ticker}: {e}")

    # Ordenar: primero BUY/SELL con más fuerza, HOLD al final
    def sort_key(s: Signal):
        priority = {"BUY": 0, "SELL": 1, "HOLD": 2}
        return (priority.get(s.signal, 3), -s.strength)

    return sorted(signals, key=sort_key)


# ── Reporte completo para Telegram ────────────────────────────────────────────

def build_telegram_report(signals: list[Signal], portfolio_total_ars: float) -> str:
    """
    Construye el mensaje de análisis para enviar por Telegram.
    """
    if not signals:
        return "Sin señales disponibles — revisar conexión con Yahoo Finance"

    lines = [
        "📊 <b>ANÁLISIS TÉCNICO — SUBYACENTES USD</b>",
        f"Portfolio: <b>${portfolio_total_ars:,.0f} ARS</b>",
        f"Fecha: {datetime.now().strftime('%d/%m/%Y %H:%M')} ART",
        "─" * 32,
    ]

    for sig in signals:
        lines.append("")
        lines.append(sig.to_telegram())

    # Resumen ejecutivo
    buys  = [s for s in signals if s.signal == "BUY"]
    sells = [s for s in signals if s.signal == "SELL"]

    lines.append("")
    lines.append("─" * 32)
    lines.append(f"🟢 Compra: {len(buys)}  🔴 Venta: {len(sells)}  🟡 Hold: {len(signals)-len(buys)-len(sells)}")

    if buys:
        tickers_buy = ", ".join(s.ticker for s in buys)
        lines.append(f"⚡ Señales activas de compra: <b>{tickers_buy}</b>")
    if sells:
        tickers_sell = ", ".join(s.ticker for s in sells)
        lines.append(f"⚠️ Señales activas de venta: <b>{tickers_sell}</b>")

    lines.append("")
    lines.append("<i>Análisis técnico — no es recomendación de inversión</i>")

    return "\n".join(lines)

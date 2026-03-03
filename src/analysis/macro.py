"""
src/analysis/macro.py

Capa macro: descarga y evalua indicadores globales via yfinance.
Todos los tickers son gratuitos, sin API key.

Indicadores:
  WTI     (CL=F)      → precio del petroleo crudo
  Brent   (BZ=F)      → petroleo europeo (referencia global)
  DXY     (DX-Y.NYB)  → indice del dolar americano
  VIX     (^VIX)      → volatilidad implicita / miedo del mercado
  TNX     (^TNX)      → tasa del bono a 10 años (proxy Fed)
  SP500   (^GSPC)     → mercado general (risk-on/risk-off)
  GOLD    (GC=F)      → oro (flight-to-safety)

Output por ticker del portfolio:
  macro_score   float  -1.0 a +1.0
  macro_context dict   datos crudos para el sintetizador
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

try:
    import yfinance as yf
    import pandas as pd
    import numpy as np
    HAS_DEPS = True
except ImportError:
    HAS_DEPS = False


# ── Mapa sector → sensibilidad macro ────────────────────────────────────────
# (indicador, direccion_favorable, peso)
# direccion_favorable: "up" = sube el indic => favorece al ticker
#                      "down" = baja el indic => favorece al ticker
SECTOR_MACRO_MAP: dict[str, list] = {
    "CVX":  [("wti","up",0.40),("brent","up",0.30),("dxy","down",0.15),("vix","down",0.15)],
    "XOM":  [("wti","up",0.40),("brent","up",0.30),("dxy","down",0.15),("vix","down",0.15)],
    "NVDA": [("sp500","up",0.35),("vix","down",0.25),("tnx","down",0.25),("dxy","down",0.15)],
    "AMD":  [("sp500","up",0.35),("vix","down",0.25),("tnx","down",0.25),("dxy","down",0.15)],
    "MU":   [("sp500","up",0.35),("vix","down",0.25),("tnx","down",0.20),("dxy","down",0.20)],
    "MELI": [("sp500","up",0.30),("vix","down",0.20),("dxy","down",0.35),("tnx","down",0.15)],
    "GGAL": [("dxy","down",0.40),("sp500","up",0.30),("vix","down",0.30)],
    "YPFD": [("wti","up",0.40),("dxy","down",0.25),("sp500","up",0.20),("vix","down",0.15)],
    "_default": [("sp500","up",0.40),("vix","down",0.30),("dxy","down",0.20),("tnx","down",0.10)],
}

MACRO_TICKERS = {
    "wti":   "CL=F",
    "brent": "BZ=F",
    "dxy":   "DX-Y.NYB",
    "vix":   "^VIX",
    "tnx":   "^TNX",
    "sp500": "^GSPC",
    "gold":  "GC=F",
}


@dataclass
class MacroSnapshot:
    fetched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    wti: Optional[float]   = None
    brent: Optional[float] = None
    dxy: Optional[float]   = None
    vix: Optional[float]   = None
    tnx: Optional[float]   = None
    sp500: Optional[float] = None
    gold: Optional[float]  = None
    wti_chg: Optional[float]   = None
    brent_chg: Optional[float] = None
    dxy_chg: Optional[float]   = None
    vix_chg: Optional[float]   = None
    tnx_chg: Optional[float]   = None
    sp500_chg: Optional[float] = None
    gold_chg: Optional[float]  = None
    wti_trend: Optional[float]   = None
    brent_trend: Optional[float] = None
    dxy_trend: Optional[float]   = None
    vix_trend: Optional[float]   = None
    sp500_trend: Optional[float] = None

    def get(self, key: str) -> Optional[float]:
        return getattr(self, key, None)

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if not k.startswith("_")}

    def summary(self) -> str:
        parts = []
        if self.wti:   parts.append(f"WTI ${self.wti:.1f} ({self.wti_chg:+.1f}%)")
        if self.brent: parts.append(f"Brent ${self.brent:.1f} ({self.brent_chg:+.1f}%)")
        if self.dxy:   parts.append(f"DXY {self.dxy:.1f} ({self.dxy_chg:+.1f}%)")
        if self.vix:   parts.append(f"VIX {self.vix:.1f} ({self.vix_chg:+.1f}%)")
        if self.tnx:   parts.append(f"10Y {self.tnx:.2f}%")
        if self.sp500: parts.append(f"SP500 {self.sp500:,.0f} ({self.sp500_chg:+.1f}%)")
        return " | ".join(parts)


def _trend_slope(series: "pd.Series", window: int = 20) -> float:
    s = series.dropna().tail(window)
    if len(s) < 5:
        return 0.0
    x = np.arange(len(s))
    slope = np.polyfit(x, s.values.astype(float), 1)[0]
    rng = float(s.max() - s.min())
    if rng == 0:
        return 0.0
    return float(np.clip(slope * len(s) / rng, -1.0, 1.0))


def fetch_macro() -> MacroSnapshot:
    """Descarga todos los indicadores macro en batch. Gratis, sin API key."""
    if not HAS_DEPS:
        raise ImportError("pip install yfinance pandas numpy")

    snap = MacroSnapshot()
    symbols = list(MACRO_TICKERS.values())
    key_map = {v: k for k, v in MACRO_TICKERS.items()}

    try:
        data = yf.download(symbols, period="1mo", interval="1d",
                           progress=False, auto_adjust=True)
        close = data["Close"]

        for symbol, key in key_map.items():
            try:
                series = close[symbol].dropna() if isinstance(close, pd.DataFrame) else close.dropna()
                if len(series) < 2:
                    continue
                current = float(series.iloc[-1])
                prev    = float(series.iloc[-2])
                chg_pct = (current - prev) / prev * 100 if prev else 0.0

                setattr(snap, key, round(current, 4))
                setattr(snap, f"{key}_chg", round(chg_pct, 4))

                if key in ("wti", "brent", "dxy", "vix", "sp500"):
                    setattr(snap, f"{key}_trend", round(_trend_slope(series, 20), 4))

            except Exception as e:
                logger.debug(f"Macro {key} ({symbol}): {e}")

        logger.info(f"Macro: {snap.summary()}")

    except Exception as e:
        logger.error(f"Error descargando macro batch: {e}")

    return snap


def score_macro_for_ticker(ticker: str, snap: MacroSnapshot) -> tuple[float, list[str]]:
    """
    Calcula macro_score (-1 a +1) para un ticker y lista de razones.
    """
    rules = SECTOR_MACRO_MAP.get(ticker, SECTOR_MACRO_MAP["_default"])
    score = 0.0
    reasons = []

    for indic, direction, weight in rules:
        trend = snap.get(f"{indic}_trend")
        chg   = snap.get(f"{indic}_chg")
        val   = snap.get(indic)

        if trend is None and chg is None:
            continue

        signal = trend if trend is not None else float(np.clip((chg or 0) / 5.0, -1.0, 1.0))
        if direction == "down":
            signal = -signal

        contribution = signal * weight
        score += contribution

        if abs(contribution) >= 0.04:
            val_str = f"{val:.1f}" if val else "N/A"
            chg_str = f"({chg:+.1f}%)" if chg is not None else ""
            icon    = "↑" if signal > 0 else "↓"
            label   = "favorable" if contribution > 0 else "adverso"
            reasons.append(f"Macro {indic.upper()} {val_str} {chg_str} {icon} — {label} para {ticker}")

    return float(np.clip(score, -1.0, 1.0)), reasons


def get_macro_regime(snap: MacroSnapshot) -> dict:
    """Clasifica el regimen macro global en categorias para el sintetizador."""
    vix         = snap.vix or 20.0
    sp500_trend = snap.sp500_trend or 0.0
    wti         = snap.wti or 70.0
    wti_trend   = snap.wti_trend or 0.0
    tnx         = snap.tnx or 4.0
    dxy_trend   = snap.dxy_trend or 0.0

    market = "risk_on"  if (vix < 15 and sp500_trend > 0.1) else \
             "risk_off" if (vix > 25 or sp500_trend < -0.2) else "neutral"

    oil    = "bull"    if (wti > 90 or wti_trend > 0.3) else \
             "bear"    if (wti < 60 or wti_trend < -0.3) else "neutral"

    rates  = "high_hawkish" if tnx > 4.5 else \
             "low_dovish"   if tnx < 3.5 else "neutral"

    dollar = "strengthening" if dxy_trend >  0.2 else \
             "weakening"     if dxy_trend < -0.2 else "neutral"

    return {"market": market, "oil": oil, "rates": rates, "dollar": dollar}
"""
src/analysis/macro.py
Capa macro: global + ARGENTINA completa.
Ahora penaliza CASH_ARS automáticamente y alimenta el optimizer con datos reales locales.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
import requests

try:
    import yfinance as yf
    import pandas as pd
    import numpy as np
    HAS_DEPS = True
except ImportError:
    HAS_DEPS = False

logger = logging.getLogger(__name__)

# ── Mapa sector → sensibilidad macro (ARGENTINA integrada) ─────────────────
SECTOR_MACRO_MAP: dict[str, list] = {
    "CVX":  [("wti","up",0.40),("brent","up",0.30),("dxy","down",0.15),("vix","down",0.15)],
    "XOM":  [("wti","up",0.40),("brent","up",0.30),("dxy","down",0.15),("vix","down",0.15)],
    "NVDA": [("sp500","up",0.35),("vix","down",0.25),("tnx","down",0.25),("dxy","down",0.15)],
    "AMD":  [("sp500","up",0.35),("vix","down",0.25),("tnx","down",0.25),("dxy","down",0.15)],
    "MU":   [("sp500","up",0.35),("vix","down",0.25),("tnx","down",0.20),("dxy","down",0.20)],
    "MELI": [("sp500","up",0.25),("vix","down",0.20),("dxy","down",0.30),("tnx","down",0.15), ("ccl","down",0.10)],
    "GGAL": [("merval","up",0.30),("ccl","down",0.40),("vix","down",0.20),("sp500","up",0.10)],
    "YPFD": [("wti","up",0.35),("merval","up",0.25),("ccl","down",0.25),("vix","down",0.15)],
    "_default": [("sp500","up",0.40),("vix","down",0.30),("dxy","down",0.20),("tnx","down",0.10)],
}

MACRO_TICKERS = {
    "wti": "CL=F", "brent": "BZ=F", "dxy": "DX-Y.NYB", "vix": "^VIX",
    "tnx": "^TNX", "sp500": "^GSPC", "gold": "GC=F", "merval": "^MERV",
}

# ── APIs Argentina gratuitas y estables (2026) ─────────────────────────────
DOLARAPI = "https://dolarapi.com/v1/dolares"
ARGENTINADATOS_RIESGO = "https://argentinadatos.com/v1/finanzas/indices/riesgo-pais/ultimo"
BCRA_RESERVAS = "https://api.bcra.gob.ar/estadisticas/v2.0/reservas"


@dataclass
class MacroSnapshot:
    fetched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    # Globales (igual que antes)
    wti: Optional[float] = None; brent: Optional[float] = None; dxy: Optional[float] = None
    vix: Optional[float] = None; tnx: Optional[float] = None; sp500: Optional[float] = None
    gold: Optional[float] = None
    # Cambios y trends
    wti_chg: Optional[float] = None; brent_chg: Optional[float] = None; dxy_chg: Optional[float] = None
    vix_chg: Optional[float] = None; tnx_chg: Optional[float] = None; sp500_chg: Optional[float] = None
    gold_chg: Optional[float] = None
    wti_trend: Optional[float] = None; brent_trend: Optional[float] = None; dxy_trend: Optional[float] = None
    vix_trend: Optional[float] = None; sp500_trend: Optional[float] = None
    # Argentina
    merval: Optional[float] = None; merval_chg: Optional[float] = None
    ccl: Optional[float] = None; mep: Optional[float] = None
    reservas: Optional[float] = None  # millones USD
    riesgo_pais: Optional[int] = None

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
        if self.merval: parts.append(f"Merval {self.merval:,.0f} ({self.merval_chg:+.1f}%)")
        if self.ccl: parts.append(f"CCL ${self.ccl:.1f}")
        if self.mep: parts.append(f"MEP ${self.mep:.1f}")
        if self.reservas: parts.append(f"Reservas ${self.reservas:,.0f}M")
        if self.riesgo_pais: parts.append(f"Riesgo País {self.riesgo_pais} pb")
        return " | ".join(parts)


def _trend_slope(series: "pd.Series", window: int = 20) -> float:
    """
    Pendiente normalizada de la regresión lineal sobre los últimos `window` períodos.
    Retorna un valor entre -1 y +1 que indica la dirección e intensidad de la tendencia.
    """
    try:
        s = series.dropna().iloc[-window:]
        if len(s) < 5:
            return 0.0
        x   = np.arange(len(s))
        m   = float(np.polyfit(x, s.values, 1)[0])
        ref = float(s.mean())
        return float(np.clip(m / ref * window, -1.0, 1.0)) if ref != 0 else 0.0
    except Exception:
        return 0.0


def _fetch_argentina() -> dict:
    """Datos Argentina vía APIs públicas gratuitas."""
    data = {}
    try:
        # CCL y MEP
        for tipo in ["ccl", "mep"]:
            r = requests.get(f"{DOLARAPI}/{tipo}", timeout=8)
            r.raise_for_status()
            data[tipo] = float(r.json()["venta"])

        # Reservas BCRA
        r = requests.get(BCRA_RESERVAS, timeout=8)
        r.raise_for_status()
        results = r.json().get("results", [])
        if results:
            data["reservas"] = round(float(results[-1]["v"]) / 1_000_000, 0)

        # Riesgo País (ArgentinaDatos)
        r = requests.get(ARGENTINADATOS_RIESGO, timeout=8)
        r.raise_for_status()
        data["riesgo_pais"] = int(r.json()["valor"])

    except Exception as e:
        logger.warning(f"Argentina macro fallback: {e}")
    return data


def fetch_macro() -> MacroSnapshot:
    """Descarga TODO (global + Argentina) en un solo llamado."""
    if not HAS_DEPS:
        raise ImportError("pip install yfinance pandas numpy requests")

    snap = MacroSnapshot()
    # Tu código original de yfinance (lo dejo intacto + agrego Merval)
    symbols = list(MACRO_TICKERS.values())
    key_map = {v: k for k, v in MACRO_TICKERS.items()}

    try:
        data = yf.download(symbols, period="1mo", interval="1d", progress=False, auto_adjust=True)
        close = data["Close"]

        for symbol, key in key_map.items():
            try:
                series = close[symbol].dropna() if isinstance(close, pd.DataFrame) else close.dropna()
                if len(series) < 2: continue
                current = float(series.iloc[-1])
                prev = float(series.iloc[-2])
                chg_pct = (current - prev) / prev * 100 if prev else 0.0

                setattr(snap, key, round(current, 4))
                setattr(snap, f"{key}_chg", round(chg_pct, 4))
                if key in ("wti", "brent", "dxy", "vix", "sp500", "merval"):
                    setattr(snap, f"{key}_trend", round(_trend_slope(series, 20), 4))
            except Exception as e:
                logger.debug(f"Macro {key}: {e}")

    except Exception as e:
        logger.error(f"yfinance error: {e}")

    # Argentina
    ar = _fetch_argentina()
    snap.ccl = ar.get("ccl")
    snap.mep = ar.get("mep")
    snap.reservas = ar.get("reservas")
    snap.riesgo_pais = ar.get("riesgo_pais")

    logger.info(f"Macro completo: {snap.summary()}")
    return snap


def score_macro_for_ticker(ticker: str, snap: MacroSnapshot) -> tuple[float, list[str]]:
    """Score + lógica CASH_ARS automática."""
    if ticker.upper() == "CASH_ARS":
        score = 0.0
        reasons = []
        if snap.ccl and snap.ccl > 1450:
            score -= 0.85
            reasons.append("CCL muy alto → cash ARS se derrite")
        if snap.riesgo_pais and snap.riesgo_pais > 800:
            score -= 0.75
            reasons.append("Riesgo País >800 → fuga de capital")
        if snap.reservas and snap.reservas < 28000:
            score -= 0.55
            reasons.append("Reservas bajas → riesgo cepo")
        return float(np.clip(score, -1.0, 1.0)), reasons

    # Resto de tickers (lógica original + Argentina)
    rules = SECTOR_MACRO_MAP.get(ticker, SECTOR_MACRO_MAP["_default"])
    score = 0.0
    reasons = []

    for indic, direction, weight in rules:
        trend = snap.get(f"{indic}_trend")
        chg   = snap.get(f"{indic}_chg")
        val   = snap.get(indic)

        if trend is None and chg is None: continue

        signal = trend if trend is not None else float(np.clip((chg or 0) / 5.0, -1.0, 1.0))
        if direction == "down": signal = -signal

        contribution = signal * weight
        score += contribution

        if abs(contribution) >= 0.04:
            reasons.append(f"Macro {indic.upper()} {val:.1f} ({chg:+.1f}%) {'↑' if signal > 0 else '↓'} — {'favorable' if contribution > 0 else 'adverso'}")

    return float(np.clip(score, -1.0, 1.0)), reasons


def get_macro_regime(snap: MacroSnapshot) -> dict:
    """Regimen global + alerta Argentina."""
    regime = {  # tu lógica original exacta
        "market": "risk_off" if (snap.vix or 0) > 25 or (snap.sp500_trend or 0) < -0.2 else "neutral",
        "oil": "bull" if (snap.wti or 0) > 80 else "neutral",
        "rates": "neutral",
        "dollar": "strengthening" if (snap.dxy_trend or 0) > 0.2 else "neutral"
    }
    regime["argentina"] = "crítico" if (snap.riesgo_pais or 0) > 800 or (snap.ccl or 0) > 1450 else "estable"
    return regime
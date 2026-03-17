"""
src/analysis/opportunity_screener.py
─────────────────────────────────────
Módulo de análisis de oportunidades externas.
Responde: "Si hoy tuviera que incorporar candidatos nuevos, ¿cuáles son los mejores?"

Arquitectura de 3 capas:
  1. Screener       — filtra el universo por liquidez, tendencia, vol tolerable, RS
  2. Scorer         — score multicapa: técnico + macro + momentum + asimetría
  3. Entry Engine   — clasifica: COMPRABLE_AHORA / EN_VIGILANCIA / DESCARTAR

Inputs:
  - universe:       lista de tickers a evaluar
  - portfolio:      posiciones actuales (para análisis de competencia)
  - macro_snap:     MacroSnapshot ya descargado (reusar del pipeline)
  - period:         historia de precio ("6mo", "1y")

Output:
  - OpportunityReport con candidatos rankeados y contexto completo

Principios:
  - Reutiliza fetch_history(), fetch_macro(), score_macro_for_ticker(),
    fetch_sentiment(), blend_scores() existentes — zero duplicación.
  - El scoring de asimetría es la métrica clave nueva:
    upside_capture / downside_risk — no solo "está lindo técnicamente".
  - La competencia contra cartera actual es explícita: el sistema dice
    si vale más abrir nueva posición o aumentar una existente.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

try:
    import numpy as np
    import pandas as pd
    HAS_DEPS = True
except ImportError:
    HAS_DEPS = False

# ─── Umbrales del screener ─────────────────────────────────────────────────────

# Liquidez
MIN_AVG_VOLUME          = 500_000    # volumen diario promedio mínimo (acciones)
MIN_PRICE_USD           = 3.0        # precio mínimo — evita penny stocks

# Tendencia
MIN_TREND_SCORE         = -0.30      # permite algunos casos bajistas con catalizador
RS_MIN_VS_SPY           = -0.10      # RS relativa vs SPY (20d): no peor que -10%

# Volatilidad
MAX_ANNUAL_VOL          = 1.20       # 120% — filtra activos con vol extrema
MIN_ANNUAL_VOL          = 0.08       # 8%  — filtra activos muertos/ilíquidos

# Distancia a máximos/mínimos (6 meses)
MAX_DIST_FROM_HIGH      = 0.45       # no más de 45% debajo del máximo 6m
MIN_DIST_FROM_LOW       = 0.03       # al menos 3% sobre el mínimo 6m

# Scoring — umbrales para clasificación
SCORE_COMPRABLE         = 0.15       # score mínimo para COMPRABLE_AHORA
SCORE_VIGILANCIA        = 0.07       # score mínimo para EN_VIGILANCIA
CONVICTION_COMPRABLE    = 0.40       # convicción mínima para COMPRABLE_AHORA
ASYMMETRY_MIN           = 1.20       # upside/downside mínimo para "asimétrico"


# ─── Universo curado de CEDEARs disponibles en Cocos ──────────────────────────
# Ordenado por liquidez estimada. Se puede extender.

COCOS_UNIVERSE_DEFAULT: list[str] = [
    # Tech / Semiconductores
    "NVDA", "AMD", "MU", "INTC", "AVGO", "QCOM", "TSM", "AMAT", "LRCX", "KLAC",
    "ASML", "MRVL", "ON", "TXN", "NXPI",
    # Mega tech
    "AAPL", "MSFT", "GOOGL", "META", "AMZN", "TSLA", "NFLX", "CRM", "ADBE",
    # Finanzas
    "JPM", "BAC", "GS", "MS", "BRK-B", "V", "MA", "AXP", "BX", "KKR",
    # Energía
    "CVX", "XOM", "COP", "PSX", "SLB", "HAL", "OXY",
    # Salud / Biotech
    "JNJ", "UNH", "LLY", "PFE", "MRK", "ABBV", "AMGN", "GILD", "REGN",
    # Consumo / Industrial
    "COST", "WMT", "HD", "NKE", "SBUX", "MCD", "CAT", "DE", "BA", "RTX",
    # Latam / Argentina
    "MELI", "NU", "GGAL", "BMA", "SUPV", "LOMA", "CEPU", "YPF",
    # ETF-like (para RS)
    "QQQ", "SPY", "XLE", "XLK", "XLF",
]

# Tickers de referencia para Relative Strength
SPY_TICKER = "SPY"
QQQ_TICKER = "QQQ"


# ─── Dataclasses ──────────────────────────────────────────────────────────────

@dataclass
class ScreenerMetrics:
    """Métricas crudas del screener para un ticker."""
    ticker: str
    price:              float = 0.0
    avg_volume:         float = 0.0
    annual_vol:         float = 0.0
    dist_from_high_6m:  float = 0.0   # negativo = debajo del máximo
    dist_from_low_6m:   float = 0.0   # positivo = sobre el mínimo
    rs_vs_spy_20d:      float = 0.0   # retorno relativo 20 días vs SPY
    rs_vs_qqq_20d:      float = 0.0
    momentum_20d:       float = 0.0   # retorno propio 20 días
    momentum_60d:       float = 0.0
    rsi:                float = 50.0
    above_sma200:       bool  = False
    above_sma50:        bool  = False
    passes_screen:      bool  = False
    fail_reason:        str   = ""


@dataclass
class AsymmetryMetrics:
    """
    Asimetría de la oportunidad: upside capturado vs riesgo a la baja.
    La idea central: no solo 'está lindo' sino 'qué tan asimétrica es la oportunidad'.
    """
    ticker: str
    # Upside: distancia al máximo 6m (potencial de recovery)
    upside_to_6m_high:   float = 0.0
    # Downside: máximo drawdown del último año (riesgo histórico)
    max_drawdown_1y:     float = 0.0
    # ATR como % del precio (volatilidad de corto plazo)
    atr_pct:             float = 0.0
    # Ratio upside/downside_risk — >1.5 es bueno, >2.0 es excelente
    asymmetry_ratio:     float = 0.0
    # Soporte técnico más cercano (estimación)
    support_level:       float = 0.0
    # Stop loss sugerido (en % debajo del precio actual)
    stop_loss_pct:       float = 0.08   # default 8%
    # R/R: cuánto upside por cada 1% de stop
    risk_reward:         float = 0.0


@dataclass
class OpportunityCandidate:
    """Un candidato que pasó el screener y tiene score completo."""
    ticker:             str
    status:             str        # COMPRABLE_AHORA | EN_VIGILANCIA | DESCARTAR
    final_score:        float
    conviction:         float      # 0-1
    # Capas
    tech_score:         float = 0.0
    macro_score:        float = 0.0
    sentiment_score:    float = 0.0
    momentum_score:     float = 0.0
    # Asimetría
    asymmetry:          Optional[AsymmetryMetrics] = None
    asymmetry_label:    str   = ""  # EXCELENTE | BUENA | MODERADA | POBRE
    # Contexto
    screener:           Optional[ScreenerMetrics] = None
    # Por qué entra / qué lo invalida
    entry_reasons:      list[str] = field(default_factory=list)
    invalidation:       list[str] = field(default_factory=list)
    # Sizing y competencia
    sizing_suggested:   float = 0.05   # % del portfolio
    competes_with:      list[str] = field(default_factory=list)  # posiciones actuales similares
    vs_portfolio_note:  str   = ""     # "¿mejor que aumentar MU?"
    # Precio y entrada
    price_usd:          float = 0.0
    entry_zone_low:     float = 0.0
    entry_zone_high:    float = 0.0
    generated_at:       datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class OpportunityReport:
    """Resultado completo del análisis de oportunidades."""
    generated_at:       datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    universe_size:      int   = 0
    screened_count:     int   = 0
    candidates:         list[OpportunityCandidate] = field(default_factory=list)
    # Por categoría
    comprable_ahora:    list[OpportunityCandidate] = field(default_factory=list)
    en_vigilancia:      list[OpportunityCandidate] = field(default_factory=list)
    # Contexto macro para el render
    macro_regime:       dict  = field(default_factory=dict)
    vix_level:          Optional[float] = None
    gate_state:         str   = "NORMAL"


# ─── Screener (Capa 1) ────────────────────────────────────────────────────────

def _compute_screener_metrics(ticker: str, df: "pd.DataFrame",
                               spy_ret_20d: float, qqq_ret_20d: float) -> ScreenerMetrics:
    """Calcula todas las métricas del screener para un ticker."""
    m = ScreenerMetrics(ticker=ticker)

    if df is None or len(df) < 40:
        m.fail_reason = "datos insuficientes"
        return m

    close  = df["Close"].squeeze()
    volume = df["Volume"].squeeze() if "Volume" in df.columns else None

    m.price = float(close.iloc[-1])

    # Volumen promedio
    if volume is not None and len(volume) > 0:
        m.avg_volume = float(volume.tail(20).mean())

    # Volatilidad anual
    rets = close.pct_change().dropna()
    if len(rets) > 20:
        m.annual_vol = float(rets.std() * np.sqrt(252))

    # Distancia a máximos/mínimos 6m (≈126 velas)
    window_6m = close.tail(126)
    high_6m   = float(window_6m.max())
    low_6m    = float(window_6m.min())
    if high_6m > 0:
        m.dist_from_high_6m = (m.price - high_6m) / high_6m   # negativo
    if low_6m > 0:
        m.dist_from_low_6m  = (m.price - low_6m)  / low_6m    # positivo

    # Momentum propio
    if len(close) >= 21:
        m.momentum_20d = float((close.iloc[-1] / close.iloc[-21] - 1))
    if len(close) >= 61:
        m.momentum_60d = float((close.iloc[-1] / close.iloc[-61] - 1))

    # RS vs SPY/QQQ (retorno relativo)
    m.rs_vs_spy_20d = m.momentum_20d - spy_ret_20d
    m.rs_vs_qqq_20d = m.momentum_20d - qqq_ret_20d

    # RSI aproximado
    if len(rets) >= 14:
        gains  = rets.clip(lower=0).tail(14).mean()
        losses = (-rets.clip(upper=0)).tail(14).mean()
        if losses > 0:
            rs_val = gains / losses
            m.rsi  = float(100 - 100 / (1 + rs_val))
        else:
            m.rsi = 100.0

    # Medias móviles
    if len(close) >= 200:
        m.above_sma200 = m.price > float(close.tail(200).mean())
    if len(close) >= 50:
        m.above_sma50  = m.price > float(close.tail(50).mean())

    # ── Filtros ────────────────────────────────────────────────────────────────
    if m.price < MIN_PRICE_USD:
        m.fail_reason = f"precio bajo (${m.price:.2f})"
        return m
    if m.avg_volume > 0 and m.avg_volume < MIN_AVG_VOLUME:
        m.fail_reason = f"volumen bajo ({m.avg_volume:,.0f})"
        return m
    if m.annual_vol > MAX_ANNUAL_VOL:
        m.fail_reason = f"volatilidad extrema ({m.annual_vol:.0%})"
        return m
    if m.annual_vol > 0 and m.annual_vol < MIN_ANNUAL_VOL:
        m.fail_reason = f"activo sin movimiento ({m.annual_vol:.0%} vol)"
        return m
    if m.dist_from_high_6m < -MAX_DIST_FROM_HIGH:
        m.fail_reason = f"demasiado lejos del máximo ({m.dist_from_high_6m:.0%})"
        return m
    if m.dist_from_low_6m < MIN_DIST_FROM_LOW:
        m.fail_reason = f"en mínimos recientes ({m.dist_from_low_6m:.0%} sobre mínimo)"
        return m
    if m.rs_vs_spy_20d < RS_MIN_VS_SPY:
        m.fail_reason = f"RS débil vs SPY ({m.rs_vs_spy_20d:+.1%})"
        return m

    m.passes_screen = True
    return m


def screen_universe(tickers: list[str], period: str = "1y") -> list[ScreenerMetrics]:
    """
    Aplica el screener a todos los tickers.
    Descarga SPY y QQQ primero para calcular RS relativa.
    """
    from src.analysis.technical import fetch_history

    logger.info(f"Screener: evaluando {len(tickers)} tickers...")

    # Referencias
    spy_ret_20d = qqq_ret_20d = 0.0
    for ref_ticker, varname in [(SPY_TICKER, "spy"), (QQQ_TICKER, "qqq")]:
        df_ref = fetch_history(ref_ticker, period=period)
        if df_ref is not None and len(df_ref) >= 21:
            c = df_ref["Close"].squeeze()
            ret = float(c.iloc[-1] / c.iloc[-21] - 1)
            if varname == "spy":
                spy_ret_20d = ret
            else:
                qqq_ret_20d = ret

    logger.info(f"Referencias: SPY 20d={spy_ret_20d:+.1%} | QQQ 20d={qqq_ret_20d:+.1%}")

    results = []
    passed  = 0
    for ticker in tickers:
        if ticker in (SPY_TICKER, QQQ_TICKER):
            continue
        df = fetch_history(ticker, period=period)
        m  = _compute_screener_metrics(ticker, df, spy_ret_20d, qqq_ret_20d)
        results.append(m)
        if m.passes_screen:
            passed += 1
        else:
            logger.debug(f"Screener FAIL {ticker}: {m.fail_reason}")

    logger.info(f"Screener: {passed}/{len(results)} tickers pasaron los filtros")
    return results


# ─── Asimetría (parte del Scorer) ─────────────────────────────────────────────

def _compute_asymmetry(ticker: str, df: "pd.DataFrame",
                        screener: ScreenerMetrics) -> AsymmetryMetrics:
    """Calcula la asimetría upside/downside de la oportunidad."""
    a = AsymmetryMetrics(ticker=ticker)

    if df is None or len(df) < 40:
        return a

    close = df["Close"].squeeze()
    a_price = screener.price

    # Upside: distancia al máximo 6m
    window_6m = close.tail(126)
    a.upside_to_6m_high = max(0.0, float(window_6m.max()) / a_price - 1)

    # Downside: max drawdown 1 año
    window_1y = close.tail(252)
    peak  = window_1y.cummax()
    dd    = (window_1y - peak) / peak
    a.max_drawdown_1y = float(dd.min())

    # ATR como % del precio
    if "High" in df.columns and "Low" in df.columns:
        atr_series = (df["High"].squeeze() - df["Low"].squeeze()).tail(14).mean()
        a.atr_pct  = float(atr_series) / a_price if a_price > 0 else 0.0
    else:
        rets = close.pct_change().dropna()
        a.atr_pct = float(rets.tail(14).std()) * 2

    # Stop loss sugerido: 1.5x ATR o 8%, lo que sea mayor
    a.stop_loss_pct = max(a.atr_pct * 1.5, 0.05)
    a.stop_loss_pct = min(a.stop_loss_pct, 0.18)  # cap 18%

    # Soporte: mínimo de las últimas 20 velas
    a.support_level = float(close.tail(20).min())

    # Asymmetry ratio: upside / stop_loss
    if a.stop_loss_pct > 0:
        a.asymmetry_ratio = a.upside_to_6m_high / a.stop_loss_pct

    # R/R: upside / stop_pct
    if a.stop_loss_pct > 0:
        a.risk_reward = a.upside_to_6m_high / a.stop_loss_pct

    return a


def _asymmetry_label(a: AsymmetryMetrics) -> str:
    r = a.asymmetry_ratio
    if r >= 3.0:  return "EXCELENTE"
    if r >= 2.0:  return "BUENA"
    if r >= 1.2:  return "MODERADA"
    return "POBRE"


# ─── Scorer (Capa 2) ──────────────────────────────────────────────────────────

def _momentum_score(m: ScreenerMetrics) -> float:
    """
    Score de momentum relativo: combina RS vs SPY, momentum propio y RSI.
    Retorna -1 a +1.
    """
    score = 0.0

    # RS vs SPY (peso 40%)
    rs = np.clip(m.rs_vs_spy_20d / 0.10, -1.0, 1.0)
    score += rs * 0.40

    # Momentum propio 20d (peso 30%)
    mo20 = np.clip(m.momentum_20d / 0.10, -1.0, 1.0)
    score += mo20 * 0.30

    # Momentum 60d (peso 20%) — tendencia de más largo plazo
    mo60 = np.clip(m.momentum_60d / 0.15, -1.0, 1.0)
    score += mo60 * 0.20

    # RSI (peso 10%) — penaliza sobrecompra extrema
    if m.rsi > 80:
        score -= 0.10
    elif m.rsi > 70:
        score -= 0.05
    elif m.rsi < 40:
        score += 0.05   # zona de valor

    return float(np.clip(score, -1.0, 1.0))


def _build_entry_reasons(candidate: OpportunityCandidate,
                          screener: ScreenerMetrics,
                          tech_signal: str) -> list[str]:
    """Genera las razones de entrada en lenguaje natural."""
    reasons = []
    m = screener

    if m.rs_vs_spy_20d > 0.05:
        reasons.append(f"RS fuerte vs SPY ({m.rs_vs_spy_20d:+.1%} en 20d)")
    if m.momentum_60d > 0.10:
        reasons.append(f"Momentum de 60 días positivo ({m.momentum_60d:+.1%})")
    if m.above_sma200 and m.above_sma50:
        reasons.append("Por encima de SMA50 y SMA200 — tendencia alcista")
    if candidate.asymmetry and candidate.asymmetry.asymmetry_ratio >= 2.0:
        a = candidate.asymmetry
        reasons.append(
            f"Asimetría favorable: {a.upside_to_6m_high:.1%} upside vs "
            f"{a.stop_loss_pct:.1%} stop sugerido (R/R {a.risk_reward:.1f}x)"
        )
    if candidate.tech_score > 0.10:
        reasons.append(f"Señal técnica positiva (score {candidate.tech_score:+.2f})")
    if candidate.macro_score > 0.08:
        reasons.append(f"Macro favorable para el sector (score {candidate.macro_score:+.2f})")
    if candidate.sentiment_score > 0.10:
        reasons.append("Sentimiento de noticias positivo")
    if m.dist_from_high_6m < -0.15:
        reasons.append(
            f"Retroceso saludable ({m.dist_from_high_6m:.0%} desde máximos) — posible zona de valor"
        )

    return reasons[:5]  # máximo 5 razones


def _build_invalidation(screener: ScreenerMetrics,
                         candidate: OpportunityCandidate) -> list[str]:
    """Qué factores invalidarían la idea."""
    inv = []
    m = screener

    if candidate.asymmetry:
        stop_price = candidate.price_usd * (1 - candidate.asymmetry.stop_loss_pct)
        inv.append(f"Cierre por debajo de ${stop_price:.2f} (stop {candidate.asymmetry.stop_loss_pct:.0%})")

    if m.above_sma50:
        sma50_approx = candidate.price_usd * (1 - abs(m.dist_from_high_6m) * 0.3)
        inv.append("Pérdida de la SMA50 como soporte")

    if m.rs_vs_spy_20d > 0:
        inv.append("Deterioro de la fortaleza relativa vs SPY por más de 2 semanas")

    inv.append("Cambio de régimen macro (VIX > 30 o SP500 en corrección > 10%)")

    if candidate.macro_score < 0:
        inv.append("El contexto macro ya es adverso — requiere señal técnica muy fuerte")

    return inv[:4]


def _sector_competition(ticker: str, portfolio_tickers: list[str]) -> list[str]:
    """
    Detecta qué posiciones actuales compiten con el candidato.
    Retorna lista de tickers del portfolio que son del mismo sector.
    """
    sector_map = {
        "semiconductores": ["NVDA", "AMD", "MU", "INTC", "AVGO", "QCOM", "TSM",
                             "AMAT", "LRCX", "KLAC", "ASML", "MRVL", "ON", "TXN"],
        "mega_tech":       ["AAPL", "MSFT", "GOOGL", "META", "AMZN", "TSLA", "NFLX",
                             "CRM", "ADBE"],
        "energia":         ["CVX", "XOM", "COP", "PSX", "SLB", "HAL", "OXY"],
        "finanzas":        ["JPM", "BAC", "GS", "MS", "V", "MA", "AXP", "BX"],
        "latam":           ["MELI", "NU", "GGAL", "BMA", "YPF", "CEPU"],
        "salud":           ["JNJ", "UNH", "LLY", "PFE", "MRK", "ABBV", "AMGN"],
    }

    ticker_sector = None
    for sector, members in sector_map.items():
        if ticker.upper() in members:
            ticker_sector = sector
            break

    if not ticker_sector:
        return []

    return [
        t for t in portfolio_tickers
        if t.upper() in sector_map.get(ticker_sector, []) and t.upper() != ticker.upper()
    ]


def _vs_portfolio_note(ticker: str, competes_with: list[str],
                        portfolio_scores: dict[str, float],
                        candidate_score: float) -> str:
    """
    Genera la nota comparativa vs cartera actual.
    portfolio_scores: {ticker: final_score} del análisis de cartera.
    """
    if not competes_with:
        return ""

    competing_scores = {t: portfolio_scores.get(t, 0.0) for t in competes_with}
    best_competitor  = max(competing_scores, key=competing_scores.get) if competing_scores else None

    if not best_competitor:
        return ""

    comp_score = competing_scores[best_competitor]

    if candidate_score > comp_score + 0.05:
        return f"Señal más fuerte que {best_competitor} (score {comp_score:+.3f}) — candidato nuevo parece mejor oportunidad"
    elif candidate_score < comp_score - 0.05:
        return f"Mejor aumentar {best_competitor} (score {comp_score:+.3f}) que abrir nueva posición en {ticker}"
    else:
        return f"Señal similar a {best_competitor} — diversificación podría tener sentido"


# ─── Entry Engine (Capa 3) ─────────────────────────────────────────────────────

def _classify_entry(candidate: OpportunityCandidate,
                     screener: ScreenerMetrics,
                     gate_state: str = "NORMAL") -> str:
    """
    Clasifica el candidato: COMPRABLE_AHORA | EN_VIGILANCIA | DESCARTAR.

    COMPRABLE_AHORA:
      - score >= SCORE_COMPRABLE
      - conviction >= CONVICTION_COMPRABLE
      - asimetría >= MODERADA
      - gate no bloqueado
      - precio por encima de soporte técnico

    EN_VIGILANCIA:
      - score >= SCORE_VIGILANCIA (pero no llega a comprable)
      - potencial real pero falta confirmación técnica o el precio no llegó aún

    DESCARTAR:
      - todo lo demás
    """
    if gate_state == "BLOCKED":
        return "EN_VIGILANCIA"  # nunca COMPRABLE en gate bloqueado

    score      = candidate.final_score
    conviction = candidate.conviction
    asym_ok    = candidate.asymmetry and candidate.asymmetry.asymmetry_ratio >= ASYMMETRY_MIN

    above_support = True
    if candidate.asymmetry and candidate.price_usd > 0:
        support = candidate.asymmetry.support_level
        above_support = candidate.price_usd >= support * 0.97  # 3% de margen

    # Condición para COMPRABLE_AHORA
    if (score >= SCORE_COMPRABLE
            and conviction >= CONVICTION_COMPRABLE
            and asym_ok
            and above_support
            and screener.momentum_20d > -0.05):  # no en caída libre
        return "COMPRABLE_AHORA"

    # Condición para EN_VIGILANCIA
    if score >= SCORE_VIGILANCIA:
        return "EN_VIGILANCIA"

    return "DESCARTAR"


def _suggest_sizing(candidate: OpportunityCandidate,
                     portfolio_total_ars: float,
                     existing_allocations: dict[str, float]) -> float:
    """
    Sugiere tamaño de posición considerando:
    - Convicción del candidato
    - Volatilidad del activo
    - Cuánto ya tenemos en el mismo sector
    """
    base = 0.05  # 5% base

    # Ajuste por convicción
    if candidate.conviction >= 0.70:
        base = 0.08
    elif candidate.conviction >= 0.50:
        base = 0.06
    else:
        base = 0.04

    # Ajuste por score
    if candidate.final_score >= 0.25:
        base *= 1.3
    elif candidate.final_score < 0.15:
        base *= 0.7

    # Reducir si ya tenemos mucho en activos similares
    sector_exposure = sum(
        v for t, v in existing_allocations.items()
        if t in candidate.competes_with
    )
    if sector_exposure > 0.30:
        base *= 0.50  # ya tenemos mucho en el sector
    elif sector_exposure > 0.15:
        base *= 0.75

    # Ajuste por asimetría
    if candidate.asymmetry and candidate.asymmetry.asymmetry_ratio >= 3.0:
        base *= 1.20

    return round(float(np.clip(base, 0.02, 0.15)), 4)


# ─── Pipeline principal ───────────────────────────────────────────────────────

def run_opportunity_analysis(
    universe:           list[str],
    portfolio_positions: list[dict],
    macro_snap,                        # MacroSnapshot
    macro_regime:       dict,
    period:             str = "1y",
    no_sentiment:       bool = False,
    portfolio_scores:   dict[str, float] = None,  # {ticker: final_score} del pipeline
    max_candidates:     int = 10,
) -> OpportunityReport:
    """
    Pipeline completo de análisis de oportunidades.

    Args:
        universe:             tickers a evaluar
        portfolio_positions:  posiciones actuales [{ticker, market_value, ...}]
        macro_snap:           MacroSnapshot (ya descargado por run_analysis)
        macro_regime:         dict del régimen actual
        period:               historia de precio
        no_sentiment:         omitir análisis de sentiment (más rápido)
        portfolio_scores:     scores del análisis de cartera (para comparación)
        max_candidates:       máximo de candidatos a devolver

    Returns:
        OpportunityReport
    """
    from src.analysis.technical import fetch_history, analyze_portfolio
    from src.analysis.macro import score_macro_for_ticker
    from src.analysis.sentiment import fetch_sentiment
    from src.analysis.synthesis import blend_scores

    portfolio_tickers = [p.get("ticker", "").upper() for p in portfolio_positions]
    portfolio_scores  = portfolio_scores or {}

    # Calcular exposición actual por ticker
    total_mv = sum(float(p.get("market_value", 0) or 0) for p in portfolio_positions)
    existing_alloc = {
        p.get("ticker", "").upper(): float(p.get("market_value", 0) or 0) / total_mv
        for p in portfolio_positions if total_mv > 0
    }

    # Gate state del régimen
    vix = getattr(macro_snap, "vix", None)
    gate_state = "NORMAL"
    if vix and vix > 38:
        gate_state = "BLOCKED"
    elif vix and vix > 28:
        gate_state = "CAUTIOUS"

    report = OpportunityReport(
        universe_size  = len(universe),
        macro_regime   = macro_regime,
        vix_level      = vix,
        gate_state     = gate_state,
    )

    # ── Capa 1: Screener ──────────────────────────────────────────────────────
    # No analizar lo que ya tenemos en cartera
    candidates_tickers = [t for t in universe if t.upper() not in
                          {p.upper() for p in portfolio_tickers}]

    screener_results = screen_universe(candidates_tickers, period=period)
    passed = [m for m in screener_results if m.passes_screen]
    report.screened_count = len(passed)
    logger.info(f"Opportunity: {len(passed)} candidatos pasaron el screener")

    if not passed:
        return report

    # ── Capa 2: Scoring ───────────────────────────────────────────────────────
    passed_tickers = [m.ticker for m in passed]
    tech_signals   = analyze_portfolio(passed_tickers, period=period)
    tech_map       = {s.ticker: s for s in tech_signals}

    # Sentiment solo para los más prometedores técnicamente (limitar API calls)
    top_tech = sorted(
        [s for s in tech_signals if s.signal == "BUY" or s.strength > 0.35],
        key=lambda s: s.strength,
        reverse=True,
    )[:12]
    top_tech_tickers = {s.ticker for s in top_tech}

    sent_map = {}
    if not no_sentiment:
        for ticker in top_tech_tickers:
            try:
                sent_map[ticker] = fetch_sentiment(ticker)
            except Exception:
                pass

    candidates = []
    for screener_m in passed:
        ticker = screener_m.ticker
        tech   = tech_map.get(ticker)
        if not tech:
            continue

        macro_score, _ = score_macro_for_ticker(ticker, macro_snap)
        sent           = sent_map.get(ticker)
        sent_score     = sent.score if sent else 0.0
        momentum_score = _momentum_score(screener_m)

        # Blend de síntesis (reutiliza el motor existente)
        synth = blend_scores(
            ticker             = ticker,
            technical_signal   = tech.signal,
            technical_strength = tech.strength,
            macro_score        = macro_score,
            risk_position      = {
                "risk_level": "NORMAL", "warnings": [],
                "suggested_pct_adj": 0.05, "current_pct": 0.0,
                "volatility_annual": screener_m.annual_vol, "sharpe": 0.0,
                "action": "MANTENER",
            },
            sentiment_score    = sent_score,
            technical_score_raw= getattr(tech, "score_raw", 0.0),
            skip_sentiment     = no_sentiment,
        )

        # Score final: blend con momentum como capa extra
        # Fórmula: 80% synthesis + 20% momentum (no duplica las capas internas)
        final_score = synth.final_score * 0.80 + momentum_score * 0.20
        conviction  = float(synth.conviction if hasattr(synth, "conviction") else synth.confidence)

        # Asimetría
        df       = fetch_history(ticker, period=period)
        asym     = _compute_asymmetry(ticker, df, screener_m)
        asym_lbl = _asymmetry_label(asym)

        # Competencia con cartera
        competes = _sector_competition(ticker, portfolio_tickers)
        vs_note  = _vs_portfolio_note(ticker, competes, portfolio_scores, final_score)

        cand = OpportunityCandidate(
            ticker          = ticker,
            status          = "DESCARTAR",
            final_score     = round(float(final_score), 4),
            conviction      = round(conviction, 4),
            tech_score      = round(float(getattr(tech, "score_raw", synth.final_score)), 4),
            macro_score     = round(float(macro_score), 4),
            sentiment_score = round(float(sent_score), 4),
            momentum_score  = round(float(momentum_score), 4),
            asymmetry       = asym,
            asymmetry_label = asym_lbl,
            screener        = screener_m,
            price_usd       = screener_m.price,
            competes_with   = competes,
            vs_portfolio_note = vs_note,
        )

        # Zona de entrada: ±1 ATR del precio actual
        if asym.atr_pct > 0:
            cand.entry_zone_low  = round(screener_m.price * (1 - asym.atr_pct * 0.5), 2)
            cand.entry_zone_high = round(screener_m.price * (1 + asym.atr_pct * 0.3), 2)
        else:
            cand.entry_zone_low  = round(screener_m.price * 0.98, 2)
            cand.entry_zone_high = round(screener_m.price * 1.02, 2)

        # ── Capa 3: Entry Engine ───────────────────────────────────────────────
        cand.status         = _classify_entry(cand, screener_m, gate_state)
        cand.entry_reasons  = _build_entry_reasons(cand, screener_m, tech.signal)
        cand.invalidation   = _build_invalidation(screener_m, cand)
        cand.sizing_suggested = _suggest_sizing(cand, total_mv, existing_alloc)

        candidates.append(cand)

    # ── Ranking final ─────────────────────────────────────────────────────────
    # Ordenar: COMPRABLE_AHORA primero, luego por score × conviction × asymmetry
    def _rank_key(c: OpportunityCandidate) -> float:
        priority = {"COMPRABLE_AHORA": 3, "EN_VIGILANCIA": 2, "DESCARTAR": 1}.get(c.status, 0)
        asym_bonus = (c.asymmetry.asymmetry_ratio / 3.0) if c.asymmetry else 0.0
        return priority * 10 + c.final_score * c.conviction * (1 + asym_bonus)

    candidates.sort(key=_rank_key, reverse=True)
    candidates = candidates[:max_candidates]

    report.candidates      = candidates
    report.comprable_ahora = [c for c in candidates if c.status == "COMPRABLE_AHORA"]
    report.en_vigilancia   = [c for c in candidates if c.status == "EN_VIGILANCIA"]

    logger.info(
        f"Opportunity: {len(report.comprable_ahora)} comprables ahora, "
        f"{len(report.en_vigilancia)} en vigilancia"
    )
    return report


# ─── Render para Telegram ─────────────────────────────────────────────────────

def render_opportunity_report(report: OpportunityReport,
                                portfolio_total_ars: float = 0.0) -> str:
    """
    Genera el reporte de oportunidades en HTML para Telegram.
    """
    from html import escape

    def _money(x):
        try:
            return f"${float(x):,.0f} ARS".replace(",", ".")
        except Exception:
            return "$0 ARS"

    def _pct(x):
        try:
            return f"{float(x) * 100:.1f}%"
        except Exception:
            return "0.0%"

    def _bar(x):
        n = max(0, min(5, round(float(x or 0.0) * 5)))
        return "█" * n + "░" * (5 - n)

    h = []

    # Header
    h.append("🔭 <b>RADAR DE OPORTUNIDADES</b>")
    h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    h.append(f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M')} ART")
    h.append(f"🔍 Universo: {report.universe_size} tickers → {report.screened_count} pasaron el screener")

    # Gate
    gate_icons = {"NORMAL": "✅", "CAUTIOUS": "⚠️", "BLOCKED": "🔴"}
    gate_icon  = gate_icons.get(report.gate_state, "⚪")
    h.append(f"{gate_icon} Gate: <b>{report.gate_state}</b>")
    if report.vix_level:
        h.append(f"   VIX: {report.vix_level:.1f}")
    h.append("")

    if not report.candidates:
        h.append("Sin candidatos que cumplan los criterios en este momento.")
        h.append("El screener no encontró oportunidades con suficiente calidad.")
        h.append("")
        h.append("<i>Sistema cuantitativo multicapa — no es asesoramiento financiero</i>")
        return "\n".join(h)

    # ── Comprables ahora ──────────────────────────────────────────────────────
    if report.comprable_ahora:
        h.append(f"🟢 <b>COMPRABLE AHORA ({len(report.comprable_ahora)})</b>")
        h.append("")

        for c in report.comprable_ahora:
            asym_icons = {"EXCELENTE": "🟢🟢", "BUENA": "🟢", "MODERADA": "🟡", "POBRE": "🔴"}
            asym_icon  = asym_icons.get(c.asymmetry_label, "⚪")

            h.append(f"<b>━━ {c.ticker} ━━</b>")
            h.append(
                f"Score: <code>{c.final_score:+.3f}</code> | "
                f"Conv: <b>{round(c.conviction * 100)}%</b> [{_bar(c.conviction)}] | "
                f"Precio: <b>${c.price_usd:.2f}</b>"
            )

            # Capas
            h.append(
                f"<code>técnico {c.tech_score:+.3f} | macro {c.macro_score:+.3f} | "
                f"momentum {c.momentum_score:+.3f} | sent {c.sentiment_score:+.3f}</code>"
            )

            # Asimetría
            if c.asymmetry:
                a = c.asymmetry
                h.append(
                    f"{asym_icon} Asimetría <b>{c.asymmetry_label}</b> — "
                    f"upside {a.upside_to_6m_high:.1%} | stop {a.stop_loss_pct:.0%} | "
                    f"R/R <b>{a.risk_reward:.1f}x</b>"
                )

            # Zona de entrada
            h.append(
                f"📍 Entrada: <b>${c.entry_zone_low:.2f} – ${c.entry_zone_high:.2f}</b>"
            )

            # Sizing
            if portfolio_total_ars > 0:
                ars_amount = c.sizing_suggested * portfolio_total_ars
                h.append(
                    f"💰 Sizing sugerido: <b>{_pct(c.sizing_suggested)}</b> del portfolio "
                    f"≈ {_money(ars_amount)}"
                )
            else:
                h.append(f"💰 Sizing sugerido: <b>{_pct(c.sizing_suggested)}</b> del portfolio")

            # Por qué entra
            if c.entry_reasons:
                h.append("✅ <b>Por qué entra:</b>")
                for r in c.entry_reasons:
                    h.append(f"   • {escape(r)}")

            # Qué lo invalida
            if c.invalidation:
                h.append("🚫 <b>Qué invalida la idea:</b>")
                for inv in c.invalidation[:3]:
                    h.append(f"   • {escape(inv)}")

            # Competencia con cartera
            if c.competes_with:
                h.append(f"⚖️ Compite con: <b>{', '.join(c.competes_with)}</b>")
            if c.vs_portfolio_note:
                h.append(f"   → {escape(c.vs_portfolio_note)}")

            h.append("")

    # ── En vigilancia ─────────────────────────────────────────────────────────
    if report.en_vigilancia:
        h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        h.append(f"👁 <b>EN VIGILANCIA ({len(report.en_vigilancia)})</b>")
        h.append("<i>Potencial real, pero falta confirmación técnica o precio de entrada.</i>")
        h.append("")

        for c in report.en_vigilancia:
            asym_r = c.asymmetry.asymmetry_ratio if c.asymmetry else 0.0
            h.append(
                f"  <b>{c.ticker}</b>: score <code>{c.final_score:+.3f}</code> | "
                f"conv. {round(c.conviction * 100)}% | "
                f"R/R {asym_r:.1f}x | ${c.price_usd:.2f}"
            )
            if c.entry_reasons:
                h.append(f"   └ {escape(c.entry_reasons[0])}")
            if c.competes_with:
                h.append(f"   ⚖️ Compite con: {', '.join(c.competes_with)}")
            if c.vs_portfolio_note:
                h.append(f"   → {escape(c.vs_portfolio_note)}")
            h.append("")

    # Footer
    h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    h.append("<i>Sistema cuantitativo multicapa — no es asesoramiento financiero</i>")

    return "\n".join(h)

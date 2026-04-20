"""
src/analysis/opportunity_screener.py
─────────────────────────────────────
Radar de oportunidades externas — v2.1 (calibración fina).

Cambios respecto a v2.0:
  - _classify(): regla "near-miss R/R" — candidatos con score/conviction/edge
    fuertes que solo fallan levemente en R/R suben a VIGILANCIA_A, no caen a C.
  - _classify(): mejor_que_holding ahora funciona también para SWAP_CANDIDATE,
    no solo NEW_ENTRY. Corrige el bug de YPF cayendo a C.
  - OpportunityCandidate: nuevos campos tech_contradiction y swap_strength.
  - tech_contradiction flag: cuando tech_score < -0.15 y score global positivo.
    El render muestra una nota explícita en lugar de silenciar la contradicción.
  - swap_strength: "FUERTE" / "TÁCTICO" / "MODERADO" según edge.
    HAL (edge ~0.02) sale como TÁCTICO, no igual que un swap de edge 0.10.
  - _rank_key(): edge negativo aplica penalización multiplicativa explícita,
    no solo se ordena por debajo.
  - Render: nota de contradicción técnica, etiqueta de intensidad de swap,
    sección de edge negativo separada del resto de vigilancia.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)

try:
    import numpy as np
    import pandas as pd
    HAS_DEPS = True
except ImportError:
    HAS_DEPS = False


# ══════════════════════════════════════════════════════════════════════════════
# ENUMS Y CONSTANTES
# ══════════════════════════════════════════════════════════════════════════════

class CandidateStatus(str, Enum):
    COMPRABLE_AHORA   = "COMPRABLE_AHORA"
    COMPRA_HABILITADA = "COMPRA_HABILITADA"
    SWAP_CANDIDATO    = "SWAP_CANDIDATO"
    VIGILANCIA_A      = "VIGILANCIA_A"    # casi operable / near-miss
    VIGILANCIA_B      = "VIGILANCIA_B"    # mejor que holdings, sin setup
    VIGILANCIA_C      = "VIGILANCIA_C"    # solo observación pasiva
    NO_OPERABLE       = "NO_OPERABLE"     # R/R inválido o deficiente
    DESCARTAR         = "DESCARTAR"


class TradeType(str, Enum):
    NEW_ENTRY      = "NEW_ENTRY"
    SWAP_CANDIDATE = "SWAP_CANDIDATE"
    ADD_ON         = "ADD_ON"
    WATCHLIST      = "WATCHLIST"


class EdgeLabel(str, Enum):
    FUERTE   = "fuerte"      # > 0.05
    MODERADO = "moderado"    # 0.02 – 0.05
    MARGINAL = "marginal"    # 0 – 0.02
    NEGATIVO = "negativo"    # < 0


# ── Screener ──────────────────────────────────────────────────────────────────
MIN_AVG_VOLUME     = 500_000
MIN_PRICE_USD      = 3.0
RS_MIN_VS_SPY      = -0.10
MAX_ANNUAL_VOL     = 1.20
MIN_ANNUAL_VOL     = 0.08
MAX_DIST_FROM_HIGH = 0.45
MIN_DIST_FROM_LOW  = 0.03

# ── Clasificación — umbrales duros ────────────────────────────────────────────
SCORE_COMPRABLE_DURO   = 0.18
CONVICTION_COMPRABLE   = 0.55
RR_COMPRABLE           = 1.50
EDGE_COMPRABLE         = 0.02

SCORE_HABILITADA       = 0.13
CONVICTION_HABILITADA  = 0.40
RR_HABILITADA          = 1.20

SCORE_SWAP             = 0.10
SWAP_SCORE_DELTA       = 0.04

SCORE_VIGILANCIA       = 0.07

# ── Near-miss R/R: zona entre deficiente y habilitada ─────────────────────────
# Si score/conviction/edge son fuertes pero R/R cae aquí, sube a VIGILANCIA_A
RR_NEAR_MISS_LOWER = 0.80   # = RR_DEFICIENTE
RR_NEAR_MISS_UPPER = 1.20   # = RR_HABILITADA
# Umbrales de "fuerte" para activar near-miss rescue
SCORE_NEAR_MISS_MIN     = 0.15
CONVICTION_NEAR_MISS_MIN = 0.50
EDGE_NEAR_MISS_MIN      = 0.03

# ── R/R alerts ────────────────────────────────────────────────────────────────
RR_INVALIDO    = 0.10
RR_DEFICIENTE  = 0.80
RR_EXCEPCIONAL = 6.00

# ── Technical contradiction ───────────────────────────────────────────────────
# Si tech_score < este umbral y final_score > 0: marcar contradicción
TECH_CONTRADICTION_THRESH = -0.15

# ── Swap strength ─────────────────────────────────────────────────────────────
# Define la intensidad del swap según el edge vs holding
EDGE_SWAP_FUERTE  = 0.07   # edge >= esto → FUERTE
EDGE_SWAP_TACTICO = 0.03   # edge entre este y FUERTE → TÁCTICO, resto → MODERADO

# ── Universo curado ───────────────────────────────────────────────────────────
COCOS_UNIVERSE_DEFAULT: list[str] = [
    "NVDA", "AMD", "MU", "INTC", "AVGO", "QCOM", "TSM", "AMAT", "LRCX", "KLAC",
    "ASML", "MRVL", "ON", "TXN", "NXPI",
    "AAPL", "MSFT", "GOOGL", "META", "AMZN", "TSLA", "NFLX", "CRM", "ADBE",
    "JPM", "BAC", "GS", "MS", "BRK-B", "V", "MA", "AXP", "BX", "KKR",
    "CVX", "XOM", "COP", "PSX", "SLB", "HAL", "OXY",
    "JNJ", "UNH", "LLY", "PFE", "MRK", "ABBV", "AMGN", "GILD", "REGN",
    "COST", "WMT", "HD", "NKE", "SBUX", "MCD", "CAT", "DE", "BA", "RTX",
    "MELI", "NU", "GGAL", "BMA", "SUPV", "LOMA", "CEPU", "YPF",
    "QQQ", "SPY", "XLE", "XLK", "XLF",
]

SPY_TICKER = "SPY"
QQQ_TICKER = "QQQ"

SECTOR_MAP: dict[str, list[str]] = {
    "semiconductores": ["NVDA", "AMD", "MU", "INTC", "AVGO", "QCOM", "TSM",
                        "AMAT", "LRCX", "KLAC", "ASML", "MRVL", "ON", "TXN", "NXPI"],
    "mega_tech":       ["AAPL", "MSFT", "GOOGL", "META", "AMZN", "TSLA", "NFLX",
                        "CRM", "ADBE"],
    "energia":         ["CVX", "XOM", "COP", "PSX", "SLB", "HAL", "OXY"],
    "finanzas":        ["JPM", "BAC", "GS", "MS", "V", "MA", "AXP", "BX", "KKR"],
    "latam":           ["MELI", "NU", "GGAL", "BMA", "YPF", "CEPU"],
    "salud":           ["JNJ", "UNH", "LLY", "PFE", "MRK", "ABBV", "AMGN", "GILD", "REGN"],
    "consumo":         ["COST", "WMT", "HD", "NKE", "SBUX", "MCD"],
    "industrial":      ["CAT", "DE", "BA", "RTX"],
}


# ══════════════════════════════════════════════════════════════════════════════
# DATACLASSES
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class ScreenerMetrics:
    ticker:             str
    price:              float = 0.0
    avg_volume:         float = 0.0
    annual_vol:         float = 0.0
    dist_from_high_6m:  float = 0.0
    dist_from_low_6m:   float = 0.0
    rs_vs_spy_20d:      float = 0.0
    rs_vs_qqq_20d:      float = 0.0
    momentum_20d:       float = 0.0
    momentum_60d:       float = 0.0
    rsi:                float = 50.0
    above_sma200:       bool  = False
    above_sma50:        bool  = False
    passes_screen:      bool  = False
    fail_reason:        str   = ""


@dataclass
class AsymmetryMetrics:
    ticker:              str
    upside_to_6m_high:   float = 0.0
    max_drawdown_1y:     float = 0.0
    atr_pct:             float = 0.0
    asymmetry_ratio:     float = 0.0
    support_level:       float = 0.0
    stop_loss_pct:       float = 0.08
    risk_reward:         float = 0.0
    rr_valid:            bool  = True
    rr_alert:            str   = ""


@dataclass
class EdgeMetrics:
    raw:         float
    label:       EdgeLabel
    vs_ticker:   str
    vs_score:    float
    explanation: str


@dataclass
class OpportunityCandidate:
    ticker:              str
    status:              CandidateStatus
    trade_type:          TradeType
    final_score:         float
    conviction:          float
    # Capas
    tech_score:          float = 0.0
    macro_score:         float = 0.0
    sentiment_score:     float = 0.0
    momentum_score:      float = 0.0
    # Asimetría y edge
    asymmetry:           Optional[AsymmetryMetrics] = None
    asymmetry_label:     str   = ""
    edge:                Optional[EdgeMetrics] = None
    # Reasoning
    entry_reasons:       list[str] = field(default_factory=list)
    invalidation:        list[str] = field(default_factory=list)
    why_not_now:         str   = ""
    action_concreta:     str   = ""
    # Competencia
    competes_with:       list[str] = field(default_factory=list)
    swap_vs:             str   = ""
    vs_portfolio_note:   str   = ""
    # Nuevos flags de calibración fina
    tech_contradiction:  bool  = False   # tech muy negativo con score positivo
    swap_strength:       str   = ""      # FUERTE | TÁCTICO | MODERADO
    near_miss_rr:        bool  = False   # salvado por regla near-miss
    # Sizing
    sizing_base:         float = 0.05
    sizing_adjusted:     float = 0.05
    sizing_adjustment_note: str = ""
    sizing_suggested:    float = 0.05
    # Precio
    price_usd:           float = 0.0
    entry_zone_low:      float = 0.0
    entry_zone_high:     float = 0.0
    # Alertas
    alerts:              list[str] = field(default_factory=list)
    generated_at:        datetime  = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class OpportunityReport:
    generated_at:        datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    universe_size:       int   = 0
    screened_count:      int   = 0
    gate_state:          str   = "NORMAL"
    vix_level:           Optional[float] = None
    macro_regime:        dict  = field(default_factory=dict)
    candidates:          list[OpportunityCandidate] = field(default_factory=list)
    comprable_ahora:     list[OpportunityCandidate] = field(default_factory=list)
    compra_habilitada:   list[OpportunityCandidate] = field(default_factory=list)
    swap_candidatos:     list[OpportunityCandidate] = field(default_factory=list)
    en_vigilancia:       list[OpportunityCandidate] = field(default_factory=list)
    no_operables:        list[OpportunityCandidate] = field(default_factory=list)


# ══════════════════════════════════════════════════════════════════════════════
# CAPA 1 — SCREENER
# ══════════════════════════════════════════════════════════════════════════════

def _compute_screener_metrics(
    ticker: str,
    df: "pd.DataFrame",
    spy_ret_20d: float,
    qqq_ret_20d: float,
) -> ScreenerMetrics:
    m = ScreenerMetrics(ticker=ticker)
    if df is None or len(df) < 40:
        m.fail_reason = "datos insuficientes"
        return m

    close  = df["Close"].squeeze()
    volume = df["Volume"].squeeze() if "Volume" in df.columns else None

    m.price = float(close.iloc[-1])
    if volume is not None and len(volume) > 0:
        m.avg_volume = float(volume.tail(20).mean())

    rets = close.pct_change().dropna()
    if len(rets) > 20:
        m.annual_vol = float(rets.std() * np.sqrt(252))

    window_6m = close.tail(126)
    high_6m   = float(window_6m.max())
    low_6m    = float(window_6m.min())
    if high_6m > 0:
        m.dist_from_high_6m = (m.price - high_6m) / high_6m
    if low_6m > 0:
        m.dist_from_low_6m  = (m.price - low_6m) / low_6m

    if len(close) >= 21:
        m.momentum_20d = float(close.iloc[-1] / close.iloc[-21] - 1)
    if len(close) >= 61:
        m.momentum_60d = float(close.iloc[-1] / close.iloc[-61] - 1)

    m.rs_vs_spy_20d = m.momentum_20d - spy_ret_20d
    m.rs_vs_qqq_20d = m.momentum_20d - qqq_ret_20d

    if len(rets) >= 14:
        gains  = rets.clip(lower=0).tail(14).mean()
        losses = (-rets.clip(upper=0)).tail(14).mean()
        m.rsi  = float(100 - 100 / (1 + gains / losses)) if losses > 0 else 100.0

    if len(close) >= 200:
        m.above_sma200 = m.price > float(close.tail(200).mean())
    if len(close) >= 50:
        m.above_sma50  = m.price > float(close.tail(50).mean())

    if m.price < MIN_PRICE_USD:
        m.fail_reason = f"precio bajo (${m.price:.2f})"
        return m
    if m.avg_volume > 0 and m.avg_volume < MIN_AVG_VOLUME:
        m.fail_reason = f"volumen bajo ({m.avg_volume:,.0f})"
        return m
    if m.annual_vol > MAX_ANNUAL_VOL:
        m.fail_reason = f"volatilidad extrema ({m.annual_vol:.0%})"
        return m
    if 0 < m.annual_vol < MIN_ANNUAL_VOL:
        m.fail_reason = f"activo sin movimiento ({m.annual_vol:.0%})"
        return m
    if m.dist_from_high_6m < -MAX_DIST_FROM_HIGH:
        m.fail_reason = f"caída libre ({m.dist_from_high_6m:.0%} desde máximos)"
        return m
    if m.dist_from_low_6m < MIN_DIST_FROM_LOW:
        m.fail_reason = f"en mínimos recientes ({m.dist_from_low_6m:.0%})"
        return m
    if m.rs_vs_spy_20d < RS_MIN_VS_SPY:
        m.fail_reason = f"RS débil vs SPY ({m.rs_vs_spy_20d:+.1%})"
        return m

    m.passes_screen = True
    return m


def screen_universe(tickers: list[str], period: str = "1y") -> list[ScreenerMetrics]:
    from src.analysis.technical import fetch_history

    logger.info(f"Screener: evaluando {len(tickers)} tickers...")

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


# ══════════════════════════════════════════════════════════════════════════════
# CAPA 2 — SCORER
# ══════════════════════════════════════════════════════════════════════════════

def _compute_asymmetry(
    ticker: str,
    df: "pd.DataFrame",
    screener: ScreenerMetrics,
) -> AsymmetryMetrics:
    a = AsymmetryMetrics(ticker=ticker)
    if df is None or len(df) < 40:
        a.rr_valid = False
        a.rr_alert = "datos insuficientes para calcular R/R"
        return a

    close   = df["Close"].squeeze()
    a_price = screener.price
    if a_price <= 0:
        a.rr_valid = False
        a.rr_alert = "precio inválido"
        return a

    window_6m = close.tail(126)
    a.upside_to_6m_high = max(0.0, float(window_6m.max()) / a_price - 1)

    window_1y = close.tail(252)
    peak      = window_1y.cummax()
    dd        = (window_1y - peak) / peak
    a.max_drawdown_1y = float(dd.min())

    if "High" in df.columns and "Low" in df.columns:
        atr_series = (df["High"].squeeze() - df["Low"].squeeze()).tail(14).mean()
        a.atr_pct  = float(atr_series) / a_price if a_price > 0 else 0.0
    else:
        rets      = close.pct_change().dropna()
        a.atr_pct = float(rets.tail(14).std()) * 2

    a.stop_loss_pct = max(a.atr_pct * 1.5, 0.05)
    a.stop_loss_pct = min(a.stop_loss_pct, 0.18)
    a.support_level = float(close.tail(20).min())

    if a.stop_loss_pct > 0 and a.upside_to_6m_high > 0:
        a.asymmetry_ratio = a.upside_to_6m_high / a.stop_loss_pct
        a.risk_reward     = a.upside_to_6m_high / a.stop_loss_pct
    else:
        a.asymmetry_ratio = 0.0
        a.risk_reward     = 0.0
        a.rr_valid        = False
        if a.upside_to_6m_high <= 0.001:
            a.rr_alert = "precio cerca o en máximos — upside mínimo, target demasiado cercano"
        else:
            a.rr_alert = "stop no calculable"
        return a

    rr = a.risk_reward
    if rr <= RR_INVALIDO:
        a.rr_valid = False
        a.rr_alert = f"R/R {rr:.1f}x — setup inválido"
    elif rr <= RR_DEFICIENTE:
        a.rr_valid = False
        a.rr_alert = f"R/R {rr:.1f}x — asimetría deficiente, no operable"
    elif rr >= RR_EXCEPCIONAL:
        a.rr_alert = f"R/R {rr:.1f}x — excepcional, verificar robustez del target/stop"

    return a


def _asymmetry_label(a: AsymmetryMetrics) -> str:
    if not a.rr_valid:
        return "INVÁLIDA"
    r = a.asymmetry_ratio
    if r >= 3.0: return "EXCELENTE"
    if r >= 2.0: return "BUENA"
    if r >= 1.2: return "MODERADA"
    return "POBRE"


def _momentum_score(m: ScreenerMetrics) -> float:
    score = 0.0
    rs    = np.clip(m.rs_vs_spy_20d / 0.10, -1.0, 1.0)
    score += rs * 0.40
    mo20  = np.clip(m.momentum_20d / 0.10, -1.0, 1.0)
    score += mo20 * 0.30
    mo60  = np.clip(m.momentum_60d / 0.15, -1.0, 1.0)
    score += mo60 * 0.20
    if m.rsi > 80:   score -= 0.10
    elif m.rsi > 70: score -= 0.05
    elif m.rsi < 40: score += 0.05
    return float(np.clip(score, -1.0, 1.0))


def _compute_edge(
    candidate_score:      float,
    competes_with:        list[str],
    portfolio_scores:     dict[str, float],
    best_portfolio_score: float,
) -> EdgeMetrics:
    raw_edge  = candidate_score - best_portfolio_score
    vs_ticker = ""
    vs_score  = best_portfolio_score

    if competes_with and portfolio_scores:
        best_competing = max(
            [(t, portfolio_scores.get(t, 0.0)) for t in competes_with],
            key=lambda x: x[1],
            default=("", 0.0),
        )
        vs_ticker = best_competing[0]
        vs_score  = best_competing[1]
        raw_edge  = candidate_score - vs_score

    if raw_edge > 0.05:
        label = EdgeLabel.FUERTE
    elif raw_edge > 0.02:
        label = EdgeLabel.MODERADO
    elif raw_edge >= 0:
        label = EdgeLabel.MARGINAL
    else:
        label = EdgeLabel.NEGATIVO

    ref_name = vs_ticker or "cartera"
    explanation = (
        f"Mejora esperada vs mantener {ref_name}: {raw_edge:+.1%} ({label.value})"
    )
    return EdgeMetrics(
        raw=round(raw_edge, 4), label=label,
        vs_ticker=vs_ticker, vs_score=round(vs_score, 4),
        explanation=explanation,
    )


def _sector_competition(ticker: str, portfolio_tickers: list[str]) -> list[str]:
    ticker_sector = None
    for sector, members in SECTOR_MAP.items():
        if ticker.upper() in members:
            ticker_sector = sector
            break
    if not ticker_sector:
        return []
    return [
        t for t in portfolio_tickers
        if t.upper() in SECTOR_MAP.get(ticker_sector, []) and t.upper() != ticker.upper()
    ]


def _best_swap_target(
    ticker: str,
    competes_with: list[str],
    portfolio_scores: dict[str, float],
    candidate_score: float,
) -> str:
    if not competes_with or not portfolio_scores:
        return ""
    weakest = min(
        [(t, portfolio_scores.get(t, 0.0)) for t in competes_with],
        key=lambda x: x[1],
        default=("", 0.0),
    )
    if candidate_score > weakest[1] + SWAP_SCORE_DELTA:
        return weakest[0]
    return ""


def _vs_portfolio_note(
    ticker: str,
    competes_with: list[str],
    portfolio_scores: dict[str, float],
    candidate_score: float,
) -> str:
    if not competes_with:
        return ""
    competing_scores = {t: portfolio_scores.get(t, 0.0) for t in competes_with}
    best_competitor  = max(competing_scores, key=competing_scores.get)
    comp_score       = competing_scores[best_competitor]

    if candidate_score > comp_score + 0.05:
        return (
            f"Señal más fuerte que {best_competitor} (score {comp_score:+.3f}) — "
            f"candidato nuevo parece mejor oportunidad"
        )
    elif candidate_score < comp_score - 0.05:
        return (
            f"Mejor aumentar {best_competitor} (score {comp_score:+.3f}) que "
            f"abrir nueva posición en {ticker}"
        )
    return f"Señal similar a {best_competitor} — diversificación podría tener sentido"


def _compute_swap_strength(edge_raw: float) -> str:
    """Intensidad del swap según edge vs holding competidor."""
    if edge_raw >= EDGE_SWAP_FUERTE:
        return "FUERTE"
    if edge_raw >= EDGE_SWAP_TACTICO:
        return "TÁCTICO"
    return "MODERADO"


# ══════════════════════════════════════════════════════════════════════════════
# CAPA 3 — CLASSIFIER (calibración fina)
# ══════════════════════════════════════════════════════════════════════════════

def _classify(
    score:            float,
    conviction:       float,
    asym:             AsymmetryMetrics,
    edge:             EdgeMetrics,
    screener:         ScreenerMetrics,
    competes_with:    list[str],
    portfolio_scores: dict[str, float],
    gate_state:       str,
    tech_score_raw:   float = 0.0,
) -> tuple[CandidateStatus, TradeType, str, str, bool]:
    """
    Clasifica el candidato.

    Returns:
        (status, trade_type, action_concreta, why_not_now, near_miss_rr)

    Cambios de calibración fina:
    ─────────────────────────────
    1. near-miss R/R rescue:
       Si score >= SCORE_NEAR_MISS_MIN AND conviction >= CONVICTION_NEAR_MISS_MIN
       AND edge >= EDGE_NEAR_MISS_MIN AND R/R está en zona near-miss
       (RR_DEFICIENTE < rr < RR_HABILITADA) → VIGILANCIA_A en lugar de C.
       Corrige: YPF con score/conviction/edge fuertes que falla solo por poco en R/R.

    2. mejor_que_holding ahora funciona para SWAP_CANDIDATE también.
       Corrige: YPF (compite con MELI) caía a C porque el check era
       `trade_type == NEW_ENTRY` exclusivamente.

    3. Edge negativo no bloquea VIGILANCIA_A ni B — solo reduce prioridad en ranking.
       El candidato con edge < 0 puede subir a A si tiene score/conviction fuertes.

    4. El gate CAUTIOUS baja COMPRABLE_AHORA a COMPRA_HABILITADA, no bloquea todo.
    """
    rr       = asym.risk_reward if asym else 0.0
    rr_valid = asym.rr_valid if asym else False

    # ── R/R inválido → NO_OPERABLE ────────────────────────────────────────────
    # Excepción: si R/R está en zona near-miss y candidato es fuerte → VIGILANCIA_A
    if not rr_valid:
        rr_raw = asym.risk_reward if asym else 0.0
        near_miss = (
            RR_NEAR_MISS_LOWER < rr_raw < RR_NEAR_MISS_UPPER
            and score >= SCORE_NEAR_MISS_MIN
            and conviction >= CONVICTION_NEAR_MISS_MIN
            and edge.raw >= EDGE_NEAR_MISS_MIN
        )
        if near_miss:
            # Saltar a VIGILANCIA_A — el R/R es deficiente pero el resto es fuerte
            reason = (
                f"R/R {rr_raw:.1f}x casi suficiente (mín {RR_HABILITADA:.1f}x) — "
                f"esperar mejor asimetría o pullback para mejorar stop/target"
            )
            trade_type = _determine_trade_type(competes_with, portfolio_scores, score)
            action = (
                "Esperar mejor asimetría" if trade_type != TradeType.SWAP_CANDIDATE
                else f"Swap posible si mejora R/R — reducir {_best_swap_target('', competes_with, portfolio_scores, score)} primero"
            )
            return CandidateStatus.VIGILANCIA_A, trade_type, action, reason, True

        reason = asym.rr_alert if asym else "R/R no calculable"
        return CandidateStatus.NO_OPERABLE, TradeType.WATCHLIST, "No operar", reason, False

    if gate_state == "BLOCKED":
        trade_type = _determine_trade_type(competes_with, portfolio_scores, score)
        return (
            CandidateStatus.VIGILANCIA_C, TradeType.WATCHLIST,
            "Esperar — gate bloqueado",
            f"Gate {gate_state} activo — no se habilitan nuevas entradas",
            False,
        )

    trade_type = _determine_trade_type(competes_with, portfolio_scores, score)
    swap_target = _best_swap_target("", competes_with, portfolio_scores, score)

    # ── COMPRABLE_AHORA ───────────────────────────────────────────────────────
    # Gate CAUTIOUS baja la categoría pero no bloquea completamente
    gate_ok = gate_state == "NORMAL"
    gate_reduced = gate_state == "CAUTIOUS"

    if (score >= SCORE_COMPRABLE_DURO
            and conviction >= CONVICTION_COMPRABLE
            and rr >= RR_COMPRABLE
            and edge.raw >= EDGE_COMPRABLE
            and screener.momentum_20d > -0.05
            and gate_ok):
        action = (
            f"Swap vs {swap_target}" if trade_type == TradeType.SWAP_CANDIDATE
            else "Abrir posición"
        )
        return CandidateStatus.COMPRABLE_AHORA, trade_type, action, "", False

    # Con gate CAUTIOUS, COMPRABLE_AHORA se degrada a COMPRA_HABILITADA
    if (score >= SCORE_COMPRABLE_DURO
            and conviction >= CONVICTION_COMPRABLE
            and rr >= RR_COMPRABLE
            and edge.raw >= EDGE_COMPRABLE
            and screener.momentum_20d > -0.05
            and gate_reduced):
        action = (
            f"Swap posible cuando mejore el régimen — vs {swap_target}"
            if trade_type == TradeType.SWAP_CANDIDATE
            else "Habilitada cuando mejore el régimen"
        )
        return (
            CandidateStatus.COMPRA_HABILITADA, trade_type, action,
            f"Gate {gate_state} activo — reducido de COMPRABLE a HABILITADA", False,
        )

    # ── SWAP_CANDIDATO ────────────────────────────────────────────────────────
    if (score >= SCORE_SWAP
            and trade_type == TradeType.SWAP_CANDIDATE
            and rr >= RR_HABILITADA
            and gate_state != "BLOCKED"):
        action = f"Swap vs {swap_target} — reducir antes de comprar"
        why_not = (
            "" if score >= SCORE_HABILITADA
            else f"score {score:+.3f} insuficiente para compra directa — solo swap"
        )
        return CandidateStatus.SWAP_CANDIDATO, trade_type, action, why_not, False

    # ── COMPRA_HABILITADA ─────────────────────────────────────────────────────
    if (score >= SCORE_HABILITADA
            and conviction >= CONVICTION_HABILITADA
            and rr >= RR_HABILITADA
            and gate_ok):
        why_not_parts = []
        if edge.raw < EDGE_COMPRABLE:
            why_not_parts.append(f"edge {edge.raw:+.3f} marginal vs cartera")
        if conviction < CONVICTION_COMPRABLE:
            why_not_parts.append(f"convicción {conviction:.0%} < umbral")
        if rr < RR_COMPRABLE:
            why_not_parts.append(f"R/R {rr:.1f}x < umbral duro {RR_COMPRABLE:.1f}x")
        action = (
            "Compra habilitada (no prioritaria)" if not swap_target
            else f"Swap vs {swap_target} si se libera capital"
        )
        return (
            CandidateStatus.COMPRA_HABILITADA, trade_type, action,
            "; ".join(why_not_parts), False,
        )

    # ── VIGILANCIA ────────────────────────────────────────────────────────────
    if score >= SCORE_VIGILANCIA:
        falta_score      = score < SCORE_HABILITADA
        falta_rr         = rr < RR_HABILITADA
        falta_conviction = conviction < CONVICTION_HABILITADA

        why_not_parts = []
        if falta_rr:
            why_not_parts.append(f"R/R {rr:.1f}x insuficiente (mín {RR_HABILITADA:.1f}x)")
        if falta_score:
            why_not_parts.append(f"score {score:+.3f} < umbral operativo")
        if falta_conviction:
            why_not_parts.append(f"convicción {conviction:.0%} < umbral")
        if edge.raw < 0:
            why_not_parts.append(
                f"no supera a {edge.vs_ticker or 'cartera actual'} "
                f"(edge {edge.raw:+.3f})"
            )
        why_not = "; ".join(why_not_parts) or "falta confirmación técnica"

        # ── Subnivel A: solo le falta un catalizador ──────────────────────────
        # Condición ampliada: también aplica para SWAP_CANDIDATE (fix YPF)
        un_solo_freno = sum([falta_score, falta_rr, falta_conviction]) <= 1
        if un_solo_freno and not (falta_score and falta_rr):
            action = (
                "Esperar pullback hacia soporte o mejora de R/R"
                if falta_rr else "Esperar confirmación técnica"
            )
            return CandidateStatus.VIGILANCIA_A, trade_type, action, why_not, False

        # ── Subnivel B: mejor que holdings pero sin setup completo ─────────────
        # Corregido: funciona también para SWAP_CANDIDATE, no solo NEW_ENTRY
        es_mejor_que_holding = edge.raw > 0.04
        if es_mejor_que_holding and not falta_score:
            action = "Esperar mejor asimetría o setup técnico"
            return CandidateStatus.VIGILANCIA_B, trade_type, action, why_not, False

        # ── Subnivel C: solo observación ──────────────────────────────────────
        return CandidateStatus.VIGILANCIA_C, trade_type, "Solo observar", why_not, False

    return CandidateStatus.DESCARTAR, TradeType.WATCHLIST, "No operar", "bajo umbral mínimo", False


def _determine_trade_type(
    competes_with:    list[str],
    portfolio_scores: dict[str, float],
    score:            float,
) -> TradeType:
    """Helper extraído de _classify para reutilizar en near-miss."""
    swap_target = _best_swap_target("", competes_with, portfolio_scores, score)
    if competes_with and swap_target:
        return TradeType.SWAP_CANDIDATE
    if competes_with:
        return TradeType.WATCHLIST
    return TradeType.NEW_ENTRY


# ══════════════════════════════════════════════════════════════════════════════
# CAPA 4 — ENTRY ENGINE
# ══════════════════════════════════════════════════════════════════════════════

def _build_entry_reasons(
    c: OpportunityCandidate,
    screener: ScreenerMetrics,
) -> list[str]:
    reasons = []
    m = screener
    if m.rs_vs_spy_20d > 0.05:
        reasons.append(f"RS fuerte vs SPY ({m.rs_vs_spy_20d:+.1%} en 20d)")
    if m.momentum_60d > 0.10:
        reasons.append(f"Momentum de 60 días positivo ({m.momentum_60d:+.1%})")
    if m.above_sma200 and m.above_sma50:
        reasons.append("Por encima de SMA50 y SMA200 — tendencia alcista")
    if c.asymmetry and c.asymmetry.rr_valid and c.asymmetry.asymmetry_ratio >= 2.0:
        a = c.asymmetry
        reasons.append(
            f"Asimetría favorable: {a.upside_to_6m_high:.1%} upside vs "
            f"{a.stop_loss_pct:.0%} stop (R/R {a.risk_reward:.1f}x)"
        )
    if c.tech_score > 0.10:
        reasons.append(f"Señal técnica positiva (score {c.tech_score:+.2f})")
    if c.macro_score > 0.08:
        reasons.append(f"Macro favorable para el sector (score {c.macro_score:+.2f})")
    if m.dist_from_high_6m < -0.15:
        reasons.append(f"Retroceso saludable ({m.dist_from_high_6m:.0%} desde máximos)")
    return reasons[:5]


def _build_invalidation(
    screener: ScreenerMetrics,
    c: OpportunityCandidate,
) -> list[str]:
    inv = []
    if c.asymmetry and c.asymmetry.rr_valid:
        stop_price = c.price_usd * (1 - c.asymmetry.stop_loss_pct)
        inv.append(f"Cierre por debajo de ${stop_price:.2f} (stop {c.asymmetry.stop_loss_pct:.0%})")
    if screener.above_sma50:
        inv.append("Pérdida de la SMA50 como soporte")
    if screener.rs_vs_spy_20d > 0:
        inv.append("Deterioro de la RS vs SPY por más de 2 semanas")
    inv.append("VIX > 30 o SP500 en corrección > 10%")
    return inv[:4]


def _suggest_sizing(
    c: OpportunityCandidate,
    existing_allocations: dict[str, float],
) -> tuple[float, float, str]:
    score      = c.final_score
    conviction = c.conviction
    asym_r     = c.asymmetry.asymmetry_ratio if (c.asymmetry and c.asymmetry.rr_valid) else 0.5

    base = float(np.clip(score * 25.0, 2.0, 9.0)) / 100.0

    if conviction >= 0.70:   conv_mult = 1.30
    elif conviction >= 0.55: conv_mult = 1.10
    elif conviction >= 0.40: conv_mult = 1.00
    elif conviction >= 0.25: conv_mult = 0.75
    else:                    conv_mult = 0.50

    if asym_r >= 4.0:   asym_mult = 1.35
    elif asym_r >= 3.0: asym_mult = 1.20
    elif asym_r >= 2.0: asym_mult = 1.05
    elif asym_r >= 1.2: asym_mult = 1.00
    else:               asym_mult = 0.70

    sector_exposure = sum(
        v for t, v in existing_allocations.items()
        if t in c.competes_with
    )

    note_parts = []
    if sector_exposure > 0.30:
        sector_penalty = 0.40
        note_parts.append(
            f"penalizado por concentración sectorial ({sector_exposure:.0%}) — "
            f"requiere recorte previo en {', '.join(c.competes_with[:2])}"
        )
    elif sector_exposure > 0.20:
        sector_penalty = 0.65
        note_parts.append(f"reducido por exposición sectorial ({sector_exposure:.0%})")
    elif sector_exposure > 0.10:
        sector_penalty = 0.85
        note_parts.append(f"ajuste leve por solapamiento sectorial ({sector_exposure:.0%})")
    else:
        sector_penalty = 1.00

    sizing_base = round(float(np.clip(base * conv_mult * asym_mult, 0.02, 0.15)), 4)
    sizing_adj  = round(float(np.clip(sizing_base * sector_penalty, 0.02, 0.15)), 4)
    adj_note    = "; ".join(note_parts) if note_parts else ""

    return sizing_base, sizing_adj, adj_note


# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

def run_opportunity_analysis(
    universe:            list[str],
    portfolio_positions: list[dict],
    macro_snap,
    macro_regime:        dict,
    period:              str  = "1y",
    no_sentiment:        bool = False,
    portfolio_scores:    dict[str, float] = None,
    max_candidates:      int  = 10,
    min_score:           float = 0.0,
    min_rr:              float = 0.0,
    exclude_portfolio:   bool = True,
) -> OpportunityReport:
    from src.analysis.technical import fetch_history, analyze_portfolio
    from src.analysis.macro import score_macro_for_ticker
    from src.analysis.sentiment import fetch_sentiment
    from src.analysis.synthesis import blend_scores

    portfolio_tickers = [p.get("ticker", "").upper() for p in portfolio_positions]
    portfolio_scores  = portfolio_scores or {}

    total_mv = sum(float(p.get("market_value", 0) or 0) for p in portfolio_positions)
    existing_alloc = {
        p.get("ticker", "").upper(): float(p.get("market_value", 0) or 0) / total_mv
        for p in portfolio_positions if total_mv > 0
    }

    vix        = getattr(macro_snap, "vix", None)
    gate_state = "NORMAL"
    if vix and vix > 38:   gate_state = "BLOCKED"
    elif vix and vix > 28: gate_state = "CAUTIOUS"

    best_portfolio_score = max(portfolio_scores.values()) if portfolio_scores else 0.0

    report = OpportunityReport(
        universe_size=len(universe),
        macro_regime=macro_regime,
        vix_level=vix,
        gate_state=gate_state,
    )

    screener_results = screen_universe(universe, period=period)
    passed = [m for m in screener_results if m.passes_screen]
    report.screened_count = len(passed)
    logger.info(f"Opportunity: {len(passed)} candidatos pasaron el screener")
    if not passed:
        return report

    passed_tickers = [m.ticker for m in passed]
    tech_signals   = analyze_portfolio(passed_tickers, period=period)
    tech_map       = {s.ticker: s for s in tech_signals}

    top_tech_tickers = {
        s.ticker for s in sorted(
            [s for s in tech_signals if s.signal == "BUY" or s.strength > 0.35],
            key=lambda s: s.strength, reverse=True,
        )[:12]
    }

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
        momentum_s     = _momentum_score(screener_m)

        synth = blend_scores(
            ticker              = ticker,
            technical_signal    = tech.signal,
            technical_strength  = tech.strength,
            macro_score         = macro_score,
            risk_position       = {
                "risk_level": "NORMAL", "warnings": [],
                "suggested_pct_adj": 0.05, "current_pct": 0.0,
                "volatility_annual": screener_m.annual_vol, "sharpe": 0.0,
                "action": "MANTENER",
            },
            sentiment_score     = sent_score,
            technical_score_raw = getattr(tech, "score_raw", 0.0),
            skip_sentiment      = no_sentiment,
        )

        final_score = synth.final_score * 0.80 + momentum_s * 0.20
        conviction  = float(
            synth.conviction if hasattr(synth, "conviction") else synth.confidence
        )
        if conviction > 1.0:
            conviction /= 100.0

        # Tech score raw para detección de contradicción
        tech_score_raw = float(getattr(tech, "score_raw", 0.0))

        df       = fetch_history(ticker, period=period)
        asym     = _compute_asymmetry(ticker, df, screener_m)
        asym_lbl = _asymmetry_label(asym)

        competes  = _sector_competition(ticker, portfolio_tickers)
        edge      = _compute_edge(final_score, competes, portfolio_scores, best_portfolio_score)
        vs_note   = _vs_portfolio_note(ticker, competes, portfolio_scores, final_score)
        swap_vs   = _best_swap_target(ticker, competes, portfolio_scores, final_score)

        status, trade_type, action_concreta, why_not_now, near_miss = _classify(
            score            = float(final_score),
            conviction       = float(conviction),
            asym             = asym,
            edge             = edge,
            screener         = screener_m,
            competes_with    = competes,
            portfolio_scores = portfolio_scores,
            gate_state       = gate_state,
            tech_score_raw   = tech_score_raw,
        )

        if status == CandidateStatus.DESCARTAR:
            continue

        # ── Flags de calibración fina ─────────────────────────────────────────
        # Contradicción técnica: técnico muy negativo pero score positivo
        tech_contradiction = (
            tech_score_raw < TECH_CONTRADICTION_THRESH
            and float(final_score) > 0
        )

        # Intensidad del swap
        swap_strength = ""
        if trade_type == TradeType.SWAP_CANDIDATE:
            swap_strength = _compute_swap_strength(edge.raw)

        sizing_base, sizing_adj, sizing_note = _suggest_sizing(
            c=type("C", (), {
                "final_score": float(final_score),
                "conviction":  float(conviction),
                "asymmetry":   asym,
                "competes_with": competes,
            })(),
            existing_allocations=existing_alloc,
        )

        alerts = []
        if asym.rr_alert:
            alerts.append(asym.rr_alert)
        if asym.rr_valid and asym.risk_reward >= RR_EXCEPCIONAL:
            alerts.append(
                f"R/R {asym.risk_reward:.1f}x excepcional — verificar robustez del target/stop"
            )

        cand = OpportunityCandidate(
            ticker               = ticker,
            status               = status,
            trade_type           = trade_type,
            final_score          = round(float(final_score), 4),
            conviction           = round(float(conviction), 4),
            tech_score           = round(float(tech_score_raw), 4),
            macro_score          = round(float(macro_score), 4),
            sentiment_score      = round(float(sent_score), 4),
            momentum_score       = round(float(momentum_s), 4),
            asymmetry            = asym,
            asymmetry_label      = asym_lbl,
            edge                 = edge,
            competes_with        = competes,
            swap_vs              = swap_vs,
            vs_portfolio_note    = vs_note,
            why_not_now          = why_not_now,
            action_concreta      = action_concreta,
            tech_contradiction   = tech_contradiction,
            swap_strength        = swap_strength,
            near_miss_rr         = near_miss,
            sizing_base          = sizing_base,
            sizing_adjusted      = sizing_adj,
            sizing_adjustment_note = sizing_note,
            sizing_suggested     = sizing_adj,
            price_usd            = screener_m.price,
            alerts               = alerts,
        )

        if asym.atr_pct > 0:
            cand.entry_zone_low  = round(screener_m.price * (1 - asym.atr_pct * 0.5), 2)
            cand.entry_zone_high = round(screener_m.price * (1 + asym.atr_pct * 0.3), 2)
        else:
            cand.entry_zone_low  = round(screener_m.price * 0.98, 2)
            cand.entry_zone_high = round(screener_m.price * 1.02, 2)

        cand.entry_reasons = _build_entry_reasons(cand, screener_m)
        cand.invalidation  = _build_invalidation(screener_m, cand)

        candidates.append(cand)

    # ── Filtros post-scoring ──────────────────────────────────────────────────
    if min_score > 0:
        candidates = [c for c in candidates if c.final_score >= min_score]
    if min_rr > 0:
        candidates = [
            c for c in candidates
            if c.asymmetry and c.asymmetry.rr_valid and c.asymmetry.risk_reward >= min_rr
        ]

    # ── Ranking compuesto ─────────────────────────────────────────────────────
    STATUS_PRIORITY = {
        CandidateStatus.COMPRABLE_AHORA:   6,
        CandidateStatus.COMPRA_HABILITADA: 5,
        CandidateStatus.SWAP_CANDIDATO:    4,
        CandidateStatus.VIGILANCIA_A:      3,
        CandidateStatus.VIGILANCIA_B:      2,
        CandidateStatus.VIGILANCIA_C:      1,
        CandidateStatus.NO_OPERABLE:       0,
    }

    def _rank_key(c: OpportunityCandidate) -> float:
        asym_bonus = (
            c.asymmetry.asymmetry_ratio / 3.0
            if (c.asymmetry and c.asymmetry.rr_valid) else 0.0
        )
        edge_raw   = c.edge.raw if c.edge else 0.0
        priority   = STATUS_PRIORITY.get(c.status, 0)

        # Penalización explícita por edge negativo:
        # multiplicador 0.4 sobre el score compuesto → cae visiblemente en la lista
        # pero no sale de su categoría de vigilancia
        edge_mult  = 0.4 if edge_raw < 0 else (1.0 + min(edge_raw, 0.15) * 2)

        base_score = c.final_score * c.conviction * (1 + asym_bonus)
        return priority * 10 + base_score * edge_mult

    candidates.sort(key=_rank_key, reverse=True)
    candidates = candidates[:max_candidates]

    report.candidates        = candidates
    report.comprable_ahora   = [c for c in candidates if c.status == CandidateStatus.COMPRABLE_AHORA]
    report.compra_habilitada = [c for c in candidates if c.status == CandidateStatus.COMPRA_HABILITADA]
    report.swap_candidatos   = [c for c in candidates if c.status == CandidateStatus.SWAP_CANDIDATO]
    report.en_vigilancia     = [
        c for c in candidates
        if c.status in (CandidateStatus.VIGILANCIA_A, CandidateStatus.VIGILANCIA_B, CandidateStatus.VIGILANCIA_C)
    ]
    report.no_operables      = [c for c in candidates if c.status == CandidateStatus.NO_OPERABLE]

    logger.info(
        f"Opportunity: "
        f"{len(report.comprable_ahora)} comprables | "
        f"{len(report.compra_habilitada)} habilitadas | "
        f"{len(report.swap_candidatos)} swaps | "
        f"{len(report.en_vigilancia)} vigilancia | "
        f"{len(report.no_operables)} no operables"
    )
    return report


# ══════════════════════════════════════════════════════════════════════════════
# RENDER
# ══════════════════════════════════════════════════════════════════════════════

def render_opportunity_report(
    report:              OpportunityReport,
    portfolio_total_ars: float = 0.0,
) -> str:
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

    def _edge_icon(label: EdgeLabel) -> str:
        return {
            "fuerte": "🟢", "moderado": "🟡", "marginal": "🟠", "negativo": "🔴"
        }.get(label.value if hasattr(label, "value") else str(label), "⚪")

    def _trade_tag(t: TradeType, swap_strength: str = "") -> str:
        base = {
            TradeType.NEW_ENTRY:      "🆕 NUEVA ENTRADA",
            TradeType.SWAP_CANDIDATE: "🔄 SWAP",
            TradeType.ADD_ON:         "➕ ADD-ON",
            TradeType.WATCHLIST:      "👁 WATCHLIST",
        }.get(t, "")
        if t == TradeType.SWAP_CANDIDATE and swap_strength:
            strength_tags = {
                "FUERTE":   " [fuerte]",
                "TÁCTICO":  " [táctico]",
                "MODERADO": " [moderado]",
            }
            base += strength_tags.get(swap_strength, "")
        return base

    def _tech_contradiction_note(c: OpportunityCandidate) -> str:
        """Nota explícita cuando el técnico contradice el score final."""
        if not c.tech_contradiction:
            return ""
        if c.trade_type == TradeType.SWAP_CANDIDATE:
            return (
                f"⚠️ El técnico corto es negativo (score {c.tech_score:+.2f}) — "
                f"entra por momentum/RS y edge relativo, no por señal técnica directa."
            )
        return (
            f"⚠️ Contradicción interna: técnico {c.tech_score:+.2f} en contra, "
            f"score final positivo por momentum/macro. "
            f"Confirmar antes de entrar."
        )

    def _swap_context(c: OpportunityCandidate) -> str:
        """Texto de contexto para swaps según su intensidad."""
        if not c.swap_vs or c.trade_type != TradeType.SWAP_CANDIDATE:
            return ""
        strength = c.swap_strength or "MODERADO"
        if strength == "FUERTE":
            return (
                f"Swap fuerte vs <b>{c.swap_vs}</b> — "
                f"señal claramente superior al holding actual."
            )
        if strength == "TÁCTICO":
            return (
                f"Swap táctico vs <b>{c.swap_vs}</b> — mejora moderada, "
                f"no contundente. Solo si se busca diversificar el sector."
            )
        return (
            f"Swap moderado vs <b>{c.swap_vs}</b> — edge marginal. "
            f"Considerar solo si ya se decidió rotar esa posición."
        )

    h = []

    h.append("🔭 <b>RADAR DE OPORTUNIDADES</b>")
    h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    h.append(f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M')} ART")
    h.append(
        f"🔍 Universo: {report.universe_size} tickers → "
        f"{report.screened_count} pasaron el screener"
    )
    gate_icon = {"NORMAL": "✅", "CAUTIOUS": "⚠️", "BLOCKED": "🔴"}.get(report.gate_state, "⚪")
    h.append(f"{gate_icon} Gate: <b>{report.gate_state}</b>")
    if report.vix_level:
        h.append(f"   VIX: {report.vix_level:.1f}")
    h.append("")

    if not report.candidates:
        h.append("Sin candidatos que cumplan los criterios en este momento.")
        h.append("")
        h.append("<i>Sistema cuantitativo multicapa — no es asesoramiento financiero</i>")
        return "\n".join(h)

    # ── Helpers de bloque ─────────────────────────────────────────────────────

    def _render_full(c: OpportunityCandidate) -> None:
        asym_icons = {
            "EXCELENTE": "🟢🟢", "BUENA": "🟢", "MODERADA": "🟡",
            "POBRE": "🔴", "INVÁLIDA": "⛔"
        }
        asym_icon  = asym_icons.get(c.asymmetry_label, "⚪")
        tag        = _trade_tag(c.trade_type, c.swap_strength)
        edge_icon  = _edge_icon(c.edge.label) if c.edge else "⚪"
        edge_str   = (
            f"  Edge: {edge_icon} <code>{c.edge.raw:+.3f}</code> ({c.edge.label.value})"
            if c.edge else ""
        )
        near_miss_tag = "  <i>[near-miss R/R]</i>" if c.near_miss_rr else ""

        h.append(f"<b>━━ {c.ticker} ━━</b>  {tag}{edge_str}{near_miss_tag}")
        h.append(
            f"Score: <code>{c.final_score:+.3f}</code> | "
            f"Conv: <b>{round(c.conviction * 100)}%</b> [{_bar(c.conviction)}] | "
            f"Precio: <b>${c.price_usd:.2f}</b>"
        )
        h.append(
            f"<code>técnico {c.tech_score:+.3f} | macro {c.macro_score:+.3f} | "
            f"momentum {c.momentum_score:+.3f} | sent {c.sentiment_score:+.3f}</code>"
        )

        # Contradicción técnica — nota explícita
        tech_note = _tech_contradiction_note(c)
        if tech_note:
            h.append(f"   {tech_note}")

        # Alertas R/R
        for alert in c.alerts:
            h.append(f"   ⚠️ {escape(alert)}")

        # Asimetría
        if c.asymmetry and c.asymmetry.rr_valid:
            a = c.asymmetry
            h.append(
                f"{asym_icon} Asimetría <b>{c.asymmetry_label}</b> — "
                f"upside {a.upside_to_6m_high:.1%} | stop {a.stop_loss_pct:.0%} | "
                f"R/R <b>{a.risk_reward:.1f}x</b>"
            )
        elif c.asymmetry and c.asymmetry.rr_alert:
            h.append(f"⛔ {escape(c.asymmetry.rr_alert)}")

        h.append(f"📍 Entrada: <b>${c.entry_zone_low:.2f} – ${c.entry_zone_high:.2f}</b>")

        # Sizing
        if portfolio_total_ars > 0:
            ars_adj  = c.sizing_adjusted * portfolio_total_ars
            if c.sizing_base != c.sizing_adjusted:
                ars_base = c.sizing_base * portfolio_total_ars
                h.append(f"💰 Sizing base: <b>{_pct(c.sizing_base)}</b> ≈ {_money(ars_base)}")
                h.append(f"   Ajustado: <b>{_pct(c.sizing_adjusted)}</b> ≈ {_money(ars_adj)}")
                if c.sizing_adjustment_note:
                    h.append(f"   <i>({escape(c.sizing_adjustment_note)})</i>")
            else:
                h.append(
                    f"💰 Sizing sugerido: <b>{_pct(c.sizing_adjusted)}</b> "
                    f"del portfolio ≈ {_money(ars_adj)}"
                )
        else:
            h.append(f"💰 Sizing sugerido: <b>{_pct(c.sizing_adjusted)}</b> del portfolio")

        if c.entry_reasons:
            h.append("✅ <b>Por qué entra:</b>")
            for r in c.entry_reasons:
                h.append(f"   • {escape(r)}")

        if c.invalidation:
            h.append("🚫 <b>Qué invalida la idea:</b>")
            for inv in c.invalidation[:3]:
                h.append(f"   • {escape(inv)}")

        # Competencia y contexto de swap
        swap_ctx = _swap_context(c)
        if c.competes_with:
            h.append(f"⚖️ Compite con: <b>{', '.join(c.competes_with)}</b>")
            if swap_ctx:
                h.append(f"   → {swap_ctx}")
            elif c.vs_portfolio_note:
                h.append(f"   → {escape(c.vs_portfolio_note)}")

        h.append(f"🎯 <b>Acción sugerida:</b> {escape(c.action_concreta)}")
        if c.why_not_now:
            h.append(f"   <i>Freno actual: {escape(c.why_not_now)}</i>")
        h.append("")

    def _render_compact(c: OpportunityCandidate, show_why_not: bool = True) -> None:
        asym_r = c.asymmetry.risk_reward if (c.asymmetry and c.asymmetry.rr_valid) else 0.0
        rr_str = f"R/R {asym_r:.1f}x" if (c.asymmetry and c.asymmetry.rr_valid) else "R/R ⚠️"
        edge_str = (
            f" | edge {c.edge.raw:+.3f}"
            if c.edge and abs(c.edge.raw) > 0.005 else ""
        )
        near_tag = " <i>[near-miss]</i>" if c.near_miss_rr else ""
        h.append(
            f"  <b>{c.ticker}</b>: score <code>{c.final_score:+.3f}</code> | "
            f"conv. {round(c.conviction * 100)}% | "
            f"{rr_str} | ${c.price_usd:.2f}{edge_str}{near_tag}"
        )
        # Contradicción técnica en compacto
        if c.tech_contradiction:
            h.append(
                f"   ⚠️ Técnico {c.tech_score:+.2f} en contra — "
                f"score positivo por momentum/RS, no por señal técnica directa"
            )
        if c.entry_reasons:
            h.append(f"   └ {escape(c.entry_reasons[0])}")
        if c.competes_with:
            swap_ctx = _swap_context(c)
            h.append(f"   ⚖️ Compite con: {', '.join(c.competes_with)}")
            if swap_ctx:
                h.append(f"   → {swap_ctx}")
            elif c.vs_portfolio_note:
                h.append(f"   → {escape(c.vs_portfolio_note)}")
        if show_why_not and c.why_not_now:
            h.append(f"   ⏸ Por qué no entra: <i>{escape(c.why_not_now)}</i>")
        for alert in c.alerts[:1]:
            h.append(f"   ⚠️ {escape(alert)}")
        h.append(f"   🎯 Acción: {escape(c.action_concreta)}")
        h.append("")

    # ── COMPRABLE AHORA ───────────────────────────────────────────────────────
    if report.comprable_ahora:
        h.append(f"🟢 <b>COMPRABLE AHORA ({len(report.comprable_ahora)})</b>")
        h.append(
            f"<i>Cumple todos los umbrales: score ≥ {SCORE_COMPRABLE_DURO}, "
            f"conviction ≥ {CONVICTION_COMPRABLE:.0%}, R/R ≥ {RR_COMPRABLE:.1f}x, "
            f"edge positivo, gate NORMAL.</i>"
        )
        h.append("")
        for c in report.comprable_ahora:
            _render_full(c)

    # ── COMPRA HABILITADA ─────────────────────────────────────────────────────
    if report.compra_habilitada:
        h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        h.append(f"🟡 <b>COMPRA HABILITADA ({len(report.compra_habilitada)})</b>")
        h.append("<i>Buena señal, pero edge marginal o asimetría justa.</i>")
        h.append("")
        for c in report.compra_habilitada:
            _render_full(c)

    # ── SWAPS POSIBLES ────────────────────────────────────────────────────────
    if report.swap_candidatos:
        h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        h.append(f"🔄 <b>SWAPS POSIBLES ({len(report.swap_candidatos)})</b>")
        h.append(
            "<i>Superan un holding actual. Solo si se reduce primero la posición "
            "competidora. [fuerte] = edge claro, [táctico] = mejora moderada, "
            "[moderado] = edge marginal.</i>"
        )
        h.append("")
        for c in report.swap_candidatos:
            _render_full(c)

    # ── EN VIGILANCIA ─────────────────────────────────────────────────────────
    if report.en_vigilancia:
        h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        h.append(f"👁 <b>EN VIGILANCIA ({len(report.en_vigilancia)})</b>")
        h.append("")

        vig_a = [c for c in report.en_vigilancia if c.status == CandidateStatus.VIGILANCIA_A]
        vig_b = [c for c in report.en_vigilancia if c.status == CandidateStatus.VIGILANCIA_B]
        vig_c = [
            c for c in report.en_vigilancia
            if c.status == CandidateStatus.VIGILANCIA_C and (c.edge.raw if c.edge else 0) >= 0
        ]
        vig_c_neg = [
            c for c in report.en_vigilancia
            if c.status == CandidateStatus.VIGILANCIA_C and (c.edge.raw if c.edge else 0) < 0
        ]

        if vig_a:
            h.append("<b>A — Casi operables</b> <i>(uno o dos frenos — falta poco)</i>")
            for c in vig_a:
                _render_compact(c, show_why_not=True)

        if vig_b:
            h.append("<b>B — Mejores que holdings actuales</b> <i>(sin setup suficiente)</i>")
            for c in vig_b:
                _render_compact(c, show_why_not=True)

        if vig_c:
            h.append("<b>C — Solo observación</b>")
            for c in vig_c:
                _render_compact(c, show_why_not=True)

        # Edge negativo en sección separada — menor visibilidad
        if vig_c_neg:
            h.append("<b>C' — Observación (no supera cartera actual)</b>")
            h.append(
                "<i>Edge negativo — no mejoran los holdings actuales. "
                "Solo relevantes si cambia el contexto.</i>"
            )
            for c in vig_c_neg:
                _render_compact(c, show_why_not=True)

    # ── NO OPERABLES ──────────────────────────────────────────────────────────
    if report.no_operables:
        h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        h.append(f"⛔ <b>SEÑAL PRESENTE, TRADE NO OPERABLE ({len(report.no_operables)})</b>")
        h.append("<i>R/R inválido o deficiente — señal interesante pero setup no ejecutable.</i>")
        h.append("")
        for c in report.no_operables:
            rr_str = (
                f"R/R {c.asymmetry.risk_reward:.1f}x" if c.asymmetry else "R/R n/a"
            )
            h.append(
                f"  <b>{c.ticker}</b>: score <code>{c.final_score:+.3f}</code> | "
                f"{rr_str} | ${c.price_usd:.2f}"
            )
            if c.asymmetry and c.asymmetry.rr_alert:
                h.append(f"   ⚠️ {escape(c.asymmetry.rr_alert)}")
            h.append(f"   🎯 {escape(c.action_concreta)}")
            h.append("")

    h.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    h.append("<i>Sistema cuantitativo multicapa — no es asesoramiento financiero</i>")

    return "\n".join(h)
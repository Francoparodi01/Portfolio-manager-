"""
scripts/backtest.py
Backtest vectorizado del pipeline cuantitativo multicapa.

Simula 2 años de operación semanal (lunes) con:
  - Pipeline completo: macro + technical + risk + synthesis + optimizer
  - Slippage: 0.15% por trade (estimado CEDEARs Cocos)
  - Comisión: 0.60% por trade (Cocos Capital)
  - Look-ahead bias: CERO — todos los indicadores usan solo datos pasados

Métricas de salida:
  - Equity curve: sistema vs buy & hold equal weight
  - Sharpe ratio anualizado (ambos)
  - Max drawdown (ambos)
  - Win rate de señales
  - Tabla completa de trades

Uso:
  python scripts/backtest.py
  python scripts/backtest.py --tickers CVX NVDA MU MELI --years 2
  python scripts/backtest.py --no-telegram
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd

from src.core.config import get_config
from src.core.logger import get_logger
from src.collector.notifier import TelegramNotifier

logger = get_logger(__name__)

# ── Parámetros ────────────────────────────────────────────────────────────────
SLIPPAGE     = 0.0015   # 0.15% por trade
COMMISSION   = 0.0060   # 0.60% por trade (Cocos)
TOTAL_COST   = SLIPPAGE + COMMISSION   # 0.75% round-trip / 2 = 0.375% por leg
REBAL_THRESH_BUY  = 0.08   # comprar si delta > 8%
REBAL_THRESH_SELL = 0.15   # vender si delta < -15% (más difícil salir)
REBAL_THRESH      = 0.08   # fallback genérico
RF_ANNUAL    = 0.05     # tasa libre de riesgo USD
W_MIN        = 0.02
W_MAX        = 0.40
INITIAL_CASH = 1_000_000.0   # ARS ficticio para normalizar


# ── Dataclasses ───────────────────────────────────────────────────────────────

@dataclass
class Trade:
    date: datetime
    ticker: str
    action: str          # BUY / SELL
    weight_before: float
    weight_after: float
    delta: float
    price: float
    cost_pct: float      # comisión + slippage aplicado
    signal_score: float  # score del pipeline en esa fecha


@dataclass
class BacktestResult:
    equity_curve: pd.Series          # optimizer (sistema completo)
    bh_curve: pd.Series              # buy & hold equal weight
    score_only_curve: pd.Series      # score-only sin optimizer matemático
    trades: list[Trade]
    weekly_scores: pd.DataFrame
    tickers: list[str]
    start_date: datetime
    end_date: datetime

    total_return: float = 0.0
    bh_return: float = 0.0
    score_only_return: float = 0.0
    sharpe: float = 0.0
    bh_sharpe: float = 0.0
    score_only_sharpe: float = 0.0
    max_drawdown: float = 0.0
    bh_max_drawdown: float = 0.0
    score_only_max_drawdown: float = 0.0
    win_rate: float = 0.0
    n_trades: int = 0
    avg_cost_per_rebal: float = 0.0
    alpha: float = 0.0
    alpha_vs_score_only: float = 0.0
    misaligned_trades: int = 0   # trades contra dirección del score
    misaligned_pct: float = 0.0  # % del total


# ── Descarga de datos ─────────────────────────────────────────────────────────

def download_prices(tickers: list[str], years: int = 2) -> pd.DataFrame:
    """Descarga precios de cierre ajustados para todos los tickers."""
    import yfinance as yf
    period = f"{years}y"
    logger.info(f"Descargando {years} años de precios para {tickers}...")
    data = yf.download(tickers, period=period, interval="1d",
                       progress=False, auto_adjust=True)
    if isinstance(data.columns, pd.MultiIndex):
        prices = data["Close"]
    else:
        prices = data[["Close"]].rename(columns={"Close": tickers[0]})
    prices = prices.dropna(how="all").ffill()
    logger.info(f"Precios descargados: {len(prices)} días, {prices.shape[1]} tickers")
    return prices


def download_macro_history(years: int = 2) -> pd.DataFrame:
    """Descarga indicadores macro históricos."""
    import yfinance as yf
    macro_tickers = {
        "vix":   "^VIX",
        "sp500": "^GSPC",
        "wti":   "CL=F",
        "tnx":   "^TNX",
        "dxy":   "DX-Y.NYB",
    }
    period = f"{years}y"
    symbols = list(macro_tickers.values())
    data = yf.download(symbols, period=period, interval="1d",
                       progress=False, auto_adjust=True)
    if isinstance(data.columns, pd.MultiIndex):
        close = data["Close"]
    else:
        close = data
    result = pd.DataFrame()
    for key, sym in macro_tickers.items():
        if sym in close.columns:
            result[key] = close[sym]
    return result.ffill().bfill()


# ── Cálculo de señales técnicas (vectorizado, sin look-ahead) ─────────────────

def compute_technical_scores(prices: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula señal técnica para cada ticker y cada día.
    Usa ventanas rodantes — sin look-ahead bias.
    Retorna DataFrame con scores en [-1, +1].
    """
    scores = pd.DataFrame(index=prices.index, columns=prices.columns, dtype=float)

    for ticker in prices.columns:
        s = prices[ticker].dropna()

        # RSI(14)
        delta = s.diff()
        gain  = delta.clip(lower=0).rolling(14).mean()
        loss  = (-delta.clip(upper=0)).rolling(14).mean()
        rs    = gain / loss.replace(0, np.nan)
        rsi   = 100 - (100 / (1 + rs))

        # MACD
        ema12  = s.ewm(span=12).mean()
        ema26  = s.ewm(span=26).mean()
        macd   = ema12 - ema26
        signal = macd.ewm(span=9).mean()
        macd_hist = macd - signal

        # Bollinger Bands
        sma20 = s.rolling(20).mean()
        std20 = s.rolling(20).std()
        bb_upper = sma20 + 2 * std20
        bb_lower = sma20 - 2 * std20
        bb_pos   = (s - bb_lower) / (bb_upper - bb_lower + 1e-10) - 0.5  # -0.5 a +0.5

        # EMA trend (12 vs 26)
        ema_trend = (ema12 - ema26) / (ema26 + 1e-10)

        # Combinar en score
        rsi_score   = np.where(rsi < 30, 1.0, np.where(rsi > 70, -1.0, (50 - rsi) / 50))
        macd_score  = np.sign(macd_hist) * np.clip(abs(macd_hist) / (s * 0.01 + 1e-10), 0, 1)
        bb_score    = -bb_pos * 2   # sobreventa (bajo) = positivo
        trend_score = np.clip(ema_trend * 10, -1, 1)

        raw = (rsi_score * 0.25 + macd_score * 0.30 +
               bb_score * 0.20 + trend_score * 0.25)

        scores[ticker] = pd.Series(np.clip(raw, -1, 1), index=s.index)

    return scores.reindex(prices.index).ffill()


def compute_macro_scores(macro: pd.DataFrame, ticker: str) -> pd.Series:
    """
    Score macro para un ticker dado el histórico de indicadores.
    Versión simplificada vectorizada del score_macro_for_ticker.
    """
    WEIGHTS = {
        "CVX":  {"vix": -0.20, "sp500": 0.20, "wti": 0.40, "tnx": -0.05, "dxy": -0.15},
        "NVDA": {"vix": -0.25, "sp500": 0.35, "wti":  0.0, "tnx": -0.25, "dxy": -0.15},
        "MU":   {"vix": -0.25, "sp500": 0.35, "wti":  0.0, "tnx": -0.20, "dxy": -0.20},
        "MELI": {"vix": -0.20, "sp500": 0.30, "wti":  0.0, "tnx": -0.15, "dxy": -0.35},
    }
    weights = WEIGHTS.get(ticker, {"vix": -0.30, "sp500": 0.40, "tnx": -0.10, "dxy": -0.20})

    score = pd.Series(0.0, index=macro.index)
    for indic, w in weights.items():
        if indic not in macro.columns:
            continue
        # Normalizar cambio % a [-1, +1]
        chg = macro[indic].pct_change(5).fillna(0)   # cambio 1 semana
        norm = np.clip(chg * 20, -1, 1)              # 5% chg → score 1.0
        score += norm * w

    return np.clip(score, -1, 1)


def compute_risk_scores(prices: pd.DataFrame) -> pd.DataFrame:
    """Risk score por activo: penaliza vol extrema y drawdown individual."""
    scores = pd.DataFrame(0.0, index=prices.index, columns=prices.columns)
    for ticker in prices.columns:
        s   = prices[ticker].dropna()
        vol = s.pct_change().rolling(60).std() * np.sqrt(252)
        # Penalizar si vol > 60%
        penalty = np.where(vol > 0.80, -0.40,
                  np.where(vol > 0.60, -0.10, 0.0))
        # Drawdown individual (ventana 60 días)
        roll_max = s.rolling(60, min_periods=1).max()
        dd = (s - roll_max) / roll_max
        dd_penalty = np.where(dd < -0.20, -0.10, 0.0)
        scores[ticker] = pd.Series(penalty + dd_penalty, index=vol.index)
    return scores.reindex(prices.index).fillna(0)


def compute_pipeline_scores(prices: pd.DataFrame, macro: pd.DataFrame) -> pd.DataFrame:
    """
    Score final del pipeline para cada ticker y cada fecha.
    Replica blend_scores_local con pesos: tech=30%, macro=30%, risk=25%, sentiment=15%
    (sentiment=0 en backtest — sin datos históricos de noticias)
    """
    tech   = compute_technical_scores(prices)
    risk   = compute_risk_scores(prices)

    final  = pd.DataFrame(index=prices.index, columns=prices.columns, dtype=float)
    for ticker in prices.columns:
        mac = compute_macro_scores(macro, ticker).reindex(prices.index).ffill().bfill()
        t   = tech[ticker].fillna(0)
        r   = risk[ticker].fillna(0)
        s   = t * 0.30 + mac * 0.30 + r * 0.25   # sentiment=0
        final[ticker] = np.clip(s, -1, 1)

    return final


# ── Optimizer vectorizado ─────────────────────────────────────────────────────

def _project_simplex(w: np.ndarray, w_min: float, w_max_arr: np.ndarray) -> np.ndarray:
    w = np.clip(w, w_min, w_max_arr)
    for _ in range(200):
        diff = w.sum() - 1.0
        if abs(diff) < 1e-9: break
        if diff > 0:
            red = w - w_min; tot = red.sum()
            if tot > 1e-10: w -= red * (diff / tot)
        else:
            aum = w_max_arr - w; tot = aum.sum()
            if tot > 1e-10: w += aum * (-diff / tot)
        w = np.clip(w, w_min, w_max_arr)
    return w


def optimize_weights(returns_window: pd.DataFrame, scores: dict[str, float],
                     method: str = "SCORE_BASED", vix: float = 20.0,
                     w_current: dict = None) -> dict[str, float]:
    """
    Optimizer-Lite v2: clamp direccional duro.

    Arquitectura: score_target → risk_adjustment → clamp_direccional → execution_target

    Reglas:
      - score < 0  → nunca aumentar peso respecto al actual
      - score > 0  → nunca reducir más del 10% de trim (solo riesgo extremo)
      - vol alta   → reduce el STEP máximo, no el target
      - step_max   → ±15% por rebalanceo para evitar overreaction
    """
    tickers = list(returns_window.columns)
    W_MIN_LITE = 0.05
    W_MAX_LITE = 0.55
    STEP_MAX   = 0.15   # máximo cambio por rebalanceo
    w_cur = w_current or {t: 1/len(tickers) for t in tickers}

    # ── PASO 1: Score target (pesos ideales según señal) ──────────────────────
    # Shift mínimo para que todos sean positivos, pero preservando diferencias
    min_score = min(scores.get(t, 0) for t in tickers)
    shift = max(-min_score + 0.10, 0.10)   # shift dinámico, no fijo en 0.6
    raw = {t: max(scores.get(t, 0) + shift, W_MIN_LITE) for t in tickers}
    tot = sum(raw.values())
    w_score = {t: min(v / tot, W_MAX_LITE) for t, v in raw.items()}
    # renormalizar después del cap
    tot2 = sum(w_score.values())
    w_score = {t: v / tot2 for t, v in w_score.items()}

    # ── PASO 2: Risk adjustment (modifica step, no target) ────────────────────
    vol = returns_window.std() * np.sqrt(252)
    step_multiplier = {}
    for t in tickers:
        v = float(vol.get(t, 0.3))
        if v > 0.80:   step_multiplier[t] = 0.50   # alta vol: pasos pequeños
        elif v > 0.60: step_multiplier[t] = 0.70
        else:          step_multiplier[t] = 1.00

    # VIX alto: reducir step de todas las posiciones
    vix_mult = 0.60 if vix > 32 else 1.00

    # ── PASO 3: Clamp direccional duro ────────────────────────────────────────
    w_target = {}
    for t in tickers:
        sc       = scores.get(t, 0)
        w_c      = w_cur.get(t, 1/len(tickers))
        w_s      = w_score[t]
        step_max = STEP_MAX * step_multiplier[t] * vix_mult

        if sc >= 0:
            # Score positivo: puede subir, pero solo trim mínimo si baja
            if w_s >= w_c:
                # Quiere subir: OK, pero limitado por step_max
                w_target[t] = min(w_s, w_c + step_max)
            else:
                # Quiere bajar con score positivo: solo trim si es > 10% de exceso
                trim = w_c - w_s
                if trim > 0.10:   # solo si es un exceso real
                    w_target[t] = max(w_s, w_c - step_max * 0.5)  # trim lento
                else:
                    w_target[t] = w_c  # mantener, no tocar

        else:
            # Score negativo: puede bajar, nunca subir
            if w_s <= w_c:
                # Quiere bajar: OK, limitado por step_max
                w_target[t] = max(w_s, w_c - step_max, W_MIN_LITE)
            else:
                # Quiere subir con score negativo: BLOQUEADO
                w_target[t] = w_c  # mantener actual, no aumentar

    # ── PASO 4: Normalizar a suma=1 ───────────────────────────────────────────
    tot3 = sum(w_target.values())
    if tot3 <= 0:
        return {t: 1/len(tickers) for t in tickers}
    w_final = {t: max(v / tot3, W_MIN_LITE) for t, v in w_target.items()}
    tot4 = sum(w_final.values())
    return {t: v / tot4 for t, v in w_final.items()}


def select_method(vix: float, sp500_chg_5d: float) -> str:
    if vix > 35:  return "MIN_VARIANCE"   # crisis real
    if vix > 32:  return "MIN_VARIANCE"   # defensivo (subido de 25 a 32)
    if sp500_chg_5d < -0.05: return "MIN_VARIANCE"
    return "MAX_SHARPE"


# ── Métricas ──────────────────────────────────────────────────────────────────

def sharpe_ratio(returns: pd.Series, rf: float = RF_ANNUAL) -> float:
    if returns.std() == 0: return 0.0
    excess = returns.mean() * 252 - rf
    vol    = returns.std() * np.sqrt(252)
    return float(excess / vol)


def max_drawdown(equity: pd.Series) -> float:
    roll_max = equity.cummax()
    dd = (equity - roll_max) / roll_max
    return float(dd.min())


def compute_win_rate(trades: list[Trade], prices: pd.DataFrame) -> float:
    """% de BUY trades que subieron en los 5 días siguientes."""
    wins = 0; total = 0
    buy_trades = [t for t in trades if t.action == "BUY"]
    for tr in buy_trades:
        if tr.ticker not in prices.columns: continue
        date_idx = prices.index.searchsorted(tr.date)
        if date_idx + 5 >= len(prices): continue
        price_now  = prices[tr.ticker].iloc[date_idx]
        price_5d   = prices[tr.ticker].iloc[date_idx + 5]
        if price_now > 0:
            ret_5d = (price_5d - price_now) / price_now
            if ret_5d > 0: wins += 1
            total += 1
    return wins / total if total > 0 else 0.0


# ── Motor del backtest ────────────────────────────────────────────────────────

def run_backtest(tickers: list[str], years: int = 2,
                 rebal_thresh: float = REBAL_THRESH,
                 rebal_freq: int = 2) -> BacktestResult:
    logger.info(f"Iniciando backtest: {tickers} | {years} años | quincenal | buy≥8% sell≥15%")

    # 1. Descargar datos
    prices = download_prices(tickers, years)
    macro  = download_macro_history(years)
    macro  = macro.reindex(prices.index).ffill().bfill()

    # 2. Pipeline scores (vectorizado)
    logger.info("Calculando scores del pipeline...")
    pipeline_scores = compute_pipeline_scores(prices, macro)

    # 3. Returns
    returns = prices.pct_change().fillna(0)

    # 4. Fechas de rebalanceo (lunes, o día siguiente si feriado)
    rebal_dates = []
    prev_week = None
    rebal_counter = 0
    for date in prices.index:
        week = date.isocalendar()[:2]   # (year, week)
        if week != prev_week:
            rebal_counter += 1
            if rebal_counter % rebal_freq == 1:   # cada N semanas
                rebal_dates.append(date)
            prev_week = week
    # Necesitamos al menos 60 días de historia para el optimizer
    warmup = 60
    rebal_dates = [d for d in rebal_dates if prices.index.get_loc(d) >= warmup]
    logger.info(f"Fechas de rebalanceo: {len(rebal_dates)} semanas")

    # 5. Simulación — 3 portfolios en paralelo
    #    A) Optimizer  (MIN_VARIANCE / MAX_SHARPE según VIX)  ← sistema completo
    #    B) Score-only (pesos proporcionales al score, sin math)
    #    C) Buy & hold equal weight (benchmark pasivo)

    def _drift(w, dr, tickers):
        tot = sum(w.get(t, 0) * (1 + dr[t]) for t in tickers)
        if tot <= 0: return {t: 1/len(tickers) for t in tickers}
        return {t: w.get(t, 0) * (1 + dr[t]) / tot for t in tickers}

    def _score_only_weights(scores, tickers):
        """Pesos shift+clip proporcionales al score. Equal weight si todos negativos."""
        raw = {t: max(scores.get(t, 0.0) + 0.5, W_MIN) for t in tickers}
        tot = sum(raw.values())
        w = {t: min(v / tot, W_MAX) for t, v in raw.items()}
        tot2 = sum(w.values())
        return {t: v / tot2 for t, v in w.items()}

    def _apply_thresh(w_cur, w_tgt, scores, tickers, vix=20.0):
        """
        Thresholds dinámicos por score + VIX.
        - Señal fuerte + dirección correcta: threshold bajo (actuar)
        - Señal débil o contraria: threshold alto (no moverse)
        """
        cost = 0.0
        w_eff = dict(w_cur)
        for t in tickers:
            delta = w_tgt.get(t, 0) - w_cur.get(t, 0)
            sc = scores.get(t, 0)

            if delta > 0:  # BUY
                if sc > 0.20:   thresh = 0.06   # señal fuerte: actuar
                elif sc > 0.10: thresh = 0.10
                else:           thresh = 0.18   # señal débil: no comprar
            else:  # SELL
                if sc < -0.20:  thresh = 0.05   # vender rápido
                elif sc < -0.10: thresh = 0.10
                elif vix > 30:  thresh = 0.10   # VIX alto: más ágil para salir
                else:           thresh = 0.15   # no vender con score positivo

            if abs(delta) < thresh:
                continue
            cost += abs(delta) * TOTAL_COST
            w_eff[t] = w_tgt.get(t, 0)

        tot = sum(w_eff.values())
        w_eff = {t: v / tot if tot > 0 else 1/len(tickers) for t, v in w_eff.items()}
        return w_eff, cost


    # Estado inicial
    port_opt  = INITIAL_CASH;  w_opt = {t: 1/len(tickers) for t in tickers}
    port_so   = INITIAL_CASH;  w_so  = {t: 1/len(tickers) for t in tickers}
    bh_value  = INITIAL_CASH;  bh_w  = {t: 1/len(tickers) for t in tickers}

    eq_opt: dict = {}; eq_so: dict = {}
    trades_log: list[Trade] = []; weekly_scores_log = []

    all_dates = prices.index.tolist()
    rebal_set = set(rebal_dates)
    bh_start_prices = {t: prices[t].iloc[warmup] for t in tickers}
    eq_opt[all_dates[warmup]] = port_opt
    eq_so[all_dates[warmup]]  = port_so

    for i, date in enumerate(all_dates[warmup:], start=warmup):
        day_ret = {t: float(returns[t].iloc[i]) for t in tickers}

        port_opt *= (1 + sum(w_opt.get(t,0)*day_ret[t] for t in tickers))
        port_so  *= (1 + sum(w_so.get(t,0) *day_ret[t] for t in tickers))
        bh_value *= (1 + sum(bh_w.get(t,0) *day_ret[t] for t in tickers))

        w_opt = _drift(w_opt, day_ret, tickers)
        w_so  = _drift(w_so,  day_ret, tickers)

        if date in rebal_set:
            scores_today = {t: float(pipeline_scores[t].loc[date])
                            for t in tickers if date in pipeline_scores.index}
            weekly_scores_log.append({"date": date, **scores_today})

            idx    = prices.index.get_loc(date)
            window = returns.iloc[max(0, idx-60):idx]
            vix_today = float(macro["vix"].loc[date]) if "vix" in macro.columns else 20.0
            sp500_chg = float(macro["sp500"].pct_change(5).loc[date]) if "sp500" in macro.columns else 0.0
            method    = select_method(vix_today, sp500_chg)

            # A: optimizer-lite v2 (con clamp direccional)
            w_opt_tgt  = optimize_weights(window, scores_today, vix=vix_today, w_current=w_opt)
            w_opt_eff, cost_opt = _apply_thresh(w_opt, w_opt_tgt, scores_today, tickers, vix=vix_today)

            for t in tickers:
                delta = w_opt_eff.get(t,0) - w_opt.get(t,0)
                if abs(delta) > 0.001:
                    trades_log.append(Trade(
                        date=date, ticker=t,
                        action="BUY" if delta > 0 else "SELL",
                        weight_before=round(w_opt.get(t,0), 4),
                        weight_after=round(w_opt_eff.get(t,0), 4),
                        delta=round(delta, 4),
                        price=round(float(prices[t].loc[date]), 2),
                        cost_pct=round(abs(delta)*TOTAL_COST*100, 4),
                        signal_score=round(scores_today.get(t,0), 4),
                    ))
            port_opt *= (1 - cost_opt)
            w_opt = w_opt_eff

            # B: score-only
            w_so_tgt = _score_only_weights(scores_today, tickers)
            w_so_eff, cost_so = _apply_thresh(w_so, w_so_tgt, scores_today, tickers, vix=vix_today)
            port_so *= (1 - cost_so)
            w_so = w_so_eff

        eq_opt[date] = port_opt
        eq_so[date]  = port_so

    # 6. Calcular métricas
    eq = pd.Series(eq_opt)
    so = pd.Series(eq_so)
    bh = pd.Series({
        d: INITIAL_CASH * (prices.loc[d] / pd.Series(bh_start_prices)).mean()
        for d in eq.index if d in prices.index
    })
    so = so.reindex(eq.index).ffill()

    total_ret    = (eq.iloc[-1] / eq.iloc[0]) - 1
    so_ret       = (so.iloc[-1] / so.iloc[0]) - 1
    bh_ret       = (bh.iloc[-1] / bh.iloc[0]) - 1
    win_rate     = compute_win_rate(trades_log, prices)
    n_rebal      = len(rebal_dates)
    avg_cost     = sum(t.cost_pct for t in trades_log) / n_rebal if n_rebal > 0 else 0

    # Trades contra dirección del score (clave de alineamiento)
    misaligned = sum(
        1 for t in trades_log
        if (t.action == "BUY"  and t.signal_score < 0) or
           (t.action == "SELL" and t.signal_score > 0)
    )
    misaligned_pct = misaligned / len(trades_log) if trades_log else 0.0

    result = BacktestResult(
        equity_curve=eq,
        bh_curve=bh,
        score_only_curve=so,
        trades=trades_log,
        weekly_scores=pd.DataFrame(weekly_scores_log).set_index("date") if weekly_scores_log else pd.DataFrame(),
        tickers=tickers,
        start_date=eq.index[0],
        end_date=eq.index[-1],
        total_return=round(total_ret, 4),
        bh_return=round(bh_ret, 4),
        score_only_return=round(so_ret, 4),
        sharpe=round(sharpe_ratio(eq.pct_change().dropna()), 3),
        bh_sharpe=round(sharpe_ratio(bh.pct_change().dropna()), 3),
        score_only_sharpe=round(sharpe_ratio(so.pct_change().dropna()), 3),
        max_drawdown=round(max_drawdown(eq), 4),
        bh_max_drawdown=round(max_drawdown(bh), 4),
        score_only_max_drawdown=round(max_drawdown(so), 4),
        win_rate=round(win_rate, 3),
        n_trades=len(trades_log),
        avg_cost_per_rebal=round(avg_cost, 4),
        alpha=round(total_ret - bh_ret, 4),
        alpha_vs_score_only=round(total_ret - so_ret, 4),
        misaligned_trades=misaligned,
        misaligned_pct=round(misaligned_pct, 3),
    )

    logger.info(f"Backtest completo: {len(trades_log)} trades | "
                f"return={total_ret:.1%} vs BH={bh_ret:.1%} | "
                f"sharpe={result.sharpe:.2f} | DD={result.max_drawdown:.1%}")
    return result


# ── Reporte ───────────────────────────────────────────────────────────────────

def format_console_report(r: BacktestResult) -> str:
    lines = [
        "=" * 76,
        "  BACKTEST — SISTEMA CUANTITATIVO MULTICAPA (quincenal, buy≥8% / sell≥15%)",
        f"  {r.start_date.strftime('%d/%m/%Y')} → {r.end_date.strftime('%d/%m/%Y')}",
        f"  Universo: {', '.join(r.tickers)}",
        "=" * 76,
        "",
        "  PERFORMANCE — 3 ESTRATEGIAS",
        f"  {'Métrica':<30} {'Optimizer':>12} {'Score-only':>12} {'Buy&Hold':>10}",
        "  " + "─" * 64,
        f"  {'Retorno total':<30} {r.total_return:>+11.1%} {r.score_only_return:>+11.1%} {r.bh_return:>+9.1%}",
        f"  {'Sharpe ratio':<30} {r.sharpe:>12.2f} {r.score_only_sharpe:>12.2f} {r.bh_sharpe:>10.2f}",
        f"  {'Max drawdown':<30} {r.max_drawdown:>11.1%} {r.score_only_max_drawdown:>11.1%} {r.bh_max_drawdown:>9.1%}",
        f"  {'Alpha vs Buy&Hold':<30} {r.alpha:>+11.1%} {r.score_only_return - r.bh_return:>+11.1%}",
        f"  {'Alpha optimizer vs score-only':<30} {r.alpha_vs_score_only:>+11.1%}",
        f"  {'Win rate (5d)':<30} {r.win_rate:>11.1%}",
        f"  {'Total trades (optimizer)':<30} {r.n_trades:>12}",
        f"  {'Trades contra score':<30} {r.misaligned_trades:>9} ({r.misaligned_pct:.0%})",
        f"  {'Costo prom/rebalanceo':<30} {r.avg_cost_per_rebal:>11.4f}%",
        "",
        "  TABLA DE TRADES",
        f"  {'Fecha':<12} {'Ticker':<6} {'Acción':<6} {'Antes':>7} {'Después':>7} "
        f"{'Delta':>7} {'Score':>7} {'Precio':>10}",
        "  " + "─" * 70,
    ]
    for t in r.trades:
        icon = "▲" if t.action == "BUY" else "▼"
        lines.append(
            f"  {t.date.strftime('%Y-%m-%d'):<12} {t.ticker:<6} {icon}{t.action:<5} "
            f"{t.weight_before:>6.1%} {t.weight_after:>7.1%} "
            f"{t.delta:>+6.1%} {t.signal_score:>+7.3f} ${t.price:>9,.2f}"
        )
    lines += [
        "",
        "  EVOLUCIÓN EQUITY CURVE (mensual)",
        f"  {'Fecha':<12} {'Sistema':>12} {'Buy & Hold':>12} {'Alpha':>10}",
        "  " + "─" * 46,
    ]
    monthly    = r.equity_curve.resample("ME").last()
    so_monthly = r.score_only_curve.resample("ME").last()
    bh_monthly = r.bh_curve.resample("ME").last()
    lines += [
        "",
        "  EQUITY CURVE MENSUAL",
        f"  {'Fecha':<12} {'Optimizer':>10} {'Score-only':>10} {'Buy&Hold':>10} {'Alpha/OPT':>10}",
        "  " + "─" * 52,
    ]
    for date in monthly.index:
        sys_val = monthly[date]
        so_val  = so_monthly.get(date, np.nan)
        bh_val  = bh_monthly.get(date, np.nan)
        sys_ret = (sys_val / r.equity_curve.iloc[0]) - 1
        so_ret_ = (so_val  / r.score_only_curve.iloc[0]) - 1 if not np.isnan(so_val) else np.nan
        bh_ret_ = (bh_val  / r.bh_curve.iloc[0]) - 1 if not np.isnan(bh_val) else np.nan
        alpha   = sys_ret - bh_ret_ if not np.isnan(bh_ret_) else np.nan
        lines.append(
            f"  {date.strftime('%Y-%m'):<12} {sys_ret:>+9.1%} "
            f"{so_ret_:>+9.1%} {bh_ret_:>+9.1%} {alpha:>+9.1%}"
        )
    lines.append("=" * 76)
    return "\n".join(lines)


def format_telegram_report(r: BacktestResult) -> str:
    opt_icon    = "🟢" if r.alpha > 0 else "🔴"
    so_icon     = "🟢" if r.score_only_return > r.bh_return else "🔴"
    sharpe_icon = "🟢" if r.sharpe > r.bh_sharpe else "🔴"
    dd_icon     = "🟢" if abs(r.max_drawdown) < abs(r.bh_max_drawdown) else "🔴"
    vs_so_icon  = "🟢" if r.alpha_vs_score_only > 0 else "🔴"

    lines = [
        "━" * 38,
        "🔬 <b>BACKTEST — 3 ESTRATEGIAS</b>",
        f"📅 {r.start_date.strftime('%d/%m/%Y')} → {r.end_date.strftime('%d/%m/%Y')}",
        f"🎯 Universo: <code>{', '.join(r.tickers)}</code>",
        "",
        "<b>Performance comparada</b>",
        f"  {opt_icon} Optimizer:   <b>{r.total_return:+.1%}</b>  sharpe {r.sharpe:.2f}  DD {r.max_drawdown:.1%}",
        f"  {so_icon} Score-only:  <b>{r.score_only_return:+.1%}</b>  sharpe {r.score_only_sharpe:.2f}  DD {r.score_only_max_drawdown:.1%}",
        f"  📊 Buy&Hold:   <b>{r.bh_return:+.1%}</b>  sharpe {r.bh_sharpe:.2f}  DD {r.bh_max_drawdown:.1%}",
        "",
        f"  {opt_icon} Alpha opt vs BH:        {r.alpha:>+.1%}",
        f"  {vs_so_icon} Alpha opt vs score-only: {r.alpha_vs_score_only:>+.1%}",
        f"  {sharpe_icon} Sharpe opt vs BH:       {r.sharpe - r.bh_sharpe:>+.2f}",
        f"  {dd_icon} Max DD opt vs BH:       {abs(r.max_drawdown) - abs(r.bh_max_drawdown):>+.1%}",
        f"  🎯 Win rate: <b>{r.win_rate:.0%}</b>   Trades: <b>{r.n_trades}</b>   Contra score: <b>{r.misaligned_pct:.0%}</b>",
        f"  💸 Costo/rebal: {r.avg_cost_per_rebal:.3f}%",
        "",
        "<b>Últimos 10 trades (optimizer):</b>",
    ]
    for t in r.trades[-10:]:
        icon = "🟢" if t.action == "BUY" else "🔴"
        lines.append(
            f"  {icon} {t.date.strftime('%d/%m/%y')} <b>{t.ticker}</b> "
            f"{t.weight_before:.0%}→{t.weight_after:.0%} "
            f"(score {t.signal_score:+.2f})"
        )
    lines.append("━" * 38)
    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────

async def main(tickers: list[str], years: int, no_telegram: bool,
               threshold: float = 0.08, freq: int = 2):
    result = run_backtest(tickers, years, threshold, freq)
    print(format_console_report(result))

    if not no_telegram:
        cfg      = get_config()
        notifier = TelegramNotifier(cfg.scraper.telegram_bot_token,
                                    cfg.scraper.telegram_chat_id)
        report = format_telegram_report(result)
        notifier.send_raw(report)
        logger.info("Reporte enviado a Telegram")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--tickers", nargs="+",
                   default=["CVX", "NVDA", "MU", "MELI"])
    p.add_argument("--years",     type=int,   default=2)
    p.add_argument("--threshold", type=float, default=0.08,
                   help="Delta mínimo para rebalancear (default 0.08 = 8%%)")
    p.add_argument("--freq",      type=int,   default=2,
                   help="Rebalancear cada N semanas (default 2 = quincenal)")
    p.add_argument("--no-telegram", action="store_true")
    args = p.parse_args()
    asyncio.run(main(args.tickers, args.years, args.no_telegram,
                     args.threshold, args.freq))
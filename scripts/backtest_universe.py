"""
scripts/backtest_universe.py — Búsqueda del portfolio óptimo sobre universo completo

FASE 1: Score individual de cada ticker (rápido, vectorizado)
  - Sharpe, retorno, drawdown, calidad de señal
  - Rankea los ~20 tickers disponibles

FASE 2: Backtest completo sobre combinaciones prometedoras
  - Toma el top N tickers del ranking
  - Prueba todas las combinaciones de tamaño 4 y 5
  - Reporta las mejores por Sharpe y por alpha

Uso:
  python scripts/backtest_universe.py
  python scripts/backtest_universe.py --top 10 --size 4 5 --years 2
  python scripts/backtest_universe.py --tickers CVX NVDA MU MELI AAPL MSFT TSLA AMZN
  python scripts/backtest_universe.py --no-telegram

Salida:
  - Ranking individual de tickers
  - Top 10 portfolios por Sharpe
  - Top 10 portfolios por Alpha vs B&H
  - Recomendación final
"""
from __future__ import annotations

import argparse
import asyncio
import itertools
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd

from src.core.config import get_config
from src.core.logger import get_logger
from src.collector.notifier import TelegramNotifier

# Reusar funciones del backtest principal
from scripts.backtest import (
    download_prices, download_macro_history,
    compute_pipeline_scores, optimize_weights, select_method,
    sharpe_ratio, max_drawdown,
    RF_ANNUAL, W_MIN, W_MAX, TOTAL_COST, INITIAL_CASH,
)

logger = get_logger(__name__)

# ── Universo default (tickers disponibles en CEDEARs Cocos) ──────────────────
DEFAULT_UNIVERSE = [
    # Portfolio actual
    "CVX", "NVDA", "MU", "MELI",
    # Tech / semiconductores
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA",
    "AMD", "INTC", "QCOM", "AVGO",
    # Energía / commodities
    "XOM", "COP", "SLB",
    # Financials / otros
    "JPM", "BAC", "GS",
]

# ── Dataclasses ───────────────────────────────────────────────────────────────

@dataclass
class TickerStats:
    ticker: str
    total_return: float
    sharpe: float
    max_drawdown: float
    avg_score: float          # score promedio del pipeline
    score_volatility: float   # qué tan estable es la señal
    signal_quality: float     # sharpe × (1 + avg_score) — métrica compuesta
    data_ok: bool = True
    error: str = ""


@dataclass
class PortfolioResult:
    tickers: tuple
    total_return: float
    sharpe: float
    max_drawdown: float
    alpha_vs_bh: float
    alpha_vs_so: float
    n_trades: int
    composite_score: float    # sharpe × (1 - max_drawdown) — para rankear


# ── FASE 1: Scoring individual rápido ────────────────────────────────────────

def score_ticker_fast(ticker: str, prices: pd.DataFrame,
                       macro: pd.DataFrame,
                       pipeline_scores: pd.DataFrame) -> TickerStats:
    """
    Métricas individuales de un ticker: retorno, Sharpe, DD, calidad de señal.
    Simula un portfolio de un solo activo (100% en ese ticker).
    """
    try:
        if ticker not in prices.columns:
            return TickerStats(ticker=ticker, total_return=0, sharpe=0,
                               max_drawdown=0, avg_score=0, score_volatility=0,
                               signal_quality=0, data_ok=False, error="sin datos")

        p   = prices[ticker].dropna()
        ret = p.pct_change().dropna()

        if len(ret) < 60:
            return TickerStats(ticker=ticker, total_return=0, sharpe=0,
                               max_drawdown=0, avg_score=0, score_volatility=0,
                               signal_quality=0, data_ok=False, error="pocos datos")

        total_ret = float(p.iloc[-1] / p.iloc[0] - 1)
        sr        = float(sharpe_ratio(ret))
        dd        = float(max_drawdown(p))

        # Score del pipeline para este ticker
        if ticker in pipeline_scores.columns:
            sc = pipeline_scores[ticker].dropna()
            avg_sc  = float(sc.mean())
            sc_vol  = float(sc.std())
        else:
            avg_sc = 0.0
            sc_vol = 0.5

        # Métrica compuesta: Sharpe ajustado por calidad de señal
        # Un ticker con buen Sharpe Y señales consistentes vale más
        signal_q = sr * (1 + max(avg_sc, 0)) * max(1 - sc_vol, 0.3)

        return TickerStats(
            ticker=ticker,
            total_return=round(total_ret, 4),
            sharpe=round(sr, 3),
            max_drawdown=round(dd, 4),
            avg_score=round(avg_sc, 4),
            score_volatility=round(sc_vol, 4),
            signal_quality=round(signal_q, 4),
        )

    except Exception as e:
        return TickerStats(ticker=ticker, total_return=0, sharpe=0,
                           max_drawdown=0, avg_score=0, score_volatility=0,
                           signal_quality=0, data_ok=False, error=str(e))


# ── FASE 2: Backtest de portfolio (reusar lógica del backtest principal) ──────

def run_portfolio_backtest_fast(tickers: list[str],
                                 prices: pd.DataFrame,
                                 returns: pd.DataFrame,
                                 macro: pd.DataFrame,
                                 pipeline_scores: pd.DataFrame,
                                 rebal_dates: list,
                                 years: int = 2) -> Optional[PortfolioResult]:
    """
    Backtest rápido para una combinación de tickers.
    Reutiliza datos ya descargados — no hace requests adicionales.
    """
    try:
        # Filtrar tickers con datos
        valid = [t for t in tickers if t in prices.columns
                 and prices[t].notna().sum() > 120]
        if len(valid) < len(tickers):
            return None
        if len(valid) < 2:
            return None

        p_sub  = prices[valid]
        r_sub  = returns[valid]
        sc_sub = pipeline_scores[valid] if all(t in pipeline_scores.columns for t in valid) else None

        warmup = 60
        valid_rebal = [d for d in rebal_dates if p_sub.index.get_loc(d) >= warmup]
        if len(valid_rebal) < 4:
            return None

        n = len(valid)

        def _drift(w, dr):
            tot = sum(w.get(t, 0) * (1 + dr.get(t, 0)) for t in valid)
            if tot <= 0: return {t: 1/n for t in valid}
            return {t: w.get(t, 0) * (1 + dr.get(t, 0)) / tot for t in valid}

        def _score_only_w(scores):
            raw = {t: max(scores.get(t, 0.0) + 0.5, W_MIN) for t in valid}
            tot = sum(raw.values())
            w = {t: min(v / tot, W_MAX) for t, v in raw.items()}
            tot2 = sum(w.values())
            return {t: v / tot2 for t, v in w.items()}

        port_opt = INITIAL_CASH; w_opt = {t: 1/n for t in valid}
        port_so  = INITIAL_CASH; w_so  = {t: 1/n for t in valid}
        bh_val   = INITIAL_CASH; bh_w  = {t: 1/n for t in valid}

        eq_opt = {}; eq_so = {}
        n_trades = 0
        rebal_set = set(valid_rebal)
        bh_start  = {t: p_sub[t].iloc[warmup] for t in valid}

        all_dates = p_sub.index.tolist()

        for i, date in enumerate(all_dates[warmup:], start=warmup):
            dr = {t: float(r_sub[t].iloc[i]) for t in valid}

            port_opt *= (1 + sum(w_opt.get(t, 0) * dr[t] for t in valid))
            port_so  *= (1 + sum(w_so.get(t, 0)  * dr[t] for t in valid))
            bh_val   *= (1 + sum(bh_w.get(t, 0)  * dr[t] for t in valid))

            w_opt = _drift(w_opt, dr)
            w_so  = _drift(w_so,  dr)

            if date in rebal_set:
                scores_today = {}
                if sc_sub is not None and date in sc_sub.index:
                    scores_today = {t: float(sc_sub[t].loc[date]) for t in valid}

                idx = p_sub.index.get_loc(date)
                window = r_sub.iloc[max(0, idx-60):idx][valid]

                vix_today = float(macro["vix"].loc[date]) if "vix" in macro.columns else 20.0
                sp500_chg = float(macro["sp500"].pct_change(5).loc[date]) if "sp500" in macro.columns else 0.0

                # Optimizer-lite con clamp direccional
                w_tgt = optimize_weights(window, scores_today, vix=vix_today, w_current=w_opt)

                # Apply threshold (simplificado para velocidad)
                cost = 0.0
                w_eff = dict(w_opt)
                for t in valid:
                    delta = w_tgt.get(t, 0) - w_opt.get(t, 0)
                    sc = scores_today.get(t, 0)
                    thresh = 0.06 if (delta > 0 and sc > 0.20) else \
                             0.05 if (delta < 0 and sc < -0.20) else 0.10
                    if abs(delta) >= thresh:
                        cost += abs(delta) * TOTAL_COST
                        w_eff[t] = w_tgt.get(t, 0)
                        n_trades += 1

                tot = sum(w_eff.values())
                w_opt = {t: v / tot for t, v in w_eff.items()} if tot > 0 else w_opt
                port_opt *= (1 - cost)

                # Score-only
                if scores_today:
                    w_so_tgt = _score_only_w(scores_today)
                    w_so_eff = dict(w_so)
                    cost_so  = 0.0
                    for t in valid:
                        delta = w_so_tgt.get(t, 0) - w_so.get(t, 0)
                        if abs(delta) >= 0.08:
                            cost_so += abs(delta) * TOTAL_COST
                            w_so_eff[t] = w_so_tgt.get(t, 0)
                    tot2 = sum(w_so_eff.values())
                    w_so  = {t: v / tot2 for t, v in w_so_eff.items()} if tot2 > 0 else w_so
                    port_so *= (1 - cost_so)

            eq_opt[date] = port_opt
            eq_so[date]  = port_so

        eq  = pd.Series(eq_opt)
        so  = pd.Series(eq_so)
        bh  = pd.Series({
            d: INITIAL_CASH * sum(p_sub[t].loc[d] / bh_start[t] / n for t in valid)
            for d in eq.index if d in p_sub.index
        })
        so = so.reindex(eq.index).ffill()

        total_ret = float(eq.iloc[-1] / eq.iloc[0] - 1)
        bh_ret    = float(bh.iloc[-1] / bh.iloc[0] - 1)
        so_ret    = float(so.iloc[-1] / so.iloc[0] - 1)
        sr        = float(sharpe_ratio(eq.pct_change().dropna()))
        dd        = float(max_drawdown(eq))

        composite = sr * (1 + max(total_ret - bh_ret, 0)) * (1 + dd)  # dd es negativo

        return PortfolioResult(
            tickers=tuple(sorted(valid)),
            total_return=round(total_ret, 4),
            sharpe=round(sr, 4),
            max_drawdown=round(dd, 4),
            alpha_vs_bh=round(total_ret - bh_ret, 4),
            alpha_vs_so=round(total_ret - so_ret, 4),
            n_trades=n_trades,
            composite_score=round(composite, 4),
        )

    except Exception as e:
        logger.debug(f"Backtest {tickers}: {e}")
        return None


# ── Reporte ───────────────────────────────────────────────────────────────────

def format_universe_report(ticker_stats: list[TickerStats],
                            top_by_sharpe: list[PortfolioResult],
                            top_by_alpha:  list[PortfolioResult],
                            current_portfolio: tuple,
                            current_result: Optional[PortfolioResult],
                            elapsed: float) -> str:
    lines = [
        "=" * 70,
        "  UNIVERSE BACKTEST — BÚSQUEDA DE PORTFOLIO ÓPTIMO",
        f"  {datetime.now().strftime('%d/%m/%Y %H:%M')} | {elapsed:.0f}s",
        "=" * 70,
        "",
        "  RANKING INDIVIDUAL DE TICKERS",
        f"  {'Ticker':<8} {'Retorno':>8} {'Sharpe':>7} {'MaxDD':>7} {'AvgScore':>9} {'Calidad':>8}",
        "  " + "─" * 52,
    ]

    for s in sorted(ticker_stats, key=lambda x: x.signal_quality, reverse=True):
        if not s.data_ok:
            lines.append(f"  {s.ticker:<8} {'N/A':>8} {'N/A':>7} {'N/A':>7} {'N/A':>9} {'N/A':>8}  ❌ {s.error}")
            continue
        flag = "⭐" if s.signal_quality > 0.5 else "✅" if s.signal_quality > 0.2 else "🟡" if s.signal_quality > 0 else "🔴"
        lines.append(
            f"  {flag} {s.ticker:<6} {s.total_return:>+8.1%} {s.sharpe:>7.2f} "
            f"{s.max_drawdown:>7.1%} {s.avg_score:>+9.3f} {s.signal_quality:>8.3f}"
        )

    lines += ["", "  TOP 10 PORTFOLIOS — POR SHARPE", "  " + "─" * 65]
    lines.append(f"  {'#':<3} {'Tickers':<28} {'Retorno':>8} {'Sharpe':>7} {'DD':>7} {'α/BH':>7} {'α/SO':>7}")
    for i, r in enumerate(top_by_sharpe[:10], 1):
        current = " ◄ ACTUAL" if tuple(sorted(current_portfolio)) == r.tickers else ""
        lines.append(
            f"  {i:<3} {' '.join(r.tickers):<28} {r.total_return:>+8.1%} "
            f"{r.sharpe:>7.2f} {r.max_drawdown:>7.1%} "
            f"{r.alpha_vs_bh:>+7.1%} {r.alpha_vs_so:>+7.1%}{current}"
        )

    lines += ["", "  TOP 10 PORTFOLIOS — POR ALPHA VS BUY&HOLD", "  " + "─" * 65]
    lines.append(f"  {'#':<3} {'Tickers':<28} {'Retorno':>8} {'Sharpe':>7} {'DD':>7} {'α/BH':>7} {'α/SO':>7}")
    for i, r in enumerate(top_by_alpha[:10], 1):
        current = " ◄ ACTUAL" if tuple(sorted(current_portfolio)) == r.tickers else ""
        lines.append(
            f"  {i:<3} {' '.join(r.tickers):<28} {r.total_return:>+8.1%} "
            f"{r.sharpe:>7.2f} {r.max_drawdown:>7.1%} "
            f"{r.alpha_vs_bh:>+7.1%} {r.alpha_vs_so:>+7.1%}{current}"
        )

    # Portfolio actual como referencia
    if current_result:
        lines += [
            "",
            "  PORTFOLIO ACTUAL COMO REFERENCIA",
            f"  {' '.join(sorted(current_portfolio))}: "
            f"ret={current_result.total_return:+.1%}  "
            f"sharpe={current_result.sharpe:.2f}  "
            f"dd={current_result.max_drawdown:.1%}  "
            f"α/BH={current_result.alpha_vs_bh:+.1%}",
        ]

    # Recomendación
    best = top_by_sharpe[0] if top_by_sharpe else None
    if best and current_result:
        gain_sharpe = best.sharpe - current_result.sharpe
        gain_alpha  = best.alpha_vs_bh - current_result.alpha_vs_bh
        lines += [
            "",
            "  RECOMENDACIÓN",
            f"  Mejor portfolio encontrado: {' '.join(best.tickers)}",
            f"  vs portfolio actual: Sharpe {gain_sharpe:+.2f} | Alpha {gain_alpha:+.1%}",
        ]
        if gain_sharpe > 0.05 or gain_alpha > 0.03:
            lines.append("  → Considerar rotación hacia composición sugerida")
        else:
            lines.append("  → Portfolio actual es competitivo. Cambio no justificado.")

    lines.append("=" * 70)
    return "\n".join(lines)


def format_telegram_universe(top_by_sharpe: list[PortfolioResult],
                               top_by_alpha:  list[PortfolioResult],
                               current_portfolio: tuple,
                               current_result: Optional[PortfolioResult],
                               ticker_stats: list[TickerStats]) -> str:
    best = top_by_sharpe[0] if top_by_sharpe else None

    lines = [
        "🔍 <b>UNIVERSE BACKTEST</b>",
        f"<i>{datetime.now().strftime('%d/%m/%Y %H:%M')}</i>",
        "",
        "📊 <b>Ranking individual (top 8):</b>",
    ]

    quality_icon = {"⭐": "⭐", "✅": "✅", "🟡": "🟡", "🔴": "🔴"}
    for s in sorted([x for x in ticker_stats if x.data_ok],
                    key=lambda x: x.signal_quality, reverse=True)[:8]:
        flag = "⭐" if s.signal_quality > 0.5 else "✅" if s.signal_quality > 0.2 else "🟡" if s.signal_quality > 0 else "🔴"
        lines.append(
            f"  {flag} <b>{s.ticker}</b>  ret={s.total_return:+.0%}  "
            f"sharpe={s.sharpe:.2f}  score={s.avg_score:+.2f}"
        )

    lines += ["", "🏆 <b>Top 5 portfolios por Sharpe:</b>"]
    for i, r in enumerate(top_by_sharpe[:5], 1):
        current = " ◄" if tuple(sorted(current_portfolio)) == r.tickers else ""
        lines.append(
            f"  {i}. <code>{' '.join(r.tickers)}</code>  "
            f"ret={r.total_return:+.0%}  sharpe={r.sharpe:.2f}  α={r.alpha_vs_bh:+.0%}{current}"
        )

    lines += ["", "📈 <b>Top 5 portfolios por Alpha:</b>"]
    for i, r in enumerate(top_by_alpha[:5], 1):
        current = " ◄" if tuple(sorted(current_portfolio)) == r.tickers else ""
        lines.append(
            f"  {i}. <code>{' '.join(r.tickers)}</code>  "
            f"ret={r.total_return:+.0%}  sharpe={r.sharpe:.2f}  α={r.alpha_vs_bh:+.0%}{current}"
        )

    if best and current_result:
        gain_sharpe = best.sharpe - current_result.sharpe
        gain_alpha  = best.alpha_vs_bh - current_result.alpha_vs_bh
        verdict = "✅ Portfolio actual es competitivo" if (gain_sharpe < 0.05 and gain_alpha < 0.03) \
                  else f"⚠️ Considerar rotar a <b>{' '.join(best.tickers)}</b>"
        lines += ["", verdict]

    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────

def run_universe_backtest(universe: list[str], years: int = 2,
                           top_n: int = 12, sizes: list[int] = None,
                           current_portfolio: list[str] = None) -> dict:
    """
    Ejecuta el universe backtest completo.
    Retorna dict con todos los resultados.
    """
    if sizes is None:
        sizes = [4, 5]
    if current_portfolio is None:
        current_portfolio = ["CVX", "NVDA", "MU", "MELI"]

    t0 = time.time()

    # ── Descarga única de datos ───────────────────────────────────────────────
    logger.info(f"Descargando datos para {len(universe)} tickers...")
    try:
        prices = download_prices(universe, years)
    except Exception as e:
        logger.error(f"Error descargando precios: {e}")
        raise

    macro   = download_macro_history(years)
    macro   = macro.reindex(prices.index).ffill().bfill()
    returns = prices.pct_change().fillna(0)

    logger.info(f"Datos: {len(prices)} días | {prices.shape[1]} tickers válidos")

    # ── FASE 1: Score individual ──────────────────────────────────────────────
    logger.info("Fase 1: scoring individual de tickers...")
    pipeline_scores = compute_pipeline_scores(prices, macro)

    ticker_stats = []
    for ticker in universe:
        stat = score_ticker_fast(ticker, prices, macro, pipeline_scores)
        ticker_stats.append(stat)
        flag = "✓" if stat.data_ok else "✗"
        logger.info(f"  [{flag}] {ticker}: sharpe={stat.sharpe:.2f}  ret={stat.total_return:+.1%}  quality={stat.signal_quality:.3f}")

    # Top N tickers para combinaciones
    valid_stats = [s for s in ticker_stats if s.data_ok and s.sharpe > -2]
    top_tickers = [s.ticker for s in sorted(valid_stats,
                   key=lambda x: x.signal_quality, reverse=True)[:top_n]]
    logger.info(f"Top {len(top_tickers)} tickers para combinaciones: {top_tickers}")

    # Siempre incluir el portfolio actual si sus tickers tienen datos
    for t in current_portfolio:
        if t in prices.columns and t not in top_tickers:
            top_tickers.append(t)

    # ── Fechas de rebalanceo (una sola vez) ───────────────────────────────────
    rebal_dates = []
    prev_week = None; counter = 0
    for date in prices.index:
        week = date.isocalendar()[:2]
        if week != prev_week:
            counter += 1
            if counter % 2 == 1:
                rebal_dates.append(date)
            prev_week = week
    warmup = 60
    rebal_dates = [d for d in rebal_dates if prices.index.get_loc(d) >= warmup]

    # ── FASE 2: Backtest por combinaciones ────────────────────────────────────
    all_combos = []
    for size in sizes:
        combos = list(itertools.combinations(top_tickers, size))
        all_combos.extend(combos)

    logger.info(f"Fase 2: {len(all_combos)} combinaciones (tamaños {sizes}) sobre top {len(top_tickers)} tickers...")

    portfolio_results = []
    current_result = None
    current_key = tuple(sorted(current_portfolio))

    for i, combo in enumerate(all_combos):
        if i % 50 == 0:
            elapsed = time.time() - t0
            logger.info(f"  {i}/{len(all_combos)} combinaciones | {elapsed:.0f}s")

        result = run_portfolio_backtest_fast(
            list(combo), prices, returns, macro, pipeline_scores, rebal_dates, years
        )
        if result:
            portfolio_results.append(result)
            if result.tickers == current_key:
                current_result = result

    logger.info(f"Completadas {len(portfolio_results)}/{len(all_combos)} combinaciones válidas")

    # ── Ranking ───────────────────────────────────────────────────────────────
    top_by_sharpe = sorted(portfolio_results, key=lambda x: x.sharpe, reverse=True)
    top_by_alpha  = sorted(portfolio_results, key=lambda x: x.alpha_vs_bh, reverse=True)

    # Si el portfolio actual no estaba en las combinaciones, correrlo aparte
    if not current_result:
        logger.info(f"Corriendo portfolio actual {current_portfolio} aparte...")
        current_result = run_portfolio_backtest_fast(
            current_portfolio, prices, returns, macro, pipeline_scores, rebal_dates, years
        )

    elapsed = time.time() - t0

    return {
        "ticker_stats":     ticker_stats,
        "top_by_sharpe":    top_by_sharpe,
        "top_by_alpha":     top_by_alpha,
        "current_result":   current_result,
        "current_portfolio": tuple(sorted(current_portfolio)),
        "elapsed":          elapsed,
        "n_combos":         len(all_combos),
        "n_valid":          len(portfolio_results),
    }


async def main(universe: list[str], years: int, top_n: int,
               sizes: list[int], current_portfolio: list[str],
               no_telegram: bool):

    logger.info(f"Universe backtest: {len(universe)} tickers | top {top_n} | sizes {sizes} | {years}y")

    results = run_universe_backtest(
        universe=universe,
        years=years,
        top_n=top_n,
        sizes=sizes,
        current_portfolio=current_portfolio,
    )

    # Reporte consola
    report = format_universe_report(
        ticker_stats=results["ticker_stats"],
        top_by_sharpe=results["top_by_sharpe"],
        top_by_alpha=results["top_by_alpha"],
        current_portfolio=results["current_portfolio"],
        current_result=results["current_result"],
        elapsed=results["elapsed"],
    )
    print(report)
    logger.info(f"Universe backtest: {results['n_valid']}/{results['n_combos']} combos válidas en {results['elapsed']:.0f}s")

    # Telegram
    if not no_telegram:
        try:
            cfg = get_config()
            notifier = TelegramNotifier(cfg.scraper.telegram_bot_token,
                                         cfg.scraper.telegram_chat_id)
            tg = format_telegram_universe(
                top_by_sharpe=results["top_by_sharpe"],
                top_by_alpha=results["top_by_alpha"],
                current_portfolio=results["current_portfolio"],
                current_result=results["current_result"],
                ticker_stats=results["ticker_stats"],
            )
            notifier.send_raw(tg)
            logger.info("Reporte enviado a Telegram")
        except Exception as e:
            logger.warning(f"Telegram fallo: {e}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Universe backtest — búsqueda de portfolio óptimo")
    p.add_argument("--tickers",   nargs="+", default=DEFAULT_UNIVERSE,
                   help="Universo de tickers a evaluar")
    p.add_argument("--current",   nargs="+", default=["CVX", "NVDA", "MU", "MELI"],
                   help="Portfolio actual (para comparar)")
    p.add_argument("--years",     type=int,  default=2)
    p.add_argument("--top",       type=int,  default=12,
                   help="Top N tickers para armar combinaciones")
    p.add_argument("--size",      type=int,  nargs="+", default=[4, 5],
                   help="Tamaños de portfolio a probar (ej: 4 5)")
    p.add_argument("--no-telegram", action="store_true")
    args = p.parse_args()

    asyncio.run(main(
        universe=args.tickers,
        years=args.years,
        top_n=args.top,
        sizes=args.size,
        current_portfolio=args.current,
        no_telegram=args.no_telegram,
    ))
    
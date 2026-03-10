"""
scripts/backtest_walkforward.py — Walk-forward validation del sistema cuantitativo

Metodología:
  - Descarga N años de datos (default: 3)
  - Corre ventanas rodantes in-sample / out-of-sample
  - Mide consistencia: cuántas ventanas gana, pierde, y cuánto depende de un período fuerte

Diseño de ventanas:
  - train_months  : historia usada para calibrar scores (warmup del pipeline)
  - test_months   : período de evaluación out-of-sample
  - step_months   : avance entre ventanas

Default (3 años de datos):
  train=6m, test=3m, step=3m → ~8-10 ventanas independientes

Métricas por ventana:
  - retorno optimizer / score-only / BH
  - Sharpe
  - max drawdown
  - alpha vs score-only
  - alpha vs BH
  - trades y costo

Métricas de consistencia (resumen final):
  - % ventanas donde optimizer > BH
  - % ventanas donde optimizer > score-only
  - contribución de cada ventana al retorno total
  - dependencia de subperíodo (si una ventana aporta >40% del retorno total → alerta)

Uso:
  python scripts/backtest_walkforward.py
  python scripts/backtest_walkforward.py --tickers CVX NVDA MU MELI --years 3
  python scripts/backtest_walkforward.py --train 6 --test 3 --step 3
  python scripts/backtest_walkforward.py --no-telegram
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd

from src.core.config import get_config
from src.core.logger import get_logger
from src.collector.notifier import TelegramNotifier

from scripts.backtest import (
    download_prices, download_macro_history,
    compute_pipeline_scores, optimize_weights, select_method,
    sharpe_ratio, max_drawdown,
    RF_ANNUAL, W_MIN, W_MAX, TOTAL_COST, INITIAL_CASH,
)

logger = get_logger(__name__)


# ── Dataclasses ───────────────────────────────────────────────────────────────

@dataclass
class WindowResult:
    window_id:    int
    start:        datetime
    end:          datetime
    n_days:       int
    n_trades:     int

    # Optimizer
    ret_opt:      float
    sharpe_opt:   float
    dd_opt:       float

    # Score-only
    ret_so:       float
    sharpe_so:    float
    dd_so:        float

    # Buy & Hold
    ret_bh:       float
    sharpe_bh:    float
    dd_bh:        float

    # Alpha
    alpha_vs_bh:  float   # opt - bh
    alpha_vs_so:  float   # opt - so

    # Veredictos
    beats_bh:     bool
    beats_so:     bool
    cost_total:   float


@dataclass
class WalkForwardResult:
    tickers:       list[str]
    windows:       list[WindowResult]
    train_months:  int
    test_months:   int
    step_months:   int
    total_years:   float

    # Métricas agregadas
    n_windows:         int = 0
    n_beats_bh:        int = 0
    n_beats_so:        int = 0
    pct_beats_bh:      float = 0.0
    pct_beats_so:      float = 0.0

    # Retorno compuesto encadenando ventanas (out-of-sample real)
    compound_ret_opt:  float = 0.0
    compound_ret_so:   float = 0.0
    compound_ret_bh:   float = 0.0

    # Sharpe promedio ponderado por duración
    avg_sharpe_opt:    float = 0.0
    avg_sharpe_so:     float = 0.0
    avg_sharpe_bh:     float = 0.0

    # Max DD en todo el período concatenado
    worst_dd_opt:      float = 0.0

    # Dependencia de subperíodo
    max_window_contribution: float = 0.0   # % del retorno total que viene de la mejor ventana
    dominant_window_id:      int   = 0
    subperiod_dependent:     bool  = False  # True si >40% viene de una ventana


# ── Simulación de una ventana ─────────────────────────────────────────────────

def run_window(
    window_id: int,
    prices:    pd.DataFrame,
    returns:   pd.DataFrame,
    macro:     pd.DataFrame,
    pipeline_scores: pd.DataFrame,
    start_idx: int,
    end_idx:   int,
    tickers:   list[str],
    rebal_freq: int = 2,
) -> WindowResult:
    """
    Simula optimizer + score-only + BH en el slice [start_idx:end_idx].
    Warmup interno de 60 días incluido en start_idx.
    """
    n = len(tickers)
    p_slice  = prices.iloc[start_idx:end_idx]
    r_slice  = returns.iloc[start_idx:end_idx]
    sc_slice = pipeline_scores.iloc[start_idx:end_idx]
    m_slice  = macro.reindex(p_slice.index).ffill().bfill()

    all_dates = p_slice.index.tolist()
    if len(all_dates) < 20:
        # Ventana demasiado corta — retornar ceros
        return WindowResult(
            window_id=window_id,
            start=all_dates[0] if all_dates else datetime.now(timezone.utc),
            end=all_dates[-1] if all_dates else datetime.now(timezone.utc),
            n_days=len(all_dates), n_trades=0,
            ret_opt=0, sharpe_opt=0, dd_opt=0,
            ret_so=0,  sharpe_so=0,  dd_so=0,
            ret_bh=0,  sharpe_bh=0,  dd_bh=0,
            alpha_vs_bh=0, alpha_vs_so=0,
            beats_bh=False, beats_so=False, cost_total=0,
        )

    # Fechas de rebalanceo dentro de la ventana
    rebal_dates = []
    prev_week = None; counter = 0
    for date in all_dates:
        week = date.isocalendar()[:2]
        if week != prev_week:
            counter += 1
            if counter % rebal_freq == 1:
                rebal_dates.append(date)
            prev_week = week

    rebal_set = set(rebal_dates)

    def _drift(w, dr):
        tot = sum(w.get(t, 0) * (1 + dr.get(t, 0)) for t in tickers)
        if tot <= 0: return {t: 1/n for t in tickers}
        return {t: w.get(t, 0) * (1 + dr.get(t, 0)) / tot for t in tickers}

    def _so_weights(scores):
        raw  = {t: max(scores.get(t, 0.0) + 0.5, W_MIN) for t in tickers}
        tot  = sum(raw.values())
        w    = {t: min(v / tot, W_MAX) for t, v in raw.items()}
        tot2 = sum(w.values())
        return {t: v / tot2 for t, v in w.items()}

    def _apply_thresh(w_cur, w_tgt, scores, vix=20.0):
        cost = 0.0; w_eff = dict(w_cur)
        for t in tickers:
            delta = w_tgt.get(t, 0) - w_cur.get(t, 0)
            sc = scores.get(t, 0)
            if delta > 0:
                thresh = 0.06 if sc > 0.20 else 0.10 if sc > 0.10 else 0.18
            else:
                thresh = 0.05 if sc < -0.20 else 0.10 if sc < -0.10 else \
                         0.10 if vix > 30 else 0.15
            if abs(delta) < thresh: continue
            cost += abs(delta) * TOTAL_COST
            w_eff[t] = w_tgt.get(t, 0)
        tot = sum(w_eff.values())
        w_eff = {t: v / tot if tot > 0 else 1/n for t, v in w_eff.items()}
        return w_eff, cost

    # Estado inicial — igual weight
    port_opt = INITIAL_CASH; w_opt = {t: 1/n for t in tickers}
    port_so  = INITIAL_CASH; w_so  = {t: 1/n for t in tickers}
    port_bh  = INITIAL_CASH; bh_w  = {t: 1/n for t in tickers}

    eq_opt = {}; eq_so = {}; eq_bh = {}
    n_trades = 0; cost_total = 0.0

    for i, date in enumerate(all_dates):
        # Índice global para buscar en returns/scores completos
        global_idx = prices.index.get_loc(date)
        dr = {t: float(returns[t].iloc[global_idx]) for t in tickers}

        port_opt *= (1 + sum(w_opt.get(t,0) * dr[t] for t in tickers))
        port_so  *= (1 + sum(w_so.get(t,0)  * dr[t] for t in tickers))
        port_bh  *= (1 + sum(bh_w.get(t,0)  * dr[t] for t in tickers))

        w_opt = _drift(w_opt, dr)
        w_so  = _drift(w_so,  dr)

        if date in rebal_set:
            if date in pipeline_scores.index:
                scores_today = {t: float(pipeline_scores[t].loc[date]) for t in tickers}
            else:
                scores_today = {t: 0.0 for t in tickers}

            # Ventana de retornos para el optimizer (60d hacia atrás desde global_idx)
            win_r = returns.iloc[max(0, global_idx-60):global_idx][tickers]
            vix_today = float(macro["vix"].loc[date]) if "vix" in macro.columns else 20.0

            # Optimizer-lite v2
            w_tgt = optimize_weights(win_r, scores_today, vix=vix_today, w_current=w_opt)
            w_opt_eff, c_opt = _apply_thresh(w_opt, w_tgt, scores_today, vix_today)
            port_opt *= (1 - c_opt); cost_total += c_opt
            n_trades += sum(1 for t in tickers if abs(w_opt_eff.get(t,0) - w_opt.get(t,0)) > 0.001)
            w_opt = w_opt_eff

            # Score-only
            w_so_tgt = _so_weights(scores_today)
            w_so_eff, c_so = _apply_thresh(w_so, w_so_tgt, scores_today, vix_today)
            port_so *= (1 - c_so)
            w_so = w_so_eff

        eq_opt[date] = port_opt
        eq_so[date]  = port_so
        eq_bh[date]  = port_bh

    eq  = pd.Series(eq_opt)
    so  = pd.Series(eq_so)
    bh  = pd.Series(eq_bh)

    r_opt = float(eq.iloc[-1] / eq.iloc[0] - 1)
    r_so  = float(so.iloc[-1] / so.iloc[0] - 1)
    r_bh  = float(bh.iloc[-1] / bh.iloc[0] - 1)

    sr_opt = float(sharpe_ratio(eq.pct_change().dropna()))
    sr_so  = float(sharpe_ratio(so.pct_change().dropna()))
    sr_bh  = float(sharpe_ratio(bh.pct_change().dropna()))

    dd_opt = float(max_drawdown(eq))
    dd_so  = float(max_drawdown(so))
    dd_bh  = float(max_drawdown(bh))

    return WindowResult(
        window_id=window_id,
        start=all_dates[0],
        end=all_dates[-1],
        n_days=len(all_dates),
        n_trades=n_trades,
        ret_opt=round(r_opt, 4),   sharpe_opt=round(sr_opt, 3),  dd_opt=round(dd_opt, 4),
        ret_so=round(r_so, 4),     sharpe_so=round(sr_so, 3),    dd_so=round(dd_so, 4),
        ret_bh=round(r_bh, 4),     sharpe_bh=round(sr_bh, 3),    dd_bh=round(dd_bh, 4),
        alpha_vs_bh=round(r_opt - r_bh, 4),
        alpha_vs_so=round(r_opt - r_so, 4),
        beats_bh=(r_opt > r_bh),
        beats_so=(r_opt > r_so),
        cost_total=round(cost_total, 6),
    )


# ── Walk-forward engine ───────────────────────────────────────────────────────

def run_walkforward(
    tickers:      list[str],
    years:        int   = 3,
    train_months: int   = 6,
    test_months:  int   = 3,
    step_months:  int   = 3,
    rebal_freq:   int   = 2,
) -> WalkForwardResult:

    logger.info(f"Walk-forward: {tickers} | {years}y | train={train_months}m test={test_months}m step={step_months}m")

    # Descargar datos completos una vez
    prices = download_prices(tickers, years)
    macro  = download_macro_history(years)
    macro  = macro.reindex(prices.index).ffill().bfill()
    returns = prices.pct_change().fillna(0)

    logger.info(f"Datos: {len(prices)} días ({prices.index[0].date()} → {prices.index[-1].date()})")

    logger.info("Calculando pipeline scores (vectorizado)...")
    pipeline_scores = compute_pipeline_scores(prices, macro)

    # Construir ventanas
    # train_days: mínimo de historia necesaria antes de cada ventana de test
    # Usamos business days aproximados: 1 mes ≈ 21 días
    DAYS_PER_MONTH = 21
    train_days = train_months * DAYS_PER_MONTH
    test_days  = test_months  * DAYS_PER_MONTH
    step_days  = step_months  * DAYS_PER_MONTH

    n_total = len(prices)
    windows_raw = []
    test_start = train_days   # primera ventana de test empieza después del warmup
    while test_start + test_days <= n_total:
        test_end = test_start + test_days
        windows_raw.append((test_start, test_end))
        test_start += step_days

    logger.info(f"Ventanas generadas: {len(windows_raw)}")

    # Correr cada ventana
    results = WalkForwardResult(
        tickers=tickers,
        windows=[],
        train_months=train_months,
        test_months=test_months,
        step_months=step_months,
        total_years=years,
    )

    for wid, (s, e) in enumerate(windows_raw, start=1):
        w = run_window(
            window_id=wid,
            prices=prices, returns=returns,
            macro=macro, pipeline_scores=pipeline_scores,
            start_idx=s, end_idx=e,
            tickers=tickers, rebal_freq=rebal_freq,
        )
        results.windows.append(w)
        sign_bh = "✓" if w.beats_bh else "✗"
        sign_so = "✓" if w.beats_so else "✗"
        logger.info(
            f"  W{wid} {w.start.strftime('%Y-%m')}→{w.end.strftime('%Y-%m')} | "
            f"opt={w.ret_opt:+.1%} bh={w.ret_bh:+.1%} so={w.ret_so:+.1%} | "
            f"α/BH={w.alpha_vs_bh:+.1%} [{sign_bh}] α/SO={w.alpha_vs_so:+.1%} [{sign_so}]"
        )

    if not results.windows:
        logger.error("Sin ventanas generadas — insuficientes datos")
        return results

    # ── Métricas de consistencia ──────────────────────────────────────────────
    n = len(results.windows)
    results.n_windows   = n
    results.n_beats_bh  = sum(1 for w in results.windows if w.beats_bh)
    results.n_beats_so  = sum(1 for w in results.windows if w.beats_so)
    results.pct_beats_bh = results.n_beats_bh / n
    results.pct_beats_so = results.n_beats_so / n

    # Retorno compuesto encadenando ventanas (cada una arranca desde 1.0)
    compound_opt = 1.0; compound_so = 1.0; compound_bh = 1.0
    for w in results.windows:
        compound_opt *= (1 + w.ret_opt)
        compound_so  *= (1 + w.ret_so)
        compound_bh  *= (1 + w.ret_bh)
    results.compound_ret_opt = round(compound_opt - 1, 4)
    results.compound_ret_so  = round(compound_so  - 1, 4)
    results.compound_ret_bh  = round(compound_bh  - 1, 4)

    # Sharpe promedio ponderado por n_days
    total_days = sum(w.n_days for w in results.windows)
    results.avg_sharpe_opt = round(sum(w.sharpe_opt * w.n_days / total_days for w in results.windows), 3)
    results.avg_sharpe_so  = round(sum(w.sharpe_so  * w.n_days / total_days for w in results.windows), 3)
    results.avg_sharpe_bh  = round(sum(w.sharpe_bh  * w.n_days / total_days for w in results.windows), 3)

    # Worst DD
    results.worst_dd_opt = round(min(w.dd_opt for w in results.windows), 4)

    # Dependencia de subperíodo
    # Contribución de cada ventana al retorno compuesto total
    total_gain = compound_opt - 1
    if abs(total_gain) > 0.001:
        contributions = []
        running = 1.0
        for w in results.windows:
            prev = running
            running *= (1 + w.ret_opt)
            contrib = (running - prev) / compound_opt
            contributions.append((w.window_id, contrib))

        max_contrib = max(contributions, key=lambda x: x[1])
        results.max_window_contribution = round(max_contrib[1], 4)
        results.dominant_window_id      = max_contrib[0]
        results.subperiod_dependent     = max_contrib[1] > 0.40
    else:
        results.max_window_contribution = 0.0
        results.dominant_window_id      = 0
        results.subperiod_dependent     = False

    return results


# ── Reportes ──────────────────────────────────────────────────────────────────

def format_console_report(r: WalkForwardResult) -> str:
    lines = [
        "=" * 72,
        "  WALK-FORWARD VALIDATION — SISTEMA CUANTITATIVO MULTICAPA",
        f"  Tickers: {' '.join(r.tickers)} | {r.total_years}y datos",
        f"  Ventanas: train={r.train_months}m  test={r.test_months}m  step={r.step_months}m",
        "=" * 72,
        "",
        "  RESULTADOS POR VENTANA (out-of-sample)",
        f"  {'W':<3} {'Período':<20} {'OPT':>7} {'SO':>7} {'BH':>7} "
        f"{'α/BH':>7} {'α/SO':>7} {'Shr':>5} {'DD':>7} {'Trades':>7}",
        "  " + "─" * 70,
    ]

    for w in r.windows:
        b_bh = "✓" if w.beats_bh else "✗"
        b_so = "✓" if w.beats_so else "✗"
        period = f"{w.start.strftime('%Y-%m')} → {w.end.strftime('%Y-%m')}"
        lines.append(
            f"  W{w.window_id:<2} {period:<20} "
            f"{w.ret_opt:>+7.1%} {w.ret_so:>+7.1%} {w.ret_bh:>+7.1%} "
            f"{w.alpha_vs_bh:>+6.1%}{b_bh} {w.alpha_vs_so:>+6.1%}{b_so} "
            f"{w.sharpe_opt:>5.2f} {w.dd_opt:>7.1%} {w.n_trades:>7}"
        )

    lines += [
        "",
        "  MÉTRICAS AGREGADAS",
        "  " + "─" * 50,
        f"  {'Ventanas totales':<35} {r.n_windows}",
        f"  {'Gana vs Buy&Hold':<35} {r.n_beats_bh}/{r.n_windows}  ({r.pct_beats_bh:.0%})",
        f"  {'Gana vs Score-only':<35} {r.n_beats_so}/{r.n_windows}  ({r.pct_beats_so:.0%})",
        "",
        f"  {'Retorno compuesto (encadenado)':<35}",
        f"    Optimizer:   {r.compound_ret_opt:>+8.1%}",
        f"    Score-only:  {r.compound_ret_so:>+8.1%}",
        f"    Buy & Hold:  {r.compound_ret_bh:>+8.1%}",
        "",
        f"  {'Sharpe promedio ponderado':<35}",
        f"    Optimizer:   {r.avg_sharpe_opt:>8.3f}",
        f"    Score-only:  {r.avg_sharpe_so:>8.3f}",
        f"    Buy & Hold:  {r.avg_sharpe_bh:>8.3f}",
        "",
        f"  {'Peor drawdown (cualquier ventana)':<35} {r.worst_dd_opt:.1%}",
        "",
        "  ANÁLISIS DE DEPENDENCIA DE SUBPERÍODO",
        "  " + "─" * 50,
    ]

    if r.dominant_window_id > 0:
        dw = next(w for w in r.windows if w.window_id == r.dominant_window_id)
        lines += [
            f"  Ventana dominante: W{r.dominant_window_id} "
            f"({dw.start.strftime('%Y-%m')} → {dw.end.strftime('%Y-%m')})",
            f"  Contribución al retorno total: {r.max_window_contribution:.0%}",
        ]
        if r.subperiod_dependent:
            lines += [
                "  ⚠️  ALERTA: >40% del retorno viene de una sola ventana",
                "     El sistema puede ser dependiente de un subperíodo específico.",
                "     Interpretar resultados con cautela.",
            ]
        else:
            lines.append("  ✅ Retorno distribuido — no hay dependencia fuerte de un subperíodo.")
    else:
        lines.append("  Sin datos suficientes para análisis de dependencia.")

    # Veredicto final
    lines += ["", "  VEREDICTO", "  " + "─" * 50]

    score = 0
    if r.pct_beats_bh >= 0.70:   score += 2
    elif r.pct_beats_bh >= 0.50: score += 1
    if r.pct_beats_so >= 0.60:   score += 2
    elif r.pct_beats_so >= 0.40: score += 1
    if not r.subperiod_dependent: score += 1
    if r.avg_sharpe_opt > r.avg_sharpe_bh: score += 1
    if r.compound_ret_opt > r.compound_ret_bh: score += 1

    if score >= 6:
        verdict = "🟢 EDGE REAL — el sistema muestra consistencia genuina across ventanas."
    elif score >= 4:
        verdict = "🟡 EDGE MODERADO — funciona en la mayoría de ventanas pero con inconsistencias."
    elif score >= 2:
        verdict = "🟠 EDGE DÉBIL — resultados mixtos. Validar más antes de confiar."
    else:
        verdict = "🔴 SIN EDGE CLARO — el sistema no es consistente out-of-sample."

    lines += [
        f"  Score de consistencia: {score}/7",
        f"  {verdict}",
        "=" * 72,
    ]

    return "\n".join(lines)


def format_telegram_report(r: WalkForwardResult) -> str:
    beats_bh_icon = "🟢" if r.pct_beats_bh >= 0.70 else "🟡" if r.pct_beats_bh >= 0.50 else "🔴"
    beats_so_icon = "🟢" if r.pct_beats_so >= 0.60 else "🟡" if r.pct_beats_so >= 0.40 else "🔴"
    dep_icon = "⚠️" if r.subperiod_dependent else "✅"

    score = 0
    if r.pct_beats_bh >= 0.70:   score += 2
    elif r.pct_beats_bh >= 0.50: score += 1
    if r.pct_beats_so >= 0.60:   score += 2
    elif r.pct_beats_so >= 0.40: score += 1
    if not r.subperiod_dependent: score += 1
    if r.avg_sharpe_opt > r.avg_sharpe_bh: score += 1
    if r.compound_ret_opt > r.compound_ret_bh: score += 1

    verdict_map = {
        (6,7): "🟢 EDGE REAL",
        (4,5): "🟡 EDGE MODERADO",
        (2,3): "🟠 EDGE DÉBIL",
        (0,1): "🔴 SIN EDGE",
    }
    verdict = next(v for (lo, hi), v in verdict_map.items() if lo <= score <= hi)

    window_lines = []
    for w in r.windows:
        b = "✓" if w.beats_bh else "✗"
        window_lines.append(
            f"  W{w.window_id} <code>{w.start.strftime('%Y-%m')}→{w.end.strftime('%Y-%m')}</code>  "
            f"opt={w.ret_opt:+.0%}  α/BH={w.alpha_vs_bh:+.0%}{b}  shr={w.sharpe_opt:.2f}"
        )

    lines = [
        "🔄 <b>WALK-FORWARD VALIDATION</b>",
        f"<i>{' '.join(r.tickers)} | train={r.train_months}m test={r.test_months}m</i>",
        "",
        "<b>Ventanas out-of-sample:</b>",
    ] + window_lines + [
        "",
        "<b>Consistencia:</b>",
        f"  {beats_bh_icon} Gana vs B&amp;H: <b>{r.n_beats_bh}/{r.n_windows}</b> ({r.pct_beats_bh:.0%})",
        f"  {beats_so_icon} Gana vs Score-only: <b>{r.n_beats_so}/{r.n_windows}</b> ({r.pct_beats_so:.0%})",
        f"  {dep_icon} Dependencia subperíodo: <b>{r.max_window_contribution:.0%}</b>",
        "",
        "<b>Retorno compuesto (encadenado):</b>",
        f"  Optimizer:  <b>{r.compound_ret_opt:+.0%}</b>",
        f"  Score-only: {r.compound_ret_so:+.0%}",
        f"  Buy&amp;Hold:   {r.compound_ret_bh:+.0%}",
        "",
        f"<b>Sharpe promedio:</b> opt={r.avg_sharpe_opt:.2f}  so={r.avg_sharpe_so:.2f}  bh={r.avg_sharpe_bh:.2f}",
        f"<b>Peor DD:</b> {r.worst_dd_opt:.1%}",
        "",
        f"<b>Veredicto [{score}/7]:</b> {verdict}",
    ]

    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────

async def main(tickers, years, train_months, test_months, step_months, no_telegram):
    result = run_walkforward(
        tickers=tickers, years=years,
        train_months=train_months,
        test_months=test_months,
        step_months=step_months,
    )

    print(format_console_report(result))

    if not no_telegram:
        try:
            cfg = get_config()
            notifier = TelegramNotifier(cfg.scraper.telegram_bot_token,
                                         cfg.scraper.telegram_chat_id)
            notifier.send_raw(format_telegram_report(result))
            logger.info("Reporte enviado a Telegram")
        except Exception as e:
            logger.warning(f"Telegram fallo: {e}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Walk-forward validation del sistema cuantitativo")
    p.add_argument("--tickers",     nargs="+",  default=["CVX", "NVDA", "MU", "MELI"])
    p.add_argument("--years",       type=int,   default=3,
                   help="Años de datos a descargar (default: 3)")
    p.add_argument("--train",       type=int,   default=6,
                   help="Meses de warmup antes de cada ventana de test (default: 6)")
    p.add_argument("--test",        type=int,   default=3,
                   help="Meses de evaluación out-of-sample por ventana (default: 3)")
    p.add_argument("--step",        type=int,   default=3,
                   help="Avance entre ventanas en meses (default: 3)")
    p.add_argument("--no-telegram", action="store_true")
    args = p.parse_args()

    asyncio.run(main(
        tickers=args.tickers,
        years=args.years,
        train_months=args.train,
        test_months=args.test,
        step_months=args.step,
        no_telegram=args.no_telegram,
    ))
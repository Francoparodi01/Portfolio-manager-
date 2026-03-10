from __future__ import annotations

import argparse
import asyncio
import os
import sys
from dataclasses import dataclass

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.backtest import run_backtest, TOTAL_COST


@dataclass
class HorizonSummary:
    label: str
    freq_weeks: int
    total_return: float
    sharpe: float
    max_drawdown: float
    alpha_vs_bh: float
    alpha_vs_so: float
    n_trades: int
    total_cost_pct: float
    avg_cost_per_rebal: float


def _horizon_label(freq: int) -> str:
    if freq == 1:
        return "1 semana"
    if freq == 2:
        return "2 semanas"
    if freq == 4:
        return "1 mes"
    return f"{freq} semanas"


def compare_horizons(tickers: list[str], years: int, freqs: list[int]) -> tuple[list[HorizonSummary], dict[int, object]]:
    summaries: list[HorizonSummary] = []
    results: dict[int, object] = {}

    for freq in freqs:
        r = run_backtest(tickers=tickers, years=years, rebal_freq=freq)
        results[freq] = r
        total_cost_pct = round(sum(t.cost_pct for t in r.trades), 4)
        summaries.append(
            HorizonSummary(
                label=_horizon_label(freq),
                freq_weeks=freq,
                total_return=r.total_return,
                sharpe=r.sharpe,
                max_drawdown=r.max_drawdown,
                alpha_vs_bh=r.alpha,
                alpha_vs_so=r.alpha_vs_score_only,
                n_trades=r.n_trades,
                total_cost_pct=total_cost_pct,
                avg_cost_per_rebal=r.avg_cost_per_rebal,
            )
        )

    return summaries, results


def choose_best(summaries: list[HorizonSummary]) -> HorizonSummary:
    # prioriza alpha vs BH; si están cerca, prioriza Sharpe; si sigue cerca, menos trades
    ranked = sorted(
        summaries,
        key=lambda x: (
            round(x.alpha_vs_bh, 4),
            round(x.sharpe, 4),
            -round(abs(x.max_drawdown), 4),
            -x.n_trades,
        ),
        reverse=True,
    )

    best = ranked[0]
    if len(ranked) >= 2:
        second = ranked[1]
        if abs(best.alpha_vs_bh - second.alpha_vs_bh) <= 0.01 and second.sharpe > best.sharpe:
            best = second
        if abs(best.alpha_vs_bh - second.alpha_vs_bh) <= 0.01 and abs(best.sharpe - second.sharpe) <= 0.05 and second.n_trades < best.n_trades:
            best = second
    return best


def format_console(summaries: list[HorizonSummary], best: HorizonSummary, tickers: list[str], years: int) -> str:
    lines = [
        "=" * 78,
        "  BACKTEST DE HORIZONTE / HOLDING PERIOD",
        f"  Universo: {', '.join(tickers)} | {years} años",
        "  Objetivo: comparar neto de costos 2 semanas vs 1 mes (y otros horizontes)",
        "=" * 78,
        "",
        f"  {'Horizonte':<12} {'Freq':>5} {'Ret':>9} {'Sharpe':>8} {'DD':>8} {'α/BH':>8} {'α/SO':>8} {'Trades':>8} {'Coste total':>12}",
        "  " + "─" * 72,
    ]

    for s in summaries:
        marker = "⭐" if s.freq_weeks == best.freq_weeks else " "
        lines.append(
            f"  {marker}{s.label:<11} {s.freq_weeks:>5} {s.total_return:>+8.1%} {s.sharpe:>8.2f} {s.max_drawdown:>7.1%} "
            f"{s.alpha_vs_bh:>+7.1%} {s.alpha_vs_so:>+7.1%} {s.n_trades:>8} {s.total_cost_pct:>11.2f}%"
        )

    lines += [
        "",
        "  LECTURA RÁPIDA",
        "  " + "─" * 72,
        f"  Mejor horizonte sugerido: {best.label} (cada {best.freq_weeks} semanas)",
        f"  Porque combina α/BH {best.alpha_vs_bh:+.1%}, Sharpe {best.sharpe:.2f}, DD {best.max_drawdown:.1%} y {best.n_trades} trades.",
        "",
        "  Regla práctica:",
        "  - usar este horizonte como holding base",
        "  - revisar antes solo si se invalida la tesis / cambia el score fuerte",
        "  - no sobretradear si el edge neto no mejora después de costos",
        "=" * 78,
    ]
    return "\n".join(lines)


async def main() -> None:
    p = argparse.ArgumentParser(description="Comparar holding periods netos de costos")
    p.add_argument("--tickers", nargs="+", default=["CVX", "NVDA", "MU", "MELI"])
    p.add_argument("--years", type=int, default=2)
    p.add_argument("--freqs", nargs="+", type=int, default=[2, 4], help="Frecuencias en semanas. Default: 2 4")
    args = p.parse_args()

    summaries, _ = compare_horizons(args.tickers, args.years, args.freqs)
    best = choose_best(summaries)
    print(format_console(summaries, best, args.tickers, args.years))


if __name__ == "__main__":
    asyncio.run(main())

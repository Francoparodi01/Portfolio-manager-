"""
scripts/run_opportunity.py — Pipeline de análisis de oportunidades externas.

Responde: "Si hoy tuviera que incorporar candidatos nuevos, ¿cuáles son los mejores?"

Flujo:
  1. Carga posiciones actuales desde DB (para contexto de competencia)
  2. Descarga macro (reutiliza fetch_macro)
  3. Screener: filtra universo por liquidez, tendencia, RS, vol
  4. Scorer: técnico + macro + momentum + asimetría upside/downside
  5. Entry Engine: COMPRABLE_AHORA / EN_VIGILANCIA / DESCARTAR
  6. Render HTML → stdout (Telegram lo captura)

Uso:
  python scripts/run_opportunity.py
  python scripts/run_opportunity.py --universe NVDA AMD AVGO MU TSM
  python scripts/run_opportunity.py --no-sentiment
  python scripts/run_opportunity.py --no-telegram
  python scripts/run_opportunity.py --period 1y --max 8
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.config import get_config
from src.core.logger import get_logger
from src.collector.db import PortfolioDatabase
from src.collector.notifier import TelegramNotifier
from src.analysis.macro import fetch_macro, get_macro_regime
from src.analysis.opportunity_screener import (
    run_opportunity_analysis,
    render_opportunity_report,
    COCOS_UNIVERSE_DEFAULT,
)

logger = get_logger(__name__)


async def _load_portfolio(cfg):
    """Carga posiciones actuales y scores del pipeline si existen."""
    db = PortfolioDatabase(cfg.database.url)
    await db.connect()
    try:
        snap = await db.get_latest_snapshot()
        if not snap:
            logger.warning("Sin snapshots en DB — corriendo sin contexto de cartera")
            return [], 0.0, 0.0

        positions = snap.get("positions", [])
        total_ars = float(snap.get("total_value_ars", 0))
        cash_ars  = float(snap.get("cash_ars", 0))
        return positions, total_ars, cash_ars
    finally:
        await db.close()


async def _load_portfolio_scores(cfg, tickers: list[str]) -> dict[str, float]:
    """
    Intenta cargar los scores más recientes del decision_log para tickers del portfolio.
    Esto permite comparar candidatos nuevos vs posiciones actuales.
    """
    db = PortfolioDatabase(cfg.database.url)
    scores = {}
    try:
        await db.connect()
        pool = await db.get_pool()
        if pool and tickers:
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT DISTINCT ON (ticker) ticker, final_score
                    FROM decision_log
                    WHERE ticker = ANY($1::text[])
                    ORDER BY ticker, decided_at DESC
                    """,
                    [t.upper() for t in tickers],
                )
            scores = {r["ticker"]: float(r["final_score"]) for r in rows}
    except Exception as e:
        logger.debug(f"No se pudieron cargar scores históricos: {e}")
    finally:
        try:
            await db.close()
        except Exception:
            pass
    return scores


async def main(
    universe_override:  list[str],
    period:             str,
    no_telegram:        bool,
    no_sentiment:       bool,
    max_candidates:     int,
    min_score:          float = 0.0,
    min_rr:             float = 0.0,
    exclude_portfolio:  bool = True,
):
    cfg      = get_config()
    notifier = TelegramNotifier(cfg.scraper.telegram_bot_token, cfg.scraper.telegram_chat_id)

    # ── 1. Portfolio actual ────────────────────────────────────────────────────
    positions, total_ars, cash_ars = await _load_portfolio(cfg)
    portfolio_tickers = [p.get("ticker", "").upper() for p in positions]
    logger.info(f"Portfolio actual: {portfolio_tickers} | ${total_ars:,.0f} ARS")

    # Scores del pipeline de cartera (para comparación)
    portfolio_scores = await _load_portfolio_scores(cfg, portfolio_tickers)
    if portfolio_scores:
        logger.info(f"Scores cargados para {list(portfolio_scores.keys())}")

    # ── 2. Universo ────────────────────────────────────────────────────────────
    if universe_override:
        universe = [t.upper() for t in universe_override]
        logger.info(f"Universo manual: {universe}")
    else:
        universe = COCOS_UNIVERSE_DEFAULT
        logger.info(f"Universo Cocos default: {len(universe)} tickers")

    # Excluir lo que ya tenemos en cartera (por defecto siempre, a menos que --include-portfolio)
    if exclude_portfolio:
        universe_filtered = [t for t in universe if t.upper() not in set(portfolio_tickers)]
        logger.info(f"Universo final: {len(universe_filtered)} tickers (excluidos {len(portfolio_tickers)} de cartera)")
    else:
        universe_filtered = universe
        logger.info(f"Universo final: {len(universe_filtered)} tickers (portfolio incluido)")

    # ── 3. Macro ───────────────────────────────────────────────────────────────
    logger.info("Descargando macro...")
    macro_snap   = fetch_macro()
    macro_regime = get_macro_regime(macro_snap)
    logger.info(f"Régimen: {macro_regime}")

    # ── 4. Pipeline de oportunidades ──────────────────────────────────────────
    logger.info("Ejecutando análisis de oportunidades...")
    report = run_opportunity_analysis(
        universe            = universe_filtered,
        portfolio_positions = positions,
        macro_snap          = macro_snap,
        macro_regime        = macro_regime,
        period              = period,
        no_sentiment        = no_sentiment,
        portfolio_scores    = portfolio_scores,
        max_candidates      = max_candidates,
        min_score           = min_score,
        min_rr              = min_rr,
        exclude_portfolio   = exclude_portfolio,
    )

    # ── 5. Render ──────────────────────────────────────────────────────────────
    output = render_opportunity_report(report, portfolio_total_ars=total_ars)
    print(output)

    if not no_telegram and cfg.scraper.telegram_enabled:
        logger.info("Enviando a Telegram...")
        notifier.send_raw(output)
        logger.info("Reporte de oportunidades enviado")
    else:
        logger.info("Telegram omitido")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Radar de oportunidades — Cocos Copilot")
    p.add_argument("--universe",     nargs="+", default=[],
                   help="Lista de tickers a evaluar (default: universo Cocos completo)")
    p.add_argument("--period",       default="1y",
                   choices=["3mo", "6mo", "1y", "2y"],
                   help="Período de historia de precios (default: 1y)")
    p.add_argument("--no-telegram",  action="store_true",
                   help="No enviar a Telegram")
    p.add_argument("--no-sentiment", action="store_true",
                   help="Omitir análisis de noticias RSS (más rápido)")
    p.add_argument("--max",              type=int,   default=8,   dest="max_candidates",
                   help="Máximo de candidatos en el reporte (default: 8)")
    p.add_argument("--top",              type=int,   default=0,
                   help="Alias de --max: devolver solo los N mejores setups")
    p.add_argument("--min-score",        type=float, default=0.0, dest="min_score",
                   help="Score mínimo para aparecer en el reporte (ej: 0.15)")
    p.add_argument("--min-rr",           type=float, default=0.0, dest="min_rr",
                   help="R/R mínimo para aparecer en el reporte (ej: 1.5)")
    p.add_argument("--include-portfolio",action="store_true",
                   help="Incluir tickers del portfolio en el análisis (default: excluidos)")
    args = p.parse_args()

    # --top es alias de --max
    max_c = args.top if args.top > 0 else args.max_candidates

    asyncio.run(main(
        universe_override  = args.universe,
        period             = args.period,
        no_telegram        = args.no_telegram,
        no_sentiment       = args.no_sentiment,
        max_candidates     = max_c,
        min_score          = args.min_score,
        min_rr             = args.min_rr,
        exclude_portfolio  = not args.include_portfolio,
    ))

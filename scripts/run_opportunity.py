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
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.config import get_config
from src.core.logger import get_logger
from src.collector.db import PortfolioDatabase
from src.collector.notifier import TelegramNotifier
from src.collector.cocos_history import candles_to_frame
from src.collector.portfolio_quality import enrich_positions_with_market_metadata
from src.analysis.macro import fetch_macro, get_macro_regime
from src.analysis.opportunity_screener import (
    CandidateStatus,
    run_opportunity_analysis,
    render_opportunity_report,
    COCOS_UNIVERSE_DEFAULT,
)

logger = get_logger(__name__)


async def _load_portfolio(cfg, owner_chat_id: int | None = None):
    """Carga posiciones actuales y scores del pipeline si existen."""
    db = PortfolioDatabase(cfg.database.url)
    await db.connect()
    try:
        snap = await db.get_latest_snapshot(owner_chat_id=owner_chat_id)
        if not snap:
            logger.warning("Sin snapshots en DB — corriendo sin contexto de cartera")
            return [], 0.0, 0.0

        positions = snap.get("positions", [])
        try:
            positions = enrich_positions_with_market_metadata(
                positions,
                await db.get_latest_market_prices(),
            )
        except Exception as exc:
            logger.warning("No se pudo auditar frescura de portfolio: %s", exc)
        total_ars = float(snap.get("total_value_ars", 0))
        cash_ars  = float(snap.get("cash_ars", 0))
        return positions, total_ars, cash_ars
    finally:
        await db.close()


async def _load_portfolio_scores(
    cfg,
    tickers: list[str],
    owner_chat_id: int | None = None,
) -> dict[str, float]:
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
                      AND ($2::bigint IS NULL OR owner_chat_id = $2)
                    ORDER BY ticker, decided_at DESC
                    """,
                    [t.upper() for t in tickers],
                    owner_chat_id,
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


async def _load_cocos_universe_assets(cfg) -> list[dict]:
    db = PortfolioDatabase(cfg.database.url)
    await db.connect()
    try:
        return await db.get_cocos_universe_assets()
    finally:
        await db.close()


async def _load_cocos_history_frames(cfg, assets: list[dict], limit: int = 260) -> dict:
    frames = {}
    db = PortfolioDatabase(cfg.database.url)
    await db.connect()
    try:
        for asset in assets:
            rows = await db.get_market_candles(
                asset["ticker"],
                asset_type=asset.get("asset_type"),
                limit=limit,
            )
            frame = candles_to_frame(rows)
            if len(frame) >= 60:
                frames[asset["ticker"]] = frame
    finally:
        await db.close()
    return frames


def _enum_value(value) -> str:
    return str(getattr(value, "value", value) or "")


def _radar_candidate_layers(candidate) -> dict:
    asym = candidate.asymmetry
    edge = candidate.edge
    return {
        "source": "radar",
        "candidate_status": _enum_value(candidate.status),
        "trade_type": _enum_value(candidate.trade_type),
        "technical": {"raw": candidate.tech_score},
        "macro": {"raw": candidate.macro_score},
        "sentiment": {"raw": candidate.sentiment_score},
        "momentum": {"raw": candidate.momentum_score},
        "final_score": candidate.final_score,
        "conviction": candidate.conviction,
        "edge": edge.raw if edge else None,
        "edge_label": _enum_value(edge.label) if edge else "",
        "edge_vs": edge.vs_ticker if edge else "",
        "rr": asym.risk_reward if asym else None,
        "stop_loss_pct": asym.stop_loss_pct if asym else None,
        "asymmetry_ratio": asym.asymmetry_ratio if asym else None,
        "technical_data_source_mode": candidate.technical_candle_source_mode,
        "technical_has_reconstructed_candles": candidate.technical_has_reconstructed_candles,
        "technical_candle_sources": list(candidate.technical_candle_sources or ()),
        "technical_candle_source_counts": dict(candidate.technical_candle_source_counts or {}),
    }


def _radar_decision_type(candidate) -> str:
    status = _enum_value(candidate.status).lower()
    trade_type = _enum_value(candidate.trade_type).lower()
    return f"radar_{status}_{trade_type}".strip("_")


def _radar_is_executable(candidate) -> bool:
    return candidate.status in {
        CandidateStatus.COMPRABLE_AHORA,
        CandidateStatus.COMPRA_HABILITADA,
        CandidateStatus.SWAP_CANDIDATO,
    }


async def _save_radar_candidates(
    cfg,
    report,
    macro_snap,
    macro_regime,
    *,
    portfolio_total_ars: float,
    owner_chat_id: int | None = None,
) -> list[int]:
    """
    Persiste señales del radar como ideas teóricas auditables.
    No representa ejecución real ni modifica el portfolio.
    """
    candidates = [
        c for c in (getattr(report, "candidates", []) or [])
        if c.status not in {CandidateStatus.EXTERNO, CandidateStatus.DESCARTAR}
    ]
    if not candidates:
        return []

    db = PortfolioDatabase(cfg.database.url)
    saved_ids: list[int] = []
    await db.connect()
    try:
        pool = await db.get_pool()
        async with pool.acquire() as conn:
            for candidate in candidates:
                ticker = str(candidate.ticker or "").upper().strip()
                if not ticker:
                    continue

                exists = await conn.fetchval(
                    """
                    SELECT 1
                    FROM decision_log
                    WHERE ticker = $1
                      AND decision = 'BUY'
                      AND decision_date = (NOW() AT TIME ZONE 'America/Argentina/Buenos_Aires')::date
                      AND COALESCE(owner_chat_id, 0) = COALESCE($2::bigint, 0)
                    LIMIT 1
                    """,
                    ticker,
                    owner_chat_id,
                )
                if exists:
                    logger.info("Dedup radar: BUY %s ya existe hoy - skip", ticker)
                    continue

                asym = candidate.asymmetry
                stop_loss_pct = -abs(float(asym.stop_loss_pct)) if asym else None
                rr_ratio = float(asym.risk_reward) if asym and asym.risk_reward else None
                target_pct = (
                    abs(float(asym.stop_loss_pct)) * rr_ratio
                    if asym and rr_ratio and rr_ratio > 0 else None
                )
                theoretical_amount = (
                    float(portfolio_total_ars or 0.0) * float(candidate.sizing_suggested or 0.0)
                )
                is_executable = _radar_is_executable(candidate)
                block_reason = "" if is_executable else (
                    candidate.why_not_now
                    or candidate.action_concreta
                    or _enum_value(candidate.status)
                )
                row = await conn.fetchrow(
                    """
                    INSERT INTO decision_log (
                        owner_chat_id,
                        decided_at,
                        ticker,
                        decision,
                        final_score,
                        confidence,
                        layers,
                        price_at_decision,
                        vix_at_decision,
                        regime,
                        size_pct,
                        stop_loss_pct,
                        target_pct,
                        horizon_days,
                        rr_ratio,
                        decision_type,
                        source,
                        status,
                        block_reason,
                        theoretical_amount_ars,
                        executed_amount_ars,
                        is_executable,
                        was_blocked
                    )
                    VALUES (
                        $1,
                        $2, $3, 'BUY', $4, $5,
                        $6::jsonb,
                        $7, $8, $9,
                        $10, $11, $12, $13, $14,
                        $15, 'radar', $16, $17,
                        $18, 0.0, $19, $20
                    )
                    ON CONFLICT DO NOTHING
                    RETURNING id
                    """,
                    owner_chat_id,
                    datetime.now(timezone.utc),
                    ticker,
                    float(candidate.final_score or 0.0),
                    float(candidate.conviction or 0.0),
                    json.dumps(_radar_candidate_layers(candidate)),
                    float(candidate.price_usd) if candidate.price_usd and candidate.price_usd > 0 else None,
                    float(getattr(macro_snap, "vix", 0.0) or 0.0),
                    str(macro_regime),
                    float(candidate.sizing_suggested or 0.0),
                    stop_loss_pct,
                    target_pct,
                    20,
                    rr_ratio,
                    _radar_decision_type(candidate),
                    "THEORETICAL" if is_executable else "BLOCKED",
                    block_reason,
                    theoretical_amount,
                    bool(is_executable),
                    bool(not is_executable),
                )
                if row:
                    saved_ids.append(int(row["id"]))
        logger.info("Radar persistido: %s señales auditables", len(saved_ids))
    except Exception as exc:
        logger.error("No se pudieron persistir señales del radar: %s", exc, exc_info=True)
    finally:
        try:
            await db.close()
        except Exception:
            pass
    return saved_ids


async def main(
    universe_override:  list[str],
    period:             str,
    no_telegram:        bool,
    no_sentiment:       bool,
    max_candidates:     int,
    min_score:          float = 0.0,
    min_rr:             float = 0.0,
    exclude_portfolio:  bool = True,
    owner_chat_id:      int | None = None,
):
    cfg      = get_config()
    notifier = TelegramNotifier(cfg.scraper.telegram_bot_token, cfg.scraper.telegram_chat_id)

    # ── 1. Portfolio actual ────────────────────────────────────────────────────
    positions, total_ars, cash_ars = await _load_portfolio(
        cfg,
        owner_chat_id=owner_chat_id,
    )
    portfolio_tickers = [p.get("ticker", "").upper() for p in positions]
    logger.info(f"Portfolio actual: {portfolio_tickers} | ${total_ars:,.0f} ARS")

    # Scores del pipeline de cartera (para comparación)
    portfolio_scores = await _load_portfolio_scores(
        cfg,
        portfolio_tickers,
        owner_chat_id=owner_chat_id,
    )
    if portfolio_scores:
        logger.info(f"Scores cargados para {list(portfolio_scores.keys())}")

    # ── 2. Universo ────────────────────────────────────────────────────────────
    cocos_assets = await _load_cocos_universe_assets(cfg)
    assets_by_ticker = {asset["ticker"]: asset for asset in cocos_assets}

    if universe_override:
        universe = [t.upper() for t in universe_override]
        universe_assets = [
            assets_by_ticker[ticker]
            for ticker in universe
            if ticker in assets_by_ticker
        ]
        logger.info(f"Universo manual: {universe}")
    else:
        universe_assets = cocos_assets
        universe = [asset["ticker"] for asset in universe_assets] or COCOS_UNIVERSE_DEFAULT
        logger.info(f"Universo Cocos DB: {len(universe)} tickers")

    # Excluir lo que ya tenemos en cartera (por defecto siempre, a menos que --include-portfolio)
    if exclude_portfolio:
        universe_filtered = [t for t in universe if t.upper() not in set(portfolio_tickers)]
        logger.info(f"Universo final: {len(universe_filtered)} tickers (excluidos {len(portfolio_tickers)} de cartera)")
    else:
        universe_filtered = universe
        logger.info(f"Universo final: {len(universe_filtered)} tickers (portfolio incluido)")

    filtered_assets = [
        asset
        for asset in universe_assets
        if asset["ticker"] in set(universe_filtered)
    ]
    asset_types = {
        asset["ticker"]: asset.get("asset_type", "UNKNOWN")
        for asset in filtered_assets
    }
    history_frames = await _load_cocos_history_frames(cfg, filtered_assets)
    logger.info(
        "Historial Cocos disponible para oportunidades: %s/%s tickers",
        len(history_frames),
        len(universe_filtered),
    )

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
        history_frames      = history_frames,
        asset_types          = asset_types,
        available_cash_ars  = cash_ars,
    )

    # ── 5. Render ──────────────────────────────────────────────────────────────
    await _save_radar_candidates(
        cfg,
        report,
        macro_snap,
        macro_regime,
        portfolio_total_ars=total_ars,
        owner_chat_id=owner_chat_id,
    )

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
    p.add_argument("--owner-chat-id", type=int, default=None)
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
        owner_chat_id      = args.owner_chat_id,
    ))

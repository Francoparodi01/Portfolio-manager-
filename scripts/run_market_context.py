"""One-shot market/news context report.

Ticker-specific news is retrieved through the active entity-matched provider
(Marketaux by default) and scored locally with FinBERT. Broad RSS feeds remain
available only for macro/market context. The report is decision-support only: it
never writes to decision_log and never changes planner thresholds.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.macro import fetch_macro
from src.analysis.news_retrieval_tool import get_news_context
from src.analysis.nlp_scorer import (
    DEFAULT_MODEL,
    DEFAULT_MODEL_REVISION,
    DEFAULT_OLLAMA_URL,
    score_pending_items,
)
from src.analysis.sentiment_fetcher import (
    fetch_raw_sentiment_items,
    get_sentiment_sources,
    load_active_portfolio_tickers,
    save_raw_sentiment_items,
)
from src.analysis.sentiment_symbols import expand_news_symbols
from src.analysis.signal_aggregator import (
    ACTIVE_SENTIMENT_SCORER,
    ACTIVE_TICKER_RETRIEVAL_POLICY,
    AGGREGATION_POLICY,
    aggregate_sentiment,
)
from src.collector.db import PortfolioDatabase
from src.collector.notifier import TelegramNotifier
from src.core.config import get_config
from src.core.logger import get_logger
from src.core.telegram_format import ART, html_text, validate_telegram_html

logger = get_logger(__name__)


def _fmt_dt(value: Any) -> str:
    if not value:
        return "N/A"
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except Exception:
            return str(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(ART).strftime("%d/%m %H:%M")


def _num(value: Any, decimals: int = 1) -> str:
    if value is None:
        return "N/A"
    try:
        formatted = f"{float(value):,.{decimals}f}"
        return formatted.replace(",", "_").replace(".", ",").replace("_", ".")
    except Exception:
        return "N/A"


def _chg(value: Any) -> str:
    if value is None:
        return "N/A"
    try:
        return f"{float(value):+.1f}%"
    except Exception:
        return "N/A"


def _score(value: Any) -> str:
    try:
        return f"{float(value):+.3f}"
    except Exception:
        return "N/A"


def _source_summary(sources: list) -> str:
    tiers = Counter(getattr(src, "trust_tier", "unknown") for src in sources)
    parts = []
    for key in ("official", "market", "local_market", "aggregator", "custom"):
        if tiers.get(key):
            parts.append(f"{key} {tiers[key]}")
    return " | ".join(parts) if parts else "sin fuentes"


def _macro_lines(macro) -> list[str]:
    if not macro:
        return ["Macro no disponible en esta corrida."]
    return [
        (
            "Global: "
            f"SP500 {_num(macro.sp500, 0)} ({_chg(macro.sp500_chg)}) | "
            f"Dow {_num(macro.dow, 0)} ({_chg(macro.dow_chg)}) | "
            f"VIX {_num(macro.vix, 1)} ({_chg(macro.vix_chg)})"
        ),
        (
            "Tasas/dolar: "
            f"10Y {_num(macro.tnx, 2)} | "
            f"DXY {_num(macro.dxy, 1)} ({_chg(macro.dxy_chg)})"
        ),
        (
            "Commodities: "
            f"WTI {_num(macro.wti, 1)} ({_chg(macro.wti_chg)}) | "
            f"Brent {_num(macro.brent, 1)} ({_chg(macro.brent_chg)}) | "
            f"Gold {_num(macro.gold, 1)} ({_chg(macro.gold_chg)})"
        ),
        (
            "Argentina: "
            f"Merval {_num(macro.merval, 0)} ({_chg(macro.merval_chg)}) | "
            f"CCL ${_num(macro.ccl, 1)} | MEP ${_num(macro.mep, 1)} | "
            f"Riesgo pais {_num(macro.riesgo_pais, 0)} pb"
        ),
    ]


def _market_tone(macro, aggregates: list[dict]) -> tuple[str, list[str]]:
    reasons: list[str] = []
    score = 0
    if macro:
        if (macro.sp500_chg or 0) > 0.4 or (macro.dow_chg or 0) > 0.4:
            score += 1
            reasons.append("equity US acompana")
        if (macro.sp500_chg or 0) < -0.4 or (macro.dow_chg or 0) < -0.4:
            score -= 1
            reasons.append("equity US presiona")
        if (macro.vix_chg or 0) > 5 or (macro.vix or 0) > 25:
            score -= 1
            reasons.append("volatilidad sube")
        if (macro.vix_chg or 0) < -5:
            score += 1
            reasons.append("volatilidad baja")
        if (macro.wti_chg or 0) > 2 or (macro.brent_chg or 0) > 2:
            reasons.append("petroleo en movimiento fuerte")
        if (macro.dxy_chg or 0) > 0.5:
            score -= 1
            reasons.append("dolar global firme")

    macro_contexts = [a for a in aggregates if str(a.get("ticker")) == "MACRO"]
    if macro_contexts:
        sent = float(macro_contexts[0].get("score") or 0.0)
        if sent > 0.15:
            score += 1
            reasons.append("noticias macro positivas")
        elif sent < -0.15:
            score -= 1
            reasons.append("noticias macro negativas")

    if score >= 2:
        return "risk-on cauteloso", reasons
    if score <= -2:
        return "risk-off / defensivo", reasons
    return "mixto / sin confirmacion fuerte", reasons


async def _latest_portfolio_tickers(conn, owner_chat_id: int | None) -> list[str]:
    owner_clause = ""
    args: list[Any] = []
    if owner_chat_id is not None:
        owner_clause = "WHERE owner_chat_id = $1"
        args.append(int(owner_chat_id))
    snap = await conn.fetchrow(
        f"""
        SELECT snapshot_id
        FROM portfolio_snapshots
        {owner_clause}
        ORDER BY scraped_at DESC
        LIMIT 1
        """,
        *args,
    )
    if not snap:
        return []
    rows = await conn.fetch(
        """
        SELECT DISTINCT ticker
        FROM positions
        WHERE snapshot_id = $1
        ORDER BY ticker
        """,
        snap["snapshot_id"],
    )
    return [str(row["ticker"]).upper() for row in rows]


def _general_sources() -> list:
    return [
        source
        for source in get_sentiment_sources([])
        if source.category != "ticker_news"
        and not source.name.startswith("yahoo_finance_ticker_")
    ]


async def _refresh_sentiment(
    conn,
    *,
    max_items_per_source: int,
    score_limit: int,
    lookback_hours: int,
    model: str,
    revision: str,
    ollama_url: str,
    timeout_seconds: float,
) -> tuple[dict[str, Any], list]:
    sources = _general_sources()
    active_tickers = await load_active_portfolio_tickers(conn)
    news_tickers = expand_news_symbols(active_tickers)
    provider = os.getenv("SENTIMENT_TICKER_NEWS_PROVIDER", "marketaux").strip().lower() or "marketaux"

    general_items = await fetch_raw_sentiment_items(
        sources=sources,
        max_items_per_source=max_items_per_source,
    )
    ticker_items = []
    retrieval = {
        "status": "disabled",
        "calls": 0,
        "accepted_associations": 0,
        "rejected_associations": 0,
    }
    if provider == "marketaux":
        ticker_items, retrieval = await get_news_context(
            news_tickers,
            lookback_hours=int(os.getenv("SENTIMENT_MARKETAUX_LOOKBACK_HOURS", "72")),
        )
    elif provider not in {"", "none", "disabled"}:
        retrieval["status"] = "unsupported_provider"
        logger.warning("unsupported ticker news provider=%s; failing closed", provider)

    items = general_items + ticker_items
    saved = await save_raw_sentiment_items(conn, items)
    scoring = await score_pending_items(
        conn,
        limit=score_limit,
        model=model,
        revision=revision,
        ollama_url=ollama_url,
        timeout_seconds=timeout_seconds,
    )
    aggregation = await aggregate_sentiment(conn, window_hours=lookback_hours)

    stats: dict[str, Any] = {
        "raw_items": len(items),
        "raw_saved": int(saved),
        "general_raw_items": len(general_items),
        "ticker_raw_items": len(ticker_items),
        "portfolio_tickers": len(active_tickers),
        "news_tickers": len(news_tickers),
        "ticker_news_provider": provider,
        "ticker_retrieval_status": str(retrieval.get("status") or "unknown"),
        "ticker_retrieval_calls": int(retrieval.get("calls") or 0),
        "ticker_retrieval_accepted": int(retrieval.get("accepted_associations") or 0),
        "ticker_retrieval_rejected": int(retrieval.get("rejected_associations") or 0),
        "score_pending": int(scoring.get("pending") or 0),
        "score_scored": int(scoring.get("scored") or 0),
        "score_failed": int(scoring.get("failed") or 0),
        "aggregated": int(aggregation.get("upserts") or 0),
        "backend": ACTIVE_SENTIMENT_SCORER,
        "aggregation_policy": AGGREGATION_POLICY,
        "ticker_retrieval_policy": ACTIVE_TICKER_RETRIEVAL_POLICY,
    }
    return stats, sources


async def _load_context_rows(
    conn,
    *,
    lookback_hours: int,
    top: int,
) -> tuple[list[dict], list[dict], dict]:
    events = await conn.fetch(
        """
        WITH latest AS (
            SELECT DISTINCT ON (ss.raw_id)
                sr.source,
                sr.url,
                sr.headline,
                COALESCE(sr.published_at, sr.fetched_at) AS event_ts,
                sr.raw_payload,
                ss.ticker,
                ss.asset_scope,
                ss.score,
                ss.impact,
                ss.confidence,
                ss.horizon,
                ss.event_type,
                ss.summary,
                ss.scored_at
            FROM sentiment_scored ss
            JOIN sentiment_raw sr ON sr.id = ss.raw_id
            WHERE ss.status = 'SCORED'
              AND ss.scorer = $1
              AND COALESCE(sr.published_at, sr.fetched_at) <= NOW()
              AND COALESCE(sr.published_at, sr.fetched_at) >= NOW() - ($2::int * INTERVAL '1 hour')
              AND (
                  COALESCE(ss.asset_scope, 'unknown') <> 'ticker'
                  OR sr.raw_payload->>'retrieval_policy' = $3
              )
            ORDER BY ss.raw_id, ss.scored_at DESC
        )
        SELECT *
        FROM latest
        ORDER BY
            (ABS(COALESCE(score, 0)) * COALESCE(confidence, 0)) DESC,
            event_ts DESC
        LIMIT $4
        """,
        ACTIVE_SENTIMENT_SCORER,
        int(lookback_hours),
        ACTIVE_TICKER_RETRIEVAL_POLICY,
        int(top),
    )
    aggregates = await conn.fetch(
        """
        SELECT DISTINCT ON (ticker, asset_scope)
            ticker, asset_scope, score, confidence, event_count,
            high_impact_count, top_summary, sources, bucket_ts
        FROM sentiment_aggregated
        WHERE bucket_ts >= NOW() - ($1::int * INTERVAL '1 hour')
          AND sources->>'_policy' = $2
          AND sources->>'_scorer' = $3
          AND sources->>'_ticker_retrieval_policy' = $4
        ORDER BY ticker, asset_scope, bucket_ts DESC
        """,
        int(lookback_hours),
        AGGREGATION_POLICY,
        ACTIVE_SENTIMENT_SCORER,
        ACTIVE_TICKER_RETRIEVAL_POLICY,
    )
    counts = await conn.fetchrow(
        """
        SELECT
            COUNT(*) FILTER (
                WHERE fetched_at >= NOW() - ($1::int * INTERVAL '1 hour')
                  AND NOT (
                      source LIKE 'yahoo_finance_ticker_%'
                      AND COALESCE(raw_payload->>'retrieval_policy', '') <> $2
                  )
            ) AS raw_recent,
            COUNT(*) FILTER (
                WHERE COALESCE(published_at, fetched_at) >= NOW() - ($1::int * INTERVAL '1 hour')
                  AND COALESCE(published_at, fetched_at) <= NOW()
                  AND NOT (
                      source LIKE 'yahoo_finance_ticker_%'
                      AND COALESCE(raw_payload->>'retrieval_policy', '') <> $2
                  )
            ) AS event_recent,
            COUNT(*) FILTER (
                WHERE score_status = 'PENDING_SCORE'
                  AND NOT (
                      source LIKE 'yahoo_finance_ticker_%'
                      AND COALESCE(raw_payload->>'retrieval_policy', '') <> $2
                  )
            ) AS pending_score
        FROM sentiment_raw
        """,
        int(lookback_hours),
        ACTIVE_TICKER_RETRIEVAL_POLICY,
    )
    return [dict(row) for row in events], [dict(row) for row in aggregates], dict(counts or {})


def render_report(
    *,
    pipeline_stats: dict,
    sources: list,
    macro,
    events: list[dict],
    aggregates: list[dict],
    counts: dict,
    portfolio_tickers: list[str],
    lookback_hours: int,
) -> str:
    now = datetime.now(ART)
    tone, tone_reasons = _market_tone(macro, aggregates)
    source_counts = Counter(str(event.get("source") or "unknown") for event in events)
    portfolio_set = set(portfolio_tickers)
    portfolio_context = [
        item for item in aggregates
        if str(item.get("ticker") or "").upper() in portfolio_set
    ]
    non_macro_context = [
        item for item in aggregates
        if str(item.get("ticker") or "").upper() not in {"MACRO", ""}
    ]

    lines: list[str] = [
        "<b>CONTEXTO DE MERCADO Y NOTICIAS</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"Fecha: <b>{now.strftime('%d/%m/%Y %H:%M')} ART</b>",
        "Objetivo: soporte para decisiones; no predice precios ni ejecuta ordenes.",
        "",
        "<b>RESUMEN</b>",
        f"Lectura actual: <b>{html_text(tone)}</b>",
    ]
    if tone_reasons:
        lines.append("Drivers: " + html_text(", ".join(tone_reasons[:5])))
    lines.append(
        "Noticias: "
        f"{int(pipeline_stats.get('raw_items') or 0)} leidas | "
        f"{int(pipeline_stats.get('raw_saved') or 0)} nuevas | "
        f"{int(pipeline_stats.get('score_scored') or 0)} scoreadas."
    )
    lines.append(
        "Ticker retrieval: "
        f"{html_text(pipeline_stats.get('ticker_news_provider') or 'disabled')} | "
        f"{html_text(pipeline_stats.get('ticker_retrieval_status') or 'unknown')} | "
        f"calls {int(pipeline_stats.get('ticker_retrieval_calls') or 0)} | "
        f"aceptadas {int(pipeline_stats.get('ticker_retrieval_accepted') or 0)}"
    )
    lines.append(
        "Estado cola activa: "
        f"{int(counts.get('event_recent') or counts.get('raw_recent') or 0)} eventos recientes {lookback_hours}h | "
        f"{int(counts.get('pending_score') or 0)} pendientes."
    )
    lines.append("")

    lines.append("<b>MACRO / MERCADO</b>")
    for item in _macro_lines(macro):
        lines.append("• " + html_text(item))
    lines.append("")

    lines.append("<b>FUENTES</b>")
    lines.append("Cobertura general: " + html_text(_source_summary(sources)))
    if source_counts:
        top_sources = ", ".join(f"{key} {value}" for key, value in source_counts.most_common(6))
        lines.append("Eventos activos leidos: " + html_text(top_sources))
    lines.append(
        "Ticker-news: solo entity-matched por "
        + html_text(ACTIVE_TICKER_RETRIEVAL_POLICY)
        + "; sin fallback silencioso a Yahoo."
    )
    lines.append("")

    lines.append("<b>PORTFOLIO / TICKERS RELEVANTES</b>")
    if portfolio_context:
        for item in sorted(
            portfolio_context,
            key=lambda value: abs(float(value.get("score") or 0)),
            reverse=True,
        )[:8]:
            ticker = str(item.get("ticker") or "").upper()
            lines.append(
                f"• <b>{html_text(ticker)}</b>: score {_score(item.get('score'))} | "
                f"conf {_num(item.get('confidence'), 2)} | eventos {int(item.get('event_count') or 0)}"
            )
            if item.get("top_summary"):
                lines.append("  " + html_text(item.get("top_summary"), limit=130))
    elif portfolio_tickers:
        lines.append("Sin contexto de noticias activo para holdings en esta ventana.")
    else:
        lines.append("No se pudo leer portfolio actual; se muestra contexto general.")
    lines.append("")

    lines.append("<b>RADAR DE CONTEXTO</b>")
    candidates = [
        item for item in non_macro_context
        if str(item.get("ticker") or "").upper() not in portfolio_set
    ]
    if candidates:
        for item in sorted(
            candidates,
            key=lambda value: abs(float(value.get("score") or 0)),
            reverse=True,
        )[:8]:
            ticker = str(item.get("ticker") or "").upper()
            direction = "positivo" if float(item.get("score") or 0) > 0 else "negativo"
            lines.append(
                f"• <b>{html_text(ticker)}</b>: {html_text(direction)} "
                f"{_score(item.get('score'))} | conf {_num(item.get('confidence'), 2)}"
            )
    else:
        lines.append("Sin tickers externos con contexto fuerte en esta ventana.")
    lines.append("")

    lines.append("<b>EVENTOS DE ALTO IMPACTO</b>")
    important_events = [
        event for event in events
        if str(event.get("impact") or "").lower() == "high"
        or bool(event.get("ticker"))
        or str(event.get("asset_scope") or "").lower() in {"macro", "sector"}
    ]
    if important_events:
        for event in important_events[:8]:
            ticker = str(event.get("ticker") or event.get("asset_scope") or "MACRO").upper()
            source = str(event.get("source") or "unknown")
            impact = str(event.get("impact") or "low").upper()
            summary = event.get("summary") or event.get("headline") or ""
            lines.append(
                f"• <b>{html_text(ticker)}</b> [{html_text(source)} | {html_text(impact)}] "
                f"{_score(event.get('score'))} conf {_num(event.get('confidence'), 2)}"
            )
            lines.append(
                f"  {_fmt_dt(event.get('event_ts'))} — {html_text(summary, limit=160)}"
            )
    else:
        lines.append("Sin eventos activos scoreados en la ventana.")
    lines.append("")

    lines.append("<b>USO OPERATIVO</b>")
    lines.append("• Si el plan del bot coincide con macro/noticias, aumenta confianza contextual.")
    lines.append("• Si contradice macro/noticias, revalidar precio fresco antes de operar.")
    lines.append("• Esto no entra al EV principal y no modifica thresholds ni planner.")
    lines.append("• No reemplaza fills, movimientos Cocos ni performance real.")

    report = "\n".join(lines)
    ok, errors = validate_telegram_html(report)
    if not ok:
        logger.warning("market_context HTML potencialmente invalido: %s", errors[:3])
    return report


async def main(
    *,
    no_telegram: bool,
    max_items_per_source: int,
    score_limit: int,
    lookback_hours: int,
    top: int,
    model: str,
    revision: str,
    ollama_url: str,
    timeout_seconds: float,
    owner_chat_id: int | None,
) -> str:
    cfg = get_config()
    db = PortfolioDatabase(cfg.database.url)
    macro = None
    try:
        macro = fetch_macro()
    except Exception as exc:
        logger.warning("market_context macro unavailable: %s", exc)

    await db.connect()
    try:
        await db.init_schema()
        pool = await db.get_pool()
        if not pool:
            raise RuntimeError("DB pool unavailable")
        async with pool.acquire() as conn:
            pipeline_stats, sources = await _refresh_sentiment(
                conn,
                max_items_per_source=max_items_per_source,
                score_limit=score_limit,
                lookback_hours=lookback_hours,
                model=model,
                revision=revision,
                ollama_url=ollama_url,
                timeout_seconds=timeout_seconds,
            )
            portfolio_tickers = await _latest_portfolio_tickers(conn, owner_chat_id)
            events, aggregates, counts = await _load_context_rows(
                conn,
                lookback_hours=lookback_hours,
                top=top,
            )
    finally:
        await db.close()

    report = render_report(
        pipeline_stats=pipeline_stats,
        sources=sources,
        macro=macro,
        events=events,
        aggregates=aggregates,
        counts=counts,
        portfolio_tickers=portfolio_tickers,
        lookback_hours=lookback_hours,
    )
    print(report)

    if not no_telegram and cfg.scraper.telegram_enabled:
        TelegramNotifier(
            cfg.scraper.telegram_bot_token,
            cfg.scraper.telegram_chat_id,
        ).send_raw(report)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="One-shot Marketaux + FinBERT market/news context")
    parser.add_argument("--no-telegram", action="store_true")
    parser.add_argument("--max-items-per-source", type=int, default=20)
    parser.add_argument("--score-limit", type=int, default=40)
    parser.add_argument("--lookback-hours", type=int, default=12)
    parser.add_argument("--top", type=int, default=12)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--revision", default=DEFAULT_MODEL_REVISION)
    parser.add_argument("--ollama-url", default=DEFAULT_OLLAMA_URL, help=argparse.SUPPRESS)
    parser.add_argument("--timeout-seconds", type=float, default=5.0, help=argparse.SUPPRESS)
    parser.add_argument("--owner-chat-id", type=int, default=None)
    args = parser.parse_args()

    asyncio.run(
        main(
            no_telegram=args.no_telegram,
            max_items_per_source=args.max_items_per_source,
            score_limit=args.score_limit,
            lookback_hours=args.lookback_hours,
            top=args.top,
            model=args.model,
            revision=args.revision,
            ollama_url=args.ollama_url,
            timeout_seconds=args.timeout_seconds,
            owner_chat_id=args.owner_chat_id,
        )
    )

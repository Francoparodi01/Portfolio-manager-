"""One-shot market/news context report.

This command refreshes sentiment inputs, scores a bounded queue, aggregates the
latest context and renders a single decision-support report. It never writes to
decision_log and never changes planner thresholds.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from html import escape
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.macro import fetch_macro
from src.analysis.nlp_scorer import (
    DEFAULT_MODEL,
    DEFAULT_OLLAMA_URL,
    rescore_recent_heuristic_items,
    score_pending_items,
)
from src.analysis.sentiment_fetcher import (
    fetch_raw_sentiment_items,
    get_sentiment_sources,
    save_raw_sentiment_items,
)
from src.analysis.signal_aggregator import AGGREGATION_POLICY, aggregate_sentiment
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
    lines = [
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
    return lines


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


async def _load_context_rows(conn, *, lookback_hours: int, top: int) -> tuple[list[dict], list[dict], dict]:
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
              AND COALESCE(sr.published_at, sr.fetched_at) >= NOW() - ($1::int * INTERVAL '1 hour')
            ORDER BY ss.raw_id, ss.scored_at DESC
        )
        SELECT *
        FROM latest
        ORDER BY
            (ABS(COALESCE(score, 0)) * COALESCE(confidence, 0)) DESC,
            event_ts DESC
        LIMIT $2
        """,
        int(lookback_hours),
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
        ORDER BY ticker, asset_scope, bucket_ts DESC
        """,
        int(lookback_hours),
        AGGREGATION_POLICY,
    )
    counts = await conn.fetchrow(
        """
        SELECT
            COUNT(*) FILTER (WHERE fetched_at >= NOW() - ($1::int * INTERVAL '1 hour')) AS raw_recent,
            COUNT(*) FILTER (WHERE COALESCE(published_at, fetched_at) >= NOW() - ($1::int * INTERVAL '1 hour')) AS event_recent,
            COUNT(*) FILTER (WHERE score_status = 'PENDING_SCORE') AS pending_score,
            COUNT(*) FILTER (WHERE score_status = 'SCORED') AS scored_total
        FROM sentiment_raw
        """,
        int(lookback_hours),
    )
    return [dict(r) for r in events], [dict(r) for r in aggregates], dict(counts or {})


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
    source_counts = Counter(str(e.get("source") or "unknown") for e in events)
    portfolio_set = set(portfolio_tickers)
    portfolio_context = [
        a for a in aggregates
        if str(a.get("ticker") or "").upper() in portfolio_set
    ]
    non_macro_context = [
        a for a in aggregates
        if str(a.get("ticker") or "").upper() not in {"MACRO", ""}
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
        f"{int(pipeline_stats.get('raw_saved') or 0)} capturadas, "
        f"{int(pipeline_stats.get('score_scored') or 0)} scoreadas en esta ejecucion."
    )
    if int(pipeline_stats.get("heuristic_rescored") or 0) > 0:
        lines.append(
            "Revalidacion heuristica: "
            f"{int(pipeline_stats.get('heuristic_rescored') or 0)} eventos recientes."
        )
    lines.append(
        "Estado cola: "
        f"{int(counts.get('event_recent') or counts.get('raw_recent') or 0)} eventos recientes {lookback_hours}h | "
        f"{int(counts.get('pending_score') or 0)} pendientes de score."
    )
    lines.append("")

    lines.append("<b>MACRO / MERCADO</b>")
    for item in _macro_lines(macro):
        lines.append("• " + html_text(item))
    lines.append("")

    lines.append("<b>FUENTES</b>")
    lines.append("Cobertura: " + html_text(_source_summary(sources)))
    if source_counts:
        top_sources = ", ".join(f"{k} {v}" for k, v in source_counts.most_common(6))
        lines.append("Eventos leidos: " + html_text(top_sources))
    lines.append("Regla: oficiales/mercado pesan mas; agregadores solo completan contexto.")
    lines.append("")

    lines.append("<b>PORTFOLIO / TICKERS RELEVANTES</b>")
    if portfolio_context:
        for item in sorted(portfolio_context, key=lambda x: abs(float(x.get("score") or 0)), reverse=True)[:8]:
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
        a for a in non_macro_context
        if str(a.get("ticker") or "").upper() not in portfolio_set
    ]
    if candidates:
        for item in sorted(candidates, key=lambda x: abs(float(x.get("score") or 0)), reverse=True)[:8]:
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
        lines.append("Sin eventos scoreados en la ventana.")
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
    ollama_url: str,
    timeout_seconds: float,
    owner_chat_id: int | None,
) -> str:
    cfg = get_config()
    db = PortfolioDatabase(cfg.database.url)
    sources = get_sentiment_sources()
    pipeline_stats = {
        "raw_items": 0,
        "raw_saved": 0,
        "score_pending": 0,
        "score_scored": 0,
        "score_failed": 0,
        "heuristic_rescored": 0,
        "aggregated": 0,
    }
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
            items = await fetch_raw_sentiment_items(
                sources=sources,
                max_items_per_source=max_items_per_source,
            )
            pipeline_stats["raw_items"] = len(items)
            pipeline_stats["raw_saved"] = await save_raw_sentiment_items(conn, items)

            stats = await score_pending_items(
                conn,
                limit=score_limit,
                model=model,
                ollama_url=ollama_url,
                timeout_seconds=timeout_seconds,
            )
            pipeline_stats["score_pending"] = int(stats.get("pending", 0))
            pipeline_stats["score_scored"] = int(stats.get("scored", 0))
            pipeline_stats["score_failed"] = int(stats.get("failed", 0))

            rescore_stats = await rescore_recent_heuristic_items(
                conn,
                window_hours=max(lookback_hours, 24),
                limit=max(score_limit * 2, 20),
            )
            pipeline_stats["heuristic_rescored"] = int(rescore_stats.get("rescored", 0))

            agg = await aggregate_sentiment(conn, window_hours=lookback_hours)
            pipeline_stats["aggregated"] = int(agg.get("upserts", 0))

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
    parser = argparse.ArgumentParser(description="One-shot market/news context report")
    parser.add_argument("--no-telegram", action="store_true")
    parser.add_argument("--max-items-per-source", type=int, default=20)
    parser.add_argument("--score-limit", type=int, default=40)
    parser.add_argument("--lookback-hours", type=int, default=12)
    parser.add_argument("--top", type=int, default=12)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--ollama-url", default=DEFAULT_OLLAMA_URL)
    parser.add_argument("--timeout-seconds", type=float, default=5.0)
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
            ollama_url=args.ollama_url,
            timeout_seconds=args.timeout_seconds,
            owner_chat_id=args.owner_chat_id,
        )
    )

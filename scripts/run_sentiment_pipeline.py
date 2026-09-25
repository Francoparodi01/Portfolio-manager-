"""Run the point-in-time FinBERT sentiment pipeline.

The pipeline is contextual/auditable. It replaces the previous sentiment
scorer in the existing sentiment slot but does not alter planner thresholds.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.nlp_scorer import (
    DEFAULT_MODEL,
    DEFAULT_MODEL_REVISION,
    DEFAULT_OLLAMA_URL,
    rescore_recent_items,
    score_pending_items,
)
from src.analysis.sentiment_fetcher import (
    fetch_raw_sentiment_items,
    load_active_portfolio_tickers,
    save_raw_sentiment_items,
)
from src.analysis.sentiment_symbols import expand_news_symbols
from src.analysis.signal_aggregator import aggregate_sentiment
from src.collector.db import PortfolioDatabase
from src.core.config import get_config
from src.core.logger import get_logger

logger = get_logger(__name__)


async def main(
    *,
    fetch: bool,
    score: bool,
    aggregate: bool,
    max_items_per_source: int,
    score_limit: int,
    model: str,
    revision: str = DEFAULT_MODEL_REVISION,
    rescore_hours: int = 0,
    ollama_url: str = DEFAULT_OLLAMA_URL,
    timeout_seconds: float = 5.0,
) -> dict:
    # ollama_url/timeout_seconds are accepted for scheduler/CLI compatibility;
    # the FinBERT scorer intentionally ignores them.
    cfg = get_config()
    db = PortfolioDatabase(cfg.database.url)
    result = {
        "raw_items": 0,
        "raw_saved": 0,
        "portfolio_tickers": 0,
        "news_tickers": 0,
        "score_pending": 0,
        "score_scored": 0,
        "score_failed": 0,
        "rescore_candidates": 0,
        "rescored": 0,
        "rescore_failed": 0,
        "aggregated": 0,
        "backend": "finbert",
        "model": model,
        "revision": revision,
    }
    await db.connect()
    try:
        pool = await db.get_pool()
        if not pool:
            raise RuntimeError("DB pool unavailable")

        async with pool.acquire() as conn:
            schema_ready = await conn.fetchval(
                "SELECT to_regclass('public.sentiment_raw') IS NOT NULL"
            )
            if not schema_ready:
                raise RuntimeError("sentiment schema ausente; ejecutar scripts/init_db.py")

            if fetch:
                active_tickers = await load_active_portfolio_tickers(conn)
                news_tickers = expand_news_symbols(active_tickers)
                result["portfolio_tickers"] = len(active_tickers)
                result["news_tickers"] = len(news_tickers)
                items = await fetch_raw_sentiment_items(
                    tickers=news_tickers,
                    max_items_per_source=max_items_per_source,
                )
                result["raw_items"] = len(items)
                result["raw_saved"] = await save_raw_sentiment_items(conn, items)
                logger.info(
                    "sentiment fetch: %s items saved=%s portfolio=%s news_symbols=%s",
                    len(items),
                    result["raw_saved"],
                    active_tickers,
                    news_tickers,
                )

            if rescore_hours > 0:
                stats = await rescore_recent_items(
                    conn,
                    window_hours=rescore_hours,
                    limit=max(score_limit, 500),
                    model=model,
                    revision=revision,
                )
                result["rescore_candidates"] = int(stats.get("candidates", 0))
                result["rescored"] = int(stats.get("rescored", 0))
                result["rescore_failed"] = int(stats.get("failed", 0))
                logger.info("sentiment FinBERT cutover rescore: %s", stats)

            if score:
                stats = await score_pending_items(
                    conn,
                    limit=score_limit,
                    model=model,
                    revision=revision,
                    ollama_url=ollama_url,
                    timeout_seconds=timeout_seconds,
                )
                result["score_pending"] = int(stats.get("pending", 0))
                result["score_scored"] = int(stats.get("scored", 0))
                result["score_failed"] = int(stats.get("failed", 0))
                logger.info("sentiment FinBERT score: %s", stats)

            if aggregate:
                stats = await aggregate_sentiment(conn)
                result["aggregated"] = int(stats.get("upserts", 0))
                logger.info("sentiment aggregate: %s", stats)

    finally:
        await db.close()
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FinBERT sentiment pipeline contextual")
    parser.add_argument("--fetch-only", action="store_true")
    parser.add_argument("--score-only", action="store_true")
    parser.add_argument("--aggregate-only", action="store_true")
    parser.add_argument("--max-items-per-source", type=int, default=25)
    parser.add_argument("--score-limit", type=int, default=20)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--revision", default=DEFAULT_MODEL_REVISION)
    parser.add_argument(
        "--rescore-hours",
        type=int,
        default=0,
        help="Cutover/bootstrap: re-score raw evidence from the last N hours with FinBERT.",
    )
    # Deprecated compatibility flags still used by scheduler deployments built
    # from older runner.py. They no longer control sentiment scoring.
    parser.add_argument("--ollama-url", default=DEFAULT_OLLAMA_URL, help=argparse.SUPPRESS)
    parser.add_argument("--timeout-seconds", type=float, default=5.0, help=argparse.SUPPRESS)
    parser.add_argument("--no-fetch", action="store_true")
    parser.add_argument("--no-score", action="store_true")
    parser.add_argument("--no-aggregate", action="store_true")
    args = parser.parse_args()

    explicit = args.fetch_only or args.score_only or args.aggregate_only
    run_fetch = args.fetch_only or (not explicit and not args.no_fetch)
    run_score = args.score_only or (not explicit and not args.no_score)
    run_aggregate = args.aggregate_only or (not explicit and not args.no_aggregate)

    output = asyncio.run(
        main(
            fetch=run_fetch,
            score=run_score,
            aggregate=run_aggregate,
            max_items_per_source=args.max_items_per_source,
            score_limit=args.score_limit,
            model=args.model,
            revision=args.revision,
            rescore_hours=max(0, args.rescore_hours),
            ollama_url=args.ollama_url,
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(output)

"""Run the sentiment ingestion/scoring/aggregation pipeline.

This script is contextual/auditable by design. It does not change trading
thresholds, planner decisions or execution audit metrics.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.nlp_scorer import DEFAULT_MODEL, DEFAULT_OLLAMA_URL, score_pending_items
from src.analysis.sentiment_fetcher import (
    fetch_raw_sentiment_items,
    load_active_portfolio_tickers,
    save_raw_sentiment_items,
)
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
    ollama_url: str,
    timeout_seconds: float,
) -> dict:
    cfg = get_config()
    db = PortfolioDatabase(cfg.database.url)
    result = {
        "raw_items": 0,
        "raw_saved": 0,
        "yahoo_tickers": 0,
        "score_pending": 0,
        "score_scored": 0,
        "score_failed": 0,
        "aggregated": 0,
    }
    await db.connect()
    try:
        await db.init_schema()
        pool = await db.get_pool()
        if not pool:
            raise RuntimeError("DB pool unavailable")

        async with pool.acquire() as conn:
            if fetch:
                active_tickers = await load_active_portfolio_tickers(conn)
                result["yahoo_tickers"] = len(active_tickers)
                items = await fetch_raw_sentiment_items(
                    tickers=active_tickers,
                    max_items_per_source=max_items_per_source,
                )
                result["raw_items"] = len(items)
                result["raw_saved"] = await save_raw_sentiment_items(conn, items)
                logger.info("sentiment fetch: %s items saved=%s", len(items), result["raw_saved"])

            if score:
                stats = await score_pending_items(
                    conn,
                    limit=score_limit,
                    model=model,
                    ollama_url=ollama_url,
                    timeout_seconds=timeout_seconds,
                )
                result["score_pending"] = int(stats.get("pending", 0))
                result["score_scored"] = int(stats.get("scored", 0))
                result["score_failed"] = int(stats.get("failed", 0))
                logger.info("sentiment score: %s", stats)

            if aggregate:
                stats = await aggregate_sentiment(conn)
                result["aggregated"] = int(stats.get("upserts", 0))
                logger.info("sentiment aggregate: %s", stats)

    finally:
        await db.close()
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sentiment pipeline contextual")
    parser.add_argument("--fetch-only", action="store_true")
    parser.add_argument("--score-only", action="store_true")
    parser.add_argument("--aggregate-only", action="store_true")
    parser.add_argument("--max-items-per-source", type=int, default=25)
    parser.add_argument("--score-limit", type=int, default=20)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--ollama-url", default=DEFAULT_OLLAMA_URL)
    parser.add_argument("--timeout-seconds", type=float, default=5.0)
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
            ollama_url=args.ollama_url,
            timeout_seconds=args.timeout_seconds,
        )
    )
    print(output)

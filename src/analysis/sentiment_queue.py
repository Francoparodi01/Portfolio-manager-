"""Active FinBERT queue policy for entity-matched ticker sentiment.

Historical raw rows remain untouched for auditability. The active queue skips
legacy per-ticker RSS evidence unless the raw item carries the configured
entity-matched retrieval policy. Broad market/macro feeds remain eligible.
"""
from __future__ import annotations

import os
from typing import Any

from .news_retrieval_tool import RETRIEVAL_POLICY
from .nlp_scorer import (
    DEFAULT_MODEL,
    DEFAULT_MODEL_REVISION,
    DEFAULT_SCORER,
    mark_score_attempt,
    save_sentiment_score,
    score_with_finbert,
)

ACTIVE_TICKER_RETRIEVAL_POLICY = os.getenv(
    "SENTIMENT_ACTIVE_TICKER_RETRIEVAL_POLICY",
    RETRIEVAL_POLICY,
).strip() or RETRIEVAL_POLICY


async def load_active_pending_raw_items(
    conn,
    *,
    limit: int = 25,
    max_attempts: int = 3,
) -> list[dict[str, Any]]:
    """Load pending evidence that is eligible for the active sentiment policy."""
    rows = await conn.fetch(
        """
        SELECT id, fetched_at, source, url, headline, body_snippet, published_at, raw_payload
        FROM sentiment_raw
        WHERE score_status = 'PENDING_SCORE'
          AND score_attempts < $1
          AND (
              (
                  COALESCE(raw_payload->>'category', '') <> 'ticker_news'
                  AND source NOT LIKE 'yahoo_finance_ticker_%'
              )
              OR raw_payload->>'retrieval_policy' = $2
          )
        ORDER BY COALESCE(published_at, fetched_at) DESC
        LIMIT $3
        """,
        int(max_attempts),
        ACTIVE_TICKER_RETRIEVAL_POLICY,
        max(0, int(limit)),
    )
    return [dict(row) for row in rows]


async def score_active_pending_items(
    conn,
    *,
    limit: int = 25,
    model: str = DEFAULT_MODEL,
    revision: str = DEFAULT_MODEL_REVISION,
    max_attempts: int = 3,
) -> dict[str, int | str]:
    """Score only rows eligible for the active Marketaux/FinBERT policy."""
    pending = await load_active_pending_raw_items(
        conn,
        limit=limit,
        max_attempts=max_attempts,
    )
    stats: dict[str, int | str] = {
        "pending": len(pending),
        "scored": 0,
        "failed": 0,
        "backend": DEFAULT_SCORER,
        "queue_policy": ACTIVE_TICKER_RETRIEVAL_POLICY,
    }
    for row in pending:
        raw_id = int(row["id"])
        try:
            scored = await score_with_finbert(
                row,
                model=model,
                revision=revision,
            )
            await save_sentiment_score(
                conn,
                scored,
                model=model,
                scorer=DEFAULT_SCORER,
            )
            stats["scored"] = int(stats["scored"]) + 1
        except Exception as exc:
            await mark_score_attempt(conn, raw_id, error=str(exc))
            stats["failed"] = int(stats["failed"]) + 1
            if isinstance(exc, (RuntimeError, OSError, ImportError)):
                break
    return stats

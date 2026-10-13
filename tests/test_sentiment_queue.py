import asyncio
from pathlib import Path

from src.analysis.sentiment_queue import (
    ACTIVE_TICKER_RETRIEVAL_POLICY,
    load_active_pending_raw_items,
    score_active_pending_items,
)


def test_active_queue_excludes_legacy_ticker_rss():
    class _Connection:
        async def fetch(self, query, *params):
            assert "source NOT LIKE 'yahoo_finance_ticker_%'" in query
            assert "raw_payload->>'retrieval_policy' = $2" in query
            assert params[1] == ACTIVE_TICKER_RETRIEVAL_POLICY
            return []

    rows = asyncio.run(load_active_pending_raw_items(_Connection(), limit=40))
    assert rows == []


def test_active_queue_empty_is_noop():
    class _Connection:
        async def fetch(self, _query, *_params):
            return []

    result = asyncio.run(score_active_pending_items(_Connection(), limit=40))
    assert result["pending"] == 0
    assert result["scored"] == 0
    assert result["failed"] == 0
    assert result["queue_policy"] == ACTIVE_TICKER_RETRIEVAL_POLICY


def test_mercado_report_query_is_entity_gated():
    source = Path("scripts/run_market_context.py").read_text(encoding="utf-8")
    assert "ss.scorer = $1" in source
    assert "sr.raw_payload->>'retrieval_policy' = $3" in source
    assert "sources->>'_ticker_retrieval_policy' = $4" in source
    assert "sin fallback silencioso a Yahoo" in source
    assert "rescore_recent_heuristic_items" not in source

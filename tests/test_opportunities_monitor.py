"""Contract tests for the owner-scoped prospective Opportunities read model."""
from datetime import date, datetime, timezone
import unittest
from unittest.mock import AsyncMock

from src.monitor.opportunities import STALE_AFTER_SECONDS, fetch_top_opportunities


class TopOpportunitiesTests(unittest.IsolatedAsyncioTestCase):
    async def test_missing_tables_does_not_fallback_to_historical_rows(self):
        conn = AsyncMock()
        conn.fetchval.return_value = False
        result = await fetch_top_opportunities(conn, owner_chat_id=123)
        self.assertEqual(result["status"], "SCHEMA_MISSING")
        self.assertEqual(result["items"], [])
        conn.fetchrow.assert_not_awaited()

    async def test_no_capture_is_empty(self):
        conn = AsyncMock()
        conn.fetchval.return_value = True
        conn.fetchrow.return_value = None
        result = await fetch_top_opportunities(conn, owner_chat_id=123)
        self.assertEqual(result["status"], "NO_CAPTURE")
        conn.fetch.assert_not_awaited()

    async def test_latest_frozen_rank_owner_and_no_future_outcomes(self):
        now = datetime.now(timezone.utc)
        conn = AsyncMock()
        conn.fetchval.return_value = True
        conn.fetchrow.return_value = {
            "run_id": "frozen-run",
            "captured_at": now,
            "captured_session": date(2026, 10, 15),
            "scoring_version": "v2-sha",
            "universe_count": 273,
            "eligible_count": 87,
            "capture_age_seconds": 60,
        }
        conn.fetch.return_value = [{
            "ticker": "MU", "rank_position": 2, "reference_ts": now,
            "ranking_score": 0.5, "trigger_price": None,
        }]
        result = await fetch_top_opportunities(conn, owner_chat_id=987)
        self.assertEqual(result["status"], "READY")
        self.assertFalse(result["stale"])
        self.assertEqual(result["items"][0]["ticker"], "MU")
        self.assertIsNone(result["items"][0]["trigger_price"])
        run_sql, run_owner = conn.fetchrow.await_args.args
        self.assertEqual(run_owner, 987)
        self.assertIn("owner_chat_id = $1", run_sql)
        sql, selected_run = conn.fetch.await_args.args
        self.assertEqual(selected_run, "frozen-run")
        self.assertIn("ORDER BY rank_position ASC", sql)
        self.assertIn("in_portfolio = FALSE", sql)
        self.assertIn("radar_eligible = TRUE", sql)
        self.assertNotIn("radar_discovery_outcomes", sql)

    async def test_stale_capture_is_explicit(self):
        now = datetime.now(timezone.utc)
        conn = AsyncMock()
        conn.fetchval.return_value = True
        conn.fetchrow.return_value = {
            "run_id": "old", "captured_at": now,
            "captured_session": date(2026, 10, 1),
            "scoring_version": "v2", "universe_count": 1,
            "eligible_count": 1,
            "capture_age_seconds": STALE_AFTER_SECONDS + 10,
        }
        conn.fetch.return_value = []
        result = await fetch_top_opportunities(conn, owner_chat_id=10)
        self.assertTrue(result["stale"])
        self.assertEqual(result["count"], 0)


if __name__ == "__main__":
    unittest.main()

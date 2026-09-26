from __future__ import annotations

import asyncio

from src.analysis.bot_counterfactual import fetch_normalized_bot_counterfactual


class _Conn:
    def __init__(self) -> None:
        self.sql = ""
        self.args = ()

    async def fetchrow(self, sql, *args):
        self.sql = sql
        self.args = args
        return {
            "candidate_plans_total": 54,
            "raw_plans_total": 52,
            "excluded_missing_notional": 2,
            "episodes_total": 17,
            "episodes_closed_5d": 15,
            "episodes_closed_10d": 12,
            "episodes_closed_20d": 5,
            "pnl_5d_ars": -32000,
            "pnl_10d_ars": 18000,
            "pnl_20d_ars": 9000,
            "avg_return_5d": -0.01,
            "avg_return_10d": 0.02,
            "avg_return_20d": 0.03,
        }


def test_normalized_bot_counterfactual_deduplicates_consecutive_plan_episodes():
    conn = _Conn()
    data = asyncio.run(
        fetch_normalized_bot_counterfactual(
            conn,
            days=25,
            owner_chat_id=123,
            legacy_single_owner=False,
        )
    )

    assert data["schema_version"] == "bot-follow-pnl-normalized-v1"
    assert data["lookback_days"] == 25
    assert data["candidate_plans_total"] == 54
    assert data["raw_plans_total"] == 52
    assert data["excluded_missing_notional"] == 2
    assert data["episodes_total"] == 17
    assert data["duplicates_removed"] == 35
    assert data["pnl_5d_ars"] == -32000.0
    assert data["pnl_10d_ars"] == 18000.0
    assert data["pnl_20d_ars"] == 9000.0
    assert "consecutive formal runs" in data["episode_definition"]
    assert "valid positive notional" in data["episode_definition"]

    sql = conn.sql.lower()
    assert "from candidate_plans p" in sql
    assert "lag(p.decision)" in sql
    assert "lag(p.run_seq)" in sql
    assert "row_number()" in sql
    assert "case when e.target_amount_ars is not null" in sql
    assert "episode_row = 1" in sql
    assert "target_amount_ars > 0" in sql
    assert "greatest(" not in sql
    assert "plan_execution_attributions" not in sql
    assert "broker_movements" not in sql
    assert conn.args == (25, 123, False)

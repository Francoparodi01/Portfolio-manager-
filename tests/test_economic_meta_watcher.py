from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.analysis import economic_meta_watcher as watcher_module
from src.analysis.economic_meta_store import EconomicMetaShadowStore
from src.analysis.economic_meta_watcher import EconomicMetaAnalysisWatcher


RUN_ID = "11111111-1111-1111-1111-111111111111"


class FakeConn:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.closed = False

    async def fetch(self, statement, *args):
        self.statements.append(statement)
        stable_at = datetime.now(timezone.utc) - timedelta(minutes=2)
        if "MAX(decided_at)" in statement:
            return [{"run_id": RUN_ID, "last_seen": stable_at}]
        if "MAX(observed_at)" in statement:
            return [{"run_id": RUN_ID, "last_seen": stable_at}]
        if "FROM decision_log" in statement:
            return [
                {
                    "id": 10,
                    "owner_chat_id": 123,
                    "run_id": RUN_ID,
                    "as_of": stable_at,
                    "ticker": "NVDA",
                    "action": "SELL",
                    "final_score": -0.14,
                    "regime": "TRANSITIONAL",
                    "delta_weight": -0.12,
                    "status": "EXECUTED",
                    "decision_type": "executable",
                }
            ]
        if "FROM position_hold_observations" in statement:
            return [
                {
                    "id": 20,
                    "owner_chat_id": 123,
                    "run_id": RUN_ID,
                    "as_of": stable_at,
                    "ticker": "AAPL",
                    "action": "HOLD",
                    "final_score": 0.02,
                    "regime": "TRANSITIONAL",
                    "delta_weight": 0.0,
                    "status": "OBSERVED",
                    "decision_type": "hold_observation",
                }
            ]
        raise AssertionError(statement)

    async def fetchrow(self, statement, *args):
        self.statements.append(statement)
        assert "FROM execution_plans" in statement
        return {
            "gross_sell_ars": 1000.0,
            "fee_sell_ars": 7.5,
            "gross_buy_ars": 1000.0,
            "fee_buy_ars": 7.5,
        }

    async def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_watcher_captures_trade_and_hold_without_production_writes(tmp_path, monkeypatch):
    fake = FakeConn()

    async def fake_connect(_dsn):
        return fake

    monkeypatch.setattr(watcher_module.asyncpg, "connect", fake_connect)
    store_path = tmp_path / "shadow.jsonl"
    state_path = tmp_path / "state.json"
    watcher = EconomicMetaAnalysisWatcher(
        "postgresql+asyncpg://example/db",
        store_path=store_path,
        state_path=state_path,
        settle_seconds=10,
    )

    summary = await watcher.run_once()
    assert summary.runs_seen == 1
    assert summary.candidates_seen == 2
    assert summary.records_written == 6
    assert fake.closed is True
    assert all(statement.lstrip().upper().startswith("SELECT") for statement in fake.statements)

    rows = EconomicMetaShadowStore(store_path).read_all()
    assert len(rows) == 6
    assert {row["ticker"] for row in rows} == {"NVDA", "AAPL"}
    assert {row["policy_name"] for row in rows} == {"META-A", "META-B", "META-C"}

    nvda = [row for row in rows if row["ticker"] == "NVDA"]
    assert {row["candidate_action"] for row in nvda} == {"SELL"}
    assert {row["estimated_cost_bps"] for row in nvda} == {75.0}
    assert {row["portfolio_turnover"] for row in nvda} == {0.12}

    aapl = [row for row in rows if row["ticker"] == "AAPL"]
    assert all(row["decision"] == "REJECT_TO_HOLD" for row in aapl)
    assert all(row["rejection_reason"] == "SOURCE_ALREADY_HOLD" for row in aapl)
    assert all(row["capital_effect"] is False for row in rows)


@pytest.mark.asyncio
async def test_watcher_is_idempotent_per_run_ticker_policy(tmp_path, monkeypatch):
    connections: list[FakeConn] = []

    async def fake_connect(_dsn):
        conn = FakeConn()
        connections.append(conn)
        return conn

    monkeypatch.setattr(watcher_module.asyncpg, "connect", fake_connect)
    store_path = tmp_path / "shadow.jsonl"
    watcher = EconomicMetaAnalysisWatcher(
        "postgresql://example/db",
        store_path=store_path,
        state_path=tmp_path / "state.json",
        settle_seconds=10,
    )

    first = await watcher.run_once()
    second = await watcher.run_once()
    assert first.records_written == 6
    assert second.records_written == 0
    assert len(EconomicMetaShadowStore(store_path).read_all()) == 6


def test_watcher_state_is_shadow_only_and_persistent(tmp_path):
    state_path = tmp_path / "state.json"
    first = EconomicMetaAnalysisWatcher(
        "postgresql://example/db",
        store_path=tmp_path / "shadow.jsonl",
        state_path=state_path,
        settle_seconds=10,
    )
    second = EconomicMetaAnalysisWatcher(
        "postgresql://example/db",
        store_path=tmp_path / "shadow.jsonl",
        state_path=state_path,
        settle_seconds=10,
    )
    assert first.activated_at == second.activated_at
    text = state_path.read_text(encoding="utf-8")
    assert '"mode": "SHADOW_ONLY"' in text
    assert '"capital_effect": false' in text

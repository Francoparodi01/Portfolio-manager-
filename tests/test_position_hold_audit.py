import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

from src.analysis import position_hold_audit
from src.analysis.position_hold_audit import (
    hold_observations_from_plan,
    persist_position_holds,
    resolve_position_hold_outcomes,
)


class _FakeConn:
    def __init__(self, rows=None):
        self.rows = list(rows or [])
        self.calls = []

    async def execute(self, query, *args):
        self.calls.append((query, args))
        if "INSERT INTO position_hold_observations" in query:
            return "INSERT 0 1"
        if "UPDATE position_hold_observations" in query:
            return "UPDATE 1"
        return "OK"

    async def fetch(self, query, *args):
        self.calls.append((query, args))
        return self.rows


def test_hold_observations_use_final_planner_action_only():
    hold = SimpleNamespace(
        ticker="AMD",
        action=SimpleNamespace(value="HOLD"),
        score=-0.02,
        conviction=0.6,
        current_weight=0.03,
        target_weight=0.02,
        delta_weight=-0.01,
        reason_primary="Una señal neutral no justifica operar",
        reason_secondary="Optimizer sugirió reducir",
    )
    buy = SimpleNamespace(ticker="YPFD", action=SimpleNamespace(value="BUY"))

    observations = hold_observations_from_plan(
        [hold, buy],
        result_by_ticker={"AMD": SimpleNamespace(final_score=-0.02)},
        price_by_ticker={"AMD": 76500.0, "YPFD": 7815.0},
        regime="NORMAL",
        portfolio_snapshot_id="2026-08-14T17:02:00-03:00",
        layers_by_ticker={"AMD": {"technical": {"score": -0.02}}},
    )

    assert len(observations) == 1
    assert observations[0]["ticker"] == "AMD"
    assert observations[0]["reference_price"] == 76500.0
    assert observations[0]["reason_primary"] == "Una señal neutral no justifica operar"


def test_hold_persistence_is_separate_from_decision_log():
    conn = _FakeConn()
    run_id = uuid4()
    plan_id = uuid4()

    saved = asyncio.run(
        persist_position_holds(
            conn,
            owner_chat_id=123,
            run_id=run_id,
            execution_plan_id=plan_id,
            observed_at=datetime(2026, 8, 14, 20, 2, tzinfo=timezone.utc),
            observations=[{
                "ticker": "AMD",
                "final_score": -0.02,
                "confidence": 0.6,
                "reference_price": 76500,
                "layers": {},
            }],
        )
    )

    assert saved == 1
    sql = "\n".join(query for query, _ in conn.calls)
    assert "INSERT INTO position_hold_observations" in sql
    assert "ON CONFLICT (run_id, ticker) DO NOTHING" in sql
    assert "INSERT INTO decision_log" not in sql


def test_hold_outcomes_are_raw_returns_and_keep_all_four_horizons(monkeypatch):
    observed_at = datetime(2026, 6, 1, 18, 0, tzinfo=timezone.utc)
    conn = _FakeConn(rows=[{
        "id": 7,
        "ticker": "AMD",
        "observed_at": observed_at,
        "reference_price": 100.0,
        "outcome_5d": None,
        "outcome_10d": None,
        "outcome_20d": None,
        "outcome_40d": None,
    }])

    async def _candles(_conn, _tickers, *, since):
        return {"AMD": []}

    async def _effects(_conn, _tickers, *, since):
        return {"AMD": []}

    def _outcomes(**_kwargs):
        result = {}
        for horizon, value in ((5, 0.05), (10, 0.08), (20, -0.02), (40, 0.12)):
            result[f"outcome_{horizon}d"] = value
            result[f"outcome_date_{horizon}d"] = observed_at.date()
            result[f"outcome_price_{horizon}d"] = 100 * (1 + value)
        return result

    monkeypatch.setattr(position_hold_audit, "fetch_canonical_outcome_candles", _candles)
    monkeypatch.setattr(position_hold_audit, "fetch_outcome_corporate_effects", _effects)
    monkeypatch.setattr(position_hold_audit, "compute_execution_session_outcomes", _outcomes)

    updated = asyncio.run(resolve_position_hold_outcomes(conn, lookback_days=180))

    assert updated == 1
    update_call = next(call for call in conn.calls if "UPDATE position_hold_observations" in call[0])
    assert update_call[1][1:5] == (0.05, 0.08, -0.02, 0.12)
    assert "outcome_basis = 'canonical_cocos'" in update_call[0]

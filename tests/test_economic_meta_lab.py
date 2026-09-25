from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from src.analysis import economic_meta_lab as lab


class FakeConn:
    def __init__(self, rows):
        self.rows = rows
        self.cutoff = None
        self.closed = False

    async def fetchval(self, sql, *args):
        assert sql.strip().upper().startswith("SELECT")
        return "decision_lab_runs"

    async def fetch(self, sql, *args):
        assert sql.strip().upper().startswith("SELECT")
        self.cutoff = args[1]
        return self.rows

    async def close(self):
        self.closed = True


def _summary():
    return {
        "replay_run_id": "lab-run",
        "metrics": [
            {
                "horizon": 20,
                "segment": "contains_ticker=MU",
                "strategy_version": "recorded-production-proposal-v1",
                "population": "PRIMARY",
                "n": 12,
                "n_dates": 8,
                "n_effective": 6,
                "plan_mean": 0.03,
                "hold_mean": 0.01,
                "mean": 0.02,
                "ci": {"lower": -0.01, "upper": 0.05, "confidence_level": 0.95},
            }
        ],
        "comparisons": [],
        "quality": [],
        "alternatives": [],
        "strategy_comparisons": [],
    }


@pytest.mark.asyncio
async def test_meta_lab_uses_meta_cutoff_and_never_promotes_account_dva(monkeypatch, tmp_path):
    cutoff = datetime(2026, 9, 25, 1, 6, tzinfo=timezone.utc)
    row = {
        "replay_run_id": "lab-run",
        "summary": json.dumps(_summary()),
        "config": json.dumps(
            {
                "evaluated_as_of": "2026-09-25T01:00:00+00:00",
                "mode": "RECORDED_PLAN_EVALUATION",
                "costs": {},
            }
        ),
        "evaluated_as_of": cutoff,
    }
    conn = FakeConn([row])

    async def fake_connect(*args, **kwargs):
        return conn

    monkeypatch.setattr(lab, "connect_read_only", fake_connect)
    payload = await lab.load_decision_lab_context(
        "postgres://example",
        123,
        ticker="MU",
        cutoff=cutoff,
        path=tmp_path / "unused.jsonl",
    )

    assert conn.cutoff == cutoff
    assert conn.closed is True
    assert payload["status"] == "AVAILABLE"
    assert payload["ticker_attributed"] is False
    assert payload["meta_c_eligible"] is False
    assert payload["meta_c_ineligible_reason"] == "ACCOUNT_LEVEL_EPISODES_NOT_TICKER_ATTRIBUTED"


def test_meta_lab_render_is_explicitly_context_only():
    payload = {
        "ticker": "MU",
        "evaluated_as_of": "2026-09-25T01:00:00+00:00",
        "metrics": [
            {
                "plan_mean": 0.03,
                "hold_mean": 0.01,
                "mean": 0.02,
                "n": 12,
                "n_effective": 6,
                "ci": {"lower": -0.01, "upper": 0.05},
            }
        ],
        "quality": [],
    }
    text = lab.render_decision_lab_context(payload)
    assert "Decision Lab · META-C" in text
    assert "DVA +2.00%" in text
    assert "contexto solamente" in text
    assert "no habilita META-C individual" in text

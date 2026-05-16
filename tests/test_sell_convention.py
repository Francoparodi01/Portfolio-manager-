import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import pytest

from scripts.run_performance import directional_return_for_report
from src.analysis.decision_engine import directional_return
from src.collector.db import PortfolioDatabase


class _FakeIndex:
    def searchsorted(self, target_date, side="left"):
        return 0


class _FakeSeries:
    empty = False

    def __init__(self, close_price: float):
        self._close_price = close_price
        self.index = _FakeIndex()

    def __len__(self):
        return 1

    @property
    def iloc(self):
        return self

    def __getitem__(self, index):
        return self._close_price


class _FakeDownloadResult:
    def __init__(self, close_price: float):
        self._series = _FakeSeries(close_price)

    def __getitem__(self, key):
        assert key == "Close"
        return self

    def squeeze(self):
        return self._series


class _FakeConnection:
    def __init__(self, row):
        self.row = row
        self.execute_calls = []

    async def fetch(self, *args):
        return [self.row]

    async def execute(self, *args):
        self.execute_calls.append(args)


class _FakePool:
    def __init__(self, conn):
        self.conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self.conn


def test_sell_return_sign_positive_on_gain():
    assert directional_return(100, 90, "SELL") == pytest.approx(0.10)


def test_sell_return_sign_negative_on_loss():
    assert directional_return(100, 110, "SELL") == pytest.approx(-0.10)


def test_sell_persisted_sign_matches_convention(monkeypatch):
    decided_at = datetime.now(timezone.utc) - timedelta(days=10)
    conn = _FakeConnection(
        {
            "id": 1,
            "ticker": "NVDA",
            "decision": "SELL",
            "decided_at": decided_at,
            "price_at_decision": 100.0,
            "outcome_5d": None,
            "outcome_10d": None,
            "outcome_20d": None,
        }
    )
    db = PortfolioDatabase("postgresql://unused")
    db._pool = _FakePool(conn)

    monkeypatch.setitem(
        __import__("sys").modules,
        "yfinance",
        type(
            "YF",
            (),
            {"download": staticmethod(lambda *args, **kwargs: _FakeDownloadResult(90.0))},
        )(),
    )

    updated = asyncio.run(db.update_outcomes(lookback_days=30))

    assert updated == 1
    persisted = conn.execute_calls[0]
    assert persisted[2] == pytest.approx(0.10)


def test_no_sign_inversion_across_modules():
    assert directional_return(100, 90, "SELL") == pytest.approx(
        directional_return_for_report(100, 90, "SELL")
    )

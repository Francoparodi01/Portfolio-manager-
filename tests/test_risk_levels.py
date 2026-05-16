import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from scripts import run_analysis
from src.analysis.decision_engine import make_decision
from src.analysis.risk_levels import RiskLevels, compute_risk_levels
from src.analysis.trade_lifecycle import build_trade_decision


def test_risk_levels_buy_basic():
    levels = compute_risk_levels(100, "POS_OPERABLE", "BUY")

    assert levels.stop < 100
    assert levels.target > 100
    assert levels.rr >= 1.5


def test_risk_levels_sell_basic():
    levels = compute_risk_levels(100, "NEG_OPERABLE", "SELL")

    assert levels.stop > 100
    assert levels.target < 100
    assert levels.stop_pct < 0
    assert levels.target_pct > 0


def test_decision_engine_delegates_to_risk_levels():
    fake_levels = RiskLevels(
        stop=92.0,
        target=116.0,
        rr=2.0,
        stop_pct=-0.08,
        target_pct=0.16,
    )

    with patch(
        "src.analysis.decision_engine.compute_risk_levels",
        return_value=fake_levels,
    ) as mocked:
        make_decision(
            ticker="NVDA",
            score=0.12,
            conviction=0.80,
            regime="NORMAL",
            entry_price=100.0,
            current_weight=0.10,
            target_weight=0.16,
        )

    mocked.assert_called_once()


def test_trade_lifecycle_delegates_to_risk_levels():
    fake_levels = RiskLevels(
        stop=92.0,
        target=116.0,
        rr=2.0,
        stop_pct=-0.08,
        target_pct=0.16,
        stop_source="FIXED",
    )

    with patch(
        "src.analysis.trade_lifecycle.compute_risk_levels",
        return_value=fake_levels,
    ) as mocked:
        build_trade_decision(
            ticker="NVDA",
            score=0.20,
            conviction=0.80,
            delta_weight=0.10,
            regime="NORMAL",
            entry_price=100.0,
            size_pct=0.10,
        )

    mocked.assert_called_once()


class _FakeConnection:
    def __init__(self):
        self.fetchrow_calls = 0

    async def fetchrow(self, *args):
        self.fetchrow_calls += 1
        if self.fetchrow_calls == 1:
            return None
        return {"id": 1}


class _FakePool:
    def __init__(self):
        self.conn = _FakeConnection()

    @asynccontextmanager
    async def acquire(self):
        yield self.conn


class _FakeDatabase:
    def __init__(self, url):
        self.url = url
        self.pool = _FakePool()

    async def connect(self):
        return None

    async def get_pool(self):
        return self.pool

    async def close(self):
        return None


def test_run_analysis_delegates_to_risk_levels():
    fake_levels = RiskLevels(
        stop=92.0,
        target=116.0,
        rr=2.0,
        stop_pct=-0.08,
        target_pct=0.16,
    )
    cfg = SimpleNamespace(database=SimpleNamespace(url="postgresql://unused"))
    rebalance_report = SimpleNamespace(
        trades=[
            SimpleNamespace(
                ticker="NVDA",
                weight_current=0.10,
                weight_optimal=0.20,
            )
        ]
    )
    results = [
        SimpleNamespace(
            ticker="NVDA",
            final_score=0.20,
            conviction=0.80,
            volatility_annual=0.30,
            layers=[],
        )
    ]
    macro_snap = SimpleNamespace(vix=20.0)

    with patch("scripts.run_analysis.compute_risk_levels", return_value=fake_levels) as mocked:
        with patch("scripts.run_analysis.PortfolioDatabase", _FakeDatabase):
            saved_ids = asyncio.run(
                run_analysis._save_optimizer_trades(
                    cfg=cfg,
                    rebalance_report=rebalance_report,
                    current_w={},
                    positions=[],
                    results=results,
                    macro_snap=macro_snap,
                    macro_regime="NORMAL",
                    total_ars=1000.0,
                )
            )

    assert saved_ids == [1]
    mocked.assert_called_once()

import asyncio
from types import SimpleNamespace
from unittest.mock import patch

from scripts import run_analysis, run_opportunity, weekly_summary


class _PortfolioDb:
    latest_owner = None
    history_owner = None

    def __init__(self, _dsn):
        pass

    async def connect(self):
        return None

    async def close(self):
        return None

    async def get_latest_snapshot(self, owner_chat_id=None):
        type(self).latest_owner = owner_chat_id
        return {
            "positions": [],
            "total_value_ars": 1000,
            "cash_ars": 100,
        }

    async def get_portfolio_history(self, limit=60, owner_chat_id=None):
        type(self).history_owner = owner_chat_id
        return []


def test_run_analysis_loads_owner_scoped_portfolio():
    cfg = SimpleNamespace(database=SimpleNamespace(url="unused"))

    with patch("scripts.run_analysis.PortfolioDatabase", _PortfolioDb):
        asyncio.run(run_analysis._load_portfolio(cfg, owner_chat_id=123))

    assert _PortfolioDb.latest_owner == 123
    assert _PortfolioDb.history_owner == 123


def test_run_opportunity_loads_owner_scoped_portfolio():
    cfg = SimpleNamespace(database=SimpleNamespace(url="unused"))

    with patch("scripts.run_opportunity.PortfolioDatabase", _PortfolioDb):
        asyncio.run(run_opportunity._load_portfolio(cfg, owner_chat_id=456))

    assert _PortfolioDb.latest_owner == 456


def test_weekly_summary_uses_owner_scoped_history():
    cfg = SimpleNamespace(database=SimpleNamespace(url="unused"))

    with patch("scripts.weekly_summary.PortfolioDatabase", _PortfolioDb):
        report = asyncio.run(
            weekly_summary.generate_weekly_summary(
                no_telegram=True,
                owner_chat_id=789,
            )
        )

    assert _PortfolioDb.history_owner == 789
    assert "capturas privadas" in report

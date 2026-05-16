import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

from src.analysis.opportunity_screener import run_opportunity_analysis
from src.collector.db import PortfolioDatabase


class _OutcomeConnection:
    def __init__(self, recent_row):
        self.recent_row = recent_row
        self.updates = []
        self.fetch_params = None

    async def fetch(self, _query, maturity_cutoff, lookback_cutoff):
        self.fetch_params = (maturity_cutoff, lookback_cutoff)
        if self.recent_row["decided_at"] >= lookback_cutoff:
            return [self.recent_row]
        return []

    async def execute(self, _query, *params):
        self.updates.append(params)


class _AcquireContext:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _OutcomePool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        return _AcquireContext(self.conn)


def test_update_outcomes_respects_lookback():
    now = datetime.now(timezone.utc)
    recent_row = {
        "id": 1,
        "ticker": "NVDA",
        "price_at_decision": 100.0,
        "decided_at": now - timedelta(days=6),
        "decision": "BUY",
    }
    conn = _OutcomeConnection(recent_row)
    db = PortfolioDatabase("postgresql://unused")
    db._pool = _OutcomePool(conn)
    db.get_market_candles = lambda *_args, **_kwargs: _async_result(
        [
            {
                "ts": now - timedelta(days=1),
                "close_price": 110.0,
            }
        ]
    )

    updated = asyncio.run(db.update_outcomes(lookback_days=7))

    assert updated == 1
    assert len(conn.updates) == 1
    _, lookback_cutoff = conn.fetch_params
    assert recent_row["decided_at"] >= lookback_cutoff


async def _async_result(value):
    return value


def test_exclude_portfolio_removes_held_tickers():
    macro = SimpleNamespace(vix=None)
    seen_universe = []

    def _screen_universe(universe, period):
        seen_universe.extend(universe)
        return []

    with patch(
        "src.analysis.opportunity_screener.screen_universe",
        side_effect=_screen_universe,
    ):
        report = run_opportunity_analysis(
            universe=["NVDA", "AMD"],
            portfolio_positions=[{"ticker": "NVDA", "market_value": 100.0}],
            macro_snap=macro,
            macro_regime={},
            exclude_portfolio=True,
        )

    assert seen_universe == ["AMD"]
    assert all(candidate.ticker != "NVDA" for candidate in report.candidates)


def test_screen_universe_prefers_history_frames(monkeypatch):
    import pandas as pd
    from src.analysis.opportunity_screener import screen_universe

    frame = pd.DataFrame(
        {
            "Close": [100 + i for i in range(260)],
            "High": [101 + i for i in range(260)],
            "Low": [99 + i for i in range(260)],
            "Volume": [1_000_000 for _ in range(260)],
        }
    )

    def fail_fetch_history(*_args, **_kwargs):
        raise AssertionError("legacy fetch should not run when frames are injected")

    monkeypatch.setattr("src.analysis.technical.fetch_history", fail_fetch_history)

    results = screen_universe(
        ["AAPL"],
        history_frames={"SPY": frame, "QQQ": frame, "AAPL": frame},
    )

    assert len(results) == 1
    assert results[0].ticker == "AAPL"


def test_screen_universe_strict_cocos_does_not_fetch_missing_frames(monkeypatch):
    import pandas as pd
    from src.analysis.opportunity_screener import screen_universe

    frame = pd.DataFrame(
        {
            "Close": [100 + i for i in range(260)],
            "High": [101 + i for i in range(260)],
            "Low": [99 + i for i in range(260)],
            "Volume": [1_000_000 for _ in range(260)],
        }
    )

    def fail_fetch_history(*_args, **_kwargs):
        raise AssertionError("legacy fetch should not run for missing Cocos frames")

    monkeypatch.setattr("src.analysis.technical.fetch_history", fail_fetch_history)

    results = screen_universe(["AAPL"], history_frames={"SPY": frame, "QQQ": frame})

    assert len(results) == 1
    assert results[0].ticker == "AAPL"
    assert results[0].passes_screen is False
    assert results[0].fail_reason == "sin velas Cocos suficientes"


def test_screen_universe_without_frames_stays_canonical(monkeypatch):
    from src.analysis.opportunity_screener import screen_universe

    def fail_fetch_history(*_args, **_kwargs):
        raise AssertionError("legacy fetch should not run without canonical frames")

    monkeypatch.setattr("src.analysis.technical.fetch_history", fail_fetch_history)

    results = screen_universe(["AAPL"])

    assert len(results) == 1
    assert results[0].ticker == "AAPL"
    assert results[0].passes_screen is False
    assert results[0].fail_reason == "sin velas Cocos suficientes"

import asyncio
from datetime import datetime, timezone

from src.collector.db import CANONICAL_OUTCOME_BASIS, PortfolioDatabase


def test_directional_outcome_uses_market_date_not_intraday_timestamp():
    db = PortfolioDatabase("postgresql://unused")
    candles = [
        {
            "ts": datetime(2026, 5, 26, 0, 0, tzinfo=timezone.utc),
            "close_price": 110.0,
        }
    ]

    outcomes = asyncio.run(
        db._compute_directional_outcomes(
            entry_price=100.0,
            decided_at=datetime(2026, 5, 21, 20, 12, tzinfo=timezone.utc),
            direction="BUY",
            now=datetime(2026, 5, 26, 23, 59, tzinfo=timezone.utc),
            candles=candles,
        )
    )

    assert outcomes["outcome_5d"] == 0.1


def test_outcome_basis_uses_market_date_for_same_day_daily_candle():
    db = PortfolioDatabase("postgresql://unused")
    candles = [
        {
            "ts": datetime(2026, 5, 21, 0, 0, tzinfo=timezone.utc),
            "close_price": 101.0,
            "asset_type": "ACCION",
        }
    ]

    basis, ratio = db._assess_outcome_basis(
        entry_price=100.0,
        decided_at=datetime(2026, 5, 21, 20, 12, tzinfo=timezone.utc),
        candles=candles,
    )

    assert basis == CANONICAL_OUTCOME_BASIS
    assert ratio == 1.01

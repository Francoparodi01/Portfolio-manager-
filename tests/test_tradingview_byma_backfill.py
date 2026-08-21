import asyncio

from scripts.backfill_tradingview_byma import _targets
from src.collector.cocos_scraper import _is_market_ticker_candidate


class _FakeDb:
    async def get_cocos_universe_assets(self):
        return [
            {"ticker": "NVDA", "asset_type": "CEDEAR"},
            {"ticker": "C.", "asset_type": "ACCION"},
            {"ticker": "ETF", "asset_type": "CEDEAR"},
        ]

    async def get_latest_portfolio_instrument_seeds(self, *, recent_exit_days):
        assert recent_exit_days == 0
        return [{"ticker": "NVS", "asset_type": "CEDEAR"}]

    async def get_market_candles(self, ticker, **kwargs):
        return []


def test_targets_include_current_position_missing_from_daily_universe():
    targets = asyncio.run(
        _targets(
            _FakeDb(),
            tickers=[],
            asset_type="ALL",
            min_rows=60,
            all_assets=True,
        )
    )

    assert [target.ticker for target in targets] == ["NVDA", "NVS"]


def test_portfolio_only_targets_exclude_daily_universe():
    targets = asyncio.run(
        _targets(
            _FakeDb(),
            tickers=[],
            asset_type="ALL",
            min_rows=60,
            all_assets=True,
            portfolio_only=True,
        )
    )

    assert [target.ticker for target in targets] == ["NVS"]


def test_market_ticker_candidate_rejects_segment_labels_and_truncated_names():
    assert _is_market_ticker_candidate("NVDA") is True
    assert _is_market_ticker_candidate("BA.C") is True
    assert _is_market_ticker_candidate("ETF") is False
    assert _is_market_ticker_candidate("C.") is False

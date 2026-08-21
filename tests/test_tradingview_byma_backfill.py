import asyncio

from scripts.backfill_tradingview_byma import _targets


class _FakeDb:
    async def get_cocos_universe_assets(self):
        return [{"ticker": "NVDA", "asset_type": "CEDEAR"}]

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

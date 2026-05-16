from __future__ import annotations

import asyncio
from pathlib import Path

from scripts import backfill_cocos_history
from scripts import capture_cocos_history


def test_market_from_asset_type_maps_cocos_segments():
    assert backfill_cocos_history._market_from_asset_type("ACCION") == "ACCIONES"
    assert backfill_cocos_history._market_from_asset_type("CEDEAR") == "CEDEARS"


def test_history_output_path_uses_asset_type(tmp_path: Path):
    path = backfill_cocos_history._history_output_path(
        tmp_path,
        {"ticker": "GGAL", "asset_type": "ACCION"},
    )

    assert path.name == "ggal_accion_history.json"


def test_missing_history_assets_filters_assets_with_enough_rows():
    class _FakeDB:
        async def get_market_candles(self, ticker, **_kwargs):
            return list(range(60 if ticker == "T" else 20))

    missing = asyncio.run(
        backfill_cocos_history._missing_history_assets(
            _FakeDB(),
            [
                {"ticker": "T", "asset_type": "CEDEAR"},
                {"ticker": "GGAL", "asset_type": "ACCION"},
            ],
            min_rows=60,
        )
    )

    assert missing == [{"ticker": "GGAL", "asset_type": "ACCION"}]


def test_rate_limit_page_is_detected():
    assert capture_cocos_history._is_rate_limited_page("Error 1015")
    assert capture_cocos_history._is_rate_limited_page("You are being rate limited")
    assert not capture_cocos_history._is_rate_limited_page("<html>ok</html>")

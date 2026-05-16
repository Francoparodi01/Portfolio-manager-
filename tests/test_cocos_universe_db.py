from __future__ import annotations

import asyncio

from src.collector.db import PortfolioDatabase


def test_get_cocos_universe_assets_preserves_asset_type():
    db = PortfolioDatabase("postgresql://unused")

    async def fake_latest_prices():
        return [
            {"ticker": "ggal", "asset_type": "ACCION", "currency": "ARS"},
            {"ticker": "t", "asset_type": "CEDEAR", "currency": "ARS"},
        ]

    db.get_latest_market_prices = fake_latest_prices

    assets = asyncio.run(db.get_cocos_universe_assets())

    assert assets == [
        {"ticker": "GGAL", "asset_type": "ACCION", "currency": "ARS"},
        {"ticker": "T", "asset_type": "CEDEAR", "currency": "ARS"},
    ]

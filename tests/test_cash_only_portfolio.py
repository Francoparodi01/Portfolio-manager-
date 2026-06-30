from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from src.collector.cocos_scraper import CocosCapitalScraper
from src.collector.data.models import PortfolioSnapshot


class _CashOnlyPage:
    async def query_selector_all(self, selector: str):
        assert selector == "[class*='assetWrapper']"
        return []

    async def inner_text(self, selector: str):
        assert selector == "body"
        return """
        $ 1.623.432,28
        Tenencia valorizada
        $ 0,00
        Total dinero
        Dinero
        Peso Argentino
        AR$ 1.623.432,28
        Instrumentos
        Hacé una inversión
        Ir al mercado
        """


def test_cash_only_portfolio_extracts_zero_positions_with_valid_confidence():
    scraper = object.__new__(CocosCapitalScraper)
    scraper._page = _CashOnlyPage()

    positions, confidence = asyncio.run(scraper._extract_positions())

    assert positions == []
    assert confidence.parsed_ratio == 1.0
    assert confidence.is_acceptable(0.8) is True


def test_cash_only_portfolio_snapshot_is_valid():
    snapshot = PortfolioSnapshot(
        scraped_at=datetime(2026, 6, 23, 20, 5, tzinfo=timezone.utc),
        positions=[],
        total_value_ars=0,
        cash_ars=1_623_432.28,
        confidence_score=1.0,
        dom_hash="dom",
        raw_html_hash="raw",
    )

    assert snapshot.validate() == []

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


def test_portfolio_api_extracts_positions_from_settlements():
    payload = {
        "holdings": [
            {
                "ticker": "NVDA",
                "name": "Cedear Nvidia Corp.",
                "type": "CEDEARS",
                "currencyId": "ARS",
                "price": 14390,
                "priceFactor": 1,
                "allocation": 0.13294208996984289,
                "settlements": [
                    {"period": "CI", "amount": 302190, "quantity": 21},
                    {"period": "24hs", "amount": 302190, "quantity": 21},
                    {"period": "INF", "amount": 302190, "quantity": 21},
                ],
            }
        ]
    }

    positions, confidence = CocosCapitalScraper._extract_positions_from_portfolio_api(
        payload
    )

    assert confidence.is_acceptable(0.8) is True
    assert len(positions) == 1
    assert positions[0].ticker == "NVDA"
    assert float(positions[0].quantity) == 21.0
    assert float(positions[0].current_price) == 14390.0
    assert float(positions[0].market_value) == 302190.0


def test_portfolio_api_extracts_totals_from_balance_payload():
    portfolio_payload = {"cash": 15}
    balance_payload = {
        "totalBalance": 303205,
        "holdingsBalance": 302190,
        "cashBalance": 1015,
    }

    total, cash = CocosCapitalScraper._extract_totals_from_portfolio_api(
        portfolio_payload,
        balance_payload,
    )

    assert float(total) == 302190.0
    assert float(cash) == 1015.0

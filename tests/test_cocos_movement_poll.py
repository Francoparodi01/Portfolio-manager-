import asyncio
from unittest.mock import AsyncMock

from src.collector.cocos_scraper import CocosCapitalScraper


def test_movement_poll_reloads_activity_on_the_existing_session():
    scraper = object.__new__(CocosCapitalScraper)
    expected = [object()]
    scraper.scrape_portfolio_movements = AsyncMock(return_value=expected)

    movements = asyncio.run(scraper.poll_portfolio_movements())

    assert movements == expected
    scraper.scrape_portfolio_movements.assert_awaited_once_with(
        wait_ms=500,
        fetch_api_pages=False,
    )

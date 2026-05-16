import asyncio
import sys
import types
from types import SimpleNamespace
from unittest.mock import MagicMock, patch


def _install_fake_playwright():
    playwright_module = types.ModuleType("playwright")
    async_api_module = types.ModuleType("playwright.async_api")
    async_api_module.Browser = object
    async_api_module.BrowserContext = object
    async_api_module.Page = object
    async_api_module.Playwright = object
    async_api_module.TimeoutError = TimeoutError
    async_api_module.async_playwright = lambda: None
    sys.modules.setdefault("playwright", playwright_module)
    sys.modules.setdefault("playwright.async_api", async_api_module)


_install_fake_playwright()

from scripts import run_once


class _DummyScraper:
    def __init__(self, _config):
        self.snapshot = SimpleNamespace(
            total_value_ars=1000.0,
            cash_ars=100.0,
            confidence_score=1.0,
            positions=[],
            to_dict=lambda: {},
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def login(self):
        return True

    async def scrape_portfolio(self):
        return self.snapshot


def test_no_telegram_flag_suppresses_notification():
    cfg = SimpleNamespace(
        scraper=SimpleNamespace(
            validate=lambda: [],
            telegram_bot_token="token",
            telegram_chat_id="chat",
        ),
        database=SimpleNamespace(url="postgresql://unused"),
    )
    notifier = MagicMock()

    with (
        patch.object(run_once, "get_config", return_value=cfg),
        patch.object(run_once, "TelegramNotifier", return_value=notifier),
        patch.object(run_once, "CocosCapitalScraper", _DummyScraper),
    ):
        asyncio.run(run_once.main(no_db=True, no_telegram=True))

    notifier.notify_scrape_complete.assert_not_called()

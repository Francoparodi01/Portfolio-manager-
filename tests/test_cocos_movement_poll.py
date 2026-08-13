import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

from src.collector.cocos_scraper import CocosCapitalScraper
from src.core.config import ScraperConfig


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


def test_empty_storage_state_is_not_treated_as_a_saved_session(tmp_path):
    session_file = tmp_path / "cocos_session.json"
    session_file.write_text('{"cookies":[],"origins":[]}', encoding="utf-8")
    scraper = CocosCapitalScraper(ScraperConfig(session_file=str(session_file)))

    assert scraper._has_saved_session() is False


def test_enter_uses_one_context_with_the_saved_session(tmp_path):
    session_file = tmp_path / "cocos_session.json"
    session_file.write_text(
        json.dumps({"cookies": [{"name": "session", "value": "redacted"}], "origins": []}),
        encoding="utf-8",
    )
    scraper = CocosCapitalScraper(ScraperConfig(session_file=str(session_file)))
    page = AsyncMock()
    context = AsyncMock()
    context.new_page.return_value = page
    browser = AsyncMock()
    browser.new_context.return_value = context
    scraper._browser = browser
    scraper._init_browser = AsyncMock()

    entered = asyncio.run(scraper.__aenter__())

    assert entered is scraper
    assert scraper._context is context
    assert scraper._page is page
    assert scraper._session_loaded is True
    browser.new_context.assert_awaited_once()
    assert browser.new_context.await_args.kwargs["storage_state"] == str(session_file)
    context.new_page.assert_awaited_once_with()


def test_save_session_uses_the_active_context_and_replaces_atomically(tmp_path):
    session_file = tmp_path / "cocos_session.json"
    scraper = CocosCapitalScraper(ScraperConfig(session_file=str(session_file)))
    context = AsyncMock()

    async def write_state(*, path, indexed_db):
        assert indexed_db is True
        with open(path, "w", encoding="utf-8") as handle:
            json.dump({"cookies": [{"name": "session"}], "origins": []}, handle)

    context.storage_state.side_effect = write_state
    scraper._context = context

    asyncio.run(scraper._save_session_state())

    assert scraper._session_loaded is True
    assert json.loads(session_file.read_text(encoding="utf-8"))["cookies"]
    assert not list(tmp_path.glob("*.tmp"))


def test_restore_saved_session_avoids_a_new_credential_login(tmp_path):
    scraper = CocosCapitalScraper(
        ScraperConfig(
            session_file=str(tmp_path / "cocos_session.json"),
            portfolio_url="https://app.cocos.capital/capital-portfolio",
        )
    )
    page = AsyncMock()
    page.url = "https://app.cocos.capital/capital-portfolio"
    page.goto.return_value = SimpleNamespace(status=200)
    scraper._page = page
    scraper._session_loaded = True
    scraper._raise_if_access_blocked = AsyncMock()
    scraper._resolve_trusted_device_step = AsyncMock(return_value=False)

    restored = asyncio.run(scraper._restore_saved_session())

    assert restored is True
    assert scraper._is_logged_in is True
    page.goto.assert_awaited_once_with(
        "https://app.cocos.capital/capital-portfolio",
        wait_until="domcontentloaded",
        timeout=60_000,
    )

import asyncio

from src.collector.cocos_scraper import CocosAccessBlockedError, CocosCapitalScraper
from src.core.config import ScraperConfig


class _FakeLocator:
    def __init__(self, text: str):
        self._text = text

    async def inner_text(self, timeout=None):
        return self._text


class _FakePage:
    url = "https://app.cocos.capital/login"

    async def title(self):
        return "Error 1015"

    def locator(self, selector: str):
        assert selector == "body"
        return _FakeLocator(
            "You are being rate limited. "
            "The owner of this website has banned you temporarily from accessing this website. "
            "Cloudflare Ray ID: test"
        )


class _FakeInput:
    def __init__(self, *, visible: bool, width: int = 40, height: int = 60):
        self.visible = visible
        self.width = width
        self.height = height

    async def is_visible(self):
        return self.visible

    async def bounding_box(self):
        return {"width": self.width, "height": self.height}


class _FakeInputsPage:
    def __init__(self, inputs):
        self.inputs = inputs

    async def query_selector_all(self, selector: str):
        assert selector == "input"
        return self.inputs


def test_cocos_access_block_detects_cloudflare_1015():
    scraper = CocosCapitalScraper(ScraperConfig())
    scraper._page = _FakePage()

    try:
        asyncio.run(scraper._raise_if_access_blocked("login", response_status=429))
    except CocosAccessBlockedError as exc:
        assert "Cloudflare 1015 rate limit" in str(exc)
    else:
        raise AssertionError("expected CocosAccessBlockedError")


def test_visible_input_elements_ignores_hidden_mfa_inputs():
    hidden_a = _FakeInput(visible=False)
    hidden_b = _FakeInput(visible=True, width=0, height=0)
    visible = [_FakeInput(visible=True) for _ in range(6)]

    scraper = CocosCapitalScraper(ScraperConfig())
    scraper._page = _FakeInputsPage([hidden_a, *visible[:3], hidden_b, *visible[3:]])

    result = asyncio.run(scraper._visible_input_elements("input"))

    assert result == visible

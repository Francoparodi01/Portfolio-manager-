import asyncio
from urllib.parse import parse_qs, urlparse

from src.analysis.sentiment_fetcher import (
    NewsSource,
    _parse_rss,
    get_sentiment_sources,
    load_active_portfolio_tickers,
)
from src.analysis.signal_aggregator import SentimentContext


def test_default_sources_add_yahoo_ticker_feeds_and_reuters_sections(monkeypatch):
    monkeypatch.delenv("SENTIMENT_FEED_URLS", raising=False)
    monkeypatch.delenv("SENTIMENT_YAHOO_TICKERS", raising=False)
    monkeypatch.setenv("SENTIMENT_YAHOO_MAX_TICKERS", "20")

    sources = get_sentiment_sources(["nvda", "AMD", "NVDA"])
    by_name = {source.name: source for source in sources}

    assert "yahoo_finance" in by_name
    assert by_name["yahoo_finance_ticker_nvda"].ticker_hint == "NVDA"
    assert by_name["yahoo_finance_ticker_amd"].ticker_hint == "AMD"
    assert parse_qs(urlparse(by_name["yahoo_finance_ticker_nvda"].url).query)["s"] == ["NVDA"]
    assert by_name["reuters_business"].publisher == "Reuters"
    assert by_name["reuters_technology"].publisher == "Reuters"
    assert by_name["reuters_business"].delivery == "google_news_rss_publisher_filter"


def test_rss_parser_persists_source_audit_metadata():
    source = NewsSource(
        "yahoo_finance_ticker_nvda",
        "https://example.test/feed",
        category="ticker_news",
        ticker_hint="NVDA",
        publisher="Yahoo Finance",
    )
    xml = b"""<?xml version="1.0"?><rss><channel><item>
        <title>Nvidia announces a new product</title>
        <link>https://finance.yahoo.com/news/example</link>
        <description>Company update</description>
        <pubDate>Wed, 17 Jun 2026 20:00:00 GMT</pubDate>
    </item></channel></rss>"""

    items = _parse_rss(source, xml, 10)

    assert len(items) == 1
    assert items[0].source == "yahoo_finance_ticker_nvda"
    assert items[0].raw_payload["ticker_hint"] == "NVDA"
    assert items[0].raw_payload["publisher"] == "Yahoo Finance"
    assert items[0].published_at is not None


def test_reuters_bridge_rejects_non_reuters_items():
    source = NewsSource(
        "reuters_business",
        "https://news.google.com/rss/search?q=source%3AReuters",
        publisher="Reuters",
        delivery="google_news_rss_publisher_filter",
    )
    xml = b"""<rss><channel>
      <item><title>Market story - Reuters</title><link>https://news.google.com/a</link></item>
      <item><title>Unrelated market story - Other</title><link>https://news.google.com/b</link></item>
    </channel></rss>"""

    items = _parse_rss(source, xml, 10)

    assert [item.headline for item in items] == ["Market story - Reuters"]


def test_load_active_portfolio_tickers_uses_latest_snapshots():
    class _Connection:
        async def fetch(self, query, *params):
            assert "latest_snapshots" in query
            assert params == (7, 20)
            return [{"ticker": "AMD"}, {"ticker": "NVDA"}]

    tickers = asyncio.run(load_active_portfolio_tickers(_Connection()))

    assert tickers == ["AMD", "NVDA"]


def test_sentiment_context_metadata_declares_weighted_input():
    payload = SentimentContext(
        ticker="NVDA",
        asset_scope="ticker",
        score=0.4,
        confidence=0.8,
        event_count=2,
    ).to_layers_payload()

    assert payload["input_mode"] == "weighted_input"
    assert payload["configured_weight"] == 0.15
    assert payload["used_in_score"] is True
    assert payload["reason"] == "used_as_sentiment_layer"
    assert "context_only" not in payload

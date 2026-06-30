"""Fetch and persist raw financial sentiment inputs.

This module only captures raw news/events. It does not score, trade or modify
planner decisions.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import warnings
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Iterable
from urllib.parse import urlencode

import httpx
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SentimentRawItem:
    source: str
    url: str
    headline: str
    body_snippet: str = ""
    published_at: datetime | None = None
    raw_payload: dict | None = None

    @property
    def url_hash(self) -> str:
        return hashlib.sha256(self.url.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class NewsSource:
    name: str
    url: str
    kind: str = "rss"
    trust_tier: str = "market"
    category: str = "general"
    ticker_hint: str | None = None
    publisher: str | None = None
    delivery: str = "direct_rss"


YAHOO_TICKER_FEED_URL = "https://feeds.finance.yahoo.com/rss/2.0/headline"
REUTERS_BUSINESS_RSS_BRIDGE = (
    "https://news.google.com/rss/search?"
    + urlencode(
        {
            "q": "business markets source:Reuters",
            "hl": "en-US",
            "gl": "US",
            "ceid": "US:en",
        }
    )
)
REUTERS_TECHNOLOGY_RSS_BRIDGE = (
    "https://news.google.com/rss/search?"
    + urlencode(
        {
            "q": "technology AI chips source:Reuters",
            "hl": "en-US",
            "gl": "US",
            "ceid": "US:en",
        }
    )
)


DEFAULT_FEEDS: tuple[NewsSource, ...] = (
    # Official / primary policy sources.
    NewsSource(
        "fed_press_all",
        "https://www.federalreserve.gov/feeds/press_all.xml",
        trust_tier="official",
        category="rates_policy",
    ),
    NewsSource(
        "fed_monetary_policy",
        "https://www.federalreserve.gov/feeds/press_monetary.xml",
        trust_tier="official",
        category="rates_policy",
    ),
    # Market news with stable public RSS feeds.
    NewsSource(
        "cnbc_markets",
        "https://www.cnbc.com/id/100003114/device/rss/rss.html",
        trust_tier="market",
        category="global_markets",
    ),
    NewsSource(
        "cnbc_economy",
        "https://www.cnbc.com/id/20910258/device/rss/rss.html",
        trust_tier="market",
        category="global_macro",
    ),
    NewsSource(
        "marketwatch_top",
        "https://feeds.content.dowjones.io/public/rss/mw_topstories",
        trust_tier="market",
        category="global_markets",
    ),
    NewsSource(
        "marketwatch_realtime",
        "https://feeds.content.dowjones.io/public/rss/mw_realtimeheadlines",
        trust_tier="market",
        category="global_markets",
    ),
    NewsSource(
        "yahoo_finance",
        "https://finance.yahoo.com/news/rssindex",
        trust_tier="market",
        category="global_markets",
        publisher="Yahoo Finance",
    ),
    # Reuters retired the legacy feeds.reuters.com endpoints. These public,
    # keyless RSS searches are publisher-filtered to Reuters and keep the
    # delivery mechanism explicit in raw_payload for auditability.
    NewsSource(
        "reuters_business",
        REUTERS_BUSINESS_RSS_BRIDGE,
        trust_tier="market",
        category="global_business",
        publisher="Reuters",
        delivery="google_news_rss_publisher_filter",
    ),
    NewsSource(
        "reuters_technology",
        REUTERS_TECHNOLOGY_RSS_BRIDGE,
        trust_tier="market",
        category="global_technology",
        publisher="Reuters",
        delivery="google_news_rss_publisher_filter",
    ),
    # Argentina / local market context.
    NewsSource("ambito_economia", "https://www.ambito.com/rss/pages/economia.xml", trust_tier="local_market", category="argentina_macro"),
    NewsSource("ambito_finanzas", "https://www.ambito.com/rss/pages/finanzas.xml", trust_tier="local_market", category="argentina_market"),
    # Aggregators are kept as secondary context to catch breaking local/CEDEAR topics.
    NewsSource(
        "google_news_cedears",
        "https://news.google.com/rss/search?q=BYMA%20CEDEARs%20acciones%20Argentina&hl=es-419&gl=AR&ceid=AR:es-419",
        trust_tier="aggregator",
        category="cedears_watch",
    ),
    NewsSource(
        "google_news_macro_ar",
        "https://news.google.com/rss/search?q=BCRA%20dolar%20CCL%20MEP%20acciones%20argentinas&hl=es-419&gl=AR&ceid=AR:es-419",
        trust_tier="aggregator",
        category="argentina_macro",
    ),
    NewsSource(
        "google_news_global_macro",
        "https://news.google.com/rss/search?q=oil%20S%26P%20500%20Dow%20Nasdaq%20Fed%20markets&hl=en-US&gl=US&ceid=US:en",
        trust_tier="aggregator",
        category="global_macro",
    ),
    NewsSource(
        "google_news_geopolitics_oil",
        "https://news.google.com/rss/search?q=Iran%20Trump%20oil%20markets%20Wall%20Street&hl=en-US&gl=US&ceid=US:en",
        trust_tier="aggregator",
        category="geopolitics_oil",
    ),
)


def _clean_tickers(tickers: Iterable[str] | None) -> list[str]:
    configured = os.getenv("SENTIMENT_YAHOO_TICKERS", "")
    values = list(tickers or []) + configured.split(",")
    clean: list[str] = []
    for value in values:
        ticker = str(value or "").upper().strip()
        if not ticker or len(ticker) > 16:
            continue
        if not all(char.isalnum() or char in {".", "-", "^", "="} for char in ticker):
            continue
        if ticker not in clean:
            clean.append(ticker)
    try:
        max_tickers = max(0, int(os.getenv("SENTIMENT_YAHOO_MAX_TICKERS", "20")))
    except ValueError:
        max_tickers = 20
    return clean[:max_tickers]


def _yahoo_ticker_sources(tickers: Iterable[str] | None) -> list[NewsSource]:
    sources: list[NewsSource] = []
    for ticker in _clean_tickers(tickers):
        query = urlencode({"s": ticker, "region": "US", "lang": "en-US"})
        source_key = "".join(char.lower() if char.isalnum() else "_" for char in ticker).strip("_")
        sources.append(
            NewsSource(
                name=f"yahoo_finance_ticker_{source_key}",
                url=f"{YAHOO_TICKER_FEED_URL}?{query}",
                trust_tier="market",
                category="ticker_news",
                ticker_hint=ticker,
                publisher="Yahoo Finance",
            )
        )
    return sources


def _env_sources(tickers: Iterable[str] | None = None) -> list[NewsSource]:
    raw = os.getenv("SENTIMENT_FEED_URLS", "").strip()
    if not raw:
        return list(DEFAULT_FEEDS) + _yahoo_ticker_sources(tickers)

    sources: list[NewsSource] = []
    for idx, url in enumerate(raw.split(","), start=1):
        url = url.strip()
        if not url:
            continue
        sources.append(NewsSource(f"custom_{idx}", url, trust_tier="custom", category="custom"))
    return sources or list(DEFAULT_FEEDS)


def get_sentiment_sources(tickers: Iterable[str] | None = None) -> list[NewsSource]:
    """Return configured news sources for reports and diagnostics."""
    return _env_sources(tickers)


async def load_active_portfolio_tickers(
    conn,
    *,
    lookback_days: int = 7,
    limit: int = 20,
) -> list[str]:
    """Return tickers from each owner's latest recent portfolio snapshot."""
    rows = await conn.fetch(
        """
        WITH latest_snapshots AS (
            SELECT DISTINCT ON (COALESCE(owner_chat_id, 0))
                snapshot_id
            FROM portfolio_snapshots
            WHERE scraped_at >= NOW() - ($1::int * INTERVAL '1 day')
            ORDER BY COALESCE(owner_chat_id, 0), scraped_at DESC
        )
        SELECT DISTINCT UPPER(p.ticker) AS ticker
        FROM positions p
        JOIN latest_snapshots ls USING (snapshot_id)
        WHERE COALESCE(p.ticker, '') <> ''
        ORDER BY ticker
        LIMIT $2
        """,
        max(1, int(lookback_days)),
        max(1, int(limit)),
    )
    return [str(row["ticker"]).upper() for row in rows if row.get("ticker")]


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass
    try:
        from dateutil import parser as date_parser

        dt = date_parser.parse(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _text(node, *names: str) -> str:
    for name in names:
        found = node.find(name)
        if found and found.get_text(strip=True):
            return found.get_text(" ", strip=True)
    return ""


def _link(node) -> str:
    link = node.find("link")
    if link:
        href = link.get("href")
        if href:
            return href.strip()
        text = link.get_text(strip=True)
        if text:
            return text
        sibling = link.next_sibling
        if isinstance(sibling, str) and sibling.strip().startswith("http"):
            return sibling.strip()

    description = node.find("description")
    if description:
        desc_soup = BeautifulSoup(description.get_text(" ", strip=True), "html.parser")
        anchor = desc_soup.find("a")
        if anchor and anchor.get("href"):
            return str(anchor.get("href")).strip()
    return ""


def _parse_rss(source: NewsSource, content: bytes, max_items: int) -> list[SentimentRawItem]:
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
        soup = BeautifulSoup(content, "html.parser")
    items = soup.find_all("item")
    if not items:
        items = soup.find_all("entry")

    parsed: list[SentimentRawItem] = []
    for item in items[:max_items]:
        headline = _text(item, "title")
        url = _link(item)
        if not headline or not url:
            continue
        if (
            source.publisher == "Reuters"
            and source.delivery == "google_news_rss_publisher_filter"
            and "reuters" not in headline.lower()
        ):
            continue
        snippet = _text(item, "description", "summary", "content")
        published = _parse_datetime(
            _text(item, "pubDate", "pubdate", "published", "updated", "dc:date")
        )
        parsed.append(
            SentimentRawItem(
                source=source.name,
                url=url,
                headline=headline[:500],
                body_snippet=snippet[:1500],
                published_at=published,
                raw_payload={
                    "source_url": source.url,
                    "kind": source.kind,
                    "trust_tier": source.trust_tier,
                    "category": source.category,
                    "ticker_hint": source.ticker_hint,
                    "publisher": source.publisher,
                    "delivery": source.delivery,
                },
            )
        )
    return parsed


async def fetch_raw_sentiment_items(
    *,
    sources: Iterable[NewsSource] | None = None,
    tickers: Iterable[str] | None = None,
    max_items_per_source: int = 25,
    timeout_seconds: float = 8.0,
) -> list[SentimentRawItem]:
    """Fetch configured news feeds and return raw items."""
    selected = list(sources or _env_sources(tickers))
    if not selected:
        return []

    headers = {
        "User-Agent": "CocosCopilotSentiment/1.0 (+local research bot)",
    }
    try:
        concurrency = max(1, int(os.getenv("SENTIMENT_FETCH_CONCURRENCY", "6")))
    except ValueError:
        concurrency = 6
    semaphore = asyncio.Semaphore(concurrency)

    async def _fetch_source(client, source: NewsSource) -> list[SentimentRawItem]:
        async with semaphore:
            try:
                resp = await client.get(source.url, headers=headers)
                resp.raise_for_status()
                if source.kind == "rss":
                    return _parse_rss(source, resp.content, max_items_per_source)
            except Exception as exc:
                logger.warning("sentiment fetch failed source=%s: %s", source.name, exc)
            return []

    items: list[SentimentRawItem] = []
    async with httpx.AsyncClient(timeout=timeout_seconds, follow_redirects=True) as client:
        batches = await asyncio.gather(*(_fetch_source(client, source) for source in selected))
        for batch in batches:
            items.extend(batch)

    # Dedupe within the batch before DB upsert.
    unique: dict[str, SentimentRawItem] = {}
    for item in items:
        unique[item.url_hash] = item
    return list(unique.values())


async def save_raw_sentiment_items(conn, items: Iterable[SentimentRawItem]) -> int:
    """Persist raw sentiment items. Returns inserted/updated row count."""
    rows = list(items or [])
    if not rows:
        return 0

    values = [
        (
            item.source,
            item.url,
            item.url_hash,
            item.headline,
            item.body_snippet,
            item.published_at,
            json.dumps(item.raw_payload or {}),
        )
        for item in rows
    ]
    await conn.executemany(
        """
        INSERT INTO sentiment_raw (
            source, url, url_hash, headline, body_snippet, published_at, raw_payload
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
        ON CONFLICT (url_hash) DO UPDATE SET
            source = EXCLUDED.source,
            headline = EXCLUDED.headline,
            body_snippet = EXCLUDED.body_snippet,
            published_at = COALESCE(EXCLUDED.published_at, sentiment_raw.published_at),
            raw_payload = EXCLUDED.raw_payload
        """,
        values,
    )
    return len(rows)

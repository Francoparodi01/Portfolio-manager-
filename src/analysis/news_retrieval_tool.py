"""Entity-matched financial news retrieval for Quantia sentiment.

This module is the retrieval boundary for ticker-specific news. It deliberately
keeps provider sentiment out of the active score: Marketaux is used to discover
articles and resolve entities, while local FinBERT remains the sentiment model.

The contract is fail-closed for ticker evidence. An article is emitted only when
Marketaux identifies the requested symbol with a match score above the configured
threshold and the publication timestamp is point-in-time valid for ``as_of``.
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable
from urllib.parse import quote

import httpx

from .sentiment_fetcher import SentimentRawItem

logger = logging.getLogger(__name__)

MARKETAUX_ENDPOINT = "https://api.marketaux.com/v1/news/all"
RETRIEVAL_POLICY = "marketaux_entity_v1"
DEFAULT_MIN_MATCH_SCORE = 0.65
DEFAULT_LOOKBACK_HOURS = 72
DEFAULT_LIMIT = 50
DEFAULT_BATCH_SIZE = 10


def _as_utc(value: datetime | None) -> datetime:
    value = value or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_timestamp(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return float(default)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return int(default)


def _clean_symbols(symbols: Iterable[str]) -> list[str]:
    clean: list[str] = []
    for value in symbols:
        symbol = str(value or "").upper().strip()
        if not symbol or len(symbol) > 20:
            continue
        if not all(char.isalnum() or char in {".", "-", "^", "="} for char in symbol):
            continue
        if symbol not in clean:
            clean.append(symbol)
    return clean


def _batches(values: list[str], size: int) -> list[list[str]]:
    size = max(1, int(size))
    return [values[index : index + size] for index in range(0, len(values), size)]


def _association_url(canonical_url: str, symbol: str) -> str:
    """Create a stable article+entity identity without changing the destination.

    URL fragments are not sent to the publisher, so opening the stored URL still
    resolves to the canonical article while ``sentiment_raw.url_hash`` becomes
    unique per entity association.
    """
    separator = "&" if "#" in canonical_url else "#"
    return f"{canonical_url}{separator}quantia_entity={quote(symbol)}"


def _matching_entity(
    entities: Iterable[dict[str, Any]],
    symbol: str,
    *,
    min_match_score: float,
) -> dict[str, Any] | None:
    matches: list[dict[str, Any]] = []
    for entity in entities or []:
        if str(entity.get("symbol") or "").upper().strip() != symbol:
            continue
        try:
            match_score = float(entity.get("match_score") or 0.0)
        except (TypeError, ValueError):
            match_score = 0.0
        if match_score >= min_match_score:
            matches.append(entity)
    if not matches:
        return None
    return max(matches, key=lambda entity: float(entity.get("match_score") or 0.0))


def _source_name(article: dict[str, Any]) -> str:
    publisher = str(article.get("source") or "unknown").lower().strip()
    safe = "".join(char if char.isalnum() else "_" for char in publisher).strip("_")
    return f"marketaux:{safe or 'unknown'}"


def _to_raw_item(
    article: dict[str, Any],
    *,
    symbol: str,
    entity: dict[str, Any],
    as_of: datetime,
    lookback_hours: int,
) -> SentimentRawItem | None:
    canonical_url = str(article.get("url") or "").strip()
    headline = str(article.get("title") or "").strip()
    if not canonical_url or not headline:
        return None

    published_at = _parse_timestamp(article.get("published_at"))
    if published_at is None:
        # Point-in-time evidence without a provider publication timestamp is not
        # safe enough for Decision Lab or historical reconstruction.
        return None
    oldest = as_of - timedelta(hours=max(1, int(lookback_hours)))
    if published_at > as_of or published_at < oldest:
        return None

    try:
        match_score = float(entity.get("match_score") or 0.0)
    except (TypeError, ValueError):
        match_score = 0.0

    highlights = entity.get("highlights") or []
    if not isinstance(highlights, list):
        highlights = []
    description = str(article.get("description") or article.get("snippet") or "").strip()
    provider_sentiment = entity.get("sentiment_score")

    return SentimentRawItem(
        source=_source_name(article),
        url=_association_url(canonical_url, symbol),
        headline=headline[:500],
        body_snippet=description[:1500],
        published_at=published_at,
        raw_payload={
            "provider": "marketaux",
            "retrieval_policy": RETRIEVAL_POLICY,
            "provider_uuid": article.get("uuid"),
            "canonical_url": canonical_url,
            "publisher": article.get("source"),
            "language": article.get("language"),
            "ticker_hint": symbol,
            "entity_symbol": symbol,
            "entity_name": entity.get("name"),
            "entity_type": entity.get("type"),
            "entity_exchange": entity.get("exchange"),
            "entity_country": entity.get("country"),
            "entity_match_score": match_score,
            "provider_entity_sentiment": provider_sentiment,
            "provider_sentiment_used_for_scoring": False,
            "entity_highlights": highlights[:8],
            "query_as_of": as_of.isoformat(),
            "query_lookback_hours": int(lookback_hours),
        },
    )


async def get_news_context(
    symbols: Iterable[str],
    *,
    as_of: datetime | None = None,
    lookback_hours: int | None = None,
    min_match_score: float | None = None,
    limit: int | None = None,
    batch_size: int | None = None,
    api_token: str | None = None,
    timeout_seconds: float = 10.0,
    client: httpx.AsyncClient | None = None,
) -> tuple[list[SentimentRawItem], dict[str, Any]]:
    """Retrieve PIT-safe, entity-matched ticker news from Marketaux.

    Returns ``(items, stats)``. Provider sentiment is persisted only as audit
    metadata and never used as the active Quantia sentiment score.
    """
    requested = _clean_symbols(symbols)
    as_of_utc = _as_utc(as_of)
    lookback = max(1, int(lookback_hours or _env_int("SENTIMENT_MARKETAUX_LOOKBACK_HOURS", DEFAULT_LOOKBACK_HOURS)))
    threshold = max(
        0.0,
        min(
            1.0,
            float(
                min_match_score
                if min_match_score is not None
                else _env_float("SENTIMENT_MARKETAUX_MIN_MATCH_SCORE", DEFAULT_MIN_MATCH_SCORE)
            ),
        ),
    )
    request_limit = max(1, min(int(limit or _env_int("SENTIMENT_MARKETAUX_LIMIT", DEFAULT_LIMIT)), 100))
    request_batch_size = max(1, min(int(batch_size or _env_int("SENTIMENT_MARKETAUX_BATCH_SIZE", DEFAULT_BATCH_SIZE)), 20))
    token = str(api_token or os.getenv("MARKETAUX_API_TOKEN", "")).strip()

    stats: dict[str, Any] = {
        "provider": "marketaux",
        "retrieval_policy": RETRIEVAL_POLICY,
        "requested_symbols": len(requested),
        "calls": 0,
        "provider_articles": 0,
        "accepted_associations": 0,
        "rejected_associations": 0,
        "min_match_score": threshold,
        "lookback_hours": lookback,
        "status": "ok",
    }
    if not requested:
        stats["status"] = "no_symbols"
        return [], stats
    if not token:
        stats["status"] = "disabled_missing_token"
        logger.warning("Marketaux ticker retrieval disabled: MARKETAUX_API_TOKEN is empty")
        return [], stats

    start = as_of_utc - timedelta(hours=lookback)
    own_client = client is None
    http = client or httpx.AsyncClient(timeout=timeout_seconds, follow_redirects=True)

    async def _fetch_batch(batch: list[str]) -> tuple[list[SentimentRawItem], dict[str, int]]:
        params = {
            "api_token": token,
            "symbols": ",".join(batch),
            "filter_entities": "true",
            "must_have_entities": "true",
            "min_match_score": f"{threshold:.4f}",
            "published_after": start.strftime("%Y-%m-%dT%H:%M:%S"),
            "published_before": as_of_utc.strftime("%Y-%m-%dT%H:%M:%S"),
            "language": "en",
            "limit": str(request_limit),
        }
        response = await http.get(MARKETAUX_ENDPOINT, params=params)
        response.raise_for_status()
        payload = response.json()
        articles = payload.get("data") or []
        if not isinstance(articles, list):
            articles = []

        accepted: list[SentimentRawItem] = []
        rejected = 0
        for article in articles:
            if not isinstance(article, dict):
                continue
            entities = article.get("entities") or []
            for symbol in batch:
                entity = _matching_entity(entities, symbol, min_match_score=threshold)
                if entity is None:
                    rejected += 1
                    continue
                item = _to_raw_item(
                    article,
                    symbol=symbol,
                    entity=entity,
                    as_of=as_of_utc,
                    lookback_hours=lookback,
                )
                if item is None:
                    rejected += 1
                    continue
                accepted.append(item)
        return accepted, {"articles": len(articles), "rejected": rejected}

    try:
        batches = _batches(requested, request_batch_size)
        results = await asyncio.gather(*(_fetch_batch(batch) for batch in batches))
        stats["calls"] = len(batches)
        items: list[SentimentRawItem] = []
        for batch_items, batch_stats in results:
            items.extend(batch_items)
            stats["provider_articles"] += int(batch_stats["articles"])
            stats["rejected_associations"] += int(batch_stats["rejected"])
    except (httpx.HTTPError, ValueError, TypeError) as exc:
        logger.warning("Marketaux ticker retrieval failed closed: %s", exc)
        stats["status"] = "provider_error"
        stats["error"] = str(exc)[:300]
        return [], stats
    finally:
        if own_client:
            await http.aclose()

    unique: dict[str, SentimentRawItem] = {}
    for item in items:
        unique[item.url_hash] = item
    output = list(unique.values())
    stats["accepted_associations"] = len(output)
    return output, stats

import asyncio
from datetime import datetime, timedelta, timezone

from src.analysis.news_retrieval_tool import RETRIEVAL_POLICY, get_news_context


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _FakeClient:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    async def get(self, url, params=None):
        self.calls.append((url, dict(params or {})))
        return _FakeResponse(self.payload)

    async def aclose(self):
        return None


def _article(as_of):
    return {
        "uuid": "article-1",
        "title": "Chipmakers expand AI infrastructure",
        "description": "Nvidia and AMD announced new infrastructure partnerships.",
        "url": "https://example.com/chips",
        "language": "en",
        "published_at": (as_of - timedelta(hours=2)).isoformat(),
        "source": "example.com",
        "entities": [
            {
                "symbol": "NVDA",
                "name": "NVIDIA Corporation",
                "type": "equity",
                "match_score": 0.94,
                "sentiment_score": 0.31,
                "highlights": [{"highlight": "Nvidia expands AI infrastructure"}],
            },
            {
                "symbol": "AMD",
                "name": "Advanced Micro Devices, Inc.",
                "type": "equity",
                "match_score": 0.82,
                "sentiment_score": 0.22,
                "highlights": [{"highlight": "AMD announced a partnership"}],
            },
        ],
    }


def test_marketaux_tool_creates_distinct_article_entity_evidence():
    as_of = datetime(2026, 9, 25, 22, 0, tzinfo=timezone.utc)
    client = _FakeClient({"data": [_article(as_of)]})

    items, stats = asyncio.run(
        get_news_context(
            ["NVDA", "AMD"],
            as_of=as_of,
            lookback_hours=72,
            min_match_score=0.65,
            api_token="test-token",
            client=client,
        )
    )

    assert len(items) == 2
    assert len({item.url_hash for item in items}) == 2
    assert {item.raw_payload["ticker_hint"] for item in items} == {"NVDA", "AMD"}
    assert all(item.raw_payload["retrieval_policy"] == RETRIEVAL_POLICY for item in items)
    assert all(item.raw_payload["canonical_url"] == "https://example.com/chips" for item in items)
    assert all(item.raw_payload["provider_sentiment_used_for_scoring"] is False for item in items)
    assert stats["accepted_associations"] == 2
    assert client.calls[0][1]["filter_entities"] == "true"
    assert client.calls[0][1]["min_match_score"] == "0.6500"


def test_marketaux_tool_rejects_weak_entity_match_locally():
    as_of = datetime(2026, 9, 25, 22, 0, tzinfo=timezone.utc)
    article = _article(as_of)
    article["entities"] = [{"symbol": "NVDA", "match_score": 0.22}]
    client = _FakeClient({"data": [article]})

    items, stats = asyncio.run(
        get_news_context(
            ["NVDA"],
            as_of=as_of,
            min_match_score=0.65,
            api_token="test-token",
            client=client,
        )
    )

    assert items == []
    assert stats["accepted_associations"] == 0
    assert stats["rejected_associations"] == 1


def test_marketaux_tool_rejects_future_article_even_if_provider_returns_it():
    as_of = datetime(2026, 9, 25, 22, 0, tzinfo=timezone.utc)
    article = _article(as_of)
    article["published_at"] = (as_of + timedelta(seconds=1)).isoformat()
    client = _FakeClient({"data": [article]})

    items, stats = asyncio.run(
        get_news_context(
            ["NVDA"],
            as_of=as_of,
            min_match_score=0.65,
            api_token="test-token",
            client=client,
        )
    )

    assert items == []
    assert stats["rejected_associations"] == 1


def test_marketaux_tool_fails_closed_without_token(monkeypatch):
    monkeypatch.delenv("MARKETAUX_API_TOKEN", raising=False)
    items, stats = asyncio.run(get_news_context(["NVDA"], api_token=""))

    assert items == []
    assert stats["status"] == "disabled_missing_token"

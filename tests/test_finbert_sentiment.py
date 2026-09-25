import asyncio
from datetime import datetime, timedelta, timezone

from src.analysis import nlp_scorer
from src.analysis.sentiment_symbols import (
    expand_news_symbols,
    news_symbol_for_portfolio_ticker,
    portfolio_ticker_for_news_symbol,
)
from src.analysis.sentiment_tool import _trend, _weighted_window, get_sentiment
from src.analysis.signal_aggregator import (
    ACTIVE_SENTIMENT_SCORER,
    AGGREGATION_POLICY,
    aggregate_sentiment,
)


class _FakeRuntime:
    def __init__(self, probabilities):
        self.probabilities = probabilities

    def classify(self, _text):
        return dict(self.probabilities)


def test_underlying_mapping_is_explicit_and_reversible():
    assert news_symbol_for_portfolio_ticker("YPFD") == "YPF"
    assert portfolio_ticker_for_news_symbol("YPF") == "YPFD"
    assert expand_news_symbols(["YPFD", "NVDA", "YPFD"]) == ["YPF", "NVDA"]


def test_finbert_score_uses_probability_difference_and_ticker_hint(monkeypatch):
    monkeypatch.setattr(
        nlp_scorer,
        "_runtime",
        lambda *_args, **_kwargs: _FakeRuntime(
            {"positive": 0.72, "negative": 0.08, "neutral": 0.20}
        ),
    )
    row = {
        "id": 7,
        "source": "yahoo_finance_ticker_ypf",
        "headline": "YPF posts stronger quarterly earnings",
        "body_snippet": "Revenue and guidance improved.",
        "raw_payload": {"ticker_hint": "YPF"},
    }

    scored = nlp_scorer.score_with_finbert_sync(row)

    assert scored.ticker == "YPFD"
    assert scored.asset_scope == "ticker"
    assert round(scored.score, 2) == 0.64
    assert scored.confidence == 0.72
    assert scored.raw_response["positive"] == 0.72
    assert scored.raw_response["negative"] == 0.08
    assert scored.raw_response["neutral"] == 0.20
    assert scored.raw_response["method"] == "finbert_local"


def test_sentiment_windows_never_consume_events_after_as_of():
    as_of = datetime(2026, 9, 25, 15, 0, tzinfo=timezone.utc)
    rows = [
        {
            "event_ts": as_of - timedelta(hours=2),
            "score": 0.50,
            "confidence": 0.80,
            "raw_response": {"positive": 0.70, "negative": 0.10, "neutral": 0.20},
        },
        {
            "event_ts": as_of + timedelta(minutes=1),
            "score": -0.95,
            "confidence": 0.99,
            "raw_response": {"positive": 0.01, "negative": 0.98, "neutral": 0.01},
        },
    ]

    window = _weighted_window(rows, 6, as_of)

    assert window["count"] == 1
    assert window["score"] == 0.50
    assert round(window["positive"], 2) == 0.70


def test_sentiment_trend_detects_deterioration():
    assert _trend(-0.40, -0.20, -0.10) == "DETERIORATING"
    assert _trend(0.40, 0.20, 0.10) == "IMPROVING"
    assert _trend(0.10, 0.09, 0.08) == "STABLE"


def test_aggregator_sql_enforces_active_scorer_and_as_of_boundary():
    as_of = datetime(2026, 9, 25, 18, 0, tzinfo=timezone.utc)

    class _Connection:
        async def fetch(self, query, *params):
            assert "ss.scorer = $1" in query
            assert "<= $2" in query
            assert params[0] == ACTIVE_SENTIMENT_SCORER
            assert params[1] == as_of
            return []

    result = asyncio.run(aggregate_sentiment(_Connection(), now=as_of))

    assert result["upserts"] == 0
    assert result["policy"] == AGGREGATION_POLICY
    assert result["scorer"] == "finbert"


def test_sentiment_tool_queries_only_point_in_time_finbert_rows():
    as_of = datetime(2026, 9, 25, 18, 0, tzinfo=timezone.utc)

    class _Connection:
        async def fetch(self, query, *params):
            assert "ss.scorer = $1" in query
            assert "<= $3" in query
            assert params[0] == "finbert"
            assert "YPFD" in params[1]
            assert "YPF" in params[1]
            assert params[2] == as_of
            return [
                {
                    "ticker": "YPFD",
                    "score": 0.55,
                    "confidence": 0.80,
                    "raw_response": {
                        "positive": 0.70,
                        "negative": 0.15,
                        "neutral": 0.15,
                    },
                    "model": "ProsusAI/finbert@test",
                    "scorer": "finbert",
                    "source": "yahoo_finance_ticker_ypf",
                    "event_ts": as_of - timedelta(hours=2),
                }
            ]

    result = asyncio.run(get_sentiment(_Connection(), "YPFD", as_of=as_of))

    assert result.symbol == "YPFD"
    assert result.news_symbol == "YPF"
    assert result.label == "POSITIVE"
    assert result.article_count == 1
    assert result.sentiment_score == 0.55
    assert result.model_version == "ProsusAI/finbert@test"

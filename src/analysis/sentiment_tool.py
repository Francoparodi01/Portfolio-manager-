"""Point-in-time read tool for Quantia sentiment evidence.

This module is intentionally read-only. It exposes the active FinBERT evidence
without granting sentiment any direct execution authority.
"""
from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from .sentiment_symbols import news_symbol_for_portfolio_ticker

ACTIVE_SCORER = os.getenv("SENTIMENT_ACTIVE_SCORER", "finbert").strip().lower() or "finbert"
WINDOWS_HOURS = (6, 24, 72)


@dataclass(frozen=True)
class SentimentToolResult:
    symbol: str
    news_symbol: str
    as_of: str
    sentiment_score: float
    label: str
    confidence: float
    article_count: int
    source_count: int
    positive: float
    neutral: float
    negative: float
    sentiment_6h: float
    sentiment_24h: float
    sentiment_72h: float
    trend: str
    quality: str
    model: str
    model_version: str
    scorer: str = ACTIVE_SCORER

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _aware_utc(value: datetime | None) -> datetime:
    value = value or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _json_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _weighted_window(rows: list[dict[str, Any]], hours: int, as_of: datetime) -> dict[str, float | int]:
    relevant: list[dict[str, Any]] = []
    for row in rows:
        ts = row.get("event_ts")
        if ts is None:
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        age_hours = (as_of - ts.astimezone(timezone.utc)).total_seconds() / 3600.0
        if 0 <= age_hours <= hours:
            relevant.append(row)

    if not relevant:
        return {"score": 0.0, "positive": 0.0, "neutral": 1.0, "negative": 0.0, "count": 0}

    weighted_score = 0.0
    weighted_positive = 0.0
    weighted_neutral = 0.0
    weighted_negative = 0.0
    total_weight = 0.0
    for row in relevant:
        confidence = max(float(row.get("confidence") or 0.0), 0.05)
        payload = _json_payload(row.get("raw_response"))
        positive = float(payload.get("positive") or 0.0)
        negative = float(payload.get("negative") or 0.0)
        neutral = float(payload.get("neutral") or max(0.0, 1.0 - positive - negative))
        total = positive + negative + neutral
        if total <= 0:
            positive, negative, neutral = 0.0, 0.0, 1.0
            total = 1.0
        positive, negative, neutral = positive / total, negative / total, neutral / total
        weight = confidence
        weighted_score += float(row.get("score") or 0.0) * weight
        weighted_positive += positive * weight
        weighted_negative += negative * weight
        weighted_neutral += neutral * weight
        total_weight += weight

    return {
        "score": weighted_score / total_weight,
        "positive": weighted_positive / total_weight,
        "neutral": weighted_neutral / total_weight,
        "negative": weighted_negative / total_weight,
        "count": len(relevant),
    }


def _trend(score_6h: float, score_24h: float, score_72h: float) -> str:
    if score_6h >= score_24h + 0.12 and score_24h >= score_72h - 0.05:
        return "IMPROVING"
    if score_6h <= score_24h - 0.12 and score_24h <= score_72h + 0.05:
        return "DETERIORATING"
    return "STABLE"


def _quality(article_count: int, source_count: int, confidence: float) -> str:
    if article_count >= 8 and source_count >= 3 and confidence >= 0.65:
        return "GOOD"
    if article_count >= 3 and source_count >= 2 and confidence >= 0.50:
        return "PARTIAL"
    return "INSUFFICIENT"


async def get_sentiment(
    conn,
    symbol: str,
    *,
    as_of: datetime | None = None,
    lookback_hours: int = 72,
) -> SentimentToolResult:
    """Return strict point-in-time FinBERT sentiment for one portfolio symbol."""
    as_of_utc = _aware_utc(as_of)
    portfolio_symbol = str(symbol or "").upper().strip()
    news_symbol = news_symbol_for_portfolio_ticker(portfolio_symbol)
    lookup = sorted({portfolio_symbol, news_symbol})

    rows = await conn.fetch(
        """
        WITH latest AS (
            SELECT DISTINCT ON (ss.raw_id)
                ss.raw_id, ss.ticker, ss.score, ss.confidence, ss.raw_response,
                ss.model, ss.scorer, ss.scored_at, sr.source,
                COALESCE(sr.published_at, sr.fetched_at) AS event_ts
            FROM sentiment_scored ss
            JOIN sentiment_raw sr ON sr.id = ss.raw_id
            WHERE ss.status = 'SCORED'
              AND ss.scorer = $1
              AND ss.ticker = ANY($2::text[])
              AND COALESCE(sr.published_at, sr.fetched_at) <= $3
              AND COALESCE(sr.published_at, sr.fetched_at) >= $3 - ($4::int * INTERVAL '1 hour')
            ORDER BY ss.raw_id, ss.scored_at DESC
        )
        SELECT ticker, score, confidence, raw_response, model, scorer, source, event_ts
        FROM latest
        ORDER BY event_ts DESC
        """,
        ACTIVE_SCORER,
        lookup,
        as_of_utc,
        max(int(lookback_hours), 72),
    )
    data = [dict(row) for row in rows]
    w6 = _weighted_window(data, 6, as_of_utc)
    w24 = _weighted_window(data, 24, as_of_utc)
    w72 = _weighted_window(data, 72, as_of_utc)

    score = float(w24["score"] if int(w24["count"]) else w72["score"])
    positive = float(w24["positive"] if int(w24["count"]) else w72["positive"])
    neutral = float(w24["neutral"] if int(w24["count"]) else w72["neutral"])
    negative = float(w24["negative"] if int(w24["count"]) else w72["negative"])
    evidence_count = len(data)
    confidence = max(positive, neutral, negative) if evidence_count else 0.0
    label = "POSITIVE" if positive >= max(neutral, negative) else "NEGATIVE" if negative >= neutral else "NEUTRAL"
    sources = {str(row.get("source") or "unknown") for row in data}
    models = [str(row.get("model") or "") for row in data if row.get("model")]
    default_model = os.getenv("SENTIMENT_FINBERT_MODEL", "ProsusAI/finbert")
    model_version = models[0] if models else default_model
    model_name = model_version.split("@", 1)[0] if model_version else default_model

    return SentimentToolResult(
        symbol=portfolio_symbol,
        news_symbol=news_symbol,
        as_of=as_of_utc.isoformat(),
        sentiment_score=round(score, 6),
        label=label,
        confidence=round(confidence, 6),
        article_count=evidence_count,
        source_count=len(sources),
        positive=round(positive, 6),
        neutral=round(neutral, 6),
        negative=round(negative, 6),
        sentiment_6h=round(float(w6["score"]), 6),
        sentiment_24h=round(float(w24["score"]), 6),
        sentiment_72h=round(float(w72["score"]), 6),
        trend=_trend(float(w6["score"]), float(w24["score"]), float(w72["score"])),
        quality=_quality(evidence_count, len(sources), confidence),
        model=model_name,
        model_version=model_version,
    )

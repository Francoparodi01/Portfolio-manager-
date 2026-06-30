"""Aggregate scored sentiment into ticker and macro context signals."""
from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .synthesis import LAYER_WEIGHTS

logger = logging.getLogger(__name__)

SOURCE_WEIGHTS = {
    "fed_press_all": 1.0,
    "fed_monetary_policy": 1.0,
    "reuters": 1.0,
    "reuters_business": 1.0,
    "cnbc": 0.88,
    "marketwatch": 0.88,
    "yahoo_finance": 0.72,
    "infobae": 0.75,
    "infobae_economia": 0.75,
    "ambito": 0.75,
    "ambito_economia": 0.75,
    "ambito_finanzas": 0.78,
    "google_news": 0.55,
    "yahoo": 0.65,
    "twitter": 0.45,
    "telegram": 0.35,
    "custom": 0.50,
}

AGGREGATION_POLICY = "event_time_v2"

IMPACT_WEIGHTS = {
    "low": 0.7,
    "mid": 1.0,
    "high": 1.35,
}


@dataclass(frozen=True)
class SentimentContext:
    ticker: str
    asset_scope: str
    score: float = 0.0
    confidence: float = 0.0
    event_count: int = 0
    high_impact_count: int = 0
    top_summary: str = ""
    sources: dict[str, int] | None = None

    @property
    def active(self) -> bool:
        return self.event_count > 0

    def to_layers_payload(self) -> dict[str, Any]:
        return {
            "aggregation_score": self.score,
            "input_mode": "weighted_input",
            "configured_weight": LAYER_WEIGHTS["sentiment"],
            "active": self.active,
            "used_in_score": self.active,
            "asset_scope": self.asset_scope,
            "confidence": self.confidence,
            "event_count": self.event_count,
            "high_impact_count": self.high_impact_count,
            "top_summary": self.top_summary,
            "sources": self.sources or {},
            "reason": "used_as_sentiment_layer",
        }


def _source_weight(source: str) -> float:
    source = str(source or "").lower()
    for key, weight in SOURCE_WEIGHTS.items():
        if key in source:
            return float(weight)
    return 0.6


def _age_hours(reference: datetime, value: datetime) -> float:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    delta = reference - value.astimezone(timezone.utc)
    return max(delta.total_seconds() / 3600.0, 0.0)


def _decay(age_hours: float, half_life_hours: float = 8.0) -> float:
    if half_life_hours <= 0:
        return 1.0
    return math.exp(-math.log(2) * age_hours / half_life_hours)


def _bucket_hour(now: datetime | None = None) -> datetime:
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    now = now.astimezone(timezone.utc)
    return now.replace(minute=0, second=0, microsecond=0)


async def aggregate_sentiment(
    conn,
    *,
    window_hours: int = 6,
    half_life_hours: float = 8.0,
    now: datetime | None = None,
) -> dict[str, int]:
    """Aggregate recent scored rows and upsert sentiment_aggregated."""
    now = now or datetime.now(timezone.utc)
    bucket = _bucket_hour(now)
    rows = await conn.fetch(
        """
        WITH latest AS (
            SELECT DISTINCT ON (ss.raw_id)
                ss.raw_id,
                ss.ticker,
                ss.asset_scope,
                ss.score,
                ss.impact,
                ss.confidence,
                ss.summary,
                ss.scored_at,
                sr.source,
                COALESCE(sr.published_at, sr.fetched_at) AS event_ts
            FROM sentiment_scored ss
            JOIN sentiment_raw sr ON sr.id = ss.raw_id
            WHERE ss.status = 'SCORED'
              AND ss.score IS NOT NULL
              AND COALESCE(sr.published_at, sr.fetched_at) >= NOW() - ($1::int * INTERVAL '1 hour')
              AND (
                  ss.ticker IS NOT NULL
                  OR ss.asset_scope IN ('macro', 'sector')
              )
            ORDER BY ss.raw_id, ss.scored_at DESC
        )
        SELECT
            ticker, asset_scope, score, impact, confidence, summary,
            scored_at, source, event_ts
        FROM latest
        """,
        int(window_hours),
    )

    grouped: dict[tuple[str, str], list[dict]] = {}
    for row in rows:
        item = dict(row)
        scope = str(item.get("asset_scope") or "unknown").lower()
        ticker = str(item.get("ticker") or "").upper().strip()
        if not ticker:
            ticker = "MACRO" if scope == "macro" else f"{scope.upper()}_GENERAL"
        grouped.setdefault((ticker, scope), []).append(item)

    upserts = 0
    for (ticker, scope), items in grouped.items():
        weighted_values: list[float] = []
        weights: list[float] = []
        sources: dict[str, int] = {}
        high_impact_count = 0
        top_item = None
        top_abs = -1.0

        for item in items:
            score = float(item.get("score") or 0.0)
            confidence = max(float(item.get("confidence") or 0.0), 0.05)
            impact = str(item.get("impact") or "low").lower()
            if impact == "high":
                high_impact_count += 1
            src = str(item.get("source") or "unknown")
            sources[src] = sources.get(src, 0) + 1
            age = _age_hours(now, item.get("event_ts") or item.get("scored_at") or now)
            weight = (
                _source_weight(src)
                * IMPACT_WEIGHTS.get(impact, 0.7)
                * confidence
                * _decay(age, half_life_hours)
            )
            weighted_values.append(score * weight)
            weights.append(weight)
            if abs(score) * weight > top_abs:
                top_abs = abs(score) * weight
                top_item = item

        if not weights or sum(weights) <= 0:
            continue

        raw_mean = sum(weighted_values) / sum(weights)
        score = float(math.tanh(raw_mean))
        confidence = min(1.0, sum(weights) / max(len(weights), 1))
        top_summary = str((top_item or {}).get("summary") or "")[:180]

        sources_payload: dict[str, Any] = dict(sources)
        sources_payload["_policy"] = AGGREGATION_POLICY
        sources_payload["_window_hours"] = int(window_hours)
        sources_payload["_half_life_hours"] = float(half_life_hours)

        await conn.execute(
            """
            INSERT INTO sentiment_aggregated (
                bucket_ts, ticker, asset_scope, score, confidence, event_count,
                high_impact_count, top_summary, sources, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, NOW())
            ON CONFLICT (bucket_ts, ticker, asset_scope) DO UPDATE SET
                score = EXCLUDED.score,
                confidence = EXCLUDED.confidence,
                event_count = EXCLUDED.event_count,
                high_impact_count = EXCLUDED.high_impact_count,
                top_summary = EXCLUDED.top_summary,
                sources = EXCLUDED.sources,
                updated_at = NOW()
            """,
            bucket,
            ticker,
            scope,
            score,
            confidence,
            len(items),
            high_impact_count,
            top_summary,
            json.dumps(sources_payload),
        )
        upserts += 1

    return {"groups": len(grouped), "upserts": upserts}


async def load_sentiment_contexts(
    conn,
    tickers: list[str],
    *,
    max_age_hours: int = 12,
) -> dict[str, SentimentContext]:
    """Load latest aggregated context for tickers and MACRO."""
    clean = sorted({str(t or "").upper().strip() for t in tickers if str(t or "").strip()})
    lookup = clean + ["MACRO"]
    if not lookup:
        return {}

    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (ticker)
            ticker, asset_scope, score, confidence, event_count,
            high_impact_count, top_summary, sources, bucket_ts
        FROM sentiment_aggregated
        WHERE ticker = ANY($1::text[])
          AND bucket_ts >= NOW() - ($2::int * INTERVAL '1 hour')
          AND sources->>'_policy' = $3
        ORDER BY ticker, bucket_ts DESC
        """,
        lookup,
        int(max_age_hours),
        AGGREGATION_POLICY,
    )

    contexts: dict[str, SentimentContext] = {}
    for row in rows:
        payload = dict(row)
        sources = payload.get("sources") or {}
        if isinstance(sources, str):
            try:
                sources = json.loads(sources)
            except Exception:
                sources = {}
        ctx = SentimentContext(
            ticker=str(payload.get("ticker") or "").upper(),
            asset_scope=str(payload.get("asset_scope") or "unknown"),
            score=float(payload.get("score") or 0.0),
            confidence=float(payload.get("confidence") or 0.0),
            event_count=int(payload.get("event_count") or 0),
            high_impact_count=int(payload.get("high_impact_count") or 0),
            top_summary=str(payload.get("top_summary") or ""),
            sources=sources if isinstance(sources, dict) else {},
        )
        contexts[ctx.ticker] = ctx
    return contexts


async def load_top_sentiment_events(conn, *, limit: int = 3) -> list[dict[str, Any]]:
    """Load the highest-impact scored events from the current ART day."""
    rows = await conn.fetch(
        """
        WITH latest AS (
            SELECT DISTINCT ON (ss.raw_id)
                ss.raw_id,
                ss.summary,
                ss.impact,
                ss.confidence,
                ss.score,
                ss.ticker,
                ss.asset_scope,
                sr.source,
                sr.headline,
                COALESCE(sr.published_at, sr.fetched_at) AS event_ts
            FROM sentiment_scored ss
            JOIN sentiment_raw sr ON sr.id = ss.raw_id
            WHERE ss.status = 'SCORED'
              AND (COALESCE(sr.published_at, sr.fetched_at)
                   AT TIME ZONE 'America/Argentina/Buenos_Aires')::date
                  = (NOW() AT TIME ZONE 'America/Argentina/Buenos_Aires')::date
            ORDER BY ss.raw_id, ss.scored_at DESC
        )
        SELECT summary, impact, confidence, score, ticker, asset_scope,
               source, headline, event_ts
        FROM latest
        ORDER BY
            CASE LOWER(COALESCE(impact, 'low'))
                WHEN 'high' THEN 3 WHEN 'mid' THEN 2 ELSE 1
            END DESC,
            COALESCE(confidence, 0) DESC,
            ABS(COALESCE(score, 0)) DESC,
            event_ts DESC
        LIMIT $1
        """,
        max(1, min(int(limit), 10)),
    )
    return [dict(row) for row in rows]


async def composite_sentiment(conn, ticker: str, *, window_hours: int = 12) -> SentimentContext:
    contexts = await load_sentiment_contexts(conn, [ticker], max_age_hours=window_hours)
    return contexts.get(str(ticker).upper(), SentimentContext(str(ticker).upper(), "ticker"))

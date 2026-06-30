"""PostgreSQL boundary for the independent shadow causal audit."""
from __future__ import annotations

import json
from typing import Any, Mapping, Sequence

from src.analysis.shadow_causal import CausalAnalysis, CausalAnalysisInput, NewsEvidence


TICKER_ALIASES = {"YPF": ("YPF", "YPFD"), "YPFD": ("YPF", "YPFD")}


class ShadowCausalAnalysisStore:
    def __init__(self, pool):
        self.pool = pool

    async def save_analysis(
        self,
        *,
        owner_chat_id: int,
        input_data: CausalAnalysisInput,
        analysis: CausalAnalysis,
    ) -> int:
        projection = input_data.projection
        async with self.pool.acquire() as conn:
            row_id = await conn.fetchval(
                """
                INSERT INTO shadow_thesis_causal_analysis (
                    forecast_id, owner_chat_id, analyzed_at, context_as_of,
                    ticker, projection_as_of, horizon_sessions,
                    expected_return, probability_up, macro_context,
                    macro_news, ticker_news, primary_driver, durability,
                    reversal_risks, conclusion, conclusion_reason,
                    evidence_gaps, model, prompt_version, schema_version,
                    input_fingerprint, raw_response
                ) VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,
                    $11::jsonb,$12::jsonb,$13::jsonb,$14::jsonb,
                    $15::jsonb,$16,$17,$18::jsonb,$19,$20,$21,$22,$23::jsonb
                )
                ON CONFLICT (owner_chat_id, input_fingerprint, model, prompt_version)
                DO UPDATE SET
                    analyzed_at = EXCLUDED.analyzed_at,
                    primary_driver = EXCLUDED.primary_driver,
                    durability = EXCLUDED.durability,
                    reversal_risks = EXCLUDED.reversal_risks,
                    conclusion = EXCLUDED.conclusion,
                    conclusion_reason = EXCLUDED.conclusion_reason,
                    evidence_gaps = EXCLUDED.evidence_gaps,
                    raw_response = EXCLUDED.raw_response
                RETURNING id
                """,
                projection.forecast_id,
                int(owner_chat_id),
                analysis.analyzed_at,
                input_data.context_as_of,
                projection.ticker,
                projection.as_of_ts,
                projection.horizon_sessions,
                projection.expected_return,
                projection.probability_up,
                _json(input_data.macro_context),
                _json([item.to_dict() for item in input_data.macro_news]),
                _json([item.to_dict() for item in input_data.ticker_news]),
                _json(analysis.primary_driver),
                _json(analysis.durability),
                _json([item.to_dict() for item in analysis.reversal_risks]),
                analysis.conclusion,
                analysis.conclusion_reason,
                _json(list(analysis.evidence_gaps)),
                analysis.model,
                analysis.prompt_version,
                analysis.schema_version,
                analysis.input_fingerprint,
                _json(analysis.raw_response),
            )
        return int(row_id)

    async def latest_projections(
        self,
        *,
        owner_chat_id: int,
        tickers: Sequence[str],
        horizon_sessions: int = 20,
    ) -> list[dict[str, Any]]:
        aliases = sorted({alias for ticker in tickers for alias in ticker_aliases(ticker)})
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (ticker)
                    id AS forecast_id, ticker, expected_return, probability_up,
                    horizon_sessions, as_of_ts
                FROM shadow_thesis_forecasts
                WHERE owner_chat_id = $1
                  AND horizon_sessions = $2
                  AND UPPER(ticker) = ANY($3::text[])
                ORDER BY ticker, as_of_ts DESC, captured_at DESC, id DESC
                """,
                int(owner_chat_id),
                int(horizon_sessions),
                aliases,
            )
        return [dict(row) for row in rows]

    async def recent_ticker_news(
        self,
        *,
        ticker: str,
        limit: int = 3,
        lookback_days: int = 14,
    ) -> list[NewsEvidence]:
        aliases = list(ticker_aliases(ticker))
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                WITH latest AS (
                    SELECT DISTINCT ON (ss.raw_id)
                        sr.headline, sr.source, sr.url,
                        COALESCE(sr.published_at, sr.fetched_at) AS published_at,
                        COALESCE(ss.summary, sr.body_snippet, '') AS summary,
                        ss.ticker, ss.asset_scope, ss.scored_at
                    FROM sentiment_scored ss
                    JOIN sentiment_raw sr ON sr.id = ss.raw_id
                    WHERE ss.status = 'SCORED'
                      AND COALESCE(sr.published_at, sr.fetched_at)
                          >= NOW() - ($1::int * INTERVAL '1 day')
                    ORDER BY ss.raw_id, ss.scored_at DESC
                )
                SELECT headline, source, url, published_at, summary
                FROM latest
                WHERE UPPER(COALESCE(ticker, '')) = ANY($2::text[])
                   OR EXISTS (
                        SELECT 1 FROM unnest($2::text[]) alias
                        WHERE UPPER(headline) LIKE '%' || alias || '%'
                   )
                ORDER BY published_at DESC
                LIMIT $3
                """,
                int(lookback_days),
                aliases,
                min(3, max(0, int(limit))),
            )
        return [NewsEvidence.from_mapping(dict(row)) for row in rows]

    async def recent_macro_news(
        self,
        *,
        limit: int = 6,
        lookback_days: int = 7,
    ) -> list[NewsEvidence]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                WITH latest AS (
                    SELECT DISTINCT ON (ss.raw_id)
                        sr.headline, sr.source, sr.url,
                        COALESCE(sr.published_at, sr.fetched_at) AS published_at,
                        COALESCE(ss.summary, sr.body_snippet, '') AS summary,
                        ss.asset_scope, ss.event_type, ss.impact, ss.scored_at
                    FROM sentiment_scored ss
                    JOIN sentiment_raw sr ON sr.id = ss.raw_id
                    WHERE ss.status = 'SCORED'
                      AND COALESCE(sr.published_at, sr.fetched_at)
                          >= NOW() - ($1::int * INTERVAL '1 day')
                    ORDER BY ss.raw_id, ss.scored_at DESC
                )
                SELECT headline, source, url, published_at, summary
                FROM latest
                WHERE asset_scope = 'macro'
                   OR event_type IN ('macro', 'commodity', 'regulation', 'fx')
                ORDER BY
                    CASE impact WHEN 'high' THEN 0 WHEN 'mid' THEN 1 ELSE 2 END,
                    published_at DESC
                LIMIT $2
                """,
                int(lookback_days),
                min(8, max(0, int(limit))),
            )
        return [NewsEvidence.from_mapping(dict(row)) for row in rows]


def ticker_aliases(ticker: str) -> tuple[str, ...]:
    normalized = str(ticker or "").strip().upper()
    return TICKER_ALIASES.get(normalized, (normalized,))


def _json(value: Mapping[str, Any] | Sequence[Any]) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)

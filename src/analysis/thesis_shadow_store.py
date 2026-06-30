"""Persistence boundary for the shadow thesis experiment."""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence
from uuid import UUID

from src.analysis.thesis_shadow import HorizonForecast, MaturedOutcome, ShadowThesis


class ShadowThesisStore:
    def __init__(self, pool):
        self.pool = pool

    async def save_theses(
        self,
        *,
        run_id: UUID,
        owner_chat_id: int,
        theses: Sequence[ShadowThesis],
        captured_at: datetime | None = None,
    ) -> tuple[UUID, int]:
        if not theses:
            return run_id, 0
        captured_at = captured_at or datetime.now(timezone.utc)
        as_of_ts = max(thesis.as_of_ts for thesis in theses)
        model_version = theses[0].model_version
        schema_version = theses[0].schema_version
        roles = {
            "positions": sum(item.universe_role == "POSITION" for item in theses),
            "candidates": sum(item.universe_role == "CANDIDATE" for item in theses),
        }

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                stored_run_id = await conn.fetchval(
                    """
                    INSERT INTO shadow_thesis_runs (
                        run_id, owner_chat_id, captured_at, as_of_ts,
                        model_version, schema_version, universe_count, status, metadata
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,'COMPLETE',$8::jsonb)
                    ON CONFLICT (owner_chat_id, as_of_ts, model_version)
                    DO UPDATE SET
                        universe_count = EXCLUDED.universe_count,
                        status = EXCLUDED.status,
                        metadata = EXCLUDED.metadata
                    RETURNING run_id
                    """,
                    run_id,
                    int(owner_chat_id),
                    captured_at,
                    as_of_ts,
                    model_version,
                    schema_version,
                    len(theses),
                    json.dumps(roles),
                )
                inserted = 0
                for thesis in theses:
                    feature_payload = dict(thesis.feature_snapshot)
                    feature_payload["rationale"] = list(thesis.rationale)
                    for forecast in thesis.forecasts:
                        row_id = await conn.fetchval(
                            """
                            INSERT INTO shadow_thesis_forecasts (
                                run_id, owner_chat_id, captured_at, as_of_ts,
                                ticker, universe_role, horizon_sessions,
                                model_version, schema_version, price_basis,
                                reference_price, expected_return, probability_up,
                                lower_return, upper_return, uncertainty,
                                thesis_action, thesis_confidence, signal_strength,
                                input_sessions, feature_snapshot
                            ) VALUES (
                                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
                                $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21::jsonb
                            )
                            ON CONFLICT (
                                owner_chat_id, ticker, horizon_sessions,
                                as_of_ts, model_version
                            ) DO NOTHING
                            RETURNING id
                            """,
                            stored_run_id,
                            int(owner_chat_id),
                            captured_at,
                            thesis.as_of_ts,
                            thesis.ticker,
                            thesis.universe_role,
                            forecast.horizon_sessions,
                            thesis.model_version,
                            thesis.schema_version,
                            thesis.price_basis,
                            thesis.reference_price,
                            forecast.expected_return,
                            forecast.probability_up,
                            forecast.lower_return,
                            forecast.upper_return,
                            forecast.uncertainty,
                            thesis.thesis_action,
                            thesis.thesis_confidence,
                            forecast.signal_strength,
                            thesis.input_sessions,
                            json.dumps(feature_payload),
                        )
                        inserted += row_id is not None
        return stored_run_id, inserted

    async def pending_outcomes(
        self,
        *,
        owner_chat_id: int,
        limit: int = 5000,
    ) -> list[dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    f.id, f.ticker, f.as_of_ts, f.reference_price,
                    f.horizon_sessions, f.expected_return
                FROM shadow_thesis_forecasts f
                LEFT JOIN shadow_thesis_outcomes o ON o.forecast_id = f.id
                WHERE f.owner_chat_id = $1
                  AND o.forecast_id IS NULL
                ORDER BY f.as_of_ts ASC, f.id ASC
                LIMIT $2
                """,
                int(owner_chat_id),
                int(limit),
            )
        return [dict(row) for row in rows]

    async def save_outcome(
        self,
        *,
        forecast_id: int,
        outcome: MaturedOutcome,
        matured_at: datetime | None = None,
    ) -> bool:
        matured_at = matured_at or datetime.now(timezone.utc)
        async with self.pool.acquire() as conn:
            row_id = await conn.fetchval(
                """
                INSERT INTO shadow_thesis_outcomes (
                    forecast_id, target_session_ts, outcome_price,
                    realized_return, direction_correct, absolute_error,
                    squared_error, matured_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                ON CONFLICT (forecast_id) DO NOTHING
                RETURNING forecast_id
                """,
                int(forecast_id),
                outcome.target_session_ts,
                outcome.outcome_price,
                outcome.realized_return,
                outcome.direction_correct,
                outcome.absolute_error,
                outcome.squared_error,
                matured_at,
            )
        return row_id is not None

    async def evaluation_metrics(
        self,
        *,
        owner_chat_id: int,
    ) -> list[dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    f.horizon_sessions,
                    COUNT(*)::integer AS samples,
                    AVG(CASE WHEN o.direction_correct THEN 1.0 ELSE 0.0 END)
                        AS directional_accuracy,
                    AVG(o.absolute_error) AS mean_absolute_error,
                    AVG(f.expected_return) AS mean_expected_return,
                    AVG(o.realized_return) AS mean_realized_return
                FROM shadow_thesis_forecasts f
                JOIN shadow_thesis_outcomes o ON o.forecast_id = f.id
                WHERE f.owner_chat_id = $1
                GROUP BY f.horizon_sessions
                ORDER BY f.horizon_sessions
                """,
                int(owner_chat_id),
            )
        return [dict(row) for row in rows]

    async def latest_theses(
        self,
        *,
        owner_chat_id: int,
    ) -> list[ShadowThesis]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT f.*
                FROM shadow_thesis_forecasts f
                WHERE f.run_id = (
                    SELECT run_id
                    FROM shadow_thesis_runs
                    WHERE owner_chat_id = $1
                    ORDER BY as_of_ts DESC, captured_at DESC
                    LIMIT 1
                )
                ORDER BY f.universe_role DESC, f.ticker, f.horizon_sessions
                """,
                int(owner_chat_id),
            )

        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            item = dict(row)
            grouped[str(item["ticker"]).upper()].append(item)

        theses: list[ShadowThesis] = []
        for ticker, horizon_rows in grouped.items():
            base = horizon_rows[0]
            snapshot = base.get("feature_snapshot") or {}
            if isinstance(snapshot, str):
                snapshot = json.loads(snapshot)
            snapshot = dict(snapshot)
            rationale = tuple(snapshot.pop("rationale", []) or [])
            forecasts = tuple(
                HorizonForecast(
                    horizon_sessions=int(row["horizon_sessions"]),
                    expected_return=float(row["expected_return"]),
                    probability_up=float(row["probability_up"]),
                    lower_return=float(row["lower_return"]),
                    upper_return=float(row["upper_return"]),
                    uncertainty=float(row["uncertainty"]),
                    confidence=float(row["thesis_confidence"]),
                    signal_strength=str(row["signal_strength"]),
                )
                for row in sorted(horizon_rows, key=lambda value: int(value["horizon_sessions"]))
            )
            theses.append(
                ShadowThesis(
                    ticker=ticker,
                    universe_role=str(base["universe_role"]),
                    as_of_ts=base["as_of_ts"],
                    reference_price=float(base["reference_price"]),
                    thesis_action=str(base["thesis_action"]),
                    thesis_confidence=float(base["thesis_confidence"]),
                    forecasts=forecasts,
                    input_sessions=int(base["input_sessions"]),
                    feature_snapshot=snapshot,
                    rationale=rationale,
                    model_version=str(base["model_version"]),
                    schema_version=int(base["schema_version"]),
                    price_basis=str(base["price_basis"]),
                )
            )
        return theses


def as_mapping(value: Mapping[str, Any] | Any) -> dict[str, Any]:
    return dict(value)

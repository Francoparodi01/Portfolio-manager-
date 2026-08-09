"""Persistence boundary for the v3 shadow calibration experiment."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence
from uuid import UUID

from src.analysis.shadow_calibration import (
    MODEL_VERSION,
    SCHEMA_VERSION,
    CalibrationModel,
    CalibratedProjection,
)


SHADOW_CALIBRATION_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS shadow_calibration_runs (
    calibration_run_id UUID PRIMARY KEY,
    owner_chat_id BIGINT NOT NULL DEFAULT 0,
    source_run_id UUID NOT NULL REFERENCES shadow_thesis_runs(run_id) ON DELETE CASCADE,
    source_model_version TEXT NOT NULL,
    model_version TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    trained_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    train_cutoff TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'COMPLETE_SHADOW',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (owner_chat_id, source_run_id, model_version)
);

CREATE TABLE IF NOT EXISTS shadow_calibration_models (
    calibration_run_id UUID NOT NULL REFERENCES shadow_calibration_runs(calibration_run_id) ON DELETE CASCADE,
    horizon_sessions INTEGER NOT NULL CHECK (horizon_sessions IN (5, 20)),
    sample_count INTEGER NOT NULL,
    cohort_count INTEGER NOT NULL,
    train_start_ts TIMESTAMPTZ NOT NULL,
    train_end_ts TIMESTAMPTZ NOT NULL,
    parameters JSONB NOT NULL,
    fit_metrics JSONB NOT NULL,
    walk_forward_metrics JSONB NOT NULL,
    diagnostics JSONB NOT NULL,
    PRIMARY KEY (calibration_run_id, horizon_sessions)
);

CREATE TABLE IF NOT EXISTS shadow_calibrated_forecasts (
    id BIGSERIAL PRIMARY KEY,
    calibration_run_id UUID NOT NULL REFERENCES shadow_calibration_runs(calibration_run_id) ON DELETE CASCADE,
    source_forecast_id BIGINT NOT NULL REFERENCES shadow_thesis_forecasts(id) ON DELETE CASCADE,
    owner_chat_id BIGINT NOT NULL DEFAULT 0,
    ticker TEXT NOT NULL,
    as_of_ts TIMESTAMPTZ NOT NULL,
    horizon_sessions INTEGER NOT NULL CHECK (horizon_sessions IN (5, 20)),
    model_version TEXT NOT NULL,
    raw_expected_return FLOAT NOT NULL,
    raw_probability_up FLOAT NOT NULL CHECK (raw_probability_up >= 0 AND raw_probability_up <= 1),
    calibrated_expected_return FLOAT NOT NULL,
    calibrated_probability_up FLOAT NOT NULL CHECK (
        calibrated_probability_up >= 0 AND calibrated_probability_up <= 1
    ),
    calibrated_lower_return FLOAT NOT NULL,
    calibrated_upper_return FLOAT NOT NULL,
    calibration_status TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_forecast_id, model_version)
);

CREATE TABLE IF NOT EXISTS shadow_calibration_gate_state (
    owner_chat_id BIGINT NOT NULL DEFAULT 0,
    horizon_sessions INTEGER NOT NULL CHECK (horizon_sessions IN (5, 20)),
    current_gate TEXT NOT NULL,
    calibration_run_id UUID REFERENCES shadow_calibration_runs(calibration_run_id) ON DELETE SET NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (owner_chat_id, horizon_sessions)
);

CREATE TABLE IF NOT EXISTS shadow_calibration_gate_events (
    id BIGSERIAL PRIMARY KEY,
    owner_chat_id BIGINT NOT NULL DEFAULT 0,
    horizon_sessions INTEGER NOT NULL CHECK (horizon_sessions IN (5, 20)),
    previous_gate TEXT NOT NULL,
    new_gate TEXT NOT NULL,
    calibration_run_id UUID REFERENCES shadow_calibration_runs(calibration_run_id) ON DELETE SET NULL,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (calibration_run_id, horizon_sessions, previous_gate, new_gate)
);

CREATE INDEX IF NOT EXISTS idx_shadow_calibration_runs_latest
    ON shadow_calibration_runs(owner_chat_id, trained_at DESC);

CREATE INDEX IF NOT EXISTS idx_shadow_calibrated_forecasts_latest
    ON shadow_calibrated_forecasts(owner_chat_id, ticker, as_of_ts DESC);

CREATE INDEX IF NOT EXISTS idx_shadow_calibration_gate_events_latest
    ON shadow_calibration_gate_events(owner_chat_id, changed_at DESC);
"""


class ShadowCalibrationStore:
    def __init__(self, pool):
        self.pool = pool

    async def ensure_schema(self) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(SHADOW_CALIBRATION_SCHEMA_SQL)

    async def latest_source_run(
        self,
        *,
        owner_chat_id: int,
        source_model_version: str,
    ) -> dict[str, Any] | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT run_id, owner_chat_id, captured_at, as_of_ts, model_version,
                       schema_version, universe_count, status, metadata
                FROM shadow_thesis_runs
                WHERE owner_chat_id = $1 AND model_version = $2
                ORDER BY as_of_ts DESC, captured_at DESC
                LIMIT 1
                """,
                int(owner_chat_id),
                str(source_model_version),
            )
        return dict(row) if row else None

    async def training_examples(
        self,
        *,
        owner_chat_id: int,
        source_model_version: str,
        train_cutoff: datetime,
    ) -> list[dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    f.id AS forecast_id,
                    f.ticker,
                    f.as_of_ts,
                    o.target_session_ts,
                    f.horizon_sessions,
                    f.expected_return AS raw_expected_return,
                    f.probability_up AS raw_probability_up,
                    f.lower_return AS raw_lower_return,
                    f.upper_return AS raw_upper_return,
                    o.realized_return
                FROM shadow_thesis_forecasts f
                JOIN shadow_thesis_outcomes o ON o.forecast_id = f.id
                WHERE f.owner_chat_id = $1
                  AND f.model_version = $2
                  AND f.horizon_sessions IN (5, 20)
                  AND o.target_session_ts <= $3
                  AND ABS(o.realized_return) <= 1.0
                ORDER BY f.as_of_ts, f.ticker, f.horizon_sessions
                """,
                int(owner_chat_id),
                str(source_model_version),
                train_cutoff,
            )
        return [dict(row) for row in rows]

    async def source_forecasts(self, *, source_run_id: UUID) -> list[dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id AS source_forecast_id, owner_chat_id, ticker, as_of_ts,
                       horizon_sessions, expected_return AS raw_expected_return,
                       probability_up AS raw_probability_up
                FROM shadow_thesis_forecasts
                WHERE run_id = $1 AND horizon_sessions IN (5, 20)
                ORDER BY ticker, horizon_sessions
                """,
                source_run_id,
            )
        return [dict(row) for row in rows]

    async def save_calibration(
        self,
        *,
        calibration_run_id: UUID,
        owner_chat_id: int,
        source_run_id: UUID,
        source_model_version: str,
        train_cutoff: datetime,
        models: Mapping[int, CalibrationModel],
        walk_forward: Mapping[int, Mapping[str, Any]],
        projections: Sequence[tuple[Mapping[str, Any], CalibratedProjection]],
        trained_at: datetime | None = None,
    ) -> tuple[UUID, int, list[dict[str, Any]]]:
        trained_at = trained_at or datetime.now(timezone.utc)
        run_metadata = {
            "supported_horizons": sorted(models),
            "source_tables": ["shadow_thesis_forecasts", "shadow_thesis_outcomes"],
            "write_tables": [
                "shadow_calibration_runs",
                "shadow_calibration_models",
                "shadow_calibrated_forecasts",
                "shadow_calibration_gate_state",
                "shadow_calibration_gate_events",
            ],
            "affects_analysis": False,
            "affects_execution": False,
            "visible_in_telegram": False,
        }
        gate_changes: list[dict[str, Any]] = []
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                stored_run_id = await conn.fetchval(
                    """
                    INSERT INTO shadow_calibration_runs (
                        calibration_run_id, owner_chat_id, source_run_id,
                        source_model_version, model_version, schema_version,
                        trained_at, train_cutoff, status, metadata
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'COMPLETE_SHADOW',$9::jsonb)
                    ON CONFLICT (owner_chat_id, source_run_id, model_version)
                    DO UPDATE SET
                        source_model_version = EXCLUDED.source_model_version,
                        schema_version = EXCLUDED.schema_version,
                        trained_at = EXCLUDED.trained_at,
                        train_cutoff = EXCLUDED.train_cutoff,
                        status = EXCLUDED.status,
                        metadata = EXCLUDED.metadata
                    RETURNING calibration_run_id
                    """,
                    calibration_run_id,
                    int(owner_chat_id),
                    source_run_id,
                    str(source_model_version),
                    MODEL_VERSION,
                    SCHEMA_VERSION,
                    trained_at,
                    train_cutoff,
                    json.dumps(run_metadata),
                )
                for horizon, model in sorted(models.items()):
                    await conn.execute(
                        """
                        INSERT INTO shadow_calibration_models (
                            calibration_run_id, horizon_sessions, sample_count,
                            cohort_count, train_start_ts, train_end_ts, parameters,
                            fit_metrics, walk_forward_metrics, diagnostics
                        ) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb,$9::jsonb,$10::jsonb)
                        ON CONFLICT (calibration_run_id, horizon_sessions)
                        DO UPDATE SET
                            sample_count = EXCLUDED.sample_count,
                            cohort_count = EXCLUDED.cohort_count,
                            train_start_ts = EXCLUDED.train_start_ts,
                            train_end_ts = EXCLUDED.train_end_ts,
                            parameters = EXCLUDED.parameters,
                            fit_metrics = EXCLUDED.fit_metrics,
                            walk_forward_metrics = EXCLUDED.walk_forward_metrics,
                            diagnostics = EXCLUDED.diagnostics
                        """,
                        stored_run_id,
                        int(horizon),
                        model.sample_count,
                        model.cohort_count,
                        model.train_start_ts,
                        model.train_end_ts,
                        json.dumps(model.to_parameters()),
                        json.dumps(model.fit_metrics),
                        json.dumps(dict(walk_forward.get(horizon, {}))),
                        json.dumps(model.diagnostics),
                    )
                    new_gate = str(
                        model.diagnostics.get("promotion_gate") or "PENDING_PROSPECTIVE_EVIDENCE"
                    )
                    current = await conn.fetchrow(
                        """
                        SELECT current_gate
                        FROM shadow_calibration_gate_state
                        WHERE owner_chat_id = $1 AND horizon_sessions = $2
                        FOR UPDATE
                        """,
                        int(owner_chat_id),
                        int(horizon),
                    )
                    if current is None:
                        await conn.execute(
                            """
                            INSERT INTO shadow_calibration_gate_state (
                                owner_chat_id, horizon_sessions, current_gate,
                                calibration_run_id, first_seen_at,
                                last_changed_at, updated_at
                            ) VALUES ($1,$2,$3,$4,$5,$5,$5)
                            """,
                            int(owner_chat_id),
                            int(horizon),
                            new_gate,
                            stored_run_id,
                            trained_at,
                        )
                    else:
                        previous_gate = str(current["current_gate"])
                        if previous_gate != new_gate:
                            event_id = await conn.fetchval(
                                """
                                INSERT INTO shadow_calibration_gate_events (
                                    owner_chat_id, horizon_sessions, previous_gate,
                                    new_gate, calibration_run_id, changed_at, metadata
                                ) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb)
                                ON CONFLICT (
                                    calibration_run_id, horizon_sessions,
                                    previous_gate, new_gate
                                ) DO NOTHING
                                RETURNING id
                                """,
                                int(owner_chat_id),
                                int(horizon),
                                previous_gate,
                                new_gate,
                                stored_run_id,
                                trained_at,
                                json.dumps({
                                    "sample_count": model.sample_count,
                                    "cohort_count": model.cohort_count,
                                    "walk_forward": dict(walk_forward.get(horizon, {})),
                                }),
                            )
                            if event_id is not None:
                                gate_changes.append({
                                    "event_id": int(event_id),
                                    "horizon_sessions": int(horizon),
                                    "previous_gate": previous_gate,
                                    "new_gate": new_gate,
                                    "changed_at": trained_at.isoformat(),
                                })
                            await conn.execute(
                                """
                                UPDATE shadow_calibration_gate_state
                                SET current_gate = $3,
                                    calibration_run_id = $4,
                                    last_changed_at = $5,
                                    updated_at = $5
                                WHERE owner_chat_id = $1 AND horizon_sessions = $2
                                """,
                                int(owner_chat_id),
                                int(horizon),
                                new_gate,
                                stored_run_id,
                                trained_at,
                            )
                        else:
                            await conn.execute(
                                """
                                UPDATE shadow_calibration_gate_state
                                SET calibration_run_id = $3, updated_at = $4
                                WHERE owner_chat_id = $1 AND horizon_sessions = $2
                                """,
                                int(owner_chat_id),
                                int(horizon),
                                stored_run_id,
                                trained_at,
                            )

                persisted = 0
                for source, projection in projections:
                    row_id = await conn.fetchval(
                        """
                        INSERT INTO shadow_calibrated_forecasts (
                            calibration_run_id, source_forecast_id, owner_chat_id,
                            ticker, as_of_ts, horizon_sessions, model_version,
                            raw_expected_return, raw_probability_up,
                            calibrated_expected_return, calibrated_probability_up,
                            calibrated_lower_return, calibrated_upper_return,
                            calibration_status, metadata
                        ) VALUES (
                            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb
                        )
                        ON CONFLICT (source_forecast_id, model_version)
                        DO UPDATE SET
                            calibration_run_id = EXCLUDED.calibration_run_id,
                            raw_expected_return = EXCLUDED.raw_expected_return,
                            raw_probability_up = EXCLUDED.raw_probability_up,
                            calibrated_expected_return = EXCLUDED.calibrated_expected_return,
                            calibrated_probability_up = EXCLUDED.calibrated_probability_up,
                            calibrated_lower_return = EXCLUDED.calibrated_lower_return,
                            calibrated_upper_return = EXCLUDED.calibrated_upper_return,
                            calibration_status = EXCLUDED.calibration_status,
                            metadata = EXCLUDED.metadata
                        RETURNING id
                        """,
                        stored_run_id,
                        int(source["source_forecast_id"]),
                        int(source["owner_chat_id"]),
                        str(source["ticker"]),
                        source["as_of_ts"],
                        int(source["horizon_sessions"]),
                        MODEL_VERSION,
                        float(source["raw_expected_return"]),
                        float(source["raw_probability_up"]),
                        projection.calibrated_expected_return,
                        projection.calibrated_probability_up,
                        projection.calibrated_lower_return,
                        projection.calibrated_upper_return,
                        projection.calibration_status,
                        json.dumps({"operational_effect": False}),
                    )
                    persisted += row_id is not None
        return stored_run_id, persisted, gate_changes

    async def latest_models(self, *, owner_chat_id: int) -> list[dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT r.calibration_run_id, r.source_run_id, r.trained_at,
                       r.train_cutoff, r.status, m.*
                FROM shadow_calibration_runs r
                JOIN shadow_calibration_models m
                  ON m.calibration_run_id = r.calibration_run_id
                WHERE r.owner_chat_id = $1
                  AND r.calibration_run_id = (
                      SELECT calibration_run_id
                      FROM shadow_calibration_runs
                      WHERE owner_chat_id = $1
                      ORDER BY train_cutoff DESC, trained_at DESC
                      LIMIT 1
                  )
                ORDER BY m.horizon_sessions
                """,
                int(owner_chat_id),
            )
        result = []
        for row in rows:
            item = dict(row)
            for key in ("parameters", "fit_metrics", "walk_forward_metrics", "diagnostics"):
                if isinstance(item.get(key), str):
                    item[key] = json.loads(item[key])
            result.append(item)
        return result

    async def prospective_metrics(self, *, owner_chat_id: int) -> list[dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                WITH scored AS (
                    SELECT c.horizon_sessions, c.as_of_ts,
                           c.raw_expected_return, c.raw_probability_up,
                           c.calibrated_expected_return,
                           c.calibrated_probability_up,
                           c.calibrated_lower_return,
                           c.calibrated_upper_return,
                           o.realized_return
                    FROM shadow_calibrated_forecasts c
                    JOIN shadow_thesis_outcomes o
                      ON o.forecast_id = c.source_forecast_id
                    WHERE c.owner_chat_id = $1
                      AND ABS(o.realized_return) <= 1.0
                )
                SELECT horizon_sessions,
                       COUNT(*)::integer AS samples,
                       COUNT(DISTINCT as_of_ts::date)::integer AS cohorts,
                       AVG(POWER(raw_probability_up -
                           CASE WHEN realized_return > 0 THEN 1.0 ELSE 0.0 END, 2)) AS raw_brier,
                       AVG(POWER(calibrated_probability_up -
                           CASE WHEN realized_return > 0 THEN 1.0 ELSE 0.0 END, 2)) AS calibrated_brier,
                       AVG(ABS(raw_expected_return - realized_return)) AS raw_mae,
                       AVG(ABS(calibrated_expected_return - realized_return)) AS calibrated_mae,
                       AVG(CASE WHEN realized_return BETWEEN calibrated_lower_return
                                                     AND calibrated_upper_return
                                THEN 1.0 ELSE 0.0 END) AS calibrated_interval_coverage
                FROM scored
                GROUP BY horizon_sessions
                ORDER BY horizon_sessions
                """,
                int(owner_chat_id),
            )
        return [dict(row) for row in rows]

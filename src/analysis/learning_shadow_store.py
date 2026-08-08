"""Persistence for the read-only-input learning shadow audit."""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Sequence
from uuid import UUID

from src.analysis.learning_shadow import (
    LearningShadowCase,
    POLICY_VERSION,
    SCHEMA_VERSION,
    build_cohort_metric_rows,
    build_metric_rows,
    build_rule_candidates,
)


LEARNING_SHADOW_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS learning_shadow_runs (
    run_id UUID PRIMARY KEY,
    owner_chat_id BIGINT NOT NULL DEFAULT 0,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    lookback_days INTEGER NOT NULL CHECK (lookback_days > 0),
    material_return_bps INTEGER NOT NULL CHECK (material_return_bps >= 0),
    policy_version TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    decisions_seen INTEGER NOT NULL DEFAULT 0,
    cases_upserted INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'COMPLETE',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS learning_shadow_cases (
    id BIGSERIAL PRIMARY KEY,
    owner_chat_id BIGINT NOT NULL DEFAULT 0,
    decision_log_id BIGINT NOT NULL REFERENCES decision_log(id) ON DELETE CASCADE,
    ticker TEXT NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('BUY', 'SELL')),
    decided_at TIMESTAMPTZ NOT NULL,
    horizon_days INTEGER NOT NULL CHECK (horizon_days IN (5, 10, 20, 40)),
    shadow_horizon_sessions INTEGER NOT NULL CHECK (shadow_horizon_sessions IN (5, 20, 40)),
    block_reason TEXT,
    outcome_basis TEXT,
    outcome_source TEXT,
    directional_outcome FLOAT,
    material_return_bps INTEGER NOT NULL,
    classification TEXT NOT NULL CHECK (classification IN (
        'PENDING', 'MISSING_OUTCOME', 'EXCLUDED_BASIS', 'EXCLUDED_OUTLIER',
        'POTENTIAL_FALSE_NEGATIVE', 'POSITIVE_BELOW_THRESHOLD',
        'NON_POSITIVE_COUNTERFACTUAL'
    )),
    shadow_forecast_id BIGINT REFERENCES shadow_thesis_forecasts(id) ON DELETE SET NULL,
    shadow_as_of_ts TIMESTAMPTZ,
    shadow_expected_return FLOAT,
    shadow_probability_up FLOAT,
    shadow_action TEXT,
    shadow_direction_correct BOOLEAN,
    shadow_supports_direction BOOLEAN,
    policy_version TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    first_observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_evaluated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_run_id UUID NOT NULL REFERENCES learning_shadow_runs(run_id) ON DELETE RESTRICT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (owner_chat_id, decision_log_id, horizon_days)
);

ALTER TABLE learning_shadow_cases ADD COLUMN IF NOT EXISTS case_population TEXT NOT NULL DEFAULT 'UNKNOWN';
ALTER TABLE learning_shadow_cases ADD COLUMN IF NOT EXISTS block_code TEXT;
ALTER TABLE learning_shadow_cases ADD COLUMN IF NOT EXISTS block_category TEXT NOT NULL DEFAULT 'OTHER';
ALTER TABLE learning_shadow_cases ADD COLUMN IF NOT EXISTS audit_entry_price FLOAT;
ALTER TABLE learning_shadow_cases ADD COLUMN IF NOT EXISTS audit_start_day DATE;
ALTER TABLE learning_shadow_cases ADD COLUMN IF NOT EXISTS path_sessions INTEGER NOT NULL DEFAULT 0;
ALTER TABLE learning_shadow_cases ADD COLUMN IF NOT EXISTS path_max_gap FLOAT;
ALTER TABLE learning_shadow_cases ADD COLUMN IF NOT EXISTS mae FLOAT;
ALTER TABLE learning_shadow_cases ADD COLUMN IF NOT EXISTS mfe FLOAT;
ALTER TABLE learning_shadow_cases ADD COLUMN IF NOT EXISTS path_risk TEXT NOT NULL DEFAULT 'PENDING';
ALTER TABLE learning_shadow_cases ADD COLUMN IF NOT EXISTS benchmark_ticker TEXT;
ALTER TABLE learning_shadow_cases ADD COLUMN IF NOT EXISTS benchmark_outcome FLOAT;
ALTER TABLE learning_shadow_cases ADD COLUMN IF NOT EXISTS alpha_vs_benchmark FLOAT;
ALTER TABLE learning_shadow_cases ADD COLUMN IF NOT EXISTS control_decision_log_id BIGINT REFERENCES decision_log(id) ON DELETE SET NULL;
ALTER TABLE learning_shadow_cases ADD COLUMN IF NOT EXISTS control_status TEXT;
ALTER TABLE learning_shadow_cases ADD COLUMN IF NOT EXISTS control_outcome FLOAT;
ALTER TABLE learning_shadow_cases ADD COLUMN IF NOT EXISTS control_match_type TEXT;
ALTER TABLE learning_shadow_cases ADD COLUMN IF NOT EXISTS control_distance FLOAT;
ALTER TABLE learning_shadow_cases ADD COLUMN IF NOT EXISTS delta_vs_control FLOAT;
ALTER TABLE learning_shadow_cases ADD COLUMN IF NOT EXISTS review_label TEXT NOT NULL DEFAULT 'INSUFFICIENT_EVIDENCE';

CREATE TABLE IF NOT EXISTS learning_shadow_metric_snapshots_v2 (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES learning_shadow_runs(run_id) ON DELETE CASCADE,
    owner_chat_id BIGINT NOT NULL DEFAULT 0,
    captured_at TIMESTAMPTZ NOT NULL,
    snapshot_date DATE NOT NULL,
    lookback_days INTEGER NOT NULL,
    horizon_days INTEGER NOT NULL CHECK (horizon_days IN (5, 10, 20, 40)),
    shadow_horizon_sessions INTEGER NOT NULL CHECK (shadow_horizon_sessions IN (5, 20, 40)),
    material_return_bps INTEGER NOT NULL,
    policy_version TEXT NOT NULL,
    case_population TEXT NOT NULL,
    total_cases INTEGER NOT NULL,
    matured_cases INTEGER NOT NULL,
    potential_false_negatives INTEGER NOT NULL,
    potential_false_negative_rate FLOAT,
    clean_missed_opportunities INTEGER NOT NULL,
    clean_miss_rate FLOAT,
    metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (
        owner_chat_id, snapshot_date, lookback_days, horizon_days,
        material_return_bps, policy_version, case_population
    )
);

CREATE TABLE IF NOT EXISTS learning_shadow_cohort_metrics (
    id BIGSERIAL PRIMARY KEY,
    owner_chat_id BIGINT NOT NULL DEFAULT 0,
    cohort_date DATE NOT NULL,
    horizon_days INTEGER NOT NULL CHECK (horizon_days IN (5, 10, 20, 40)),
    material_return_bps INTEGER NOT NULL,
    policy_version TEXT NOT NULL,
    case_population TEXT NOT NULL,
    total_cases INTEGER NOT NULL,
    matured_cases INTEGER NOT NULL,
    potential_false_negatives INTEGER NOT NULL,
    potential_false_negative_rate FLOAT,
    clean_missed_opportunities INTEGER NOT NULL,
    clean_miss_rate FLOAT,
    metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_run_id UUID NOT NULL REFERENCES learning_shadow_runs(run_id) ON DELETE CASCADE,
    last_evaluated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (
        owner_chat_id, cohort_date, horizon_days, material_return_bps,
        policy_version, case_population
    )
);

CREATE TABLE IF NOT EXISTS learning_shadow_policy_versions (
    policy_version TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'SHADOW',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    affects_analysis BOOLEAN NOT NULL DEFAULT FALSE,
    affects_execution BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS learning_shadow_rule_candidates (
    id BIGSERIAL PRIMARY KEY,
    owner_chat_id BIGINT NOT NULL DEFAULT 0,
    policy_version TEXT NOT NULL,
    block_category TEXT NOT NULL,
    horizon_days INTEGER NOT NULL,
    candidate_type TEXT NOT NULL,
    proposed_rule JSONB NOT NULL,
    rationale TEXT NOT NULL,
    sample_size INTEGER NOT NULL,
    clean_miss_count INTEGER NOT NULL,
    clean_miss_rate FLOAT NOT NULL,
    risky_win_count INTEGER NOT NULL,
    market_driven_count INTEGER NOT NULL,
    mean_alpha_vs_benchmark FLOAT,
    evidence_start TIMESTAMPTZ NOT NULL,
    evidence_end TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'PROPOSED' CHECK (status IN (
        'PROPOSED', 'APPROVED_FOR_SHADOW', 'REJECTED', 'ARCHIVED'
    )),
    reviewed_at TIMESTAMPTZ,
    reviewed_by TEXT,
    review_note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_run_id UUID NOT NULL REFERENCES learning_shadow_runs(run_id) ON DELETE CASCADE,
    UNIQUE (owner_chat_id, policy_version, block_category, horizon_days, candidate_type)
);

CREATE INDEX IF NOT EXISTS idx_learning_shadow_cases_v2_recent
    ON learning_shadow_cases(owner_chat_id, case_population, horizon_days, decided_at DESC);
CREATE INDEX IF NOT EXISTS idx_learning_shadow_cases_v2_review
    ON learning_shadow_cases(owner_chat_id, review_label, block_category, horizon_days);
CREATE INDEX IF NOT EXISTS idx_learning_shadow_snapshots_v2_trend
    ON learning_shadow_metric_snapshots_v2(owner_chat_id, case_population, horizon_days, captured_at DESC);
CREATE INDEX IF NOT EXISTS idx_learning_shadow_cohorts_v2_trend
    ON learning_shadow_cohort_metrics(owner_chat_id, case_population, horizon_days, cohort_date);
"""


LOAD_BLOCKED_CASE_INPUTS_SQL = """
WITH blocked AS (
    SELECT
        dl.id AS decision_log_id,
        COALESCE(dl.owner_chat_id, 0) AS owner_chat_id,
        dl.ticker,
        dl.decision,
        dl.decided_at,
        dl.block_reason,
        COALESCE(
            NULLIF(dl.layers->>'block_code', ''),
            NULLIF(dl.layers->>'guard_code', ''),
            NULLIF(dl.layers->>'reason_code', '')
        ) AS block_code,
        dl.outcome_basis,
        COALESCE(dl.source, dl.layers->>'source') AS source,
        dl.status,
        dl.metric_scope,
        dl.run_intent,
        dl.final_score,
        dl.regime,
        dl.delta_weight,
        dl.price_at_decision,
        dl.next_executable_at,
        dl.next_executable_price,
        COALESCE(NULLIF(dl.next_executable_price, 0), dl.price_at_decision) AS audit_entry_price,
        COALESCE(
            (dl.next_executable_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date,
            (dl.decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date
        ) AS audit_start_day,
        dl.outcome_5d,
        dl.outcome_10d,
        dl.outcome_20d,
        dl.outcome_40d,
        dl.executable_outcome_5d,
        dl.executable_outcome_10d,
        dl.executable_outcome_20d,
        dl.executable_outcome_40d
    FROM decision_log dl
    WHERE COALESCE(dl.owner_chat_id, 0) = $1
      AND dl.decided_at >= NOW() - ($2::int * INTERVAL '1 day')
      AND dl.decision IN ('BUY', 'SELL')
      AND dl.status = 'BLOCKED'
      AND COALESCE(dl.source, dl.layers->>'source') IN ('execution_plan', 'radar')
), expanded AS (
    SELECT
        b.*,
        h.horizon_days,
        h.shadow_horizon_sessions,
        h.nominal_outcome,
        h.executable_outcome
    FROM blocked b
    CROSS JOIN LATERAL (
        VALUES
            (5, 5, b.outcome_5d, b.executable_outcome_5d),
            (10, 5, b.outcome_10d, b.executable_outcome_10d),
            (20, 20, b.outcome_20d, b.executable_outcome_20d),
            (40, 40, b.outcome_40d, b.executable_outcome_40d)
    ) AS h(horizon_days, shadow_horizon_sessions, nominal_outcome, executable_outcome)
), relevant_tickers AS (
    SELECT DISTINCT ticker FROM blocked
    UNION SELECT 'SPY'
), ranked_candles AS (
    SELECT
        mc.ticker,
        (mc.ts AT TIME ZONE 'UTC')::date AS day,
        mc.open_price::float AS open_price,
        mc.high_price::float AS high_price,
        mc.low_price::float AS low_price,
        mc.close_price::float AS close_price,
        ROW_NUMBER() OVER (
            PARTITION BY mc.ticker, (mc.ts AT TIME ZONE 'UTC')::date
            ORDER BY
                CASE
                    WHEN mc.source = 'COCOS' THEN 0
                    WHEN mc.source = 'TRADINGVIEW_BYMA' THEN 1
                    WHEN mc.source = 'internal_snapshot' THEN 2
                    ELSE 3
                END,
                mc.scraped_at DESC,
                mc.ts DESC
        ) AS source_rank
    FROM market_candles mc
    JOIN relevant_tickers rt ON rt.ticker = mc.ticker
    WHERE mc.interval = '1d'
      AND mc.ts >= NOW() - (($2::int + 60) * INTERVAL '1 day')
), daily_candles AS (
    SELECT ticker, day, open_price, high_price, low_price, close_price
    FROM ranked_candles
    WHERE source_rank = 1
), controls AS (
    SELECT
        dl.id,
        COALESCE(dl.owner_chat_id, 0) AS owner_chat_id,
        dl.ticker,
        dl.decision,
        dl.decided_at,
        dl.status,
        dl.final_score,
        dl.regime,
        dl.delta_weight,
        h.horizon_days,
        h.control_outcome
    FROM decision_log dl
    CROSS JOIN LATERAL (
        VALUES
            (5, COALESCE(dl.executable_outcome_5d, dl.outcome_5d)),
            (10, COALESCE(dl.executable_outcome_10d, dl.outcome_10d)),
            (20, COALESCE(dl.executable_outcome_20d, dl.outcome_20d)),
            (40, COALESCE(dl.executable_outcome_40d, dl.outcome_40d))
    ) AS h(horizon_days, control_outcome)
    WHERE COALESCE(dl.owner_chat_id, 0) = $1
      AND COALESCE(dl.source, dl.layers->>'source') = 'execution_plan'
      AND dl.status IN ('APPROVED', 'EXECUTED')
      AND dl.decision IN ('BUY', 'SELL')
      AND dl.outcome_basis = 'canonical_cocos'
      AND h.control_outcome IS NOT NULL
)
SELECT
    e.*,
    path.path_sessions,
    path.mae,
    path.mfe,
    CASE
        WHEN bench.entry_price IS NULL OR bench.close_price IS NULL THEN NULL
        WHEN bench.max_abs_gap >= 0.35 THEN NULL
        WHEN e.decision = 'SELL' THEN (bench.entry_price / NULLIF(bench.close_price, 0)) - 1
        ELSE (bench.close_price / NULLIF(bench.entry_price, 0)) - 1
    END AS benchmark_outcome,
    ctl.id AS control_decision_log_id,
    ctl.status AS control_status,
    ctl.control_outcome,
    ctl.control_match_type,
    ctl.control_distance,
    sf.id AS shadow_forecast_id,
    sf.as_of_ts AS shadow_as_of_ts,
    sf.expected_return AS shadow_expected_return,
    sf.probability_up AS shadow_probability_up,
    sf.thesis_action AS shadow_action,
    so.direction_correct AS shadow_direction_correct
FROM expanded e
LEFT JOIN LATERAL (
    WITH series AS (
        SELECT
            c.*,
            LAG(c.close_price) OVER (ORDER BY c.day) AS previous_close
        FROM daily_candles c
        WHERE c.ticker = e.ticker
          AND c.day >= e.audit_start_day
          AND c.day <= e.audit_start_day + e.horizon_days
    )
    SELECT
        COUNT(*) FILTER (
            WHERE c.high_price IS NOT NULL AND c.low_price IS NOT NULL
        )::integer AS path_sessions,
        GREATEST(
            COALESCE(MAX(ABS((c.close_price / NULLIF(c.previous_close, 0)) - 1)), 0),
            COALESCE((
                SELECT ABS((first_c.close_price / NULLIF(e.audit_entry_price, 0)) - 1)
                FROM series first_c
                WHERE first_c.close_price IS NOT NULL
                ORDER BY first_c.day
                LIMIT 1
            ), 0)
        ) AS path_max_gap,
        CASE
            WHEN e.audit_entry_price IS NULL OR e.audit_entry_price <= 0 THEN NULL
            WHEN e.decision = 'SELL' THEN LEAST(0, MIN(
                (e.audit_entry_price / NULLIF(c.high_price, 0)) - 1
            ))
            ELSE LEAST(0, MIN((c.low_price / e.audit_entry_price) - 1))
        END AS mae,
        CASE
            WHEN e.audit_entry_price IS NULL OR e.audit_entry_price <= 0 THEN NULL
            WHEN e.decision = 'SELL' THEN GREATEST(0, MAX(
                (e.audit_entry_price / NULLIF(c.low_price, 0)) - 1
            ))
            ELSE GREATEST(0, MAX((c.high_price / e.audit_entry_price) - 1))
        END AS mfe
    FROM series c
) path ON TRUE
LEFT JOIN LATERAL (
    WITH series AS (
        SELECT
            c.*,
            LAG(c.close_price) OVER (ORDER BY c.day) AS previous_close
        FROM daily_candles c
        WHERE c.ticker = 'SPY'
          AND c.day >= e.audit_start_day
          AND c.day <= e.audit_start_day + e.horizon_days + 7
    )
    SELECT
        (SELECT c.close_price
         FROM series c
         WHERE c.day >= e.audit_start_day
         ORDER BY c.day LIMIT 1) AS entry_price,
        (SELECT c.close_price
         FROM series c
         WHERE c.day >= e.audit_start_day + e.horizon_days
         ORDER BY c.day LIMIT 1) AS close_price
        ,MAX(ABS((c.close_price / NULLIF(c.previous_close, 0)) - 1)) AS max_abs_gap
    FROM series c
) bench ON TRUE
LEFT JOIN LATERAL (
    SELECT
        c.*,
        CASE
            WHEN c.ticker = e.ticker THEN 'SAME_TICKER'
            WHEN c.regime IS NOT DISTINCT FROM e.regime THEN 'SAME_REGIME'
            ELSE 'SCORE_TIME'
        END AS control_match_type,
        (
            ABS(EXTRACT(EPOCH FROM (c.decided_at - e.decided_at))) / 86400.0 / 45.0
            + ABS(COALESCE(c.final_score, 0) - COALESCE(e.final_score, 0)) * 5.0
            + ABS(COALESCE(c.delta_weight, 0) - COALESCE(e.delta_weight, 0)) * 2.0
        )::float AS control_distance
    FROM controls c
    WHERE c.decision = e.decision
      AND c.horizon_days = e.horizon_days
      AND c.decided_at BETWEEN e.decided_at - INTERVAL '45 days'
                           AND e.decided_at + INTERVAL '45 days'
    ORDER BY
        CASE WHEN c.ticker = e.ticker THEN 0 ELSE 1 END,
        CASE WHEN c.status = 'EXECUTED' THEN 0 ELSE 1 END,
        CASE WHEN c.regime IS NOT DISTINCT FROM e.regime THEN 0 ELSE 1 END,
        control_distance,
        c.id
    LIMIT 1
) ctl ON TRUE
LEFT JOIN LATERAL (
    SELECT f.*
    FROM shadow_thesis_forecasts f
    WHERE f.owner_chat_id = e.owner_chat_id
      AND f.ticker = e.ticker
      AND f.horizon_sessions = e.shadow_horizon_sessions
      AND f.as_of_ts <= e.decided_at
    ORDER BY f.as_of_ts DESC, f.id DESC
    LIMIT 1
) sf ON TRUE
LEFT JOIN shadow_thesis_outcomes so ON so.forecast_id = sf.id
ORDER BY e.decided_at, e.decision_log_id, e.horizon_days
"""


class LearningShadowStore:
    def __init__(self, pool):
        self.pool = pool

    async def ensure_schema(self) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(LEARNING_SHADOW_SCHEMA_SQL)

    async def load_case_inputs(
        self,
        *,
        owner_chat_id: int,
        lookback_days: int,
    ) -> list[dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                LOAD_BLOCKED_CASE_INPUTS_SQL,
                int(owner_chat_id),
                int(lookback_days),
            )
        return [dict(row) for row in rows]

    async def save_evaluation(
        self,
        *,
        run_id: UUID,
        owner_chat_id: int,
        captured_at: datetime,
        snapshot_date,
        lookback_days: int,
        material_return_bps: int,
        cases: Sequence[LearningShadowCase],
    ) -> list[dict[str, Any]]:
        case_list = list(cases)
        metric_rows = build_metric_rows(case_list)
        cohort_rows = build_cohort_metric_rows(case_list)
        rule_candidates = build_rule_candidates(case_list)
        decisions_seen = len({case.decision_log_id for case in case_list})

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO learning_shadow_policy_versions (
                        policy_version, schema_version, status, config,
                        affects_analysis, affects_execution
                    ) VALUES ($1,$2,'SHADOW',$3::jsonb,FALSE,FALSE)
                    ON CONFLICT (policy_version) DO UPDATE SET
                        schema_version = EXCLUDED.schema_version,
                        config = EXCLUDED.config,
                        affects_analysis = FALSE,
                        affects_execution = FALSE
                    """,
                    POLICY_VERSION,
                    SCHEMA_VERSION,
                    json.dumps({
                        "primary_population": "PLANNER_BLOCKED",
                        "benchmark": "SPY",
                        "path_medium_mae": -0.06,
                        "path_high_mae": -0.12,
                        "candidate_mode": "shadow_only_human_review",
                    }),
                )
                await conn.execute(
                    """
                    INSERT INTO learning_shadow_runs (
                        run_id, owner_chat_id, captured_at, lookback_days,
                        material_return_bps, policy_version, schema_version,
                        decisions_seen, cases_upserted, status, metadata
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'COMPLETE',$10::jsonb)
                    """,
                    run_id,
                    int(owner_chat_id),
                    captured_at,
                    int(lookback_days),
                    int(material_return_bps),
                    POLICY_VERSION,
                    SCHEMA_VERSION,
                    decisions_seen,
                    len(case_list),
                    json.dumps({
                        "input_tables": [
                            "decision_log", "market_candles",
                            "shadow_thesis_forecasts", "shadow_thesis_outcomes",
                        ],
                        "write_tables": [
                            "learning_shadow_runs", "learning_shadow_cases",
                            "learning_shadow_metric_snapshots_v2",
                            "learning_shadow_cohort_metrics",
                            "learning_shadow_rule_candidates",
                        ],
                        "primary_population": "PLANNER_BLOCKED",
                        "operational_effect": False,
                    }),
                )

                if case_list:
                    await conn.executemany(
                        """
                        INSERT INTO learning_shadow_cases (
                            owner_chat_id, decision_log_id, ticker, decision,
                            decided_at, horizon_days, shadow_horizon_sessions,
                            case_population, block_reason, block_code, block_category,
                            outcome_basis, outcome_source, directional_outcome,
                            material_return_bps, classification, audit_entry_price,
                            audit_start_day, path_sessions, path_max_gap, mae, mfe, path_risk,
                            benchmark_ticker, benchmark_outcome, alpha_vs_benchmark,
                            control_decision_log_id, control_status, control_outcome,
                            control_match_type, control_distance, delta_vs_control,
                            review_label, shadow_forecast_id, shadow_as_of_ts,
                            shadow_expected_return, shadow_probability_up,
                            shadow_action, shadow_direction_correct,
                            shadow_supports_direction, policy_version, schema_version,
                            last_evaluated_at, last_run_id, metadata
                        ) VALUES (
                            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,
                            $16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,
                            $30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45::jsonb
                        )
                        ON CONFLICT (owner_chat_id, decision_log_id, horizon_days)
                        DO UPDATE SET
                            ticker = EXCLUDED.ticker,
                            decision = EXCLUDED.decision,
                            decided_at = EXCLUDED.decided_at,
                            shadow_horizon_sessions = EXCLUDED.shadow_horizon_sessions,
                            case_population = EXCLUDED.case_population,
                            block_reason = EXCLUDED.block_reason,
                            block_code = EXCLUDED.block_code,
                            block_category = EXCLUDED.block_category,
                            outcome_basis = EXCLUDED.outcome_basis,
                            outcome_source = EXCLUDED.outcome_source,
                            directional_outcome = EXCLUDED.directional_outcome,
                            material_return_bps = EXCLUDED.material_return_bps,
                            classification = EXCLUDED.classification,
                            audit_entry_price = EXCLUDED.audit_entry_price,
                            audit_start_day = EXCLUDED.audit_start_day,
                            path_sessions = EXCLUDED.path_sessions,
                            path_max_gap = EXCLUDED.path_max_gap,
                            mae = EXCLUDED.mae,
                            mfe = EXCLUDED.mfe,
                            path_risk = EXCLUDED.path_risk,
                            benchmark_ticker = EXCLUDED.benchmark_ticker,
                            benchmark_outcome = EXCLUDED.benchmark_outcome,
                            alpha_vs_benchmark = EXCLUDED.alpha_vs_benchmark,
                            control_decision_log_id = EXCLUDED.control_decision_log_id,
                            control_status = EXCLUDED.control_status,
                            control_outcome = EXCLUDED.control_outcome,
                            control_match_type = EXCLUDED.control_match_type,
                            control_distance = EXCLUDED.control_distance,
                            delta_vs_control = EXCLUDED.delta_vs_control,
                            review_label = EXCLUDED.review_label,
                            shadow_forecast_id = EXCLUDED.shadow_forecast_id,
                            shadow_as_of_ts = EXCLUDED.shadow_as_of_ts,
                            shadow_expected_return = EXCLUDED.shadow_expected_return,
                            shadow_probability_up = EXCLUDED.shadow_probability_up,
                            shadow_action = EXCLUDED.shadow_action,
                            shadow_direction_correct = EXCLUDED.shadow_direction_correct,
                            shadow_supports_direction = EXCLUDED.shadow_supports_direction,
                            policy_version = EXCLUDED.policy_version,
                            schema_version = EXCLUDED.schema_version,
                            last_evaluated_at = EXCLUDED.last_evaluated_at,
                            last_run_id = EXCLUDED.last_run_id,
                            metadata = EXCLUDED.metadata
                        """,
                        [
                            (
                                case.owner_chat_id, case.decision_log_id, case.ticker,
                                case.decision, case.decided_at, case.horizon_days,
                                case.shadow_horizon_sessions, case.case_population,
                                case.block_reason, case.block_code, case.block_category,
                                case.outcome_basis, case.outcome_source,
                                case.directional_outcome, case.material_return_bps,
                                case.classification, case.audit_entry_price,
                                case.audit_start_day, case.path_sessions, case.path_max_gap,
                                case.mae, case.mfe, case.path_risk, case.benchmark_ticker,
                                case.benchmark_outcome, case.alpha_vs_benchmark,
                                case.control_decision_log_id, case.control_status,
                                case.control_outcome, case.control_match_type,
                                case.control_distance, case.delta_vs_control,
                                case.review_label, case.shadow_forecast_id,
                                case.shadow_as_of_ts, case.shadow_expected_return,
                                case.shadow_probability_up, case.shadow_action,
                                case.shadow_direction_correct,
                                case.shadow_supports_direction, POLICY_VERSION,
                                SCHEMA_VERSION, captured_at, run_id,
                                json.dumps(case.metadata, default=str),
                            )
                            for case in case_list
                        ],
                    )

                for metric in metric_rows:
                    await conn.execute(
                        """
                        INSERT INTO learning_shadow_metric_snapshots_v2 (
                            run_id, owner_chat_id, captured_at, snapshot_date,
                            lookback_days, horizon_days, shadow_horizon_sessions,
                            material_return_bps, policy_version, case_population,
                            total_cases, matured_cases, potential_false_negatives,
                            potential_false_negative_rate,
                            clean_missed_opportunities, clean_miss_rate, metrics
                        ) VALUES (
                            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17::jsonb
                        )
                        ON CONFLICT (
                            owner_chat_id, snapshot_date, lookback_days, horizon_days,
                            material_return_bps, policy_version, case_population
                        ) DO UPDATE SET
                            run_id = EXCLUDED.run_id,
                            captured_at = EXCLUDED.captured_at,
                            total_cases = EXCLUDED.total_cases,
                            matured_cases = EXCLUDED.matured_cases,
                            potential_false_negatives = EXCLUDED.potential_false_negatives,
                            potential_false_negative_rate = EXCLUDED.potential_false_negative_rate,
                            clean_missed_opportunities = EXCLUDED.clean_missed_opportunities,
                            clean_miss_rate = EXCLUDED.clean_miss_rate,
                            metrics = EXCLUDED.metrics
                        """,
                        run_id, int(owner_chat_id), captured_at, snapshot_date,
                        int(lookback_days), metric["horizon_days"],
                        metric["shadow_horizon_sessions"], int(material_return_bps),
                        POLICY_VERSION, metric["case_population"],
                        metric["total_cases"], metric["matured_cases"],
                        metric["potential_false_negatives"],
                        metric["potential_false_negative_rate"],
                        metric["clean_missed_opportunities"], metric["clean_miss_rate"],
                        json.dumps(metric, default=str),
                    )

                for cohort in cohort_rows:
                    await conn.execute(
                        """
                        INSERT INTO learning_shadow_cohort_metrics (
                            owner_chat_id, cohort_date, horizon_days,
                            material_return_bps, policy_version, case_population,
                            total_cases, matured_cases, potential_false_negatives,
                            potential_false_negative_rate,
                            clean_missed_opportunities, clean_miss_rate, metrics,
                            last_run_id, last_evaluated_at
                        ) VALUES (
                            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14,$15
                        )
                        ON CONFLICT (
                            owner_chat_id, cohort_date, horizon_days,
                            material_return_bps, policy_version, case_population
                        ) DO UPDATE SET
                            total_cases = EXCLUDED.total_cases,
                            matured_cases = EXCLUDED.matured_cases,
                            potential_false_negatives = EXCLUDED.potential_false_negatives,
                            potential_false_negative_rate = EXCLUDED.potential_false_negative_rate,
                            clean_missed_opportunities = EXCLUDED.clean_missed_opportunities,
                            clean_miss_rate = EXCLUDED.clean_miss_rate,
                            metrics = EXCLUDED.metrics,
                            last_run_id = EXCLUDED.last_run_id,
                            last_evaluated_at = EXCLUDED.last_evaluated_at
                        """,
                        int(owner_chat_id), cohort["cohort_date"],
                        cohort["horizon_days"], int(material_return_bps),
                        POLICY_VERSION, cohort["case_population"],
                        cohort["total_cases"], cohort["matured_cases"],
                        cohort["potential_false_negatives"],
                        cohort["potential_false_negative_rate"],
                        cohort["clean_missed_opportunities"], cohort["clean_miss_rate"],
                        json.dumps(cohort, default=str), run_id, captured_at,
                    )

                for candidate in rule_candidates:
                    await conn.execute(
                        """
                        INSERT INTO learning_shadow_rule_candidates (
                            owner_chat_id, policy_version, block_category,
                            horizon_days, candidate_type, proposed_rule, rationale,
                            sample_size, clean_miss_count, clean_miss_rate,
                            risky_win_count, market_driven_count,
                            mean_alpha_vs_benchmark, evidence_start, evidence_end,
                            last_run_id
                        ) VALUES (
                            $1,$2,$3,$4,$5,$6::jsonb,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16
                        )
                        ON CONFLICT (
                            owner_chat_id, policy_version, block_category,
                            horizon_days, candidate_type
                        ) DO UPDATE SET
                            proposed_rule = EXCLUDED.proposed_rule,
                            rationale = EXCLUDED.rationale,
                            sample_size = EXCLUDED.sample_size,
                            clean_miss_count = EXCLUDED.clean_miss_count,
                            clean_miss_rate = EXCLUDED.clean_miss_rate,
                            risky_win_count = EXCLUDED.risky_win_count,
                            market_driven_count = EXCLUDED.market_driven_count,
                            mean_alpha_vs_benchmark = EXCLUDED.mean_alpha_vs_benchmark,
                            evidence_start = EXCLUDED.evidence_start,
                            evidence_end = EXCLUDED.evidence_end,
                            updated_at = NOW(),
                            last_run_id = EXCLUDED.last_run_id
                        """,
                        int(owner_chat_id), POLICY_VERSION,
                        candidate["block_category"], candidate["horizon_days"],
                        candidate["candidate_type"],
                        json.dumps(candidate["proposed_rule"]),
                        candidate["rationale"], candidate["sample_size"],
                        candidate["clean_miss_count"], candidate["clean_miss_rate"],
                        candidate["risky_win_count"], candidate["market_driven_count"],
                        candidate["mean_alpha_vs_benchmark"],
                        candidate["evidence_start"], candidate["evidence_end"], run_id,
                    )
        return metric_rows

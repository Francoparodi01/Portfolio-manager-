from __future__ import annotations


EXECUTION_TIMESTAMP_META_SQL = """
ALTER TABLE broker_fills
    ADD COLUMN IF NOT EXISTS executed_at_precision TEXT NOT NULL DEFAULT 'unknown',
    ADD COLUMN IF NOT EXISTS executed_at_source    TEXT NOT NULL DEFAULT 'unknown';

ALTER TABLE broker_movements
    ADD COLUMN IF NOT EXISTS executed_at_precision TEXT NOT NULL DEFAULT 'date_only',
    ADD COLUMN IF NOT EXISTS executed_at_source    TEXT NOT NULL DEFAULT 'cocos_movements.execution_date';

UPDATE broker_movements
SET executed_at_precision = 'date_only'
WHERE executed_at_precision IS NULL
   OR executed_at_precision = ''
   OR executed_at_precision = 'unknown';

UPDATE broker_fills
SET executed_at_precision = 'date_only',
    executed_at_source = 'cocos_movements.execution_date'
WHERE source = 'cocos_movements'
  AND (
        executed_at_precision IS NULL
     OR executed_at_precision = ''
     OR executed_at_precision = 'unknown'
  );
"""


OUTCOME_HORIZON_SQL = """
ALTER TABLE decision_log
    ADD COLUMN IF NOT EXISTS outcome_40d FLOAT,
    ADD COLUMN IF NOT EXISTS executable_outcome_40d FLOAT;
"""


PLAN_EXECUTION_ATTRIBUTION_SQL = """
CREATE TABLE IF NOT EXISTS plan_execution_attributions (
    id                          BIGSERIAL PRIMARY KEY,
    attribution_key             TEXT NOT NULL UNIQUE,
    representative_decision_log_id BIGINT NOT NULL REFERENCES decision_log(id) ON DELETE CASCADE,
    owner_chat_id               BIGINT,
    ticker                      TEXT NOT NULL,
    side                        TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    plan_decided_at             TIMESTAMPTZ NOT NULL,
    executed_at                 TIMESTAMPTZ NOT NULL,
    executed_at_precision       TEXT NOT NULL DEFAULT 'unknown',
    executed_at_source          TEXT NOT NULL DEFAULT 'unknown',
    target_amount_ars           NUMERIC(20,4) NOT NULL,
    executed_amount_ars         NUMERIC(20,4) NOT NULL,
    follow_ratio                FLOAT NOT NULL,
    follow_status               TEXT NOT NULL CHECK (follow_status IN ('PARTIAL', 'FOLLOWED', 'OVERFOLLOWED')),
    temporal_quality            TEXT NOT NULL CHECK (temporal_quality IN ('CONFIRMED_SEQUENCE', 'AMBIGUOUS_SAME_DAY')),
    eligible_for_viability      BOOLEAN NOT NULL DEFAULT FALSE,
    match_window_sessions       INTEGER NOT NULL DEFAULT 2,
    matching_version            TEXT NOT NULL,
    execution_quantity          NUMERIC(20,8),
    execution_price             NUMERIC(20,4),
    execution_notional_ars      NUMERIC(20,4),
    outcome_5d                  FLOAT,
    outcome_10d                 FLOAT,
    outcome_20d                 FLOAT,
    outcome_40d                 FLOAT,
    outcome_date_5d             DATE,
    outcome_date_10d            DATE,
    outcome_date_20d            DATE,
    outcome_date_40d            DATE,
    outcome_price_5d            NUMERIC(20,4),
    outcome_price_10d           NUMERIC(20,4),
    outcome_price_20d           NUMERIC(20,4),
    outcome_price_40d           NUMERIC(20,4),
    outcome_basis               TEXT,
    outcome_version             TEXT,
    outcome_filled_at           TIMESTAMPTZ,
    metadata                    JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE plan_execution_attributions
    ADD COLUMN IF NOT EXISTS execution_quantity     NUMERIC(20,8),
    ADD COLUMN IF NOT EXISTS execution_price        NUMERIC(20,4),
    ADD COLUMN IF NOT EXISTS execution_notional_ars NUMERIC(20,4),
    ADD COLUMN IF NOT EXISTS outcome_5d             FLOAT,
    ADD COLUMN IF NOT EXISTS outcome_10d            FLOAT,
    ADD COLUMN IF NOT EXISTS outcome_20d            FLOAT,
    ADD COLUMN IF NOT EXISTS outcome_40d            FLOAT,
    ADD COLUMN IF NOT EXISTS outcome_date_5d        DATE,
    ADD COLUMN IF NOT EXISTS outcome_date_10d       DATE,
    ADD COLUMN IF NOT EXISTS outcome_date_20d       DATE,
    ADD COLUMN IF NOT EXISTS outcome_date_40d       DATE,
    ADD COLUMN IF NOT EXISTS outcome_price_5d       NUMERIC(20,4),
    ADD COLUMN IF NOT EXISTS outcome_price_10d      NUMERIC(20,4),
    ADD COLUMN IF NOT EXISTS outcome_price_20d      NUMERIC(20,4),
    ADD COLUMN IF NOT EXISTS outcome_price_40d      NUMERIC(20,4),
    ADD COLUMN IF NOT EXISTS outcome_basis          TEXT,
    ADD COLUMN IF NOT EXISTS outcome_version        TEXT,
    ADD COLUMN IF NOT EXISTS outcome_filled_at      TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_plan_execution_attributions_plan
    ON plan_execution_attributions(representative_decision_log_id);

CREATE INDEX IF NOT EXISTS idx_plan_execution_attributions_viability
    ON plan_execution_attributions(executed_at DESC)
    WHERE eligible_for_viability = TRUE;

CREATE INDEX IF NOT EXISTS idx_plan_execution_attributions_ticker
    ON plan_execution_attributions(ticker, side, executed_at DESC);

CREATE TABLE IF NOT EXISTS plan_execution_attribution_plans (
    attribution_id             BIGINT NOT NULL REFERENCES plan_execution_attributions(id) ON DELETE CASCADE,
    decision_log_id            BIGINT NOT NULL REFERENCES decision_log(id) ON DELETE CASCADE,
    is_representative          BOOLEAN NOT NULL DEFAULT FALSE,
    target_amount_ars          NUMERIC(20,4) NOT NULL,
    matched_amount_ars         NUMERIC(20,4) NOT NULL,
    follow_ratio               FLOAT NOT NULL,
    follow_status              TEXT NOT NULL,
    temporal_quality           TEXT NOT NULL,
    PRIMARY KEY (attribution_id, decision_log_id)
);

CREATE INDEX IF NOT EXISTS idx_plan_execution_attribution_plans_decision
    ON plan_execution_attribution_plans(decision_log_id);

CREATE TABLE IF NOT EXISTS plan_execution_attribution_movements (
    attribution_id             BIGINT NOT NULL REFERENCES plan_execution_attributions(id) ON DELETE CASCADE,
    broker_movement_id         BIGINT NOT NULL REFERENCES broker_movements(id) ON DELETE CASCADE,
    amount_ars                 NUMERIC(20,4) NOT NULL,
    PRIMARY KEY (attribution_id, broker_movement_id),
    UNIQUE (broker_movement_id)
);

CREATE INDEX IF NOT EXISTS idx_plan_execution_attribution_movements_attribution
    ON plan_execution_attribution_movements(attribution_id);
"""


EXECUTION_PLAN_PERSISTENCE_SQL = """
CREATE TABLE IF NOT EXISTS execution_plans (
    id                  UUID PRIMARY KEY,
    owner_chat_id       BIGINT REFERENCES bot_users(chat_id) ON DELETE SET NULL,
    run_id              UUID,
    created_at          TIMESTAMPTZ NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source              TEXT NOT NULL DEFAULT 'execution_plan',
    gate                TEXT NOT NULL,
    feasible            BOOLEAN NOT NULL,
    cash_before         FLOAT NOT NULL DEFAULT 0,
    gross_sell_ars      FLOAT NOT NULL DEFAULT 0,
    fee_sell_ars        FLOAT NOT NULL DEFAULT 0,
    net_sell_ars        FLOAT NOT NULL DEFAULT 0,
    gross_buy_ars       FLOAT NOT NULL DEFAULT 0,
    fee_buy_ars         FLOAT NOT NULL DEFAULT 0,
    cash_after          FLOAT NOT NULL DEFAULT 0,
    summary             TEXT,
    warnings            JSONB NOT NULL DEFAULT '[]'::jsonb,
    payload_version     TEXT NOT NULL DEFAULT 'execution-plan-v1'
);

CREATE INDEX IF NOT EXISTS idx_execution_plans_owner_created_at
    ON execution_plans(owner_chat_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_execution_plans_run_id
    ON execution_plans(run_id)
    WHERE run_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS order_intents (
    id                  BIGSERIAL PRIMARY KEY,
    execution_plan_id   UUID NOT NULL REFERENCES execution_plans(id) ON DELETE CASCADE,
    decision_log_id     BIGINT REFERENCES decision_log(id) ON DELETE SET NULL,
    sequence_no         INTEGER NOT NULL,
    ticker              TEXT NOT NULL,
    side                TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    action              TEXT NOT NULL,
    planner_status      TEXT NOT NULL,
    decision_status     TEXT NOT NULL,
    is_executable       BOOLEAN NOT NULL DEFAULT FALSE,
    was_blocked         BOOLEAN NOT NULL DEFAULT FALSE,
    amount_ars          FLOAT NOT NULL DEFAULT 0,
    theoretical_ars     FLOAT NOT NULL DEFAULT 0,
    quantity_est        FLOAT,
    reference_price     FLOAT,
    priority            INTEGER,
    partial             BOOLEAN NOT NULL DEFAULT FALSE,
    reason              TEXT,
    block_code          TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (execution_plan_id, sequence_no)
);

CREATE INDEX IF NOT EXISTS idx_order_intents_decision_log_id
    ON order_intents(decision_log_id)
    WHERE decision_log_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_order_intents_ticker_created_at
    ON order_intents(ticker, created_at DESC);
"""


async def ensure_execution_plan_persistence(conn) -> None:
    await conn.execute(EXECUTION_PLAN_PERSISTENCE_SQL)


__all__ = [
    "EXECUTION_TIMESTAMP_META_SQL",
    "EXECUTION_PLAN_PERSISTENCE_SQL",
    "OUTCOME_HORIZON_SQL",
    "PLAN_EXECUTION_ATTRIBUTION_SQL",
    "ensure_execution_plan_persistence",
]

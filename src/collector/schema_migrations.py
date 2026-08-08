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
    "ensure_execution_plan_persistence",
]

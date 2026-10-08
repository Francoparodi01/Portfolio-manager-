-- Full /analisis current-policy replay over the immutable portfolio reconstruction.
-- Additive, append-only and economically non-authoritative.

CREATE TABLE IF NOT EXISTS historical_analysis_replay_runs (
    run_id                  UUID PRIMARY KEY,
    reconstruction_id      UUID NOT NULL REFERENCES historical_portfolio_reconstructions(reconstruction_id) ON DELETE RESTRICT,
    parent_contextual_run_id UUID REFERENCES historical_contextual_replay_runs(run_id) ON DELETE RESTRICT,
    owner_chat_id           BIGINT NOT NULL REFERENCES bot_users(chat_id) ON DELETE RESTRICT,
    evaluated_at            TIMESTAMPTZ NOT NULL,
    window_start            DATE NOT NULL,
    window_end              DATE NOT NULL,
    code_version            TEXT NOT NULL,
    method_version          TEXT NOT NULL,
    data_status             TEXT NOT NULL CHECK (data_status = 'CURRENT_POLICY_RETROSPECTIVE_NOT_PIT_COMPLETE'),
    mode                    TEXT NOT NULL CHECK (mode = 'SHADOW_ONLY'),
    affects_analysis        BOOLEAN NOT NULL CHECK (affects_analysis = FALSE),
    affects_execution       BOOLEAN NOT NULL CHECK (affects_execution = FALSE),
    orders_executable       BOOLEAN NOT NULL CHECK (orders_executable = FALSE),
    summary                 JSONB NOT NULL,
    source_manifest         JSONB NOT NULL,
    content_hash            TEXT NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_historical_analysis_replay_owner_created
    ON historical_analysis_replay_runs(owner_chat_id, created_at DESC);

CREATE TABLE IF NOT EXISTS historical_analysis_replay_events (
    event_id                UUID PRIMARY KEY,
    run_id                  UUID NOT NULL REFERENCES historical_analysis_replay_runs(run_id) ON DELETE RESTRICT,
    owner_chat_id           BIGINT NOT NULL REFERENCES bot_users(chat_id) ON DELETE RESTRICT,
    status                  TEXT NOT NULL CHECK (status IN ('STARTED', 'COMPLETE', 'FAILED', 'ABORTED')),
    occurred_at             TIMESTAMPTZ NOT NULL,
    reason_code             TEXT,
    details                 JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, status)
);

CREATE OR REPLACE VIEW historical_analysis_replay_state AS
SELECT DISTINCT ON (run_id)
    run_id, owner_chat_id, status, occurred_at, reason_code, details, created_at
FROM historical_analysis_replay_events
ORDER BY run_id, occurred_at DESC, created_at DESC, event_id DESC;

CREATE TABLE IF NOT EXISTS historical_analysis_replay_days (
    run_id                  UUID NOT NULL REFERENCES historical_analysis_replay_runs(run_id) ON DELETE RESTRICT,
    observed_date           DATE NOT NULL,
    portfolio_snapshot_id   TEXT NOT NULL,
    source_analysis_run_id  UUID,
    cutoff                  TIMESTAMPTZ,
    status                  TEXT NOT NULL CHECK (status IN ('COMPLETE', 'UNAVAILABLE')),
    reason                  TEXT,
    source_owner_scope      TEXT,
    positions               INTEGER,
    signals                 INTEGER,
    decisions               INTEGER,
    order_intents           INTEGER,
    blocked_orders          INTEGER,
    gate                    TEXT,
    feasible                BOOLEAN,
    cash_before             NUMERIC,
    cash_after              NUMERIC,
    productive_hash         TEXT,
    contextual_hash         TEXT,
    day_payload             JSONB NOT NULL,
    row_hash                TEXT NOT NULL CHECK (row_hash ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (run_id, observed_date),
    CHECK (status <> 'COMPLETE' OR productive_hash ~ '^[0-9a-f]{64}$'),
    CHECK (status <> 'COMPLETE' OR contextual_hash ~ '^[0-9a-f]{64}$')
);

CREATE INDEX IF NOT EXISTS idx_historical_analysis_days_status
    ON historical_analysis_replay_days(run_id, status, observed_date);

CREATE TABLE IF NOT EXISTS historical_analysis_replay_decisions (
    run_id                  UUID NOT NULL REFERENCES historical_analysis_replay_runs(run_id) ON DELETE RESTRICT,
    observed_date           DATE NOT NULL,
    ticker                  TEXT NOT NULL,
    portfolio_snapshot_id   TEXT NOT NULL,
    source_analysis_run_id  UUID NOT NULL,
    cutoff                  TIMESTAMPTZ NOT NULL,
    signal                  TEXT NOT NULL,
    score                   DOUBLE PRECISION NOT NULL CHECK (score BETWEEN -1 AND 1),
    conviction              DOUBLE PRECISION NOT NULL CHECK (conviction BETWEEN 0 AND 1),
    planner_action          TEXT NOT NULL,
    portfolio_intent        TEXT,
    current_weight          DOUBLE PRECISION,
    target_weight           DOUBLE PRECISION,
    delta_weight            DOUBLE PRECISION,
    theoretical_ars         NUMERIC,
    order_side              TEXT CHECK (order_side IS NULL OR order_side IN ('BUY', 'SELL')),
    order_amount_ars        NUMERIC,
    order_quantity          NUMERIC,
    order_blocked           BOOLEAN NOT NULL,
    context_severity        TEXT CHECK (context_severity IN ('PASS', 'WARN', 'FAIL', 'UNKNOWN')),
    context_confidence      TEXT CHECK (context_confidence IN ('HIGH', 'MEDIUM', 'LOW')),
    outcome_5d_status       TEXT,
    asset_return_5d         DOUBLE PRECISION,
    directional_return_5d   DOUBLE PRECISION,
    outcome_10d_status      TEXT,
    asset_return_10d        DOUBLE PRECISION,
    directional_return_10d  DOUBLE PRECISION,
    outcome_20d_status      TEXT,
    asset_return_20d        DOUBLE PRECISION,
    directional_return_20d  DOUBLE PRECISION,
    decision_payload        JSONB NOT NULL,
    row_hash                TEXT NOT NULL CHECK (row_hash ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (run_id, observed_date, ticker)
);

CREATE INDEX IF NOT EXISTS idx_historical_analysis_decisions_action
    ON historical_analysis_replay_decisions(run_id, planner_action, outcome_5d_status);

CREATE OR REPLACE FUNCTION quantia_validate_historical_analysis_event()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    run_owner BIGINT;
    prior_count INTEGER;
    terminal_count INTEGER;
    started_at TIMESTAMPTZ;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext(NEW.run_id::text));
    SELECT owner_chat_id INTO run_owner
      FROM historical_analysis_replay_runs WHERE run_id = NEW.run_id;
    IF run_owner IS DISTINCT FROM NEW.owner_chat_id THEN
        RAISE EXCEPTION 'historical analysis replay event owner mismatch';
    END IF;
    SELECT COUNT(*),
           COUNT(*) FILTER (WHERE status IN ('COMPLETE', 'FAILED', 'ABORTED')),
           MIN(occurred_at) FILTER (WHERE status = 'STARTED')
      INTO prior_count, terminal_count, started_at
      FROM historical_analysis_replay_events WHERE run_id = NEW.run_id;
    IF NEW.status = 'STARTED' THEN
        IF prior_count > 0 THEN RAISE EXCEPTION 'historical analysis replay already started'; END IF;
    ELSE
        IF started_at IS NULL THEN RAISE EXCEPTION 'terminal state requires STARTED'; END IF;
        IF terminal_count > 0 THEN RAISE EXCEPTION 'historical analysis replay already terminal'; END IF;
        IF NEW.occurred_at < started_at THEN RAISE EXCEPTION 'terminal state precedes STARTED'; END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION quantia_validate_historical_analysis_child()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    run_mode TEXT;
    analysis_authority BOOLEAN;
    execution_authority BOOLEAN;
    executable BOOLEAN;
    run_state TEXT;
BEGIN
    SELECT mode, affects_analysis, affects_execution, orders_executable
      INTO run_mode, analysis_authority, execution_authority, executable
      FROM historical_analysis_replay_runs WHERE run_id = NEW.run_id;
    SELECT status INTO run_state FROM historical_analysis_replay_state WHERE run_id = NEW.run_id;
    IF run_state IS DISTINCT FROM 'STARTED' THEN
        RAISE EXCEPTION 'historical analysis replay children require STARTED lifecycle';
    END IF;
    IF run_mode IS DISTINCT FROM 'SHADOW_ONLY'
       OR analysis_authority IS DISTINCT FROM FALSE
       OR execution_authority IS DISTINCT FROM FALSE
       OR executable IS DISTINCT FROM FALSE THEN
        RAISE EXCEPTION 'historical analysis replay lost non-executable shadow contract';
    END IF;
    RETURN NEW;
END;
$$;

DO $$
DECLARE
    table_name TEXT;
    trigger_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'historical_analysis_replay_runs',
        'historical_analysis_replay_events',
        'historical_analysis_replay_days',
        'historical_analysis_replay_decisions'
    ] LOOP
        trigger_name := 'har_' || substr(md5(table_name), 1, 12) || '_immutable';
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger WHERE tgname = trigger_name AND tgrelid = table_name::regclass
        ) THEN
            EXECUTE format(
                'CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION quantia_reject_historical_replay_mutation()',
                trigger_name, table_name
            );
        END IF;
        trigger_name := 'har_' || substr(md5(table_name), 1, 12) || '_no_truncate';
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger WHERE tgname = trigger_name AND tgrelid = table_name::regclass
        ) THEN
            EXECUTE format(
                'CREATE TRIGGER %I BEFORE TRUNCATE ON %I FOR EACH STATEMENT EXECUTE FUNCTION quantia_reject_historical_replay_mutation()',
                trigger_name, table_name
            );
        END IF;
    END LOOP;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'historical_analysis_replay_event_validate'
          AND tgrelid = 'historical_analysis_replay_events'::regclass
    ) THEN
        CREATE TRIGGER historical_analysis_replay_event_validate
        BEFORE INSERT ON historical_analysis_replay_events
        FOR EACH ROW EXECUTE FUNCTION quantia_validate_historical_analysis_event();
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'historical_analysis_replay_day_validate'
          AND tgrelid = 'historical_analysis_replay_days'::regclass
    ) THEN
        CREATE TRIGGER historical_analysis_replay_day_validate
        BEFORE INSERT ON historical_analysis_replay_days
        FOR EACH ROW EXECUTE FUNCTION quantia_validate_historical_analysis_child();
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'historical_analysis_replay_decision_validate'
          AND tgrelid = 'historical_analysis_replay_decisions'::regclass
    ) THEN
        CREATE TRIGGER historical_analysis_replay_decision_validate
        BEFORE INSERT ON historical_analysis_replay_decisions
        FOR EACH ROW EXECUTE FUNCTION quantia_validate_historical_analysis_child();
    END IF;
END $$;

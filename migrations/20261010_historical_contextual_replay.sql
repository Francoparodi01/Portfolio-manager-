-- Historical holdings + contextual replay evidence.
--
-- This namespace is intentionally separate from operational portfolio,
-- decisions, plans and orders.  It preserves a validated reconstruction and
-- retrospective SHADOW_ONLY research without updating any production row.

CREATE TABLE IF NOT EXISTS historical_portfolio_reconstructions (
    reconstruction_id      UUID PRIMARY KEY,
    owner_chat_id           BIGINT NOT NULL REFERENCES bot_users(chat_id) ON DELETE RESTRICT,
    window_start            DATE NOT NULL,
    window_end              DATE NOT NULL,
    canonical_rows          INTEGER NOT NULL CHECK (canonical_rows >= 0),
    canonical_sha256        TEXT NOT NULL CHECK (canonical_sha256 ~ '^[0-9a-f]{64}$'),
    events_sha256           TEXT NOT NULL CHECK (events_sha256 ~ '^[0-9a-f]{64}$'),
    source_manifest         JSONB NOT NULL,
    content_hash            TEXT NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    code_version            TEXT NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (owner_chat_id, canonical_sha256, events_sha256)
);

CREATE TABLE IF NOT EXISTS historical_portfolio_reconstruction_positions (
    reconstruction_id      UUID NOT NULL REFERENCES historical_portfolio_reconstructions(reconstruction_id) ON DELETE RESTRICT,
    observed_date           DATE NOT NULL,
    ticker                  TEXT NOT NULL,
    asset_type              TEXT,
    currency                TEXT,
    quantity_observed       NUMERIC,
    price_observed          NUMERIC,
    market_value_observed   NUMERIC,
    weight_observed         DOUBLE PRECISION,
    invested_total_ars      NUMERIC,
    cash_observed_ars       NUMERIC,
    account_total_ars       NUMERIC,
    portfolio_snapshot_id   TEXT NOT NULL,
    snapshot_scraped_at     TIMESTAMPTZ NOT NULL,
    observed_owner_chat_id  BIGINT,
    confidence              TEXT NOT NULL CHECK (confidence IN ('HIGH', 'MEDIUM', 'LOW')),
    quality_flags           TEXT NOT NULL,
    observed_payload        JSONB NOT NULL,
    row_hash                TEXT NOT NULL CHECK (row_hash ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (reconstruction_id, observed_date, ticker)
);

CREATE INDEX IF NOT EXISTS idx_historical_reconstruction_positions_owner_date
    ON historical_portfolio_reconstruction_positions(reconstruction_id, observed_date, ticker);

CREATE TABLE IF NOT EXISTS historical_portfolio_reconstruction_events (
    reconstruction_id      UUID NOT NULL REFERENCES historical_portfolio_reconstructions(reconstruction_id) ON DELETE RESTRICT,
    event_date              DATE NOT NULL,
    ticker                  TEXT NOT NULL,
    classification          TEXT NOT NULL,
    confidence              TEXT NOT NULL CHECK (confidence IN ('HIGH', 'MEDIUM', 'LOW')),
    event_payload           JSONB NOT NULL,
    row_hash                TEXT NOT NULL CHECK (row_hash ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (reconstruction_id, event_date, ticker)
);

CREATE TABLE IF NOT EXISTS historical_contextual_replay_runs (
    run_id                  UUID PRIMARY KEY,
    reconstruction_id      UUID NOT NULL REFERENCES historical_portfolio_reconstructions(reconstruction_id) ON DELETE RESTRICT,
    owner_chat_id           BIGINT NOT NULL REFERENCES bot_users(chat_id) ON DELETE RESTRICT,
    evaluated_at            TIMESTAMPTZ NOT NULL,
    window_start            DATE NOT NULL,
    window_end              DATE NOT NULL,
    code_version            TEXT NOT NULL,
    method_version          TEXT NOT NULL,
    data_status             TEXT NOT NULL CHECK (data_status = 'RETROSPECTIVE_MARKET_HISTORY_NOT_PIT'),
    mode                    TEXT NOT NULL CHECK (mode = 'SHADOW_ONLY'),
    affects_analysis        BOOLEAN NOT NULL CHECK (affects_analysis = FALSE),
    affects_execution       BOOLEAN NOT NULL CHECK (affects_execution = FALSE),
    summary                 JSONB NOT NULL,
    source_manifest         JSONB NOT NULL,
    content_hash            TEXT NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_historical_contextual_replay_owner_created
    ON historical_contextual_replay_runs(owner_chat_id, created_at DESC);

CREATE TABLE IF NOT EXISTS historical_contextual_replay_run_events (
    event_id                UUID PRIMARY KEY,
    run_id                  UUID NOT NULL REFERENCES historical_contextual_replay_runs(run_id) ON DELETE RESTRICT,
    owner_chat_id           BIGINT NOT NULL REFERENCES bot_users(chat_id) ON DELETE RESTRICT,
    status                  TEXT NOT NULL CHECK (status IN ('STARTED', 'COMPLETE', 'FAILED', 'ABORTED')),
    occurred_at             TIMESTAMPTZ NOT NULL,
    reason_code             TEXT,
    details                 JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, status)
);

CREATE INDEX IF NOT EXISTS idx_historical_contextual_replay_events_state
    ON historical_contextual_replay_run_events(run_id, occurred_at DESC, created_at DESC);

CREATE OR REPLACE VIEW historical_contextual_replay_state AS
SELECT DISTINCT ON (run_id)
    run_id,
    owner_chat_id,
    status,
    occurred_at,
    reason_code,
    details,
    created_at
FROM historical_contextual_replay_run_events
ORDER BY run_id, occurred_at DESC, created_at DESC, event_id DESC;

CREATE TABLE IF NOT EXISTS historical_contextual_replay_results (
    run_id                          UUID NOT NULL REFERENCES historical_contextual_replay_runs(run_id) ON DELETE RESTRICT,
    observed_date                   DATE NOT NULL,
    ticker                          TEXT NOT NULL,
    market_session                  DATE,
    portfolio_snapshot_id           TEXT NOT NULL,
    analysis_status                 TEXT NOT NULL,
    metric_eligible                 BOOLEAN NOT NULL,
    metric_exclusion_reason         TEXT,
    portfolio_confidence            TEXT NOT NULL,
    old_signal                      TEXT,
    old_score_raw                   DOUBLE PRECISION,
    old_strength                    DOUBLE PRECISION,
    new_signal                      TEXT,
    context_severity                TEXT,
    context_confidence              TEXT,
    context_confidence_value        DOUBLE PRECISION,
    asset_return_5d                 DOUBLE PRECISION,
    directional_or_hold_return_5d   DOUBLE PRECISION,
    outcome_5d_status               TEXT,
    asset_return_10d                DOUBLE PRECISION,
    directional_or_hold_return_10d  DOUBLE PRECISION,
    outcome_10d_status              TEXT,
    asset_return_20d                DOUBLE PRECISION,
    directional_or_hold_return_20d  DOUBLE PRECISION,
    outcome_20d_status              TEXT,
    result_payload                  JSONB NOT NULL,
    row_hash                        TEXT NOT NULL CHECK (row_hash ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (run_id, observed_date, ticker),
    CHECK (old_signal IS NULL OR old_signal IN ('BUY', 'SELL', 'HOLD')),
    CHECK (new_signal IS NULL OR new_signal IN ('BUY', 'SELL', 'HOLD')),
    CHECK (context_severity IS NULL OR context_severity IN ('PASS', 'WARN', 'FAIL', 'UNKNOWN')),
    CHECK (context_confidence IS NULL OR context_confidence IN ('HIGH', 'MEDIUM', 'LOW')),
    CHECK (old_score_raw IS NULL OR old_score_raw BETWEEN -20 AND 20),
    CHECK (old_strength IS NULL OR old_strength BETWEEN 0 AND 1),
    CHECK (context_confidence_value IS NULL OR context_confidence_value BETWEEN 0 AND 1),
    CHECK (new_signal IS NULL OR old_signal = new_signal)
);

CREATE INDEX IF NOT EXISTS idx_historical_contextual_results_run_session
    ON historical_contextual_replay_results(run_id, market_session, ticker);
CREATE INDEX IF NOT EXISTS idx_historical_contextual_results_run_context
    ON historical_contextual_replay_results(run_id, context_severity, old_signal)
    WHERE metric_eligible = TRUE;

CREATE OR REPLACE FUNCTION quantia_reject_historical_replay_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'historical reconstruction and replay evidence is append-only';
END;
$$;

CREATE OR REPLACE FUNCTION quantia_validate_historical_replay_event()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    run_owner BIGINT;
    prior_count INTEGER;
    terminal_count INTEGER;
    started_at TIMESTAMPTZ;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext(NEW.run_id::text));
    SELECT owner_chat_id INTO run_owner
      FROM historical_contextual_replay_runs
     WHERE run_id = NEW.run_id;
    IF run_owner IS DISTINCT FROM NEW.owner_chat_id THEN
        RAISE EXCEPTION 'historical replay event owner mismatch';
    END IF;
    SELECT COUNT(*),
           COUNT(*) FILTER (WHERE status IN ('COMPLETE', 'FAILED', 'ABORTED')),
           MIN(occurred_at) FILTER (WHERE status = 'STARTED')
      INTO prior_count, terminal_count, started_at
      FROM historical_contextual_replay_run_events
     WHERE run_id = NEW.run_id;
    IF NEW.status = 'STARTED' THEN
        IF prior_count > 0 THEN
            RAISE EXCEPTION 'historical replay already started';
        END IF;
    ELSE
        IF started_at IS NULL THEN
            RAISE EXCEPTION 'historical replay terminal state requires STARTED';
        END IF;
        IF terminal_count > 0 THEN
            RAISE EXCEPTION 'historical replay already terminal';
        END IF;
        IF NEW.occurred_at < started_at THEN
            RAISE EXCEPTION 'historical replay terminal state precedes STARTED';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION quantia_validate_historical_replay_result()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    run_owner BIGINT;
    run_mode TEXT;
    run_affects_analysis BOOLEAN;
    run_affects_execution BOOLEAN;
    run_state TEXT;
BEGIN
    SELECT owner_chat_id, mode, affects_analysis, affects_execution
      INTO run_owner, run_mode, run_affects_analysis, run_affects_execution
      FROM historical_contextual_replay_runs
     WHERE run_id = NEW.run_id;
    SELECT status INTO run_state
      FROM historical_contextual_replay_state
     WHERE run_id = NEW.run_id;
    IF run_state IS DISTINCT FROM 'STARTED' THEN
        RAISE EXCEPTION 'historical replay results require STARTED lifecycle';
    END IF;
    IF run_mode IS DISTINCT FROM 'SHADOW_ONLY'
       OR run_affects_analysis IS DISTINCT FROM FALSE
       OR run_affects_execution IS DISTINCT FROM FALSE THEN
        RAISE EXCEPTION 'historical replay lost shadow-only authority contract';
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
        'historical_portfolio_reconstructions',
        'historical_portfolio_reconstruction_positions',
        'historical_portfolio_reconstruction_events',
        'historical_contextual_replay_runs',
        'historical_contextual_replay_run_events',
        'historical_contextual_replay_results'
    ] LOOP
        trigger_name := 'hr_' || substr(md5(table_name), 1, 12) || '_immutable';
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger
             WHERE tgname = trigger_name
               AND tgrelid = table_name::regclass
        ) THEN
            EXECUTE format(
                'CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION quantia_reject_historical_replay_mutation()',
                trigger_name,
                table_name
            );
        END IF;
        trigger_name := 'hr_' || substr(md5(table_name), 1, 12) || '_no_truncate';
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger
             WHERE tgname = trigger_name
               AND tgrelid = table_name::regclass
        ) THEN
            EXECUTE format(
                'CREATE TRIGGER %I BEFORE TRUNCATE ON %I FOR EACH STATEMENT EXECUTE FUNCTION quantia_reject_historical_replay_mutation()',
                trigger_name,
                table_name
            );
        END IF;
    END LOOP;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
         WHERE tgname = 'historical_contextual_replay_event_validate'
           AND tgrelid = 'historical_contextual_replay_run_events'::regclass
    ) THEN
        CREATE TRIGGER historical_contextual_replay_event_validate
        BEFORE INSERT ON historical_contextual_replay_run_events
        FOR EACH ROW EXECUTE FUNCTION quantia_validate_historical_replay_event();
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
         WHERE tgname = 'historical_contextual_replay_result_validate'
           AND tgrelid = 'historical_contextual_replay_results'::regclass
    ) THEN
        CREATE TRIGGER historical_contextual_replay_result_validate
        BEFORE INSERT ON historical_contextual_replay_results
        FOR EACH ROW EXECUTE FUNCTION quantia_validate_historical_replay_result();
    END IF;
END $$;

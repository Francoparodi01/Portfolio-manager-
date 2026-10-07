-- Quantia Contextual Analysis G3
-- Extends the G2 evidence tables in place. No parallel candle store is added.

ALTER TABLE market_candle_observations
    ADD COLUMN IF NOT EXISTS observation_digest TEXT;

-- G2's uniqueness omitted owner/capture/provider identity.  Replace only that
-- constraint, preserving every row, with the observation identity used by G3.
DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'market_candle_observations_instrument_id_interval_candle_ti_key'
          AND conrelid = 'market_candle_observations'::regclass
    ) THEN
        ALTER TABLE market_candle_observations
            DROP CONSTRAINT market_candle_observations_instrument_id_interval_candle_ti_key;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'market_candle_observations_g3_identity_key'
          AND conrelid = 'market_candle_observations'::regclass
    ) THEN
        ALTER TABLE market_candle_observations
            ADD CONSTRAINT market_candle_observations_g3_identity_key
            UNIQUE (
                owner_chat_id, ingestion_run_id, instrument_id, market,
                provider_symbol, interval, bar_start, scraped_at, source,
                observation_digest
            );
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'market_candle_observations_digest_format'
          AND conrelid = 'market_candle_observations'::regclass
    ) THEN
        ALTER TABLE market_candle_observations
            ADD CONSTRAINT market_candle_observations_digest_format
            CHECK (
                observation_digest IS NULL
                OR observation_digest ~ '^[0-9a-f]{64}$'
            );
    END IF;
END $$;

-- The same observation may legitimately serve as both a general and a sector
-- benchmark.  Preserve role identity while rejecting duplicates within a role.
DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'contextual_snapshot_candles_snapshot_id_observation_id_key'
          AND conrelid = 'contextual_snapshot_candles'::regclass
    ) THEN
        ALTER TABLE contextual_snapshot_candles
            DROP CONSTRAINT contextual_snapshot_candles_snapshot_id_observation_id_key;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'contextual_snapshot_candles_g3_role_observation_key'
          AND conrelid = 'contextual_snapshot_candles'::regclass
    ) THEN
        ALTER TABLE contextual_snapshot_candles
            ADD CONSTRAINT contextual_snapshot_candles_g3_role_observation_key
            UNIQUE (snapshot_id, input_role, observation_id);
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS market_evidence_capture_events (
    event_id        UUID PRIMARY KEY,
    capture_id      UUID NOT NULL,
    owner_chat_id   BIGINT NOT NULL REFERENCES bot_users(chat_id) ON DELETE RESTRICT,
    status          TEXT NOT NULL CHECK (status IN ('STARTED', 'COMPLETE', 'ABORTED', 'FAILED')),
    occurred_at     TIMESTAMPTZ NOT NULL,
    reason_code     TEXT,
    details         JSONB NOT NULL DEFAULT '{}'::jsonb,
    code_version    TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (capture_id, status)
);

CREATE INDEX IF NOT EXISTS idx_market_evidence_capture_events_state
    ON market_evidence_capture_events(capture_id, occurred_at DESC, created_at DESC);

CREATE OR REPLACE VIEW market_evidence_capture_state AS
SELECT DISTINCT ON (capture_id)
    capture_id,
    owner_chat_id,
    status,
    occurred_at,
    reason_code,
    details,
    code_version,
    created_at
FROM market_evidence_capture_events
ORDER BY capture_id, occurred_at DESC, created_at DESC, event_id DESC;

CREATE OR REPLACE FUNCTION quantia_validate_market_capture_event()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    prior_count INTEGER;
    prior_owner BIGINT;
    terminal_count INTEGER;
    started_at TIMESTAMPTZ;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext(NEW.capture_id::text));

    SELECT COUNT(*), MIN(owner_chat_id),
           COUNT(*) FILTER (WHERE status IN ('COMPLETE', 'ABORTED', 'FAILED')),
           MIN(occurred_at) FILTER (WHERE status = 'STARTED')
      INTO prior_count, prior_owner, terminal_count, started_at
      FROM market_evidence_capture_events
     WHERE capture_id = NEW.capture_id;

    IF prior_count > 0 AND prior_owner IS DISTINCT FROM NEW.owner_chat_id THEN
        RAISE EXCEPTION 'market evidence capture owner mismatch';
    END IF;
    IF NEW.status = 'STARTED' THEN
        IF prior_count > 0 THEN
            RAISE EXCEPTION 'market evidence capture already started';
        END IF;
    ELSE
        IF prior_count = 0 OR NOT EXISTS (
            SELECT 1 FROM market_evidence_capture_events
             WHERE capture_id = NEW.capture_id AND status = 'STARTED'
        ) THEN
            RAISE EXCEPTION 'market evidence terminal state requires STARTED';
        END IF;
        IF terminal_count > 0 THEN
            RAISE EXCEPTION 'market evidence capture already terminal';
        END IF;
        IF NEW.occurred_at < started_at THEN
            RAISE EXCEPTION 'market evidence terminal state precedes STARTED';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION quantia_validate_market_observation_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    capture_owner BIGINT;
    capture_status TEXT;
BEGIN
    IF COALESCE(NEW.observation_digest, '') !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION 'new market observation requires sha256 observation_digest';
    END IF;
    SELECT owner_chat_id, status
      INTO capture_owner, capture_status
      FROM market_evidence_capture_state
     WHERE capture_id = NEW.ingestion_run_id;
    IF capture_status IS DISTINCT FROM 'STARTED' THEN
        RAISE EXCEPTION 'market observation requires a STARTED capture';
    END IF;
    IF capture_owner IS DISTINCT FROM NEW.owner_chat_id THEN
        RAISE EXCEPTION 'market observation owner does not match capture';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION quantia_validate_contextual_g2_snapshot()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    plan_owner BIGINT;
    plan_run UUID;
    portfolio_owner BIGINT;
    capture_owner BIGINT;
    capture_status TEXT;
BEGIN
    SELECT owner_chat_id, run_id INTO plan_owner, plan_run
      FROM execution_plans WHERE id = NEW.plan_id;
    IF plan_owner IS DISTINCT FROM NEW.owner_chat_id OR plan_run IS DISTINCT FROM NEW.run_id THEN
        RAISE EXCEPTION 'contextual snapshot owner/run does not match execution plan';
    END IF;

    SELECT owner_chat_id INTO portfolio_owner
      FROM portfolio_snapshots WHERE snapshot_id = NEW.portfolio_snapshot_id;
    IF portfolio_owner IS DISTINCT FROM NEW.owner_chat_id THEN
        RAISE EXCEPTION 'contextual snapshot owner does not match portfolio snapshot';
    END IF;

    SELECT owner_chat_id, status INTO capture_owner, capture_status
      FROM market_evidence_capture_state WHERE capture_id = NEW.run_id;
    IF capture_status IS DISTINCT FROM 'STARTED'
       OR capture_owner IS DISTINCT FROM NEW.owner_chat_id THEN
        RAISE EXCEPTION 'contextual snapshot requires matching STARTED capture';
    END IF;

    IF COALESCE(NEW.feature_snapshot_v3->>'schema_version', '') <> 'feature_snapshot_v3' THEN
        RAISE EXCEPTION 'contextual snapshot requires feature_snapshot_v3';
    END IF;
    IF COALESCE(NEW.contextual_snapshot->>'mode', '') <> 'SHADOW_ONLY'
       OR COALESCE((NEW.contextual_snapshot->>'affects_analysis')::boolean, TRUE)
       OR COALESCE((NEW.contextual_snapshot->>'affects_execution')::boolean, TRUE) THEN
        RAISE EXCEPTION 'contextual snapshot must remain SHADOW_ONLY and neutral';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION quantia_validate_contextual_snapshot_candle()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    observation_capture UUID;
    observation_owner BIGINT;
    capture_owner BIGINT;
    capture_status TEXT;
    snapshot_owner BIGINT;
    snapshot_run UUID;
BEGIN
    SELECT ingestion_run_id, owner_chat_id
      INTO observation_capture, observation_owner
      FROM market_candle_observations
     WHERE observation_id = NEW.observation_id;
    SELECT owner_chat_id, status
      INTO capture_owner, capture_status
      FROM market_evidence_capture_state
     WHERE capture_id = observation_capture;
    SELECT owner_chat_id, run_id INTO snapshot_owner, snapshot_run
      FROM contextual_market_snapshots WHERE snapshot_id = NEW.snapshot_id;

    IF capture_status IS DISTINCT FROM 'COMPLETE'
       AND NOT (
           observation_capture = snapshot_run
           AND capture_status = 'STARTED'
       ) THEN
        RAISE EXCEPTION 'contextual snapshot can only link COMPLETE market evidence';
    END IF;
    IF observation_owner IS DISTINCT FROM capture_owner
       OR observation_owner IS DISTINCT FROM snapshot_owner THEN
        RAISE EXCEPTION 'contextual snapshot candle owner mismatch';
    END IF;
    RETURN NEW;
END;
$$;

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'quantia_market_capture_events_validate'
          AND tgrelid = 'market_evidence_capture_events'::regclass
    ) THEN
        CREATE TRIGGER quantia_market_capture_events_validate
        BEFORE INSERT ON market_evidence_capture_events
        FOR EACH ROW EXECUTE FUNCTION quantia_validate_market_capture_event();
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'quantia_market_capture_events_immutable'
          AND tgrelid = 'market_evidence_capture_events'::regclass
    ) THEN
        CREATE TRIGGER quantia_market_capture_events_immutable
        BEFORE UPDATE OR DELETE ON market_evidence_capture_events
        FOR EACH ROW EXECUTE FUNCTION quantia_reject_contextual_g2_mutation();
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'quantia_market_observations_validate_insert'
          AND tgrelid = 'market_candle_observations'::regclass
    ) THEN
        CREATE TRIGGER quantia_market_observations_validate_insert
        BEFORE INSERT ON market_candle_observations
        FOR EACH ROW EXECUTE FUNCTION quantia_validate_market_observation_insert();
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'quantia_contextual_snapshot_candles_validate'
          AND tgrelid = 'contextual_snapshot_candles'::regclass
    ) THEN
        CREATE TRIGGER quantia_contextual_snapshot_candles_validate
        BEFORE INSERT ON contextual_snapshot_candles
        FOR EACH ROW EXECUTE FUNCTION quantia_validate_contextual_snapshot_candle();
    END IF;
END $$;

DO $$
DECLARE
    target_table TEXT;
    trigger_name TEXT;
BEGIN
    FOREACH target_table IN ARRAY ARRAY[
        'market_candle_observations',
        'contextual_market_snapshots',
        'contextual_snapshot_candles',
        'market_evidence_capture_events'
    ] LOOP
        trigger_name := 'quantia_' || target_table || '_no_truncate';
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger
            WHERE tgname = trigger_name
              AND tgrelid = target_table::regclass
        ) THEN
            EXECUTE format(
                'CREATE TRIGGER %I BEFORE TRUNCATE ON %I '
                'FOR EACH STATEMENT EXECUTE FUNCTION quantia_reject_contextual_g2_mutation()',
                trigger_name,
                target_table
            );
        END IF;
    END LOOP;
END $$;

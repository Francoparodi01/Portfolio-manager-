-- Quantia Contextual Analysis G2
-- Additive only. Existing candle rows deliberately remain NULL for facts that
-- were not observed when they were collected.

ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS bar_start TIMESTAMPTZ;
ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS bar_end TIMESTAMPTZ;
ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ;
ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS is_closed BOOLEAN;
ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS volume_unit TEXT;
ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS calendar TEXT;
ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS calendar_validation TEXT;
ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS adjustment_policy TEXT;
ALTER TABLE market_candles ADD COLUMN IF NOT EXISTS depositary_ratio TEXT;

CREATE TABLE IF NOT EXISTS market_candle_observations (
    observation_id      UUID PRIMARY KEY,
    owner_chat_id       BIGINT NOT NULL REFERENCES bot_users(chat_id) ON DELETE RESTRICT,
    ingestion_run_id    UUID NOT NULL,
    instrument_id       TEXT NOT NULL,
    ticker              TEXT NOT NULL,
    provider_symbol     TEXT NOT NULL,
    asset_type          TEXT NOT NULL,
    market              TEXT NOT NULL,
    currency            TEXT NOT NULL,
    interval            TEXT NOT NULL,
    candle_timestamp    TIMESTAMPTZ NOT NULL,
    bar_start           TIMESTAMPTZ NOT NULL,
    bar_end             TIMESTAMPTZ,
    available_at        TIMESTAMPTZ NOT NULL,
    scraped_at          TIMESTAMPTZ NOT NULL,
    is_closed           BOOLEAN,
    open_price          NUMERIC(24,8) NOT NULL,
    high_price          NUMERIC(24,8) NOT NULL,
    low_price           NUMERIC(24,8) NOT NULL,
    close_price         NUMERIC(24,8) NOT NULL,
    volume              NUMERIC(28,8),
    price_unit          TEXT,
    volume_unit         TEXT,
    source              TEXT NOT NULL,
    provenance          JSONB NOT NULL DEFAULT '{}'::jsonb,
    quality             JSONB NOT NULL DEFAULT '{}'::jsonb,
    missingness         JSONB NOT NULL DEFAULT '[]'::jsonb,
    code_version        TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (instrument_id, interval, candle_timestamp, scraped_at, source),
    CHECK (available_at <= scraped_at),
    CHECK (bar_end IS NULL OR bar_end >= bar_start),
    CHECK (open_price > 0 AND high_price > 0 AND low_price > 0 AND close_price > 0),
    CHECK (high_price >= GREATEST(open_price, close_price, low_price)),
    CHECK (low_price <= LEAST(open_price, close_price, high_price)),
    CHECK (volume IS NULL OR volume >= 0)
);

CREATE INDEX IF NOT EXISTS idx_market_candle_observations_pit
    ON market_candle_observations(
        instrument_id, interval, candle_timestamp, available_at, scraped_at
    );

CREATE INDEX IF NOT EXISTS idx_market_candle_observations_run
    ON market_candle_observations(owner_chat_id, ingestion_run_id, scraped_at);

CREATE TABLE IF NOT EXISTS contextual_market_snapshots (
    snapshot_id             TEXT PRIMARY KEY,
    owner_chat_id           BIGINT NOT NULL REFERENCES bot_users(chat_id) ON DELETE RESTRICT,
    run_id                  UUID NOT NULL,
    plan_id                 UUID NOT NULL REFERENCES execution_plans(id) ON DELETE RESTRICT,
    portfolio_snapshot_id   UUID NOT NULL REFERENCES portfolio_snapshots(snapshot_id) ON DELETE RESTRICT,
    instrument_id           TEXT NOT NULL,
    signal                  TEXT NOT NULL,
    conviction              DOUBLE PRECISION NOT NULL,
    cutoff                  TIMESTAMPTZ NOT NULL,
    feature_snapshot_v3     JSONB NOT NULL,
    contextual_snapshot     JSONB NOT NULL,
    input_hashes            JSONB NOT NULL DEFAULT '{}'::jsonb,
    productive_baseline     JSONB NOT NULL,
    productive_shadow       JSONB NOT NULL,
    non_regression          JSONB NOT NULL,
    code_version            TEXT NOT NULL,
    mode                    TEXT NOT NULL DEFAULT 'SHADOW_ONLY',
    affects_analysis        BOOLEAN NOT NULL DEFAULT FALSE,
    affects_execution       BOOLEAN NOT NULL DEFAULT FALSE,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (owner_chat_id, run_id, plan_id, instrument_id),
    CHECK (mode = 'SHADOW_ONLY'),
    CHECK (affects_analysis = FALSE),
    CHECK (affects_execution = FALSE),
    CHECK (conviction >= 0 AND conviction <= 1)
);

CREATE INDEX IF NOT EXISTS idx_contextual_market_snapshots_lineage
    ON contextual_market_snapshots(owner_chat_id, run_id, plan_id, cutoff);

CREATE TABLE IF NOT EXISTS contextual_snapshot_candles (
    snapshot_id         TEXT NOT NULL REFERENCES contextual_market_snapshots(snapshot_id) ON DELETE RESTRICT,
    observation_id      UUID NOT NULL REFERENCES market_candle_observations(observation_id) ON DELETE RESTRICT,
    input_role          TEXT NOT NULL CHECK (input_role IN ('ASSET', 'GENERAL_BENCHMARK', 'SECTOR_BENCHMARK')),
    ordinal             INTEGER NOT NULL CHECK (ordinal >= 0),
    PRIMARY KEY (snapshot_id, input_role, ordinal),
    UNIQUE (snapshot_id, observation_id)
);

CREATE OR REPLACE FUNCTION quantia_reject_contextual_g2_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Quantia contextual G2 evidence is append-only';
END;
$$;

CREATE OR REPLACE FUNCTION quantia_validate_contextual_g2_snapshot()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    plan_owner BIGINT;
    plan_run UUID;
    portfolio_owner BIGINT;
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

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'quantia_market_candle_observations_immutable'
          AND tgrelid = 'market_candle_observations'::regclass
    ) THEN
        CREATE TRIGGER quantia_market_candle_observations_immutable
        BEFORE UPDATE OR DELETE ON market_candle_observations
        FOR EACH ROW EXECUTE FUNCTION quantia_reject_contextual_g2_mutation();
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'quantia_contextual_market_snapshots_validate'
          AND tgrelid = 'contextual_market_snapshots'::regclass
    ) THEN
        CREATE TRIGGER quantia_contextual_market_snapshots_validate
        BEFORE INSERT ON contextual_market_snapshots
        FOR EACH ROW EXECUTE FUNCTION quantia_validate_contextual_g2_snapshot();
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'quantia_contextual_market_snapshots_immutable'
          AND tgrelid = 'contextual_market_snapshots'::regclass
    ) THEN
        CREATE TRIGGER quantia_contextual_market_snapshots_immutable
        BEFORE UPDATE OR DELETE ON contextual_market_snapshots
        FOR EACH ROW EXECUTE FUNCTION quantia_reject_contextual_g2_mutation();
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'quantia_contextual_snapshot_candles_immutable'
          AND tgrelid = 'contextual_snapshot_candles'::regclass
    ) THEN
        CREATE TRIGGER quantia_contextual_snapshot_candles_immutable
        BEFORE UPDATE OR DELETE ON contextual_snapshot_candles
        FOR EACH ROW EXECUTE FUNCTION quantia_reject_contextual_g2_mutation();
    END IF;
END $$;

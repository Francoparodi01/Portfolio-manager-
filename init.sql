-- init.sql — Schema completo de Cocos Copilot
-- Idempotente: seguro de correr múltiples veces (IF NOT EXISTS en todo)

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- TimescaleDB es opcional: la nube puede correr sobre PostgreSQL comun.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_available_extensions
        WHERE name = 'timescaledb'
    ) THEN
        CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
    END IF;
END
$$;

-- ── portfolio_snapshots ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    snapshot_id      UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    scraped_at       TIMESTAMPTZ NOT NULL,
    total_value_ars  NUMERIC(20,4),
    cash_ars         NUMERIC(20,4),
    confidence_score FLOAT,
    dom_hash         TEXT,
    raw_html_hash    TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── positions (hypertable) ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS positions (
    id                  BIGSERIAL,
    snapshot_id         UUID        NOT NULL REFERENCES portfolio_snapshots(snapshot_id) ON DELETE CASCADE,
    scraped_at          TIMESTAMPTZ NOT NULL,
    ticker              TEXT        NOT NULL,
    asset_type          TEXT,
    currency            TEXT,
    quantity            NUMERIC(20,8),
    avg_cost            NUMERIC(20,4),
    current_price       NUMERIC(20,4),
    market_value        NUMERIC(20,4),
    unrealized_pnl      NUMERIC(20,4),
    unrealized_pnl_pct  NUMERIC(10,6),
    weight_in_portfolio NUMERIC(10,6),
    sector              TEXT,
    PRIMARY KEY (id, scraped_at),
    UNIQUE (snapshot_id, ticker, scraped_at)
);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_extension
        WHERE extname = 'timescaledb'
    ) THEN
        PERFORM create_hypertable('positions', 'scraped_at', if_not_exists => TRUE);
    END IF;
END
$$;

-- ── market_prices (hypertable) ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS market_prices (
    ts            TIMESTAMPTZ NOT NULL,
    ticker        TEXT        NOT NULL,
    asset_type    TEXT,
    currency      TEXT,
    last_price    NUMERIC(20,4),
    change_pct_1d NUMERIC(10,6),
    volume        NUMERIC(20,2),
    UNIQUE (ts, ticker)
);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_extension
        WHERE extname = 'timescaledb'
    ) THEN
        PERFORM create_hypertable('market_prices', 'ts', if_not_exists => TRUE);
    END IF;
END
$$;

-- ── market_candles (hypertable) ───────────────────────────────────────────────
-- Velas OHLCV locales de Cocos/BYMA para ACCIONES y CEDEARS.
CREATE TABLE IF NOT EXISTS market_candles (
    ts             TIMESTAMPTZ NOT NULL,
    ticker         TEXT        NOT NULL,
    long_ticker    TEXT        NOT NULL,
    asset_type     TEXT        NOT NULL,
    currency       TEXT        NOT NULL,
    venue          TEXT        NOT NULL,
    interval       TEXT        NOT NULL DEFAULT '1d',
    open_price     NUMERIC(20,4),
    high_price     NUMERIC(20,4),
    low_price      NUMERIC(20,4),
    close_price    NUMERIC(20,4),
    volume         NUMERIC(20,4),
    source         TEXT        NOT NULL DEFAULT 'COCOS',
    scraped_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (ts, long_ticker, interval)
);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_extension
        WHERE extname = 'timescaledb'
    ) THEN
        PERFORM create_hypertable('market_candles', 'ts', if_not_exists => TRUE);
    END IF;
END
$$;

-- ── raw_snapshots (hypertable) ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_snapshots (
    snapshot_id UUID        NOT NULL REFERENCES portfolio_snapshots(snapshot_id) ON DELETE CASCADE,
    scraped_at  TIMESTAMPTZ NOT NULL,
    payload     JSONB       NOT NULL,
    PRIMARY KEY (snapshot_id, scraped_at)
);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_extension
        WHERE extname = 'timescaledb'
    ) THEN
        PERFORM create_hypertable('raw_snapshots', 'scraped_at', if_not_exists => TRUE);
    END IF;
END
$$;

-- ── bot_users ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bot_users (
    chat_id      BIGINT PRIMARY KEY,
    cocos_user   TEXT,
    cocos_pass   TEXT,
    mfa_timeout  INTEGER NOT NULL DEFAULT 120,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── decision_log ──────────────────────────────────────────────────────────────
-- Tabla central de decisiones, trades y lifecycle.
-- Columnas base + columnas trade_lifecycle agregadas de forma additive.
CREATE TABLE IF NOT EXISTS decision_log (
    id                BIGSERIAL    PRIMARY KEY,
    decided_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    decision_date     DATE         GENERATED ALWAYS AS ((decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date) STORED,
    ticker            TEXT         NOT NULL,
    -- Campo legacy: 'BUY' | 'SELL' | 'HOLD'
    -- Para semántica completa usar decision_type
    decision          TEXT         NOT NULL,
    final_score       FLOAT        NOT NULL,
    confidence        FLOAT        NOT NULL,
    layers            JSONB,

    -- Precio y contexto de mercado al momento de la decisión
    price_at_decision FLOAT,
    vix_at_decision   FLOAT,
    regime            TEXT,

    -- Outcomes (rellenados por update_outcomes)
    outcome_5d        FLOAT,
    outcome_10d       FLOAT,
    outcome_20d       FLOAT,
    outcome_filled_at TIMESTAMPTZ,
    was_correct       BOOLEAN,
    -- Outcome basis:
    --   canonical_cocos = comparable with the canonical market_candles series
    --   legacy_external = legacy row stored in another price basis
    outcome_basis       TEXT,
    outcome_basis_ratio FLOAT,

    -- Sizing y riesgo básico
    size_pct          FLOAT,
    stop_loss_pct     FLOAT,
    target_pct        FLOAT,
    horizon_days      INTEGER,
    rr_ratio          FLOAT,

    -- ── trade_lifecycle columns ───────────────────────────────────────────────
    -- decision_type: semántica extendida
    --   BUY           = compra por señal real del activo
    --   BUY_REBALANCE = aumento por optimizer, señal floja
    --   SELL_PARTIAL  = recorte parcial
    --   SELL_FULL     = salida total (stop, target, invalidación)
    --   HOLD          = sin acción
    decision_type     TEXT,

    -- Intensidad de la señal del activo al momento de la decisión
    -- FUERTE | MODERADA | DÉBIL | NEGATIVA
    signal_strength   TEXT,

    -- Precios absolutos de stop y target (en USD, no porcentaje)
    stop_loss_price   FLOAT,
    target_price      FLOAT,

    -- Política de salida
    exit_scope        TEXT,         -- FULL | PARTIAL
    exit_reason_rule  TEXT,         -- STOP_LOSS | TARGET_HIT | HORIZON_END | REBALANCE | MANUAL
    stop_policy       TEXT,         -- HARD | CLOSE_ONLY | TRAILING
    stop_source       TEXT,         -- FIXED | ATR | VIX_DYNAMIC

    trailing_active   BOOLEAN DEFAULT FALSE,

    -- Cierre del trade (rellenado cuando se cierra)
    was_stopped       BOOLEAN,      -- TRUE si cerró por stop-loss
    exit_reason       TEXT,         -- razón final de cierre (free-form)
    closed_at         TIMESTAMPTZ,  -- timestamp del cierre
    close_price       FLOAT,        -- precio de cierre efectivo

    -- Origen de la decisión
    source            TEXT,         -- 'signal' | 'optimizer' | 'execution_plan'

    -- Auditoría operativa: planner vs ejecución real
    status                 TEXT,    -- THEORETICAL | APPROVED | BLOCKED | EXECUTED | SKIPPED
    block_reason           TEXT,
    theoretical_amount_ars FLOAT,
    executed_amount_ars    FLOAT,
    current_weight         FLOAT,
    target_weight          FLOAT,
    delta_weight           FLOAT,
    is_executable          BOOLEAN,
    was_blocked            BOOLEAN
);

-- ── broker_fills ──────────────────────────────────────────────────────────────
-- Fills reales confirmados por broker. Hoy entran por import manual; la tabla
-- queda lista para una fuente automática futura si aparece una API confiable.
CREATE TABLE IF NOT EXISTS broker_fills (
    id               BIGSERIAL     PRIMARY KEY,
    source           TEXT          NOT NULL DEFAULT 'manual_import',
    external_fill_id TEXT          NOT NULL,
    executed_at      TIMESTAMPTZ   NOT NULL,
    ticker           TEXT          NOT NULL,
    side             TEXT          NOT NULL CHECK (side IN ('BUY', 'SELL')),
    quantity         NUMERIC(20,8) NOT NULL,
    avg_fill_price   NUMERIC(20,4) NOT NULL,
    gross_amount_ars NUMERIC(20,4),
    fees_ars         NUMERIC(20,4),
    raw_payload      JSONB,
    decision_log_id  BIGINT REFERENCES decision_log(id) ON DELETE SET NULL,
    reconciled_at    TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source, external_fill_id)
);

-- FEATURE: ML - feature store experimental para entrenamiento e inferencia.
CREATE TABLE IF NOT EXISTS ml_decision_features (
    decision_log_id                  BIGINT PRIMARY KEY REFERENCES decision_log(id) ON DELETE CASCADE,
    ticker                           TEXT NOT NULL,
    captured_at                      TIMESTAMPTZ NOT NULL,
    decision                         TEXT,
    regime                           TEXT,
    source                           TEXT,
    final_score                      FLOAT,
    confidence_score                 FLOAT,
    prob_target_hit_prior            FLOAT,
    expected_value_prior             FLOAT,
    stop_loss_pct                    FLOAT,
    target_pct                       FLOAT,
    rr_ratio                         FLOAT,
    horizon_days                     FLOAT,
    size_pct                         FLOAT,
    technical_score                  FLOAT,
    rsi_14                           FLOAT,
    macd_hist                        FLOAT,
    bb_pos                           FLOAT,
    atr_pct                          FLOAT,
    distance_sma20_pct               FLOAT,
    distance_sma50_pct               FLOAT,
    distance_sma200_pct              FLOAT,
    momentum_20d                     FLOAT,
    momentum_60d                     FLOAT,
    volatility_20d                   FLOAT,
    drawdown_60d                     FLOAT,
    macro_score                      FLOAT,
    vix_level                        FLOAT,
    spy_return_5d                    FLOAT,
    spy_return_20d                   FLOAT,
    dxy_return_20d                   FLOAT,
    tnx_level                        FLOAT,
    wti_return_20d                   FLOAT,
    regime_code                      FLOAT,
    cash_pct                         FLOAT,
    portfolio_concentration_pct      FLOAT,
    weight_in_portfolio_pct          FLOAT,
    relative_strength_vs_spy_20d     FLOAT,
    sector_score                     FLOAT,
    sector_momentum_20d              FLOAT,
    sector_relative_strength_20d     FLOAT,
    label_target_hit                 INTEGER,
    label_stop_hit                   INTEGER,
    label_timeout                    INTEGER,
    outcome_return_pct               FLOAT,
    outcome_days                     INTEGER,
    closed_at                        TIMESTAMPTZ
);

-- FEATURE: ML - registry experimental de modelos versionados.
CREATE TABLE IF NOT EXISTS ml_model_registry (
    id                         BIGSERIAL PRIMARY KEY,
    model_type                 TEXT NOT NULL,
    version                    TEXT NOT NULL,
    trained_at                 TIMESTAMPTZ NOT NULL,
    train_samples              INTEGER,
    train_start                DATE,
    train_end                  DATE,
    val_samples                INTEGER,
    val_start                  DATE,
    val_end                    DATE,
    brier_score                FLOAT,
    roc_auc                    FLOAT,
    precision_at_top25pct      FLOAT,
    ev_mean                    FLOAT,
    ev_positive_rate           FLOAT,
    baseline_brier             FLOAT,
    beats_baseline             BOOLEAN,
    is_active                  BOOLEAN NOT NULL DEFAULT FALSE,
    is_promoted                BOOLEAN NOT NULL DEFAULT FALSE,
    artifact_path              TEXT,
    feature_names              TEXT,
    promotion_notes            TEXT,
    UNIQUE (model_type, version)
);

-- ── Índices ───────────────────────────────────────────────────────────────────
-- Migration para bases existentes:
-- las columnas deben existir antes de crear indices que dependen de ellas.
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS size_pct          FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS stop_loss_pct     FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS target_pct        FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS horizon_days      INTEGER;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS rr_ratio          FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS decision_date     DATE GENERATED ALWAYS AS ((decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date) STORED;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS decision_type     TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS signal_strength   TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS stop_loss_price   FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS target_price      FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS exit_scope        TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS exit_reason_rule  TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS stop_policy       TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS stop_source       TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS trailing_active   BOOLEAN DEFAULT FALSE;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS was_stopped       BOOLEAN;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS exit_reason       TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS closed_at         TIMESTAMPTZ;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS close_price       FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS source            TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS status                 TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS block_reason           TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS theoretical_amount_ars FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS executed_amount_ars    FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS current_weight         FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS target_weight          FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS delta_weight           FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS is_executable          BOOLEAN;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS was_blocked            BOOLEAN;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS outcome_basis          TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS outcome_basis_ratio    FLOAT;

UPDATE decision_log
SET decision_type = CASE
    WHEN decision = 'BUY'  THEN 'BUY'
    WHEN decision = 'SELL' THEN 'SELL_FULL'
    WHEN decision = 'HOLD' THEN 'HOLD'
    ELSE decision
END
WHERE decision_type IS NULL;

-- Antes de imponer unicidad diaria, conservar solo la decision mas reciente.
WITH ranked_daily_decisions AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            PARTITION BY ticker, decision_date, decision
            ORDER BY decided_at DESC, id DESC
        ) AS rn
    FROM decision_log
)
DELETE FROM decision_log
WHERE id IN (
    SELECT id
    FROM ranked_daily_decisions
    WHERE rn > 1
);

CREATE INDEX IF NOT EXISTS idx_portfolio_snapshots_scraped_at
    ON portfolio_snapshots(scraped_at DESC);

CREATE INDEX IF NOT EXISTS idx_positions_ticker
    ON positions(ticker, scraped_at DESC);

CREATE INDEX IF NOT EXISTS idx_positions_snapshot_id
    ON positions(snapshot_id);

CREATE INDEX IF NOT EXISTS idx_market_prices_ticker
    ON market_prices(ticker, ts DESC);

CREATE INDEX IF NOT EXISTS idx_raw_snapshots_scraped_at
    ON raw_snapshots(scraped_at DESC);

CREATE INDEX IF NOT EXISTS idx_decision_log_ticker
    ON decision_log(ticker);

CREATE INDEX IF NOT EXISTS idx_decision_log_decided_at
    ON decision_log(decided_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_decision_log_unique_daily_action
    ON decision_log(ticker, decision_date, decision);

-- Índice para queries de performance (solo cerrados)
CREATE INDEX IF NOT EXISTS idx_decision_log_outcome
    ON decision_log(decided_at DESC)
    WHERE outcome_5d IS NOT NULL
      AND outcome_basis = 'canonical_cocos';

-- Índice para update_outcomes (pendientes)
CREATE INDEX IF NOT EXISTS idx_decision_log_pending
    ON decision_log(decided_at)
    WHERE outcome_5d IS NULL
      AND COALESCE(outcome_basis, '') <> 'legacy_external';

-- Índice para check_stop_activations (trades abiertos con stop definido)
CREATE INDEX IF NOT EXISTS idx_decision_log_stops
    ON decision_log(decision, stop_loss_price, outcome_5d)
    WHERE stop_loss_price IS NOT NULL AND outcome_5d IS NULL;

CREATE INDEX IF NOT EXISTS idx_broker_fills_executed_at
    ON broker_fills(executed_at DESC);

CREATE INDEX IF NOT EXISTS idx_broker_fills_ticker_side
    ON broker_fills(ticker, side, executed_at DESC);

CREATE INDEX IF NOT EXISTS idx_broker_fills_decision_log_id
    ON broker_fills(decision_log_id);

-- ── Migration para bases existentes ───────────────────────────────────────────
-- Si la tabla decision_log ya existe sin las columnas nuevas, agregar:
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS size_pct          FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS stop_loss_pct     FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS target_pct        FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS horizon_days      INTEGER;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS rr_ratio          FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS decision_date     DATE GENERATED ALWAYS AS ((decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date) STORED;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS decision_type     TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS signal_strength   TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS stop_loss_price   FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS target_price      FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS exit_scope        TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS exit_reason_rule  TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS stop_policy       TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS stop_source       TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS trailing_active   BOOLEAN DEFAULT FALSE;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS was_stopped       BOOLEAN;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS exit_reason       TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS closed_at         TIMESTAMPTZ;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS close_price       FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS source            TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS status                 TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS block_reason           TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS theoretical_amount_ars FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS executed_amount_ars    FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS current_weight         FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS target_weight          FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS delta_weight           FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS is_executable          BOOLEAN;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS was_blocked            BOOLEAN;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS outcome_basis          TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS outcome_basis_ratio    FLOAT;

-- Rellenar decision_type para filas legacy
UPDATE decision_log
SET decision_type = CASE
    WHEN decision = 'BUY'  THEN 'BUY'
    WHEN decision = 'SELL' THEN 'SELL_FULL'
    WHEN decision = 'HOLD' THEN 'HOLD'
    ELSE decision
END
WHERE decision_type IS NULL;

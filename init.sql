-- init.sql — Schema completo de Cocos Copilot
-- Idempotente: seguro de correr múltiples veces (IF NOT EXISTS en todo)

CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

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

SELECT create_hypertable('positions', 'scraped_at', if_not_exists => TRUE);

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

SELECT create_hypertable('market_prices', 'ts', if_not_exists => TRUE);

-- ── raw_snapshots (hypertable) ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_snapshots (
    snapshot_id UUID        NOT NULL REFERENCES portfolio_snapshots(snapshot_id) ON DELETE CASCADE,
    scraped_at  TIMESTAMPTZ NOT NULL,
    payload     JSONB       NOT NULL,
    PRIMARY KEY (snapshot_id, scraped_at)
);

SELECT create_hypertable('raw_snapshots', 'scraped_at', if_not_exists => TRUE);

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
    source            TEXT          -- 'signal' | 'optimizer'
);

-- ── Índices ───────────────────────────────────────────────────────────────────
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

-- Índice para queries de performance (solo cerrados)
CREATE INDEX IF NOT EXISTS idx_decision_log_outcome
    ON decision_log(decided_at DESC)
    WHERE outcome_5d IS NOT NULL;

-- Índice para update_outcomes (pendientes)
CREATE INDEX IF NOT EXISTS idx_decision_log_pending
    ON decision_log(decided_at)
    WHERE outcome_5d IS NULL;

-- Índice para check_stop_activations (trades abiertos con stop definido)
CREATE INDEX IF NOT EXISTS idx_decision_log_stops
    ON decision_log(decision, stop_loss_price, outcome_5d)
    WHERE stop_loss_price IS NOT NULL AND outcome_5d IS NULL;

-- ── Migration para bases existentes ───────────────────────────────────────────
-- Si la tabla decision_log ya existe sin las columnas nuevas, agregar:
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS size_pct          FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS stop_loss_pct     FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS target_pct        FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS horizon_days      INTEGER;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS rr_ratio          FLOAT;
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

-- Rellenar decision_type para filas legacy
UPDATE decision_log
SET decision_type = CASE
    WHEN decision = 'BUY'  THEN 'BUY'
    WHEN decision = 'SELL' THEN 'SELL_FULL'
    WHEN decision = 'HOLD' THEN 'HOLD'
    ELSE decision
END
WHERE decision_type IS NULL;
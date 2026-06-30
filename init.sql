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
    owner_chat_id    BIGINT,
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
    chat_id                      BIGINT PRIMARY KEY,
    telegram_username            TEXT,
    display_name                 TEXT,
    cocos_user                   TEXT, -- legacy plaintext column; do not use for new writes
    cocos_pass                   TEXT, -- legacy plaintext column; do not use for new writes
    cocos_user_ciphertext        TEXT,
    cocos_pass_ciphertext        TEXT,
    credentials_key_version      INTEGER NOT NULL DEFAULT 1,
    credentials_last_verified_at TIMESTAMPTZ,
    mfa_timeout                  INTEGER NOT NULL DEFAULT 120,
    is_active                    BOOLEAN NOT NULL DEFAULT TRUE,
    created_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── decision_log ──────────────────────────────────────────────────────────────
-- Tabla central de decisiones, trades y lifecycle.
-- Columnas base + columnas trade_lifecycle agregadas de forma additive.
CREATE TABLE IF NOT EXISTS decision_log (
    id                BIGSERIAL    PRIMARY KEY,
    owner_chat_id     BIGINT REFERENCES bot_users(chat_id) ON DELETE CASCADE,
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
    was_blocked            BOOLEAN,

    -- Alcance auditable. Evita mezclar consultas exploratorias, radar,
    -- planes formales y ejecuciones reales en una misma metrica.
    run_id                 UUID,
    run_intent             TEXT,
    decision_stage         TEXT,
    metric_scope           TEXT,
    is_primary_metric      BOOLEAN NOT NULL DEFAULT FALSE,
    superseded_by_id       BIGINT REFERENCES decision_log(id) ON DELETE SET NULL
);

-- ── broker_fills ──────────────────────────────────────────────────────────────
-- Fills reales confirmados por broker. Hoy entran por import manual; la tabla
-- queda lista para una fuente automática futura si aparece una API confiable.
CREATE TABLE IF NOT EXISTS broker_fills (
    id               BIGSERIAL     PRIMARY KEY,
    source           TEXT          NOT NULL DEFAULT 'manual_import',
    external_fill_id TEXT          NOT NULL,
    executed_at      TIMESTAMPTZ   NOT NULL,
    executed_at_precision TEXT     NOT NULL DEFAULT 'unknown',
    executed_at_source    TEXT     NOT NULL DEFAULT 'unknown',
    ticker           TEXT          NOT NULL,
    side             TEXT          NOT NULL CHECK (side IN ('BUY', 'SELL')),
    quantity         NUMERIC(20,8) NOT NULL,
    avg_fill_price   NUMERIC(20,4) NOT NULL,
    gross_amount_ars NUMERIC(20,4),
    fees_ars         NUMERIC(20,4),
    raw_payload      JSONB,
    owner_chat_id    BIGINT REFERENCES bot_users(chat_id) ON DELETE CASCADE,
    decision_log_id  BIGINT REFERENCES decision_log(id) ON DELETE SET NULL,
    reconciled_at    TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source, external_fill_id)
);

-- Movimientos de Actividad/Instrumentos de Cocos. Auditan las ultimas acciones
-- del portfolio y tambien pueden alimentar broker_fills cuando traen precio/cantidad.
CREATE TABLE IF NOT EXISTS broker_movements (
    id                   BIGSERIAL PRIMARY KEY,
    source               TEXT NOT NULL DEFAULT 'cocos_movements',
    external_movement_id TEXT NOT NULL,
    executed_at          TIMESTAMPTZ NOT NULL,
    executed_at_precision TEXT NOT NULL DEFAULT 'date_only',
    executed_at_source    TEXT NOT NULL DEFAULT 'cocos_movements.execution_date',
    movement_type        TEXT NOT NULL,
    currency             TEXT NOT NULL DEFAULT 'ARS',
    amount               NUMERIC(20,4),
    quantity             NUMERIC(20,8),
    price                NUMERIC(20,4),
    ticker               TEXT,
    instrument_type      TEXT,
    settlement_date      DATE,
    description          TEXT,
    detail               TEXT,
    label                TEXT,
    balance              NUMERIC(20,4),
    raw_payload          JSONB,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source, external_movement_id)
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

-- Shadow thesis v1: pronosticos independientes y no ejecutables.
-- Se mantienen fuera de decision_log para no contaminar metricas operativas.
CREATE TABLE IF NOT EXISTS shadow_thesis_runs (
    run_id          UUID PRIMARY KEY,
    owner_chat_id   BIGINT NOT NULL DEFAULT 0,
    captured_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    as_of_ts        TIMESTAMPTZ NOT NULL,
    model_version   TEXT NOT NULL,
    schema_version  INTEGER NOT NULL,
    universe_count  INTEGER NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'COMPLETE',
    metadata        JSONB,
    UNIQUE (owner_chat_id, as_of_ts, model_version)
);

CREATE TABLE IF NOT EXISTS shadow_thesis_forecasts (
    id                  BIGSERIAL PRIMARY KEY,
    run_id              UUID NOT NULL REFERENCES shadow_thesis_runs(run_id) ON DELETE CASCADE,
    owner_chat_id       BIGINT NOT NULL DEFAULT 0,
    captured_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    as_of_ts            TIMESTAMPTZ NOT NULL,
    ticker              TEXT NOT NULL,
    universe_role       TEXT NOT NULL CHECK (universe_role IN ('POSITION', 'CANDIDATE')),
    horizon_sessions    INTEGER NOT NULL CHECK (horizon_sessions IN (5, 20, 40)),
    model_version       TEXT NOT NULL,
    schema_version      INTEGER NOT NULL,
    price_basis         TEXT NOT NULL DEFAULT 'canonical_cocos',
    reference_price     FLOAT NOT NULL CHECK (reference_price > 0),
    expected_return     FLOAT NOT NULL,
    probability_up      FLOAT NOT NULL CHECK (probability_up >= 0 AND probability_up <= 1),
    lower_return        FLOAT NOT NULL,
    upper_return        FLOAT NOT NULL,
    uncertainty         FLOAT NOT NULL CHECK (uncertainty >= 0),
    thesis_action       TEXT NOT NULL,
    thesis_confidence   FLOAT NOT NULL CHECK (thesis_confidence >= 0 AND thesis_confidence <= 1),
    signal_strength     TEXT NOT NULL,
    input_sessions      INTEGER NOT NULL,
    feature_snapshot    JSONB NOT NULL,
    UNIQUE (owner_chat_id, ticker, horizon_sessions, as_of_ts, model_version)
);

CREATE TABLE IF NOT EXISTS shadow_thesis_outcomes (
    forecast_id         BIGINT PRIMARY KEY REFERENCES shadow_thesis_forecasts(id) ON DELETE CASCADE,
    target_session_ts   TIMESTAMPTZ NOT NULL,
    outcome_price       FLOAT NOT NULL CHECK (outcome_price > 0),
    realized_return     FLOAT NOT NULL,
    direction_correct   BOOLEAN NOT NULL,
    absolute_error      FLOAT NOT NULL CHECK (absolute_error >= 0),
    squared_error       FLOAT NOT NULL CHECK (squared_error >= 0),
    matured_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_shadow_thesis_forecasts_latest
    ON shadow_thesis_forecasts(owner_chat_id, ticker, as_of_ts DESC);

CREATE INDEX IF NOT EXISTS idx_shadow_thesis_forecasts_pending
    ON shadow_thesis_forecasts(owner_chat_id, as_of_ts, horizon_sessions);

CREATE INDEX IF NOT EXISTS idx_shadow_thesis_outcomes_matured
    ON shadow_thesis_outcomes(matured_at DESC);

-- Parallel causal audit for shadow forecasts. This table is intentionally
-- independent from decision_log and does not alter forecasts or outcomes.
CREATE TABLE IF NOT EXISTS shadow_thesis_causal_analysis (
    id                  BIGSERIAL PRIMARY KEY,
    forecast_id         BIGINT REFERENCES shadow_thesis_forecasts(id) ON DELETE SET NULL,
    owner_chat_id       BIGINT NOT NULL DEFAULT 0,
    analyzed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    context_as_of       TIMESTAMPTZ NOT NULL,
    ticker              TEXT NOT NULL,
    projection_as_of    TIMESTAMPTZ NOT NULL,
    horizon_sessions    INTEGER NOT NULL CHECK (horizon_sessions > 0),
    expected_return     FLOAT NOT NULL CHECK (expected_return > -1),
    probability_up      FLOAT NOT NULL CHECK (probability_up >= 0 AND probability_up <= 1),
    macro_context       JSONB NOT NULL,
    macro_news          JSONB NOT NULL DEFAULT '[]'::jsonb,
    ticker_news         JSONB NOT NULL DEFAULT '[]'::jsonb,
    primary_driver      JSONB NOT NULL,
    durability          JSONB NOT NULL,
    reversal_risks      JSONB NOT NULL,
    conclusion          TEXT NOT NULL CHECK (conclusion IN ('FUNDADO', 'ESPECULATIVO', 'MIXTO')),
    conclusion_reason   TEXT NOT NULL,
    evidence_gaps       JSONB NOT NULL DEFAULT '[]'::jsonb,
    model               TEXT NOT NULL,
    prompt_version      TEXT NOT NULL,
    schema_version      INTEGER NOT NULL,
    input_fingerprint   TEXT NOT NULL,
    raw_response        JSONB NOT NULL,
    UNIQUE (owner_chat_id, input_fingerprint, model, prompt_version)
);

CREATE INDEX IF NOT EXISTS idx_shadow_causal_latest
    ON shadow_thesis_causal_analysis(owner_chat_id, ticker, analyzed_at DESC);

CREATE INDEX IF NOT EXISTS idx_shadow_causal_forecast
    ON shadow_thesis_causal_analysis(forecast_id)
    WHERE forecast_id IS NOT NULL;

-- Sentiment pipeline: raw news/events, LLM scoring and ticker aggregates.
-- Default use is contextual/auditable; it must not push buys by itself.
CREATE TABLE IF NOT EXISTS sentiment_raw (
    id                    BIGSERIAL PRIMARY KEY,
    fetched_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source                TEXT        NOT NULL,
    url                   TEXT        NOT NULL,
    url_hash              TEXT        NOT NULL,
    headline              TEXT        NOT NULL,
    body_snippet          TEXT,
    published_at          TIMESTAMPTZ,
    raw_payload           JSONB,
    score_status          TEXT        NOT NULL DEFAULT 'PENDING_SCORE',
    score_attempts        INTEGER     NOT NULL DEFAULT 0,
    last_score_attempt_at TIMESTAMPTZ,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (url_hash)
);

CREATE TABLE IF NOT EXISTS sentiment_scored (
    id           BIGSERIAL PRIMARY KEY,
    raw_id       BIGINT      NOT NULL REFERENCES sentiment_raw(id) ON DELETE CASCADE,
    scored_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    scorer       TEXT        NOT NULL DEFAULT 'ollama',
    model        TEXT,
    ticker       TEXT,
    asset_scope  TEXT        NOT NULL DEFAULT 'unknown',
    score        FLOAT,
    impact       TEXT,
    confidence   FLOAT,
    horizon      TEXT,
    event_type   TEXT,
    summary      TEXT,
    raw_response JSONB,
    status       TEXT        NOT NULL DEFAULT 'SCORED',
    error        TEXT,
    UNIQUE (raw_id, scorer, model)
);

CREATE TABLE IF NOT EXISTS sentiment_aggregated (
    id                BIGSERIAL PRIMARY KEY,
    bucket_ts         TIMESTAMPTZ NOT NULL,
    ticker            TEXT        NOT NULL,
    asset_scope       TEXT        NOT NULL DEFAULT 'ticker',
    score             FLOAT       NOT NULL DEFAULT 0.0,
    confidence        FLOAT       NOT NULL DEFAULT 0.0,
    event_count       INTEGER     NOT NULL DEFAULT 0,
    high_impact_count INTEGER     NOT NULL DEFAULT 0,
    top_summary       TEXT,
    sources           JSONB,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (bucket_ts, ticker, asset_scope)
);

-- Eventos/catalysts manuales cargados por el operador.
-- No scrapea fuentes externas: declara riesgos conocidos como earnings,
-- guidance, Fed, CPI, OPEC, etc. para contextualizar y bloquear entradas.
CREATE TABLE IF NOT EXISTS manual_market_events (
    id              BIGSERIAL PRIMARY KEY,
    event_date      DATE        NOT NULL,
    event_time_hint TEXT        NOT NULL DEFAULT 'unknown',
    ticker          TEXT,
    title           TEXT        NOT NULL,
    impact_scope    TEXT[]      NOT NULL DEFAULT ARRAY[]::TEXT[],
    related_tickers TEXT[]      NOT NULL DEFAULT ARRAY[]::TEXT[],
    severity        TEXT        NOT NULL DEFAULT 'medium',
    active_from     TIMESTAMPTZ NOT NULL,
    active_until    TIMESTAMPTZ NOT NULL,
    action_policy   TEXT        NOT NULL DEFAULT 'warn_only',
    notes           TEXT,
    is_active       BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (event_time_hint IN ('before_open', 'during_market', 'after_close', 'unknown')),
    CHECK (severity IN ('low', 'medium', 'high')),
    CHECK (action_policy IN ('warn_only', 'block_new_buys', 'no_action')),
    CHECK (active_until >= active_from)
);

CREATE INDEX IF NOT EXISTS idx_manual_market_events_active_window
    ON manual_market_events (is_active, active_from, active_until);

CREATE INDEX IF NOT EXISTS idx_manual_market_events_ticker
    ON manual_market_events (ticker);

CREATE TABLE IF NOT EXISTS intraday_preclose_alerts (
    id              BIGSERIAL PRIMARY KEY,
    alert_ts        TIMESTAMPTZ NOT NULL,
    business_date   DATE        NOT NULL,
    slot            TEXT        NOT NULL,
    ticker          TEXT        NOT NULL,
    alert_type      TEXT        NOT NULL,
    severity        TEXT        NOT NULL,
    current_price   FLOAT,
    reference_price FLOAT,
    change_pct      FLOAT,
    current_weight  FLOAT,
    reason          TEXT,
    evidence        JSONB       NOT NULL DEFAULT '{}'::jsonb,
    status          TEXT        NOT NULL DEFAULT 'OPEN',
    source          TEXT        NOT NULL DEFAULT 'preclose_v1',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (business_date, slot, ticker, alert_type)
);

CREATE INDEX IF NOT EXISTS idx_intraday_preclose_alerts_lookup
    ON intraday_preclose_alerts (business_date DESC, ticker, alert_type);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_extension
        WHERE extname = 'timescaledb'
    ) THEN
        -- Sentiment aggregates stay as a normal table to keep simple unique keys.
        NULL;
    END IF;
END
$$;

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
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS next_executable_at     TIMESTAMPTZ;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS next_executable_price  FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS executable_outcome_5d  FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS executable_outcome_10d FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS executable_outcome_20d FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS executable_was_correct BOOLEAN;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS owner_chat_id          BIGINT REFERENCES bot_users(chat_id) ON DELETE CASCADE;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS run_id                 UUID;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS run_intent             TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS decision_stage         TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS metric_scope           TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS is_primary_metric      BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS superseded_by_id       BIGINT REFERENCES decision_log(id) ON DELETE SET NULL;
ALTER TABLE portfolio_snapshots ADD COLUMN IF NOT EXISTS owner_chat_id   BIGINT REFERENCES bot_users(chat_id) ON DELETE CASCADE;
ALTER TABLE bot_users ADD COLUMN IF NOT EXISTS telegram_username            TEXT;
ALTER TABLE bot_users ADD COLUMN IF NOT EXISTS display_name                 TEXT;
ALTER TABLE bot_users ADD COLUMN IF NOT EXISTS cocos_user_ciphertext        TEXT;
ALTER TABLE bot_users ADD COLUMN IF NOT EXISTS cocos_pass_ciphertext        TEXT;
ALTER TABLE bot_users ADD COLUMN IF NOT EXISTS credentials_key_version      INTEGER NOT NULL DEFAULT 1;
ALTER TABLE bot_users ADD COLUMN IF NOT EXISTS credentials_last_verified_at TIMESTAMPTZ;
ALTER TABLE bot_users ADD COLUMN IF NOT EXISTS is_active                    BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE bot_users ADD COLUMN IF NOT EXISTS created_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE broker_fills ADD COLUMN IF NOT EXISTS owner_chat_id           BIGINT REFERENCES bot_users(chat_id) ON DELETE CASCADE;
ALTER TABLE broker_fills ADD COLUMN IF NOT EXISTS executed_at_precision   TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE broker_fills ADD COLUMN IF NOT EXISTS executed_at_source      TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE broker_movements ADD COLUMN IF NOT EXISTS executed_at_precision TEXT NOT NULL DEFAULT 'date_only';
ALTER TABLE broker_movements ADD COLUMN IF NOT EXISTS executed_at_source    TEXT NOT NULL DEFAULT 'cocos_movements.execution_date';

UPDATE decision_log
SET decision_type = CASE
    WHEN decision = 'BUY'  THEN 'BUY'
    WHEN decision = 'SELL' THEN 'SELL_FULL'
    WHEN decision = 'HOLD' THEN 'HOLD'
    ELSE decision
END
WHERE decision_type IS NULL;

UPDATE decision_log
SET
    run_intent = COALESCE(run_intent, CASE
        WHEN COALESCE(source, layers->>'source', '') IN ('broker_movement', 'broker_fill') THEN 'broker_sync'
        WHEN COALESCE(source, layers->>'source', '') = 'execution_plan' THEN 'formal_plan'
        WHEN COALESCE(source, layers->>'source', '') = 'radar' THEN 'scheduled_context'
        WHEN COALESCE(source, layers->>'source', '') = 'optimizer' THEN 'exploratory'
        ELSE 'exploratory'
    END),
    decision_stage = COALESCE(decision_stage, CASE
        WHEN COALESCE(status, '') IN ('EXECUTED', 'EXECUTED_MANUAL') THEN 'executed'
        WHEN COALESCE(status, '') = 'APPROVED'
             AND COALESCE(source, layers->>'source', '') = 'execution_plan'
             AND (
                (decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::time >= TIME '17:00'
                OR (decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::time < TIME '10:30'
             ) THEN 'pending_open'
        WHEN COALESCE(status, '') = 'APPROVED' THEN 'approved_decision'
        WHEN COALESCE(status, '') = 'BLOCKED' THEN 'blocked'
        WHEN COALESCE(source, layers->>'source', '') = 'radar' THEN 'idea'
        ELSE 'idea'
    END),
    metric_scope = COALESCE(metric_scope, CASE
        WHEN (
            COALESCE(source, layers->>'source', '') IN ('broker_movement', 'broker_fill')
            AND COALESCE(status, '') IN ('EXECUTED', 'EXECUTED_MANUAL')
        ) OR (
            COALESCE(source, layers->>'source', '') = 'execution_plan'
            AND COALESCE(status, '') IN ('EXECUTED', 'EXECUTED_MANUAL')
        ) THEN 'primary'
        WHEN COALESCE(source, layers->>'source', '') = 'execution_plan'
             AND COALESCE(status, '') = 'BLOCKED' THEN 'blocked_audit'
        WHEN COALESCE(source, layers->>'source', '') = 'execution_plan' THEN 'planner_audit'
        WHEN COALESCE(source, layers->>'source', '') = 'radar' THEN 'radar_audit'
        ELSE 'debug'
    END),
    is_primary_metric = CASE
        WHEN (
            COALESCE(source, layers->>'source', '') IN ('broker_movement', 'broker_fill')
            AND COALESCE(status, '') IN ('EXECUTED', 'EXECUTED_MANUAL')
        ) OR (
            COALESCE(source, layers->>'source', '') = 'execution_plan'
            AND COALESCE(status, '') IN ('EXECUTED', 'EXECUTED_MANUAL')
        ) THEN TRUE
        ELSE FALSE
    END
WHERE
    run_intent IS NULL
    OR decision_stage IS NULL
    OR metric_scope IS NULL
    OR is_primary_metric IS DISTINCT FROM CASE
        WHEN (
            COALESCE(source, layers->>'source', '') IN ('broker_movement', 'broker_fill')
            AND COALESCE(status, '') IN ('EXECUTED', 'EXECUTED_MANUAL')
        ) OR (
            COALESCE(source, layers->>'source', '') = 'execution_plan'
            AND COALESCE(status, '') IN ('EXECUTED', 'EXECUTED_MANUAL')
        ) THEN TRUE
        ELSE FALSE
    END;

-- Antes de imponer unicidad diaria, conservar solo la decision mas reciente.
WITH ranked_daily_decisions AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            PARTITION BY
                COALESCE(owner_chat_id, 0),
                ticker,
                decision_date,
                decision,
                COALESCE(source, 'sin_source'),
                COALESCE(decision_type, 'unknown')
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

CREATE INDEX IF NOT EXISTS idx_portfolio_snapshots_owner_scraped_at
    ON portfolio_snapshots(owner_chat_id, scraped_at DESC);

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

CREATE INDEX IF NOT EXISTS idx_decision_log_owner_decided_at
    ON decision_log(owner_chat_id, decided_at DESC);

CREATE INDEX IF NOT EXISTS idx_decision_log_decided_at
    ON decision_log(decided_at DESC);

CREATE INDEX IF NOT EXISTS idx_decision_log_metric_scope
    ON decision_log(metric_scope, decided_at DESC);

CREATE INDEX IF NOT EXISTS idx_decision_log_primary_metric
    ON decision_log(decided_at DESC)
    WHERE is_primary_metric = TRUE;

DROP INDEX IF EXISTS idx_decision_log_unique_daily_action;
CREATE INDEX IF NOT EXISTS idx_decision_log_daily_action_lookup
    ON decision_log(
        COALESCE(owner_chat_id, 0),
        ticker,
        decision_date,
        decision,
        COALESCE(source, 'sin_source'),
        COALESCE(decision_type, 'unknown')
    );

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

CREATE INDEX IF NOT EXISTS idx_broker_fills_owner_executed_at
    ON broker_fills(owner_chat_id, executed_at DESC);

CREATE INDEX IF NOT EXISTS idx_broker_movements_executed_at
    ON broker_movements(executed_at DESC);

CREATE INDEX IF NOT EXISTS idx_broker_movements_ticker_type
    ON broker_movements(ticker, movement_type, executed_at DESC);

CREATE INDEX IF NOT EXISTS idx_sentiment_raw_pending
    ON sentiment_raw(fetched_at DESC)
    WHERE score_status = 'PENDING_SCORE';

CREATE INDEX IF NOT EXISTS idx_sentiment_raw_source_time
    ON sentiment_raw(source, fetched_at DESC);

CREATE INDEX IF NOT EXISTS idx_sentiment_scored_ticker_time
    ON sentiment_scored(ticker, scored_at DESC)
    WHERE status = 'SCORED';

CREATE INDEX IF NOT EXISTS idx_sentiment_scored_scope_time
    ON sentiment_scored(asset_scope, scored_at DESC)
    WHERE status = 'SCORED';

CREATE INDEX IF NOT EXISTS idx_sentiment_aggregated_lookup
    ON sentiment_aggregated(ticker, asset_scope, bucket_ts DESC);

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
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS next_executable_at     TIMESTAMPTZ;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS next_executable_price  FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS executable_outcome_5d  FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS executable_outcome_10d FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS executable_outcome_20d FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS executable_was_correct BOOLEAN;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS owner_chat_id          BIGINT REFERENCES bot_users(chat_id) ON DELETE CASCADE;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS run_id                 UUID;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS run_intent             TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS decision_stage         TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS metric_scope           TEXT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS is_primary_metric      BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS superseded_by_id       BIGINT REFERENCES decision_log(id) ON DELETE SET NULL;
ALTER TABLE portfolio_snapshots ADD COLUMN IF NOT EXISTS owner_chat_id   BIGINT REFERENCES bot_users(chat_id) ON DELETE CASCADE;
ALTER TABLE bot_users ADD COLUMN IF NOT EXISTS telegram_username            TEXT;
ALTER TABLE bot_users ADD COLUMN IF NOT EXISTS display_name                 TEXT;
ALTER TABLE bot_users ADD COLUMN IF NOT EXISTS cocos_user_ciphertext        TEXT;
ALTER TABLE bot_users ADD COLUMN IF NOT EXISTS cocos_pass_ciphertext        TEXT;
ALTER TABLE bot_users ADD COLUMN IF NOT EXISTS credentials_key_version      INTEGER NOT NULL DEFAULT 1;
ALTER TABLE bot_users ADD COLUMN IF NOT EXISTS credentials_last_verified_at TIMESTAMPTZ;
ALTER TABLE bot_users ADD COLUMN IF NOT EXISTS is_active                    BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE bot_users ADD COLUMN IF NOT EXISTS created_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE broker_fills ADD COLUMN IF NOT EXISTS owner_chat_id           BIGINT REFERENCES bot_users(chat_id) ON DELETE CASCADE;
ALTER TABLE broker_fills ADD COLUMN IF NOT EXISTS executed_at_precision   TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE broker_fills ADD COLUMN IF NOT EXISTS executed_at_source      TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE broker_movements ADD COLUMN IF NOT EXISTS executed_at_precision TEXT NOT NULL DEFAULT 'date_only';
ALTER TABLE broker_movements ADD COLUMN IF NOT EXISTS executed_at_source    TEXT NOT NULL DEFAULT 'cocos_movements.execution_date';

-- Rellenar decision_type para filas legacy
UPDATE decision_log
SET decision_type = CASE
    WHEN decision = 'BUY'  THEN 'BUY'
    WHEN decision = 'SELL' THEN 'SELL_FULL'
    WHEN decision = 'HOLD' THEN 'HOLD'
    ELSE decision
END
WHERE decision_type IS NULL;

UPDATE decision_log
SET
    run_intent = COALESCE(run_intent, CASE
        WHEN COALESCE(source, layers->>'source', '') IN ('broker_movement', 'broker_fill') THEN 'broker_sync'
        WHEN COALESCE(source, layers->>'source', '') = 'execution_plan' THEN 'formal_plan'
        WHEN COALESCE(source, layers->>'source', '') = 'radar' THEN 'scheduled_context'
        WHEN COALESCE(source, layers->>'source', '') = 'optimizer' THEN 'exploratory'
        ELSE 'exploratory'
    END),
    decision_stage = COALESCE(decision_stage, CASE
        WHEN COALESCE(status, '') IN ('EXECUTED', 'EXECUTED_MANUAL') THEN 'executed'
        WHEN COALESCE(status, '') = 'APPROVED'
             AND COALESCE(source, layers->>'source', '') = 'execution_plan'
             AND (
                (decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::time >= TIME '17:00'
                OR (decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::time < TIME '10:30'
             ) THEN 'pending_open'
        WHEN COALESCE(status, '') = 'APPROVED' THEN 'approved_decision'
        WHEN COALESCE(status, '') = 'BLOCKED' THEN 'blocked'
        WHEN COALESCE(source, layers->>'source', '') = 'radar' THEN 'idea'
        ELSE 'idea'
    END),
    metric_scope = COALESCE(metric_scope, CASE
        WHEN (
            COALESCE(source, layers->>'source', '') IN ('broker_movement', 'broker_fill')
            AND COALESCE(status, '') IN ('EXECUTED', 'EXECUTED_MANUAL')
        ) OR (
            COALESCE(source, layers->>'source', '') = 'execution_plan'
            AND COALESCE(status, '') IN ('EXECUTED', 'EXECUTED_MANUAL')
        ) THEN 'primary'
        WHEN COALESCE(source, layers->>'source', '') = 'execution_plan'
             AND COALESCE(status, '') = 'BLOCKED' THEN 'blocked_audit'
        WHEN COALESCE(source, layers->>'source', '') = 'execution_plan' THEN 'planner_audit'
        WHEN COALESCE(source, layers->>'source', '') = 'radar' THEN 'radar_audit'
        ELSE 'debug'
    END),
    is_primary_metric = CASE
        WHEN (
            COALESCE(source, layers->>'source', '') IN ('broker_movement', 'broker_fill')
            AND COALESCE(status, '') IN ('EXECUTED', 'EXECUTED_MANUAL')
        ) OR (
            COALESCE(source, layers->>'source', '') = 'execution_plan'
            AND COALESCE(status, '') IN ('EXECUTED', 'EXECUTED_MANUAL')
        ) THEN TRUE
        ELSE FALSE
    END
WHERE
    run_intent IS NULL
    OR decision_stage IS NULL
    OR metric_scope IS NULL
    OR is_primary_metric IS DISTINCT FROM CASE
        WHEN (
            COALESCE(source, layers->>'source', '') IN ('broker_movement', 'broker_fill')
            AND COALESCE(status, '') IN ('EXECUTED', 'EXECUTED_MANUAL')
        ) OR (
            COALESCE(source, layers->>'source', '') = 'execution_plan'
            AND COALESCE(status, '') IN ('EXECUTED', 'EXECUTED_MANUAL')
        ) THEN TRUE
        ELSE FALSE
    END;

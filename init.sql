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

CREATE INDEX IF NOT EXISTS idx_market_candles_ticker_interval_ts
    ON market_candles(ticker, interval, ts DESC);

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

-- Reportes renderizados para respuestas Telegram rapidas y auditables.
-- No participa del scoring, optimizer ni decision_log.
CREATE TABLE IF NOT EXISTS telegram_report_artifacts (
    report_type          TEXT NOT NULL,
    owner_chat_id        BIGINT NOT NULL,
    input_fingerprint    TEXT NOT NULL,
    artifact_version     TEXT NOT NULL,
    report_text          TEXT NOT NULL,
    generated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    portfolio_snapshot_id UUID,
    portfolio_at         TIMESTAMPTZ,
    market_data_at       TIMESTAMPTZ,
    candle_data_at       TIMESTAMPTZ,
    metadata             JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (report_type, owner_chat_id)
);

CREATE INDEX IF NOT EXISTS idx_telegram_report_artifacts_generated_at
    ON telegram_report_artifacts(generated_at DESC);

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
    outcome_40d       FLOAT,
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

-- Plan operativo persistido. Es una capa aditiva de trazabilidad: decision_log
-- sigue siendo el ledger historico compatible y estas tablas no envian ordenes.
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

-- Vinculo derivado y auditable entre planes formales y movimientos reales.
-- No cambia decision_log ni implica ejecucion automatica. La tabla principal
-- contiene una fila por respuesta operativa deduplicada; las tablas puente
-- conservan todos los planes repetidos y movimientos que la componen.
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
    metadata                    JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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

CREATE TABLE IF NOT EXISTS plan_execution_attribution_movements (
    attribution_id             BIGINT NOT NULL REFERENCES plan_execution_attributions(id) ON DELETE CASCADE,
    broker_movement_id         BIGINT NOT NULL REFERENCES broker_movements(id) ON DELETE CASCADE,
    amount_ars                 NUMERIC(20,4) NOT NULL,
    PRIMARY KEY (attribution_id, broker_movement_id),
    UNIQUE (broker_movement_id)
);

CREATE INDEX IF NOT EXISTS idx_plan_execution_attributions_plan
    ON plan_execution_attributions(representative_decision_log_id);
CREATE INDEX IF NOT EXISTS idx_plan_execution_attributions_viability
    ON plan_execution_attributions(executed_at DESC)
    WHERE eligible_for_viability = TRUE;
CREATE INDEX IF NOT EXISTS idx_plan_execution_attributions_ticker
    ON plan_execution_attributions(ticker, side, executed_at DESC);
CREATE INDEX IF NOT EXISTS idx_plan_execution_attribution_plans_decision
    ON plan_execution_attribution_plans(decision_log_id);
CREATE INDEX IF NOT EXISTS idx_plan_execution_attribution_movements_attribution
    ON plan_execution_attribution_movements(attribution_id);

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

-- Shadow v3: post-hoc calibration of v2 forecasts. These tables only read
-- shadow evidence and remain disconnected from decision_log and execution.
CREATE TABLE IF NOT EXISTS shadow_calibration_runs (
    calibration_run_id UUID PRIMARY KEY,
    owner_chat_id BIGINT NOT NULL DEFAULT 0,
    source_run_id UUID NOT NULL REFERENCES shadow_thesis_runs(run_id) ON DELETE CASCADE,
    source_model_version TEXT NOT NULL,
    model_version TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    trained_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    train_cutoff TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'COMPLETE_SHADOW',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (owner_chat_id, source_run_id, model_version)
);

CREATE TABLE IF NOT EXISTS shadow_calibration_models (
    calibration_run_id UUID NOT NULL REFERENCES shadow_calibration_runs(calibration_run_id) ON DELETE CASCADE,
    horizon_sessions INTEGER NOT NULL CHECK (horizon_sessions IN (5, 20)),
    sample_count INTEGER NOT NULL,
    cohort_count INTEGER NOT NULL,
    train_start_ts TIMESTAMPTZ NOT NULL,
    train_end_ts TIMESTAMPTZ NOT NULL,
    parameters JSONB NOT NULL,
    fit_metrics JSONB NOT NULL,
    walk_forward_metrics JSONB NOT NULL,
    diagnostics JSONB NOT NULL,
    PRIMARY KEY (calibration_run_id, horizon_sessions)
);

CREATE TABLE IF NOT EXISTS shadow_calibrated_forecasts (
    id BIGSERIAL PRIMARY KEY,
    calibration_run_id UUID NOT NULL REFERENCES shadow_calibration_runs(calibration_run_id) ON DELETE CASCADE,
    source_forecast_id BIGINT NOT NULL REFERENCES shadow_thesis_forecasts(id) ON DELETE CASCADE,
    owner_chat_id BIGINT NOT NULL DEFAULT 0,
    ticker TEXT NOT NULL,
    as_of_ts TIMESTAMPTZ NOT NULL,
    horizon_sessions INTEGER NOT NULL CHECK (horizon_sessions IN (5, 20)),
    model_version TEXT NOT NULL,
    raw_expected_return FLOAT NOT NULL,
    raw_probability_up FLOAT NOT NULL CHECK (raw_probability_up >= 0 AND raw_probability_up <= 1),
    calibrated_expected_return FLOAT NOT NULL,
    calibrated_probability_up FLOAT NOT NULL CHECK (
        calibrated_probability_up >= 0 AND calibrated_probability_up <= 1
    ),
    calibrated_lower_return FLOAT NOT NULL,
    calibrated_upper_return FLOAT NOT NULL,
    calibration_status TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_forecast_id, model_version)
);

CREATE TABLE IF NOT EXISTS shadow_calibration_gate_state (
    owner_chat_id BIGINT NOT NULL DEFAULT 0,
    horizon_sessions INTEGER NOT NULL CHECK (horizon_sessions IN (5, 20)),
    current_gate TEXT NOT NULL,
    calibration_run_id UUID REFERENCES shadow_calibration_runs(calibration_run_id) ON DELETE SET NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (owner_chat_id, horizon_sessions)
);

CREATE TABLE IF NOT EXISTS shadow_calibration_gate_events (
    id BIGSERIAL PRIMARY KEY,
    owner_chat_id BIGINT NOT NULL DEFAULT 0,
    horizon_sessions INTEGER NOT NULL CHECK (horizon_sessions IN (5, 20)),
    previous_gate TEXT NOT NULL,
    new_gate TEXT NOT NULL,
    calibration_run_id UUID REFERENCES shadow_calibration_runs(calibration_run_id) ON DELETE SET NULL,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (calibration_run_id, horizon_sessions, previous_gate, new_gate)
);

CREATE INDEX IF NOT EXISTS idx_shadow_calibration_runs_latest
    ON shadow_calibration_runs(owner_chat_id, trained_at DESC);

CREATE INDEX IF NOT EXISTS idx_shadow_calibrated_forecasts_latest
    ON shadow_calibrated_forecasts(owner_chat_id, ticker, as_of_ts DESC);

CREATE INDEX IF NOT EXISTS idx_shadow_calibration_gate_events_latest
    ON shadow_calibration_gate_events(owner_chat_id, changed_at DESC);

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

-- Sentiment pipeline and later audit schemas remain outside causal shadow.
-- Learning shadow v1: counterfactual audit of blocked decisions.
-- Reads operational evidence but writes only to learning_shadow_*.
CREATE TABLE IF NOT EXISTS learning_shadow_runs (
    run_id                  UUID PRIMARY KEY,
    owner_chat_id           BIGINT NOT NULL DEFAULT 0,
    captured_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    lookback_days           INTEGER NOT NULL CHECK (lookback_days > 0),
    material_return_bps     INTEGER NOT NULL CHECK (material_return_bps >= 0),
    policy_version          TEXT NOT NULL,
    schema_version          INTEGER NOT NULL,
    decisions_seen          INTEGER NOT NULL DEFAULT 0,
    cases_upserted          INTEGER NOT NULL DEFAULT 0,
    status                  TEXT NOT NULL DEFAULT 'COMPLETE',
    metadata                JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS learning_shadow_cases (
    id                          BIGSERIAL PRIMARY KEY,
    owner_chat_id               BIGINT NOT NULL DEFAULT 0,
    decision_log_id             BIGINT NOT NULL REFERENCES decision_log(id) ON DELETE CASCADE,
    ticker                      TEXT NOT NULL,
    decision                    TEXT NOT NULL CHECK (decision IN ('BUY', 'SELL')),
    decided_at                  TIMESTAMPTZ NOT NULL,
    horizon_days                INTEGER NOT NULL CHECK (horizon_days IN (5, 10, 20, 40)),
    shadow_horizon_sessions     INTEGER NOT NULL CHECK (shadow_horizon_sessions IN (5, 20, 40)),
    block_reason                TEXT,
    outcome_basis               TEXT,
    outcome_source              TEXT,
    directional_outcome         FLOAT,
    material_return_bps         INTEGER NOT NULL,
    classification              TEXT NOT NULL CHECK (classification IN (
        'PENDING', 'MISSING_OUTCOME', 'EXCLUDED_BASIS', 'EXCLUDED_OUTLIER',
        'POTENTIAL_FALSE_NEGATIVE', 'POSITIVE_BELOW_THRESHOLD',
        'NON_POSITIVE_COUNTERFACTUAL'
    )),
    shadow_forecast_id          BIGINT REFERENCES shadow_thesis_forecasts(id) ON DELETE SET NULL,
    shadow_as_of_ts             TIMESTAMPTZ,
    shadow_expected_return      FLOAT,
    shadow_probability_up       FLOAT,
    shadow_action               TEXT,
    shadow_direction_correct    BOOLEAN,
    shadow_supports_direction   BOOLEAN,
    policy_version              TEXT NOT NULL,
    schema_version              INTEGER NOT NULL,
    first_observed_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_evaluated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_run_id                 UUID NOT NULL REFERENCES learning_shadow_runs(run_id) ON DELETE RESTRICT,
    metadata                    JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (owner_chat_id, decision_log_id, horizon_days)
);

CREATE TABLE IF NOT EXISTS learning_shadow_metric_snapshots (
    id                              BIGSERIAL PRIMARY KEY,
    run_id                          UUID NOT NULL REFERENCES learning_shadow_runs(run_id) ON DELETE CASCADE,
    owner_chat_id                   BIGINT NOT NULL DEFAULT 0,
    captured_at                     TIMESTAMPTZ NOT NULL,
    snapshot_date                   DATE NOT NULL,
    lookback_days                   INTEGER NOT NULL,
    horizon_days                    INTEGER NOT NULL CHECK (horizon_days IN (5, 10, 20, 40)),
    shadow_horizon_sessions         INTEGER NOT NULL CHECK (shadow_horizon_sessions IN (5, 20, 40)),
    material_return_bps             INTEGER NOT NULL,
    policy_version                  TEXT NOT NULL,
    total_cases                     INTEGER NOT NULL,
    matured_cases                   INTEGER NOT NULL,
    potential_false_negatives       INTEGER NOT NULL,
    positive_below_threshold        INTEGER NOT NULL,
    non_positive_cases              INTEGER NOT NULL,
    pending_cases                   INTEGER NOT NULL,
    missing_outcome_cases           INTEGER NOT NULL,
    excluded_cases                  INTEGER NOT NULL,
    shadow_linked_cases             INTEGER NOT NULL,
    shadow_aligned_cases            INTEGER NOT NULL,
    shadow_coverage_rate            FLOAT,
    shadow_alignment_rate           FLOAT,
    potential_false_negative_rate   FLOAT,
    mean_directional_outcome        FLOAT,
    mean_false_negative_outcome     FLOAT,
    UNIQUE (
        owner_chat_id, snapshot_date, lookback_days, horizon_days,
        material_return_bps, policy_version
    )
);

CREATE INDEX IF NOT EXISTS idx_learning_shadow_cases_recent
    ON learning_shadow_cases(owner_chat_id, decided_at DESC, horizon_days);

CREATE INDEX IF NOT EXISTS idx_learning_shadow_cases_classification
    ON learning_shadow_cases(owner_chat_id, classification, horizon_days, decided_at DESC);

CREATE INDEX IF NOT EXISTS idx_learning_shadow_snapshots_trend
    ON learning_shadow_metric_snapshots(owner_chat_id, horizon_days, captured_at DESC);

-- Learning shadow v2: additive evidence fields and population-separated metrics.
-- The v1 snapshot table remains intact as historical evidence.
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
    id                              BIGSERIAL PRIMARY KEY,
    run_id                          UUID NOT NULL REFERENCES learning_shadow_runs(run_id) ON DELETE CASCADE,
    owner_chat_id                   BIGINT NOT NULL DEFAULT 0,
    captured_at                     TIMESTAMPTZ NOT NULL,
    snapshot_date                   DATE NOT NULL,
    lookback_days                   INTEGER NOT NULL,
    horizon_days                    INTEGER NOT NULL CHECK (horizon_days IN (5, 10, 20, 40)),
    shadow_horizon_sessions         INTEGER NOT NULL CHECK (shadow_horizon_sessions IN (5, 20, 40)),
    material_return_bps             INTEGER NOT NULL,
    policy_version                  TEXT NOT NULL,
    case_population                 TEXT NOT NULL,
    total_cases                     INTEGER NOT NULL,
    matured_cases                   INTEGER NOT NULL,
    potential_false_negatives       INTEGER NOT NULL,
    potential_false_negative_rate   FLOAT,
    clean_missed_opportunities      INTEGER NOT NULL,
    clean_miss_rate                 FLOAT,
    metrics                         JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (
        owner_chat_id, snapshot_date, lookback_days, horizon_days,
        material_return_bps, policy_version, case_population
    )
);

CREATE TABLE IF NOT EXISTS learning_shadow_cohort_metrics (
    id                              BIGSERIAL PRIMARY KEY,
    owner_chat_id                   BIGINT NOT NULL DEFAULT 0,
    cohort_date                     DATE NOT NULL,
    horizon_days                    INTEGER NOT NULL CHECK (horizon_days IN (5, 10, 20, 40)),
    material_return_bps             INTEGER NOT NULL,
    policy_version                  TEXT NOT NULL,
    case_population                 TEXT NOT NULL,
    total_cases                     INTEGER NOT NULL,
    matured_cases                   INTEGER NOT NULL,
    potential_false_negatives       INTEGER NOT NULL,
    potential_false_negative_rate   FLOAT,
    clean_missed_opportunities      INTEGER NOT NULL,
    clean_miss_rate                 FLOAT,
    metrics                         JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_run_id                     UUID NOT NULL REFERENCES learning_shadow_runs(run_id) ON DELETE CASCADE,
    last_evaluated_at               TIMESTAMPTZ NOT NULL,
    UNIQUE (
        owner_chat_id, cohort_date, horizon_days, material_return_bps,
        policy_version, case_population
    )
);

CREATE TABLE IF NOT EXISTS learning_shadow_policy_versions (
    policy_version      TEXT PRIMARY KEY,
    schema_version      INTEGER NOT NULL,
    status              TEXT NOT NULL DEFAULT 'SHADOW',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    config              JSONB NOT NULL DEFAULT '{}'::jsonb,
    affects_analysis    BOOLEAN NOT NULL DEFAULT FALSE,
    affects_execution   BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS learning_shadow_rule_candidates (
    id                          BIGSERIAL PRIMARY KEY,
    owner_chat_id               BIGINT NOT NULL DEFAULT 0,
    policy_version              TEXT NOT NULL,
    block_category              TEXT NOT NULL,
    horizon_days                INTEGER NOT NULL,
    candidate_type              TEXT NOT NULL,
    proposed_rule               JSONB NOT NULL,
    rationale                   TEXT NOT NULL,
    sample_size                 INTEGER NOT NULL,
    clean_miss_count            INTEGER NOT NULL,
    clean_miss_rate             FLOAT NOT NULL,
    risky_win_count             INTEGER NOT NULL,
    market_driven_count         INTEGER NOT NULL,
    mean_alpha_vs_benchmark     FLOAT,
    evidence_start              TIMESTAMPTZ NOT NULL,
    evidence_end                TIMESTAMPTZ NOT NULL,
    status                      TEXT NOT NULL DEFAULT 'PROPOSED' CHECK (status IN (
        'PROPOSED', 'APPROVED_FOR_SHADOW', 'REJECTED', 'ARCHIVED'
    )),
    reviewed_at                 TIMESTAMPTZ,
    reviewed_by                 TEXT,
    review_note                 TEXT,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_run_id                 UUID NOT NULL REFERENCES learning_shadow_runs(run_id) ON DELETE CASCADE,
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

-- Corporate actions: canonical event, instrument-level transform and audit ledger.
-- Raw candles, snapshots and decision prices remain immutable.
CREATE TABLE IF NOT EXISTS corporate_events (
    id                  BIGSERIAL PRIMARY KEY,
    event_key           TEXT NOT NULL UNIQUE,
    issuer_id           TEXT NOT NULL,
    event_type          TEXT NOT NULL,
    lifecycle_status    TEXT NOT NULL,
    announced_at        TIMESTAMPTZ,
    effective_at        TIMESTAMPTZ NOT NULL,
    expires_at          TIMESTAMPTZ,
    source_name         TEXT,
    source_url          TEXT,
    source_published_at TIMESTAMPTZ,
    source_hash         TEXT,
    ingestion_method    TEXT NOT NULL,
    evidence_level      TEXT NOT NULL,
    detector_score      FLOAT,
    detector_version    TEXT,
    raw_payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
    supersedes_event_id BIGINT REFERENCES corporate_events(id) ON DELETE SET NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (event_type IN (
        'SPLIT', 'REVERSE_SPLIT', 'DEPOSITARY_RATIO_CHANGE',
        'DIVIDEND', 'SPIN_OFF', 'TICKER_CHANGE', 'OTHER'
    )),
    CHECK (lifecycle_status IN (
        'SUSPECTED', 'ANNOUNCED', 'CONFIRMED', 'EFFECTIVE',
        'CANCELLED', 'DISMISSED', 'SUPERSEDED'
    )),
    CHECK (ingestion_method IN ('MANUAL', 'AUTOMATED', 'DETECTOR')),
    CHECK (evidence_level IN (
        'PRIMARY_OFFICIAL', 'STRUCTURED_SECONDARY',
        'CORROBORATED', 'HEURISTIC_ONLY'
    )),
    CHECK (detector_score IS NULL OR (detector_score >= 0 AND detector_score <= 1))
);

CREATE TABLE IF NOT EXISTS corporate_event_instrument_effects (
    id                      BIGSERIAL PRIMARY KEY,
    event_id                BIGINT NOT NULL REFERENCES corporate_events(id) ON DELETE CASCADE,
    instrument_id           TEXT NOT NULL,
    ticker                  TEXT NOT NULL,
    venue                   TEXT,
    asset_type              TEXT,
    currency                TEXT,
    quantity_factor         NUMERIC(24,12) NOT NULL,
    price_factor            NUMERIC(24,12) NOT NULL,
    cost_basis_factor       NUMERIC(24,12) NOT NULL,
    depositary_ratio_before TEXT,
    depositary_ratio_after  TEXT,
    metadata                JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (event_id, instrument_id),
    CHECK (quantity_factor > 0),
    CHECK (price_factor > 0),
    CHECK (cost_basis_factor > 0)
);

CREATE TABLE IF NOT EXISTS price_quality_flags (
    id                       BIGSERIAL PRIMARY KEY,
    event_id                 BIGINT REFERENCES corporate_events(id) ON DELETE SET NULL,
    instrument_effect_id     BIGINT REFERENCES corporate_event_instrument_effects(id) ON DELETE SET NULL,
    ticker                   TEXT NOT NULL,
    observed_at              TIMESTAMPTZ NOT NULL,
    expires_at               TIMESTAMPTZ,
    flag_type                TEXT NOT NULL,
    resolution_status        TEXT NOT NULL DEFAULT 'OPEN',
    observed_reference_price FLOAT,
    observed_current_price   FLOAT,
    observed_return          FLOAT,
    expected_price_factor    FLOAT,
    observed_quantity_factor FLOAT,
    quantity_factor          FLOAT,
    evidence_level           TEXT NOT NULL,
    detector_score           FLOAT,
    detector_version         TEXT NOT NULL,
    action_taken             TEXT NOT NULL,
    reason                   TEXT NOT NULL,
    evidence                 JSONB NOT NULL DEFAULT '{}'::jsonb,
    idempotency_key          TEXT NOT NULL UNIQUE,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (resolution_status IN ('OPEN', 'CONFIRMED', 'DISMISSED', 'EXPIRED')),
    CHECK (flag_type IN ('PRICE_NOT_COMPARABLE', 'DATA_QUALITY_BLOCK')),
    CHECK (detector_score IS NULL OR (detector_score >= 0 AND detector_score <= 1))
);

CREATE TABLE IF NOT EXISTS corporate_event_applications (
    id                   BIGSERIAL PRIMARY KEY,
    event_id             BIGINT NOT NULL REFERENCES corporate_events(id) ON DELETE CASCADE,
    instrument_effect_id BIGINT NOT NULL REFERENCES corporate_event_instrument_effects(id) ON DELETE CASCADE,
    owner_chat_id        BIGINT,
    component            TEXT NOT NULL,
    application_status   TEXT NOT NULL,
    adjustment_version   TEXT NOT NULL,
    idempotency_key      TEXT NOT NULL UNIQUE,
    before_state         JSONB NOT NULL DEFAULT '{}'::jsonb,
    after_state          JSONB NOT NULL DEFAULT '{}'::jsonb,
    invariant_checks     JSONB NOT NULL DEFAULT '{}'::jsonb,
    error                TEXT,
    applied_at           TIMESTAMPTZ,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (application_status IN (
        'PENDING', 'APPLYING', 'APPLIED', 'ALREADY_ADJUSTED',
        'FAILED', 'ROLLED_BACK'
    ))
);

CREATE INDEX IF NOT EXISTS idx_corporate_events_effective
    ON corporate_events (effective_at DESC, lifecycle_status);

CREATE INDEX IF NOT EXISTS idx_corporate_effects_ticker
    ON corporate_event_instrument_effects (ticker, is_active);

CREATE INDEX IF NOT EXISTS idx_price_quality_flags_active
    ON price_quality_flags (ticker, resolution_status, expires_at);

CREATE INDEX IF NOT EXISTS idx_corporate_applications_event
    ON corporate_event_applications (event_id, instrument_effect_id, component);

-- Issuer-event discovery: shadow observations from SEC, FMP, Finnhub and CNV.
-- These records are evidence only; they do not alter corporate effects or plans.
CREATE TABLE IF NOT EXISTS issuer_registry (
    issuer_id         TEXT PRIMARY KEY,
    issuer_name       TEXT NOT NULL,
    source_market     TEXT NOT NULL,
    primary_symbol    TEXT,
    sec_cik           TEXT,
    cnv_entity_name   TEXT,
    metadata          JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (source_market IN ('US', 'AR', 'OTHER'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_issuer_registry_sec_cik
    ON issuer_registry (sec_cik)
    WHERE sec_cik IS NOT NULL;

CREATE TABLE IF NOT EXISTS issuer_instruments (
    id                BIGSERIAL PRIMARY KEY,
    issuer_id         TEXT NOT NULL REFERENCES issuer_registry(issuer_id) ON DELETE CASCADE,
    ticker            TEXT NOT NULL,
    instrument_id     TEXT NOT NULL UNIQUE,
    venue             TEXT,
    asset_type        TEXT,
    currency          TEXT,
    source_ticker     TEXT,
    metadata          JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (ticker, venue, currency)
);

CREATE INDEX IF NOT EXISTS idx_issuer_instruments_active
    ON issuer_instruments (issuer_id, is_active, ticker);

CREATE TABLE IF NOT EXISTS issuer_event_observations (
    id                  BIGSERIAL PRIMARY KEY,
    observation_key     TEXT NOT NULL UNIQUE,
    issuer_id           TEXT NOT NULL REFERENCES issuer_registry(issuer_id) ON DELETE CASCADE,
    ticker              TEXT,
    source              TEXT NOT NULL,
    event_type          TEXT NOT NULL,
    lifecycle_status    TEXT NOT NULL,
    event_date          DATE,
    fiscal_year         SMALLINT,
    fiscal_quarter      SMALLINT,
    fiscal_period_end   DATE,
    event_time_hint     TEXT NOT NULL DEFAULT 'unknown',
    source_published_at TIMESTAMPTZ,
    source_url          TEXT NOT NULL,
    source_hash         TEXT NOT NULL,
    confidence          FLOAT NOT NULL,
    actionable          BOOLEAN NOT NULL DEFAULT FALSE,
    title               TEXT NOT NULL,
    raw_payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (source IN ('SEC', 'FMP', 'FINNHUB', 'CNV', 'YAHOO')),
    CHECK (event_type IN (
        'FILING', 'EARNINGS', 'SPLIT', 'REVERSE_SPLIT',
        'DEPOSITARY_RATIO_CHANGE', 'DIVIDEND', 'MERGER',
        'DELISTING', 'RELEVANT_FACT'
    )),
    CHECK (lifecycle_status IN ('DISCOVERED', 'ANNOUNCED', 'CONFIRMED', 'CANCELLED', 'DISMISSED')),
    CHECK (event_time_hint IN ('before_open', 'during_market', 'after_close', 'unknown')),
    CHECK (confidence >= 0 AND confidence <= 1)
);

CREATE INDEX IF NOT EXISTS idx_issuer_event_observations_lookup
    ON issuer_event_observations (issuer_id, event_date DESC, source, event_type);

CREATE INDEX IF NOT EXISTS idx_issuer_event_observations_ticker
    ON issuer_event_observations (ticker, created_at DESC);

ALTER TABLE issuer_event_observations
    ADD COLUMN IF NOT EXISTS fiscal_year SMALLINT,
    ADD COLUMN IF NOT EXISTS fiscal_quarter SMALLINT,
    ADD COLUMN IF NOT EXISTS fiscal_period_end DATE;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'issuer_event_observations'::regclass
          AND conname = 'issuer_event_observations_fiscal_year_check'
    ) THEN
        ALTER TABLE issuer_event_observations
            ADD CONSTRAINT issuer_event_observations_fiscal_year_check
            CHECK (fiscal_year IS NULL OR fiscal_year BETWEEN 1900 AND 2200);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'issuer_event_observations'::regclass
          AND conname = 'issuer_event_observations_fiscal_quarter_check'
    ) THEN
        ALTER TABLE issuer_event_observations
            ADD CONSTRAINT issuer_event_observations_fiscal_quarter_check
            CHECK (fiscal_quarter IS NULL OR fiscal_quarter BETWEEN 1 AND 4);
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'issuer_event_observations'::regclass
          AND conname = 'issuer_event_observations_source_check'
          AND pg_get_constraintdef(oid) NOT LIKE '%YAHOO%'
    ) THEN
        ALTER TABLE issuer_event_observations
            DROP CONSTRAINT issuer_event_observations_source_check;
        ALTER TABLE issuer_event_observations
            ADD CONSTRAINT issuer_event_observations_source_check
            CHECK (source IN ('SEC', 'FMP', 'FINNHUB', 'CNV', 'YAHOO'));
    END IF;
END $$;

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
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS outcome_40d            FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS next_executable_at     TIMESTAMPTZ;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS next_executable_price  FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS executable_outcome_5d  FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS executable_outcome_10d FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS executable_outcome_20d FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS executable_outcome_40d FLOAT;
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

CREATE INDEX IF NOT EXISTS idx_execution_plans_owner_created_at
    ON execution_plans(owner_chat_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_execution_plans_run_id
    ON execution_plans(run_id)
    WHERE run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_order_intents_decision_log_id
    ON order_intents(decision_log_id)
    WHERE decision_log_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_order_intents_ticker_created_at
    ON order_intents(ticker, created_at DESC);

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
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS outcome_40d            FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS next_executable_at     TIMESTAMPTZ;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS next_executable_price  FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS executable_outcome_5d  FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS executable_outcome_10d FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS executable_outcome_20d FLOAT;
ALTER TABLE decision_log ADD COLUMN IF NOT EXISTS executable_outcome_40d FLOAT;
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

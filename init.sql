-- init.sql — Schema inicial para TimescaleDB
-- Este archivo se ejecuta automáticamente cuando se crea el contenedor por primera vez.
-- Las tablas también se crean desde Python (db.py init_schema) de forma idempotente.

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

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

CREATE TABLE IF NOT EXISTS positions (
    id                  BIGSERIAL   PRIMARY KEY,
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
    sector              TEXT
);

SELECT create_hypertable('positions', 'scraped_at', if_not_exists => TRUE);

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

CREATE TABLE IF NOT EXISTS raw_snapshots (
    snapshot_id UUID        NOT NULL REFERENCES portfolio_snapshots(snapshot_id) ON DELETE CASCADE,
    scraped_at  TIMESTAMPTZ NOT NULL,
    payload     JSONB       NOT NULL
);

SELECT create_hypertable('raw_snapshots', 'scraped_at', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS bot_users (
    chat_id      BIGINT PRIMARY KEY,
    cocos_user   TEXT,
    cocos_pass   TEXT,
    mfa_timeout  INTEGER NOT NULL DEFAULT 120,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS decision_log (
    id                BIGSERIAL    PRIMARY KEY,
    decided_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    ticker            TEXT         NOT NULL,
    decision          TEXT         NOT NULL,
    final_score       FLOAT        NOT NULL,
    confidence        FLOAT        NOT NULL,
    layers            JSONB,
    price_at_decision FLOAT,
    vix_at_decision   FLOAT,
    regime            TEXT,
    outcome_5d        FLOAT,
    outcome_10d       FLOAT,
    outcome_20d       FLOAT,
    outcome_filled_at TIMESTAMPTZ,
    was_correct       BOOLEAN
);

CREATE TABLE IF NOT EXISTS raw_snapshots (
    snapshot_id UUID NOT NULL,
    scraped_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL
);

SELECT create_hypertable('raw_snapshots', 'scraped_at', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_positions_ticker       ON positions(ticker, scraped_at DESC);
CREATE INDEX IF NOT EXISTS idx_market_prices_ticker   ON market_prices(ticker, ts DESC);
CREATE INDEX IF NOT EXISTS idx_decision_log_ticker    ON decision_log(ticker);
CREATE INDEX IF NOT EXISTS idx_decision_log_decided   ON decision_log(decided_at DESC);

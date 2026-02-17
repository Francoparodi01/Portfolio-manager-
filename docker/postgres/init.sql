CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ==============================
-- PORTFOLIO SNAPSHOTS
-- ==============================
CREATE TABLE portfolio_snapshots (
    id BIGSERIAL,
    timestamp TIMESTAMPTZ NOT NULL,
    total_value NUMERIC(18,2) NOT NULL,
    currency TEXT NOT NULL,
    raw_snapshot_id TEXT,
    normalized_at TIMESTAMPTZ,
    PRIMARY KEY (id, timestamp)
);

SELECT create_hypertable(
    'portfolio_snapshots',
    'timestamp',
    if_not_exists => TRUE
);

CREATE INDEX idx_portfolio_timestamp
ON portfolio_snapshots (timestamp DESC);


-- ==============================
-- POSITIONS
-- ==============================
CREATE TABLE positions (
    id BIGSERIAL,
    timestamp TIMESTAMPTZ NOT NULL,
    portfolio_snapshot_id BIGINT NOT NULL,
    ticker TEXT NOT NULL,
    instrument_name TEXT,
    quantity NUMERIC(18,4),
    avg_price NUMERIC(18,4),
    current_price NUMERIC(18,4),
    valuation NUMERIC(18,2),
    currency TEXT,
    pnl_amount NUMERIC(18,2),
    pnl_percent NUMERIC(10,4),
    PRIMARY KEY (id, timestamp)
);

SELECT create_hypertable(
    'positions',
    'timestamp',
    if_not_exists => TRUE
);

CREATE INDEX idx_positions_timestamp
ON positions (timestamp DESC);

CREATE INDEX idx_positions_portfolio
ON positions (portfolio_snapshot_id);

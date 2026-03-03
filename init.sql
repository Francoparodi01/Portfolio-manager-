-- =============================================================
-- init.sql — Cocos Portfolio System
-- TimescaleDB schema — ejecutado automaticamente al iniciar el
-- contenedor por primera vez (postgres entrypoint /docker-entrypoint-initdb.d/)
-- =============================================================

-- Habilitar extension TimescaleDB
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Habilitar UUID v4
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- =============================================================
-- TABLA: portfolio_snapshots
-- Un registro por ejecucion de scrape.
-- PK = snapshot_id (UUID), referenciado por todas las demas tablas.
-- =============================================================
CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    snapshot_id      UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    scraped_at       TIMESTAMPTZ NOT NULL,
    total_value_ars  NUMERIC(20, 4),
    cash_ars         NUMERIC(20, 4),
    confidence_score FLOAT       CHECK (confidence_score BETWEEN 0.0 AND 1.0),
    dom_hash         TEXT,
    raw_html_hash    TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE  portfolio_snapshots               IS 'Cabecera de cada ejecucion de scrape';
COMMENT ON COLUMN portfolio_snapshots.snapshot_id   IS 'UUID unico del snapshot';
COMMENT ON COLUMN portfolio_snapshots.scraped_at    IS 'Timestamp UTC del momento del scrape';
COMMENT ON COLUMN portfolio_snapshots.total_value_ars IS 'Tenencia valorizada en ARS (sin cash)';
COMMENT ON COLUMN portfolio_snapshots.cash_ars      IS 'Saldo disponible en ARS';
COMMENT ON COLUMN portfolio_snapshots.confidence_score IS 'Score 0-1 de confianza del scrape';
COMMENT ON COLUMN portfolio_snapshots.dom_hash      IS 'Hash de estructura DOM para detectar cambios';
COMMENT ON COLUMN portfolio_snapshots.raw_html_hash IS 'Hash del HTML completo del snapshot';


-- =============================================================
-- TABLA: positions
-- Una fila por ticker por snapshot.
-- Hypertable particionada por scraped_at.
-- =============================================================
CREATE TABLE IF NOT EXISTS positions (
    id                   BIGSERIAL   NOT NULL,
    snapshot_id          UUID        NOT NULL REFERENCES portfolio_snapshots(snapshot_id) ON DELETE CASCADE,
    scraped_at           TIMESTAMPTZ NOT NULL,
    ticker               TEXT        NOT NULL,
    asset_type           TEXT        NOT NULL DEFAULT 'UNKNOWN',  -- ACCION | CEDEAR | BONO | FCI | CASH
    currency             TEXT        NOT NULL DEFAULT 'ARS',      -- ARS | USD | USD_MEP
    quantity             NUMERIC(20, 8),
    avg_cost             NUMERIC(20, 4),
    current_price        NUMERIC(20, 4),
    market_value         NUMERIC(20, 4),
    unrealized_pnl       NUMERIC(20, 4),
    unrealized_pnl_pct   NUMERIC(10, 6),
    weight_in_portfolio  NUMERIC(10, 6),
    sector               TEXT,
    PRIMARY KEY (id, scraped_at)
);

SELECT create_hypertable(
    'positions',
    'scraped_at',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '7 days'
);

CREATE INDEX IF NOT EXISTS idx_positions_ticker     ON positions (ticker, scraped_at DESC);
CREATE INDEX IF NOT EXISTS idx_positions_snapshot   ON positions (snapshot_id);
CREATE INDEX IF NOT EXISTS idx_positions_asset_type ON positions (asset_type, scraped_at DESC);

COMMENT ON TABLE  positions                        IS 'Posiciones del portfolio por snapshot';
COMMENT ON COLUMN positions.ticker                 IS 'Simbolo normalizado (ej: GGAL, YPFD, CVX)';
COMMENT ON COLUMN positions.asset_type             IS 'Tipo de activo: ACCION | CEDEAR | BONO | FCI | CASH';
COMMENT ON COLUMN positions.weight_in_portfolio    IS 'Peso porcentual sobre total_value_ars (0-1)';
COMMENT ON COLUMN positions.unrealized_pnl_pct     IS 'Ganancia/perdida no realizada en pct (0.05 = 5%)';


-- =============================================================
-- TABLA: market_prices
-- Precio de cierre / last price por ticker y timestamp.
-- Hypertable particionada por ts.
-- Constraint UNIQUE (ts, ticker) para idempotencia en inserts.
-- =============================================================
CREATE TABLE IF NOT EXISTS market_prices (
    ts              TIMESTAMPTZ NOT NULL,
    ticker          TEXT        NOT NULL,
    asset_type      TEXT,
    currency        TEXT        DEFAULT 'ARS',
    last_price      NUMERIC(20, 4),
    change_pct_1d   NUMERIC(10, 6),
    volume          NUMERIC(20, 2),
    UNIQUE (ts, ticker)
);

SELECT create_hypertable(
    'market_prices',
    'ts',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 day'
);

CREATE INDEX IF NOT EXISTS idx_market_prices_ticker ON market_prices (ticker, ts DESC);

COMMENT ON TABLE  market_prices              IS 'Precios de mercado scrapeados (ACCIONES + CEDEARs)';
COMMENT ON COLUMN market_prices.ts           IS 'Timestamp UTC del precio';
COMMENT ON COLUMN market_prices.change_pct_1d IS 'Variacion diaria en pct (0.02 = 2%)';


-- =============================================================
-- TABLA: raw_snapshots
-- JSON completo del snapshot para auditoria y reprocesamiento.
-- Hypertable por scraped_at.
-- =============================================================
CREATE TABLE IF NOT EXISTS raw_snapshots (
    snapshot_id UUID        NOT NULL REFERENCES portfolio_snapshots(snapshot_id) ON DELETE CASCADE,
    scraped_at  TIMESTAMPTZ NOT NULL,
    payload     JSONB       NOT NULL,
    PRIMARY KEY (snapshot_id, scraped_at)
);

SELECT create_hypertable(
    'raw_snapshots',
    'scraped_at',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '30 days'
);

COMMENT ON TABLE  raw_snapshots         IS 'Payload JSON completo de cada snapshot (audit trail)';
COMMENT ON COLUMN raw_snapshots.payload IS 'PortfolioSnapshot serializado como JSON';


-- =============================================================
-- TABLA: daily_prices
-- Precios OHLCV diarios calculados a partir de market_prices.
-- Poblada por un job o vista materializada.
-- =============================================================
CREATE TABLE IF NOT EXISTS daily_prices (
    date        DATE        NOT NULL,
    ticker      TEXT        NOT NULL,
    open        NUMERIC(20, 4),
    high        NUMERIC(20, 4),
    low         NUMERIC(20, 4),
    close       NUMERIC(20, 4),
    volume      NUMERIC(20, 2),
    asset_type  TEXT,
    currency    TEXT        DEFAULT 'ARS',
    PRIMARY KEY (date, ticker)
);

COMMENT ON TABLE daily_prices IS 'Precios OHLCV diarios agregados desde market_prices';


-- =============================================================
-- TABLA: metrics
-- Metricas calculadas de performance por snapshot.
-- Volatilidad, beta, sharpe, etc.
-- =============================================================
CREATE TABLE IF NOT EXISTS metrics (
    id           BIGSERIAL   PRIMARY KEY,
    snapshot_id  UUID        NOT NULL REFERENCES portfolio_snapshots(snapshot_id) ON DELETE CASCADE,
    calculated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ticker       TEXT,                          -- NULL = metrica de portfolio total
    metric_name  TEXT        NOT NULL,          -- 'volatility_30d', 'beta', 'sharpe_1y', etc.
    metric_value NUMERIC(20, 8),
    metric_meta  JSONB                          -- parametros usados en el calculo
);

CREATE INDEX IF NOT EXISTS idx_metrics_snapshot ON metrics (snapshot_id);
CREATE INDEX IF NOT EXISTS idx_metrics_ticker   ON metrics (ticker, metric_name, calculated_at DESC);

COMMENT ON TABLE  metrics              IS 'Metricas cuantitativas calculadas sobre snapshots';
COMMENT ON COLUMN metrics.ticker       IS 'NULL si es metrica de portfolio total';
COMMENT ON COLUMN metrics.metric_name  IS 'Nombre canonico: volatility_30d | beta | sharpe_1y | ...';
COMMENT ON COLUMN metrics.metric_meta  IS 'Metadatos del calculo: ventana, benchmark, etc.';


-- =============================================================
-- TABLA: forecast_results
-- Resultados de modelos predictivos (precio objetivo, señales).
-- =============================================================
CREATE TABLE IF NOT EXISTS forecast_results (
    id              BIGSERIAL   PRIMARY KEY,
    generated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ticker          TEXT        NOT NULL,
    model_name      TEXT        NOT NULL,       -- 'arima', 'prophet', 'linear', etc.
    horizon_days    INT,
    price_target    NUMERIC(20, 4),
    confidence      FLOAT       CHECK (confidence BETWEEN 0.0 AND 1.0),
    signal          TEXT,                       -- 'BUY' | 'SELL' | 'HOLD'
    model_params    JSONB,
    raw_output      JSONB
);

CREATE INDEX IF NOT EXISTS idx_forecast_ticker ON forecast_results (ticker, generated_at DESC);

COMMENT ON TABLE  forecast_results          IS 'Resultados de modelos predictivos por ticker';
COMMENT ON COLUMN forecast_results.signal   IS 'Señal de trading: BUY | SELL | HOLD';
COMMENT ON COLUMN forecast_results.horizon_days IS 'Horizonte del forecast en dias';


-- =============================================================
-- TABLA: decisions_log
-- Registro auditado de decisiones (manuales o automaticas).
-- Inmutable: no se actualiza, solo INSERT.
-- =============================================================
CREATE TABLE IF NOT EXISTS decisions_log (
    id              BIGSERIAL   PRIMARY KEY,
    decided_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ticker          TEXT,
    decision_type   TEXT        NOT NULL,   -- 'BUY' | 'SELL' | 'REBALANCE' | 'HOLD' | 'ALERT'
    trigger_source  TEXT,                   -- 'manual' | 'scheduler' | 'signal'
    snapshot_id     UUID        REFERENCES portfolio_snapshots(snapshot_id),
    rationale       TEXT,
    params          JSONB,
    executed        BOOLEAN     DEFAULT FALSE,
    executed_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_decisions_ticker ON decisions_log (ticker, decided_at DESC);

COMMENT ON TABLE  decisions_log                IS 'Log inmutable de decisiones tacticas';
COMMENT ON COLUMN decisions_log.decision_type  IS 'Tipo: BUY | SELL | REBALANCE | HOLD | ALERT';
COMMENT ON COLUMN decisions_log.trigger_source IS 'Origen: manual | scheduler | signal';
COMMENT ON COLUMN decisions_log.executed       IS 'Si la decision fue ejecutada en el broker';


-- =============================================================
-- VISTA: portfolio_history
-- Vista conveniente para analisis temporal del portfolio.
-- =============================================================
CREATE OR REPLACE VIEW portfolio_history AS
SELECT
    ps.scraped_at,
    ps.snapshot_id,
    ps.total_value_ars,
    ps.cash_ars,
    ps.total_value_ars + ps.cash_ars   AS total_with_cash_ars,
    ps.confidence_score,
    COUNT(p.id)                         AS position_count
FROM portfolio_snapshots ps
LEFT JOIN positions p ON p.snapshot_id = ps.snapshot_id
GROUP BY ps.snapshot_id, ps.scraped_at, ps.total_value_ars, ps.cash_ars, ps.confidence_score
ORDER BY ps.scraped_at DESC;

COMMENT ON VIEW portfolio_history IS 'Vista resumen del portfolio a lo largo del tiempo';


-- =============================================================
-- VISTA: latest_positions
-- Posiciones del ultimo snapshot disponible.
-- =============================================================
CREATE OR REPLACE VIEW latest_positions AS
SELECT p.*
FROM positions p
INNER JOIN (
    SELECT snapshot_id
    FROM portfolio_snapshots
    ORDER BY scraped_at DESC
    LIMIT 1
) latest ON latest.snapshot_id = p.snapshot_id;

COMMENT ON VIEW latest_positions IS 'Posiciones del snapshot mas reciente';


-- =============================================================
-- Retention policy opcional (descomentar si se desea)
-- Borra chunks de market_prices con mas de 1 año de antiguedad
-- =============================================================
-- SELECT add_retention_policy('market_prices', INTERVAL '1 year');
-- SELECT add_retention_policy('positions',     INTERVAL '5 years');

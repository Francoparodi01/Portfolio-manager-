"""
src/collector/db.py — Capa de persistencia: TimescaleDB via asyncpg.

Tablas: portfolio_snapshots, positions, market_prices, raw_snapshots,
        decision_log (decision_memory), bot_users.
"""
from __future__ import annotations

import json
import logging
import uuid
from typing import Optional

try:
    import asyncpg
    HAS_ASYNCPG = True
except ImportError:
    HAS_ASYNCPG = False

logger = logging.getLogger(__name__)

# ── Tickers que yfinance no puede descargar con el símbolo de Cocos ───────────
# Razones: delisted, renombrados, locales sin ADR, o símbolo diferente en NYSE.
YFINANCE_BLACKLIST: set[str] = {
    "BRKB",              # yfinance requiere BRK-B
    "COME", "CRES",      # acciones locales ARG sin ADR liquid
    "DESP",              # delisted NYSE
    "IRSA",              # local ARG (NYSE ticker es IRS)
    "PAMP", "TECO2", "TGSU2", "TXAR", "TXR",  # locales ARG
    "VALE3",             # brasileña — NYSE es VALE
    "YPFD",              # local ARG — NYSE es YPF
}


SCHEMA_SQL = """
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

CREATE TABLE IF NOT EXISTS raw_snapshots (
    snapshot_id UUID        NOT NULL REFERENCES portfolio_snapshots(snapshot_id) ON DELETE CASCADE,
    scraped_at  TIMESTAMPTZ NOT NULL,
    payload     JSONB       NOT NULL
);

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
"""


class PortfolioDatabase:
    """
    Acceso a TimescaleDB via asyncpg.

    Uso:
        db = PortfolioDatabase(dsn)
        await db.connect()
        await db.save_snapshot(snapshot)
        await db.close()
    """

    def __init__(self, dsn: str):
        self._dsn  = dsn.replace("postgresql+asyncpg://", "postgresql://")
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        if not HAS_ASYNCPG:
            raise ImportError("asyncpg no instalado: pip install asyncpg")
        self._pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=5)
        logger.info("Conexion a base de datos establecida")

    async def close(self):
        if self._pool:
            await self._pool.close()
            logger.info("Conexion a base de datos cerrada")

    async def init_schema(self):
        """Crea tablas e índices si no existen. Idempotente."""
        if not self._pool:
            raise RuntimeError("Llamar connect() primero")
        statements = [s.strip() for s in SCHEMA_SQL.split(";") if s.strip()]
        async with self._pool.acquire() as conn:
            for stmt in statements:
                try:
                    await conn.execute(stmt)
                except Exception as e:
                    logger.debug(f"Schema stmt ignorado: {e!r}")
        # Hypertables TimescaleDB
        for table, col in [("positions", "scraped_at"),
                            ("market_prices", "ts"),
                            ("raw_snapshots", "scraped_at")]:
            try:
                async with self._pool.acquire() as conn:
                    await conn.execute(
                        f"SELECT create_hypertable('{table}', '{col}', if_not_exists => TRUE)"
                    )
            except Exception as e:
                logger.debug(f"Hypertable {table} ignorado: {e!r}")
        # Índices
        for sql in [
            "CREATE INDEX IF NOT EXISTS idx_positions_ticker ON positions(ticker, scraped_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_market_prices_ticker ON market_prices(ticker, ts DESC)",
            "CREATE INDEX IF NOT EXISTS idx_decision_log_ticker ON decision_log(ticker)",
            "CREATE INDEX IF NOT EXISTS idx_decision_log_decided_at ON decision_log(decided_at DESC)",
        ]:
            try:
                async with self._pool.acquire() as conn:
                    await conn.execute(sql)
            except Exception as e:
                logger.debug(f"Index ignorado: {e!r}")
        logger.info("Schema inicializado")

    # ── Snapshot ──────────────────────────────────────────────────────────────

    async def save_snapshot(self, snapshot) -> uuid.UUID:
        if not self._pool:
            raise RuntimeError("Llamar connect() primero")
        sid = snapshot.snapshot_id
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO portfolio_snapshots
                        (snapshot_id, scraped_at, total_value_ars, cash_ars,
                         confidence_score, dom_hash, raw_html_hash)
                    VALUES ($1,$2,$3,$4,$5,$6,$7)
                    ON CONFLICT (snapshot_id) DO NOTHING
                    """,
                    sid, snapshot.scraped_at,
                    float(snapshot.total_value_ars), float(snapshot.cash_ars),
                    snapshot.confidence_score, snapshot.dom_hash, snapshot.raw_html_hash,
                )
                if snapshot.positions:
                    rows = [
                        (sid, snapshot.scraped_at,
                         p.ticker, p.asset_type.value, p.currency.value,
                         float(p.quantity), float(p.avg_cost), float(p.current_price),
                         float(p.market_value), float(p.unrealized_pnl),
                         float(p.unrealized_pnl_pct),
                         float(p.weight_in_portfolio) if p.weight_in_portfolio else None,
                         p.sector)
                        for p in snapshot.positions
                    ]
                    await conn.executemany(
                        """
                        INSERT INTO positions
                            (snapshot_id, scraped_at, ticker, asset_type, currency,
                             quantity, avg_cost, current_price, market_value,
                             unrealized_pnl, unrealized_pnl_pct, weight_in_portfolio, sector)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
                        """, rows,
                    )
                await conn.execute(
                    "INSERT INTO raw_snapshots (snapshot_id, scraped_at, payload) VALUES ($1,$2,$3::jsonb)",
                    sid, snapshot.scraped_at, json.dumps(snapshot.to_dict()),
                )
        logger.info(f"Snapshot {sid} guardado ({len(snapshot.positions)} posiciones)")
        return sid

    async def save_market_prices(self, assets: list) -> int:
        if not assets or not self._pool:
            return 0
        rows = [
            (a.scraped_at, a.ticker, a.asset_type.value, a.currency.value,
             float(a.last_price), float(a.change_pct_1d or 0),
             float(a.volume) if a.volume else None)
            for a in assets
        ]
        async with self._pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO market_prices (ts, ticker, asset_type, currency, last_price, change_pct_1d, volume)
                VALUES ($1,$2,$3,$4,$5,$6,$7)
                ON CONFLICT (ts, ticker) DO NOTHING
                """, rows,
            )
        logger.info(f"{len(rows)} precios de mercado guardados")
        return len(rows)

    # ── Queries ───────────────────────────────────────────────────────────────

    async def get_latest_snapshot(self) -> Optional[dict]:
        if not self._pool:
            return None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT payload FROM raw_snapshots ORDER BY scraped_at DESC LIMIT 1"
            )
            return json.loads(row["payload"]) if row else None

    async def get_portfolio_history(self, limit: int = 60) -> list[dict]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT scraped_at, total_value_ars, cash_ars
                FROM portfolio_snapshots
                ORDER BY scraped_at ASC LIMIT $1
                """, limit,
            )
        return [dict(r) for r in rows]

    async def get_latest_market_prices(self) -> list[dict]:
        """
        Último precio registrado por ticker.
        Retorna lista de dicts: {ticker, asset_type, currency, last_price, change_pct_1d, ts}
        """
        if not self._pool:
            return []
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (ticker)
                    ticker, asset_type, currency, last_price, change_pct_1d, ts
                FROM market_prices
                ORDER BY ticker, ts DESC
                """
            )
        return [dict(r) for r in rows]

    async def get_cocos_universe(self) -> list[str]:
        """
        Retorna los tickers únicos disponibles en market_prices,
        excluyendo los de YFINANCE_BLACKLIST (tickers que yfinance no puede descargar).

        PRINCIPIO DE DISEÑO: el universo de análisis = exactamente los tickers
        scrapeados de Cocos Capital. yfinance provee históricos pero el universo
        lo define Cocos, no yfinance. Esta función es la fuente de verdad.
        """
        prices = await self.get_latest_market_prices()
        tickers = sorted({
            row["ticker"].upper()
            for row in prices
            if row["ticker"].upper() not in YFINANCE_BLACKLIST
        })
        logger.info(f"Universo Cocos: {len(tickers)} tickers disponibles")
        return tickers

    # ── Decision log ──────────────────────────────────────────────────────────

    async def get_pool(self):
        """Expone el pool para decision_memory.py."""
        return self._pool

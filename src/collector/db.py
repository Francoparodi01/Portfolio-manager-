"""
src/collector/db.py
Capa de persistencia: TimescaleDB via asyncpg directo.
Tablas: portfolio_snapshots, positions, market_prices, raw_snapshots.
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

from src.collector.data.models import MarketAsset, PortfolioSnapshot

logger = logging.getLogger(__name__)


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
        self._dsn = dsn.replace("postgresql+asyncpg://", "postgresql://")
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
        """Crea las tablas si no existen. Ejecutar una vez al inicio."""
        if not self._pool:
            raise RuntimeError("Llamar connect() primero")
        statements = [s.strip() for s in SCHEMA_SQL.split(";") if s.strip()]
        async with self._pool.acquire() as conn:
            for stmt in statements:
                try:
                    await conn.execute(stmt)
                except Exception as e:
                    logger.debug(f"Schema stmt ignorado: {e!r}")
        # Convertir a hypertables TimescaleDB
        for table, col in [("positions", "scraped_at"), ("market_prices", "ts"), ("raw_snapshots", "scraped_at")]:
            try:
                async with self._pool.acquire() as conn:
                    await conn.execute(
                        f"SELECT create_hypertable('{table}', '{col}', if_not_exists => TRUE)"
                    )
            except Exception as e:
                logger.debug(f"Hypertable {table} ignorado: {e!r}")
        # Indices
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS idx_positions_ticker ON positions(ticker, scraped_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_market_prices_ticker ON market_prices(ticker, ts DESC)",
        ]:
            try:
                async with self._pool.acquire() as conn:
                    await conn.execute(idx_sql)
            except Exception as e:
                logger.debug(f"Index ignorado: {e!r}")
        logger.info("Schema inicializado")

    async def save_snapshot(self, snapshot: PortfolioSnapshot) -> uuid.UUID:
        """
        Persiste un PortfolioSnapshot completo en una transaccion:
        - portfolio_snapshots (header)
        - positions (una fila por posicion)
        - raw_snapshots (JSON completo para auditoria)
        """
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
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (snapshot_id) DO NOTHING
                    """,
                    sid,
                    snapshot.scraped_at,
                    float(snapshot.total_value_ars),
                    float(snapshot.cash_ars),
                    snapshot.confidence_score,
                    snapshot.dom_hash,
                    snapshot.raw_html_hash,
                )

                if snapshot.positions:
                    rows = [
                        (
                            sid,
                            snapshot.scraped_at,
                            p.ticker,
                            p.asset_type.value,
                            p.currency.value,
                            float(p.quantity),
                            float(p.avg_cost),
                            float(p.current_price),
                            float(p.market_value),
                            float(p.unrealized_pnl),
                            float(p.unrealized_pnl_pct),
                            float(p.weight_in_portfolio) if p.weight_in_portfolio else None,
                            p.sector,
                        )
                        for p in snapshot.positions
                    ]
                    await conn.executemany(
                        """
                        INSERT INTO positions
                            (snapshot_id, scraped_at, ticker, asset_type, currency,
                             quantity, avg_cost, current_price, market_value,
                             unrealized_pnl, unrealized_pnl_pct, weight_in_portfolio, sector)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
                        """,
                        rows,
                    )

                await conn.execute(
                    """
                    INSERT INTO raw_snapshots (snapshot_id, scraped_at, payload)
                    VALUES ($1, $2, $3::jsonb)
                    """,
                    sid,
                    snapshot.scraped_at,
                    json.dumps(snapshot.to_dict()),
                )

        logger.info(f"Snapshot {sid} guardado ({len(snapshot.positions)} posiciones)")
        return sid

    async def save_market_prices(self, assets: list[MarketAsset]) -> int:
        if not assets or not self._pool:
            return 0
        rows = [
            (
                a.scraped_at,
                a.ticker,
                a.asset_type.value,
                a.currency.value,
                float(a.last_price),
                float(a.change_pct_1d or 0),
                float(a.volume) if a.volume else None,
            )
            for a in assets
        ]
        async with self._pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO market_prices
                    (ts, ticker, asset_type, currency, last_price, change_pct_1d, volume)
                VALUES ($1,$2,$3,$4,$5,$6,$7)
                ON CONFLICT (ts, ticker) DO NOTHING
                """,
                rows,
            )
        logger.info(f"{len(rows)} precios de mercado guardados")
        return len(rows)

    async def get_latest_snapshot(self) -> Optional[dict]:
        """Retorna el ultimo snapshot como dict (para notificaciones), o None."""
        if not self._pool:
            return None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT payload FROM raw_snapshots ORDER BY scraped_at DESC LIMIT 1"
            )
            return json.loads(row["payload"]) if row else None
        

    async def get_portfolio_history(self, limit: int = 60) -> list[dict]:
        """Retorna historial de snapshots para calcular drawdown."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT scraped_at, total_value_ars, cash_ars
                FROM portfolio_snapshots
                ORDER BY scraped_at ASC
                LIMIT $1
                """,
                limit,
            )
        return [dict(r) for r in rows]
"""
src/collector/db.py — Capa de persistencia: TimescaleDB via asyncpg.

Tablas: portfolio_snapshots, positions, market_prices, raw_snapshots,
        decision_log, bot_users.

Changelog v2:
  - get_performance_stats(): bug fixes críticos
      1. Agrupación ticker_stats por ticker solamente (antes por ticker+decision)
      2. Inversión de signo para SELL en avg_win/avg_loss/retornos
      3. Filtro was_correct IS NOT NULL en queries de cerrados
  - get_equity_curve(): agrega filtro was_correct IS NOT NULL
  - SCHEMA_SQL: columnas trade_lifecycle (decision_type, signal_strength,
    stop_loss_price, target_price, exit_scope, exit_reason_rule, stop_policy,
    stop_source, trailing_active, was_stopped, exit_reason, closed_at,
    close_price, source)
  - save_trade_decision(): nuevo método para persistir TradeDecision
  - init_schema(): corre migration trade_lifecycle automáticamente
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
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
YFINANCE_BLACKLIST: set[str] = {
    "BRKB",
    "COME", "CRES",
    "DESP",
    "IRSA",
    "PAMP", "TECO2", "TGSU2", "TXAR", "TXR",
    "VALE3",
    "YPFD",
}

# ── Guardia anti-ARS ──────────────────────────────────────────────────────────
# Precios USD del universo Cocos nunca superan $5000.
# Si price_at_decision > este umbral, fue guardado en ARS por error.
MAX_PRICE_USD = 5_000.0


SCHEMA_SQL = """
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    snapshot_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    scraped_at        TIMESTAMPTZ NOT NULL,
    total_value_ars   NUMERIC(20,4),
    cash_ars          NUMERIC(20,4),
    confidence_score  FLOAT,
    dom_hash          TEXT,
    raw_html_hash     TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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
    payload     JSONB       NOT NULL,
    PRIMARY KEY (snapshot_id, scraped_at)
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
    was_correct       BOOLEAN,
    size_pct          FLOAT,
    stop_loss_pct     FLOAT,
    target_pct        FLOAT,
    horizon_days      INTEGER,
    rr_ratio          FLOAT,
    -- trade_lifecycle columns (additive, NULL for legacy rows)
    decision_type     TEXT,
    signal_strength   TEXT,
    stop_loss_price   FLOAT,
    target_price      FLOAT,
    exit_scope        TEXT,
    exit_reason_rule  TEXT,
    stop_policy       TEXT,
    stop_source       TEXT,
    trailing_active   BOOLEAN DEFAULT FALSE,
    was_stopped       BOOLEAN,
    exit_reason       TEXT,
    closed_at         TIMESTAMPTZ,
    close_price       FLOAT,
    source            TEXT
);
"""

# ── Migration SQL para decision_log (idempotente) ─────────────────────────────
# Se corre en init_schema() además del DDL base.
# Seguro de correr múltiples veces (IF NOT EXISTS / IF NOT EXISTS).
_LIFECYCLE_MIGRATION_SQL = """
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
UPDATE decision_log
SET decision_type = CASE
    WHEN decision = 'BUY'  THEN 'BUY'
    WHEN decision = 'SELL' THEN 'SELL_FULL'
    WHEN decision = 'HOLD' THEN 'HOLD'
    ELSE decision
END
WHERE decision_type IS NULL;
"""


class PortfolioDatabase:
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
        if not self._pool:
            raise RuntimeError("Llamar connect() primero")

        # DDL base
        statements = [s.strip() for s in SCHEMA_SQL.split(";") if s.strip()]
        async with self._pool.acquire() as conn:
            for stmt in statements:
                try:
                    await conn.execute(stmt)
                except Exception as e:
                    logger.debug(f"Schema stmt ignorado: {e!r}")

        # Hypertables
        hypertables = [
            ("positions",    "scraped_at"),
            ("market_prices","ts"),
            ("raw_snapshots","scraped_at"),
        ]
        for table, col in hypertables:
            try:
                async with self._pool.acquire() as conn:
                    await conn.execute(
                        f"SELECT create_hypertable('{table}', '{col}', if_not_exists => TRUE)"
                    )
            except Exception as e:
                logger.debug(f"Hypertable {table} ignorado: {e!r}")

        # Índices
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_portfolio_snapshots_scraped_at ON portfolio_snapshots(scraped_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_positions_ticker ON positions(ticker, scraped_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_positions_snapshot_id ON positions(snapshot_id)",
            "CREATE INDEX IF NOT EXISTS idx_market_prices_ticker ON market_prices(ticker, ts DESC)",
            "CREATE INDEX IF NOT EXISTS idx_raw_snapshots_scraped_at ON raw_snapshots(scraped_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_decision_log_ticker ON decision_log(ticker)",
            "CREATE INDEX IF NOT EXISTS idx_decision_log_decided_at ON decision_log(decided_at DESC)",
            """
            CREATE INDEX IF NOT EXISTS idx_decision_log_outcome
            ON decision_log(decided_at DESC)
            WHERE outcome_5d IS NOT NULL
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_decision_log_pending
            ON decision_log(decided_at)
            WHERE outcome_5d IS NULL
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_decision_log_stops
            ON decision_log(decision, stop_loss_price, outcome_5d)
            WHERE stop_loss_price IS NOT NULL AND outcome_5d IS NULL
            """,
        ]
        for sql in indexes:
            try:
                async with self._pool.acquire() as conn:
                    await conn.execute(sql)
            except Exception as e:
                logger.debug(f"Index ignorado: {e!r}")

        # Migration trade_lifecycle (idempotente)
        migration_stmts = [
            s.strip() for s in _LIFECYCLE_MIGRATION_SQL.split(";") if s.strip()
        ]
        async with self._pool.acquire() as conn:
            for stmt in migration_stmts:
                try:
                    await conn.execute(stmt)
                except Exception as e:
                    logger.debug(f"Migration stmt ignorado: {e!r}")

        logger.info("Schema inicializado (incluyendo migration trade_lifecycle)")

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
                    ON CONFLICT (snapshot_id) DO UPDATE SET
                        scraped_at       = EXCLUDED.scraped_at,
                        total_value_ars  = EXCLUDED.total_value_ars,
                        cash_ars         = EXCLUDED.cash_ars,
                        confidence_score = EXCLUDED.confidence_score,
                        dom_hash         = EXCLUDED.dom_hash,
                        raw_html_hash    = EXCLUDED.raw_html_hash
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

                    await conn.execute(
                        """
                        DELETE FROM positions
                        WHERE snapshot_id = $1
                        """,
                        sid,
                    )

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
                    VALUES ($1,$2,$3::jsonb)
                    ON CONFLICT (snapshot_id, scraped_at) DO UPDATE SET
                        payload = EXCLUDED.payload
                    """,
                    sid,
                    snapshot.scraped_at,
                    json.dumps(snapshot.to_dict()),
                )

        logger.info(f"Snapshot {sid} guardado ({len(snapshot.positions)} posiciones)")
        return sid

    async def save_market_prices(self, assets: list) -> int:
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
                ON CONFLICT (ts, ticker) DO UPDATE SET
                    asset_type    = EXCLUDED.asset_type,
                    currency      = EXCLUDED.currency,
                    last_price    = EXCLUDED.last_price,
                    change_pct_1d = EXCLUDED.change_pct_1d,
                    volume        = EXCLUDED.volume
                """,
                rows,
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
        """
        Retorna snapshots recientes con posiciones incluidas, leídos desde raw_snapshots.
        Devuelve en orden cronológico ascendente (el más antiguo primero).
        """
        if not self._pool:
            return []
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT payload
                FROM raw_snapshots
                ORDER BY scraped_at DESC
                LIMIT $1
                """,
                limit,
            )
        result = []
        for r in reversed(rows):
            try:
                result.append(json.loads(r["payload"]))
            except Exception as e:
                logger.debug(f"get_portfolio_history: payload inválido — {e}")
        return result

    async def get_latest_market_prices(self) -> list[dict]:
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
        prices = await self.get_latest_market_prices()
        tickers = sorted({
            row["ticker"].upper()
            for row in prices
            if row["ticker"].upper() not in YFINANCE_BLACKLIST
        })
        logger.info(f"Universo Cocos: {len(tickers)} tickers disponibles")
        return tickers

    # ── Deduplicación ─────────────────────────────────────────────────────────

    async def has_recent_decision(self, ticker: str, direction: str, hours: int = 20) -> bool:
        if not self._pool:
            return False
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT 1
                FROM decision_log
                WHERE ticker    = $1
                  AND decision  = $2
                  AND decided_at > NOW() - ($3 || ' hours')::INTERVAL
                LIMIT 1
                """,
                ticker.upper(),
                direction.upper(),
                str(hours),
            )
        return row is not None

    # ── Cierre de trades ──────────────────────────────────────────────────────

    async def close_expired_trades(self, lookback_days: int = 30) -> int:
        if not self._pool:
            return 0

        cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, ticker, decision, decided_at,
                       outcome_5d, outcome_10d, outcome_20d,
                       stop_loss_pct, target_pct, horizon_days,
                       was_correct
                FROM decision_log
                WHERE decided_at >= $1
                  AND outcome_5d IS NOT NULL
                  AND was_correct IS NULL
                  AND decision IN ('BUY', 'SELL')
                """,
                cutoff,
            )

        if not rows:
            return 0

        updated = 0
        now = datetime.now(timezone.utc)

        for r in rows:
            outcome   = float(r["outcome_5d"] or 0.0)
            stop      = float(r["stop_loss_pct"] or -0.08)
            target    = float(r["target_pct"] or 0.16)
            direction = str(r["decision"]).upper()
            decided   = r["decided_at"]
            horizon   = int(r["horizon_days"] or 10)

            # Corregir signo para SELL: el trader gana cuando el precio baja
            if direction == "SELL":
                outcome = -outcome
                stop    = abs(stop)
                target  = abs(target)

            if outcome >= target:
                was_correct = True
            elif outcome <= -abs(stop):
                was_correct = False
            elif (now - decided).days >= horizon:
                was_correct = outcome > 0
            else:
                continue

            try:
                async with self._pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE decision_log SET was_correct = $1 WHERE id = $2",
                        was_correct,
                        r["id"],
                    )
                updated += 1
                logger.debug(
                    f"Trade cerrado: id={r['id']} {direction} {r['ticker']} "
                    f"outcome={outcome:+.1%} correct={was_correct}"
                )
            except Exception as e:
                logger.warning(f"close_expired_trades write error: {e}")

        logger.info(f"close_expired_trades: {updated}/{len(rows)} trades cerrados")
        return updated

    # ── Equity curve ──────────────────────────────────────────────────────────

    async def get_equity_curve(self, lookback_days: int = 90) -> list[dict]:
        """
        Equity curve sobre trades cerrados (outcome_5d AND was_correct NOT NULL).
        Corrige signo de SELL: el trader gana cuando el precio baja.
        """
        if not self._pool:
            return []

        cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    DATE(decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires') AS trade_date,
                    ticker,
                    decision,
                    outcome_5d,
                    size_pct,
                    was_correct
                FROM decision_log
                WHERE decided_at >= $1
                  AND outcome_5d IS NOT NULL
                  AND was_correct IS NOT NULL
                  AND decision IN ('BUY', 'SELL')
                ORDER BY decided_at ASC
                """,
                cutoff,
            )

        if not rows:
            return []

        equity  = 100.0
        points  = []
        n_total = 0

        for r in rows:
            outcome   = float(r["outcome_5d"] or 0.0)
            size      = float(r["size_pct"] or 0.05)
            direction = str(r["decision"]).upper()

            # Retorno del trader: BUY gana si sube, SELL gana si baja
            trader_return = outcome if direction == "BUY" else -outcome
            equity       *= (1 + trader_return * size)
            n_total      += 1

            points.append({
                "date":        str(r["trade_date"]),
                "equity":      round(equity, 4),
                "trade_count": n_total,
                "ticker":      r["ticker"],
                "direction":   direction,
                "outcome":     round(trader_return, 4),  # signo ya corregido
                "correct":     r["was_correct"],
            })

        return points

    async def get_performance_stats_v2(self, lookback_days: int = 90) -> dict:
        await self.close_expired_trades(lookback_days=lookback_days)
        stats = await self.get_performance_stats(lookback_days=lookback_days)
        curve = await self.get_equity_curve(lookback_days=lookback_days)
        stats["equity_curve"] = curve

        if curve:
            stats["equity_start"]        = curve[0]["equity"]
            stats["equity_end"]          = curve[-1]["equity"]
            stats["equity_return"]       = (curve[-1]["equity"] / 100.0) - 1.0
            peak   = 100.0
            max_dd = 0.0
            for p in curve:
                peak   = max(peak, p["equity"])
                max_dd = min(max_dd, (p["equity"] - peak) / peak)
            stats["equity_max_drawdown"] = max_dd
        else:
            stats["equity_start"]        = 100.0
            stats["equity_end"]          = 100.0
            stats["equity_return"]       = 0.0
            stats["equity_max_drawdown"] = 0.0

        return stats

    async def get_pool(self):
        return self._pool

    # ── Decision Engine ───────────────────────────────────────────────────────

    async def save_decision(self, decision) -> Optional[int]:
        """
        Persiste un DecisionOutput (del decision_engine anterior).
        Para el nuevo sistema usar save_trade_decision().
        """
        if not self._pool:
            raise RuntimeError("Llamar connect() primero")
        if not decision.is_actionable():
            return None

        import json as _json

        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    INSERT INTO decision_log (
                        decided_at, ticker, decision, final_score, confidence,
                        layers, price_at_decision, vix_at_decision, regime,
                        size_pct, stop_loss_pct, target_pct, horizon_days, rr_ratio
                    ) VALUES (
                        $1, $2, $3, $4, $5,
                        $6::jsonb, $7, $8, $9,
                        $10, $11, $12, $13, $14
                    )
                    RETURNING id
                    """,
                    decision.decided_at,
                    decision.ticker.upper(),
                    decision.direction,
                    float(decision.score),
                    float(decision.conviction),
                    _json.dumps(decision.to_dict()),
                    decision.entry_price,
                    decision.vix,
                    decision.regime,
                    float(decision.size_pct),
                    float(decision.stop_loss_pct),
                    float(decision.target_pct),
                    int(decision.horizon_days),
                    float(decision.rr_ratio),
                )
            decision_id = row["id"]
            logger.info(f"Decisión guardada: id={decision_id} {decision.direction} {decision.ticker}")
            return decision_id
        except Exception as e:
            logger.error(f"save_decision: {e}", exc_info=True)
            return None

    async def save_trade_decision(self, td) -> Optional[int]:
        """
        Persiste un TradeDecision (de trade_lifecycle.py).
        Incluye decision_type, signal_strength, stop_loss_price, target_price,
        exit_scope, exit_reason_rule, stop_policy, stop_source, source.

        Uso:
            from src.analysis.trade_lifecycle import build_trade_decision
            td = build_trade_decision(...)
            trade_id = await db.save_trade_decision(td)
        """
        if not self._pool:
            raise RuntimeError("Llamar connect() primero")

        import json as _json

        d = td.to_db_dict()

        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    INSERT INTO decision_log (
                        decided_at, ticker, decision, final_score, confidence,
                        price_at_decision, vix_at_decision, regime,
                        size_pct, stop_loss_pct, stop_loss_price,
                        target_pct, target_price,
                        horizon_days, rr_ratio,
                        decision_type, signal_strength,
                        exit_scope, exit_reason_rule,
                        stop_policy, stop_source,
                        source
                    ) VALUES (
                        $1,$2,$3,$4,$5,
                        $6,$7,$8,
                        $9,$10,$11,
                        $12,$13,
                        $14,$15,
                        $16,$17,
                        $18,$19,
                        $20,$21,
                        $22
                    )
                    RETURNING id
                    """,
                    d.get("decided_at"),
                    d.get("ticker", "").upper(),
                    d.get("decision"),
                    float(d.get("final_score") or 0.0),
                    float(d.get("confidence") or 0.0),
                    d.get("price_at_decision"),
                    d.get("vix_at_decision"),
                    d.get("regime"),
                    float(d.get("size_pct") or 0.05),
                    d.get("stop_loss_pct"),
                    d.get("stop_loss_price"),
                    d.get("target_pct"),
                    d.get("target_price"),
                    int(d.get("horizon_days") or 10),
                    d.get("rr_ratio"),
                    d.get("decision_type"),
                    d.get("signal_strength"),
                    d.get("exit_scope"),
                    d.get("exit_reason_rule"),
                    d.get("stop_policy"),
                    d.get("stop_source"),
                    d.get("source"),
                )
            trade_id = row["id"]
            logger.info(
                f"TradeDecision guardado: id={trade_id} "
                f"{d.get('decision_type')} {d.get('ticker')}"
            )
            return trade_id
        except Exception as e:
            logger.error(f"save_trade_decision: {e}", exc_info=True)
            return None

    async def update_outcomes(self, lookback_days: int = 30) -> int:
        """
        Busca decisiones sin outcome donde han pasado >=5 días y llena
        outcome_5d / outcome_10d / outcome_20d / was_correct usando yfinance.

        GUARDIA ANTI-ARS: si price_at_decision > MAX_PRICE_USD el precio
        fue guardado en ARS por error → se skipea con warning.
        """
        if not self._pool:
            raise RuntimeError("Llamar connect() primero")

        try:
            import asyncio
            import yfinance as yf

            cutoff = datetime.now(timezone.utc) - timedelta(days=5)

            async with self._pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT id, ticker, price_at_decision, decided_at, decision
                    FROM decision_log
                    WHERE outcome_5d IS NULL
                      AND decided_at <= $1
                      AND decision != 'HOLD'
                    ORDER BY decided_at DESC
                    LIMIT 200
                    """,
                    cutoff,
                )

            if not rows:
                logger.info("update_outcomes: sin decisiones pendientes")
                return 0

            updated     = 0
            skipped_ars = 0
            now         = datetime.now(timezone.utc)

            for row in rows:
                ticker     = str(row["ticker"]).upper()
                entry      = row["price_at_decision"]
                decided_at = row["decided_at"]
                direction  = str(row["decision"]).upper()

                if not entry or float(entry) <= 0:
                    logger.debug(
                        f"update_outcomes SKIP {ticker} id={row['id']}: sin precio de entrada"
                    )
                    continue

                entry_f = float(entry)

                if entry_f > MAX_PRICE_USD:
                    logger.warning(
                        f"update_outcomes SKIP {ticker} id={row['id']}: "
                        f"price_at_decision={entry_f:,.0f} parece ARS "
                        f"(umbral USD={MAX_PRICE_USD:,.0f}). "
                        f"Corregir con: python scripts/backfill_prices.py"
                    )
                    skipped_ars += 1
                    continue

                try:
                    df = await asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda t=ticker, d=decided_at: yf.download(
                            t,
                            start=d.strftime("%Y-%m-%d"),
                            progress=False,
                            auto_adjust=True,
                        )["Close"].squeeze()
                    )
                    if df is None or df.empty:
                        continue
                except Exception as e:
                    logger.debug(f"update_outcomes yfinance {ticker}: {e}")
                    continue

                outcomes = {}
                for horizon, col in [
                    (5,  "outcome_5d"),
                    (10, "outcome_10d"),
                    (20, "outcome_20d"),
                ]:
                    target_date = (decided_at + timedelta(days=horizon)).replace(tzinfo=None)
                    if target_date > now.replace(tzinfo=None):
                        continue
                    try:
                        idx = df.index.searchsorted(target_date, side="left")
                        if idx >= len(df):
                            idx = len(df) - 1
                        price_at_horizon = float(df.iloc[idx])
                        outcomes[col] = (price_at_horizon - entry_f) / entry_f
                    except Exception:
                        continue

                if not outcomes:
                    continue

                primary = outcomes.get("outcome_5d", outcomes.get("outcome_10d"))
                was_correct = None
                if primary is not None:
                    if direction == "BUY":
                        was_correct = primary > 0
                    elif direction == "SELL":
                        # Signo corregido: SELL gana si el precio bajó
                        was_correct = primary < 0

                try:
                    async with self._pool.acquire() as conn:
                        await conn.execute(
                            """
                            UPDATE decision_log SET
                                outcome_5d        = COALESCE($2, outcome_5d),
                                outcome_10d       = COALESCE($3, outcome_10d),
                                outcome_20d       = COALESCE($4, outcome_20d),
                                was_correct       = COALESCE($5, was_correct),
                                outcome_filled_at = NOW()
                            WHERE id = $1
                            """,
                            row["id"],
                            outcomes.get("outcome_5d"),
                            outcomes.get("outcome_10d"),
                            outcomes.get("outcome_20d"),
                            was_correct,
                        )
                    updated += 1
                    logger.debug(f"outcome actualizado: {ticker} id={row['id']} {outcomes}")
                except Exception as e:
                    logger.warning(f"update_outcomes write error {ticker}: {e}")

            if skipped_ars:
                logger.warning(
                    f"update_outcomes: {skipped_ars} registros skipeados por precio ARS. "
                    f"Correr: python scripts/backfill_prices.py"
                )

            logger.info(
                f"update_outcomes: {updated}/{len(rows)} decisiones actualizadas"
                + (f" | {skipped_ars} con precio ARS pendiente" if skipped_ars else "")
            )
            return updated

        except ImportError:
            logger.error("update_outcomes requiere yfinance: pip install yfinance")
            return 0
        except Exception as e:
            logger.error(f"update_outcomes: {e}", exc_info=True)
            return 0

    async def get_performance_stats(self, lookback_days: int = 90) -> dict:
        """
        Métricas de performance sobre trades CERRADOS.

        Correcciones vs versión anterior:
          1. Filtra was_correct IS NOT NULL — solo trades verdaderamente cerrados.
          2. Agrupa ticker_stats por ticker solamente (antes por ticker+decision).
          3. Invierte signo de SELL en Python antes de calcular avg_win/avg_loss
             para que reflejen el retorno del TRADER, no del activo.
        """
        if not self._pool:
            return {}

        cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)

        async with self._pool.acquire() as conn:
            # Cargar filas raw — el cálculo de retorno del trader se hace en Python
            raw_rows = await conn.fetch(
                """
                SELECT
                    id, ticker, decision,
                    outcome_5d, outcome_10d, outcome_20d,
                    was_correct, size_pct
                FROM decision_log
                WHERE decided_at >= $1
                  AND outcome_5d IS NOT NULL
                  AND was_correct IS NOT NULL
                  AND decision IN ('BUY', 'SELL')
                ORDER BY decided_at ASC
                """,
                cutoff,
            )

            pending_count = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM decision_log
                WHERE outcome_5d IS NULL
                  AND decision IN ('BUY', 'SELL')
                  AND decided_at >= $1
                """,
                cutoff,
            )

            recent_rows = await conn.fetch(
                """
                SELECT ticker, decision, final_score, confidence,
                       outcome_5d, was_correct, decided_at,
                       size_pct, stop_loss_pct, target_pct, decision_type
                FROM decision_log
                WHERE decision IN ('BUY', 'SELL')
                ORDER BY decided_at DESC
                LIMIT 8
                """
            )

        # ── Calcular retorno del trader con signo correcto ────────────────────
        # BUY: ganas si el activo sube → trader_return = outcome_5d
        # SELL: ganas si el activo baja → trader_return = -outcome_5d
        trader_returns = []
        by_ticker: dict = {}
        ret_10d_list    = []
        ret_20d_list    = []

        for r in raw_rows:
            direction  = str(r["decision"]).upper()
            out5       = float(r["outcome_5d"] or 0.0)
            out10      = float(r["outcome_10d"]) if r["outcome_10d"] is not None else None
            out20      = float(r["outcome_20d"]) if r["outcome_20d"] is not None else None

            trader_ret  = out5 if direction == "BUY" else -out5
            trader_ret10 = (out10 if direction == "BUY" else -out10) if out10 is not None else None
            trader_ret20 = (out20 if direction == "BUY" else -out20) if out20 is not None else None

            trader_returns.append(trader_ret)
            if trader_ret10 is not None:
                ret_10d_list.append(trader_ret10)
            if trader_ret20 is not None:
                ret_20d_list.append(trader_ret20)

            # Agrupar por ticker (no por ticker+decision)
            tk = str(r["ticker"]).upper()
            if tk not in by_ticker:
                by_ticker[tk] = []
            by_ticker[tk].append(trader_ret)

        n        = len(trader_returns)
        wins     = [r for r in trader_returns if r > 0]
        losses   = [r for r in trader_returns if r <= 0]
        n_wins   = len(wins)
        n_losses = len(losses)

        win_rate = n_wins / n if n > 0 else None
        avg_win  = sum(wins)   / len(wins)   if wins   else None
        avg_loss = sum(losses) / len(losses) if losses else None
        avg_ret  = sum(trader_returns) / n   if n > 0  else None

        ev = None
        if win_rate is not None and avg_win is not None and avg_loss is not None:
            # avg_loss ya es negativo → EV = WR × avg_win + (1-WR) × avg_loss
            ev = (win_rate * avg_win) + ((1 - win_rate) * avg_loss)

        profit_factor = None
        total_wins_sum   = sum(wins)
        total_losses_sum = abs(sum(losses)) if losses else 0.0
        if total_losses_sum > 0:
            profit_factor = total_wins_sum / total_losses_sum

        # ── Stats por ticker ──────────────────────────────────────────────────
        ticker_stats = []
        for tk, rets in sorted(by_ticker.items(), key=lambda x: -len(x[1])):
            tk_wins = [r for r in rets if r > 0]
            tk_n    = len(rets)
            ticker_stats.append({
                "ticker":     tk,
                "trades":     tk_n,
                "wins":       len(tk_wins),
                "win_rate":   len(tk_wins) / tk_n if tk_n > 0 else 0,
                "avg_return": sum(rets) / tk_n if tk_n > 0 else None,
                "best":       max(rets) if rets else None,
                "worst":      min(rets) if rets else None,
                "decision":   None,  # campo legacy, ahora siempre None
            })

        return {
            "total_trades":    n,
            "winners":         n_wins,
            "losers":          n_losses,
            "pending":         int(pending_count or 0),
            "win_rate":        win_rate,
            "avg_win_5d":      avg_win,
            "avg_loss_5d":     avg_loss,
            "avg_return_5d":   avg_ret,
            "avg_return_10d":  sum(ret_10d_list) / len(ret_10d_list) if ret_10d_list else None,
            "avg_return_20d":  sum(ret_20d_list) / len(ret_20d_list) if ret_20d_list else None,
            "best_trade":      max(trader_returns) if trader_returns else None,
            "worst_trade":     min(trader_returns) if trader_returns else None,
            "ev":              ev,
            "profit_factor":   profit_factor,
            "lookback_days":   lookback_days,
            "ticker_stats":    ticker_stats[:10],
            "recent":          [dict(r) for r in recent_rows],
        }
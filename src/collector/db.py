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

  # ── Decision Engine ───────────────────────────────────────────────────────
 
    async def save_decision(self, decision) -> Optional[int]:
        """
        Guarda una DecisionOutput en decision_log.
        Retorna el id generado, o None si falla.
 
        Parámetro 'decision': instancia de DecisionOutput de decision_engine.py
        """
        if not self._pool:
            raise RuntimeError("Llamar connect() primero")
        if not decision.is_actionable():
            return None  # No guardamos HOLDs
 
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
 
    async def update_outcomes(self, lookback_days: int = 30) -> int:
        """
        Busca decisiones sin outcome donde han pasado ≥5 días y llena
        outcome_5d, outcome_10d, outcome_20d y was_correct usando yfinance.
 
        Retorna la cantidad de registros actualizados.
        """
        if not self._pool:
            raise RuntimeError("Llamar connect() primero")
 
        try:
            import yfinance as yf
            from datetime import datetime, timedelta
            import asyncio
 
            cutoff = datetime.utcnow() - timedelta(days=5)
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
 
            updated = 0
            now = datetime.utcnow()
 
            for row in rows:
                ticker     = str(row["ticker"]).upper()
                entry      = row["price_at_decision"]
                decided_at = row["decided_at"]
                direction  = str(row["decision"]).upper()
 
                if not entry or float(entry) <= 0:
                    continue
 
                try:
                    df = await asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda t=ticker, d=decided_at: yf.download(
                            t, start=d.strftime("%Y-%m-%d"),
                            progress=False, auto_adjust=True,
                        )["Close"].squeeze()
                    )
                    if df is None or df.empty:
                        continue
                except Exception as e:
                    logger.debug(f"update_outcomes yfinance {ticker}: {e}")
                    continue
 
                entry_f = float(entry)
                outcomes = {}
                for horizon, col in [(5, "outcome_5d"), (10, "outcome_10d"), (20, "outcome_20d")]:
                    target_date = decided_at + timedelta(days=horizon)
                    if target_date > now:
                        continue
                    # Buscar el precio más cercano a la fecha objetivo
                    try:
                        idx = df.index.searchsorted(target_date, side="left")
                        if idx >= len(df):
                            idx = len(df) - 1
                        price_at_horizon = float(df.iloc[idx])
                        pct_change = (price_at_horizon - entry_f) / entry_f
                        outcomes[col] = pct_change
                    except Exception:
                        continue
 
                if not outcomes:
                    continue
 
                # was_correct: el movimiento va en la dirección esperada
                primary = outcomes.get("outcome_5d", outcomes.get("outcome_10d"))
                was_correct = None
                if primary is not None:
                    if direction == "BUY":
                        was_correct = primary > 0
                    elif direction == "SELL":
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
 
            logger.info(f"update_outcomes: {updated}/{len(rows)} decisiones actualizadas")
            return updated
 
        except ImportError:
            logger.error("update_outcomes requiere yfinance: pip install yfinance")
            return 0
        except Exception as e:
            logger.error(f"update_outcomes: {e}", exc_info=True)
            return 0
 
    async def get_performance_stats(self, lookback_days: int = 90) -> dict:
        """
        Calcula win rate, avg_win, avg_loss, EV y métricas por horizonte.
        Solo cuenta decisiones donde el outcome ya fue llenado.
 
        Retorna dict con todas las métricas necesarias para /performance.
        """
        if not self._pool:
            return {}
 
        from datetime import datetime, timedelta
        cutoff = datetime.utcnow() - timedelta(days=lookback_days)
 
        async with self._pool.acquire() as conn:
            # Métricas globales (horizonte 5d como primario)
            global_row = await conn.fetchrow(
                """
                SELECT
                    COUNT(*)                                        AS total_trades,
                    COUNT(*) FILTER (WHERE was_correct = TRUE)      AS winners,
                    COUNT(*) FILTER (WHERE was_correct = FALSE)     AS losers,
                    AVG(outcome_5d)                                 AS avg_return_5d,
                    AVG(outcome_5d) FILTER (WHERE outcome_5d > 0)   AS avg_win_5d,
                    AVG(outcome_5d) FILTER (WHERE outcome_5d <= 0)  AS avg_loss_5d,
                    AVG(outcome_10d)                                AS avg_return_10d,
                    AVG(outcome_20d)                                AS avg_return_20d,
                    MAX(outcome_5d)                                 AS best_trade,
                    MIN(outcome_5d)                                 AS worst_trade,
                    STDDEV(outcome_5d)                              AS std_5d
                FROM decision_log
                WHERE decided_at >= $1
                  AND outcome_5d IS NOT NULL
                  AND decision != 'HOLD'
                """,
                cutoff,
            )
 
            # Por ticker (top performers y losers)
            ticker_rows = await conn.fetch(
                """
                SELECT
                    ticker,
                    COUNT(*)                    AS trades,
                    AVG(outcome_5d)             AS avg_return,
                    COUNT(*) FILTER (WHERE was_correct = TRUE)  AS wins,
                    decision
                FROM decision_log
                WHERE decided_at >= $1
                  AND outcome_5d IS NOT NULL
                  AND decision != 'HOLD'
                GROUP BY ticker, decision
                ORDER BY avg_return DESC
                LIMIT 10
                """,
                cutoff,
            )
 
            # Pendientes de outcome (decisiones recientes sin resultado aún)
            pending_count = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM decision_log
                WHERE outcome_5d IS NULL
                  AND decision != 'HOLD'
                  AND decided_at >= $1
                """,
                cutoff,
            )
 
            # Últimas 5 decisiones (con o sin outcome)
            recent_rows = await conn.fetch(
                """
                SELECT ticker, decision, final_score, confidence,
                       outcome_5d, was_correct, decided_at,
                       size_pct, stop_loss_pct, target_pct
                FROM decision_log
                WHERE decision != 'HOLD'
                ORDER BY decided_at DESC
                LIMIT 5
                """,
            )
 
        g = dict(global_row) if global_row else {}
 
        total   = int(g.get("total_trades") or 0)
        winners = int(g.get("winners") or 0)
        losers  = int(g.get("losers") or 0)
 
        win_rate  = winners / total if total > 0 else None
        loss_rate = losers  / total if total > 0 else None
 
        avg_win  = float(g["avg_win_5d"])  if g.get("avg_win_5d")  else None
        avg_loss = float(g["avg_loss_5d"]) if g.get("avg_loss_5d") else None
 
        # EV = (win_rate * avg_win) - (loss_rate * avg_loss)
        ev = None
        if win_rate is not None and avg_win is not None and avg_loss is not None:
            ev = (win_rate * avg_win) - ((1 - win_rate) * abs(avg_loss))
 
        return {
            "total_trades":    total,
            "winners":         winners,
            "losers":          losers,
            "pending":         int(pending_count or 0),
            "win_rate":        win_rate,
            "avg_win_5d":      avg_win,
            "avg_loss_5d":     avg_loss,
            "avg_return_5d":   float(g["avg_return_5d"])  if g.get("avg_return_5d")  else None,
            "avg_return_10d":  float(g["avg_return_10d"]) if g.get("avg_return_10d") else None,
            "avg_return_20d":  float(g["avg_return_20d"]) if g.get("avg_return_20d") else None,
            "best_trade":      float(g["best_trade"])     if g.get("best_trade")      else None,
            "worst_trade":     float(g["worst_trade"])    if g.get("worst_trade")     else None,
            "ev":              ev,
            "lookback_days":   lookback_days,
            "ticker_stats":    [dict(r) for r in ticker_rows],
            "recent":          [dict(r) for r in recent_rows],
        }
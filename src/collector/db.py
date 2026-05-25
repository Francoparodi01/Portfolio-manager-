"""
src/collector/db.py — Capa de persistencia: TimescaleDB via asyncpg.

Tablas: portfolio_snapshots, positions, market_prices, market_candles, raw_snapshots,
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

from datetime import date, datetime, timedelta, timezone
import json
import logging
from pathlib import Path
import uuid
from typing import Optional
from zoneinfo import ZoneInfo
from src.analysis.decision_engine import directional_return
from src.analysis.fill_reconciliation import ExecutionCandidate, choose_execution_candidate
from src.collector.broker_fills import BrokerFill, serialize_raw_payload
from src.collector.broker_movements import (
    BrokerMovement,
    serialize_raw_payload as serialize_movement_raw_payload,
)
from src.collector.data.models import AssetType, Currency, MarketCandle
from src.core.credentials import CredentialCipher, UserCredentials

try:
    import asyncpg
    HAS_ASYNCPG = True
except ImportError:
    HAS_ASYNCPG = False

logger = logging.getLogger(__name__)

ART_TZ = ZoneInfo("America/Argentina/Buenos_Aires")
CANONICAL_OUTCOME_BASIS = "canonical_cocos"
LEGACY_EXTERNAL_OUTCOME_BASIS = "legacy_external"
MIN_COMPATIBLE_PRICE_RATIO = 0.5
MAX_COMPATIBLE_PRICE_RATIO = 2.0
CEDEAR_MIN_COMPATIBLE_PRICE_RATIO = 0.25
CEDEAR_MAX_COMPATIBLE_PRICE_RATIO = 4.0


SCHEMA_PATH = Path(__file__).resolve().parents[2] / "init.sql"


def _schema_sql() -> str:
    return SCHEMA_PATH.read_text(encoding="utf-8")


def _json_payload(value) -> dict:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {"value": parsed}
        except Exception:
            return {"value": value}
    try:
        return dict(value)
    except Exception:
        return {"value": str(value)}


# ── Migration SQL para decision_log (idempotente) ─────────────────────────────
# Se corre en init_schema() además del DDL base.
# Seguro de correr múltiples veces (IF NOT EXISTS / IF NOT EXISTS).
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

        schema_sql = _schema_sql()
        async with self._pool.acquire() as conn:
            try:
                await conn.execute(schema_sql)
            except Exception:
                logger.exception("Schema init failed while executing init.sql")
                raise

        logger.info("Schema inicializado desde init.sql")

    async def upsert_bot_user_credentials(
        self,
        *,
        chat_id: int,
        credentials: UserCredentials,
        cipher: CredentialCipher,
        telegram_username: Optional[str] = None,
        display_name: Optional[str] = None,
        mfa_timeout: int = 120,
    ) -> None:
        """Store only encrypted Cocos credentials for a Telegram user."""
        if not self._pool:
            raise RuntimeError("Llamar connect() primero")

        encrypted_user, encrypted_pass = cipher.encrypt_credentials(credentials)

        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO bot_users (
                    chat_id,
                    telegram_username,
                    display_name,
                    cocos_user_ciphertext,
                    cocos_pass_ciphertext,
                    mfa_timeout,
                    updated_at
                ) VALUES ($1,$2,$3,$4,$5,$6,NOW())
                ON CONFLICT (chat_id) DO UPDATE SET
                    telegram_username     = EXCLUDED.telegram_username,
                    display_name          = EXCLUDED.display_name,
                    cocos_user_ciphertext = EXCLUDED.cocos_user_ciphertext,
                    cocos_pass_ciphertext = EXCLUDED.cocos_pass_ciphertext,
                    mfa_timeout           = EXCLUDED.mfa_timeout,
                    is_active             = TRUE,
                    updated_at            = NOW()
                """,
                int(chat_id),
                telegram_username,
                display_name,
                encrypted_user,
                encrypted_pass,
                int(mfa_timeout),
            )

    async def get_bot_user_credentials(
        self,
        *,
        chat_id: int,
        cipher: CredentialCipher,
    ) -> Optional[UserCredentials]:
        """Load and decrypt credentials; plaintext legacy columns are ignored."""
        if not self._pool:
            raise RuntimeError("Llamar connect() primero")

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT cocos_user_ciphertext, cocos_pass_ciphertext
                FROM bot_users
                WHERE chat_id = $1
                  AND is_active = TRUE
                """,
                int(chat_id),
            )

        if not row:
            return None
        if not row["cocos_user_ciphertext"] or not row["cocos_pass_ciphertext"]:
            return None

        return cipher.decrypt_credentials(
            row["cocos_user_ciphertext"],
            row["cocos_pass_ciphertext"],
        )

    # ── Snapshot ──────────────────────────────────────────────────────────────

    async def save_snapshot(self, snapshot) -> uuid.UUID:
        if not self._pool:
            raise RuntimeError("Llamar connect() primero")

        sid = snapshot.snapshot_id

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                asset_type_map = await self._market_asset_types_for_tickers(
                    conn,
                    [p.ticker for p in snapshot.positions],
                )

                await conn.execute(
                    """
                    INSERT INTO portfolio_snapshots
                        (snapshot_id, owner_chat_id, scraped_at, total_value_ars, cash_ars,
                         confidence_score, dom_hash, raw_html_hash)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                    ON CONFLICT (snapshot_id) DO UPDATE SET
                        owner_chat_id    = EXCLUDED.owner_chat_id,
                        scraped_at       = EXCLUDED.scraped_at,
                        total_value_ars  = EXCLUDED.total_value_ars,
                        cash_ars         = EXCLUDED.cash_ars,
                        confidence_score = EXCLUDED.confidence_score,
                        dom_hash         = EXCLUDED.dom_hash,
                        raw_html_hash    = EXCLUDED.raw_html_hash
                    """,
                    sid,
                    snapshot.owner_chat_id,
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
                            asset_type_map.get(str(p.ticker).upper(), p.asset_type.value),
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
                    json.dumps(
                        self._snapshot_payload_with_asset_types(snapshot, asset_type_map)
                    ),
                )

        logger.info(f"Snapshot {sid} guardado ({len(snapshot.positions)} posiciones)")
        return sid

    async def _market_asset_types_for_tickers(self, conn, tickers: list[str]) -> dict[str, str]:
        normalized = sorted({
            str(ticker or "").upper()
            for ticker in tickers or []
            if str(ticker or "").strip()
        })
        if not normalized:
            return {}

        rows = await conn.fetch(
            """
            SELECT DISTINCT ON (ticker) ticker, asset_type
            FROM market_prices
            WHERE ticker = ANY($1::text[])
            ORDER BY ticker, ts DESC
            """,
            normalized,
        )
        result: dict[str, str] = {}
        for row in rows:
            item = dict(row)
            ticker = str(item.get("ticker", "") or "").upper()
            asset_type = str(item.get("asset_type", "") or "").upper()
            if ticker and asset_type:
                result[ticker] = asset_type
        return result

    @staticmethod
    def _snapshot_payload_with_asset_types(snapshot, asset_type_map: dict[str, str]) -> dict:
        payload = snapshot.to_dict()
        for position in payload.get("positions", []) or []:
            ticker = str(position.get("ticker", "") or "").upper()
            if ticker in asset_type_map:
                position["asset_type"] = asset_type_map[ticker]
                position["asset_type_source"] = "market_prices"
        return payload

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

    async def save_market_candles(self, candles: list[MarketCandle]) -> int:
        if not self._pool:
            raise RuntimeError("Llamar connect() primero")
        if not candles:
            return 0

        rows = [
            (
                c.ts,
                c.ticker,
                c.long_ticker,
                c.asset_type.value,
                c.currency.value,
                c.venue,
                c.interval,
                c.open_price,
                c.high_price,
                c.low_price,
                c.close_price,
                c.volume,
                c.source,
            )
            for c in candles
        ]

        async with self._pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO market_candles (
                    ts, ticker, long_ticker, asset_type, currency, venue, interval,
                    open_price, high_price, low_price, close_price, volume, source
                ) VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13
                )
                ON CONFLICT (ts, long_ticker, interval) DO UPDATE SET
                    open_price  = EXCLUDED.open_price,
                    high_price  = EXCLUDED.high_price,
                    low_price   = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price,
                    volume      = EXCLUDED.volume,
                    scraped_at  = NOW()
                """,
                rows,
            )
        return len(rows)

    async def build_daily_candles_from_market_prices(
        self,
        business_day: Optional[date] = None,
    ) -> int:
        """
        Reconstruye una vela diaria por activo desde snapshots intradiarios propios.

        Las velas oficiales de Cocos se conservan aparte. La lectura operativa
        decide luego cual usar para cada dia y prioriza COCOS sobre internal_snapshot.
        """
        if not self._pool:
            raise RuntimeError("Llamar connect() primero")

        business_day = business_day or datetime.now(ART_TZ).date()

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                WITH ranked AS (
                    SELECT
                        ticker,
                        asset_type,
                        currency,
                        last_price,
                        COALESCE(volume, 0) AS volume,
                        ROW_NUMBER() OVER (
                            PARTITION BY ticker, asset_type, currency
                            ORDER BY ts ASC
                        ) AS first_rank,
                        ROW_NUMBER() OVER (
                            PARTITION BY ticker, asset_type, currency
                            ORDER BY ts DESC
                        ) AS last_rank
                    FROM market_prices
                    WHERE (ts AT TIME ZONE 'America/Argentina/Buenos_Aires')::date = $1
                      AND last_price IS NOT NULL
                )
                SELECT
                    ticker,
                    asset_type,
                    currency,
                    MAX(last_price) FILTER (WHERE first_rank = 1) AS open_price,
                    MAX(last_price) AS high_price,
                    MIN(last_price) AS low_price,
                    MAX(last_price) FILTER (WHERE last_rank = 1) AS close_price,
                    COALESCE(MAX(volume), 0) AS volume
                FROM ranked
                GROUP BY ticker, asset_type, currency
                ORDER BY ticker
                """,
                business_day,
            )

        candles = [
            MarketCandle(
                ticker=str(row["ticker"]).upper(),
                long_ticker=(
                    "INTERNAL:"
                    f"{str(row['asset_type']).upper()}:"
                    f"{str(row['ticker']).upper()}:"
                    f"{str(row['currency']).upper()}"
                ),
                asset_type=AssetType(str(row["asset_type"]).upper()),
                currency=Currency(str(row["currency"]).upper()),
                venue="BYMA",
                interval="1d",
                ts=datetime(
                    business_day.year,
                    business_day.month,
                    business_day.day,
                    tzinfo=timezone.utc,
                ),
                open_price=float(row["open_price"]),
                high_price=float(row["high_price"]),
                low_price=float(row["low_price"]),
                close_price=float(row["close_price"]),
                volume=float(row["volume"] or 0),
                source="internal_snapshot",
            )
            for row in rows
        ]

        saved = await self.save_market_candles(candles)
        logger.info(
            "Velas internas reconstruidas para %s: %d",
            business_day.isoformat(),
            saved,
        )
        return saved

    async def get_daily_candle_build_status(
        self,
        business_day: Optional[date] = None,
    ) -> dict:
        """Resume cobertura diaria entre snapshots de precio y velas internas."""
        if not self._pool:
            raise RuntimeError("Llamar connect() primero")

        business_day = business_day or datetime.now(ART_TZ).date()
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                WITH price_assets AS (
                    SELECT DISTINCT ticker
                    FROM market_prices
                    WHERE (ts AT TIME ZONE 'America/Argentina/Buenos_Aires')::date = $1
                ),
                candle_assets AS (
                    SELECT DISTINCT ticker
                    FROM market_candles
                    WHERE (ts AT TIME ZONE 'UTC')::date = $1
                      AND source = 'internal_snapshot'
                )
                SELECT
                    (SELECT COUNT(*) FROM price_assets) AS price_assets,
                    (SELECT COUNT(*) FROM candle_assets) AS internal_candles,
                    (
                        SELECT COUNT(*)
                        FROM price_assets p
                        LEFT JOIN candle_assets c USING (ticker)
                        WHERE c.ticker IS NULL
                    ) AS missing_internal
                """,
                business_day,
            )

        return {
            "business_day": business_day,
            "price_assets": int(row["price_assets"] or 0),
            "internal_candles": int(row["internal_candles"] or 0),
            "missing_internal": int(row["missing_internal"] or 0),
        }

    # ── Queries ───────────────────────────────────────────────────────────────

    async def get_latest_snapshot(self, owner_chat_id: Optional[int] = None) -> Optional[dict]:
        if not self._pool:
            return None
        async with self._pool.acquire() as conn:
            if owner_chat_id is None:
                row = await conn.fetchrow(
                    "SELECT payload FROM raw_snapshots ORDER BY scraped_at DESC LIMIT 1"
                )
            else:
                row = await conn.fetchrow(
                    """
                    SELECT r.payload
                    FROM raw_snapshots r
                    JOIN portfolio_snapshots p USING (snapshot_id)
                    WHERE p.owner_chat_id = $1
                    ORDER BY r.scraped_at DESC
                    LIMIT 1
                    """,
                    owner_chat_id,
                )
        return json.loads(row["payload"]) if row else None

    async def get_market_candles(
        self,
        ticker: str,
        *,
        asset_type: Optional[str] = None,
        interval: str = "1d",
        limit: Optional[int] = None,
    ) -> list[dict]:
        if not self._pool:
            return []

        params = [ticker.upper(), interval]
        filters = ["ticker = $1", "interval = $2"]

        if asset_type:
            params.append(asset_type.upper())
            filters.append(f"asset_type = ${len(params)}")

        limit_sql = ""
        if limit is not None:
            params.append(int(limit))
            limit_sql = f"LIMIT ${len(params)}"

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                WITH ranked AS (
                    SELECT
                        ts, ticker, long_ticker, asset_type, currency, venue, interval,
                        open_price, high_price, low_price, close_price, volume, source,
                        ROW_NUMBER() OVER (
                            PARTITION BY (ts AT TIME ZONE 'UTC')::date
                            ORDER BY
                                CASE
                                    WHEN source = 'COCOS' THEN 0
                                    WHEN source = 'TRADINGVIEW_BYMA' THEN 1
                                    WHEN source = 'internal_snapshot' THEN 2
                                    ELSE 3
                                END,
                                scraped_at DESC,
                                ts DESC
                        ) AS source_rank
                    FROM market_candles
                    WHERE {' AND '.join(filters)}
                )
                SELECT
                    ts, ticker, long_ticker, asset_type, currency, venue, interval,
                    open_price, high_price, low_price, close_price, volume, source
                FROM ranked
                WHERE source_rank = 1
                ORDER BY ts DESC
                {limit_sql}
                """,
                *params,
            )

        return [dict(row) for row in reversed(rows)]

    async def get_portfolio_history(
        self,
        limit: int = 60,
        owner_chat_id: Optional[int] = None,
    ) -> list[dict]:
        """
        Retorna snapshots recientes con posiciones incluidas, leídos desde raw_snapshots.
        Devuelve en orden cronológico ascendente (el más antiguo primero).
        """
        if not self._pool:
            return []
        async with self._pool.acquire() as conn:
            if owner_chat_id is None:
                rows = await conn.fetch(
                    """
                    SELECT payload
                    FROM raw_snapshots
                    ORDER BY scraped_at DESC
                    LIMIT $1
                    """,
                    limit,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT r.payload
                    FROM raw_snapshots r
                    JOIN portfolio_snapshots p USING (snapshot_id)
                    WHERE p.owner_chat_id = $1
                    ORDER BY r.scraped_at DESC
                    LIMIT $2
                    """,
                    owner_chat_id,
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
        prices = await self.get_cocos_universe_assets()
        tickers = sorted({
            row["ticker"].upper()
            for row in prices
        })
        logger.info(f"Universo Cocos: {len(tickers)} tickers disponibles")
        return tickers

    async def get_cocos_universe_assets(self) -> list[dict]:
        prices = await self.get_latest_market_prices()
        assets = [
            {
                **row,
                "ticker": row["ticker"].upper(),
                "asset_type": (row.get("asset_type") or "").upper(),
            }
            for row in prices
        ]
        logger.info(f"Universo Cocos tipado: {len(assets)} activos disponibles")
        return assets

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

    async def close_expired_trades(
        self,
        lookback_days: int = 30,
        owner_chat_id: Optional[int] = None,
    ) -> int:
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
                  AND ($2::bigint IS NULL OR owner_chat_id = $2)
                  AND outcome_5d IS NOT NULL
                  AND was_correct IS NULL
                  AND outcome_basis = 'canonical_cocos'
                  AND decision IN ('BUY', 'SELL')
                """,
                cutoff,
                owner_chat_id,
            )

        if not rows:
            return 0

        updated = 0
        now = datetime.now(timezone.utc)

        for r in rows:
            # CONVENTION: SELL returns are positive-up.
            outcome   = float(r["outcome_5d"] or 0.0)
            stop      = float(r["stop_loss_pct"] or -0.08)
            target    = float(r["target_pct"] or 0.16)
            direction = str(r["decision"]).upper()
            decided   = r["decided_at"]
            horizon   = int(r["horizon_days"] or 10)

            if outcome >= target:
                was_correct = True
            elif outcome <= stop:
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

    async def get_equity_curve(
        self,
        lookback_days: int = 90,
        owner_chat_id: Optional[int] = None,
    ) -> list[dict]:
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
                  AND ($2::bigint IS NULL OR owner_chat_id = $2)
                  AND outcome_5d IS NOT NULL
                  AND was_correct IS NOT NULL
                  AND outcome_basis = 'canonical_cocos'
                  AND decision IN ('BUY', 'SELL')
                  AND (
                    (
                      COALESCE(source, layers->>'source') IN ('broker_movement', 'broker_fill')
                      AND status IN ('EXECUTED', 'EXECUTED_MANUAL')
                    )
                    OR (
                      COALESCE(source, layers->>'source') = 'execution_plan'
                      AND status IN ('EXECUTED', 'EXECUTED_MANUAL')
                    )
                  )
                ORDER BY decided_at ASC
                """,
                cutoff,
                owner_chat_id,
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

            # CONVENTION: SELL returns are positive-up.
            trader_return = outcome
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

    async def get_performance_stats_v2(
        self,
        lookback_days: int = 90,
        owner_chat_id: Optional[int] = None,
    ) -> dict:
        await self.close_expired_trades(
            lookback_days=lookback_days,
            owner_chat_id=owner_chat_id,
        )
        stats = await self.get_performance_stats(
            lookback_days=lookback_days,
            owner_chat_id=owner_chat_id,
        )
        curve = await self.get_equity_curve(
            lookback_days=lookback_days,
            owner_chat_id=owner_chat_id,
        )
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

    async def save_broker_fills(self, fills: list[BrokerFill]) -> int:
        if not self._pool:
            raise RuntimeError("Llamar connect() primero")
        if not fills:
            return 0

        rows = [
            (
                fill.source,
                fill.external_fill_id,
                fill.executed_at,
                fill.ticker.upper(),
                fill.side.upper(),
                float(fill.quantity),
                float(fill.avg_fill_price),
                float(fill.gross_amount_ars)
                if fill.gross_amount_ars is not None
                else None,
                float(fill.fees_ars) if fill.fees_ars is not None else None,
                serialize_raw_payload(fill.raw_payload),
            )
            for fill in fills
        ]

        async with self._pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO broker_fills (
                    source,
                    external_fill_id,
                    executed_at,
                    ticker,
                    side,
                    quantity,
                    avg_fill_price,
                    gross_amount_ars,
                    fees_ars,
                    raw_payload
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb)
                ON CONFLICT (source, external_fill_id) DO UPDATE SET
                    executed_at      = EXCLUDED.executed_at,
                    ticker           = EXCLUDED.ticker,
                    side             = EXCLUDED.side,
                    quantity         = EXCLUDED.quantity,
                    avg_fill_price   = EXCLUDED.avg_fill_price,
                    gross_amount_ars = EXCLUDED.gross_amount_ars,
                    fees_ars         = EXCLUDED.fees_ars,
                    raw_payload      = EXCLUDED.raw_payload
                """,
                rows,
            )

        logger.info("%s broker fills guardados", len(rows))
        return len(rows)

    async def save_broker_movements(self, movements: list[BrokerMovement]) -> int:
        if not self._pool:
            raise RuntimeError("Llamar connect() primero")
        if not movements:
            return 0

        rows = [
            (
                movement.source,
                movement.external_movement_id,
                movement.executed_at,
                movement.movement_type,
                movement.currency,
                float(movement.amount) if movement.amount is not None else None,
                float(movement.quantity) if movement.quantity is not None else None,
                float(movement.price) if movement.price is not None else None,
                movement.ticker.upper() if movement.ticker else None,
                movement.instrument_type,
                movement.settlement_date,
                movement.description,
                movement.detail,
                movement.label,
                float(movement.balance) if movement.balance is not None else None,
                serialize_movement_raw_payload(movement.raw_payload),
            )
            for movement in movements
        ]

        async with self._pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO broker_movements (
                    source,
                    external_movement_id,
                    executed_at,
                    movement_type,
                    currency,
                    amount,
                    quantity,
                    price,
                    ticker,
                    instrument_type,
                    settlement_date,
                    description,
                    detail,
                    label,
                    balance,
                    raw_payload
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16::jsonb)
                ON CONFLICT (source, external_movement_id) DO UPDATE SET
                    executed_at     = EXCLUDED.executed_at,
                    movement_type   = EXCLUDED.movement_type,
                    currency        = EXCLUDED.currency,
                    amount          = EXCLUDED.amount,
                    quantity        = EXCLUDED.quantity,
                    price           = EXCLUDED.price,
                    ticker          = EXCLUDED.ticker,
                    instrument_type = EXCLUDED.instrument_type,
                    settlement_date = EXCLUDED.settlement_date,
                    description     = EXCLUDED.description,
                    detail          = EXCLUDED.detail,
                    label           = EXCLUDED.label,
                    balance         = EXCLUDED.balance,
                    raw_payload     = EXCLUDED.raw_payload
                """,
                rows,
            )

        logger.info("%s broker movements guardados", len(rows))
        return len(rows)

    async def reconcile_broker_fills(self, max_age_days: int = 3) -> int:
        if not self._pool:
            raise RuntimeError("Llamar connect() primero")

        async with self._pool.acquire() as conn:
            fill_rows = await conn.fetch(
                """
                SELECT
                    id,
                    source,
                    external_fill_id,
                    executed_at,
                    ticker,
                    side,
                    quantity,
                    avg_fill_price,
                    gross_amount_ars,
                    fees_ars,
                    raw_payload
                FROM broker_fills
                WHERE decision_log_id IS NULL
                ORDER BY executed_at ASC, id ASC
                """
            )

            candidate_rows = await conn.fetch(
                """
                SELECT
                    id,
                    ticker,
                    decision,
                    decided_at,
                    status,
                    theoretical_amount_ars
                FROM decision_log
                WHERE COALESCE(source, layers->>'source') = 'execution_plan'
                  AND COALESCE(status, '') = 'APPROVED'
                ORDER BY decided_at ASC, id ASC
                """
            )

            candidates = [
                ExecutionCandidate(
                    id=int(row["id"]),
                    ticker=str(row["ticker"]),
                    decision=str(row["decision"]),
                    decided_at=row["decided_at"],
                    status=str(row["status"]),
                    theoretical_amount_ars=(
                        float(row["theoretical_amount_ars"])
                        if row["theoretical_amount_ars"] is not None
                        else None
                    ),
                )
                for row in candidate_rows
            ]

            updated = 0
            for row in fill_rows:
                fill = BrokerFill(
                    external_fill_id=str(row["external_fill_id"]),
                    executed_at=row["executed_at"],
                    ticker=str(row["ticker"]),
                    side=str(row["side"]),
                    quantity=float(row["quantity"]),
                    avg_fill_price=float(row["avg_fill_price"]),
                    gross_amount_ars=(
                        float(row["gross_amount_ars"])
                        if row["gross_amount_ars"] is not None
                        else None
                    ),
                    fees_ars=(
                        float(row["fees_ars"])
                        if row["fees_ars"] is not None
                        else None
                    ),
                    source=str(row["source"]),
                    raw_payload=_json_payload(row["raw_payload"]),
                )
                candidate = choose_execution_candidate(
                    fill,
                    candidates,
                    max_age=timedelta(days=max_age_days),
                )
                if candidate is None:
                    continue

                executed_amount = (
                    abs(float(fill.gross_amount_ars))
                    if fill.gross_amount_ars is not None
                    else abs(fill.quantity * fill.avg_fill_price)
                )

                await conn.execute(
                    """
                    UPDATE broker_fills
                    SET decision_log_id = $2,
                        reconciled_at = NOW()
                    WHERE id = $1
                    """,
                    int(row["id"]),
                    candidate.id,
                )

                await conn.execute(
                    """
                    UPDATE decision_log
                    SET status = 'EXECUTED',
                        executed_amount_ars = $2,
                        layers = COALESCE(layers, '{}'::jsonb) || $3::jsonb
                    WHERE id = $1
                    """,
                    candidate.id,
                    float(executed_amount),
                    json.dumps(
                        {
                            "broker_fill": {
                                "source": fill.source,
                                "external_fill_id": fill.external_fill_id,
                                "executed_at": fill.executed_at.isoformat(),
                                "quantity": fill.quantity,
                                "avg_fill_price": fill.avg_fill_price,
                                "gross_amount_ars": executed_amount,
                                "fees_ars": fill.fees_ars,
                            }
                        }
                    ),
                )
                updated += 1
                candidates = [item for item in candidates if item.id != candidate.id]

        logger.info("broker fills reconciliados: %s", updated)
        return updated

    async def materialize_unmatched_broker_fills(self) -> int:
        """
        Link real broker fills that did not match an APPROVED execution plan.

        Unplanned/manual fills are tagged as EXECUTED_MANUAL so outcomes can be
        tracked without pretending the planner approved them.
        """
        if not self._pool:
            raise RuntimeError("Llamar connect() primero")

        async with self._pool.acquire() as conn:
            groups = await conn.fetch(
                """
                SELECT
                    executed_at::date AS fill_date,
                    ticker,
                    side,
                    CASE
                        WHEN source = 'cocos_movements' THEN 'broker_movement'
                        ELSE 'broker_fill'
                    END AS decision_source,
                    ARRAY_AGG(id ORDER BY executed_at, id) AS fill_ids,
                    ARRAY_AGG(external_fill_id ORDER BY executed_at, id) AS external_ids,
                    SUM(quantity) AS quantity,
                    CASE
                        WHEN SUM(quantity) <> 0
                        THEN SUM(quantity * avg_fill_price) / SUM(quantity)
                        ELSE AVG(avg_fill_price)
                    END AS avg_fill_price,
                    SUM(ABS(COALESCE(gross_amount_ars, quantity * avg_fill_price))) AS executed_amount_ars,
                    SUM(COALESCE(fees_ars, 0)) AS fees_ars,
                    MIN(owner_chat_id) AS owner_chat_id
                FROM broker_fills
                WHERE decision_log_id IS NULL
                GROUP BY executed_at::date, ticker, side, decision_source
                ORDER BY fill_date, ticker, side
                """
            )

            linked = 0
            for group in groups:
                fill_date = group["fill_date"]
                ticker = str(group["ticker"]).upper()
                side = str(group["side"]).upper()
                fill_ids = [int(x) for x in group["fill_ids"]]
                external_ids = [str(x) for x in group["external_ids"]]
                executed_amount = float(group["executed_amount_ars"] or 0.0)
                avg_fill_price = float(group["avg_fill_price"] or 0.0)
                quantity = float(group["quantity"] or 0.0)
                owner_chat_id = group["owner_chat_id"]
                decision_source = str(group["decision_source"] or "broker_fill")
                layer_key = (
                    "broker_movement"
                    if decision_source == "broker_movement"
                    else "broker_fill"
                )

                layer_patch = {
                    layer_key: {
                        "reconciliation_mode": "manual_or_unplanned",
                        "external_fill_ids": external_ids,
                        "fill_date": fill_date.isoformat(),
                        "quantity": quantity,
                        "avg_fill_price": avg_fill_price,
                        "gross_amount_ars": executed_amount,
                        "fees_ars": float(group["fees_ars"] or 0.0),
                    }
                }

                decision_id = await conn.fetchval(
                    """
                    SELECT id
                    FROM decision_log
                    WHERE decision_date = $1
                      AND ticker = $2
                      AND (
                          decision = $3
                          OR ($3 = 'SELL' AND decision IN ('SELL_PARTIAL', 'SELL_FULL'))
                      )
                      AND COALESCE(source, '') = $5
                      AND COALESCE(decision_type, '') = $5
                      AND COALESCE(owner_chat_id, 0) = COALESCE($4::bigint, 0)
                    ORDER BY id ASC
                    LIMIT 1
                    """,
                    fill_date,
                    ticker,
                    side,
                    owner_chat_id,
                    decision_source,
                )

                if decision_id is None:
                    decision_id = await conn.fetchval(
                        """
                        INSERT INTO decision_log (
                            owner_chat_id,
                            decided_at,
                            ticker,
                            decision,
                            final_score,
                            confidence,
                            layers,
                            price_at_decision,
                            horizon_days,
                            decision_type,
                            source,
                            status,
                            theoretical_amount_ars,
                            executed_amount_ars,
                            is_executable,
                            was_blocked
                        )
                        VALUES (
                            $1,
                            ($2::date::timestamp + TIME '15:00') AT TIME ZONE 'America/Argentina/Buenos_Aires',
                            $3,
                            $4,
                            0.0,
                            1.0,
                            $5::jsonb,
                            $6,
                            20,
                            $8,
                            $8,
                            'EXECUTED_MANUAL',
                            $7,
                            $7,
                            TRUE,
                            FALSE
                        )
                        ON CONFLICT DO NOTHING
                        RETURNING id
                        """,
                        owner_chat_id,
                        fill_date,
                        ticker,
                        side,
                        json.dumps(layer_patch),
                        avg_fill_price,
                        executed_amount,
                        decision_source,
                    )

                    if decision_id is None:
                        decision_id = await conn.fetchval(
                            """
                            SELECT id
                            FROM decision_log
                            WHERE decision_date = $1
                              AND ticker = $2
                              AND decision = $3
                              AND COALESCE(source, '') = $5
                              AND COALESCE(decision_type, '') = $5
                              AND COALESCE(owner_chat_id, 0) = COALESCE($4::bigint, 0)
                            ORDER BY id ASC
                            LIMIT 1
                            """,
                            fill_date,
                            ticker,
                            side,
                            owner_chat_id,
                            decision_source,
                        )

                if decision_id is None:
                    continue

                await conn.execute(
                    """
                    UPDATE decision_log
                    SET status = 'EXECUTED_MANUAL',
                        executed_amount_ars = $2,
                        theoretical_amount_ars = COALESCE(theoretical_amount_ars, $2),
                        price_at_decision = $3,
                        source = $5,
                        decision_type = $5,
                        is_executable = TRUE,
                        was_blocked = FALSE,
                        layers = COALESCE(layers, '{}'::jsonb) || $4::jsonb
                    WHERE id = $1
                    """,
                    int(decision_id),
                    executed_amount,
                    avg_fill_price,
                    json.dumps(layer_patch),
                    decision_source,
                )

                await conn.execute(
                    """
                    UPDATE broker_fills
                    SET decision_log_id = $2,
                        reconciled_at = NOW()
                    WHERE id = ANY($1::bigint[])
                    """,
                    fill_ids,
                    int(decision_id),
                )
                linked += len(fill_ids)

        logger.info("broker fills materializados como decision_log: %s", linked)
        return linked

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

    async def _compute_directional_outcomes(
        self,
        *,
        entry_price: float,
        decided_at: datetime,
        direction: str,
        now: datetime,
        candles: list[dict],
    ) -> dict[str, float]:
        outcomes: dict[str, float] = {}
        decided_day = decided_at.astimezone(ART_TZ).date()

        for horizon, col in [
            (5, "outcome_5d"),
            (10, "outcome_10d"),
            (20, "outcome_20d"),
        ]:
            target_day = decided_day + timedelta(days=horizon)
            if target_day > now.astimezone(ART_TZ).date():
                continue
            eligible = [
                candle for candle in candles
                if candle["ts"].date() >= target_day
            ]
            if not eligible:
                continue
            price_at_horizon = float(eligible[0]["close_price"])
            # CONVENTION: SELL returns are positive-up.
            outcomes[col] = directional_return(
                entry_price,
                price_at_horizon,
                direction,
            )

        return outcomes

    def _assess_outcome_basis(
        self,
        *,
        entry_price: float,
        decided_at: datetime,
        candles: list[dict],
    ) -> tuple[str, Optional[float]]:
        """
        Decide whether decision_log and market_candles use the same price basis.

        Current production candles are Cocos/BYMA prices. Some historical rows
        were persisted with legacy external prices in another unit; those rows
        stay traceable, but must not feed canonical metrics.
        """
        decided_day = decided_at.astimezone(ART_TZ).date()
        eligible = [
            candle
            for candle in candles
            if candle["ts"].date() >= decided_day and candle.get("close_price") is not None
        ]
        if not eligible or entry_price <= 0:
            return LEGACY_EXTERNAL_OUTCOME_BASIS, None

        reference_price = float(eligible[0]["close_price"])
        ratio = reference_price / float(entry_price)
        asset_type = str(eligible[0].get("asset_type") or "").upper()
        min_ratio = MIN_COMPATIBLE_PRICE_RATIO
        max_ratio = MAX_COMPATIBLE_PRICE_RATIO
        if asset_type == "CEDEAR":
            min_ratio = CEDEAR_MIN_COMPATIBLE_PRICE_RATIO
            max_ratio = CEDEAR_MAX_COMPATIBLE_PRICE_RATIO

        if min_ratio <= ratio <= max_ratio:
            return CANONICAL_OUTCOME_BASIS, ratio

        return LEGACY_EXTERNAL_OUTCOME_BASIS, ratio

    async def update_outcomes(
        self,
        lookback_days: int = 30,
        owner_chat_id: Optional[int] = None,
    ) -> int:
        """
        Busca decisiones sin outcome donde han pasado >=5 días y llena
        outcome_5d / outcome_10d / outcome_20d / was_correct usando la serie
        canonica de market_candles.

        price_at_decision y market_candles usan la misma unidad operativa
        proveniente de Cocos, por lo que no se aplica guardia USD/ARS.
        """
        if not self._pool:
            raise RuntimeError("Llamar connect() primero")

        try:
            maturity_cutoff = datetime.now(timezone.utc) - timedelta(days=5)
            lookback_cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)

            async with self._pool.acquire() as conn:
                if owner_chat_id is None:
                    rows = await conn.fetch(
                        """
                        SELECT id, ticker, price_at_decision, decided_at, decision
                        FROM decision_log
                        WHERE outcome_5d IS NULL
                          AND COALESCE(outcome_basis, '') <> 'legacy_external'
                          AND price_at_decision IS NOT NULL
                          AND price_at_decision > 0
                          AND decided_at <= $1
                          AND decided_at >= $2
                          AND decision != 'HOLD'
                        ORDER BY decided_at DESC
                        LIMIT 200
                        """,
                        maturity_cutoff,
                        lookback_cutoff,
                    )
                else:
                    rows = await conn.fetch(
                        """
                        SELECT id, ticker, price_at_decision, decided_at, decision
                        FROM decision_log
                        WHERE outcome_5d IS NULL
                          AND owner_chat_id = $3
                          AND COALESCE(outcome_basis, '') <> 'legacy_external'
                          AND price_at_decision IS NOT NULL
                          AND price_at_decision > 0
                          AND decided_at <= $1
                          AND decided_at >= $2
                          AND decision != 'HOLD'
                        ORDER BY decided_at DESC
                        LIMIT 200
                        """,
                        maturity_cutoff,
                        lookback_cutoff,
                        owner_chat_id,
                    )

            if not rows:
                logger.info("update_outcomes: sin decisiones pendientes")
                return 0

            updated = 0
            now     = datetime.now(timezone.utc)

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

                candles = await self.get_market_candles(ticker, limit=260)
                if not candles:
                    async with self._pool.acquire() as conn:
                        await conn.execute(
                            """
                            UPDATE decision_log SET
                                outcome_basis       = $2,
                                outcome_basis_ratio = NULL
                            WHERE id = $1
                            """,
                            row["id"],
                            LEGACY_EXTERNAL_OUTCOME_BASIS,
                        )
                    logger.warning(
                        "update_outcomes SKIP %s id=%s: sin velas canonicas",
                        ticker,
                        row["id"],
                    )
                    continue

                outcome_basis, basis_ratio = self._assess_outcome_basis(
                    entry_price=entry_f,
                    decided_at=decided_at,
                    candles=candles,
                )
                if outcome_basis != CANONICAL_OUTCOME_BASIS:
                    async with self._pool.acquire() as conn:
                        await conn.execute(
                            """
                            UPDATE decision_log SET
                                outcome_basis       = $2,
                                outcome_basis_ratio = $3
                            WHERE id = $1
                            """,
                            row["id"],
                            outcome_basis,
                            basis_ratio,
                        )
                    logger.warning(
                        "update_outcomes SKIP %s id=%s: basis=%s ratio=%s",
                        ticker,
                        row["id"],
                        outcome_basis,
                        basis_ratio,
                    )
                    continue

                outcomes = await self._compute_directional_outcomes(
                    entry_price=entry_f,
                    decided_at=decided_at,
                    direction=direction,
                    now=now,
                    candles=candles,
                )

                if not outcomes:
                    continue

                primary = outcomes.get("outcome_5d", outcomes.get("outcome_10d"))
                was_correct = primary > 0 if primary is not None else None

                try:
                    async with self._pool.acquire() as conn:
                        await conn.execute(
                            """
                            UPDATE decision_log SET
                                outcome_5d        = COALESCE($2, outcome_5d),
                                outcome_10d       = COALESCE($3, outcome_10d),
                                outcome_20d       = COALESCE($4, outcome_20d),
                                was_correct       = COALESCE($5, was_correct),
                                outcome_filled_at = NOW(),
                                outcome_basis       = $6,
                                outcome_basis_ratio = $7
                            WHERE id = $1
                            """,
                            row["id"],
                            outcomes.get("outcome_5d"),
                            outcomes.get("outcome_10d"),
                            outcomes.get("outcome_20d"),
                            was_correct,
                            outcome_basis,
                            basis_ratio,
                        )
                    updated += 1
                    logger.debug(f"outcome actualizado: {ticker} id={row['id']} {outcomes}")
                except Exception as e:
                    logger.warning(f"update_outcomes write error {ticker}: {e}")

            logger.info(f"update_outcomes: {updated}/{len(rows)} decisiones actualizadas")
            return updated

        except Exception as e:
            logger.error(f"update_outcomes: {e}", exc_info=True)
            return 0

    async def recompute_outcomes(self, lookback_days: Optional[int] = None) -> int:
        """
        Recalcula outcomes ya persistidos desde la serie canónica de market_candles.

        Se usa para migraciones de convención o backfills de historia. A diferencia
        de update_outcomes(), sobrescribe valores existentes para dejar toda la
        muestra bajo las mismas reglas actuales.
        """
        if not self._pool:
            raise RuntimeError("Llamar connect() primero")

        maturity_cutoff = datetime.now(timezone.utc) - timedelta(days=5)
        lookback_cutoff = (
            datetime.now(timezone.utc) - timedelta(days=lookback_days)
            if lookback_days is not None
            else datetime(1970, 1, 1, tzinfo=timezone.utc)
        )

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, ticker, price_at_decision, decided_at, decision
                FROM decision_log
                WHERE decided_at <= $1
                  AND decided_at >= $2
                  AND decision IN ('BUY', 'SELL')
                ORDER BY decided_at ASC
                """,
                maturity_cutoff,
                lookback_cutoff,
            )

        if not rows:
            logger.info("recompute_outcomes: sin decisiones elegibles")
            return 0

        updated = 0
        now = datetime.now(timezone.utc)

        for row in rows:
            ticker = str(row["ticker"]).upper()
            entry = row["price_at_decision"]
            decided_at = row["decided_at"]
            direction = str(row["decision"]).upper()

            if not entry or float(entry) <= 0:
                logger.debug(
                    "recompute_outcomes SKIP %s id=%s: sin precio de entrada",
                    ticker,
                    row["id"],
                )
                continue

            candles = await self.get_market_candles(ticker, limit=260)
            if not candles:
                async with self._pool.acquire() as conn:
                    await conn.execute(
                        """
                        UPDATE decision_log SET
                            outcome_5d          = NULL,
                            outcome_10d         = NULL,
                            outcome_20d         = NULL,
                            was_correct         = NULL,
                            outcome_filled_at   = NULL,
                            outcome_basis       = $2,
                            outcome_basis_ratio = NULL
                        WHERE id = $1
                        """,
                        row["id"],
                        LEGACY_EXTERNAL_OUTCOME_BASIS,
                    )
                logger.warning(
                    "recompute_outcomes CLEAR %s id=%s: sin velas canonicas",
                    ticker,
                    row["id"],
                )
                continue

            outcome_basis, basis_ratio = self._assess_outcome_basis(
                entry_price=float(entry),
                decided_at=decided_at,
                candles=candles,
            )
            if outcome_basis != CANONICAL_OUTCOME_BASIS:
                async with self._pool.acquire() as conn:
                    await conn.execute(
                        """
                        UPDATE decision_log SET
                            outcome_5d          = NULL,
                            outcome_10d         = NULL,
                            outcome_20d         = NULL,
                            was_correct         = NULL,
                            outcome_filled_at   = NULL,
                            outcome_basis       = $2,
                            outcome_basis_ratio = $3
                        WHERE id = $1
                        """,
                        row["id"],
                        outcome_basis,
                        basis_ratio,
                    )
                logger.warning(
                    "recompute_outcomes CLEAR %s id=%s: basis=%s ratio=%s",
                    ticker,
                    row["id"],
                    outcome_basis,
                    basis_ratio,
                )
                continue

            outcomes = await self._compute_directional_outcomes(
                entry_price=float(entry),
                decided_at=decided_at,
                direction=direction,
                now=now,
                candles=candles,
            )
            if not outcomes:
                continue

            primary = outcomes.get("outcome_5d", outcomes.get("outcome_10d"))
            was_correct = primary > 0 if primary is not None else None

            try:
                async with self._pool.acquire() as conn:
                    await conn.execute(
                        """
                        UPDATE decision_log SET
                            outcome_5d        = $2,
                            outcome_10d       = $3,
                            outcome_20d       = $4,
                            was_correct       = $5,
                            outcome_filled_at = NOW(),
                            outcome_basis       = $6,
                            outcome_basis_ratio = $7
                        WHERE id = $1
                        """,
                        row["id"],
                        outcomes.get("outcome_5d"),
                        outcomes.get("outcome_10d"),
                        outcomes.get("outcome_20d"),
                        was_correct,
                        outcome_basis,
                        basis_ratio,
                    )
                updated += 1
            except Exception as e:
                logger.warning("recompute_outcomes write error %s: %s", ticker, e)

        logger.info("recompute_outcomes: %s/%s decisiones recalculadas", updated, len(rows))
        return updated

    async def get_performance_stats(
        self,
        lookback_days: int = 90,
        owner_chat_id: Optional[int] = None,
    ) -> dict:
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
                    was_correct, size_pct,
                    COALESCE(source, layers->>'source', 'sin_source') AS source,
                    COALESCE(status, 'UNKNOWN') AS status,
                    COALESCE(decision_type, 'unknown') AS decision_type
                FROM decision_log
                WHERE decided_at >= $1
                  AND ($2::bigint IS NULL OR owner_chat_id = $2)
                  AND outcome_5d IS NOT NULL
                  AND was_correct IS NOT NULL
                  AND outcome_basis = 'canonical_cocos'
                  AND decision IN ('BUY', 'SELL')
                  AND (
                    (
                      COALESCE(source, layers->>'source') IN ('broker_movement', 'broker_fill')
                      AND status IN ('EXECUTED', 'EXECUTED_MANUAL')
                    )
                    OR (
                      COALESCE(source, layers->>'source') = 'execution_plan'
                      AND status IN ('EXECUTED', 'EXECUTED_MANUAL')
                    )
                  )
                ORDER BY decided_at ASC
                """,
                cutoff,
                owner_chat_id,
            )

            pending_count = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM decision_log
                WHERE outcome_5d IS NULL
                  AND ($2::bigint IS NULL OR owner_chat_id = $2)
                  AND COALESCE(outcome_basis, '') <> 'legacy_external'
                  AND price_at_decision IS NOT NULL
                  AND price_at_decision > 0
                  AND decision IN ('BUY', 'SELL')
                  AND decided_at >= $1
                  AND (
                    (
                      COALESCE(source, layers->>'source') IN ('broker_movement', 'broker_fill')
                      AND status IN ('EXECUTED', 'EXECUTED_MANUAL')
                    )
                    OR (
                      COALESCE(source, layers->>'source') = 'execution_plan'
                      AND status IN ('EXECUTED', 'EXECUTED_MANUAL')
                    )
                  )
                """,
                cutoff,
                owner_chat_id,
            )

            pending_all_count = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM decision_log
                WHERE outcome_5d IS NULL
                  AND ($2::bigint IS NULL OR owner_chat_id = $2)
                  AND COALESCE(outcome_basis, '') <> 'legacy_external'
                  AND price_at_decision IS NOT NULL
                  AND price_at_decision > 0
                  AND decision IN ('BUY', 'SELL')
                  AND decided_at >= $1
                """,
                cutoff,
                owner_chat_id,
            )

            recent_rows = await conn.fetch(
                """
                SELECT
                    dl.ticker,
                    dl.decision,
                    dl.final_score,
                    dl.confidence,
                    dl.outcome_5d,
                    dl.was_correct,
                    dl.decided_at,
                    dl.size_pct,
                    dl.stop_loss_pct,
                    dl.target_pct,
                    dl.decision_type,
                    dl.source,
                    dl.status,
                    dl.block_reason,
                    dl.layers,
                    rb.decided_at AS recent_buy_at,
                    rb.price_at_decision AS recent_buy_price,
                    rb.executed_amount_ars AS recent_buy_amount
                FROM decision_log dl
                LEFT JOIN LATERAL (
                    SELECT decided_at, price_at_decision, executed_amount_ars
                    FROM decision_log bm
                    WHERE bm.ticker = dl.ticker
                      AND bm.decision = 'BUY'
                      AND COALESCE(bm.source, bm.layers->>'source') IN ('broker_movement', 'broker_fill')
                      AND bm.status IN ('EXECUTED', 'EXECUTED_MANUAL')
                      AND bm.decided_at < dl.decided_at
                      AND bm.decided_at >= dl.decided_at - INTERVAL '10 days'
                    ORDER BY bm.decided_at DESC
                    LIMIT 1
                ) rb ON TRUE
                WHERE dl.decision IN ('BUY', 'SELL')
                  AND ($1::bigint IS NULL OR dl.owner_chat_id = $1)
                  AND COALESCE(dl.outcome_basis, '') <> 'legacy_external'
                ORDER BY dl.decided_at DESC
                LIMIT 8
                """,
                owner_chat_id,
            )

        # ── Calcular retorno del trader con signo correcto ────────────────────
        # CONVENTION: SELL returns are positive-up.
        # outcome_* ya se persiste como retorno direccional canonico.
        trader_returns = []
        by_ticker: dict = {}
        by_source: dict = {}
        ret_10d_list    = []
        ret_20d_list    = []

        for r in raw_rows:
            direction  = str(r["decision"]).upper()
            out5       = float(r["outcome_5d"] or 0.0)
            out10      = float(r["outcome_10d"]) if r["outcome_10d"] is not None else None
            out20      = float(r["outcome_20d"]) if r["outcome_20d"] is not None else None

            trader_ret   = out5
            trader_ret10 = out10
            trader_ret20 = out20

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

            source_key = (
                str(r["source"] or "sin_source"),
                str(r["status"] or "UNKNOWN"),
                str(r["decision_type"] or "unknown"),
            )
            if source_key not in by_source:
                by_source[source_key] = {"events": 0, "wins": 0, "sum_return": 0.0}
            by_source[source_key]["events"] += 1
            by_source[source_key]["sum_return"] += trader_ret
            if trader_ret > 0:
                by_source[source_key]["wins"] += 1

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

        source_stats = []
        for (source, status, decision_type), values in sorted(
            by_source.items(),
            key=lambda item: (-item[1]["events"], item[0]),
        ):
            events = int(values["events"])
            source_stats.append({
                "source": source,
                "status": status,
                "decision_type": decision_type,
                "events": events,
                "wins": int(values["wins"]),
                "avg_return": values["sum_return"] / events if events else None,
            })

        return {
            "total_trades":    n,
            "winners":         n_wins,
            "losers":          n_losses,
            "pending":         int(pending_count or 0),
            "pending_all":     int(pending_all_count or 0),
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
            "source_stats":    source_stats,
            "recent":          [dict(r) for r in recent_rows],
        }

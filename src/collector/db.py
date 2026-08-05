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

from datetime import date, datetime, time, timedelta, timezone
import json
import logging
from pathlib import Path
import uuid
from typing import Any, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo
from src.analysis.audit_scope import (
    classify_decision_audit_scope,
    ensure_decision_audit_scope_columns,
)
from src.analysis.corporate_actions import (
    CORPORATE_ACTIONS_SCHEMA_SQL,
    CorporateActionApplication,
    CorporateActionEffect,
    MIN_ANOMALY_RETURN,
    PriceQualityFlag,
    corporate_action_effect_from_row,
    normalize_candle_rows,
    rebase_reference_price,
)
from src.analysis.issuer_events import (
    ISSUER_EVENTS_SCHEMA_SQL,
    IssuerEventObservation,
    IssuerInstrument,
    IssuerRegistryEntry,
)
from src.analysis.decision_context import build_decision_run_context
from src.analysis.decision_engine import directional_return
from src.analysis.fill_reconciliation import ExecutionCandidate, choose_execution_candidate
from src.analysis.manual_market_events import (
    MANUAL_MARKET_EVENTS_SCHEMA_SQL,
    ManualMarketEvent,
    manual_market_event_from_row,
    normalize_action_policy,
    normalize_event_time_hint,
    normalize_severity,
)
from src.analysis.preclose_alerts import PRE_CLOSE_ALERTS_SCHEMA_SQL, PrecloseAlert
from src.collector.broker_fills import BrokerFill, serialize_raw_payload
from src.collector.broker_movements import (
    BrokerMovement,
    serialize_raw_payload as serialize_movement_raw_payload,
)
from src.collector.data.models import AssetType, Currency, MarketCandle
from src.collector.schema_migrations import (
    EXECUTION_TIMESTAMP_META_SQL,
    OUTCOME_HORIZON_SQL,
)
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
MARKET_FRESHNESS_MIN_TICKERS = 50
MANUAL_DECISION_STRATEGY_ID = "manual"
NO_MANUAL_DECISION_COMPONENT_VERSION = "none"
SUPERSEDED_BROKER_FILL_REASON = "cocos_ticket_replaced_provisional_movement"


SCHEMA_PATH = Path(__file__).resolve().parents[2] / "init.sql"


def _schema_sql() -> str:
    return SCHEMA_PATH.read_text(encoding="utf-8")


def _manual_broker_decided_at(fill_date: date) -> datetime:
    return datetime.combine(fill_date, time(15, 0), tzinfo=ART_TZ)


def _manual_broker_run_id(
    *,
    decision_source: str,
    fill_date: date,
    ticker: str,
    side: str,
    owner_chat_id: int | None,
    external_ids: list[str],
) -> str:
    run_key = "|".join(
        [
            decision_source,
            str(owner_chat_id or ""),
            fill_date.isoformat(),
            ticker,
            side,
            *external_ids,
        ]
    )
    return f"manual:{uuid.uuid5(uuid.NAMESPACE_URL, run_key)}"


def _manual_broker_layer_patch(
    *,
    decision_source: str,
    fill_date: date,
    ticker: str,
    side: str,
    owner_chat_id: int | None,
    external_ids: list[str],
    quantity: float,
    avg_fill_price: float,
    executed_amount: float,
    fees_ars: float,
) -> dict[str, Any]:
    layer_key = "broker_movement" if decision_source == "broker_movement" else "broker_fill"
    run_context = build_decision_run_context(
        _manual_broker_run_id(
            decision_source=decision_source,
            fill_date=fill_date,
            ticker=ticker,
            side=side,
            owner_chat_id=owner_chat_id,
            external_ids=external_ids,
        ),
        strategy_id=MANUAL_DECISION_STRATEGY_ID,
        planner_version=NO_MANUAL_DECISION_COMPONENT_VERSION,
        optimizer_version=NO_MANUAL_DECISION_COMPONENT_VERSION,
        model_version=NO_MANUAL_DECISION_COMPONENT_VERSION,
        prompt_version=NO_MANUAL_DECISION_COMPONENT_VERSION,
        decided_at=_manual_broker_decided_at(fill_date),
    ).to_dict()
    return {
        "run_context": run_context,
        layer_key: {
            "reconciliation_mode": "manual_or_unplanned",
            "external_fill_ids": external_ids,
            "fill_date": fill_date.isoformat(),
            "quantity": quantity,
            "avg_fill_price": avg_fill_price,
            "gross_amount_ars": executed_amount,
            "fees_ars": fees_ars,
        },
    }


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


async def _mark_superseded_broker_fills_for_real(conn, source: str, external_fill_id: str) -> list[int]:
    rows = await conn.fetch(
        """
        WITH real_fill AS (
            SELECT id, source, external_fill_id, executed_at, ticker, side, raw_payload
            FROM broker_fills
            WHERE source = $1
              AND external_fill_id = $2
              AND external_fill_id NOT LIKE 'synthetic:%'
              AND COALESCE(raw_payload->>'id_ticket', '') <> ''
        )
        UPDATE broker_fills synthetic
        SET raw_payload = COALESCE(synthetic.raw_payload, '{}'::jsonb)
            || jsonb_build_object(
                'superseded_by_real',
                jsonb_build_object(
                    'fill_id', real_fill.id,
                    'external_fill_id', real_fill.external_fill_id,
                    'reason', $3::text
                )
            )
        FROM real_fill
        WHERE synthetic.source = real_fill.source
          AND synthetic.id <> real_fill.id
          AND synthetic.external_fill_id LIKE 'synthetic:%'
          AND NOT (COALESCE(synthetic.raw_payload, '{}'::jsonb) ? 'superseded_by_real')
          AND synthetic.executed_at::date = real_fill.executed_at::date
          AND synthetic.ticker = real_fill.ticker
          AND synthetic.side = real_fill.side
          AND COALESCE(synthetic.raw_payload->>'id_instrument', '') = COALESCE(real_fill.raw_payload->>'id_instrument', '')
          AND COALESCE(synthetic.raw_payload->>'settlement_date', '') = COALESCE(real_fill.raw_payload->>'settlement_date', '')
          AND COALESCE(synthetic.raw_payload->>'label', '') = COALESCE(real_fill.raw_payload->>'label', '')
          AND COALESCE(synthetic.raw_payload->>'id_currency', '') = COALESCE(real_fill.raw_payload->>'id_currency', '')
          AND COALESCE(synthetic.raw_payload->>'id_ticket', '') = ''
          AND COALESCE(synthetic.raw_payload->>'description', '') = ''
          AND COALESCE(synthetic.raw_payload->>'has_ticket_pdf', 'false') IN ('false', 'False', '0', '')
        RETURNING synthetic.id
        """,
        source,
        external_fill_id,
        SUPERSEDED_BROKER_FILL_REASON,
    )
    return [int(row["id"]) for row in rows]


async def _mark_synthetic_broker_fill_if_real_exists(conn, source: str, external_fill_id: str) -> list[int]:
    rows = await conn.fetch(
        """
        WITH synthetic_fill AS (
            SELECT id, source, external_fill_id, executed_at, ticker, side, raw_payload
            FROM broker_fills
            WHERE source = $1
              AND external_fill_id = $2
              AND external_fill_id LIKE 'synthetic:%'
              AND NOT (COALESCE(raw_payload, '{}'::jsonb) ? 'superseded_by_real')
              AND COALESCE(raw_payload->>'id_ticket', '') = ''
              AND COALESCE(raw_payload->>'description', '') = ''
              AND COALESCE(raw_payload->>'has_ticket_pdf', 'false') IN ('false', 'False', '0', '')
        ),
        real_fill AS (
            SELECT real.id, real.external_fill_id
            FROM broker_fills real
            JOIN synthetic_fill synthetic
              ON real.source = synthetic.source
             AND real.external_fill_id NOT LIKE 'synthetic:%'
             AND real.executed_at::date = synthetic.executed_at::date
             AND real.ticker = synthetic.ticker
             AND real.side = synthetic.side
             AND COALESCE(real.raw_payload->>'id_ticket', '') <> ''
             AND COALESCE(real.raw_payload->>'id_instrument', '') = COALESCE(synthetic.raw_payload->>'id_instrument', '')
             AND COALESCE(real.raw_payload->>'settlement_date', '') = COALESCE(synthetic.raw_payload->>'settlement_date', '')
             AND COALESCE(real.raw_payload->>'label', '') = COALESCE(synthetic.raw_payload->>'label', '')
             AND COALESCE(real.raw_payload->>'id_currency', '') = COALESCE(synthetic.raw_payload->>'id_currency', '')
            ORDER BY real.id ASC
            LIMIT 1
        )
        UPDATE broker_fills synthetic
        SET raw_payload = COALESCE(synthetic.raw_payload, '{}'::jsonb)
            || jsonb_build_object(
                'superseded_by_real',
                jsonb_build_object(
                    'fill_id', real_fill.id,
                    'external_fill_id', real_fill.external_fill_id,
                    'reason', $3::text
                )
            )
        FROM synthetic_fill, real_fill
        WHERE synthetic.id = synthetic_fill.id
        RETURNING synthetic.id
        """,
        source,
        external_fill_id,
        SUPERSEDED_BROKER_FILL_REASON,
    )
    return [int(row["id"]) for row in rows]


async def _mark_superseded_broker_fills_for_saved_rows(conn, rows: list[tuple]) -> int:
    marked: set[int] = set()
    for row in rows:
        source = str(row[0])
        external_fill_id = str(row[1])
        if external_fill_id.startswith("synthetic:"):
            marked.update(
                await _mark_synthetic_broker_fill_if_real_exists(
                    conn,
                    source,
                    external_fill_id,
                )
            )
        else:
            marked.update(
                await _mark_superseded_broker_fills_for_real(
                    conn,
                    source,
                    external_fill_id,
                )
            )
    return len(marked)


# ── Migration SQL para decision_log (idempotente) ─────────────────────────────
# Se corre en init_schema() además del DDL base.
# Seguro de correr múltiples veces (IF NOT EXISTS / IF NOT EXISTS).
class PortfolioDatabase:
    def __init__(self, dsn: str):
        self._dsn = dsn.replace("postgresql+asyncpg://", "postgresql://")
        self._pool: Optional[asyncpg.Pool] = None
        self._execution_timestamp_meta_ready = False
        self._decision_audit_scope_ready = False
        self._manual_market_events_ready = False
        self._corporate_actions_ready = False
        self._issuer_events_ready = False
        self._preclose_alerts_ready = False
        self._outcome_horizon_ready = False

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

    async def _ensure_execution_timestamp_meta_columns(self, conn) -> None:
        if self._execution_timestamp_meta_ready:
            return
        await conn.execute(EXECUTION_TIMESTAMP_META_SQL)
        self._execution_timestamp_meta_ready = True

    async def _ensure_decision_audit_scope_columns(self, conn) -> None:
        if self._decision_audit_scope_ready:
            return
        await ensure_decision_audit_scope_columns(conn)
        self._decision_audit_scope_ready = True

    async def _ensure_manual_market_events_schema(self, conn) -> None:
        if self._manual_market_events_ready:
            return
        await conn.execute(MANUAL_MARKET_EVENTS_SCHEMA_SQL)
        self._manual_market_events_ready = True

    async def ensure_manual_market_events_schema(self) -> None:
        if not self._pool:
            raise RuntimeError("Llamar connect() primero")
        async with self._pool.acquire() as conn:
            await self._ensure_manual_market_events_schema(conn)

    async def _ensure_corporate_actions_schema(self, conn) -> None:
        if self._corporate_actions_ready:
            return
        await conn.execute(CORPORATE_ACTIONS_SCHEMA_SQL)
        self._corporate_actions_ready = True

    async def ensure_corporate_actions_schema(self) -> None:
        if not self._pool:
            raise RuntimeError("Llamar connect() primero")
        async with self._pool.acquire() as conn:
            await self._ensure_corporate_actions_schema(conn)

    async def _ensure_issuer_events_schema(self, conn) -> None:
        if self._issuer_events_ready:
            return
        await conn.execute(ISSUER_EVENTS_SCHEMA_SQL)
        self._issuer_events_ready = True

    async def ensure_issuer_events_schema(self) -> None:
        if not self._pool:
            raise RuntimeError("Llamar connect() primero")
        async with self._pool.acquire() as conn:
            await self._ensure_issuer_events_schema(conn)

    async def _ensure_preclose_alerts_schema(self, conn) -> None:
        if self._preclose_alerts_ready:
            return
        await conn.execute(PRE_CLOSE_ALERTS_SCHEMA_SQL)
        self._preclose_alerts_ready = True

    async def _ensure_outcome_horizon_columns(self, conn) -> None:
        if self._outcome_horizon_ready:
            return
        await conn.execute(OUTCOME_HORIZON_SQL)
        self._outcome_horizon_ready = True

    async def save_preclose_alerts(
        self,
        alerts: list[PrecloseAlert],
        *,
        alert_ts: datetime,
        slot: str,
    ) -> int:
        if not self._pool or not alerts:
            return 0
        business_date = alert_ts.astimezone(ART_TZ).date()
        saved = 0
        async with self._pool.acquire() as conn:
            await self._ensure_preclose_alerts_schema(conn)
            for alert in alerts:
                record = alert.to_record(
                    alert_ts=alert_ts,
                    business_date=business_date,
                    slot=slot,
                )
                status = await conn.execute(
                    """
                    INSERT INTO intraday_preclose_alerts (
                        alert_ts, business_date, slot, ticker, alert_type, severity,
                        current_price, reference_price, change_pct, current_weight,
                        reason, evidence
                    )
                    VALUES (
                        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb
                    )
                    ON CONFLICT (business_date, slot, ticker, alert_type) DO UPDATE SET
                        alert_ts = EXCLUDED.alert_ts,
                        severity = EXCLUDED.severity,
                        current_price = EXCLUDED.current_price,
                        reference_price = EXCLUDED.reference_price,
                        change_pct = EXCLUDED.change_pct,
                        current_weight = EXCLUDED.current_weight,
                        reason = EXCLUDED.reason,
                        evidence = EXCLUDED.evidence,
                        status = 'OPEN',
                        created_at = NOW()
                    """,
                    record["alert_ts"],
                    record["business_date"],
                    record["slot"],
                    record["ticker"],
                    record["alert_type"],
                    record["severity"],
                    record["current_price"],
                    record["reference_price"],
                    record["change_pct"],
                    record["current_weight"],
                    record["reason"],
                    json.dumps(record["evidence"], ensure_ascii=False),
                )
                if status.startswith("INSERT") or status.startswith("UPDATE"):
                    saved += 1
        return saved

    async def get_active_manual_market_events(
        self,
        *,
        at: datetime | None = None,
        tickers: list[str] | None = None,
    ) -> list[ManualMarketEvent]:
        """Load manually declared active catalysts/events.

        This is intentionally read-side/lazy-migrated so reports keep working
        after deploy even if the live DB has not been initialized separately.
        """
        if not self._pool:
            raise RuntimeError("Llamar connect() primero")

        at = at or datetime.now(timezone.utc)
        ticker_filter = [
            str(ticker or "").upper().strip()
            for ticker in (tickers or [])
            if str(ticker or "").strip()
        ]
        ticker_filter = list(dict.fromkeys(ticker_filter))

        async with self._pool.acquire() as conn:
            await self._ensure_manual_market_events_schema(conn)
            rows = await conn.fetch(
                """
                SELECT
                    id,
                    event_date,
                    event_time_hint,
                    ticker,
                    title,
                    impact_scope,
                    related_tickers,
                    severity,
                    active_from,
                    active_until,
                    action_policy,
                    notes,
                    is_active
                FROM manual_market_events
                WHERE is_active = TRUE
                  AND active_from <= $1
                  AND active_until >= $1
                  AND (
                        cardinality($2::text[]) = 0
                     OR UPPER(COALESCE(ticker, '')) = ANY($2::text[])
                     OR EXISTS (
                            SELECT 1
                            FROM unnest(related_tickers) AS related(ticker)
                            WHERE UPPER(related.ticker) = ANY($2::text[])
                        )
                  )
                ORDER BY
                    CASE severity WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END,
                    event_date,
                    id
                """,
                at,
                ticker_filter,
            )
        return [manual_market_event_from_row(dict(row)) for row in rows]

    async def upsert_manual_market_event(
        self,
        *,
        event_date: date,
        event_time_hint: str,
        ticker: str | None,
        title: str,
        impact_scope: list[str] | tuple[str, ...] | None,
        related_tickers: list[str] | tuple[str, ...] | None,
        severity: str,
        active_from: datetime,
        active_until: datetime,
        action_policy: str,
        notes: str | None = None,
    ) -> int:
        if not self._pool:
            raise RuntimeError("Llamar connect() primero")

        ticker_value = str(ticker or "").upper().strip() or None
        impact_scope_value = [str(x).strip() for x in (impact_scope or []) if str(x).strip()]
        related_value = [
            str(x).upper().strip()
            for x in (related_tickers or [])
            if str(x).strip()
        ]

        async with self._pool.acquire() as conn:
            await self._ensure_manual_market_events_schema(conn)
            existing_id = await conn.fetchval(
                """
                SELECT id
                FROM manual_market_events
                WHERE event_date = $1
                  AND UPPER(COALESCE(ticker, '')) = UPPER(COALESCE($2::text, ''))
                  AND title = $3
                  AND is_active = TRUE
                ORDER BY id DESC
                LIMIT 1
                """,
                event_date,
                ticker_value,
                str(title or "").strip(),
            )
            if existing_id:
                row = await conn.fetchrow(
                    """
                    UPDATE manual_market_events
                    SET event_time_hint = $2,
                        impact_scope = $3,
                        related_tickers = $4,
                        severity = $5,
                        active_from = $6,
                        active_until = $7,
                        action_policy = $8,
                        notes = $9,
                        updated_at = NOW()
                    WHERE id = $1
                    RETURNING id
                    """,
                    int(existing_id),
                    normalize_event_time_hint(event_time_hint),
                    impact_scope_value,
                    related_value,
                    normalize_severity(severity),
                    active_from,
                    active_until,
                    normalize_action_policy(action_policy),
                    notes,
                )
            else:
                row = await conn.fetchrow(
                    """
                    INSERT INTO manual_market_events (
                        event_date,
                        event_time_hint,
                        ticker,
                        title,
                        impact_scope,
                        related_tickers,
                        severity,
                        active_from,
                        active_until,
                        action_policy,
                        notes,
                        is_active,
                        updated_at
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,TRUE,NOW())
                    RETURNING id
                    """,
                    event_date,
                    normalize_event_time_hint(event_time_hint),
                    ticker_value,
                    str(title or "").strip(),
                    impact_scope_value,
                    related_value,
                    normalize_severity(severity),
                    active_from,
                    active_until,
                    normalize_action_policy(action_policy),
                    notes,
                )
        return int(row["id"])

    async def deactivate_manual_market_event(self, event_id: int) -> bool:
        if not self._pool:
            raise RuntimeError("Llamar connect() primero")

        async with self._pool.acquire() as conn:
            await self._ensure_manual_market_events_schema(conn)
            result = await conn.execute(
                """
                UPDATE manual_market_events
                SET is_active = FALSE,
                    updated_at = NOW()
                WHERE id = $1
                """,
                int(event_id),
            )
        try:
            return int(result.split()[-1]) > 0
        except Exception:
            return False

    async def upsert_corporate_action(
        self,
        *,
        event_key: str,
        issuer_id: str,
        event_type: str,
        lifecycle_status: str,
        effective_at: datetime,
        instrument_id: str,
        ticker: str,
        quantity_factor: float,
        price_factor: float,
        cost_basis_factor: float,
        announced_at: datetime | None = None,
        expires_at: datetime | None = None,
        source_name: str = "",
        source_url: str = "",
        source_published_at: datetime | None = None,
        source_hash: str = "",
        ingestion_method: str = "MANUAL",
        evidence_level: str = "PRIMARY_OFFICIAL",
        detector_score: float | None = None,
        detector_version: str | None = None,
        raw_payload: Mapping[str, Any] | None = None,
        venue: str = "BYMA",
        asset_type: str = "UNKNOWN",
        currency: str = "ARS",
        depositary_ratio_before: str = "",
        depositary_ratio_after: str = "",
        metadata: Mapping[str, Any] | None = None,
    ) -> tuple[int, int]:
        if not self._pool:
            raise RuntimeError("Llamar connect() primero")
        async with self._pool.acquire() as conn:
            await self._ensure_corporate_actions_schema(conn)
            async with conn.transaction():
                event_id = await conn.fetchval(
                    """
                    INSERT INTO corporate_events (
                        event_key, issuer_id, event_type, lifecycle_status,
                        announced_at, effective_at, expires_at,
                        source_name, source_url, source_published_at, source_hash,
                        ingestion_method, evidence_level, detector_score,
                        detector_version, raw_payload, updated_at
                    ) VALUES (
                        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16::jsonb,NOW()
                    )
                    ON CONFLICT (event_key) DO UPDATE SET
                        issuer_id = EXCLUDED.issuer_id,
                        event_type = EXCLUDED.event_type,
                        lifecycle_status = EXCLUDED.lifecycle_status,
                        announced_at = EXCLUDED.announced_at,
                        effective_at = EXCLUDED.effective_at,
                        expires_at = EXCLUDED.expires_at,
                        source_name = EXCLUDED.source_name,
                        source_url = EXCLUDED.source_url,
                        source_published_at = EXCLUDED.source_published_at,
                        source_hash = EXCLUDED.source_hash,
                        ingestion_method = EXCLUDED.ingestion_method,
                        evidence_level = EXCLUDED.evidence_level,
                        detector_score = EXCLUDED.detector_score,
                        detector_version = EXCLUDED.detector_version,
                        raw_payload = EXCLUDED.raw_payload,
                        updated_at = NOW()
                    RETURNING id
                    """,
                    str(event_key).strip(),
                    str(issuer_id).strip(),
                    str(event_type).upper().strip(),
                    str(lifecycle_status).upper().strip(),
                    announced_at,
                    effective_at,
                    expires_at,
                    str(source_name or "").strip(),
                    str(source_url or "").strip(),
                    source_published_at,
                    str(source_hash or "").strip(),
                    str(ingestion_method).upper().strip(),
                    str(evidence_level).upper().strip(),
                    detector_score,
                    detector_version,
                    json.dumps(dict(raw_payload or {}), ensure_ascii=False),
                )
                effect_id = await conn.fetchval(
                    """
                    INSERT INTO corporate_event_instrument_effects (
                        event_id, instrument_id, ticker, venue, asset_type, currency,
                        quantity_factor, price_factor, cost_basis_factor,
                        depositary_ratio_before, depositary_ratio_after,
                        metadata, is_active, updated_at
                    ) VALUES (
                        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,TRUE,NOW()
                    )
                    ON CONFLICT (event_id, instrument_id) DO UPDATE SET
                        ticker = EXCLUDED.ticker,
                        venue = EXCLUDED.venue,
                        asset_type = EXCLUDED.asset_type,
                        currency = EXCLUDED.currency,
                        quantity_factor = EXCLUDED.quantity_factor,
                        price_factor = EXCLUDED.price_factor,
                        cost_basis_factor = EXCLUDED.cost_basis_factor,
                        depositary_ratio_before = EXCLUDED.depositary_ratio_before,
                        depositary_ratio_after = EXCLUDED.depositary_ratio_after,
                        metadata = EXCLUDED.metadata,
                        is_active = TRUE,
                        updated_at = NOW()
                    RETURNING id
                    """,
                    int(event_id),
                    str(instrument_id).upper().strip(),
                    str(ticker).upper().strip(),
                    str(venue or "").upper().strip(),
                    str(asset_type or "").upper().strip(),
                    str(currency or "").upper().strip(),
                    float(quantity_factor),
                    float(price_factor),
                    float(cost_basis_factor),
                    str(depositary_ratio_before or "").strip(),
                    str(depositary_ratio_after or "").strip(),
                    json.dumps(dict(metadata or {}), ensure_ascii=False),
                )
        return int(event_id), int(effect_id)

    async def get_corporate_action_effects(
        self,
        *,
        tickers: list[str] | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
    ) -> list[CorporateActionEffect]:
        if not self._pool:
            return []
        clean_tickers = sorted({
            str(ticker or "").upper().strip()
            for ticker in (tickers or [])
            if str(ticker or "").strip()
        })
        since = since or (datetime.now(timezone.utc) - timedelta(days=730))
        until = until or (datetime.now(timezone.utc) + timedelta(days=7))
        async with self._pool.acquire() as conn:
            await self._ensure_corporate_actions_schema(conn)
            rows = await conn.fetch(
                """
                SELECT
                    e.id AS event_id,
                    effect.id AS effect_id,
                    e.event_key,
                    e.issuer_id,
                    e.event_type,
                    e.lifecycle_status,
                    e.effective_at,
                    e.expires_at,
                    e.source_name,
                    e.source_url,
                    e.ingestion_method,
                    e.evidence_level,
                    e.detector_score,
                    effect.instrument_id,
                    effect.ticker,
                    effect.venue,
                    effect.asset_type,
                    effect.currency,
                    effect.quantity_factor,
                    effect.price_factor,
                    effect.cost_basis_factor,
                    effect.depositary_ratio_before,
                    effect.depositary_ratio_after,
                    effect.metadata
                FROM corporate_event_instrument_effects effect
                JOIN corporate_events e ON e.id = effect.event_id
                WHERE effect.is_active = TRUE
                  AND e.lifecycle_status IN ('ANNOUNCED', 'CONFIRMED', 'EFFECTIVE')
                  AND e.effective_at >= $1
                  AND e.effective_at <= $2
                  AND (
                        cardinality($3::text[]) = 0
                     OR UPPER(effect.ticker) = ANY($3::text[])
                  )
                ORDER BY e.effective_at, e.id, effect.id
                """,
                since,
                until,
                clean_tickers,
            )
        return [corporate_action_effect_from_row(dict(row)) for row in rows]

    async def get_latest_portfolio_instrument_seeds(self) -> list[dict[str, str]]:
        """Return local instruments from each owner's newest portfolio snapshot.

        The result is discovery input only. It is not an instruction to create
        an effect or trade, and may contain an issuer hint from a previously
        confirmed corporate action such as YPFD -> YPF.
        """
        if not self._pool:
            raise RuntimeError("Llamar connect() primero")
        async with self._pool.acquire() as conn:
            await self._ensure_corporate_actions_schema(conn)
            rows = await conn.fetch(
                """
                WITH latest_snapshots AS (
                    SELECT DISTINCT ON (COALESCE(owner_chat_id, 0))
                        snapshot_id
                    FROM portfolio_snapshots
                    ORDER BY COALESCE(owner_chat_id, 0), scraped_at DESC
                )
                SELECT DISTINCT ON (UPPER(p.ticker), p.asset_type, p.currency)
                    UPPER(p.ticker) AS ticker,
                    COALESCE(p.asset_type, 'UNKNOWN') AS asset_type,
                    COALESCE(p.currency, 'ARS') AS currency,
                    COALESCE(corporate.issuer_id, '') AS issuer_hint
                FROM positions p
                JOIN latest_snapshots latest USING (snapshot_id)
                LEFT JOIN LATERAL (
                    SELECT event.issuer_id
                    FROM corporate_event_instrument_effects effect
                    JOIN corporate_events event ON event.id = effect.event_id
                    WHERE UPPER(effect.ticker) = UPPER(p.ticker)
                      AND event.lifecycle_status IN ('CONFIRMED', 'EFFECTIVE')
                    ORDER BY event.effective_at DESC, event.id DESC
                    LIMIT 1
                ) corporate ON TRUE
                WHERE COALESCE(p.ticker, '') <> ''
                ORDER BY UPPER(p.ticker), p.asset_type, p.currency
                """
            )
        return [dict(row) for row in rows]

    async def upsert_issuer_registry(
        self,
        entries: Sequence[IssuerRegistryEntry],
        instruments: Sequence[IssuerInstrument],
    ) -> dict[str, int]:
        if not self._pool:
            raise RuntimeError("Llamar connect() primero")
        normalized_entries = [entry.normalized() for entry in entries]
        normalized_instruments = [instrument.normalized() for instrument in instruments]
        if not normalized_entries and not normalized_instruments:
            return {"issuers": 0, "instruments": 0}

        async with self._pool.acquire() as conn:
            await self._ensure_issuer_events_schema(conn)
            async with conn.transaction():
                issuers_saved = 0
                for entry in normalized_entries:
                    status = await conn.execute(
                        """
                        INSERT INTO issuer_registry (
                            issuer_id, issuer_name, source_market, primary_symbol,
                            sec_cik, cnv_entity_name, metadata, is_active, updated_at
                        ) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,TRUE,NOW())
                        ON CONFLICT (issuer_id) DO UPDATE SET
                            issuer_name = EXCLUDED.issuer_name,
                            source_market = EXCLUDED.source_market,
                            primary_symbol = EXCLUDED.primary_symbol,
                            sec_cik = NULLIF(EXCLUDED.sec_cik, ''),
                            cnv_entity_name = NULLIF(EXCLUDED.cnv_entity_name, ''),
                            metadata = EXCLUDED.metadata,
                            is_active = TRUE,
                            updated_at = NOW()
                        """,
                        entry.issuer_id,
                        entry.issuer_name,
                        entry.source_market,
                        entry.primary_symbol,
                        entry.sec_cik or None,
                        entry.cnv_entity_name or None,
                        json.dumps(entry.metadata, ensure_ascii=False),
                    )
                    issuers_saved += status.startswith(("INSERT", "UPDATE"))

                instruments_saved = 0
                for instrument in normalized_instruments:
                    status = await conn.execute(
                        """
                        INSERT INTO issuer_instruments (
                            issuer_id, ticker, instrument_id, venue, asset_type,
                            currency, source_ticker, metadata, is_active, updated_at
                        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,TRUE,NOW())
                        ON CONFLICT (instrument_id) DO UPDATE SET
                            issuer_id = EXCLUDED.issuer_id,
                            ticker = EXCLUDED.ticker,
                            venue = EXCLUDED.venue,
                            asset_type = EXCLUDED.asset_type,
                            currency = EXCLUDED.currency,
                            source_ticker = EXCLUDED.source_ticker,
                            metadata = EXCLUDED.metadata,
                            is_active = TRUE,
                            updated_at = NOW()
                        """,
                        instrument.issuer_id,
                        instrument.ticker,
                        instrument.instrument_id,
                        instrument.venue,
                        instrument.asset_type,
                        instrument.currency,
                        instrument.source_ticker,
                        json.dumps(instrument.metadata, ensure_ascii=False),
                    )
                    instruments_saved += status.startswith(("INSERT", "UPDATE"))
        return {"issuers": issuers_saved, "instruments": instruments_saved}

    async def get_active_issuer_registry(self) -> list[IssuerRegistryEntry]:
        if not self._pool:
            return []
        async with self._pool.acquire() as conn:
            await self._ensure_issuer_events_schema(conn)
            rows = await conn.fetch(
                """
                SELECT issuer_id, issuer_name, source_market, primary_symbol,
                       sec_cik, cnv_entity_name, metadata
                FROM issuer_registry
                WHERE is_active = TRUE
                ORDER BY source_market, primary_symbol, issuer_id
                """
            )
        return [
            IssuerRegistryEntry(
                issuer_id=str(row["issuer_id"]),
                issuer_name=str(row["issuer_name"]),
                source_market=str(row["source_market"]),
                primary_symbol=str(row["primary_symbol"] or ""),
                sec_cik=str(row["sec_cik"] or ""),
                cnv_entity_name=str(row["cnv_entity_name"] or ""),
                metadata=_json_payload(row["metadata"]),
            ).normalized()
            for row in rows
        ]

    async def save_issuer_event_observations(
        self,
        observations: Sequence[IssuerEventObservation],
    ) -> int:
        if not self._pool or not observations:
            return 0
        normalized = [observation.normalized() for observation in observations]
        saved = 0
        async with self._pool.acquire() as conn:
            await self._ensure_issuer_events_schema(conn)
            for observation in normalized:
                status = await conn.execute(
                    """
                    INSERT INTO issuer_event_observations (
                        observation_key, issuer_id, ticker, source, event_type,
                        lifecycle_status, event_date, event_time_hint,
                        source_published_at, source_url, source_hash, confidence,
                        actionable, title, raw_payload, updated_at
                    ) VALUES (
                        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb,NOW()
                    )
                    ON CONFLICT (observation_key) DO UPDATE SET
                        lifecycle_status = EXCLUDED.lifecycle_status,
                        event_date = EXCLUDED.event_date,
                        event_time_hint = EXCLUDED.event_time_hint,
                        source_published_at = EXCLUDED.source_published_at,
                        source_url = EXCLUDED.source_url,
                        source_hash = EXCLUDED.source_hash,
                        confidence = EXCLUDED.confidence,
                        title = EXCLUDED.title,
                        raw_payload = EXCLUDED.raw_payload,
                        updated_at = NOW()
                    """,
                    observation.observation_key,
                    observation.issuer_id,
                    observation.ticker or None,
                    observation.source,
                    observation.event_type,
                    observation.lifecycle_status,
                    observation.event_date,
                    observation.event_time_hint,
                    observation.source_published_at,
                    observation.source_url,
                    observation.source_hash,
                    observation.confidence,
                    False,
                    observation.title,
                    json.dumps(observation.raw_payload, ensure_ascii=False),
                )
                saved += status.startswith(("INSERT", "UPDATE"))
        return saved

    async def get_issuer_event_observations(
        self,
        *,
        limit: int = 100,
        ticker: str | None = None,
    ) -> list[dict[str, Any]]:
        if not self._pool:
            return []
        async with self._pool.acquire() as conn:
            await self._ensure_issuer_events_schema(conn)
            rows = await conn.fetch(
                """
                SELECT observation_key, issuer_id, ticker, source, event_type,
                       lifecycle_status, event_date, event_time_hint,
                       source_published_at, source_url, confidence, actionable,
                       title, raw_payload, created_at, updated_at
                FROM issuer_event_observations
                WHERE ($1::text IS NULL OR UPPER(ticker) = UPPER($1))
                ORDER BY COALESCE(source_published_at, updated_at) DESC, id DESC
                LIMIT $2
                """,
                str(ticker).strip() if ticker else None,
                max(1, min(int(limit), 1000)),
            )
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["raw_payload"] = _json_payload(item.get("raw_payload"))
            result.append(item)
        return result

    async def save_price_quality_flags(
        self,
        flags: Sequence[PriceQualityFlag],
    ) -> int:
        if not self._pool or not flags:
            return 0
        saved = 0
        async with self._pool.acquire() as conn:
            await self._ensure_corporate_actions_schema(conn)
            for flag in flags:
                if flag.resolution_status == "CONFIRMED":
                    await conn.execute(
                        """
                        UPDATE price_quality_flags
                        SET resolution_status = 'CONFIRMED',
                            action_taken = 'SUPERSEDED_BY_CONFIRMED_EVENT',
                            updated_at = NOW()
                        WHERE UPPER(ticker) = UPPER($1)
                          AND resolution_status = 'OPEN'
                          AND observed_at::date BETWEEN ($2::date - 1) AND ($2::date + 1)
                        """,
                        flag.ticker,
                        flag.observed_at,
                    )
                status = await conn.execute(
                    """
                    INSERT INTO price_quality_flags (
                        event_id, instrument_effect_id, ticker, observed_at, expires_at,
                        flag_type, resolution_status, observed_reference_price,
                        observed_current_price, observed_return, expected_price_factor,
                        observed_quantity_factor, quantity_factor, evidence_level,
                        detector_score, detector_version, action_taken, reason,
                        evidence, idempotency_key, updated_at
                    ) VALUES (
                        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,
                        $17,$18,$19::jsonb,$20,NOW()
                    )
                    ON CONFLICT (idempotency_key) DO UPDATE SET
                        expires_at = EXCLUDED.expires_at,
                        resolution_status = EXCLUDED.resolution_status,
                        detector_score = EXCLUDED.detector_score,
                        action_taken = EXCLUDED.action_taken,
                        reason = EXCLUDED.reason,
                        evidence = EXCLUDED.evidence,
                        updated_at = NOW()
                    """,
                    flag.event_id,
                    flag.instrument_effect_id,
                    flag.ticker,
                    flag.observed_at,
                    flag.expires_at,
                    flag.flag_type,
                    flag.resolution_status,
                    flag.observed_reference_price,
                    flag.observed_current_price,
                    flag.observed_return,
                    flag.expected_price_factor,
                    flag.observed_quantity_factor,
                    flag.quantity_factor,
                    flag.evidence_level,
                    flag.detector_score,
                    flag.detector_version,
                    flag.action_taken,
                    flag.reason,
                    json.dumps(flag.evidence or {}, ensure_ascii=False),
                    flag.idempotency_key,
                )
                saved += status.startswith("INSERT")
        return saved

    async def get_active_price_quality_flags(
        self,
        *,
        tickers: list[str] | None = None,
        at: datetime | None = None,
    ) -> list[dict[str, Any]]:
        if not self._pool:
            return []
        at = at or datetime.now(timezone.utc)
        clean_tickers = sorted({
            str(ticker or "").upper().strip()
            for ticker in (tickers or [])
            if str(ticker or "").strip()
        })
        async with self._pool.acquire() as conn:
            await self._ensure_corporate_actions_schema(conn)
            await conn.execute(
                """
                UPDATE price_quality_flags
                SET resolution_status = 'EXPIRED',
                    updated_at = NOW()
                WHERE resolution_status = 'OPEN'
                  AND expires_at IS NOT NULL
                  AND expires_at < $1
                """,
                at,
            )
            rows = await conn.fetch(
                """
                SELECT *
                FROM price_quality_flags
                WHERE resolution_status = 'OPEN'
                  AND (expires_at IS NULL OR expires_at >= $1)
                  AND (
                        cardinality($2::text[]) = 0
                     OR UPPER(ticker) = ANY($2::text[])
                  )
                ORDER BY observed_at DESC, id DESC
                """,
                at,
                clean_tickers,
            )
        return [dict(row) for row in rows]

    async def record_corporate_action_applications(
        self,
        applications: Sequence[CorporateActionApplication],
    ) -> int:
        if not self._pool or not applications:
            return 0
        saved = 0
        async with self._pool.acquire() as conn:
            await self._ensure_corporate_actions_schema(conn)
            for application in applications:
                status = await conn.execute(
                    """
                    INSERT INTO corporate_event_applications (
                        event_id, instrument_effect_id, owner_chat_id, component,
                        application_status, adjustment_version, idempotency_key,
                        before_state, after_state, invariant_checks, error, applied_at
                    ) VALUES (
                        $1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb,$10::jsonb,$11,
                        CASE WHEN $5 IN ('APPLIED', 'ALREADY_ADJUSTED') THEN NOW() ELSE NULL END
                    )
                    ON CONFLICT (idempotency_key) DO UPDATE SET
                        application_status = EXCLUDED.application_status,
                        before_state = EXCLUDED.before_state,
                        after_state = EXCLUDED.after_state,
                        invariant_checks = EXCLUDED.invariant_checks,
                        error = EXCLUDED.error,
                        applied_at = EXCLUDED.applied_at
                    """,
                    application.event_id,
                    application.instrument_effect_id,
                    application.owner_chat_id,
                    application.component,
                    application.application_status,
                    application.adjustment_version,
                    application.idempotency_key,
                    json.dumps(application.before_state or {}, ensure_ascii=False),
                    json.dumps(application.after_state or {}, ensure_ascii=False),
                    json.dumps(application.invariant_checks or {}, ensure_ascii=False),
                    application.error,
                )
                saved += status.startswith("INSERT")
        return saved

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

    async def get_latest_market_prices(
        self,
        *,
        fresh_only: bool = False,
        min_fresh_tickers: int = MARKET_FRESHNESS_MIN_TICKERS,
    ) -> list[dict]:
        if not self._pool:
            return []
        async with self._pool.acquire() as conn:
            if fresh_only:
                latest_day = await conn.fetchrow(
                    """
                    SELECT
                        ts::date AS market_date,
                        COUNT(DISTINCT ticker) AS ticker_count
                    FROM market_prices
                    WHERE last_price IS NOT NULL
                      AND last_price > 0
                    GROUP BY ts::date
                    HAVING COUNT(DISTINCT ticker) >= $1
                    ORDER BY market_date DESC
                    LIMIT 1
                    """,
                    int(min_fresh_tickers),
                )
                if not latest_day:
                    logger.warning(
                        "market_prices freshness: no hay rueda con >= %s tickers; "
                        "universo fresco queda vacio",
                        int(min_fresh_tickers),
                    )
                    return []

                market_date = latest_day["market_date"]
                rows = await conn.fetch(
                    """
                    WITH latest_per_ticker AS (
                        SELECT DISTINCT ON (ticker)
                            ticker,
                            asset_type,
                            currency,
                            last_price,
                            change_pct_1d,
                            ts,
                            ts::date AS latest_price_date
                        FROM market_prices
                        WHERE last_price IS NOT NULL
                          AND last_price > 0
                        ORDER BY ticker, ts DESC
                    )
                    SELECT
                        ticker,
                        asset_type,
                        currency,
                        last_price,
                        change_pct_1d,
                        ts,
                        latest_price_date,
                        (latest_price_date < $1::date) AS excluded_by_freshness
                    FROM latest_per_ticker
                    ORDER BY ticker
                    """,
                    market_date,
                )

                fresh: list[dict] = []
                excluded: list[dict] = []
                for row in rows:
                    item = dict(row)
                    public_row = {
                        key: item.get(key)
                        for key in (
                            "ticker",
                            "asset_type",
                            "currency",
                            "last_price",
                            "change_pct_1d",
                            "ts",
                        )
                    }
                    if item.get("excluded_by_freshness"):
                        excluded.append({
                            "ticker": item.get("ticker"),
                            "latest_price_date": item.get("latest_price_date"),
                        })
                    else:
                        fresh.append(public_row)

                if excluded:
                    sample = ", ".join(
                        f"{str(item.get('ticker')).upper()}@{item.get('latest_price_date')}"
                        for item in excluded[:20]
                    )
                    suffix = "" if len(excluded) <= 20 else f" (+{len(excluded) - 20} mas)"
                    logger.warning(
                        "market_prices freshness: excluidos %s tickers stale vs rueda %s "
                        "(min_tickers=%s): %s%s",
                        len(excluded),
                        market_date,
                        int(min_fresh_tickers),
                        sample,
                        suffix,
                    )
                logger.info(
                    "market_prices freshness: incluidos=%s excluidos=%s rueda=%s tickers_rueda=%s",
                    len(fresh),
                    len(excluded),
                    market_date,
                    int(latest_day["ticker_count"] or 0),
                )
                return fresh

            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (ticker)
                    ticker, asset_type, currency, last_price, change_pct_1d, ts
                FROM market_prices
                ORDER BY ticker, ts DESC
                """
            )
        return [dict(r) for r in rows]

    async def get_previous_candle_closes(
        self,
        tickers: list[str],
        *,
        before_day: Optional[date] = None,
    ) -> dict[str, float]:
        if not self._pool or not tickers:
            return {}
        clean = sorted({str(t).upper() for t in tickers if str(t or "").strip()})
        if not clean:
            return {}
        before_day = before_day or datetime.now(ART_TZ).date()
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (ticker)
                    ticker, close_price
                FROM market_candles
                WHERE ticker = ANY($1::text[])
                  AND close_price IS NOT NULL
                  AND close_price > 0
                  AND ts::date < $2::date
                ORDER BY ticker, ts DESC
                """,
                clean,
                before_day,
            )
        return {str(r["ticker"]).upper(): float(r["close_price"]) for r in rows}

    async def get_cocos_universe(self) -> list[str]:
        prices = await self.get_cocos_universe_assets()
        tickers = sorted({
            row["ticker"].upper()
            for row in prices
        })
        logger.info(f"Universo Cocos: {len(tickers)} tickers disponibles")
        return tickers

    async def get_cocos_universe_assets(
        self,
        *,
        fresh_only: bool = True,
        min_fresh_tickers: int = MARKET_FRESHNESS_MIN_TICKERS,
    ) -> list[dict]:
        prices = await self.get_latest_market_prices(
            fresh_only=fresh_only,
            min_fresh_tickers=min_fresh_tickers,
        )
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
                FROM decision_log dl
                WHERE decided_at >= $1
                  AND ($2::bigint IS NULL OR owner_chat_id = $2)
                  AND outcome_5d IS NOT NULL
                  AND was_correct IS NOT NULL
                  AND outcome_basis = 'canonical_cocos'
                  AND decision IN ('BUY', 'SELL')
                  AND is_primary_metric = TRUE
                  AND NOT EXISTS (
                      SELECT 1
                      FROM broker_fills bf
                      WHERE bf.decision_log_id = dl.id
                        AND COALESCE(bf.raw_payload, '{}'::jsonb) ? 'superseded_by_real'
                        AND NOT EXISTS (
                            SELECT 1
                            FROM broker_fills live_bf
                            WHERE live_bf.decision_log_id = dl.id
                              AND NOT (COALESCE(live_bf.raw_payload, '{}'::jsonb) ? 'superseded_by_real')
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
                str(fill.executed_at_precision or "unknown").lower(),
                str(fill.executed_at_source or "unknown"),
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
            await self._ensure_execution_timestamp_meta_columns(conn)
            await conn.executemany(
                """
                INSERT INTO broker_fills (
                    source,
                    external_fill_id,
                    executed_at,
                    executed_at_precision,
                    executed_at_source,
                    ticker,
                    side,
                    quantity,
                    avg_fill_price,
                    gross_amount_ars,
                    fees_ars,
                    raw_payload
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb)
                ON CONFLICT (source, external_fill_id) DO UPDATE SET
                    executed_at      = EXCLUDED.executed_at,
                    executed_at_precision = EXCLUDED.executed_at_precision,
                    executed_at_source    = EXCLUDED.executed_at_source,
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
            superseded = await _mark_superseded_broker_fills_for_saved_rows(conn, rows)

        logger.info(
            "%s broker fills guardados; %s placeholders synthetic superseded",
            len(rows),
            superseded,
        )
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
                str(movement.executed_at_precision or "date_only").lower(),
                str(movement.executed_at_source or "cocos_movements.execution_date"),
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
            await self._ensure_execution_timestamp_meta_columns(conn)
            for row in rows:
                external_id = str(row[1])
                if external_id.startswith("synthetic:"):
                    continue
                await conn.execute(
                    """
                    UPDATE broker_movements
                    SET external_movement_id = $2
                    WHERE id = (
                        SELECT id
                        FROM broker_movements
                        WHERE source = $1
                          AND external_movement_id LIKE 'synthetic:%'
                          AND executed_at::date = $3::date
                          AND movement_type = $4
                          AND COALESCE(ticker, '') = COALESCE($8::text, '')
                          AND ABS(COALESCE(quantity, 0) - COALESCE($6::numeric, 0)) < 0.000001
                          AND ABS(COALESCE(price, 0) - COALESCE($7::numeric, 0)) < 0.01
                          AND ABS(COALESCE(amount, 0) - COALESCE($5::numeric, 0)) < 0.01
                        ORDER BY id
                        LIMIT 1
                    )
                    AND NOT EXISTS (
                        SELECT 1
                        FROM broker_movements
                        WHERE source = $1
                          AND external_movement_id = $2
                    )
                    """,
                    row[0],
                    row[1],
                    row[2],
                    row[5],
                    row[7],
                    row[8],
                    row[9],
                    row[10],
                )

            await conn.executemany(
                """
                INSERT INTO broker_movements (
                    source,
                    external_movement_id,
                    executed_at,
                    executed_at_precision,
                    executed_at_source,
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
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18::jsonb)
                ON CONFLICT (source, external_movement_id) DO UPDATE SET
                    executed_at     = EXCLUDED.executed_at,
                    executed_at_precision = EXCLUDED.executed_at_precision,
                    executed_at_source    = EXCLUDED.executed_at_source,
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

    async def existing_broker_movement_keys(
        self,
        movements: list[BrokerMovement],
    ) -> set[tuple[str, str]]:
        """Return movement keys already persisted before an upsert.

        This is used only for notification/deduplication. Persistence remains
        handled by save_broker_movements().
        """
        if not self._pool or not movements:
            return set()

        sources: list[str] = []
        external_ids: list[str] = []
        for movement in movements:
            source = str(movement.source or "").strip()
            external_id = str(movement.external_movement_id or "").strip()
            if source and external_id:
                sources.append(source)
                external_ids.append(external_id)

        if not sources:
            return set()

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT bm.source, bm.external_movement_id
                FROM broker_movements bm
                JOIN unnest($1::text[], $2::text[]) AS k(source, external_movement_id)
                  ON bm.source = k.source
                 AND bm.external_movement_id = k.external_movement_id
                """,
                sources,
                external_ids,
            )

        return {
            (str(row["source"]), str(row["external_movement_id"]))
            for row in rows
        }

    async def get_latest_broker_movement_summary(self) -> Optional[dict]:
        if not self._pool:
            return None

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT
                    executed_at,
                    executed_at_precision,
                    executed_at_source,
                    movement_type,
                    ticker,
                    quantity,
                    price,
                    amount,
                    created_at
                FROM broker_movements
                WHERE movement_type IN ('BUY', 'SELL')
                ORDER BY created_at DESC
                LIMIT 1
                """
            )

        return dict(row) if row else None

    async def mark_superseded_broker_fills(
        self,
        superseded_to_real: Mapping[int, int],
        *,
        reason: str = SUPERSEDED_BROKER_FILL_REASON,
    ) -> int:
        if not self._pool:
            raise RuntimeError("Llamar connect() primero")
        if not superseded_to_real:
            return 0

        pairs = sorted(
            {
                (int(superseded_id), int(real_id))
                for superseded_id, real_id in superseded_to_real.items()
            }
        )
        superseded_ids = [pair[0] for pair in pairs]
        real_ids = [pair[1] for pair in pairs]

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                WITH mapping AS (
                    SELECT *
                    FROM unnest($1::bigint[], $2::bigint[]) AS m(superseded_id, real_id)
                ),
                real_fills AS (
                    SELECT
                        mapping.superseded_id,
                        real.id AS real_id,
                        real.external_fill_id AS real_external_fill_id
                    FROM mapping
                    JOIN broker_fills real ON real.id = mapping.real_id
                )
                UPDATE broker_fills synthetic
                SET raw_payload = COALESCE(synthetic.raw_payload, '{}'::jsonb)
                    || jsonb_build_object(
                        'superseded_by_real',
                        jsonb_build_object(
                            'fill_id', real_fills.real_id,
                            'external_fill_id', real_fills.real_external_fill_id,
                            'reason', $3::text
                        )
                    )
                FROM real_fills
                WHERE synthetic.id = real_fills.superseded_id
                RETURNING synthetic.id
                """,
                superseded_ids,
                real_ids,
                reason,
            )

        marked = len(rows)
        logger.info("broker fills marcados superseded_by_real: %s", marked)
        return marked

    async def reconcile_broker_fills(self, max_age_days: int = 3) -> int:
        if not self._pool:
            raise RuntimeError("Llamar connect() primero")

        async with self._pool.acquire() as conn:
            await self._ensure_execution_timestamp_meta_columns(conn)
            fill_rows = await conn.fetch(
                """
                SELECT
                    id,
                    source,
                    external_fill_id,
                    executed_at,
                    executed_at_precision,
                    executed_at_source,
                    ticker,
                    side,
                    quantity,
                    avg_fill_price,
                    gross_amount_ars,
                    fees_ars,
                    raw_payload
                FROM broker_fills
                WHERE decision_log_id IS NULL
                  AND NOT (COALESCE(raw_payload, '{}'::jsonb) ? 'superseded_by_real')
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
                  AND COALESCE(metric_scope, 'planner_audit') <> 'blocked_audit'
                  AND COALESCE(decision_stage, 'approved_decision') <> 'blocked'
                  AND COALESCE(was_blocked, FALSE) = FALSE
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
                    executed_at_precision=str(row["executed_at_precision"] or "unknown"),
                    executed_at_source=str(row["executed_at_source"] or "unknown"),
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
                        price_at_decision = COALESCE(price_at_decision, $4),
                        is_executable = TRUE,
                        was_blocked = FALSE,
                        decision_stage = 'executed',
                        metric_scope = 'primary',
                        is_primary_metric = TRUE,
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
                                "executed_at_precision": fill.executed_at_precision,
                                "executed_at_source": fill.executed_at_source,
                                "quantity": fill.quantity,
                                "avg_fill_price": fill.avg_fill_price,
                                "gross_amount_ars": executed_amount,
                                "fees_ars": fill.fees_ars,
                            }
                        }
                    ),
                    float(fill.avg_fill_price),
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
            await self._ensure_decision_audit_scope_columns(conn)
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
                  AND NOT (COALESCE(raw_payload, '{}'::jsonb) ? 'superseded_by_real')
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
                audit_scope = classify_decision_audit_scope(
                    source=decision_source,
                    status="EXECUTED_MANUAL",
                    decision_type=decision_source,
                )
                layer_patch = _manual_broker_layer_patch(
                    decision_source=decision_source,
                    fill_date=fill_date,
                    ticker=ticker,
                    side=side,
                    owner_chat_id=owner_chat_id,
                    external_ids=external_ids,
                    quantity=quantity,
                    avg_fill_price=avg_fill_price,
                    executed_amount=executed_amount,
                    fees_ars=float(group["fees_ars"] or 0.0),
                )

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
                            was_blocked,
                            run_intent,
                            decision_stage,
                            metric_scope,
                            is_primary_metric
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
                            FALSE,
                            $9,
                            $10,
                            $11,
                            $12
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
                        audit_scope["run_intent"],
                        audit_scope["decision_stage"],
                        audit_scope["metric_scope"],
                        audit_scope["is_primary_metric"],
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
                        run_intent = $6,
                        decision_stage = $7,
                        metric_scope = $8,
                        is_primary_metric = $9,
                        layers = COALESCE(layers, '{}'::jsonb) || $4::jsonb
                    WHERE id = $1
                    """,
                    int(decision_id),
                    executed_amount,
                    avg_fill_price,
                    json.dumps(layer_patch),
                    decision_source,
                    audit_scope["run_intent"],
                    audit_scope["decision_stage"],
                    audit_scope["metric_scope"],
                    audit_scope["is_primary_metric"],
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
                await self._ensure_decision_audit_scope_columns(conn)
                audit_scope = classify_decision_audit_scope(
                    source="signal",
                    status=None,
                    decision_type=getattr(decision, "direction", None),
                    run_intent="exploratory",
                )
                row = await conn.fetchrow(
                    """
                    INSERT INTO decision_log (
                        decided_at, ticker, decision, final_score, confidence,
                        layers, price_at_decision, vix_at_decision, regime,
                        size_pct, stop_loss_pct, target_pct, horizon_days, rr_ratio,
                        run_intent, decision_stage, metric_scope, is_primary_metric
                    ) VALUES (
                        $1, $2, $3, $4, $5,
                        $6::jsonb, $7, $8, $9,
                        $10, $11, $12, $13, $14,
                        $15, $16, $17, $18
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
                    audit_scope["run_intent"],
                    audit_scope["decision_stage"],
                    audit_scope["metric_scope"],
                    audit_scope["is_primary_metric"],
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
                await self._ensure_decision_audit_scope_columns(conn)
                audit_scope = classify_decision_audit_scope(
                    source=d.get("source"),
                    status=d.get("status"),
                    decision_type=d.get("decision_type"),
                    run_intent="exploratory",
                )
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
                        source,
                        run_intent, decision_stage, metric_scope, is_primary_metric
                    ) VALUES (
                        $1,$2,$3,$4,$5,
                        $6,$7,$8,
                        $9,$10,$11,
                        $12,$13,
                        $14,$15,
                        $16,$17,
                        $18,$19,
                        $20,$21,
                        $22,
                        $23,$24,$25,$26
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
                    audit_scope["run_intent"],
                    audit_scope["decision_stage"],
                    audit_scope["metric_scope"],
                    audit_scope["is_primary_metric"],
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
            (40, "outcome_40d"),
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

    def _next_executable_reference(
        self,
        *,
        entry_price: float,
        decided_at: datetime,
        candles: list[dict],
    ) -> tuple[datetime | None, float | None, date | None]:
        """
        Returns the first realistically executable reference.

        If a decision is generated after the local close, the first executable
        reference is the next candle's open (or close fallback). Intraday
        decisions keep price_at_decision as the executable reference.
        """
        decided_art = decided_at.astimezone(ART_TZ)
        decided_day = decided_art.date()
        after_close = decided_art.hour > 17 or (decided_art.hour == 17 and decided_art.minute >= 0)

        if not after_close:
            return decided_at, float(entry_price), decided_day

        eligible = [
            candle for candle in candles
            if candle["ts"].date() > decided_day
            and (candle.get("open_price") is not None or candle.get("close_price") is not None)
        ]
        if not eligible:
            return None, None, None

        candle = eligible[0]
        px = candle.get("open_price")
        if px is None or float(px) <= 0:
            px = candle.get("close_price")
        if px is None or float(px) <= 0:
            return None, None, None
        return candle["ts"], float(px), candle["ts"].date()

    async def _compute_executable_outcomes(
        self,
        *,
        entry_price: float,
        start_day: date,
        direction: str,
        now: datetime,
        candles: list[dict],
    ) -> dict[str, float]:
        outcomes: dict[str, float] = {}

        for horizon, col in [
            (5, "executable_outcome_5d"),
            (10, "executable_outcome_10d"),
            (20, "executable_outcome_20d"),
            (40, "executable_outcome_40d"),
        ]:
            target_day = start_day + timedelta(days=horizon)
            if target_day > now.astimezone(ART_TZ).date():
                continue
            eligible = [
                candle for candle in candles
                if candle["ts"].date() >= target_day
                and candle.get("close_price") is not None
            ]
            if not eligible:
                continue
            price_at_horizon = float(eligible[0]["close_price"])
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

    async def _corporate_action_adjusted_outcome_inputs(
        self,
        *,
        ticker: str,
        entry_price: float,
        decided_at: datetime,
        candles: list[dict],
        as_of: datetime,
    ) -> tuple[float, list[dict], float]:
        closes = []
        for candle in candles:
            try:
                close = float(candle.get("close_price"))
            except (TypeError, ValueError):
                continue
            if close > 0:
                closes.append(close)
        has_discontinuity = any(
            abs((current / previous) - 1.0) >= MIN_ANOMALY_RETURN
            for previous, current in zip(closes, closes[1:])
            if previous > 0
        )
        if not has_discontinuity:
            return entry_price, candles, 1.0

        effects = await self.get_corporate_action_effects(
            tickers=[ticker],
            since=decided_at - timedelta(days=7),
            until=as_of,
        )
        if not effects:
            return entry_price, candles, 1.0
        adjusted_entry, adjustment_factor = rebase_reference_price(
            entry_price,
            reference_at=decided_at,
            as_of=as_of,
            effects=effects,
        )
        return (
            float(adjusted_entry or entry_price),
            normalize_candle_rows(candles, effects),
            float(adjustment_factor),
        )

    async def update_outcomes(
        self,
        lookback_days: int = 30,
        owner_chat_id: Optional[int] = None,
    ) -> int:
        """
        Busca decisiones sin outcome donde han pasado >=5 días y llena
        outcome_5d / outcome_10d / outcome_20d / outcome_40d / was_correct usando la serie
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
                await self._ensure_outcome_horizon_columns(conn)
                if owner_chat_id is None:
                    rows = await conn.fetch(
                        """
                        SELECT id, ticker, price_at_decision, decided_at, decision
                        FROM decision_log
                        WHERE (
                              outcome_5d IS NULL
                           OR executable_outcome_5d IS NULL
                           OR (
                                decided_at <= NOW() - INTERVAL '10 days'
                                AND (outcome_10d IS NULL OR executable_outcome_10d IS NULL)
                              )
                           OR (
                                decided_at <= NOW() - INTERVAL '20 days'
                                AND (outcome_20d IS NULL OR executable_outcome_20d IS NULL)
                              )
                           OR (
                                decided_at <= NOW() - INTERVAL '40 days'
                                AND (outcome_40d IS NULL OR executable_outcome_40d IS NULL)
                              )
                        )
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
                        WHERE (
                              outcome_5d IS NULL
                           OR executable_outcome_5d IS NULL
                           OR (
                                decided_at <= NOW() - INTERVAL '10 days'
                                AND (outcome_10d IS NULL OR executable_outcome_10d IS NULL)
                              )
                           OR (
                                decided_at <= NOW() - INTERVAL '20 days'
                                AND (outcome_20d IS NULL OR executable_outcome_20d IS NULL)
                              )
                           OR (
                                decided_at <= NOW() - INTERVAL '40 days'
                                AND (outcome_40d IS NULL OR executable_outcome_40d IS NULL)
                              )
                        )
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

                entry_f, candles, corporate_adjustment_factor = (
                    await self._corporate_action_adjusted_outcome_inputs(
                        ticker=ticker,
                        entry_price=entry_f,
                        decided_at=decided_at,
                        candles=candles,
                        as_of=now,
                    )
                )
                if corporate_adjustment_factor != 1.0:
                    logger.info(
                        "update_outcomes %s id=%s corporate price factor=%.8f",
                        ticker,
                        row["id"],
                        corporate_adjustment_factor,
                    )

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

                (
                    next_executable_at,
                    next_executable_price,
                    executable_start_day,
                ) = self._next_executable_reference(
                    entry_price=entry_f,
                    decided_at=decided_at,
                    candles=candles,
                )
                executable_outcomes: dict[str, float] = {}
                if next_executable_price is not None and executable_start_day is not None:
                    executable_outcomes = await self._compute_executable_outcomes(
                        entry_price=float(next_executable_price),
                        start_day=executable_start_day,
                        direction=direction,
                        now=now,
                        candles=candles,
                    )

                if not outcomes and not executable_outcomes:
                    continue

                primary = outcomes.get("outcome_5d", outcomes.get("outcome_10d"))
                was_correct = primary > 0 if primary is not None else None
                executable_primary = executable_outcomes.get(
                    "executable_outcome_5d",
                    executable_outcomes.get("executable_outcome_10d"),
                )
                executable_was_correct = (
                    executable_primary > 0
                    if executable_primary is not None
                    else None
                )

                try:
                    async with self._pool.acquire() as conn:
                        await conn.execute(
                            """
                            UPDATE decision_log SET
                                outcome_5d        = COALESCE($2, outcome_5d),
                                outcome_10d       = COALESCE($3, outcome_10d),
                                outcome_20d       = COALESCE($4, outcome_20d),
                                outcome_40d       = COALESCE($14, outcome_40d),
                                was_correct       = COALESCE($5, was_correct),
                                outcome_filled_at = NOW(),
                                outcome_basis       = $6,
                                outcome_basis_ratio = $7,
                                next_executable_at     = COALESCE($8, next_executable_at),
                                next_executable_price  = COALESCE($9, next_executable_price),
                                executable_outcome_5d  = COALESCE($10, executable_outcome_5d),
                                executable_outcome_10d = COALESCE($11, executable_outcome_10d),
                                executable_outcome_20d = COALESCE($12, executable_outcome_20d),
                                executable_outcome_40d = COALESCE($15, executable_outcome_40d),
                                executable_was_correct = COALESCE($13, executable_was_correct)
                            WHERE id = $1
                            """,
                            row["id"],
                            outcomes.get("outcome_5d"),
                            outcomes.get("outcome_10d"),
                            outcomes.get("outcome_20d"),
                            was_correct,
                            outcome_basis,
                            basis_ratio,
                            next_executable_at,
                            next_executable_price,
                            executable_outcomes.get("executable_outcome_5d"),
                            executable_outcomes.get("executable_outcome_10d"),
                            executable_outcomes.get("executable_outcome_20d"),
                            executable_was_correct,
                            outcomes.get("outcome_40d"),
                            executable_outcomes.get("executable_outcome_40d"),
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
            await self._ensure_outcome_horizon_columns(conn)
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
                            outcome_40d         = NULL,
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

            adjusted_entry, candles, corporate_adjustment_factor = (
                await self._corporate_action_adjusted_outcome_inputs(
                    ticker=ticker,
                    entry_price=float(entry),
                    decided_at=decided_at,
                    candles=candles,
                    as_of=now,
                )
            )
            if corporate_adjustment_factor != 1.0:
                logger.info(
                    "recompute_outcomes %s id=%s corporate price factor=%.8f",
                    ticker,
                    row["id"],
                    corporate_adjustment_factor,
                )

            outcome_basis, basis_ratio = self._assess_outcome_basis(
                entry_price=adjusted_entry,
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
                            outcome_40d         = NULL,
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
                entry_price=adjusted_entry,
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
                            outcome_40d       = $8,
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
                        outcomes.get("outcome_40d"),
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
            await self._ensure_decision_audit_scope_columns(conn)
            await self._ensure_outcome_horizon_columns(conn)
            # Cargar filas raw — el cálculo de retorno del trader se hace en Python
            raw_rows = await conn.fetch(
                """
                SELECT
                    id, ticker, decision,
                    COALESCE(executable_outcome_5d, outcome_5d) AS outcome_5d,
                    COALESCE(executable_outcome_10d, outcome_10d) AS outcome_10d,
                    COALESCE(executable_outcome_20d, outcome_20d) AS outcome_20d,
                    COALESCE(executable_outcome_40d, outcome_40d) AS outcome_40d,
                    COALESCE(executable_was_correct, was_correct) AS was_correct,
                    size_pct,
                    COALESCE(source, layers->>'source', 'sin_source') AS source,
                    COALESCE(status, 'UNKNOWN') AS status,
                    COALESCE(decision_type, 'unknown') AS decision_type,
                    COALESCE(metric_scope, 'debug') AS metric_scope
                FROM decision_log dl
                WHERE decided_at >= $1
                  AND ($2::bigint IS NULL OR owner_chat_id = $2)
                  AND COALESCE(executable_outcome_5d, outcome_5d) IS NOT NULL
                  AND COALESCE(executable_was_correct, was_correct) IS NOT NULL
                  AND outcome_basis = 'canonical_cocos'
                  AND decision IN ('BUY', 'SELL')
                  AND is_primary_metric = TRUE
                  AND NOT EXISTS (
                      SELECT 1
                      FROM broker_fills bf
                      WHERE bf.decision_log_id = dl.id
                        AND COALESCE(bf.raw_payload, '{}'::jsonb) ? 'superseded_by_real'
                        AND NOT EXISTS (
                            SELECT 1
                            FROM broker_fills live_bf
                            WHERE live_bf.decision_log_id = dl.id
                              AND NOT (COALESCE(live_bf.raw_payload, '{}'::jsonb) ? 'superseded_by_real')
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
                WHERE COALESCE(executable_outcome_5d, outcome_5d) IS NULL
                  AND ($2::bigint IS NULL OR owner_chat_id = $2)
                  AND COALESCE(outcome_basis, '') <> 'legacy_external'
                  AND price_at_decision IS NOT NULL
                  AND price_at_decision > 0
                  AND decision IN ('BUY', 'SELL')
                  AND decided_at >= $1
                  AND is_primary_metric = TRUE
                """,
                cutoff,
                owner_chat_id,
            )

            pending_all_count = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM decision_log
                WHERE COALESCE(executable_outcome_5d, outcome_5d) IS NULL
                  AND ($2::bigint IS NULL OR owner_chat_id = $2)
                  AND COALESCE(outcome_basis, '') <> 'legacy_external'
                  AND price_at_decision IS NOT NULL
                  AND price_at_decision > 0
                  AND decision IN ('BUY', 'SELL')
                  AND decided_at >= $1
                  AND COALESCE(metric_scope, 'debug') IN ('primary', 'planner_audit', 'blocked_audit')
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
                    COALESCE(dl.executable_outcome_5d, dl.outcome_5d) AS outcome_5d,
                    COALESCE(dl.executable_was_correct, dl.was_correct) AS was_correct,
                    dl.next_executable_at,
                    dl.next_executable_price,
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
                  AND COALESCE(dl.metric_scope, 'debug') IN ('primary', 'planner_audit', 'blocked_audit')
                  AND NOT EXISTS (
                      SELECT 1
                      FROM broker_fills bf
                      WHERE bf.decision_log_id = dl.id
                        AND COALESCE(bf.raw_payload, '{}'::jsonb) ? 'superseded_by_real'
                        AND NOT EXISTS (
                            SELECT 1
                            FROM broker_fills live_bf
                            WHERE live_bf.decision_log_id = dl.id
                              AND NOT (COALESCE(live_bf.raw_payload, '{}'::jsonb) ? 'superseded_by_real')
                        )
                  )
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
        ret_40d_list    = []

        for r in raw_rows:
            direction  = str(r["decision"]).upper()
            out5       = float(r["outcome_5d"] or 0.0)
            out10      = float(r["outcome_10d"]) if r["outcome_10d"] is not None else None
            out20      = float(r["outcome_20d"]) if r["outcome_20d"] is not None else None
            out40      = float(r["outcome_40d"]) if r["outcome_40d"] is not None else None

            trader_ret   = out5
            trader_ret10 = out10
            trader_ret20 = out20
            trader_ret40 = out40

            trader_returns.append(trader_ret)
            if trader_ret10 is not None:
                ret_10d_list.append(trader_ret10)
            if trader_ret20 is not None:
                ret_20d_list.append(trader_ret20)
            if trader_ret40 is not None:
                ret_40d_list.append(trader_ret40)

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
            "avg_return_40d":  sum(ret_40d_list) / len(ret_40d_list) if ret_40d_list else None,
            "best_trade":      max(trader_returns) if trader_returns else None,
            "worst_trade":     min(trader_returns) if trader_returns else None,
            "ev":              ev,
            "profit_factor":   profit_factor,
            "lookback_days":   lookback_days,
            "ticker_stats":    ticker_stats[:10],
            "source_stats":    source_stats,
            "recent":          [dict(r) for r in recent_rows],
        }

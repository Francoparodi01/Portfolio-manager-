"""Intraday, audit-only Telegram alerts for prospective Radar setups."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from html import escape
import json
import math
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from src.analysis.radar_discovery import RADAR_DISCOVERY_SCHEMA_SQL


ART_TZ = ZoneInfo("America/Argentina/Buenos_Aires")
RADAR_SETUP_ALERT_PROTOCOL_VERSION = "radar-setup-intraday-alert-v1"
ALERT_TYPE_TRIGGERED_PRICE = "TRIGGERED_PRICE"
DELIVERY_PENDING = "PENDING"
DELIVERY_SENT = "SENT"
DELIVERY_FAILED = "FAILED"
USER_ACTION_FOLLOW = "FOLLOW"
USER_ACTION_DISMISS = "DISMISS"


RADAR_SETUP_ALERT_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS radar_setup_alerts (
    id BIGSERIAL PRIMARY KEY,
    snapshot_id BIGINT NOT NULL
        REFERENCES radar_discovery_snapshots(id) ON DELETE CASCADE,
    owner_chat_id BIGINT NOT NULL,
    ticker TEXT NOT NULL,
    asset_type TEXT NOT NULL,
    scoring_version TEXT NOT NULL,
    setup_shadow_version TEXT NOT NULL,
    protocol_version TEXT NOT NULL,
    alert_type TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    observed_session DATE NOT NULL,
    market_price_ts TIMESTAMPTZ NOT NULL,
    observed_price FLOAT NOT NULL,
    setup_percentile FLOAT NOT NULL,
    setup_score FLOAT,
    trigger_price FLOAT NOT NULL,
    invalidation_price FLOAT NOT NULL,
    target_price FLOAT,
    setup_risk_reward FLOAT,
    feature_quality_flag TEXT NOT NULL,
    delivery_status TEXT NOT NULL DEFAULT 'PENDING',
    send_attempts INTEGER NOT NULL DEFAULT 0,
    telegram_message_id BIGINT,
    sent_at TIMESTAMPTZ,
    send_error TEXT,
    user_action TEXT,
    user_action_at TIMESTAMPTZ,
    broker_fill_id BIGINT REFERENCES broker_fills(id) ON DELETE SET NULL,
    broker_fill_linked_at TIMESTAMPTZ,
    follow_match_status TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (snapshot_id, alert_type)
);

ALTER TABLE radar_setup_alerts
    ADD COLUMN IF NOT EXISTS broker_fill_id BIGINT
        REFERENCES broker_fills(id) ON DELETE SET NULL;
ALTER TABLE radar_setup_alerts
    ADD COLUMN IF NOT EXISTS broker_fill_linked_at TIMESTAMPTZ;
ALTER TABLE radar_setup_alerts
    ADD COLUMN IF NOT EXISTS follow_match_status TEXT;

CREATE INDEX IF NOT EXISTS idx_radar_setup_alerts_owner_recent
    ON radar_setup_alerts(owner_chat_id, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_radar_setup_alerts_ticker_recent
    ON radar_setup_alerts(owner_chat_id, ticker, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_radar_setup_alerts_delivery
    ON radar_setup_alerts(delivery_status, created_at)
    WHERE delivery_status IN ('PENDING', 'FAILED');
CREATE UNIQUE INDEX IF NOT EXISTS idx_radar_setup_alerts_broker_fill
    ON radar_setup_alerts(broker_fill_id)
    WHERE broker_fill_id IS NOT NULL;
"""


@dataclass(frozen=True)
class RadarSetupAlertCandidate:
    snapshot_id: int
    owner_chat_id: int
    ticker: str
    asset_type: str
    scoring_version: str
    setup_shadow_version: str
    captured_session: Any
    reference_ts: datetime
    observed_at: datetime
    market_price_ts: datetime
    observed_price: float
    setup_percentile: float
    setup_score: float | None
    trigger_price: float
    invalidation_price: float
    target_price: float | None
    setup_risk_reward: float | None
    feature_quality_flag: str
    setup_warnings: tuple[str, ...]
    run_type: str

    @property
    def extension_pct(self) -> float:
        return self.observed_price / self.trigger_price - 1.0

    @property
    def risk_pct(self) -> float:
        return self.invalidation_price / self.observed_price - 1.0

    @property
    def target_return_pct(self) -> float | None:
        if self.target_price is None:
            return None
        return self.target_price / self.observed_price - 1.0

    @property
    def is_preclose(self) -> bool:
        return self.run_type.startswith("16:40")


def evaluate_setup_alert_candidate(
    row: Mapping[str, Any],
    *,
    observed_at: datetime,
    run_type: str,
    min_setup_percentile: float = 0.80,
    min_risk_reward: float = 2.0,
    max_extension_pct: float = 0.06,
    max_price_age_seconds: int = 900,
    max_snapshot_age_days: int = 7,
) -> RadarSetupAlertCandidate | None:
    """Apply the shadow alert gate without changing the underlying score."""
    asset_type = str(row.get("asset_type") or "").upper()
    if (
        asset_type != "CEDEAR"
        or bool(row.get("in_portfolio"))
        or bool(row.get("current_in_portfolio"))
    ):
        return None
    if str(row.get("readiness_state") or "").upper() != "PRE_BREAKOUT":
        return None
    if str(row.get("feature_quality_flag") or "").upper() not in {"GOOD", "PARTIAL"}:
        return None
    if str(row.get("manual_event_risk") or "").strip():
        return None

    warnings = _string_tuple(row.get("setup_warnings"))
    if any(
        term in warning.lower()
        for warning in warnings
        for term in ("corporate_action", "split_suspected", "basis_break")
    ):
        return None

    percentile = _finite(row.get("setup_percentile"))
    observed_price = _finite(row.get("observed_price"))
    trigger = _finite(row.get("trigger_price"))
    invalidation = _finite(row.get("invalidation_price"))
    risk_reward = _finite(row.get("setup_risk_reward"))
    if None in (percentile, observed_price, trigger, invalidation, risk_reward):
        return None
    if percentile < float(min_setup_percentile):
        return None
    if risk_reward < float(min_risk_reward):
        return None
    if invalidation <= 0 or trigger <= invalidation:
        return None
    if observed_price < trigger or observed_price <= invalidation:
        return None
    if observed_price > trigger * (1.0 + float(max_extension_pct)):
        return None

    now = _aware(observed_at)
    captured_session = _session_date(row.get("captured_session"))
    if captured_session is None:
        return None
    snapshot_age_days = (now.astimezone(ART_TZ).date() - captured_session).days
    if snapshot_age_days < 0 or snapshot_age_days > max(int(max_snapshot_age_days), 0):
        return None
    price_ts = _aware(row.get("market_price_ts"))
    if price_ts.astimezone(ART_TZ).date() != now.astimezone(ART_TZ).date():
        return None
    age_seconds = (now - price_ts).total_seconds()
    if age_seconds < -60 or age_seconds > max(int(max_price_age_seconds), 1):
        return None

    reference_ts = _aware(row.get("reference_ts"))
    return RadarSetupAlertCandidate(
        snapshot_id=int(row["snapshot_id"]),
        owner_chat_id=int(row["owner_chat_id"]),
        ticker=str(row.get("ticker") or "").upper(),
        asset_type=asset_type,
        scoring_version=str(row.get("scoring_version") or ""),
        setup_shadow_version=str(row.get("setup_shadow_version") or ""),
        captured_session=row.get("captured_session"),
        reference_ts=reference_ts,
        observed_at=now,
        market_price_ts=price_ts,
        observed_price=observed_price,
        setup_percentile=percentile,
        setup_score=_finite(row.get("setup_score")),
        trigger_price=trigger,
        invalidation_price=invalidation,
        target_price=_finite(row.get("target_price")),
        setup_risk_reward=risk_reward,
        feature_quality_flag=str(row.get("feature_quality_flag") or "").upper(),
        setup_warnings=warnings,
        run_type=str(run_type or "INTRADAY"),
    )


class RadarSetupAlertStore:
    def __init__(self, pool: Any):
        self.pool = pool

    async def ensure_schema(self) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(RADAR_DISCOVERY_SCHEMA_SQL)
            await conn.execute(RADAR_SETUP_ALERT_SCHEMA_SQL)

    async def reserve_trigger_alerts(
        self,
        *,
        owner_chat_id: int,
        observed_at: datetime,
        run_type: str,
        min_setup_percentile: float = 0.80,
        min_risk_reward: float = 2.0,
        max_extension_pct: float = 0.06,
        max_price_age_seconds: int = 900,
        max_snapshot_age_days: int = 7,
        cooldown_days: int = 14,
        max_alerts: int = 3,
    ) -> list[dict[str, Any]]:
        await self.ensure_schema()
        observed = _aware(observed_at)
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                WITH latest_run AS (
                    SELECT run_id, owner_chat_id, captured_at, captured_session,
                           scoring_version
                    FROM radar_discovery_runs
                    WHERE owner_chat_id = $1
                    ORDER BY captured_at DESC
                    LIMIT 1
                ), latest_prices AS (
                    SELECT DISTINCT ON (UPPER(ticker))
                           UPPER(ticker) AS ticker,
                           ts AS market_price_ts,
                           last_price::float AS observed_price,
                           volume::float AS observed_volume
                    FROM market_prices
                    WHERE last_price IS NOT NULL AND last_price > 0
                    ORDER BY UPPER(ticker), ts DESC
                ), latest_portfolio AS (
                    SELECT snapshot_id
                    FROM portfolio_snapshots
                    WHERE owner_chat_id = $1 OR owner_chat_id IS NULL
                    ORDER BY (owner_chat_id = $1) DESC NULLS LAST, scraped_at DESC
                    LIMIT 1
                ), current_holdings AS (
                    SELECT DISTINCT UPPER(p.ticker) AS ticker
                    FROM positions p
                    JOIN latest_portfolio lp USING (snapshot_id)
                    WHERE COALESCE(p.quantity, 0) > 0
                )
                SELECT s.id AS snapshot_id, r.owner_chat_id, r.scoring_version,
                       r.captured_session, s.ticker, s.asset_type,
                       s.reference_ts, s.setup_shadow_version,
                       s.setup_percentile, s.setup_score, s.readiness_state,
                       s.trigger_price, s.invalidation_price, s.target_price,
                       s.setup_risk_reward, s.feature_quality_flag,
                       s.setup_warnings, s.in_portfolio,
                       (h.ticker IS NOT NULL) AS current_in_portfolio,
                       NULLIF(s.metadata->>'manual_event_risk', '') AS manual_event_risk,
                       p.market_price_ts, p.observed_price, p.observed_volume
                FROM latest_run r
                JOIN radar_discovery_snapshots s ON s.run_id = r.run_id
                JOIN latest_prices p ON p.ticker = UPPER(s.ticker)
                LEFT JOIN current_holdings h ON h.ticker = UPPER(s.ticker)
                LEFT JOIN radar_setup_events e ON e.snapshot_id = s.id
                WHERE e.snapshot_id IS NULL
                ORDER BY s.setup_percentile DESC NULLS LAST, s.ticker
                """,
                int(owner_chat_id),
            )
            recent_rows = await conn.fetch(
                """
                SELECT DISTINCT UPPER(ticker) AS ticker
                FROM radar_setup_alerts
                WHERE owner_chat_id = $1
                  AND delivery_status IN ('PENDING', 'SENT')
                  AND observed_at >= $2
                """,
                int(owner_chat_id),
                observed - timedelta(days=max(int(cooldown_days), 1)),
            )
        recent_tickers = {str(row["ticker"]).upper() for row in recent_rows}
        candidates: list[RadarSetupAlertCandidate] = []
        for raw in rows:
            row = dict(raw)
            if str(row.get("ticker") or "").upper() in recent_tickers:
                continue
            candidate = evaluate_setup_alert_candidate(
                row,
                observed_at=observed,
                run_type=run_type,
                min_setup_percentile=min_setup_percentile,
                min_risk_reward=min_risk_reward,
                max_extension_pct=max_extension_pct,
                max_price_age_seconds=max_price_age_seconds,
                max_snapshot_age_days=max_snapshot_age_days,
            )
            if candidate is not None:
                candidates.append(candidate)
            if len(candidates) >= max(int(max_alerts), 1):
                break

        reserved: list[dict[str, Any]] = []
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                for candidate in candidates:
                    alert_id = await conn.fetchval(
                        """
                        INSERT INTO radar_setup_alerts (
                            snapshot_id, owner_chat_id, ticker, asset_type,
                            scoring_version, setup_shadow_version,
                            protocol_version, alert_type, observed_at,
                            observed_session, market_price_ts, observed_price,
                            setup_percentile, setup_score, trigger_price,
                            invalidation_price, target_price, setup_risk_reward,
                            feature_quality_flag, metadata
                        ) VALUES (
                            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,
                            $15,$16,$17,$18,$19,$20::jsonb
                        )
                        ON CONFLICT (snapshot_id, alert_type) DO NOTHING
                        RETURNING id
                        """,
                        candidate.snapshot_id,
                        candidate.owner_chat_id,
                        candidate.ticker,
                        candidate.asset_type,
                        candidate.scoring_version,
                        candidate.setup_shadow_version,
                        RADAR_SETUP_ALERT_PROTOCOL_VERSION,
                        ALERT_TYPE_TRIGGERED_PRICE,
                        candidate.observed_at,
                        candidate.observed_at.astimezone(ART_TZ).date(),
                        candidate.market_price_ts,
                        candidate.observed_price,
                        candidate.setup_percentile,
                        candidate.setup_score,
                        candidate.trigger_price,
                        candidate.invalidation_price,
                        candidate.target_price,
                        candidate.setup_risk_reward,
                        candidate.feature_quality_flag,
                        json.dumps({
                            "run_type": candidate.run_type,
                            "captured_session": str(candidate.captured_session),
                            "reference_ts": candidate.reference_ts.isoformat(),
                            "setup_warnings": list(candidate.setup_warnings),
                            "trigger_confirmation": "price_only",
                            "affects_radar_ranking": False,
                            "affects_analysis": False,
                            "affects_execution": False,
                        }),
                    )
                    if alert_id is None:
                        continue
                    await conn.execute(
                        """
                        INSERT INTO radar_setup_events (
                            snapshot_id, setup_shadow_version, event_status,
                            event_ts, event_session, event_price,
                            sessions_from_discovery, trigger_price,
                            invalidation_price, trigger_volume_ratio,
                            event_basis, metadata
                        ) VALUES (
                            $1,$2,'TRIGGERED_AFTER_DISCOVERY',$3,$4,$5,NULL,
                            $6,$7,NULL,'intraday_market_prices_v1',$8::jsonb
                        )
                        ON CONFLICT (snapshot_id) DO NOTHING
                        """,
                        candidate.snapshot_id,
                        candidate.setup_shadow_version,
                        candidate.observed_at,
                        candidate.observed_at.astimezone(ART_TZ).date(),
                        candidate.observed_price,
                        candidate.trigger_price,
                        candidate.invalidation_price,
                        json.dumps({
                            "alert_id": int(alert_id),
                            "observed_market_price_ts": candidate.market_price_ts.isoformat(),
                            "trigger_confirmation": "price_only",
                            "same_session_high_low_order": "live_observation",
                        }),
                    )
                    reserved.append({"id": int(alert_id), **candidate.__dict__})
        return reserved

    async def pending_deliveries(
        self,
        *,
        owner_chat_id: int,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        await self.ensure_schema()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT *
                FROM radar_setup_alerts
                WHERE owner_chat_id = $1
                  AND delivery_status IN ('PENDING', 'FAILED')
                  AND send_attempts < 3
                  AND created_at >= NOW() - INTERVAL '1 day'
                ORDER BY created_at, id
                LIMIT $2
                """,
                int(owner_chat_id),
                max(int(limit), 1),
            )
        return [dict(row) for row in rows]

    async def mark_delivery(
        self,
        alert_id: int,
        *,
        message_id: int | None = None,
        error: str | None = None,
    ) -> None:
        status = DELIVERY_SENT if message_id is not None else DELIVERY_FAILED
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE radar_setup_alerts
                SET delivery_status = $2,
                    send_attempts = send_attempts + 1,
                    telegram_message_id = COALESCE($3, telegram_message_id),
                    sent_at = CASE WHEN $3::bigint IS NOT NULL THEN NOW() ELSE sent_at END,
                    send_error = $4,
                    updated_at = NOW()
                WHERE id = $1
                """,
                int(alert_id),
                status,
                message_id,
                str(error or "")[:500] or None,
            )

    async def get_alert(
        self,
        alert_id: int,
        *,
        owner_chat_id: int,
    ) -> dict[str, Any] | None:
        await self.ensure_schema()
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT *
                FROM radar_setup_alerts
                WHERE id = $1 AND owner_chat_id = $2
                """,
                int(alert_id),
                int(owner_chat_id),
            )
        return dict(row) if row is not None else None

    async def record_user_action(
        self,
        alert_id: int,
        *,
        owner_chat_id: int,
        action: str,
    ) -> dict[str, Any] | None:
        normalized = str(action or "").upper()
        if normalized not in {USER_ACTION_FOLLOW, USER_ACTION_DISMISS}:
            raise ValueError(f"Acción Radar inválida: {action}")
        await self.ensure_schema()
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE radar_setup_alerts
                SET user_action = $3,
                    user_action_at = NOW(),
                    follow_match_status = CASE
                        WHEN $3 = 'FOLLOW' THEN 'AWAITING_BUY_FILL'
                        ELSE 'DISMISSED'
                    END,
                    updated_at = NOW()
                WHERE id = $1 AND owner_chat_id = $2
                  AND user_action IS NULL
                RETURNING *
                """,
                int(alert_id),
                int(owner_chat_id),
                normalized,
            )
            action_recorded = row is not None
            if row is None:
                row = await conn.fetchrow(
                    """
                    SELECT *
                    FROM radar_setup_alerts
                    WHERE id = $1 AND owner_chat_id = $2
                    """,
                    int(alert_id),
                    int(owner_chat_id),
                )
        if row is None:
            return None
        result = dict(row)
        result["action_recorded"] = action_recorded
        result["action_conflict"] = bool(
            not action_recorded
            and str(result.get("user_action") or "").upper() != normalized
        )
        return result

    async def reconcile_followed_fills(
        self,
        *,
        owner_chat_id: int,
        max_calendar_days: int = 16,
    ) -> int:
        """Link an explicit FOLLOW action to a later real BUY from the same owner."""
        await self.ensure_schema()
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                """
                WITH candidate_matches AS (
                    SELECT a.id AS alert_id, matched_fill.id AS broker_fill_id
                    FROM radar_setup_alerts a
                    JOIN LATERAL (
                        SELECT f.id
                        FROM broker_fills f
                        WHERE f.owner_chat_id = a.owner_chat_id
                          AND UPPER(f.ticker) = UPPER(a.ticker)
                          AND f.side = 'BUY'
                          AND NOT EXISTS (
                              SELECT 1
                              FROM radar_setup_alerts linked
                              WHERE linked.broker_fill_id = f.id
                          )
                          AND CASE
                              WHEN f.executed_at_precision = 'date_only'
                              THEN (
                                  f.executed_at AT TIME ZONE
                                  'America/Argentina/Buenos_Aires'
                              )::date >= (
                                  a.user_action_at AT TIME ZONE
                                  'America/Argentina/Buenos_Aires'
                              )::date
                              ELSE f.executed_at >= a.user_action_at
                          END
                          AND (
                              f.executed_at AT TIME ZONE
                              'America/Argentina/Buenos_Aires'
                          )::date <= (
                              a.user_action_at AT TIME ZONE
                              'America/Argentina/Buenos_Aires'
                          )::date + $2::integer
                        ORDER BY f.executed_at, f.id
                        LIMIT 1
                    ) matched_fill ON TRUE
                    WHERE a.owner_chat_id = $1
                      AND a.user_action = 'FOLLOW'
                      AND a.user_action_at IS NOT NULL
                      AND a.broker_fill_id IS NULL
                )
                UPDATE radar_setup_alerts a
                SET broker_fill_id = candidate_matches.broker_fill_id,
                    broker_fill_linked_at = NOW(),
                    follow_match_status = 'MATCHED_OWNER_TICKER_TIME',
                    updated_at = NOW()
                FROM candidate_matches
                WHERE a.id = candidate_matches.alert_id
                """,
                int(owner_chat_id),
                max(int(max_calendar_days), 1),
            )
        return _command_count(result)


def render_radar_setup_alert(alert: Mapping[str, Any]) -> str:
    ticker = escape(str(alert.get("ticker") or "N/D").upper())
    quality = escape(str(alert.get("feature_quality_flag") or "UNKNOWN").upper())
    percentile = _finite(alert.get("setup_percentile"))
    score = _finite(alert.get("setup_score"))
    observed = _finite(alert.get("observed_price"))
    trigger = _finite(alert.get("trigger_price"))
    invalidation = _finite(alert.get("invalidation_price"))
    target = _finite(alert.get("target_price"))
    rr = _finite(alert.get("setup_risk_reward"))
    observed_at = _aware(alert.get("observed_at"))
    metadata = _mapping(alert.get("metadata"))
    run_type = str(metadata.get("run_type") or alert.get("run_type") or "")

    lines = [
        f"🔔 <b>SETUP ACTIVADO · {ticker}</b>",
        "<i>Radar shadow intradía · no genera órdenes automáticas.</i>",
        "",
        f"Estado: <b>trigger alcanzado por precio</b>",
        f"Setup: <b>percentil {_pct_rank(percentile)}</b> · "
        f"score {_score(score)}/50",
        f"Precio observado: <b>{_price(observed)}</b>",
        f"Trigger: {_price(trigger)} · extensión {_signed_pct(_ratio(observed, trigger))}",
        f"Invalidación: {_price(invalidation)} · riesgo {_signed_pct(_ratio(invalidation, observed))}",
        f"Objetivo experimental: {_price(target)} · potencial {_signed_pct(_ratio(target, observed))}",
        f"Riesgo/retorno del setup: <b>{_rr(rr)}</b>",
        "Horizonte auditado: <b>20 ruedas</b>",
        "",
        f"Datos: {observed_at.astimezone(ART_TZ):%d/%m %H:%M} ART · calidad <b>{quality}</b>",
    ]
    if quality == "PARTIAL":
        lines.append("⚠️ Calidad parcial: el cruce no confirma volumen intradía.")
    if run_type.startswith("16:40"):
        lines.append("⚠️ Pre-cierre: tratarlo como candidato para validar en la próxima rueda.")
    lines.extend([
        "",
        "<i>Seguir registra interés; no equivale a comprar. Si aparece un fill real, se audita por separado.</i>",
    ])
    return "\n".join(lines)


def radar_setup_alert_keyboard(alert_id: int) -> list[list[dict[str, str]]]:
    identifier = int(alert_id)
    return [
        [
            {"text": "🔎 Ver análisis", "callback_data": f"rs:view:{identifier}"},
            {"text": "👁 Seguir", "callback_data": f"rs:follow:{identifier}"},
        ],
        [
            {"text": "Descartar", "callback_data": f"rs:dismiss:{identifier}"},
        ],
    ]


def parse_radar_setup_callback(value: str) -> tuple[str, int] | None:
    parts = str(value or "").split(":")
    if len(parts) != 3 or parts[0] != "rs" or not parts[2].isdigit():
        return None
    action = parts[1].lower()
    if action not in {"view", "follow", "dismiss"}:
        return None
    return action, int(parts[2])


def _aware(value: Any) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError("timestamp requerido para alerta Radar")
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _session_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return _aware(value).astimezone(ART_TZ).date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def _command_count(result: Any) -> int:
    try:
        return int(str(result).rsplit(" ", 1)[-1])
    except (TypeError, ValueError):
        return 0


def _string_tuple(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError):
            decoded = [value]
        value = decoded
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return tuple(str(item) for item in value if str(item).strip())
    return ()


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return dict(decoded) if isinstance(decoded, Mapping) else {}
    return {}


def _price(value: float | None) -> str:
    if value is None:
        return "n/d"
    return "$" + f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _signed_pct(value: float | None) -> str:
    if value is None:
        return "n/d"
    return f"{value * 100:+.1f}%".replace(".", ",")


def _pct_rank(value: float | None) -> str:
    if value is None:
        return "n/d"
    return f"{value * 100:.0f}".replace(".", ",")


def _score(value: float | None) -> str:
    if value is None:
        return "n/d"
    return f"{value:.1f}".replace(".", ",")


def _rr(value: float | None) -> str:
    if value is None:
        return "n/d"
    return f"{value:.1f}x".replace(".", ",")


def _ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator <= 0:
        return None
    return numerator / denominator - 1.0


__all__ = [
    "ALERT_TYPE_TRIGGERED_PRICE",
    "RADAR_SETUP_ALERT_PROTOCOL_VERSION",
    "RADAR_SETUP_ALERT_SCHEMA_SQL",
    "RadarSetupAlertCandidate",
    "RadarSetupAlertStore",
    "USER_ACTION_DISMISS",
    "USER_ACTION_FOLLOW",
    "evaluate_setup_alert_candidate",
    "parse_radar_setup_callback",
    "radar_setup_alert_keyboard",
    "render_radar_setup_alert",
]

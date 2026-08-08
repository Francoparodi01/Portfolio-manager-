from __future__ import annotations

import asyncio
import hmac
import os
import re
import time as time_module
from collections import defaultdict, deque
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import asyncpg
import pyotp
from aiohttp import web

from src.analysis.audit_scope import ensure_decision_audit_scope_columns
from src.analysis.corporate_actions import (
    CorporateActionEffect,
    corporate_action_effect_from_row,
    effects_by_ticker,
    matching_effect_for_quantity_transition,
    rebase_position_view,
)
from src.analysis.decision_ledger import fetch_decision_ledger
from src.analysis.override_classification import (
    classify_override as _classify_override,
    dominant_override_status,
    override_delta as _override_delta,
    override_opposite_ratio as _override_opposite_ratio,
    override_same_ratio as _override_same_ratio,
)
from src.core.config import get_config
from src.core.logger import redact_secrets
from src.core.market_calendar import (
    is_settlement_day,
    is_trading_day,
    market_closed_reason,
    market_session_note,
)
from src.core.redis_client import client as redis_client


ART_TZ = ZoneInfo("America/Argentina/Buenos_Aires")
PROJECT_ROOT = Path(__file__).resolve().parents[2]
STATIC_DIR = Path(__file__).resolve().parent / "static"
LOG_DIR = Path(os.getenv("LOG_DIR", PROJECT_ROOT / "logs"))

MARKET_HEARTBEAT_KEY = "cocos:monitor:market:last_tick"
RISK_HEARTBEAT_KEY = "cocos:monitor:risk:last_check"
MONITOR_STATE_KEY = "cocos:monitor:state"
BOT_BUSY_KEY = "cocos:bot:busy"
BOT_HEARTBEAT_KEY = "cocos:bot:last_heartbeat"
SCHEDULER_HEARTBEAT_KEY = "cocos:scheduler:last_heartbeat"

TOKEN = os.getenv("MONITOR_API_TOKEN", "")
TOTP_SECRET = os.getenv("MONITOR_TOTP_SECRET", "")
AUTH_WINDOW_SECONDS = int(os.getenv("MONITOR_AUTH_WINDOW_SECONDS", "60"))
AUTH_MAX_FAILURES = int(os.getenv("MONITOR_AUTH_MAX_FAILURES", "8"))
TRUST_PROXY_HEADERS = os.getenv("MONITOR_TRUST_PROXY_HEADERS", "false").lower() in {"1", "true", "yes", "y"}
AUTH_FAILURES: dict[str, deque[float]] = defaultdict(deque)


def _now_art() -> datetime:
    return datetime.now(tz=ART_TZ)


def _is_market_hours(now: datetime | None = None) -> bool:
    now = now or _now_art()
    current = time(now.hour, now.minute)
    return time(10, 30) <= current <= time(17, 0)


def _json(data: dict, status: int = 200) -> web.Response:
    data.setdefault("generated_at", _now_art().isoformat())
    return web.json_response(data, status=status)


async def _corporate_action_schema_ready(conn: asyncpg.Connection) -> bool:
    return bool(await conn.fetchval(
        """
        SELECT
            to_regclass('public.corporate_events') IS NOT NULL
            AND to_regclass('public.corporate_event_instrument_effects') IS NOT NULL
            AND to_regclass('public.price_quality_flags') IS NOT NULL
            AND to_regclass('public.corporate_event_applications') IS NOT NULL
        """
    ))


async def _load_monitor_corporate_effects(
    conn: asyncpg.Connection,
    *,
    since: datetime,
    until: datetime,
    tickers: list[str] | None = None,
) -> list[CorporateActionEffect]:
    if not await _corporate_action_schema_ready(conn):
        return []
    clean_tickers = sorted({
        str(ticker or "").upper()
        for ticker in (tickers or [])
        if str(ticker or "").strip()
    })
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
          AND e.lifecycle_status IN ('CONFIRMED', 'EFFECTIVE')
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


def _client_key(request: web.Request) -> str:
    if TRUST_PROXY_HEADERS:
        cf_ip = request.headers.get("CF-Connecting-IP", "").strip()
        if cf_ip:
            return cf_ip
        forwarded = request.headers.get("X-Forwarded-For", "")
        if forwarded:
            return forwarded.split(",", 1)[0].strip()
    return request.remote or "unknown"


def _auth_limited(request: web.Request) -> bool:
    now = time_module.monotonic()
    key = _client_key(request)
    bucket = AUTH_FAILURES[key]
    while bucket and now - bucket[0] > AUTH_WINDOW_SECONDS:
        bucket.popleft()
    return len(bucket) >= AUTH_MAX_FAILURES


def _record_auth_failure(request: web.Request) -> None:
    now = time_module.monotonic()
    key = _client_key(request)
    bucket = AUTH_FAILURES[key]
    while bucket and now - bucket[0] > AUTH_WINDOW_SECONDS:
        bucket.popleft()
    bucket.append(now)


def _clear_auth_failures(request: web.Request) -> None:
    AUTH_FAILURES.pop(_client_key(request), None)


def _iso(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.astimezone(ART_TZ).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _age_seconds(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return max(0.0, datetime.now(tz=timezone.utc).timestamp() - float(value))
    if isinstance(value, str) and value.isdigit():
        return max(0.0, datetime.now(tz=timezone.utc).timestamp() - float(value))
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return max(0.0, (datetime.now(tz=timezone.utc) - dt.astimezone(timezone.utc)).total_seconds())
    return None


def _num(value):
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return value


def _row(row) -> dict:
    if not row:
        return {}
    out = {}
    for key, value in dict(row).items():
        if isinstance(value, (date, datetime)):
            out[key] = _iso(value)
            if isinstance(value, datetime):
                out[f"{key}_age_seconds"] = _age_seconds(value)
        else:
            converted = _num(value)
            if isinstance(converted, (str, int, float, bool)) or converted is None:
                out[key] = converted
            else:
                out[key] = str(converted)
    return out


def _float(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _override_intent_summary(items: list[dict]) -> dict:
    groups: dict[tuple[str, str], list[dict]] = {}
    for item in items:
        key = (
            str(item.get("ticker") or "").upper(),
            str(item.get("decision") or "").upper(),
        )
        groups.setdefault(key, []).append(item)

    by_status: dict[str, int] = {}
    bot_returns: list[float] = []
    deltas: list[float] = []
    closed = 0

    for group in groups.values():
        statuses = [str(row.get("override_status") or "UNKNOWN") for row in group]
        dominant = dominant_override_status(statuses)
        by_status[dominant] = by_status.get(dominant, 0) + 1

        group_returns = [
            _float(row.get("outcome_5d"))
            for row in group
            if row.get("outcome_5d") is not None
        ]
        group_deltas = [
            delta
            for row in group
            if row.get("outcome_5d") is not None
            for delta in [_override_delta(str(row.get("override_status")), row.get("outcome_5d"))]
            if delta is not None
        ]
        if group_returns:
            closed += 1
            bot_returns.append(_mean(group_returns) or 0.0)
        if group_deltas:
            deltas.append(_mean(group_deltas) or 0.0)

    return {
        "total": len(groups),
        "closed_5d": closed,
        "by_status": by_status,
        "avg_bot_5d": _mean(bot_returns),
        "avg_override_delta_5d": _mean(deltas),
    }


def _path_risk_label(mae_10d) -> str:
    if mae_10d is None:
        return "PENDING"
    mae = _float(mae_10d)
    if mae <= -0.12:
        return "HIGH"
    if mae <= -0.06:
        return "MEDIUM"
    return "OK"


async def _redis_get(key: str):
    try:
        return await redis_client.get(key)
    except Exception:
        return None


async def _redis_ping() -> bool:
    try:
        return bool(await redis_client.ping())
    except Exception:
        return False


def _extract_token(request: web.Request) -> str:
    auth = request.headers.get("Authorization", "")
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return request.headers.get("X-API-Token", "").strip()


@web.middleware
async def auth_middleware(request: web.Request, handler):
    public_paths = {"/", "/api/auth/status"}
    if request.path in public_paths or request.path.startswith("/static/"):
        return await handler(request)

    if request.method == "OPTIONS":
        return web.Response(status=204)

    if not TOKEN:
        return _json({"ok": False, "error": "MONITOR_API_TOKEN no configurado"}, status=503)

    if _auth_limited(request):
        return _json({"ok": False, "error": "demasiados intentos invalidos"}, status=429)

    provided = _extract_token(request)
    if not hmac.compare_digest(provided, TOKEN):
        _record_auth_failure(request)
        return _json({"ok": False, "error": "token invalido"}, status=401)

    if TOTP_SECRET:
        code = request.headers.get("X-TOTP-Code", "").strip().replace(" ", "")
        if not code or not pyotp.TOTP(TOTP_SECRET).verify(code, valid_window=1):
            _record_auth_failure(request)
            return _json({"ok": False, "error": "codigo TOTP invalido"}, status=401)

    _clear_auth_failures(request)
    return await handler(request)


@web.middleware
async def security_headers_middleware(request: web.Request, handler):
    response = await handler(request)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "no-referrer")
    response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
    response.headers.setdefault(
        "Content-Security-Policy",
        "default-src 'self'; connect-src 'self'; img-src 'self' data:; "
        "style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; "
        "base-uri 'none'; frame-ancestors 'none'",
    )
    if request.path == "/" or request.path.startswith("/api/"):
        response.headers.setdefault("Cache-Control", "no-store")
    return response


@web.middleware
async def cors_middleware(request: web.Request, handler):
    response = await handler(request)
    origin = request.headers.get("Origin", "")
    configured = [
        item.strip()
        for item in os.getenv("MONITOR_CORS_ORIGIN", "").split(",")
        if item.strip()
    ]

    allowed = False
    if origin:
        if configured:
            allowed = "*" in configured or origin in configured
        else:
            parsed_origin = urlparse(origin)
            request_host = request.host.split(":", 1)[0]
            origin_host = parsed_origin.hostname or ""
            allowed = (
                parsed_origin.scheme in {"http", "https"}
                and bool(origin_host)
                and (
                    origin_host in {"localhost", "127.0.0.1", "::1"}
                    or origin_host == request_host
                )
            )

    if allowed:
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Headers"] = "Authorization,X-API-Token,X-TOTP-Code,Content-Type"
        response.headers["Access-Control-Allow-Methods"] = "GET,OPTIONS"
        response.headers["Vary"] = "Origin"
    return response


async def index(_request: web.Request) -> web.Response:
    html = _request.app.get("index_html")
    if html is None:
        html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    return web.Response(
        text=html,
        content_type="text/html",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


async def auth_status(_request: web.Request) -> web.Response:
    return _json({
        "ok": True,
        "auth": {
            "token_required": True,
            "totp_required": bool(TOTP_SECRET),
        },
    })


async def health(request: web.Request) -> web.Response:
    pool: asyncpg.Pool = request.app["pool"]
    db_ok = False
    try:
        async with pool.acquire() as conn:
            db_ok = bool(await conn.fetchval("SELECT 1"))
    except Exception:
        db_ok = False

    redis_results = await asyncio.gather(
        _redis_ping(),
        _redis_get(SCHEDULER_HEARTBEAT_KEY),
        _redis_get(BOT_HEARTBEAT_KEY),
        _redis_get(MARKET_HEARTBEAT_KEY),
        _redis_get(RISK_HEARTBEAT_KEY),
        _redis_get(MONITOR_STATE_KEY),
        _redis_get(BOT_BUSY_KEY),
        return_exceptions=True,
    )
    redis_ok = bool(redis_results[0]) if not isinstance(redis_results[0], Exception) else False
    redis_values = [
        None if isinstance(value, Exception) else value
        for value in redis_results[1:]
    ]
    keys = {
        "scheduler": redis_values[0],
        "bot": redis_values[1],
        "market": redis_values[2],
        "risk": redis_values[3],
        "monitor_state": redis_values[4],
        "bot_busy": redis_values[5],
    }
    scheduler_age = _age_seconds(keys["scheduler"])
    bot_age = _age_seconds(keys["bot"])
    market_age = _age_seconds(keys["market"])
    risk_age = _age_seconds(keys["risk"])

    now = _now_art()
    business = is_trading_day(now)
    market_open = business and _is_market_hours(now)

    return _json({
        "ok": db_ok and redis_ok,
        "database": {"ok": db_ok},
        "redis": {"ok": redis_ok},
        "market": {
            "business_day": business,
            "open": market_open,
            "settlement_day": is_settlement_day(now),
            "closed_reason": market_closed_reason(now),
            "session_note": market_session_note(now),
            "now_art": now.isoformat(),
        },
        "services": {
            "scheduler": {
                "heartbeat_age_seconds": scheduler_age,
                "alive": (scheduler_age or 999999) < 90,
            },
            "telegram_bot": {
                "heartbeat_age_seconds": bot_age,
                "alive": (bot_age or 999999) < 90,
                "busy": bool(keys["bot_busy"]),
            },
            "intraday_monitor_state": keys["monitor_state"],
            "market_heartbeat_age_seconds": market_age,
            "risk_heartbeat_age_seconds": risk_age,
        },
    })


async def ingestion(request: web.Request) -> web.Response:
    pool: asyncpg.Pool = request.app["pool"]
    async with pool.acquire() as conn:
        latest_portfolio = await conn.fetchrow("""
            SELECT scraped_at, total_value_ars, cash_ars, confidence_score
            FROM portfolio_snapshots
            ORDER BY scraped_at DESC
            LIMIT 1
        """)
        portfolio_counts = await conn.fetchrow("""
            SELECT
                COUNT(*) FILTER (WHERE scraped_at >= NOW() - INTERVAL '24 hours') AS last_24h,
                COUNT(*) FILTER (WHERE scraped_at >= NOW() - INTERVAL '7 days') AS last_7d,
                COUNT(*) AS total
            FROM portfolio_snapshots
        """)
        latest_market = await conn.fetchrow("""
            SELECT
                MAX(ts) AS latest_ts,
                COUNT(*) FILTER (WHERE ts >= NOW() - INTERVAL '24 hours') AS rows_24h,
                COUNT(DISTINCT ticker) FILTER (WHERE ts >= NOW() - INTERVAL '24 hours') AS tickers_24h,
                COUNT(*) FILTER (WHERE ts >= NOW() - INTERVAL '7 days') AS rows_7d,
                COUNT(DISTINCT ticker) FILTER (WHERE ts >= NOW() - INTERVAL '7 days') AS tickers_7d
            FROM market_prices
        """)
        sample = await conn.fetch("""
            SELECT ticker, MAX(ts) AS latest_ts, COUNT(*) AS rows
            FROM market_prices
            WHERE ts >= NOW() - INTERVAL '7 days'
            GROUP BY ticker
            ORDER BY latest_ts DESC, ticker
            LIMIT 12
        """)
        asset_breakdown = await conn.fetch("""
            WITH latest AS (
                SELECT DISTINCT ON (ticker)
                    ticker, asset_type, ts
                FROM market_prices
                ORDER BY ticker, ts DESC
            )
            SELECT
                COALESCE(asset_type, 'UNKNOWN') AS asset_type,
                COUNT(*) AS tickers,
                COUNT(*) FILTER (WHERE ts >= NOW() - INTERVAL '24 hours') AS tickers_24h,
                COUNT(*) FILTER (WHERE ts >= NOW() - INTERVAL '7 days') AS tickers_7d
            FROM latest
            GROUP BY 1
            ORDER BY 1
        """)

    return _json({
        "ok": True,
        "portfolio": {
            "latest": _row(latest_portfolio),
            "counts": _row(portfolio_counts),
        },
        "market_prices": {
            "latest": _row(latest_market),
            "sample": [_row(r) for r in sample],
            "asset_breakdown": [_row(r) for r in asset_breakdown],
        },
    })


async def candles(request: web.Request) -> web.Response:
    pool: asyncpg.Pool = request.app["pool"]
    now = _now_art()
    business = is_trading_day(now)
    async with pool.acquire() as conn:
        coverage = await conn.fetchrow("""
            WITH latest_price_day AS (
                SELECT MAX((ts AT TIME ZONE 'America/Argentina/Buenos_Aires')::date) AS day
                FROM market_prices
            ),
            price_assets AS (
                SELECT COUNT(DISTINCT ticker) AS n
                FROM market_prices, latest_price_day
                WHERE (ts AT TIME ZONE 'America/Argentina/Buenos_Aires')::date = latest_price_day.day
            ),
            candle_assets AS (
                SELECT COUNT(DISTINCT ticker) AS n
                FROM market_candles, latest_price_day
                WHERE (ts AT TIME ZONE 'UTC')::date = latest_price_day.day
                  AND source = 'internal_snapshot'
            )
            SELECT
                latest_price_day.day AS business_day,
                price_assets.n AS price_assets,
                candle_assets.n AS internal_candles,
                GREATEST(price_assets.n - candle_assets.n, 0) AS missing_internal
            FROM latest_price_day, price_assets, candle_assets
        """)
        recent = await conn.fetch("""
            SELECT
                (ts AT TIME ZONE 'UTC')::date AS business_day,
                COUNT(*) AS rows,
                COUNT(DISTINCT ticker) AS tickers,
                MIN(ts) AS min_ts,
                MAX(ts) AS max_ts
            FROM market_candles
            WHERE ts >= NOW() - INTERVAL '14 days'
              AND source = 'internal_snapshot'
            GROUP BY 1
            ORDER BY 1 DESC
            LIMIT 10
        """)

    return _json({
        "ok": True,
        "market": {
            "business_day": business,
            "open": business and _is_market_hours(now),
            "settlement_day": is_settlement_day(now),
            "closed_reason": market_closed_reason(now),
            "session_note": market_session_note(now),
            "expects_daily_candle": business and now.time() >= time(18, 0),
        },
        "coverage": _row(coverage),
        "recent": [_row(r) for r in recent],
    })


async def decisions(request: web.Request) -> web.Response:
    days = max(1, min(int(request.query.get("days", "90")), 365))
    pool: asyncpg.Pool = request.app["pool"]
    async with pool.acquire() as conn:
        await ensure_decision_audit_scope_columns(conn)
        summary = await conn.fetchrow("""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE outcome_5d IS NULL) AS pending_5d,
                COUNT(*) FILTER (WHERE outcome_5d IS NOT NULL) AS closed_5d,
                COUNT(*) FILTER (WHERE source = 'execution_plan') AS execution_plan,
                COUNT(*) FILTER (WHERE status = 'BLOCKED') AS blocked,
                COUNT(*) FILTER (WHERE status = 'APPROVED') AS approved,
                COUNT(*) FILTER (WHERE status = 'EXECUTED') AS executed,
                COUNT(*) FILTER (WHERE is_primary_metric = TRUE) AS primary_metric,
                COUNT(*) FILTER (WHERE metric_scope = 'radar_audit') AS radar_audit,
                COUNT(*) FILTER (WHERE metric_scope = 'debug') AS debug_events
            FROM decision_log
            WHERE decided_at >= NOW() - ($1::int * INTERVAL '1 day')
        """, days)
        groups = await conn.fetch("""
            SELECT
                COALESCE(metric_scope, 'debug') AS metric_scope,
                COALESCE(run_intent, 'unknown') AS run_intent,
                COALESCE(source, layers->>'source', 'sin_source') AS source,
                COALESCE(status, 'UNKNOWN') AS status,
                COALESCE(decision_type, 'unknown') AS decision_type,
                decision,
                COUNT(*) AS n,
                COUNT(outcome_5d) FILTER (WHERE outcome_basis = 'canonical_cocos') AS con_5d,
                COUNT(outcome_10d) FILTER (WHERE outcome_basis = 'canonical_cocos') AS con_10d,
                COUNT(outcome_20d) FILTER (WHERE outcome_basis = 'canonical_cocos') AS con_20d
            FROM decision_log
            WHERE decided_at >= NOW() - ($1::int * INTERVAL '1 day')
            GROUP BY 1,2,3,4,5,6
            ORDER BY n DESC, metric_scope, source, status
            LIMIT 30
        """, days)
        recent = await conn.fetch("""
            SELECT decided_at, ticker, decision, status, source, final_score,
                   metric_scope, run_intent, decision_stage,
                   outcome_5d, outcome_basis, was_correct
            FROM decision_log
            WHERE decided_at >= NOW() - ($1::int * INTERVAL '1 day')
            ORDER BY decided_at DESC
            LIMIT 20
        """, days)

    return _json({
        "ok": True,
        "days": days,
        "summary": _row(summary),
        "groups": [_row(r) for r in groups],
        "recent": [_row(r) for r in recent],
    })


async def portfolio_view(request: web.Request) -> web.Response:
    days = max(7, min(int(request.query.get("days", "90")), 365))
    pool: asyncpg.Pool = request.app["pool"]
    corporate_applications = []
    async with pool.acquire() as conn:
        latest_snapshot = await conn.fetchrow("""
            SELECT snapshot_id, scraped_at, total_value_ars, cash_ars, confidence_score
            FROM portfolio_snapshots
            ORDER BY scraped_at DESC
            LIMIT 1
        """)
        positions = []
        allocation = []
        if latest_snapshot:
            positions = await conn.fetch("""
                SELECT
                    ticker,
                    COALESCE(asset_type, 'UNKNOWN') AS asset_type,
                    quantity,
                    avg_cost,
                    current_price,
                    market_value,
                    unrealized_pnl,
                    unrealized_pnl_pct,
                    weight_in_portfolio
                FROM positions
                WHERE snapshot_id = $1
                ORDER BY market_value DESC NULLS LAST, ticker
            """, latest_snapshot["snapshot_id"])
            allocation = await conn.fetch("""
                SELECT
                    COALESCE(asset_type, 'UNKNOWN') AS asset_type,
                    SUM(COALESCE(market_value, 0)) AS market_value,
                    COUNT(*) AS positions
                FROM positions
                WHERE snapshot_id = $1
                GROUP BY 1
                ORDER BY market_value DESC
            """, latest_snapshot["snapshot_id"])

            position_rows = [dict(row) for row in positions]
            tickers = [str(row.get("ticker") or "").upper() for row in position_rows]
            effects = await _load_monitor_corporate_effects(
                conn,
                since=latest_snapshot["scraped_at"],
                until=datetime.now(timezone.utc),
                tickers=tickers,
            )
            if effects:
                grouped_effects = effects_by_ticker(effects)
                reconciled_positions = []
                for position in position_rows:
                    ticker = str(position.get("ticker") or "").upper()
                    reconciled, applications = rebase_position_view(
                        position,
                        snapshot_at=latest_snapshot["scraped_at"],
                        as_of=datetime.now(timezone.utc),
                        effects=grouped_effects.get(ticker, ()),
                    )
                    reconciled_positions.append(reconciled)
                    corporate_applications.extend(applications)
                positions = reconciled_positions
                allocation_by_asset: dict[str, dict] = {}
                for position in positions:
                    asset_type = str(position.get("asset_type") or "UNKNOWN")
                    bucket = allocation_by_asset.setdefault(
                        asset_type,
                        {"asset_type": asset_type, "market_value": 0.0, "positions": 0},
                    )
                    bucket["market_value"] += _float(position.get("market_value"))
                    bucket["positions"] += 1
                allocation = sorted(
                    allocation_by_asset.values(),
                    key=lambda item: item["market_value"],
                    reverse=True,
                )

        history = await conn.fetch("""
            SELECT DISTINCT ON ((scraped_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date)
                (scraped_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date AS day,
                scraped_at,
                total_value_ars,
                cash_ars,
                confidence_score
            FROM portfolio_snapshots
            WHERE scraped_at >= NOW() - ($1::int * INTERVAL '1 day')
            ORDER BY (scraped_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date, scraped_at DESC
        """, days)

    snapshot_payload = _row(latest_snapshot)
    if latest_snapshot and corporate_applications:
        snapshot_payload["reported_total_value_ars"] = snapshot_payload.get("total_value_ars")
        snapshot_payload["total_value_ars"] = (
            sum(_float(position.get("market_value")) for position in positions)
            + _float(latest_snapshot["cash_ars"])
        )

    return _json({
        "ok": True,
        "days": days,
        "snapshot": snapshot_payload,
        "positions": [_row(r) for r in positions],
        "allocation": [_row(r) for r in allocation],
        "history": [_row(r) for r in history],
        "price_basis": (
            "corporate_action_reconciled" if corporate_applications else "reported"
        ),
        "corporate_action_applications": [
            {
                "event_id": application.event_id,
                "instrument_effect_id": application.instrument_effect_id,
                "component": application.component,
                "application_status": application.application_status,
                "idempotency_key": application.idempotency_key,
                "invariant_checks": application.invariant_checks,
            }
            for application in corporate_applications
        ],
    })


async def performance_view(request: web.Request) -> web.Response:
    days = max(7, min(int(request.query.get("days", "180")), 365))
    pool: asyncpg.Pool = request.app["pool"]
    async with pool.acquire() as conn:
        await ensure_decision_audit_scope_columns(conn)
        perf_base_cte = """
            WITH fill_link AS (
                SELECT
                    decision_log_id,
                    MIN(executed_at) AS fill_executed_at
                FROM broker_fills
                WHERE decision_log_id IS NOT NULL
                GROUP BY decision_log_id
            ),
            perf_base AS (
                SELECT
                    dl.id,
                    COALESCE(fl.fill_executed_at, dl.next_executable_at, dl.decided_at) AS effective_at,
                    dl.decided_at,
                    dl.ticker,
                    dl.decision,
                    dl.status,
                    COALESCE(dl.metric_scope, 'debug') AS metric_scope,
                    COALESCE(dl.run_intent, 'unknown') AS run_intent,
                    COALESCE(dl.source, dl.layers->>'source') AS source,
                    COALESCE(dl.layers->>'reason', dl.block_reason, '') AS reason,
                    dl.final_score,
                    dl.confidence,
                    dl.outcome_basis,
                    COALESCE(dl.is_primary_metric, FALSE) AS is_primary_metric,
                    COALESCE(dl.executable_was_correct, dl.was_correct) AS was_correct,
                    COALESCE(dl.executable_outcome_5d, dl.outcome_5d) AS outcome_5d,
                    COALESCE(dl.executable_outcome_10d, dl.outcome_10d) AS outcome_10d,
                    COALESCE(dl.executable_outcome_20d, dl.outcome_20d) AS outcome_20d,
                    dl.next_executable_at,
                    dl.next_executable_price,
                    dl.decision_type,
                    dl.signal_strength,
                    COALESCE(dl.delta_weight::float, 0.0) AS delta_weight,
                    CASE
                        WHEN dl.final_score IS NOT NULL AND ABS(dl.final_score) > 0.08
                            THEN 'SIGNAL_GENUINE'
                        WHEN COALESCE(dl.layers->>'reason', dl.block_reason, '') ILIKE '%rebalance%'
                          OR COALESCE(dl.layers->>'reason', dl.block_reason, '') ILIKE '%concentr%'
                          OR ABS(COALESCE(dl.delta_weight::float, 0.0)) >= 0.05
                            THEN 'REBALANCE'
                        ELSE 'WEAK_MECHANICAL'
                    END AS signal_family,
                    CASE
                        WHEN (dl.layers #>> '{trend_shadow,score}') ~ '^-?[0-9]+([.][0-9]+)?$'
                            THEN (dl.layers #>> '{trend_shadow,score}')::float
                        ELSE NULL
                    END AS trend_shadow_score,
                    dl.layers #>> '{trend_shadow,regime}' AS trend_shadow_regime,
                    ca.conclusion AS causal_conclusion,
                    ca.conclusion_reason AS causal_reason,
                    ca.analyzed_at AS causal_analyzed_at,
                    CASE
                        WHEN UPPER(dl.decision) <> 'BUY' THEN 'NO_BUY'
                        WHEN (
                            CASE
                                WHEN (dl.layers #>> '{trend_shadow,score}') ~ '^-?[0-9]+([.][0-9]+)?$'
                                    THEN (dl.layers #>> '{trend_shadow,score}')::float
                                ELSE NULL
                            END
                        ) >= 0.15
                         AND ca.conclusion = 'FUNDADO'
                            THEN 'SHADOW_CAUSAL'
                        WHEN (
                            CASE
                                WHEN (dl.layers #>> '{trend_shadow,score}') ~ '^-?[0-9]+([.][0-9]+)?$'
                                    THEN (dl.layers #>> '{trend_shadow,score}')::float
                                ELSE NULL
                            END
                        ) >= 0.15
                          OR dl.layers #>> '{trend_shadow,regime}' = 'STRONG_UPTREND'
                            THEN 'SHADOW_ONLY'
                        WHEN ca.conclusion = 'FUNDADO'
                            THEN 'CAUSAL_ONLY'
                        WHEN dl.layers #>> '{trend_shadow,score}' IS NULL AND ca.conclusion IS NULL
                            THEN 'NO_EVIDENCE'
                        ELSE 'UNCONFIRMED'
                    END AS buy_confirmation
                FROM decision_log dl
                LEFT JOIN fill_link fl ON fl.decision_log_id = dl.id
                LEFT JOIN LATERAL (
                    SELECT
                        conclusion,
                        conclusion_reason,
                        analyzed_at
                    FROM shadow_thesis_causal_analysis ca
                    WHERE UPPER(ca.ticker) = UPPER(dl.ticker)
                      AND ca.analyzed_at <= COALESCE(fl.fill_executed_at, dl.next_executable_at, dl.decided_at)
                    ORDER BY ca.analyzed_at DESC
                    LIMIT 1
                ) ca ON TRUE
            )
        """
        summary = await conn.fetchrow(perf_base_cte + """
            , real AS (
                SELECT
                    outcome_5d,
                    outcome_10d,
                    outcome_20d
                FROM perf_base
                WHERE effective_at >= NOW() - ($1::int * INTERVAL '1 day')
                  AND outcome_basis = 'canonical_cocos'
                  AND outcome_5d IS NOT NULL
                  AND was_correct IS NOT NULL
                  AND is_primary_metric = TRUE
            )
            SELECT
                COUNT(*) AS closed_5d,
                AVG(outcome_5d) AS avg_5d,
                AVG(outcome_10d) AS avg_10d,
                AVG(outcome_20d) AS avg_20d,
                AVG(CASE WHEN outcome_5d > 0 THEN 1.0 ELSE 0.0 END) AS win_rate_5d,
                AVG(outcome_5d) FILTER (WHERE outcome_5d > 0) AS avg_win_5d,
                AVG(outcome_5d) FILTER (WHERE outcome_5d < 0) AS avg_loss_5d,
                MAX(outcome_5d) AS best_5d,
                MIN(outcome_5d) AS worst_5d
            FROM real
        """, days)
        by_ticker = await conn.fetch(perf_base_cte + """
            SELECT
                ticker,
                COUNT(*) AS n,
                AVG(outcome_5d) AS avg_5d,
                AVG(CASE WHEN outcome_5d > 0 THEN 1.0 ELSE 0.0 END) AS win_rate_5d
            FROM perf_base
            WHERE effective_at >= NOW() - ($1::int * INTERVAL '1 day')
              AND outcome_basis = 'canonical_cocos'
              AND outcome_5d IS NOT NULL
              AND was_correct IS NOT NULL
              AND is_primary_metric = TRUE
            GROUP BY ticker
            ORDER BY n DESC, avg_5d DESC
            LIMIT 12
        """, days)
        score_points = await conn.fetch(perf_base_cte + """
            SELECT
                decided_at,
                effective_at,
                ticker,
                decision,
                status,
                metric_scope,
                run_intent,
                source,
                final_score,
                confidence,
                outcome_5d,
                outcome_10d,
                outcome_20d,
                next_executable_at,
                next_executable_price
            FROM perf_base
            WHERE effective_at >= NOW() - ($1::int * INTERVAL '1 day')
              AND outcome_basis = 'canonical_cocos'
              AND outcome_5d IS NOT NULL
              AND final_score IS NOT NULL
              AND metric_scope IN ('primary', 'planner_audit')
            ORDER BY effective_at DESC, decided_at DESC
            LIMIT 160
        """, days)
        status_counts = await conn.fetch(perf_base_cte + """
            SELECT
                metric_scope,
                COALESCE(source, 'sin_source') AS source,
                COALESCE(status, 'UNKNOWN') AS status,
                COUNT(*) AS n,
                COUNT(outcome_5d) FILTER (WHERE outcome_basis = 'canonical_cocos') AS closed_5d
            FROM perf_base
            WHERE effective_at >= NOW() - ($1::int * INTERVAL '1 day')
            GROUP BY 1, 2, 3
            ORDER BY n DESC
            LIMIT 16
        """, days)
        window_counts = await conn.fetchrow(perf_base_cte + """
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (
                    WHERE outcome_basis = 'canonical_cocos'
                      AND outcome_5d IS NOT NULL
                ) AS closed_any_5d,
                COUNT(*) FILTER (
                    WHERE outcome_basis = 'canonical_cocos'
                      AND outcome_5d IS NOT NULL
                      AND is_primary_metric = TRUE
                ) AS closed_primary_5d,
                COUNT(*) FILTER (
                    WHERE outcome_basis = 'canonical_cocos'
                      AND outcome_5d IS NOT NULL
                      AND is_primary_metric = FALSE
                ) AS closed_audit_5d,
                COUNT(*) FILTER (
                    WHERE outcome_5d IS NULL
                      AND is_primary_metric = TRUE
                ) AS pending_primary_5d
            FROM perf_base
            WHERE effective_at >= NOW() - ($1::int * INTERVAL '1 day')
        """, days)
        bot_prediction_summary = await conn.fetchrow(perf_base_cte + """
            , bot AS (
                SELECT
                    *,
                    outcome_5d AS directional_5d
                FROM perf_base
                WHERE effective_at >= NOW() - ($1::int * INTERVAL '1 day')
                  AND source = 'execution_plan'
                  AND metric_scope IN ('planner_audit', 'primary')
                  AND status IN ('APPROVED', 'EXECUTED', 'EXECUTED_MANUAL')
            )
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (
                    WHERE outcome_basis = 'canonical_cocos'
                      AND outcome_5d IS NOT NULL
                ) AS closed_5d,
                COUNT(*) FILTER (WHERE outcome_5d IS NULL) AS pending_5d,
                AVG(CASE
                    WHEN COALESCE(was_correct, directional_5d > 0) THEN 1.0
                    ELSE 0.0
                END) FILTER (
                    WHERE outcome_basis = 'canonical_cocos'
                      AND outcome_5d IS NOT NULL
                      AND directional_5d IS NOT NULL
                ) AS win_rate_5d,
                AVG(directional_5d) FILTER (
                    WHERE outcome_basis = 'canonical_cocos'
                      AND outcome_5d IS NOT NULL
                ) AS avg_directional_5d,
                MAX(directional_5d) FILTER (
                    WHERE outcome_basis = 'canonical_cocos'
                      AND outcome_5d IS NOT NULL
                ) AS best_directional_5d,
                MIN(directional_5d) FILTER (
                    WHERE outcome_basis = 'canonical_cocos'
                      AND outcome_5d IS NOT NULL
                ) AS worst_directional_5d
            FROM bot
        """, days)
        bot_direction_breakdown = await conn.fetch(perf_base_cte + """
            , bot AS (
                SELECT *
                FROM perf_base
                WHERE effective_at >= NOW() - ($1::int * INTERVAL '1 day')
                  AND source = 'execution_plan'
                  AND metric_scope IN ('planner_audit', 'primary')
                  AND status IN ('APPROVED', 'EXECUTED', 'EXECUTED_MANUAL')
            ),
            agg AS (
                SELECT
                    decision,
                    COUNT(*) AS total,
                    COUNT(*) FILTER (
                        WHERE outcome_basis = 'canonical_cocos'
                          AND outcome_5d IS NOT NULL
                    ) AS closed_5d,
                    COUNT(*) FILTER (WHERE outcome_5d IS NULL) AS pending_5d,
                    AVG(CASE WHEN outcome_5d > 0 THEN 1.0 ELSE 0.0 END) FILTER (
                        WHERE outcome_basis = 'canonical_cocos'
                          AND outcome_5d IS NOT NULL
                    ) AS win_rate_5d,
                    AVG(outcome_5d) FILTER (
                        WHERE outcome_basis = 'canonical_cocos'
                          AND outcome_5d IS NOT NULL
                    ) AS avg_5d,
                    AVG(outcome_5d) FILTER (
                        WHERE outcome_basis = 'canonical_cocos'
                          AND outcome_5d > 0
                    ) AS avg_win_5d,
                    AVG(outcome_5d) FILTER (
                        WHERE outcome_basis = 'canonical_cocos'
                          AND outcome_5d < 0
                    ) AS avg_loss_5d,
                    MAX(outcome_5d) FILTER (
                        WHERE outcome_basis = 'canonical_cocos'
                          AND outcome_5d IS NOT NULL
                    ) AS best_5d,
                    MIN(outcome_5d) FILTER (
                        WHERE outcome_basis = 'canonical_cocos'
                          AND outcome_5d IS NOT NULL
                    ) AS worst_5d
                FROM bot
                GROUP BY decision
            )
            SELECT
                *,
                CASE
                    WHEN avg_loss_5d IS NOT NULL AND avg_loss_5d < 0
                        THEN avg_win_5d / ABS(avg_loss_5d)
                    ELSE NULL
                END AS payoff_ratio
            FROM agg
            ORDER BY decision
        """, days)
        bot_signal_breakdown = await conn.fetch(perf_base_cte + """
            , bot AS (
                SELECT *
                FROM perf_base
                WHERE effective_at >= NOW() - ($1::int * INTERVAL '1 day')
                  AND source = 'execution_plan'
                  AND metric_scope IN ('planner_audit', 'primary')
                  AND status IN ('APPROVED', 'EXECUTED', 'EXECUTED_MANUAL')
            ),
            agg AS (
                SELECT
                    signal_family,
                    COUNT(*) AS total,
                    COUNT(*) FILTER (
                        WHERE outcome_basis = 'canonical_cocos'
                          AND outcome_5d IS NOT NULL
                    ) AS closed_5d,
                    COUNT(*) FILTER (WHERE outcome_5d IS NULL) AS pending_5d,
                    AVG(CASE WHEN outcome_5d > 0 THEN 1.0 ELSE 0.0 END) FILTER (
                        WHERE outcome_basis = 'canonical_cocos'
                          AND outcome_5d IS NOT NULL
                    ) AS win_rate_5d,
                    AVG(outcome_5d) FILTER (
                        WHERE outcome_basis = 'canonical_cocos'
                          AND outcome_5d IS NOT NULL
                    ) AS avg_5d,
                    AVG(outcome_5d) FILTER (
                        WHERE outcome_basis = 'canonical_cocos'
                          AND outcome_5d > 0
                    ) AS avg_win_5d,
                    AVG(outcome_5d) FILTER (
                        WHERE outcome_basis = 'canonical_cocos'
                          AND outcome_5d < 0
                    ) AS avg_loss_5d
                FROM bot
                GROUP BY signal_family
            )
            SELECT
                *,
                CASE
                    WHEN avg_loss_5d IS NOT NULL AND avg_loss_5d < 0
                        THEN avg_win_5d / ABS(avg_loss_5d)
                    ELSE NULL
                END AS payoff_ratio
            FROM agg
            ORDER BY closed_5d DESC, total DESC, signal_family
        """, days)
        source_breakdown = await conn.fetch(perf_base_cte + """
            SELECT
                COALESCE(source, 'sin_source') AS source,
                metric_scope,
                COUNT(*) AS total,
                COUNT(*) FILTER (
                    WHERE outcome_basis = 'canonical_cocos'
                      AND outcome_5d IS NOT NULL
                      AND was_correct IS NOT NULL
                ) AS closed_5d,
                AVG(CASE WHEN outcome_5d > 0 THEN 1.0 ELSE 0.0 END) FILTER (
                    WHERE outcome_basis = 'canonical_cocos'
                      AND outcome_5d IS NOT NULL
                      AND was_correct IS NOT NULL
                ) AS win_rate_5d,
                AVG(outcome_5d) FILTER (
                    WHERE outcome_basis = 'canonical_cocos'
                      AND outcome_5d IS NOT NULL
                      AND was_correct IS NOT NULL
                ) AS avg_5d,
                MIN(outcome_5d) FILTER (
                    WHERE outcome_basis = 'canonical_cocos'
                      AND outcome_5d IS NOT NULL
                      AND was_correct IS NOT NULL
                ) AS worst_5d,
                MAX(outcome_5d) FILTER (
                    WHERE outcome_basis = 'canonical_cocos'
                      AND outcome_5d IS NOT NULL
                      AND was_correct IS NOT NULL
                ) AS best_5d
            FROM perf_base
            WHERE effective_at >= NOW() - ($1::int * INTERVAL '1 day')
            GROUP BY source, metric_scope
            HAVING COUNT(*) > 0
            ORDER BY closed_5d DESC, total DESC, source, metric_scope
            LIMIT 12
        """, days)
        buy_confirmation_breakdown = await conn.fetch(perf_base_cte + """
            , bot_buy AS (
                SELECT *
                FROM perf_base
                WHERE effective_at >= NOW() - ($1::int * INTERVAL '1 day')
                  AND source = 'execution_plan'
                  AND metric_scope IN ('planner_audit', 'primary')
                  AND status IN ('APPROVED', 'EXECUTED', 'EXECUTED_MANUAL')
                  AND decision = 'BUY'
            )
            SELECT
                buy_confirmation,
                COUNT(*) AS total,
                COUNT(*) FILTER (
                    WHERE outcome_basis = 'canonical_cocos'
                      AND outcome_5d IS NOT NULL
                ) AS closed_5d,
                COUNT(*) FILTER (WHERE outcome_5d IS NULL) AS pending_5d,
                AVG(CASE WHEN outcome_5d > 0 THEN 1.0 ELSE 0.0 END) FILTER (
                    WHERE outcome_basis = 'canonical_cocos'
                      AND outcome_5d IS NOT NULL
                ) AS win_rate_5d,
                AVG(outcome_5d) FILTER (
                    WHERE outcome_basis = 'canonical_cocos'
                      AND outcome_5d IS NOT NULL
                ) AS avg_5d,
                AVG(trend_shadow_score) AS avg_shadow_score,
                COUNT(*) FILTER (WHERE causal_conclusion = 'FUNDADO') AS causal_founded,
                COUNT(*) FILTER (WHERE causal_conclusion = 'ESPECULATIVO') AS causal_speculative,
                COUNT(*) FILTER (WHERE causal_conclusion = 'MIXTO') AS causal_mixed
            FROM bot_buy
            GROUP BY buy_confirmation
            ORDER BY closed_5d DESC, total DESC, buy_confirmation
        """, days)
        evitable_loss = await conn.fetchrow(perf_base_cte + """
            , strong_negative_sell AS (
                SELECT *
                FROM perf_base
                WHERE effective_at >= NOW() - ($1::int * INTERVAL '1 day')
                  AND source = 'execution_plan'
                  AND metric_scope IN ('planner_audit', 'primary')
                  AND status IN ('APPROVED', 'EXECUTED', 'EXECUTED_MANUAL')
                  AND decision = 'SELL'
                  AND final_score < -0.08
                  AND outcome_basis = 'canonical_cocos'
                  AND outcome_5d IS NOT NULL
            )
            SELECT
                COUNT(*) AS closed_5d,
                COUNT(*) FILTER (WHERE outcome_5d > 0) AS correct_sells,
                COUNT(*) FILTER (WHERE outcome_5d < 0) AS false_alarms,
                AVG(CASE WHEN outcome_5d > 0 THEN 1.0 ELSE 0.0 END) AS hit_rate,
                AVG(outcome_5d) AS avg_directional_5d,
                AVG(outcome_5d) FILTER (WHERE outcome_5d > 0) AS avg_avoided_loss_5d,
                SUM(outcome_5d) FILTER (WHERE outcome_5d > 0) AS total_avoided_loss_5d,
                AVG(outcome_5d) FILTER (WHERE outcome_5d < 0) AS avg_false_alarm_5d,
                MIN(outcome_5d) AS worst_false_alarm_5d,
                MAX(outcome_5d) AS best_avoided_loss_5d
            FROM strong_negative_sell
        """, days)
        bot_prediction_recent = await conn.fetch(perf_base_cte + """
            SELECT
                decided_at,
                effective_at,
                ticker,
                decision,
                status,
                metric_scope,
                signal_family,
                buy_confirmation,
                trend_shadow_score,
                trend_shadow_regime,
                causal_conclusion,
                final_score,
                outcome_5d,
                outcome_5d AS directional_5d,
                CASE
                    WHEN outcome_5d IS NULL THEN NULL
                    ELSE COALESCE(
                        was_correct,
                        outcome_5d > 0
                    )
                END AS bot_was_right
            FROM perf_base
            WHERE effective_at >= NOW() - ($1::int * INTERVAL '1 day')
              AND source = 'execution_plan'
              AND metric_scope IN ('planner_audit', 'primary')
              AND status IN ('APPROVED', 'EXECUTED', 'EXECUTED_MANUAL')
            ORDER BY effective_at DESC, decided_at DESC
            LIMIT 12
        """, days)

    summary_dict = _row(summary)
    if int(summary_dict.get("closed_5d") or 0) > 0:
        win_rate = summary_dict.get("win_rate_5d") or 0
        avg_win = summary_dict.get("avg_win_5d") or 0
        avg_loss = abs(summary_dict.get("avg_loss_5d") or 0)
        summary_dict["ev_5d"] = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)
    else:
        summary_dict["ev_5d"] = None

    return _json({
        "ok": True,
        "days": days,
        "summary": summary_dict,
        "by_ticker": [_row(r) for r in by_ticker],
        "score_points": [_row(r) for r in score_points],
        "status_counts": [_row(r) for r in status_counts],
        "window_counts": _row(window_counts),
        "bot_predictions": _row(bot_prediction_summary),
        "bot_direction_breakdown": [_row(r) for r in bot_direction_breakdown],
        "bot_signal_breakdown": [_row(r) for r in bot_signal_breakdown],
        "source_breakdown": [_row(r) for r in source_breakdown],
        "buy_confirmation_breakdown": [_row(r) for r in buy_confirmation_breakdown],
        "evitable_loss": _row(evitable_loss),
        "bot_prediction_recent": [_row(r) for r in bot_prediction_recent],
    })


async def override_audit(request: web.Request) -> web.Response:
    days = max(7, min(int(request.query.get("days", "90")), 365))
    match_window_days = max(1, min(int(request.query.get("match_window_days", "2")), 10))
    pool: asyncpg.Pool = request.app["pool"]
    async with pool.acquire() as conn:
        await ensure_decision_audit_scope_columns(conn)
        rows = await conn.fetch("""
            WITH decision_base AS (
                SELECT
                    id,
                    decided_at,
                    ticker,
                    decision,
                    final_score,
                    price_at_decision,
                    COALESCE(run_intent, 'formal_plan') AS run_intent,
                    COALESCE(decision_stage, 'approved_decision') AS decision_stage,
                    COALESCE(metric_scope, 'planner_audit') AS metric_scope,
                    ABS(COALESCE(theoretical_amount_ars, executed_amount_ars, 0)) AS target_amount_ars,
                    COALESCE(executable_outcome_5d, outcome_5d) AS outcome_5d,
                    COALESCE(executable_outcome_10d, outcome_10d) AS outcome_10d,
                    COALESCE(executable_outcome_20d, outcome_20d) AS outcome_20d,
                    next_executable_at,
                    next_executable_price,
                    CASE
                        WHEN next_executable_at IS NOT NULL THEN next_executable_at
                        WHEN (decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::time >= TIME '17:00'
                            THEN ((((decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date + 1) + TIME '10:30') AT TIME ZONE 'America/Argentina/Buenos_Aires')
                        WHEN (decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::time < TIME '10:30'
                            THEN (((decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date + TIME '10:30') AT TIME ZONE 'America/Argentina/Buenos_Aires')
                        ELSE decided_at
                    END AS provisional_match_start_at,
                    (
                        (decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::time >= TIME '17:00'
                        OR (decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::time < TIME '10:30'
                    ) AS needs_open_revalidation,
                    layers->>'reason' AS reason
                FROM decision_log
                WHERE decided_at >= NOW() - ($1::int * INTERVAL '1 day')
                  AND COALESCE(source, layers->>'source') = 'execution_plan'
                  AND COALESCE(run_intent, 'formal_plan') = 'formal_plan'
                  AND COALESCE(metric_scope, 'planner_audit') IN ('planner_audit', 'primary')
                  AND status = 'APPROVED'
                  AND decision_type = 'executable'
                  AND decision IN ('BUY', 'SELL')
                  AND price_at_decision IS NOT NULL
            ),
            decisions AS (
                SELECT
                    d.*,
                    CASE
                        WHEN d.next_executable_at IS NOT NULL THEN d.next_executable_at
                        WHEN d.needs_open_revalidation THEN open_price.first_price_at
                        ELSE d.provisional_match_start_at
                    END AS match_start_at,
                    CASE
                        WHEN d.next_executable_at IS NOT NULL
                            THEN (d.next_executable_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date
                        WHEN d.needs_open_revalidation AND open_price.first_price_at IS NOT NULL
                            THEN (open_price.first_price_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date
                        WHEN NOT d.needs_open_revalidation
                            THEN (d.provisional_match_start_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date
                        ELSE NULL
                    END AS match_day,
                    CASE
                        WHEN d.next_executable_at IS NOT NULL THEN 'next_executable'
                        WHEN d.needs_open_revalidation AND open_price.first_price_at IS NOT NULL THEN 'fresh_open_price'
                        WHEN d.needs_open_revalidation THEN 'pending_open_revalidation'
                        ELSE 'intraday'
                    END AS match_basis
                FROM decision_base d
                LEFT JOIN LATERAL (
                    SELECT MIN(mp.ts) AS first_price_at
                    FROM market_prices mp
                    WHERE mp.ticker = d.ticker
                      AND mp.last_price IS NOT NULL
                      AND mp.last_price > 0
                      AND mp.ts >= d.provisional_match_start_at
                      AND mp.ts < d.provisional_match_start_at + INTERVAL '1 day'
                ) open_price ON TRUE
            )
            SELECT
                d.*,
                same_fill.first_at AS same_executed_at,
                same_fill.executed_at_precision AS same_executed_at_precision,
                same_fill.executed_at_source AS same_executed_at_source,
                same_fill.amount_ars AS same_amount_ars,
                opposite_fill.first_at AS opposite_executed_at,
                opposite_fill.executed_at_precision AS opposite_executed_at_precision,
                opposite_fill.executed_at_source AS opposite_executed_at_source,
                opposite_fill.amount_ars AS opposite_amount_ars,
                CASE
                    WHEN COALESCE(same_fill.amount_ars, 0) / GREATEST(d.target_amount_ars, 1) >= 0.75 THEN 'FOLLOWED'
                    WHEN COALESCE(same_fill.amount_ars, 0) / GREATEST(d.target_amount_ars, 1) >= 0.15 THEN 'PARTIAL'
                    WHEN COALESCE(opposite_fill.amount_ars, 0) / GREATEST(d.target_amount_ars, 1) >= 0.15 THEN 'OPPOSITE'
                    ELSE 'IGNORED'
                END AS override_status
            FROM decisions d
            LEFT JOIN LATERAL (
                SELECT
                    MIN(executed_at) AS first_at,
                    (ARRAY_AGG(COALESCE(executed_at_precision, 'unknown') ORDER BY executed_at, id))[1] AS executed_at_precision,
                    (ARRAY_AGG(COALESCE(executed_at_source, 'unknown') ORDER BY executed_at, id))[1] AS executed_at_source,
                    SUM(ABS(COALESCE(amount, quantity * price, 0))) AS amount_ars
                FROM broker_movements bm
                WHERE bm.ticker = d.ticker
                  AND bm.movement_type = d.decision
                  AND d.match_start_at IS NOT NULL
                  AND (
                      (
                          bm.executed_at >= d.match_start_at
                          AND bm.executed_at < d.match_start_at + ($2::int * INTERVAL '1 day')
                      )
                      OR (
                          d.match_day IS NOT NULL
                          AND (bm.executed_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date >= d.match_day
                          AND (bm.executed_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date < d.match_day + $2::int
                      )
                  )
                  AND bm.quantity IS NOT NULL
                  AND bm.price IS NOT NULL
            ) same_fill ON TRUE
            LEFT JOIN LATERAL (
                SELECT
                    MIN(executed_at) AS first_at,
                    (ARRAY_AGG(COALESCE(executed_at_precision, 'unknown') ORDER BY executed_at, id))[1] AS executed_at_precision,
                    (ARRAY_AGG(COALESCE(executed_at_source, 'unknown') ORDER BY executed_at, id))[1] AS executed_at_source,
                    SUM(ABS(COALESCE(amount, quantity * price, 0))) AS amount_ars
                FROM broker_movements bm
                WHERE bm.ticker = d.ticker
                  AND bm.movement_type = CASE WHEN d.decision = 'BUY' THEN 'SELL' ELSE 'BUY' END
                  AND d.match_start_at IS NOT NULL
                  AND (
                      (
                          bm.executed_at >= d.match_start_at
                          AND bm.executed_at < d.match_start_at + ($2::int * INTERVAL '1 day')
                      )
                      OR (
                          d.match_day IS NOT NULL
                          AND (bm.executed_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date >= d.match_day
                          AND (bm.executed_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date < d.match_day + $2::int
                      )
                  )
                  AND bm.quantity IS NOT NULL
                  AND bm.price IS NOT NULL
            ) opposite_fill ON TRUE
            ORDER BY d.decided_at DESC
        """, days, match_window_days)

    items = [_row(r) for r in rows]
    for item in items:
        item["override_status"] = _classify_override(item)
        item["same_ratio"] = _override_same_ratio(item)
        item["opposite_ratio"] = _override_opposite_ratio(item)

    by_status: dict[str, int] = {}
    unique_intents = set()
    closed = []
    bot_returns: list[float] = []
    override_deltas: list[float] = []
    for item in items:
        status = item.get("override_status") or "UNKNOWN"
        by_status[status] = by_status.get(status, 0) + 1
        unique_intents.add((item.get("ticker"), item.get("decision")))
        if item.get("outcome_5d") is not None:
            closed.append(item)
            bot_returns.append(_float(item.get("outcome_5d")))
            delta = _override_delta(status, item.get("outcome_5d"))
            if delta is not None:
                override_deltas.append(delta)

    bot_wins = sum(1 for item in closed if item.get("override_status") in {"IGNORED", "OPPOSITE"} and (item.get("outcome_5d") or 0) > 0)
    human_wins = sum(1 for item in closed if item.get("override_status") in {"IGNORED", "OPPOSITE"} and (item.get("outcome_5d") or 0) < 0)
    by_intent = _override_intent_summary(items)

    return _json({
        "ok": True,
        "days": days,
        "match_window_days": match_window_days,
        "summary": {
            "plans": len(items),
            "unique_intents": len(unique_intents),
            "repeated_plans": max(0, len(items) - len(unique_intents)),
            "closed_5d": len(closed),
            "by_status": by_status,
            "by_intent": by_intent,
            "avg_bot_5d": _mean(bot_returns),
            "avg_override_delta_5d": _mean(override_deltas),
            "bot_wins_ignored": bot_wins,
            "human_wins_ignored": human_wins,
        },
        "matches": [
            {
                "ticker": item.get("ticker"),
                "decision": item.get("decision"),
                "decided_at": item.get("decided_at"),
                "override_status": item.get("override_status"),
            }
            for item in items
        ],
        "recent": items[:30],
    })


async def decision_ledger(request: web.Request) -> web.Response:
    days = max(7, min(int(request.query.get("days", "90")), 365))
    match_window_days = max(1, min(int(request.query.get("match_window_days", "2")), 10))
    owner_chat_id = request.query.get("owner_chat_id")
    owner = int(owner_chat_id) if owner_chat_id else None
    pool: asyncpg.Pool = request.app["pool"]
    async with pool.acquire() as conn:
        data = await fetch_decision_ledger(
            conn,
            days=days,
            match_window_days=match_window_days,
            owner_chat_id=owner,
        )
    return _json({"ok": True, **data})


async def radar_audit(request: web.Request) -> web.Response:
    days = max(7, min(int(request.query.get("days", "90")), 365))
    pool: asyncpg.Pool = request.app["pool"]
    async with pool.acquire() as conn:
        await ensure_decision_audit_scope_columns(conn)
        rows = await conn.fetch("""
            WITH radar AS (
                SELECT
                    id,
                    decided_at,
                    (decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date AS decision_day,
                    COALESCE(
                        (next_executable_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date,
                        (decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date
                    ) AS audit_start_day,
                    ticker,
                    decision,
                    final_score,
                    confidence,
                    status,
                    decision_type,
                    COALESCE(run_intent, 'scheduled_context') AS run_intent,
                    COALESCE(decision_stage, 'idea') AS decision_stage,
                    COALESCE(metric_scope, 'radar_audit') AS metric_scope,
                    price_at_decision,
                    rr_ratio,
                    block_reason,
                    outcome_5d,
                    outcome_10d,
                    outcome_20d,
                    executable_outcome_5d,
                    executable_outcome_10d,
                    executable_outcome_20d,
                    COALESCE(NULLIF(next_executable_price, 0), price_at_decision) AS audit_entry_price,
                    layers
                FROM decision_log
                WHERE decided_at >= NOW() - ($1::int * INTERVAL '1 day')
                  AND COALESCE(source, layers->>'source') = 'radar'
                  AND COALESCE(metric_scope, 'radar_audit') = 'radar_audit'
                  AND decision IN ('BUY', 'SELL')
                  AND price_at_decision IS NOT NULL
                  AND price_at_decision > 0
            )
            SELECT
                r.*,
                COALESCE(r.layers->>'candidate_status', r.status) AS candidate_status,
                r.layers->>'trade_type' AS trade_type,
                r.layers->>'edge_label' AS edge_label,
                (r.layers->>'edge')::float AS edge,
                r.layers->>'technical_data_source_mode' AS technical_source,
                path.price_2d,
                path.close_5d,
                path.close_10d,
                path.close_20d,
                path.mae_10d,
                path.mfe_10d,
                CASE
                    WHEN r.decision = 'SELL' AND path.price_2d IS NOT NULL
                        THEN (r.audit_entry_price / path.price_2d) - 1
                    WHEN path.price_2d IS NOT NULL
                        THEN (path.price_2d / r.audit_entry_price) - 1
                    ELSE NULL
                END AS outcome_2d,
                COALESCE(r.executable_outcome_5d, r.outcome_5d, CASE
                    WHEN r.decision = 'SELL' AND path.close_5d IS NOT NULL
                        THEN (r.audit_entry_price / path.close_5d) - 1
                    WHEN path.close_5d IS NOT NULL
                        THEN (path.close_5d / r.audit_entry_price) - 1
                    ELSE NULL
                END) AS outcome_5d,
                COALESCE(r.executable_outcome_10d, r.outcome_10d, CASE
                    WHEN r.decision = 'SELL' AND path.close_10d IS NOT NULL
                        THEN (r.audit_entry_price / path.close_10d) - 1
                    WHEN path.close_10d IS NOT NULL
                        THEN (path.close_10d / r.audit_entry_price) - 1
                    ELSE NULL
                END) AS outcome_10d,
                COALESCE(r.executable_outcome_20d, r.outcome_20d, CASE
                    WHEN r.decision = 'SELL' AND path.close_20d IS NOT NULL
                        THEN (r.audit_entry_price / path.close_20d) - 1
                    WHEN path.close_20d IS NOT NULL
                        THEN (path.close_20d / r.audit_entry_price) - 1
                    ELSE NULL
                END) AS outcome_20d
            FROM radar r
            LEFT JOIN LATERAL (
                WITH candles AS (
                    SELECT
                        ts::date AS day,
                        close_price::float AS close_price,
                        high_price::float AS high_price,
                        low_price::float AS low_price
                    FROM market_candles
                    WHERE ticker = r.ticker
                      AND ts::date >= r.audit_start_day
                      AND ts::date <= r.audit_start_day + 20
                      AND close_price IS NOT NULL
                    ORDER BY ts ASC
                )
                SELECT
                    (SELECT close_price FROM candles WHERE day >= r.audit_start_day + 2 LIMIT 1) AS price_2d,
                    (SELECT close_price FROM candles WHERE day >= r.audit_start_day + 5 LIMIT 1) AS close_5d,
                    (SELECT close_price FROM candles WHERE day >= r.audit_start_day + 10 LIMIT 1) AS close_10d,
                    (SELECT close_price FROM candles WHERE day >= r.audit_start_day + 20 LIMIT 1) AS close_20d,
                    CASE
                        WHEN r.decision = 'SELL'
                            THEN MIN((r.audit_entry_price / NULLIF(high_price, 0)) - 1)
                        ELSE MIN((low_price / NULLIF(r.audit_entry_price, 0)) - 1)
                    END AS mae_10d,
                    CASE
                        WHEN r.decision = 'SELL'
                            THEN MAX((r.audit_entry_price / NULLIF(low_price, 0)) - 1)
                        ELSE MAX((high_price / NULLIF(r.audit_entry_price, 0)) - 1)
                    END AS mfe_10d
                FROM candles
                WHERE day <= r.audit_start_day + 10
            ) path ON TRUE
            ORDER BY r.decided_at DESC, r.id DESC
            LIMIT 160
        """, days)

    items = [_row(r) for r in rows]
    for item in items:
        item["path_risk"] = _path_risk_label(item.get("mae_10d"))

    closed_5d = [item for item in items if item.get("outcome_5d") is not None]
    closed_10d = [item for item in items if item.get("outcome_10d") is not None]
    high_path_risk = sum(1 for item in items if item.get("path_risk") == "HIGH")
    executable = sum(1 for item in items if str(item.get("status") or "").upper() == "THEORETICAL")
    blocked = sum(1 for item in items if str(item.get("status") or "").upper() == "BLOCKED")

    def _wins(values: list[dict], key: str) -> int:
        return sum(1 for item in values if _float(item.get(key)) > 0)

    return _json({
        "ok": True,
        "days": days,
        "summary": {
            "total": len(items),
            "theoretical": executable,
            "blocked": blocked,
            "closed_5d": len(closed_5d),
            "closed_10d": len(closed_10d),
            "win_rate_5d": (_wins(closed_5d, "outcome_5d") / len(closed_5d)) if closed_5d else None,
            "win_rate_10d": (_wins(closed_10d, "outcome_10d") / len(closed_10d)) if closed_10d else None,
            "avg_2d": _mean([_float(item.get("outcome_2d")) for item in items if item.get("outcome_2d") is not None]),
            "avg_5d": _mean([_float(item.get("outcome_5d")) for item in closed_5d]),
            "avg_10d": _mean([_float(item.get("outcome_10d")) for item in closed_10d]),
            "avg_mae_10d": _mean([_float(item.get("mae_10d")) for item in items if item.get("mae_10d") is not None]),
            "avg_mfe_10d": _mean([_float(item.get("mfe_10d")) for item in items if item.get("mfe_10d") is not None]),
            "high_path_risk": high_path_risk,
        },
        "chart_items": items,
        "recent": items[:40],
    })


async def human_activity(request: web.Request) -> web.Response:
    days = max(1, min(int(request.query.get("days", "7")), 30))
    pool: asyncpg.Pool = request.app["pool"]
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            WITH snaps AS (
                SELECT
                    snapshot_id,
                    scraped_at,
                    cash_ars,
                    LAG(snapshot_id) OVER (ORDER BY scraped_at) AS prev_snapshot_id,
                    LAG(scraped_at) OVER (ORDER BY scraped_at) AS prev_scraped_at,
                    LEAD(snapshot_id) OVER (ORDER BY scraped_at) AS next_snapshot_id
                FROM portfolio_snapshots
                WHERE scraped_at >= NOW() - ($1::int * INTERVAL '1 day')
                ORDER BY scraped_at
            ),
            pairs AS (
                SELECT *
                FROM snaps
                WHERE prev_snapshot_id IS NOT NULL
            ),
            pair_tickers AS (
                SELECT DISTINCT
                    p.snapshot_id,
                    p.prev_snapshot_id,
                    p.next_snapshot_id,
                    p.scraped_at,
                    p.prev_scraped_at,
                    pos.ticker
                FROM pairs p
                JOIN positions pos
                  ON pos.snapshot_id IN (p.snapshot_id, p.prev_snapshot_id)
            ),
            deltas AS (
                SELECT
                    pt.prev_scraped_at,
                    pt.scraped_at,
                    pt.next_snapshot_id,
                    pt.ticker,
                    COALESCE(prev.quantity, 0)::float AS previous_quantity,
                    COALESCE(cur.quantity, 0)::float AS current_quantity,
                    COALESCE(nxt.quantity, 0)::float AS next_quantity,
                    COALESCE(cur.quantity, 0)::float - COALESCE(prev.quantity, 0)::float AS quantity_delta,
                    COALESCE(cur.current_price, prev.current_price)::float AS reference_price,
                    COALESCE(cur.market_value, 0)::float AS current_market_value,
                    COALESCE(prev.market_value, 0)::float AS previous_market_value
                FROM pair_tickers pt
                LEFT JOIN positions cur
                    ON cur.snapshot_id = pt.snapshot_id
                   AND cur.ticker = pt.ticker
                LEFT JOIN positions prev
                    ON prev.snapshot_id = pt.prev_snapshot_id
                   AND prev.ticker = pt.ticker
                LEFT JOIN positions nxt
                    ON nxt.snapshot_id = pt.next_snapshot_id
                   AND nxt.ticker = pt.ticker
            )
            SELECT
                d.prev_scraped_at,
                d.scraped_at,
                d.ticker,
                d.previous_quantity,
                d.current_quantity,
                CASE WHEN d.quantity_delta > 0 THEN 'BUY' ELSE 'SELL' END AS side,
                ABS(d.quantity_delta) AS quantity,
                d.reference_price,
                ABS(d.quantity_delta * COALESCE(d.reference_price, 0)) AS inferred_amount_ars,
                bm.executed_at AS confirmed_at,
                bm.amount AS confirmed_amount_ars,
                bm.price AS confirmed_price
            FROM deltas d
            LEFT JOIN LATERAL (
                SELECT executed_at, amount, price
                FROM broker_movements bm
                WHERE bm.ticker = d.ticker
                  AND bm.movement_type = CASE WHEN d.quantity_delta > 0 THEN 'BUY' ELSE 'SELL' END
                  AND bm.executed_at >= d.prev_scraped_at - INTERVAL '15 minutes'
                  AND bm.executed_at <= d.scraped_at + INTERVAL '12 hours'
                  AND bm.quantity IS NOT NULL
                  AND bm.price IS NOT NULL
                ORDER BY ABS(EXTRACT(EPOCH FROM (bm.executed_at - d.scraped_at))) ASC
                LIMIT 1
            ) bm ON TRUE
            WHERE d.ticker IS NOT NULL
              AND ABS(d.quantity_delta) > 0.000001
              AND ABS(d.quantity_delta * COALESCE(d.reference_price, 0)) >= 1000
              AND ABS(d.quantity_delta) / GREATEST(ABS(d.previous_quantity), ABS(d.current_quantity), 1) >= 0.01
              AND d.next_snapshot_id IS NOT NULL
              AND ABS(d.next_quantity - d.current_quantity) <= 0.000001
            ORDER BY d.scraped_at DESC, d.ticker
            LIMIT 50
        """, days)

        activity_effects = await _load_monitor_corporate_effects(
            conn,
            since=datetime.now(timezone.utc) - timedelta(days=days + 2),
            until=datetime.now(timezone.utc),
            tickers=[str(row["ticker"] or "").upper() for row in rows],
        )

    grouped_effects = effects_by_ticker(activity_effects)
    items = []
    for row in rows:
        raw = dict(row)
        effect = None
        if not raw.get("confirmed_at"):
            effect = matching_effect_for_quantity_transition(
                ticker=str(raw.get("ticker") or ""),
                previous_quantity=raw.get("previous_quantity"),
                current_quantity=raw.get("current_quantity"),
                previous_at=raw.get("prev_scraped_at"),
                current_at=raw.get("scraped_at"),
                effects=grouped_effects.get(str(raw.get("ticker") or "").upper(), ()),
            )
        item = _row(raw)
        item["activity_type"] = "CORPORATE_ACTION" if effect else "HUMAN_TRADE_CANDIDATE"
        if effect:
            item["side"] = "CORPORATE_ACTION"
            item["corporate_action"] = {
                "event_key": effect.event_key,
                "event_type": effect.event_type,
                "effective_at": effect.effective_at.isoformat(),
                "quantity_factor": effect.quantity_factor,
                "price_factor": effect.price_factor,
                "source_name": effect.source_name,
                "source_url": effect.source_url,
            }
        items.append(item)

    human_items = [item for item in items if item["activity_type"] != "CORPORATE_ACTION"]
    corporate_items = [item for item in items if item["activity_type"] == "CORPORATE_ACTION"]
    confirmed = sum(1 for item in human_items if item.get("confirmed_at"))
    pending = len(human_items) - confirmed
    return _json({
        "ok": True,
        "days": days,
        "summary": {
            "total": len(human_items),
            "confirmed": confirmed,
            "pending": pending,
            "corporate_actions": len(corporate_items),
            "scope": "inferred_from_portfolio_snapshots",
            "note": (
                "Provisional: no entra al EV principal hasta que Cocos movements confirme "
                "el movimiento. Corporate actions confirmadas se informan por separado."
            ),
        },
        "recent": items,
    })


async def corporate_actions_view(request: web.Request) -> web.Response:
    days = max(1, min(int(request.query.get("days", "30")), 3650))
    pool: asyncpg.Pool = request.app["pool"]
    async with pool.acquire() as conn:
        if not await _corporate_action_schema_ready(conn):
            return _json({
                "ok": True,
                "available": False,
                "days": days,
                "events": [],
                "price_quality_flags": [],
                "applications": [],
            })
        events = await conn.fetch(
            """
            SELECT
                e.id AS event_id,
                e.event_key,
                e.issuer_id,
                e.event_type,
                e.lifecycle_status,
                e.effective_at,
                e.source_name,
                e.source_url,
                e.ingestion_method,
                e.evidence_level,
                e.detector_score,
                effect.id AS instrument_effect_id,
                effect.instrument_id,
                effect.ticker,
                effect.venue,
                effect.asset_type,
                effect.currency,
                effect.quantity_factor,
                effect.price_factor,
                effect.cost_basis_factor
            FROM corporate_events e
            JOIN corporate_event_instrument_effects effect ON effect.event_id = e.id
            WHERE e.effective_at >= NOW() - ($1::int * INTERVAL '1 day')
            ORDER BY e.effective_at DESC, e.id DESC, effect.id DESC
            LIMIT 100
            """,
            days,
        )
        flags = await conn.fetch(
            """
            SELECT
                ticker, observed_at, expires_at, flag_type, resolution_status,
                observed_return, expected_price_factor, observed_quantity_factor,
                quantity_factor, evidence_level, detector_score, detector_version,
                action_taken, reason, event_id, instrument_effect_id
            FROM price_quality_flags
            WHERE observed_at >= NOW() - ($1::int * INTERVAL '1 day')
            ORDER BY observed_at DESC, id DESC
            LIMIT 100
            """,
            days,
        )
        applications = await conn.fetch(
            """
            SELECT
                application.created_at, application.applied_at,
                application.component, application.application_status,
                application.adjustment_version, application.idempotency_key,
                application.invariant_checks, application.error,
                event.event_key, effect.ticker
            FROM corporate_event_applications application
            JOIN corporate_events event ON event.id = application.event_id
            JOIN corporate_event_instrument_effects effect
              ON effect.id = application.instrument_effect_id
            WHERE application.created_at >= NOW() - ($1::int * INTERVAL '1 day')
            ORDER BY application.created_at DESC, application.id DESC
            LIMIT 100
            """,
            days,
        )

    return _json({
        "ok": True,
        "available": True,
        "days": days,
        "events": [_row(row) for row in events],
        "price_quality_flags": [_row(row) for row in flags],
        "applications": [_row(row) for row in applications],
    })


async def fills(request: web.Request) -> web.Response:
    days = max(1, min(int(request.query.get("days", "90")), 365))
    limit = max(1, min(int(request.query.get("limit", "80")), 300))
    pool: asyncpg.Pool = request.app["pool"]
    async with pool.acquire() as conn:
        summary = await conn.fetchrow("""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE executed_at >= NOW() - INTERVAL '24 hours') AS last_24h,
                COUNT(*) FILTER (WHERE executed_at >= NOW() - INTERVAL '7 days') AS last_7d,
                COUNT(*) FILTER (WHERE decision_log_id IS NOT NULL) AS reconciled,
                COUNT(*) FILTER (WHERE decision_log_id IS NULL) AS unreconciled,
                MAX(executed_at) AS latest_executed_at
            FROM broker_fills
            WHERE executed_at >= NOW() - ($1::int * INTERVAL '1 day')
        """, days)
        by_source = await conn.fetch("""
            SELECT source, COUNT(*) AS n, MAX(executed_at) AS latest_executed_at
            FROM broker_fills
            WHERE executed_at >= NOW() - ($1::int * INTERVAL '1 day')
            GROUP BY source
            ORDER BY n DESC, source
        """, days)
        recent = await conn.fetch("""
            SELECT executed_at, ticker, side, quantity, avg_fill_price,
                   gross_amount_ars, source, decision_log_id, reconciled_at
            FROM broker_fills
            WHERE executed_at >= NOW() - ($1::int * INTERVAL '1 day')
            ORDER BY executed_at DESC
            LIMIT $2
        """, days, limit)
        movements_summary = await conn.fetchrow("""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE executed_at >= NOW() - INTERVAL '24 hours') AS last_24h,
                COUNT(*) FILTER (
                    WHERE movement_type IN ('BUY', 'SELL')
                      AND ticker IS NOT NULL
                      AND quantity IS NOT NULL
                      AND price IS NOT NULL
                ) AS trades,
                MAX(executed_at) AS latest_executed_at
            FROM broker_movements
            WHERE executed_at >= NOW() - ($1::int * INTERVAL '1 day')
        """, days)
        movements_recent = await conn.fetch("""
            SELECT executed_at, settlement_date, ticker, movement_type,
                   quantity, price, amount, currency, instrument_type
            FROM broker_movements
            WHERE executed_at >= NOW() - ($1::int * INTERVAL '1 day')
              AND movement_type IN ('BUY', 'SELL')
              AND ticker IS NOT NULL
              AND quantity IS NOT NULL
              AND price IS NOT NULL
            ORDER BY executed_at DESC, id DESC
            LIMIT $2
        """, days, limit)

    return _json({
        "ok": True,
        "days": days,
        "limit": limit,
        "summary": _row(summary),
        "by_source": [_row(r) for r in by_source],
        "recent": [_row(r) for r in recent],
        "movements": {
            "summary": _row(movements_summary),
            "recent": [_row(r) for r in movements_recent],
        },
    })


async def learning_shadow_view(request: web.Request) -> web.Response:
    """Counterfactual metrics for blocked decisions; never feeds analysis."""
    days = max(30, min(int(request.query.get("days", "365")), 730))
    try:
        owner_chat_id = int(request.query.get("owner_chat_id", "0"))
    except (TypeError, ValueError):
        return _json({"ok": False, "error": "owner_chat_id invalido"}, status=400)

    pool: asyncpg.Pool = request.app["pool"]
    async with pool.acquire() as conn:
        schema_ready = await conn.fetchval(
            """
            SELECT
                to_regclass('public.learning_shadow_runs') IS NOT NULL
                AND to_regclass('public.learning_shadow_cases') IS NOT NULL
                AND to_regclass('public.learning_shadow_metric_snapshots') IS NOT NULL
            """
        )
        if not schema_ready:
            return _json({
                "ok": True,
                "available": False,
                "owner_chat_id": owner_chat_id,
                "days": days,
                "note": "Learning shadow todavia no fue inicializado.",
                "run": None,
                "metrics": [],
                "trend": [],
                "cohorts": [],
                "by_block_reason": [],
                "recent_cases": [],
            })

        latest_run = await conn.fetchrow(
            """
            SELECT *
            FROM learning_shadow_runs
            WHERE owner_chat_id = $1
            ORDER BY captured_at DESC
            LIMIT 1
            """,
            owner_chat_id,
        )
        if not latest_run:
            return _json({
                "ok": True,
                "available": False,
                "owner_chat_id": owner_chat_id,
                "days": days,
                "note": "Learning shadow no tiene corridas para este owner.",
                "run": None,
                "metrics": [],
                "trend": [],
                "cohorts": [],
                "by_block_reason": [],
                "recent_cases": [],
            })

        metrics = await conn.fetch(
            """
            SELECT *
            FROM learning_shadow_metric_snapshots
            WHERE run_id = $1
            ORDER BY horizon_days
            """,
            latest_run["run_id"],
        )
        trend = await conn.fetch(
            """
            SELECT
                captured_at, snapshot_date, horizon_days, total_cases,
                matured_cases, potential_false_negatives,
                potential_false_negative_rate, shadow_coverage_rate,
                missing_outcome_cases, excluded_cases
            FROM learning_shadow_metric_snapshots
            WHERE owner_chat_id = $1
              AND captured_at >= NOW() - ($2::int * INTERVAL '1 day')
            ORDER BY captured_at, horizon_days
            """,
            owner_chat_id,
            days,
        )
        cohorts = await conn.fetch(
            """
            SELECT
                date_trunc('week', decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date
                    AS cohort_date,
                horizon_days,
                COUNT(*)::integer AS total_cases,
                COUNT(*) FILTER (WHERE classification IN (
                    'POTENTIAL_FALSE_NEGATIVE', 'POSITIVE_BELOW_THRESHOLD',
                    'NON_POSITIVE_COUNTERFACTUAL'
                ))::integer AS matured_cases,
                COUNT(*) FILTER (
                    WHERE classification = 'POTENTIAL_FALSE_NEGATIVE'
                )::integer AS potential_false_negatives,
                AVG(CASE
                    WHEN classification = 'POTENTIAL_FALSE_NEGATIVE' THEN 1.0
                    WHEN classification IN (
                        'POSITIVE_BELOW_THRESHOLD', 'NON_POSITIVE_COUNTERFACTUAL'
                    ) THEN 0.0
                END) AS potential_false_negative_rate,
                AVG(directional_outcome) FILTER (WHERE classification IN (
                    'POTENTIAL_FALSE_NEGATIVE', 'POSITIVE_BELOW_THRESHOLD',
                    'NON_POSITIVE_COUNTERFACTUAL'
                )) AS mean_directional_outcome,
                AVG(CASE WHEN shadow_forecast_id IS NOT NULL THEN 1.0 ELSE 0.0 END)
                    AS shadow_coverage_rate
            FROM learning_shadow_cases
            WHERE owner_chat_id = $1
              AND decided_at >= NOW() - ($2::int * INTERVAL '1 day')
            GROUP BY cohort_date, horizon_days
            ORDER BY cohort_date, horizon_days
            """,
            owner_chat_id,
            days,
        )
        by_block_reason = await conn.fetch(
            """
            SELECT
                COALESCE(NULLIF(block_reason, ''), 'sin motivo estructurado') AS block_reason,
                COUNT(*)::integer AS matured_cases,
                COUNT(*) FILTER (
                    WHERE classification = 'POTENTIAL_FALSE_NEGATIVE'
                )::integer AS potential_false_negatives,
                AVG(CASE
                    WHEN classification = 'POTENTIAL_FALSE_NEGATIVE' THEN 1.0
                    ELSE 0.0
                END) AS potential_false_negative_rate,
                AVG(directional_outcome) AS mean_directional_outcome
            FROM learning_shadow_cases
            WHERE owner_chat_id = $1
              AND horizon_days = 5
              AND decided_at >= NOW() - ($2::int * INTERVAL '1 day')
              AND classification IN (
                  'POTENTIAL_FALSE_NEGATIVE', 'POSITIVE_BELOW_THRESHOLD',
                  'NON_POSITIVE_COUNTERFACTUAL'
              )
            GROUP BY COALESCE(NULLIF(block_reason, ''), 'sin motivo estructurado')
            ORDER BY potential_false_negatives DESC, matured_cases DESC
            LIMIT 12
            """,
            owner_chat_id,
            days,
        )
        recent_cases = await conn.fetch(
            """
            SELECT
                decision_log_id, ticker, decision, decided_at, horizon_days,
                block_reason, outcome_source, directional_outcome,
                classification, shadow_forecast_id, shadow_as_of_ts,
                shadow_expected_return, shadow_action,
                shadow_supports_direction, last_evaluated_at
            FROM learning_shadow_cases
            WHERE owner_chat_id = $1
              AND decided_at >= NOW() - ($2::int * INTERVAL '1 day')
              AND horizon_days = 5
              AND classification = 'POTENTIAL_FALSE_NEGATIVE'
            ORDER BY decided_at DESC, horizon_days
            LIMIT 30
            """,
            owner_chat_id,
            days,
        )

    return _json({
        "ok": True,
        "available": True,
        "owner_chat_id": owner_chat_id,
        "days": days,
        "run": _row(latest_run),
        "metrics": [_row(row) for row in metrics],
        "trend": [_row(row) for row in trend],
        "cohorts": [_row(row) for row in cohorts],
        "by_block_reason": [_row(row) for row in by_block_reason],
        "recent_cases": [_row(row) for row in recent_cases],
        "note": (
            "Experimental y solo auditoria. Un falso negativo potencial indica "
            "retorno direccional posterior >= umbral; no prueba que el bloqueo haya sido incorrecto."
        ),
        "boundary": {
            "reads": ["decision_log", "shadow_thesis_forecasts", "shadow_thesis_outcomes"],
            "writes": [
                "learning_shadow_runs",
                "learning_shadow_cases",
                "learning_shadow_metric_snapshots",
            ],
            "affects_analysis": False,
            "affects_execution": False,
        },
    })


async def learning_shadow_v2_view(request: web.Request) -> web.Response:
    """Population-separated counterfactual evidence; never feeds analysis."""
    days = max(30, min(int(request.query.get("days", "365")), 730))
    try:
        owner_chat_id = int(request.query.get("owner_chat_id", "0"))
    except (TypeError, ValueError):
        return _json({"ok": False, "error": "owner_chat_id invalido"}, status=400)

    empty = {
        "ok": True,
        "available": False,
        "owner_chat_id": owner_chat_id,
        "days": days,
        "run": None,
        "metrics": [],
        "population_summary": [],
        "trend": [],
        "cohorts": [],
        "by_block_category": [],
        "review_summary": [],
        "rule_candidates": [],
        "data_quality": {},
        "recent_cases": [],
    }
    pool: asyncpg.Pool = request.app["pool"]
    async with pool.acquire() as conn:
        schema_ready = await conn.fetchval(
            """
            SELECT
                to_regclass('public.learning_shadow_runs') IS NOT NULL
                AND to_regclass('public.learning_shadow_cases') IS NOT NULL
                AND to_regclass('public.learning_shadow_metric_snapshots_v2') IS NOT NULL
                AND to_regclass('public.learning_shadow_cohort_metrics') IS NOT NULL
                AND to_regclass('public.learning_shadow_rule_candidates') IS NOT NULL
            """
        )
        if not schema_ready:
            return _json({**empty, "note": "Learning shadow v2 todavia no fue inicializado."})

        latest_run = await conn.fetchrow(
            """
            SELECT * FROM learning_shadow_runs
            WHERE owner_chat_id = $1 AND policy_version = 'learning-shadow-v2'
            ORDER BY captured_at DESC LIMIT 1
            """,
            owner_chat_id,
        )
        if not latest_run:
            return _json({**empty, "note": "Learning shadow v2 no tiene corridas."})

        metrics = await conn.fetch(
            """
            SELECT
                captured_at, snapshot_date, horizon_days,
                shadow_horizon_sessions, material_return_bps, policy_version,
                case_population, total_cases, matured_cases,
                potential_false_negatives, potential_false_negative_rate,
                clean_missed_opportunities, clean_miss_rate,
                (metrics->>'positive_below_threshold')::int AS positive_below_threshold,
                (metrics->>'non_positive_cases')::int AS non_positive_cases,
                (metrics->>'pending_cases')::int AS pending_cases,
                (metrics->>'missing_outcome_cases')::int AS missing_outcome_cases,
                (metrics->>'shadow_linked_cases')::int AS shadow_linked_cases,
                (metrics->>'benchmark_linked_cases')::int AS benchmark_linked_cases,
                (metrics->>'control_linked_cases')::int AS control_linked_cases,
                (metrics->>'unique_control_cases')::int AS unique_control_cases,
                (metrics->>'control_reuse_ratio')::float AS control_reuse_ratio,
                (metrics->>'risky_counterfactual_wins')::int AS risky_counterfactual_wins,
                (metrics->>'market_driven_wins')::int AS market_driven_wins,
                (metrics->>'uncontrolled_counterfactual_wins')::int
                    AS uncontrolled_counterfactual_wins,
                (metrics->>'insufficient_potential_wins')::int
                    AS insufficient_potential_wins,
                (metrics->>'shadow_coverage_rate')::float AS shadow_coverage_rate,
                (metrics->>'benchmark_coverage_rate')::float AS benchmark_coverage_rate,
                (metrics->>'mean_directional_outcome')::float AS mean_directional_outcome,
                (metrics->>'mean_mae')::float AS mean_mae,
                (metrics->>'mean_mfe')::float AS mean_mfe,
                (metrics->>'mean_alpha_vs_benchmark')::float AS mean_alpha_vs_benchmark,
                (metrics->>'mean_delta_vs_control')::float AS mean_delta_vs_control
            FROM learning_shadow_metric_snapshots_v2
            WHERE run_id = $1 AND case_population = 'PLANNER_BLOCKED'
            ORDER BY horizon_days
            """,
            latest_run["run_id"],
        )
        population_summary = await conn.fetch(
            """
            SELECT
                case_population, total_cases, matured_cases,
                potential_false_negatives, potential_false_negative_rate,
                clean_missed_opportunities, clean_miss_rate,
                (metrics->>'mean_directional_outcome')::float AS mean_directional_outcome,
                (metrics->>'benchmark_coverage_rate')::float AS benchmark_coverage_rate
            FROM learning_shadow_metric_snapshots_v2
            WHERE run_id = $1 AND horizon_days = 5 AND total_cases > 0
            ORDER BY CASE case_population
                WHEN 'PLANNER_BLOCKED' THEN 0
                WHEN 'RADAR_BLOCKED' THEN 1
                WHEN 'RADAR_DEBUG' THEN 2
                ELSE 3 END
            """,
            latest_run["run_id"],
        )
        trend = await conn.fetch(
            """
            SELECT captured_at, snapshot_date, horizon_days, total_cases,
                   matured_cases, potential_false_negatives,
                   potential_false_negative_rate, clean_missed_opportunities,
                   clean_miss_rate,
                   (metrics->>'benchmark_coverage_rate')::float AS benchmark_coverage_rate
            FROM learning_shadow_metric_snapshots_v2
            WHERE owner_chat_id = $1
              AND captured_at >= NOW() - ($2::int * INTERVAL '1 day')
              AND case_population = 'PLANNER_BLOCKED'
            ORDER BY captured_at, horizon_days
            """,
            owner_chat_id,
            days,
        )
        cohorts = await conn.fetch(
            """
            SELECT cohort_date, horizon_days, total_cases, matured_cases,
                   potential_false_negatives, potential_false_negative_rate,
                   clean_missed_opportunities, clean_miss_rate,
                   (metrics->>'mean_directional_outcome')::float AS mean_directional_outcome,
                   (metrics->>'benchmark_coverage_rate')::float AS benchmark_coverage_rate
            FROM learning_shadow_cohort_metrics
            WHERE owner_chat_id = $1
              AND cohort_date >= CURRENT_DATE - $2::int
              AND policy_version = 'learning-shadow-v2'
              AND case_population = 'PLANNER_BLOCKED'
            ORDER BY cohort_date, horizon_days
            """,
            owner_chat_id,
            days,
        )
        by_block_category = await conn.fetch(
            """
            SELECT block_category, COUNT(*)::integer AS matured_cases,
                   COUNT(*) FILTER (WHERE classification = 'POTENTIAL_FALSE_NEGATIVE')::integer
                       AS potential_false_negatives,
                   COUNT(*) FILTER (WHERE review_label = 'CLEAN_MISSED_OPPORTUNITY')::integer
                       AS clean_missed_opportunities,
                   COUNT(*) FILTER (WHERE review_label = 'RISKY_COUNTERFACTUAL_WIN')::integer
                       AS risky_counterfactual_wins,
                   AVG((classification = 'POTENTIAL_FALSE_NEGATIVE')::int::float)
                       AS potential_false_negative_rate,
                   AVG((review_label = 'CLEAN_MISSED_OPPORTUNITY')::int::float)
                       AS clean_miss_rate,
                   AVG(directional_outcome) AS mean_directional_outcome,
                   AVG(alpha_vs_benchmark) AS mean_alpha_vs_benchmark,
                   AVG(mae) AS mean_mae
            FROM learning_shadow_cases
            WHERE owner_chat_id = $1 AND horizon_days = 5
              AND decided_at >= NOW() - ($2::int * INTERVAL '1 day')
              AND policy_version = 'learning-shadow-v2'
              AND case_population = 'PLANNER_BLOCKED'
              AND classification IN (
                  'POTENTIAL_FALSE_NEGATIVE', 'POSITIVE_BELOW_THRESHOLD',
                  'NON_POSITIVE_COUNTERFACTUAL'
              )
            GROUP BY block_category
            ORDER BY matured_cases DESC, block_category
            """,
            owner_chat_id,
            days,
        )
        review_summary = await conn.fetch(
            """
            SELECT review_label, COUNT(*)::integer AS cases,
                   AVG(directional_outcome) AS mean_directional_outcome,
                   AVG(mae) AS mean_mae,
                   AVG(alpha_vs_benchmark) AS mean_alpha_vs_benchmark
            FROM learning_shadow_cases
            WHERE owner_chat_id = $1 AND horizon_days = 5
              AND decided_at >= NOW() - ($2::int * INTERVAL '1 day')
              AND policy_version = 'learning-shadow-v2'
              AND case_population = 'PLANNER_BLOCKED'
            GROUP BY review_label
            ORDER BY cases DESC, review_label
            """,
            owner_chat_id,
            days,
        )
        data_quality = await conn.fetchrow(
            """
            SELECT COUNT(*)::integer AS total_cases,
                   COUNT(*) FILTER (WHERE classification IN (
                       'POTENTIAL_FALSE_NEGATIVE', 'POSITIVE_BELOW_THRESHOLD',
                       'NON_POSITIVE_COUNTERFACTUAL'
                   ))::integer AS matured_cases,
                   COUNT(*) FILTER (WHERE path_risk NOT IN ('PENDING', 'OUTLIER'))::integer
                       AS usable_path_cases,
                   COUNT(*) FILTER (WHERE path_risk = 'OUTLIER')::integer AS path_outliers,
                   COUNT(*) FILTER (WHERE benchmark_outcome IS NOT NULL)::integer
                       AS benchmark_linked_cases,
                   COUNT(*) FILTER (WHERE control_decision_log_id IS NOT NULL)::integer
                       AS control_linked_cases,
                   COUNT(DISTINCT control_decision_log_id)::integer AS unique_control_cases,
                   COUNT(*) FILTER (WHERE classification = 'MISSING_OUTCOME')::integer
                       AS missing_outcome_cases,
                   COUNT(*) FILTER (WHERE shadow_forecast_id IS NOT NULL)::integer
                       AS shadow_linked_cases
            FROM learning_shadow_cases
            WHERE owner_chat_id = $1 AND horizon_days = 5
              AND decided_at >= NOW() - ($2::int * INTERVAL '1 day')
              AND policy_version = 'learning-shadow-v2'
              AND case_population = 'PLANNER_BLOCKED'
            """,
            owner_chat_id,
            days,
        )
        rule_candidates = await conn.fetch(
            """
            SELECT id, policy_version, block_category, horizon_days,
                   candidate_type, proposed_rule, rationale, sample_size,
                   clean_miss_count, clean_miss_rate, risky_win_count,
                   market_driven_count, mean_alpha_vs_benchmark,
                   evidence_start, evidence_end, status,
                   reviewed_at, reviewed_by, review_note, updated_at
            FROM learning_shadow_rule_candidates
            WHERE owner_chat_id = $1 AND policy_version = 'learning-shadow-v2'
            ORDER BY CASE status WHEN 'PROPOSED' THEN 0 ELSE 1 END,
                     clean_miss_rate DESC, block_category
            """,
            owner_chat_id,
        )
        recent_cases = await conn.fetch(
            """
            SELECT decision_log_id, ticker, decision, decided_at, horizon_days,
                   block_category, block_reason, outcome_source, directional_outcome,
                   classification, review_label, path_sessions, mae, mfe, path_risk,
                   benchmark_outcome, alpha_vs_benchmark,
                   control_decision_log_id, control_status, control_outcome,
                   control_match_type, delta_vs_control,
                   shadow_forecast_id, shadow_as_of_ts,
                   shadow_expected_return, shadow_action,
                   shadow_supports_direction, last_evaluated_at
            FROM learning_shadow_cases
            WHERE owner_chat_id = $1
              AND decided_at >= NOW() - ($2::int * INTERVAL '1 day')
              AND horizon_days = 5
              AND classification = 'POTENTIAL_FALSE_NEGATIVE'
              AND policy_version = 'learning-shadow-v2'
              AND case_population = 'PLANNER_BLOCKED'
            ORDER BY CASE review_label
                         WHEN 'CLEAN_MISSED_OPPORTUNITY' THEN 0
                         WHEN 'RISKY_COUNTERFACTUAL_WIN' THEN 1
                         WHEN 'MARKET_DRIVEN_WIN' THEN 2
                         ELSE 3 END,
                     decided_at DESC
            LIMIT 10
            """,
            owner_chat_id,
            days,
        )

    return _json({
        "ok": True,
        "available": True,
        "owner_chat_id": owner_chat_id,
        "days": days,
        "run": _row(latest_run),
        "metrics": [_row(row) for row in metrics],
        "population_summary": [_row(row) for row in population_summary],
        "trend": [_row(row) for row in trend],
        "cohorts": [_row(row) for row in cohorts],
        "by_block_category": [_row(row) for row in by_block_category],
        "review_summary": [_row(row) for row in review_summary],
        "data_quality": _row(data_quality),
        "rule_candidates": [_row(row) for row in rule_candidates],
        "recent_cases": [_row(row) for row in recent_cases],
        "note": (
            "Experimental y solo auditoria. La tasa potencial mide retornos posteriores "
            ">= umbral. La tasa limpia agrega recorrido continuo y alpha positivo contra SPY; "
            "sigue sin probar causalidad."
        ),
        "boundary": {
            "reads": [
                "decision_log", "market_candles",
                "shadow_thesis_forecasts", "shadow_thesis_outcomes",
            ],
            "writes": [
                "learning_shadow_runs", "learning_shadow_cases",
                "learning_shadow_metric_snapshots_v2",
                "learning_shadow_cohort_metrics",
                "learning_shadow_rule_candidates",
            ],
            "affects_analysis": False,
            "affects_execution": False,
        },
    })


async def shadow_view(request: web.Request) -> web.Response:
    """Latest independent 5/20/40 forecasts and their matured outcomes."""
    try:
        owner_chat_id = int(request.query.get("owner_chat_id", "0"))
    except (TypeError, ValueError):
        return _json({"ok": False, "error": "owner_chat_id invalido"}, status=400)

    pool: asyncpg.Pool = request.app["pool"]
    async with pool.acquire() as conn:
        schema_ready = await conn.fetchval(
            """
            SELECT
                to_regclass('public.shadow_thesis_runs') IS NOT NULL
                AND to_regclass('public.shadow_thesis_forecasts') IS NOT NULL
                AND to_regclass('public.shadow_thesis_outcomes') IS NOT NULL
            """
        )
        if not schema_ready:
            return _json({
                "ok": True,
                "available": False,
                "owner_chat_id": owner_chat_id,
                "note": "Schema shadow no instalado",
                "run": None,
                "forecasts": [],
                "metrics": [],
            })

        latest_run = await conn.fetchrow(
            """
            SELECT
                run_id, owner_chat_id, captured_at, as_of_ts, model_version,
                schema_version, universe_count, status, metadata
            FROM shadow_thesis_runs
            WHERE owner_chat_id = $1
            ORDER BY as_of_ts DESC, captured_at DESC
            LIMIT 1
            """,
            owner_chat_id,
        )
        if not latest_run:
            return _json({
                "ok": True,
                "available": False,
                "owner_chat_id": owner_chat_id,
                "note": "Todavia no hay corridas shadow para este owner",
                "run": None,
                "forecasts": [],
                "metrics": [],
            })

        forecasts = await conn.fetch(
            """
            SELECT
                f.ticker,
                f.universe_role,
                f.as_of_ts,
                f.horizon_sessions,
                f.reference_price,
                f.expected_return,
                f.probability_up,
                f.lower_return,
                f.upper_return,
                f.uncertainty,
                f.thesis_action,
                f.thesis_confidence,
                f.signal_strength,
                f.input_sessions,
                o.target_session_ts,
                o.outcome_price,
                o.realized_return,
                o.direction_correct,
                o.absolute_error,
                o.matured_at
            FROM shadow_thesis_forecasts f
            LEFT JOIN shadow_thesis_outcomes o ON o.forecast_id = f.id
            WHERE f.run_id = $1
            ORDER BY
                CASE WHEN f.universe_role = 'POSITION' THEN 0 ELSE 1 END,
                f.ticker,
                f.horizon_sessions
            """,
            latest_run["run_id"],
        )
        metrics = await conn.fetch(
            """
            SELECT
                f.horizon_sessions,
                COUNT(o.forecast_id)::integer AS samples,
                AVG(CASE WHEN o.direction_correct THEN 1.0 ELSE 0.0 END)
                    AS directional_accuracy,
                AVG(o.absolute_error) AS mean_absolute_error,
                AVG(f.expected_return) FILTER (WHERE o.forecast_id IS NOT NULL)
                    AS mean_expected_return,
                AVG(o.realized_return) AS mean_realized_return
            FROM shadow_thesis_forecasts f
            LEFT JOIN shadow_thesis_outcomes o ON o.forecast_id = f.id
            WHERE f.owner_chat_id = $1
            GROUP BY f.horizon_sessions
            ORDER BY f.horizon_sessions
            """,
            owner_chat_id,
        )

    return _json({
        "ok": True,
        "available": True,
        "owner_chat_id": owner_chat_id,
        "run": _row(latest_run),
        "forecasts": [_row(row) for row in forecasts],
        "metrics": [_row(row) for row in metrics],
        "axis": {
            "x": "horizon_sessions",
            "y": "return",
            "x_label": "Horizonte (ruedas)",
            "y_label": "Retorno proyectado / observado",
        },
        "note": "Shadow es experimental y no modifica decision_log ni genera ordenes.",
    })


SECRET_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"bot\d+:[A-Za-z0-9_-]+", re.I), "bot***"),
    (re.compile(r"(password=)[^\s&]+", re.I), r"\1***"),
    (re.compile(r"(token=)[^\s&]+", re.I), r"\1***"),
    (re.compile(r"(postgres(?:ql)?://[^:\s]+:)[^@\s]+@", re.I), r"\1***@"),
    (re.compile(r"(redis://[^:\s]+:)[^@\s]+@", re.I), r"\1***@"),
]


def _redact(line: str) -> str:
    out = redact_secrets(line)
    for pattern, replacement in SECRET_PATTERNS:
        out = pattern.sub(replacement, out)
    return out[-1200:]


async def logs_recent(request: web.Request) -> web.Response:
    limit = max(10, min(int(request.query.get("limit", "80")), 200))
    patterns = ("ERROR", "WARNING", "Traceback", "STOP_TRIGGERED", "run_performance", "daily_analysis")
    items: list[dict] = []

    if LOG_DIR.exists():
        for path in sorted(LOG_DIR.glob("*.log"), key=lambda p: p.stat().st_mtime, reverse=True)[:8]:
            try:
                lines = path.read_text(encoding="utf-8", errors="replace").splitlines()[-1000:]
            except Exception:
                continue
            for line in lines:
                if any(p in line for p in patterns):
                    items.append({"file": path.name, "line": _redact(line)})

    return _json({
        "ok": True,
        "log_dir": str(LOG_DIR),
        "items": items[-limit:],
        "note": None if items else "No hay logs de archivo recientes; Docker stdout no es visible desde la API.",
    })


async def create_app() -> web.Application:
    cfg = get_config()
    pool = await asyncpg.create_pool(
        cfg.database.url.replace("postgresql+asyncpg://", "postgresql://"),
        min_size=1,
        max_size=4,
    )

    app = web.Application(middlewares=[security_headers_middleware, cors_middleware, auth_middleware])
    app["pool"] = pool
    app["index_html"] = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    app.router.add_get("/", index)
    app.router.add_get("/api/auth/status", auth_status)
    app.router.add_get("/api/health", health)
    app.router.add_get("/api/ingestion", ingestion)
    app.router.add_get("/api/candles", candles)
    app.router.add_get("/api/decisions", decisions)
    app.router.add_get("/api/portfolio", portfolio_view)
    app.router.add_get("/api/performance", performance_view)
    app.router.add_get("/api/override-audit", override_audit)
    app.router.add_get("/api/decision-ledger", decision_ledger)
    app.router.add_get("/api/radar-audit", radar_audit)
    app.router.add_get("/api/shadow", shadow_view)
    app.router.add_get("/api/learning-shadow", learning_shadow_v2_view)
    app.router.add_get("/api/human-activity", human_activity)
    app.router.add_get("/api/corporate-actions", corporate_actions_view)
    app.router.add_get("/api/fills", fills)
    app.router.add_get("/api/logs/recent", logs_recent)

    async def close_pool(app_: web.Application) -> None:
        await app_["pool"].close()

    app.on_cleanup.append(close_pool)
    return app


def main() -> None:
    if not TOKEN:
        raise RuntimeError("MONITOR_API_TOKEN es obligatorio para iniciar monitor_api")
    port = int(os.getenv("MONITOR_API_PORT", "8010"))
    web.run_app(create_app(), host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()

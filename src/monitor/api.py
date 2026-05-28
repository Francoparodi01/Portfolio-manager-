from __future__ import annotations

import hmac
import os
import re
import time as time_module
from collections import defaultdict, deque
from datetime import date, datetime, time, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import asyncpg
import pyotp
from aiohttp import web

from src.core.config import get_config
from src.core.market_calendar import is_trading_day, market_closed_reason
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
    if request.path.startswith("/api/"):
        response.headers.setdefault("Cache-Control", "no-store")
    return response


@web.middleware
async def cors_middleware(request: web.Request, handler):
    response = await handler(request)
    origin = os.getenv("MONITOR_CORS_ORIGIN", "")
    if origin:
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Headers"] = "Authorization,X-API-Token,X-TOTP-Code,Content-Type"
        response.headers["Access-Control-Allow-Methods"] = "GET,OPTIONS"
    return response


async def index(_request: web.Request) -> web.Response:
    return web.FileResponse(STATIC_DIR / "index.html")


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

    redis_ok = await _redis_ping()
    keys = {
        "scheduler": await _redis_get(SCHEDULER_HEARTBEAT_KEY),
        "bot": await _redis_get(BOT_HEARTBEAT_KEY),
        "market": await _redis_get(MARKET_HEARTBEAT_KEY),
        "risk": await _redis_get(RISK_HEARTBEAT_KEY),
        "monitor_state": await _redis_get(MONITOR_STATE_KEY),
        "bot_busy": await _redis_get(BOT_BUSY_KEY),
    }

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
            "closed_reason": market_closed_reason(now),
            "now_art": now.isoformat(),
        },
        "services": {
            "scheduler": {
                "heartbeat_age_seconds": _age_seconds(keys["scheduler"]),
                "alive": (_age_seconds(keys["scheduler"]) or 999999) < 90,
            },
            "telegram_bot": {
                "heartbeat_age_seconds": _age_seconds(keys["bot"]),
                "alive": (_age_seconds(keys["bot"]) or 999999) < 90,
                "busy": bool(keys["bot_busy"]),
            },
            "intraday_monitor_state": keys["monitor_state"],
            "market_heartbeat_age_seconds": _age_seconds(keys["market"]),
            "risk_heartbeat_age_seconds": _age_seconds(keys["risk"]),
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
                SELECT MAX(ts::date) AS day FROM market_prices
            ),
            price_assets AS (
                SELECT COUNT(DISTINCT ticker) AS n
                FROM market_prices, latest_price_day
                WHERE ts::date = latest_price_day.day
            ),
            candle_assets AS (
                SELECT COUNT(DISTINCT ticker) AS n
                FROM market_candles, latest_price_day
                WHERE ts::date = latest_price_day.day
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
                ts::date AS business_day,
                COUNT(*) AS rows,
                COUNT(DISTINCT ticker) AS tickers,
                MIN(ts) AS min_ts,
                MAX(ts) AS max_ts
            FROM market_candles
            WHERE ts >= NOW() - INTERVAL '14 days'
            GROUP BY 1
            ORDER BY 1 DESC
            LIMIT 10
        """)

    return _json({
        "ok": True,
        "market": {
            "business_day": business,
            "open": business and _is_market_hours(now),
            "closed_reason": market_closed_reason(now),
            "expects_daily_candle": business and now.time() >= time(18, 0),
        },
        "coverage": _row(coverage),
        "recent": [_row(r) for r in recent],
    })


async def decisions(request: web.Request) -> web.Response:
    days = max(1, min(int(request.query.get("days", "90")), 365))
    pool: asyncpg.Pool = request.app["pool"]
    async with pool.acquire() as conn:
        summary = await conn.fetchrow("""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE outcome_5d IS NULL) AS pending_5d,
                COUNT(*) FILTER (WHERE outcome_5d IS NOT NULL) AS closed_5d,
                COUNT(*) FILTER (WHERE source = 'execution_plan') AS execution_plan,
                COUNT(*) FILTER (WHERE status = 'BLOCKED') AS blocked,
                COUNT(*) FILTER (WHERE status = 'APPROVED') AS approved,
                COUNT(*) FILTER (WHERE status = 'EXECUTED') AS executed
            FROM decision_log
            WHERE decided_at >= NOW() - ($1::int * INTERVAL '1 day')
        """, days)
        groups = await conn.fetch("""
            SELECT
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
            GROUP BY 1,2,3,4
            ORDER BY n DESC, source, status
            LIMIT 30
        """, days)
        recent = await conn.fetch("""
            SELECT decided_at, ticker, decision, status, source, final_score,
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

    return _json({
        "ok": True,
        "days": days,
        "snapshot": _row(latest_snapshot),
        "positions": [_row(r) for r in positions],
        "allocation": [_row(r) for r in allocation],
        "history": [_row(r) for r in history],
    })


async def performance_view(request: web.Request) -> web.Response:
    days = max(7, min(int(request.query.get("days", "180")), 365))
    pool: asyncpg.Pool = request.app["pool"]
    async with pool.acquire() as conn:
        summary = await conn.fetchrow("""
            WITH real AS (
                SELECT
                    COALESCE(executable_outcome_5d, outcome_5d) AS outcome_5d,
                    COALESCE(executable_outcome_10d, outcome_10d) AS outcome_10d,
                    COALESCE(executable_outcome_20d, outcome_20d) AS outcome_20d
                FROM decision_log
                WHERE decided_at >= NOW() - ($1::int * INTERVAL '1 day')
                  AND outcome_basis = 'canonical_cocos'
                  AND COALESCE(executable_outcome_5d, outcome_5d) IS NOT NULL
                  AND COALESCE(executable_was_correct, was_correct) IS NOT NULL
                  AND status = 'EXECUTED_MANUAL'
                  AND COALESCE(source, layers->>'source') = 'broker_movement'
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
        by_ticker = await conn.fetch("""
            SELECT
                ticker,
                COUNT(*) AS n,
                AVG(COALESCE(executable_outcome_5d, outcome_5d)) AS avg_5d,
                AVG(CASE WHEN COALESCE(executable_outcome_5d, outcome_5d) > 0 THEN 1.0 ELSE 0.0 END) AS win_rate_5d
            FROM decision_log
            WHERE decided_at >= NOW() - ($1::int * INTERVAL '1 day')
              AND outcome_basis = 'canonical_cocos'
              AND COALESCE(executable_outcome_5d, outcome_5d) IS NOT NULL
              AND COALESCE(executable_was_correct, was_correct) IS NOT NULL
              AND status = 'EXECUTED_MANUAL'
              AND COALESCE(source, layers->>'source') = 'broker_movement'
            GROUP BY ticker
            ORDER BY n DESC, avg_5d DESC
            LIMIT 12
        """, days)
        score_points = await conn.fetch("""
            SELECT
                decided_at,
                ticker,
                decision,
                status,
                COALESCE(source, layers->>'source') AS source,
                final_score,
                confidence,
                COALESCE(executable_outcome_5d, outcome_5d) AS outcome_5d,
                COALESCE(executable_outcome_10d, outcome_10d) AS outcome_10d,
                COALESCE(executable_outcome_20d, outcome_20d) AS outcome_20d,
                next_executable_at,
                next_executable_price
            FROM decision_log
            WHERE decided_at >= NOW() - ($1::int * INTERVAL '1 day')
              AND outcome_basis = 'canonical_cocos'
              AND COALESCE(executable_outcome_5d, outcome_5d) IS NOT NULL
              AND final_score IS NOT NULL
            ORDER BY decided_at DESC
            LIMIT 160
        """, days)
        status_counts = await conn.fetch("""
            SELECT
                COALESCE(source, layers->>'source', 'sin_source') AS source,
                COALESCE(status, 'UNKNOWN') AS status,
                COUNT(*) AS n,
                COUNT(COALESCE(executable_outcome_5d, outcome_5d)) FILTER (WHERE outcome_basis = 'canonical_cocos') AS closed_5d
            FROM decision_log
            WHERE decided_at >= NOW() - ($1::int * INTERVAL '1 day')
            GROUP BY 1, 2
            ORDER BY n DESC
            LIMIT 16
        """, days)

    summary_dict = _row(summary)
    win_rate = summary_dict.get("win_rate_5d") or 0
    avg_win = summary_dict.get("avg_win_5d") or 0
    avg_loss = abs(summary_dict.get("avg_loss_5d") or 0)
    summary_dict["ev_5d"] = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)

    return _json({
        "ok": True,
        "days": days,
        "summary": summary_dict,
        "by_ticker": [_row(r) for r in by_ticker],
        "score_points": [_row(r) for r in score_points],
        "status_counts": [_row(r) for r in status_counts],
    })


async def override_audit(request: web.Request) -> web.Response:
    days = max(7, min(int(request.query.get("days", "90")), 365))
    match_window_days = max(1, min(int(request.query.get("match_window_days", "2")), 10))
    pool: asyncpg.Pool = request.app["pool"]
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            WITH decisions AS (
                SELECT
                    id,
                    decided_at,
                    ticker,
                    decision,
                    final_score,
                    price_at_decision,
                    ABS(COALESCE(theoretical_amount_ars, executed_amount_ars, 0)) AS target_amount_ars,
                    COALESCE(executable_outcome_5d, outcome_5d) AS outcome_5d,
                    COALESCE(executable_outcome_10d, outcome_10d) AS outcome_10d,
                    COALESCE(executable_outcome_20d, outcome_20d) AS outcome_20d,
                    next_executable_at,
                    next_executable_price,
                    CASE
                        WHEN next_executable_at IS NOT NULL THEN next_executable_at
                        WHEN (decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::time >= TIME '17:00'
                            THEN (((decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date + INTERVAL '1 day') AT TIME ZONE 'America/Argentina/Buenos_Aires')
                        WHEN (decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::time < TIME '10:30'
                            THEN (((decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date) AT TIME ZONE 'America/Argentina/Buenos_Aires')
                        ELSE decided_at
                    END AS match_start_at,
                    layers->>'reason' AS reason
                FROM decision_log
                WHERE decided_at >= NOW() - ($1::int * INTERVAL '1 day')
                  AND COALESCE(source, layers->>'source') = 'execution_plan'
                  AND status = 'APPROVED'
                  AND decision_type = 'executable'
                  AND decision IN ('BUY', 'SELL')
                  AND price_at_decision IS NOT NULL
            )
            SELECT
                d.*,
                same_fill.first_at AS same_executed_at,
                same_fill.amount_ars AS same_amount_ars,
                opposite_fill.first_at AS opposite_executed_at,
                opposite_fill.amount_ars AS opposite_amount_ars,
                CASE
                    WHEN COALESCE(same_fill.amount_ars, 0) / GREATEST(d.target_amount_ars, 1) >= 0.75 THEN 'FOLLOWED'
                    WHEN COALESCE(same_fill.amount_ars, 0) / GREATEST(d.target_amount_ars, 1) >= 0.15 THEN 'PARTIAL'
                    WHEN COALESCE(opposite_fill.amount_ars, 0) / GREATEST(d.target_amount_ars, 1) >= 0.15 THEN 'OPPOSITE'
                    ELSE 'IGNORED'
                END AS override_status
            FROM decisions d
            LEFT JOIN LATERAL (
                SELECT MIN(executed_at) AS first_at, SUM(ABS(COALESCE(amount, quantity * price, 0))) AS amount_ars
                FROM broker_movements bm
                WHERE bm.ticker = d.ticker
                  AND bm.movement_type = d.decision
                  AND bm.executed_at >= d.match_start_at
                  AND bm.executed_at < d.match_start_at + ($2::int * INTERVAL '1 day')
                  AND bm.quantity IS NOT NULL
                  AND bm.price IS NOT NULL
            ) same_fill ON TRUE
            LEFT JOIN LATERAL (
                SELECT MIN(executed_at) AS first_at, SUM(ABS(COALESCE(amount, quantity * price, 0))) AS amount_ars
                FROM broker_movements bm
                WHERE bm.ticker = d.ticker
                  AND bm.movement_type = CASE WHEN d.decision = 'BUY' THEN 'SELL' ELSE 'BUY' END
                  AND bm.executed_at >= d.match_start_at
                  AND bm.executed_at < d.match_start_at + ($2::int * INTERVAL '1 day')
                  AND bm.quantity IS NOT NULL
                  AND bm.price IS NOT NULL
            ) opposite_fill ON TRUE
            ORDER BY d.decided_at DESC
        """, days, match_window_days)

    items = [_row(r) for r in rows]
    by_status: dict[str, int] = {}
    unique_intents = set()
    closed = []
    for item in items:
        status = item.get("override_status") or "UNKNOWN"
        by_status[status] = by_status.get(status, 0) + 1
        unique_intents.add((item.get("ticker"), item.get("decision")))
        if item.get("outcome_5d") is not None:
            closed.append(item)

    bot_wins = sum(1 for item in closed if item.get("override_status") in {"IGNORED", "OPPOSITE"} and (item.get("outcome_5d") or 0) > 0)
    human_wins = sum(1 for item in closed if item.get("override_status") in {"IGNORED", "OPPOSITE"} and (item.get("outcome_5d") or 0) < 0)

    return _json({
        "ok": True,
        "days": days,
        "match_window_days": match_window_days,
        "summary": {
            "plans": len(items),
            "unique_intents": len(unique_intents),
            "closed_5d": len(closed),
            "by_status": by_status,
            "bot_wins_ignored": bot_wins,
            "human_wins_ignored": human_wins,
        },
        "recent": items[:30],
    })


async def fills(request: web.Request) -> web.Response:
    days = max(1, min(int(request.query.get("days", "90")), 365))
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
            LIMIT 20
        """, days)
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
            LIMIT 20
        """, days)

    return _json({
        "ok": True,
        "days": days,
        "summary": _row(summary),
        "by_source": [_row(r) for r in by_source],
        "recent": [_row(r) for r in recent],
        "movements": {
            "summary": _row(movements_summary),
            "recent": [_row(r) for r in movements_recent],
        },
    })


SECRET_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"bot\d+:[A-Za-z0-9_-]+", re.I), "bot***"),
    (re.compile(r"(password=)[^\s&]+", re.I), r"\1***"),
    (re.compile(r"(token=)[^\s&]+", re.I), r"\1***"),
    (re.compile(r"(postgres(?:ql)?://[^:\s]+:)[^@\s]+@", re.I), r"\1***@"),
    (re.compile(r"(redis://[^:\s]+:)[^@\s]+@", re.I), r"\1***@"),
]


def _redact(line: str) -> str:
    out = line
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
    app.router.add_get("/", index)
    app.router.add_get("/api/auth/status", auth_status)
    app.router.add_get("/api/health", health)
    app.router.add_get("/api/ingestion", ingestion)
    app.router.add_get("/api/candles", candles)
    app.router.add_get("/api/decisions", decisions)
    app.router.add_get("/api/portfolio", portfolio_view)
    app.router.add_get("/api/performance", performance_view)
    app.router.add_get("/api/override-audit", override_audit)
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

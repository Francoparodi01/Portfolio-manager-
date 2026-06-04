"""
scripts/run_confidence_audit.py

Read-only operational audit for Telegram.

This is not a predictive model. It answers whether the pipeline is trustworthy
enough to interpret the current bot outputs: ingestion, candles, decisions,
execution reconciliation, and outcomes.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime, time, timezone
from html import escape
from zoneinfo import ZoneInfo

import asyncpg

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.collector.notifier import TelegramNotifier
from src.core.config import get_config
from src.core.market_calendar import is_trading_day, market_closed_reason


ART = ZoneInfo("America/Argentina/Buenos_Aires")


def _fmt_dt(value) -> str:
    if value is None:
        return "-"
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.astimezone(ART).strftime("%d/%m %H:%M")
    return str(value)


def _age_hours(value) -> float | None:
    if not isinstance(value, datetime):
        return None
    dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return max(0.0, (datetime.now(tz=timezone.utc) - dt.astimezone(timezone.utc)).total_seconds() / 3600)


def _money(value) -> str:
    if value is None:
        return "-"
    return f"${float(value):,.0f}"


def _num(value) -> int:
    return int(value or 0)


def _state(ok: bool, label: str, *, wait: bool = False) -> str:
    icon = "OK" if ok else ("WAIT" if wait else "FAIL")
    return f"<b>{icon}</b> {escape(label)}"


def _expects_same_day_candles(now: datetime) -> bool:
    """After local market close, today's canonical candles should exist."""
    return is_trading_day(now) and now.time() >= time(18, 0)


async def _table_exists(conn: asyncpg.Connection, table: str) -> bool:
    return bool(
        await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = $1
            )
            """,
            table,
        )
    )


async def build_confidence_audit(days: int = 180) -> str:
    cfg = get_config()
    dsn = cfg.database.url.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)

    try:
        latest_portfolio = await conn.fetchrow(
            """
            SELECT scraped_at, total_value_ars, cash_ars, confidence_score
            FROM portfolio_snapshots
            ORDER BY scraped_at DESC
            LIMIT 1
            """
        )
        portfolio_counts = await conn.fetchrow(
            """
            SELECT
                COUNT(*) FILTER (WHERE scraped_at >= NOW() - INTERVAL '24 hours') AS last_24h,
                COUNT(*) FILTER (WHERE scraped_at >= NOW() - INTERVAL '7 days') AS last_7d
            FROM portfolio_snapshots
            """
        )
        market = await conn.fetchrow(
            """
            SELECT
                MAX(ts) AS latest_ts,
                COUNT(*) FILTER (WHERE ts >= NOW() - INTERVAL '24 hours') AS rows_24h,
                COUNT(*) FILTER (WHERE ts >= NOW() - INTERVAL '7 days') AS rows_7d,
                COUNT(DISTINCT ticker) FILTER (WHERE ts >= NOW() - INTERVAL '7 days') AS tickers_7d
            FROM market_prices
            """
        )
        candles = await conn.fetchrow(
            """
            WITH latest_price_day AS (
                SELECT MAX((ts AT TIME ZONE 'America/Argentina/Buenos_Aires')::date) AS day
                FROM market_prices
            ),
            latest_candle_day AS (
                SELECT MAX((ts AT TIME ZONE 'UTC')::date) AS day
                FROM market_candles
                WHERE source = 'internal_snapshot'
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
            ),
            latest_candle_assets AS (
                SELECT COUNT(DISTINCT ticker) AS n
                FROM market_candles, latest_candle_day
                WHERE (ts AT TIME ZONE 'UTC')::date = latest_candle_day.day
                  AND source = 'internal_snapshot'
            )
            SELECT
                latest_price_day.day AS business_day,
                latest_candle_day.day AS latest_candle_day,
                price_assets.n AS price_assets,
                candle_assets.n AS internal_candles,
                latest_candle_assets.n AS latest_candle_assets,
                GREATEST(price_assets.n - candle_assets.n, 0) AS missing_internal
            FROM latest_price_day, latest_candle_day, price_assets, candle_assets, latest_candle_assets
            """
        )
        decisions = await conn.fetchrow(
            """
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE COALESCE(source, layers->>'source') = 'execution_plan') AS execution_plan,
                COUNT(*) FILTER (
                    WHERE COALESCE(source, layers->>'source') = 'execution_plan'
                      AND status = 'APPROVED'
                      AND decision_type = 'executable'
                ) AS approved_executable,
                COUNT(*) FILTER (
                    WHERE COALESCE(source, layers->>'source') = 'execution_plan'
                      AND status = 'EXECUTED'
                ) AS executed,
                COUNT(*) FILTER (
                    WHERE status = 'EXECUTED_MANUAL'
                ) AS executed_manual,
                COUNT(*) FILTER (
                    WHERE COALESCE(source, layers->>'source') = 'execution_plan'
                      AND status = 'BLOCKED'
                ) AS blocked,
                COUNT(*) FILTER (WHERE COALESCE(source, layers->>'source') = 'optimizer') AS optimizer,
                COUNT(*) FILTER (WHERE COALESCE(source, layers->>'source') = 'radar') AS radar,
                COUNT(outcome_5d) AS closed_5d,
                COUNT(outcome_10d) AS closed_10d,
                COUNT(outcome_20d) AS closed_20d
            FROM decision_log
            WHERE decided_at >= NOW() - ($1::int * INTERVAL '1 day')
              AND decision IN ('BUY','SELL','SELL_PARTIAL','SELL_FULL')
            """,
            days,
        )

        broker = {"exists": False, "total": 0, "reconciled": 0, "unreconciled": 0}
        if await _table_exists(conn, "broker_fills"):
            row = await conn.fetchrow(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE decision_log_id IS NOT NULL) AS reconciled,
                    COUNT(*) FILTER (WHERE decision_log_id IS NULL) AS unreconciled,
                    MAX(executed_at) AS latest_executed_at
                FROM broker_fills
                """
            )
            broker = {
                "exists": True,
                "total": _num(row["total"]),
                "reconciled": _num(row["reconciled"]),
                "unreconciled": _num(row["unreconciled"]),
                "latest_executed_at": row["latest_executed_at"],
            }

    finally:
        await conn.close()

    now = datetime.now(tz=ART)
    trading_day = is_trading_day(now)
    closed_reason = market_closed_reason(now)
    expects_same_day_candles = _expects_same_day_candles(now)

    portfolio_age = _age_hours(latest_portfolio["scraped_at"] if latest_portfolio else None)
    market_age = _age_hours(market["latest_ts"] if market else None)
    portfolio_ok = portfolio_age is not None and portfolio_age <= 72
    market_ok = market_age is not None and market_age <= 72
    candle_day = candles["latest_candle_day"] if candles else None
    candle_day_age = (now.date() - candle_day).days if candle_day else None
    candles_same_day_ok = _num(candles["missing_internal"] if candles else None) == 0
    candles_recent_ok = candle_day_age is not None and candle_day_age <= 5
    candles_closed_ok = (not trading_day) and candles_recent_ok and not candles_same_day_ok
    candles_wait = (
        trading_day
        and (not expects_same_day_candles)
        and candles_recent_ok
        and not candles_same_day_ok
    )
    candles_ok = candles_same_day_ok or candles_wait or candles_closed_ok
    decisions_ok = _num(decisions["total"] if decisions else None) > 0
    execution_ready = (
        _num(decisions["executed"] if decisions else None)
        + _num(decisions["executed_manual"] if decisions else None)
    ) > 0
    closed_5d = _num(decisions["closed_5d"] if decisions else None)
    outcomes_ready = closed_5d >= 12

    hard_ok = portfolio_ok and market_ok and candles_ok and decisions_ok
    measurement_ready = execution_ready and outcomes_ready

    if hard_ok and execution_ready and closed_5d >= 100:
        verdict = "CONFIABLE para auditoria estadistica inicial"
    elif hard_ok and measurement_ready:
        verdict = "OPERATIVO Y AUDITABLE, muestra chica"
    elif hard_ok:
        verdict = "OPERATIVO, pero todavia no estadistico"
    else:
        verdict = "REVISAR INGESTA antes de confiar"

    outside_hours = now.time() < time(10, 30) or now.time() >= time(17, 0)
    if trading_day and not outside_hours:
        market_label = "rueda"
    elif trading_day and outside_hours:
        market_label = "cerrado (fuera de horario)"
    else:
        market_label = "cerrado"
    if closed_reason and "fuera de horario" not in market_label:
        market_label += f" ({escape(closed_reason)})"

    lines = [
        "🧭 <b>CONFIANZA DEL SISTEMA</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"Veredicto: <b>{escape(verdict)}</b>",
        f"Mercado: <b>{market_label}</b>",
        "",
        "<b>Pipeline</b>",
        f"• {_state(portfolio_ok, 'Portfolio reciente')} — {_fmt_dt(latest_portfolio['scraped_at'] if latest_portfolio else None)} | {_money(latest_portfolio['total_value_ars'] if latest_portfolio else None)}",
        f"• {_state(market_ok, 'Market prices recientes')} — {_fmt_dt(market['latest_ts'] if market else None)} | 7d rows {_num(market['rows_7d'] if market else None)} / tickers {_num(market['tickers_7d'] if market else None)}",
        (
            f"• {_state(candles_ok and not candles_wait, 'Candles canonicas', wait=candles_wait)} "
            f"— velas {escape(str(candle_day or '-'))} "
            f"({_num(candles['latest_candle_assets'] if candles else None)} activos) | "
            + (
                f"hoy no aplica: {escape(closed_reason or 'mercado cerrado')}"
                if candles_closed_ok
                else (
                    f"precios dia {escape(str(candles['business_day'] if candles else '-'))} | "
                    f"faltantes hoy {_num(candles['missing_internal'] if candles else None)}"
                )
            )
        ),
        "",
        "<b>Decision log</b>",
        f"• Total {days}d: <b>{_num(decisions['total'] if decisions else None)}</b>",
        f"• Execution plan: {_num(decisions['execution_plan'] if decisions else None)} | APPROVED/executable: <b>{_num(decisions['approved_executable'] if decisions else None)}</b> | EXECUTED bot: <b>{_num(decisions['executed'] if decisions else None)}</b> | EXECUTED manual: <b>{_num(decisions['executed_manual'] if decisions else None)}</b>",
        f"• Blocked: {_num(decisions['blocked'] if decisions else None)} | Optimizer: {_num(decisions['optimizer'] if decisions else None)} | Radar: {_num(decisions['radar'] if decisions else None)}",
        f"• Outcomes cerrados: 5d {_num(decisions['closed_5d'] if decisions else None)} | 10d {_num(decisions['closed_10d'] if decisions else None)} | 20d {_num(decisions['closed_20d'] if decisions else None)}",
        "",
        "<b>Ejecucion real</b>",
    ]

    if not broker["exists"]:
        lines.append("• <b>WAIT</b> No existe broker_fills: no puedo validar movimientos reales todavia.")
    else:
        lines.append(
            f"• movimientos/fills Cocos: total {broker['total']} | reconciliados {broker['reconciled']} | pendientes {broker['unreconciled']}"
        )
        lines.append(f"• Ultimo fill: {_fmt_dt(broker.get('latest_executed_at'))}")

    lines.extend(["", "<b>Lectura</b>"])
    if not hard_ok:
        if candles_wait:
            lines.append("• Mercado cerrado o fuera de cierre: no exijo vela de hoy. Uso la ultima rueda canonica disponible.")
        else:
            lines.append("• Primero hay que estabilizar ingesta/candles. Sin eso cualquier metrica miente.")
    elif not execution_ready:
        lines.append("• El sistema esta capturando datos y decisiones, pero aun no hay fills EXECUTED reconciliados.")
        lines.append("• Para confiar en performance real falta importar/reconciliar movimientos Cocos.")
    elif not outcomes_ready:
        lines.append("• Hay ejecucion, pero faltan al menos 12 outcomes 5d cerrados para una lectura minima.")
    else:
        lines.append("• Hay base minima para diagnostico de performance/regresion, no para afirmar edge definitivo.")
        if closed_5d < 100:
            lines.append("• Muestra chica: mantener recoleccion de ruedas y revisar metricas por mes.")

    return "\n".join(lines)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Auditoria de confianza operacional")
    parser.add_argument("--days", type=int, default=180)
    parser.add_argument("--no-telegram", action="store_true")
    args = parser.parse_args()

    text = await build_confidence_audit(days=args.days)
    print(text)

    if args.no_telegram:
        return

    cfg = get_config()
    if cfg.scraper.telegram_enabled:
        TelegramNotifier(cfg.scraper.telegram_bot_token, cfg.scraper.telegram_chat_id).send_raw(text)


if __name__ == "__main__":
    asyncio.run(main())

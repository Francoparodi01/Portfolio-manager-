"""
src/scheduler/runner.py

Scheduler principal para Cocos Copilot.

Qué hace:
  - 10:31 ART → scrape mercado + portfolio, envia apertura e inicia loops intradia
  - 17:00 ART → run_full("17:00_FULL")
  - 17:01 ART → detiene loops intradía
  - 21:30 ART → run_update_outcomes()
  - Si arranca durante horario de mercado, inicia loops de inmediato.

Diseño intradía:
  Un único loop de scraping (sin competencia, sin login doble):
    - Mercado cada 90s.
    - Portfolio cada ~10min (dentro del mismo login).
  Risk guard separado: solo lee DB, sin Playwright.

Redis:
  Completamente opcional. Si falla, el sistema sigue funcionando.
  Se usa solo para heartbeats y flags de estado (fire-and-forget).

Coordinación de scraper:
  asyncio.Lock en proceso. Confiable, sin dependencia de red.
  run_scrape / run_full respetan el lock sin bloquear: si está ocupado, abortan
  con log honesto — nunca reportan éxito cuando abortaron.
"""

from __future__ import annotations

import asyncio
import os
import signal
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from html import escape
from zoneinfo import ZoneInfo

try:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.cron import CronTrigger
    HAS_APSCHEDULER = True
except ImportError:
    HAS_APSCHEDULER = False

from src.core.config import get_config
from src.core.logger import get_logger
from src.core.market_calendar import is_trading_day, market_closed_reason
from src.core.portfolio_cache import (
    cache_live_portfolio,
    cache_portfolio_snapshot,
    get_cached_portfolio_snapshot,
)
from src.core.redis_client import client as redis_client
from src.collector.cocos_scraper import CocosCapitalScraper
from src.collector.cocos_history import candles_to_frame
from src.collector.db import PortfolioDatabase
from src.collector.broker_movements import broker_fills_from_movements
from src.collector.live_portfolio import (
    PortfolioMoveAlert,
    build_live_portfolio,
    render_live_portfolio_alert,
    render_opening_portfolio_report,
    select_portfolio_move_alerts,
)
from src.collector.notifier import TelegramNotifier
from src.core.telegram_format import header as tg_header, note as tg_note, section as tg_section

logger = get_logger(__name__)

# ─── Constantes ────────────────────────────────────────────────────────────────

TIMEZONE = "America/Argentina/Buenos_Aires"
ART_TZ = ZoneInfo(TIMEZONE)
UTC = timezone.utc
BUSINESS_DAY_CRON = "mon-fri"

MARKET_OPEN_H, MARKET_OPEN_M = 10, 30
MARKET_CLOSE_H, MARKET_CLOSE_M = 17, 0

MARKET_POLL_SECONDS = 90       # frecuencia del loop de mercado
RISK_POLL_SECONDS = 60         # frecuencia del risk guard
PORTFOLIO_REFRESH_SECONDS = int(os.getenv("PORTFOLIO_REFRESH_SECONDS", "300"))
PORTFOLIO_OFFHOURS_REFRESH_SECONDS = 3600
COCOS_SYNC_FILLS = os.getenv("COCOS_SYNC_FILLS", "true").lower() == "true"
FILL_REFRESH_SECONDS = int(os.getenv("FILL_REFRESH_SECONDS", "300"))
PORTFOLIO_CACHE_TTL_SECONDS = int(os.getenv("PORTFOLIO_CACHE_TTL_SECONDS", "600"))
PORTFOLIO_LIVE_POLL_SECONDS = int(os.getenv("PORTFOLIO_LIVE_POLL_SECONDS", "60"))
PORTFOLIO_ALERT_MAJOR_PCT = float(os.getenv("PORTFOLIO_ALERT_MAJOR_PCT", "0.03"))
PORTFOLIO_ALERT_WEIGHTED_PCT = float(os.getenv("PORTFOLIO_ALERT_WEIGHTED_PCT", "0.02"))
PORTFOLIO_ALERT_MIN_WEIGHT = float(os.getenv("PORTFOLIO_ALERT_MIN_WEIGHT", "0.10"))
PORTFOLIO_ALERT_TTL_SECONDS = int(os.getenv("PORTFOLIO_ALERT_TTL_SECONDS", "86400"))
INTRADAY_REVALIDATION_ENABLED = os.getenv("INTRADAY_REVALIDATION_ENABLED", "true").lower() == "true"
INTRADAY_REVALIDATION_PCT = float(os.getenv("INTRADAY_REVALIDATION_PCT", "0.03"))
INTRADAY_REVALIDATION_MAX_PRICE_AGE_SECONDS = int(os.getenv("INTRADAY_REVALIDATION_MAX_PRICE_AGE_SECONDS", "1200"))
INTRADAY_REVALIDATION_LOOKBACK_DAYS = int(os.getenv("INTRADAY_REVALIDATION_LOOKBACK_DAYS", "7"))
INTRADAY_REVALIDATION_TTL_SECONDS = int(os.getenv("INTRADAY_REVALIDATION_TTL_SECONDS", "21600"))
RISK_ALERT_TTL_SECONDS = int(os.getenv("RISK_ALERT_TTL_SECONDS", "7200"))
STOP_TRIGGERED_ALERT_TTL_SECONDS = int(os.getenv("STOP_TRIGGERED_ALERT_TTL_SECONDS", "86400"))

WARNING_PCT = -0.04
CRITICAL_PCT = -0.06
STOP_NEAR_PCT = 0.02

# Redis keys (todos opcionales)
SCRAPER_LOCK_KEY = "cocos:lock:scraper"      # soft-lock cross-process: bot lo lee para saber si el runner está scrapando
MARKET_HEARTBEAT_KEY = "cocos:monitor:market:last_tick"
RISK_HEARTBEAT_KEY = "cocos:monitor:risk:last_check"
MONITOR_STATE_KEY = "cocos:monitor:state"
SCHEDULER_HEARTBEAT_KEY = "cocos:scheduler:last_heartbeat"
BOT_BUSY_KEY = "cocos:bot:busy"
PORTFOLIO_ALERT_KEY_PREFIX = "cocos:portfolio:alert"
INTRADAY_REVALIDATION_KEY_PREFIX = "cocos:intraday:revalidation"
RISK_ALERT_KEY_PREFIX = "cocos:risk:alert"

# Lock en proceso: garantiza un único scraper activo a la vez.
# Se crea la primera vez que se usa (dentro del event loop).
_scraper_lock: asyncio.Lock | None = None
_intraday_manager: "IntradayManager | None" = None


# ─── Helpers generales ─────────────────────────────────────────────────────────

def _get_scraper_lock() -> asyncio.Lock:
    """Lazy init del lock para que funcione dentro del event loop."""
    global _scraper_lock
    if _scraper_lock is None:
        _scraper_lock = asyncio.Lock()
    return _scraper_lock


def _now_art() -> datetime:
    return datetime.now(tz=ART_TZ)


def _is_business_day(now: datetime | None = None) -> bool:
    now = now or _now_art()
    return is_trading_day(now)


def _is_market_hours(now: datetime | None = None) -> bool:
    now = now or _now_art()
    current_mins = now.hour * 60 + now.minute
    open_mins = MARKET_OPEN_H * 60 + MARKET_OPEN_M
    close_mins = MARKET_CLOSE_H * 60 + MARKET_CLOSE_M
    return open_mins <= current_mins < close_mins


def _is_market_window(now: datetime | None = None) -> bool:
    now = now or _now_art()
    return _is_business_day(now) and _is_market_hours(now)


def _should_scrape_market(now: datetime | None = None) -> bool:
    now = now or _now_art()
    return _is_market_window(now)


def _should_scrape_portfolio(now: datetime | None = None) -> bool:
    return True


def _business_day_cron(hour: int, minute: int) -> CronTrigger:
    """Cron automatico: dispara lunes-viernes; cada job filtra feriados."""
    return CronTrigger(
        day_of_week=BUSINESS_DAY_CRON,
        hour=hour,
        minute=minute,
        timezone=TIMEZONE,
    )


def _safe_float(value, default: float | None = None) -> float | None:
    try:
        return float(value) if value is not None else default
    except Exception:
        return default


# ─── Redis helpers (fire-and-forget, nunca rompen el flujo) ────────────────────

async def _redis_set(key: str, value: str, ex: int = 3600) -> None:
    try:
        await redis_client.set(key, value, ex=ex)
    except Exception as e:
        logger.debug("Redis set ignorado [%s]: %s", key, e)


async def _redis_get(key: str) -> str | None:
    try:
        raw = await redis_client.get(key)
        if raw is None:
            return None
        return raw.decode() if isinstance(raw, bytes) else str(raw)
    except Exception as e:
        logger.debug("Redis get ignorado [%s]: %s", key, e)
        return None


async def _redis_delete(key: str) -> None:
    try:
        await redis_client.delete(key)
    except Exception as e:
        logger.debug("Redis delete ignorado [%s]: %s", key, e)


async def _heartbeat(key: str) -> None:
    await _redis_set(key, str(int(datetime.now(tz=UTC).timestamp())))


async def _scheduler_heartbeat_loop() -> None:
    while True:
        await _heartbeat(SCHEDULER_HEARTBEAT_KEY)
        await asyncio.sleep(30)


async def _set_monitor_state(state: str) -> None:
    await _redis_set(MONITOR_STATE_KEY, state)


async def _is_bot_busy() -> bool:
    """Retorna False si Redis no responde — no bloquear alertas por falla de infra."""
    return bool(await _redis_get(BOT_BUSY_KEY))


async def _cache_snapshot(snapshot) -> None:
    await cache_portfolio_snapshot(
        snapshot.to_dict(),
        ttl_seconds=PORTFOLIO_CACHE_TTL_SECONDS,
    )


# ─── Jobs programados ──────────────────────────────────────────────────────────

async def run_scrape(run_type: str = "SCHEDULED") -> dict:
    """
    Scrape de portfolio.
    Si el scraper está ocupado (loop intradía activo), aborta y lo dice claramente.
    Nunca reporta éxito si abortó.
    """
    cfg = get_config()
    notifier = TelegramNotifier(cfg.scraper.telegram_bot_token, cfg.scraper.telegram_chat_id)
    db = PortfolioDatabase(cfg.database.url)

    logger.info("=== run_scrape [%s] iniciando ===", run_type)
    result: dict = {"success": False, "run_type": run_type}

    lock = _get_scraper_lock()
    if lock.locked():
        logger.warning(
            "run_scrape [%s]: scraper ocupado por loop intradía — abortando (no es error, es coordinación normal)",
            run_type,
        )
        result["aborted"] = "scraper_busy"
        return result

    async with lock:
        # Soft-lock Redis: señaliza a otros procesos (bot) que el scraper está activo.
        # Fire-and-forget — si Redis falla, el scraping continúa igual.
        await _redis_set(SCRAPER_LOCK_KEY, f"run_scrape:{run_type}", ex=180)
        try:
            await db.connect()
            async with CocosCapitalScraper(cfg.scraper) as scraper:
                await scraper.login()
                snapshot = await scraper.scrape_portfolio()
                sid = await db.save_snapshot(snapshot)
                await _cache_snapshot(snapshot)

            result.update(
                success=True,
                snapshot_id=str(sid),
                positions=len(snapshot.positions),
            )
            logger.info(
                "run_scrape ok: %d posiciones · confianza %.2f · total %s ARS",
                len(snapshot.positions),
                snapshot.confidence_score,
                f"{snapshot.total_value_ars:,.0f}",
            )
            notifier.notify_scrape_complete(
                total_ars=float(snapshot.total_value_ars),
                positions_count=len(snapshot.positions),
                confidence=snapshot.confidence_score,
                cash_ars=float(snapshot.cash_ars),
            )
            if snapshot.positions:
                notifier.send_snapshot_json(snapshot.to_dict())

        except Exception as e:
            logger.error("run_scrape [%s] falló: %s", run_type, e, exc_info=True)
            notifier.notify_critical_error(run_type, str(e))
            result["error"] = str(e)
        finally:
            await _redis_delete(SCRAPER_LOCK_KEY)
            await db.close()

    return result


async def run_full(run_type: str = "FULL") -> dict:
    """
    Scrape completo: portfolio + mercado + análisis técnico.
    Si el scraper está ocupado, aborta con log honesto.
    """
    cfg = get_config()
    notifier = TelegramNotifier(cfg.scraper.telegram_bot_token, cfg.scraper.telegram_chat_id)
    db = PortfolioDatabase(cfg.database.url)

    logger.info("=== run_full [%s] iniciando ===", run_type)
    result: dict = {"success": False, "run_type": run_type}
    now = _now_art()
    if not _is_business_day(now):
        reason = market_closed_reason(now) or "mercado cerrado"
        logger.info("run_full [%s] omitido: %s", run_type, reason)
        result.update(skipped="market_closed", reason=reason)
        return result

    lock = _get_scraper_lock()
    if lock.locked():
        logger.warning(
            "run_full [%s]: scraper ocupado — abortando (el loop intradía corre en paralelo)",
            run_type,
        )
        result["aborted"] = "scraper_busy"
        return result

    async with lock:
        await _redis_set(SCRAPER_LOCK_KEY, f"run_full:{run_type}", ex=300)
        try:
            await db.connect()
            async with CocosCapitalScraper(cfg.scraper) as scraper:
                await scraper.login()

                snapshot = await scraper.scrape_portfolio()
                await db.save_snapshot(snapshot)
                await _cache_snapshot(snapshot)

                acciones = await scraper.scrape_market("ACCIONES")
                cedears = await scraper.scrape_cedears_segments()
                if acciones or cedears:
                    await db.save_market_prices(acciones + cedears)

                if COCOS_SYNC_FILLS:
                    try:
                        movements = await scraper.scrape_portfolio_movements()
                        fills = broker_fills_from_movements(movements)
                        saved_movements = await db.save_broker_movements(movements)
                        saved_fills = await db.save_broker_fills(fills)
                        reconciled_fills = await db.reconcile_broker_fills()
                        manual_fills = await db.materialize_unmatched_broker_fills()
                        logger.info(
                            "run_full: movements=%d/%d fills=%d/%d reconciliados=%d manuales=%d",
                            len(movements),
                            saved_movements,
                            len(fills),
                            saved_fills,
                            reconciled_fills,
                            manual_fills,
                        )
                    except Exception as e:
                        logger.warning("run_full: sync movements fallo (no critico): %s", e, exc_info=True)

            result.update(
                success=True,
                positions=len(snapshot.positions),
                acciones=len(acciones),
                cedears=len(cedears),
            )
            logger.info(
                "run_full ok: %d posiciones · %d acciones · %d cedears",
                len(snapshot.positions), len(acciones), len(cedears),
            )
            notifier.notify_scrape_complete(
                total_ars=float(snapshot.total_value_ars),
                positions_count=len(snapshot.positions),
                confidence=snapshot.confidence_score,
                cash_ars=float(snapshot.cash_ars),
            )
            notifier.send_raw(
                f"📊 Mercado EOD: {len(acciones)} acciones · {len(cedears)} CEDEARs guardados."
            )

            # Análisis técnico — no crítico, fallo no afecta el resultado principal
            if snapshot.positions:
                try:
                    from src.analysis.technical import (
                        analyze_portfolio_from_frames,
                        build_telegram_report,
                    )
                    frames = await _load_canonical_history_frames(db, snapshot.positions)
                    signals = analyze_portfolio_from_frames(frames)
                    report = build_telegram_report(signals, float(snapshot.total_value_ars))
                    notifier.send_raw(report)
                    logger.info("Análisis técnico: %d señales enviadas", len(signals))
                except Exception as e:
                    logger.warning("Análisis técnico falló (no crítico): %s", e)

        except Exception as e:
            logger.error("run_full [%s] falló: %s", run_type, e, exc_info=True)
            notifier.notify_critical_error(run_type, str(e))
            result["error"] = str(e)
        finally:
            await _redis_delete(SCRAPER_LOCK_KEY)
            await db.close()

    return result


async def run_opening_portfolio_report(run_type: str = "10:31_OPENING_PORTFOLIO") -> dict:
    """
    Primera foto operativa de la rueda: mercado + portfolio + valuacion live.

    El objetivo es enviar una devolucion clara de apertura usando el mismo
    estandar de datos que el resto del sistema: precios desde market_prices y
    posiciones/cash desde un snapshot real de Cocos.
    """
    now = _now_art()
    if not _is_business_day(now):
        reason = market_closed_reason(now) or "mercado cerrado"
        logger.info("run_opening_portfolio_report [%s] omitido: %s", run_type, reason)
        return {
            "success": False,
            "run_type": run_type,
            "skipped": "market_closed",
            "reason": reason,
        }

    cfg = get_config()
    notifier = TelegramNotifier(cfg.scraper.telegram_bot_token, cfg.scraper.telegram_chat_id)
    db = PortfolioDatabase(cfg.database.url)
    result: dict = {"success": False, "run_type": run_type}

    logger.info("=== run_opening_portfolio_report [%s] iniciando ===", run_type)

    lock = _get_scraper_lock()
    if lock.locked():
        logger.warning(
            "run_opening_portfolio_report [%s]: scraper ocupado — abortando",
            run_type,
        )
        result["aborted"] = "scraper_busy"
        return result

    async with lock:
        await _redis_set(SCRAPER_LOCK_KEY, f"opening_portfolio:{run_type}", ex=300)
        try:
            await db.connect()
            async with CocosCapitalScraper(cfg.scraper) as scraper:
                await scraper.login()

                acciones = await scraper.scrape_market("ACCIONES")
                cedears = await scraper.scrape_cedears_segments()
                if acciones or cedears:
                    await db.save_market_prices(acciones + cedears)
                    await _heartbeat(MARKET_HEARTBEAT_KEY)

                snapshot = await scraper.scrape_portfolio()
                await db.save_snapshot(snapshot)
                snapshot_payload = snapshot.to_dict()
                await _cache_snapshot(snapshot)

            latest_prices = await _latest_prices_with_previous_close(
                db,
                [p.get("ticker") for p in snapshot_payload.get("positions") or []],
                now.date(),
            )
            live_portfolio = build_live_portfolio(snapshot_payload, latest_prices)
            await cache_live_portfolio(
                live_portfolio,
                ttl_seconds=PORTFOLIO_CACHE_TTL_SECONDS,
            )

            notifier.send_raw(
                render_opening_portfolio_report(
                    live_portfolio,
                    title="APERTURA DE MERCADO - PORTFOLIO ACTUALIZADO",
                )
            )
            result.update(
                success=True,
                positions=len(snapshot.positions),
                acciones=len(acciones),
                cedears=len(cedears),
                price_coverage=live_portfolio.get("price_coverage_count", 0),
            )
            logger.info(
                "opening portfolio ok: %d posiciones · cobertura %s/%s · %dA + %dC",
                len(snapshot.positions),
                live_portfolio.get("price_coverage_count", 0),
                live_portfolio.get("positions_count", 0),
                len(acciones),
                len(cedears),
            )
        except Exception as e:
            logger.error("run_opening_portfolio_report [%s] falló: %s", run_type, e, exc_info=True)
            notifier.notify_critical_error(run_type, str(e))
            result["error"] = str(e)
        finally:
            await _redis_delete(SCRAPER_LOCK_KEY)
            await db.close()

    return result


async def _latest_prices_with_previous_close(
    db: PortfolioDatabase,
    tickers: list,
    today: date,
) -> list[dict]:
    latest_prices = await db.get_latest_market_prices()
    wanted = {str(t or "").upper() for t in tickers if str(t or "").strip()}
    previous_closes = await db.get_previous_candle_closes(
        list(wanted),
        before_day=today,
    )
    for row in latest_prices:
        ticker = str(row.get("ticker") or "").upper()
        previous_close = previous_closes.get(ticker)
        if previous_close:
            row["previous_close_price"] = previous_close
    return latest_prices


def _post_open_quality_warning(live_portfolio: dict, now: datetime) -> str | None:
    positions_count = int(live_portfolio.get("positions_count") or 0)
    covered = int(live_portfolio.get("price_coverage_count") or 0)
    if positions_count <= 0:
        return "portfolio vacio o sin posiciones; no hay marca post-open confiable."

    coverage = covered / positions_count if positions_count else 0.0
    if coverage < 0.80:
        return (
            f"cobertura de precios baja ({covered}/{positions_count}); "
            "usar este reporte solo como contexto, no como marca operable."
        )

    latest_ts: datetime | None = None
    for position in live_portfolio.get("positions") or []:
        raw_ts = position.get("market_price_ts")
        if not raw_ts:
            continue
        try:
            ts = datetime.fromisoformat(str(raw_ts).replace("Z", "+00:00"))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
        except Exception:
            continue
        latest_ts = ts if latest_ts is None or ts > latest_ts else latest_ts

    if latest_ts is None:
        return "sin timestamps de market_prices; no se puede validar frescura post-open."

    latest_art = latest_ts.astimezone(ART_TZ)
    now_art = now.astimezone(ART_TZ)
    if latest_art.date() != now_art.date():
        return (
            f"ultimo precio es de {latest_art.strftime('%d/%m %H:%M')} ART; "
            "todavia no hay precios de la rueda actual."
        )

    age_seconds = (now_art - latest_art).total_seconds()
    if age_seconds > 30 * 60:
        return (
            f"precios post-open con {age_seconds / 60:.0f} minutos de atraso; "
            "esperar proximo scrape antes de decidir."
        )

    return None


async def run_post_open_portfolio_report(run_type: str = "10:45_POST_OPEN_PORTFOLIO") -> dict:
    """
    Marca operativa post-open: usa ultimo snapshot real y precios ya tomados
    durante la rueda. No genera decisiones ni cambia el plan EOD.
    """
    now = _now_art()
    if not _is_market_window(now):
        reason = market_closed_reason(now) or "mercado cerrado"
        logger.info("run_post_open_portfolio_report [%s] omitido: %s", run_type, reason)
        return {
            "success": False,
            "run_type": run_type,
            "skipped": "market_closed",
            "reason": reason,
        }

    cfg = get_config()
    notifier = TelegramNotifier(cfg.scraper.telegram_bot_token, cfg.scraper.telegram_chat_id)
    db = PortfolioDatabase(cfg.database.url)
    result: dict = {"success": False, "run_type": run_type}

    try:
        await db.connect()
        snapshot = await get_cached_portfolio_snapshot()
        if snapshot is None:
            snapshot = await db.get_latest_snapshot()

        if not snapshot:
            result["error"] = "sin_snapshot"
            logger.warning("run_post_open_portfolio_report [%s]: sin snapshot", run_type)
            return result

        latest_prices = await _latest_prices_with_previous_close(
            db,
            [p.get("ticker") for p in snapshot.get("positions") or []],
            now.date(),
        )
        live_portfolio = build_live_portfolio(snapshot, latest_prices)
        warning = _post_open_quality_warning(live_portfolio, now)
        if warning:
            live_portfolio["post_open_warning"] = warning
        await cache_live_portfolio(
            live_portfolio,
            ttl_seconds=PORTFOLIO_CACHE_TTL_SECONDS,
        )

        title = (
            "POST OPEN - PRECIOS INSUFICIENTES"
            if warning else "POST OPEN - PORTFOLIO ACTUALIZADO"
        )
        notifier.send_raw(render_opening_portfolio_report(live_portfolio, title=title))
        result.update(
            success=not bool(warning),
            warning=warning,
            positions=live_portfolio.get("positions_count", 0),
            price_coverage=live_portfolio.get("price_coverage_count", 0),
            day_pnl_ars=live_portfolio.get("day_pnl_ars"),
            day_change_pct=live_portfolio.get("day_change_pct"),
        )
        logger.info(
            "post-open portfolio ok: %s/%s cobertura - pnl_dia=%s",
            live_portfolio.get("price_coverage_count", 0),
            live_portfolio.get("positions_count", 0),
            live_portfolio.get("day_pnl_ars"),
        )
    except Exception as e:
        logger.error("run_post_open_portfolio_report [%s] fallo: %s", run_type, e, exc_info=True)
        notifier.notify_critical_error(run_type, str(e))
        result["error"] = str(e)
    finally:
        await db.close()

    return result


async def run_update_outcomes() -> None:
    """Actualiza outcomes de decisiones pasadas. Solo DB, sin scraper."""
    if not _is_business_day():
        logger.info("update_outcomes omitido: %s", market_closed_reason() or "mercado cerrado")
        return

    cfg = get_config()
    db = PortfolioDatabase(cfg.database.url)
    try:
        await db.connect()
        updated = await db.update_outcomes(lookback_days=180)
        logger.info("update_outcomes: %s decisiones actualizadas", updated)
    except Exception as e:
        logger.error("update_outcomes falló: %s", e, exc_info=True)
    finally:
        try:
            await db.close()
        except Exception:
            pass


async def _load_canonical_history_frames(
    db: PortfolioDatabase,
    positions: list,
    limit: int = 260,
) -> dict:
    frames = {}
    latest_prices = {
        str(row.get("ticker", "") or "").upper(): row
        for row in await db.get_latest_market_prices()
    }
    for position in positions:
        ticker = str(getattr(position, "ticker", "") or "").upper()
        asset_type = getattr(getattr(position, "asset_type", None), "value", None)
        if not ticker:
            continue
        rows = await db.get_market_candles(
            ticker,
            asset_type=asset_type,
            limit=limit,
        )
        frame = candles_to_frame(rows)
        frame = _overlay_latest_market_price(frame, latest_prices.get(ticker))
        if len(frame) >= 60:
            frames[ticker] = frame
    return frames


def _overlay_latest_market_price(frame, latest_row: dict | None):
    """
    Ajusta solo el frame en memoria para el reporte tecnico EOD.

    market_candles puede tener la vela oficial del dia anterior o una vela Cocos
    stale. Para el reporte operativo se necesita que el ultimo Close refleje el
    ultimo market_prices fresco del dia, sin escribir nuevas velas ni tocar el
    pipeline canonico.
    """
    if frame is None or latest_row is None or getattr(frame, "empty", True):
        return frame
    try:
        import pandas as pd

        price = _safe_float(latest_row.get("last_price"))
        raw_ts = latest_row.get("ts")
        if price is None or price <= 0 or raw_ts is None:
            return frame

        market_ts = pd.Timestamp(raw_ts)
        if market_ts.tzinfo is None:
            market_ts = market_ts.tz_localize("UTC")
        else:
            market_ts = market_ts.tz_convert("UTC")

        if market_ts.tz_convert(TIMEZONE).date() != _now_art().date():
            return frame

        out = frame.copy()
        existing = None
        drop_indexes = []
        for idx in out.index:
            idx_ts = pd.Timestamp(idx)
            if idx_ts.tzinfo is None:
                idx_ts = idx_ts.tz_localize("UTC")
            else:
                idx_ts = idx_ts.tz_convert("UTC")
            if idx_ts.date() == market_ts.date():
                existing = out.loc[idx]
                drop_indexes.append(idx)

        open_price = high_price = low_price = close_price = float(price)
        volume = 0.0
        if existing is not None:
            if hasattr(existing, "iloc") and getattr(existing, "ndim", 1) > 1:
                existing = existing.iloc[-1]
            open_price = _safe_float(existing.get("Open"), close_price) or close_price
            high_price = max(_safe_float(existing.get("High"), close_price) or close_price, close_price)
            low_price = min(_safe_float(existing.get("Low"), close_price) or close_price, close_price)
            volume = _safe_float(existing.get("Volume"), 0.0) or 0.0

        if drop_indexes:
            out = out.drop(index=drop_indexes)
        out.loc[market_ts] = {
            "Open": open_price,
            "High": high_price,
            "Low": low_price,
            "Close": close_price,
            "Volume": volume,
            "Source": "internal_snapshot",
        }
        out = out.sort_index()

        sources = tuple(sorted(set(out["Source"].astype(str))))
        source_counts = {
            str(source): int(count)
            for source, count in out["Source"].value_counts().sort_index().items()
        }
        out.attrs["candle_sources"] = sources
        out.attrs["candle_source_counts"] = source_counts
        out.attrs["has_reconstructed_candles"] = "internal_snapshot" in sources
        return out
    except Exception as exc:
        logger.debug("overlay latest market price omitido: %s", exc)
        return frame


async def run_build_daily_candles() -> None:
    """Reconstruye la vela diaria propia desde market_prices. Solo DB, sin scraper."""
    if not _is_business_day():
        logger.info("build_daily_candles omitido: %s", market_closed_reason() or "mercado cerrado")
        return

    cfg = get_config()
    db = PortfolioDatabase(cfg.database.url)
    try:
        await db.connect()
        saved = await db.build_daily_candles_from_market_prices()
        logger.info("build_daily_candles: %s velas internas guardadas", saved)
    except Exception as e:
        logger.error("build_daily_candles fallo: %s", e, exc_info=True)
    finally:
        try:
            await db.close()
        except Exception:
            pass


async def run_verify_daily_candles() -> None:
    """Verifica cobertura diaria del pipeline market_prices -> internal_snapshot."""
    if not _is_business_day():
        logger.info("daily_candle_status omitido: %s", market_closed_reason() or "mercado cerrado")
        return

    cfg = get_config()
    db = PortfolioDatabase(cfg.database.url)
    try:
        await db.connect()
        status = await db.get_daily_candle_build_status()
        logger.info(
            "daily_candle_status %s: prices=%d internal=%d missing=%d",
            status["business_day"],
            status["price_assets"],
            status["internal_candles"],
            status["missing_internal"],
        )
        if status["price_assets"] > 0 and status["missing_internal"] > 0:
            logger.warning(
                "daily_candle_status incompleto: faltan %d velas internas",
                status["missing_internal"],
            )
    except Exception as e:
        logger.error("daily_candle_status fallo: %s", e, exc_info=True)
    finally:
        try:
            await db.close()
        except Exception:
            pass


# ─── Daily analysis health checks ──────────────────────────────────────────────

async def run_verify_decision_prices() -> None:
    """Verifica que las decisiones operativas del dia tengan precio de entrada."""
    if not _is_business_day():
        logger.info("decision_price_status omitido: %s", market_closed_reason() or "mercado cerrado")
        return

    cfg = get_config()
    notifier = TelegramNotifier(cfg.scraper.telegram_bot_token, cfg.scraper.telegram_chat_id)
    db = PortfolioDatabase(cfg.database.url)
    try:
        await db.connect()
        pool = await db.get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (
                        WHERE price_at_decision IS NULL OR price_at_decision <= 0
                    ) AS missing_price
                FROM decision_log
                WHERE decision_date = (NOW() AT TIME ZONE 'America/Argentina/Buenos_Aires')::date
                  AND COALESCE(source, '') IN ('execution_plan', 'radar')
                  AND decision IN ('BUY', 'SELL')
                """
            )
        total = int(row["total"] or 0) if row else 0
        missing = int(row["missing_price"] or 0) if row else 0
        if total and missing:
            msg = f"decision_price_status: {missing}/{total} decisiones de hoy sin price_at_decision"
            logger.warning(msg)
            notifier.send_raw(f"ADVERTENCIA: {msg}")
        else:
            logger.info(
                "decision_price_status OK: %s decisiones, %s sin precio",
                total,
                missing,
            )
    except Exception as e:
        logger.error("decision_price_status fallo: %s", e, exc_info=True)
    finally:
        try:
            await db.close()
        except Exception:
            pass


async def run_daily_analysis() -> None:
    """Corre el analisis principal despues de construir velas internas EOD."""
    if not _is_business_day():
        logger.info("daily_analysis omitido: %s", market_closed_reason() or "mercado cerrado")
        return

    cfg = get_config()
    notifier = TelegramNotifier(cfg.scraper.telegram_bot_token, cfg.scraper.telegram_chat_id)
    cmd = [sys.executable, "scripts/run_analysis.py", "--no-llm", "--no-sentiment"]
    logger.info("daily_analysis iniciando: %s", " ".join(cmd))
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=900)
        out = stdout.decode("utf-8", errors="replace")
        err = stderr.decode("utf-8", errors="replace")

        if proc.returncode != 0:
            logger.error(
                "daily_analysis fallo rc=%s stderr=%s",
                proc.returncode,
                err[-2000:],
            )
            notifier.notify_critical_error(
                "daily_analysis",
                err[-1200:] or f"run_analysis.py rc={proc.returncode}",
            )
            return

        logger.info(
            "daily_analysis OK stdout=%d chars stderr=%d chars",
            len(out),
            len(err),
        )
    except asyncio.TimeoutError:
        logger.error("daily_analysis timeout")
        notifier.notify_critical_error("daily_analysis", "Timeout ejecutando run_analysis.py")
    except Exception as e:
        logger.error("daily_analysis fallo: %s", e, exc_info=True)
        notifier.notify_critical_error("daily_analysis", str(e))

    await run_verify_decision_prices()


# ─── Risk alert ────────────────────────────────────────────────────────────────

@dataclass
class RiskAlert:
    ticker: str
    level: str          # WARNING | CRITICAL | STOP_NEAR | STOP_TRIGGERED
    current_price: float
    entry_price: float
    pnl_pct: float
    stop_loss_price: float | None = None
    target_price: float | None = None


@dataclass(frozen=True)
class IntradayRevalidationAlert:
    decision_id: int
    ticker: str
    decision: str
    decided_at: datetime
    plan_price: float
    current_price: float
    change_pct: float
    target_amount_ars: float
    current_weight: float | None = None
    target_weight: float | None = None
    reason: str | None = None
    price_ts: datetime | None = None


# ─── Intraday Manager ──────────────────────────────────────────────────────────

class IntradayManager:
    """
    Dos loops independientes durante horario de mercado:

    1. _scraper_loop:
       Un único loop de scraping — un login por iteración, sin competencia.
       - Mercado (ACCIONES + CEDEARS) cada MARKET_POLL_SECONDS (90s).
       - Portfolio cada PORTFOLIO_REFRESH_SECONDS (~10min), dentro del mismo login.
       Esto resuelve el problema de dos scrapers peleándose y logins dobles.

    2. _risk_guard_loop:
       Solo lee DB. Sin Playwright. Sin scraper.
       Emite alertas por Telegram según umbrales de PNL / stop loss.
    """

    def __init__(self) -> None:
        self.cfg = get_config()
        self.notifier = TelegramNotifier(
            self.cfg.scraper.telegram_bot_token,
            self.cfg.scraper.telegram_chat_id,
        )
        self._scraper_task: asyncio.Task | None = None
        self._risk_task: asyncio.Task | None = None
        self._portfolio_live_task: asyncio.Task | None = None
        self._running = False
        self._last_alert_sent: dict[str, datetime] = {}

    @property
    def running(self) -> bool:
        return self._running

    async def start(self) -> None:
        if self._running:
            logger.info("IntradayManager: loops ya activos, ignorando start()")
            return
        self._running = True
        self._scraper_task = asyncio.create_task(
            self._scraper_loop(), name="intraday_scraper"
        )
        self._risk_task = asyncio.create_task(
            self._risk_guard_loop(), name="intraday_risk_guard"
        )
        self._portfolio_live_task = asyncio.create_task(
            self._portfolio_live_loop(), name="intraday_portfolio_live"
        )
        await _set_monitor_state("running")
        logger.info("IntradayManager: loops intradía iniciados")
        try:
            self.notifier.send_raw(
                "🟢 <b>Monitoreo intradía iniciado</b>\n"
                "Mercado cada 90s · Portfolio cada 5min · Live cache cada 60s · "
                "Risk guard cada 60s · Revalidacion intradia official=False."
            )
        except Exception as e:
            logger.warning("No se pudo notificar inicio de monitoreo: %s", e)

    async def stop(self) -> None:
        self._running = False
        tasks = [
            t for t in (
                self._scraper_task,
                self._risk_task,
                self._portfolio_live_task,
            )
            if t is not None
        ]
        self._scraper_task = None
        self._risk_task = None
        self._portfolio_live_task = None

        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        await _set_monitor_state("stopped")
        logger.info("IntradayManager: loops intradía detenidos")
        try:
            self.notifier.send_raw("🔴 <b>Monitoreo intradía detenido</b>")
        except Exception as e:
            logger.warning("No se pudo notificar fin de monitoreo: %s", e)

    # ── Loop único de scraping ─────────────────────────────────────────────────

    async def _scraper_loop(self) -> None:
        """
        Un único loop de scraping.

        - En rueda (días hábiles 10:30-17:00 ART):
        * scrapea mercado cada MARKET_POLL_SECONDS
        * scrapea portfolio cada PORTFOLIO_REFRESH_SECONDS

        - Fuera de rueda / fines de semana:
        * NO scrapea mercado
        * S? scrapea portfolio cada PORTFOLIO_OFFHOURS_REFRESH_SECONDS
        """
        last_portfolio_ts: float = 0.0
        last_fills_ts: float = 0.0

        while self._running:
            now = _now_art()
            do_market = _should_scrape_market(now)
            do_portfolio = _should_scrape_portfolio(now)

            if not do_market and not do_portfolio:
                await asyncio.sleep(60)
                continue

            lock = _get_scraper_lock()
            if lock.locked():
                logger.info("Scraper loop: lock ocupado por job scheduled, esperando 20s...")
                await asyncio.sleep(20)
                continue

            now_ts = time.monotonic()

            portfolio_interval = (
                PORTFOLIO_REFRESH_SECONDS if do_market
                else PORTFOLIO_OFFHOURS_REFRESH_SECONDS
            )
            should_refresh_portfolio = (now_ts - last_portfolio_ts) >= portfolio_interval
            should_refresh_fills = (
                COCOS_SYNC_FILLS
                and (now_ts - last_fills_ts) >= FILL_REFRESH_SECONDS
            )

            # Si no toca ni mercado ni portfolio, dormir lo justo
            if not do_market and not should_refresh_portfolio and not should_refresh_fills:
                await asyncio.sleep(60)
                continue

            db = PortfolioDatabase(self.cfg.database.url)
            try:
                async with lock:
                    await _redis_set(SCRAPER_LOCK_KEY, "intraday_loop", ex=120)
                    try:
                        await db.connect()
                        async with CocosCapitalScraper(self.cfg.scraper) as scraper:
                            await scraper.login()

                            # ── Mercado solo en rueda ───────────────────────────────
                            if do_market:
                                acciones = await scraper.scrape_market("ACCIONES")
                                cedears = await scraper.scrape_cedears_segments()
                                total_prices = len(acciones) + len(cedears)

                                if total_prices > 0:
                                    await db.save_market_prices(acciones + cedears)
                                    await _heartbeat(MARKET_HEARTBEAT_KEY)
                                    logger.info(
                                        "Scraper loop: %d precios guardados (%dA + %dC)",
                                        total_prices, len(acciones), len(cedears),
                                    )
                                else:
                                    logger.info("Scraper loop: mercado sin filas en esta iteración")

                            # ── Portfolio siempre permitido ───────────────────────
                            if should_refresh_portfolio:
                                snapshot = await scraper.scrape_portfolio()
                                await db.save_snapshot(snapshot)
                                await _cache_snapshot(snapshot)
                                last_portfolio_ts = time.monotonic()
                                logger.info(
                                    "Scraper loop: portfolio guardado · %d posiciones · conf %.2f",
                                    len(snapshot.positions),
                                    snapshot.confidence_score,
                                )

                            if should_refresh_fills:
                                movements = await scraper.scrape_portfolio_movements()
                                fills = broker_fills_from_movements(movements)
                                saved_movements = await db.save_broker_movements(movements)
                                saved_fills = await db.save_broker_fills(fills)
                                reconciled_fills = await db.reconcile_broker_fills()
                                manual_fills = await db.materialize_unmatched_broker_fills()
                                last_fills_ts = time.monotonic()
                                logger.info(
                                    "Scraper loop: movements=%d/%d fills=%d/%d reconciliados=%d manuales=%d",
                                    len(movements),
                                    saved_movements,
                                    len(fills),
                                    saved_fills,
                                    reconciled_fills,
                                    manual_fills,
                                )

                    finally:
                        await _redis_delete(SCRAPER_LOCK_KEY)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(
                    "Scraper loop error (reintentará luego): %s",
                    e,
                    exc_info=True,
                )
            finally:
                try:
                    await db.close()
                except Exception:
                    pass

            # En rueda dormir corto; fuera de rueda/finde, no hace falta tan seguido
            await asyncio.sleep(MARKET_POLL_SECONDS if do_market else 300)

    # ── Risk guard (solo DB) ────────────────────────────────────────────────────

    async def _risk_guard_loop(self) -> None:
        """
        Lee DB. Calcula PNL contra entries de decision_log. Envía alertas.
        Sin Playwright. Sin scraper. Sin lock de scraper.
        """
        while self._running:
            if not _is_market_window():
                await asyncio.sleep(30)
                continue

            db = PortfolioDatabase(self.cfg.database.url)
            try:
                await db.connect()
                pool = await self._resolve_pool(db)
                if pool is None:
                    logger.warning("Risk guard: no se pudo obtener pool DB, reintentando en %ds", RISK_POLL_SECONDS)
                    await asyncio.sleep(RISK_POLL_SECONDS)
                    continue

                bot_busy = await _is_bot_busy()
                alerts = await self._compute_risk_alerts(pool)

                for alert in alerts:
                    # Silenciar alertas no críticas si el bot está procesando algo manual
                    if bot_busy and alert.level not in ("CRITICAL", "STOP_TRIGGERED"):
                        logger.info(
                            "Risk guard: bot busy, silenciando [%s %s]",
                            alert.level, alert.ticker,
                        )
                        continue
                    if await self._should_send_alert(alert):
                        if self._send_alert(alert):
                            await self._mark_alert_sent(alert)

                await _heartbeat(RISK_HEARTBEAT_KEY)

                if not alerts:
                    logger.info("Risk guard: todo dentro de parámetros")

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("Risk guard error (reintentará en %ds): %s", RISK_POLL_SECONDS, e, exc_info=True)
            finally:
                try:
                    await db.close()
                except Exception:
                    pass

            await asyncio.sleep(RISK_POLL_SECONDS)

    async def _portfolio_live_loop(self) -> None:
        """
        Recalcula una valuacion live del portfolio con market_prices.
        No usa Playwright: posiciones/cash salen del ultimo snapshot real cacheado.
        """
        while self._running:
            if not _is_market_window():
                await asyncio.sleep(30)
                continue

            db = PortfolioDatabase(self.cfg.database.url)
            try:
                await db.connect()
                snapshot = await get_cached_portfolio_snapshot()
                if snapshot is None:
                    snapshot = await db.get_latest_snapshot()
                    if snapshot:
                        await cache_portfolio_snapshot(
                            snapshot,
                            ttl_seconds=PORTFOLIO_CACHE_TTL_SECONDS,
                        )

                if not snapshot:
                    logger.info("Portfolio live: sin snapshot disponible")
                    await asyncio.sleep(PORTFOLIO_LIVE_POLL_SECONDS)
                    continue

                latest_prices = await _latest_prices_with_previous_close(
                    db,
                    [p.get("ticker") for p in snapshot.get("positions") or []],
                    _now_art().date(),
                )
                live_portfolio = build_live_portfolio(snapshot, latest_prices)
                await cache_live_portfolio(
                    live_portfolio,
                    ttl_seconds=PORTFOLIO_CACHE_TTL_SECONDS,
                )

                alerts = select_portfolio_move_alerts(
                    live_portfolio,
                    major_abs_pct=PORTFOLIO_ALERT_MAJOR_PCT,
                    weighted_abs_pct=PORTFOLIO_ALERT_WEIGHTED_PCT,
                    min_weight=PORTFOLIO_ALERT_MIN_WEIGHT,
                )
                unseen_alerts = [
                    alert for alert in alerts
                    if not await self._portfolio_alert_seen(alert)
                ]

                if unseen_alerts:
                    if await _is_bot_busy():
                        logger.info(
                            "Portfolio live: bot busy, postergando %d alerta(s)",
                            len(unseen_alerts),
                        )
                    else:
                        sent = self.notifier.send_raw(
                            render_live_portfolio_alert(unseen_alerts, live_portfolio)
                        )
                        if sent:
                            for alert in unseen_alerts:
                                await self._mark_portfolio_alert(alert)
                            logger.info(
                                "Portfolio live: %d alerta(s) enviadas",
                                len(unseen_alerts),
                            )

                if INTRADAY_REVALIDATION_ENABLED:
                    pool = await self._resolve_pool(db)
                    if pool is not None:
                        revalidations = await self._compute_intraday_revalidations(
                            pool,
                            latest_prices,
                        )
                        unseen_revalidations = [
                            alert for alert in revalidations
                            if not await self._intraday_revalidation_seen(alert)
                        ]
                        if unseen_revalidations:
                            if await _is_bot_busy():
                                logger.info(
                                    "Intraday revalidation: bot busy, postergando %d alerta(s)",
                                    len(unseen_revalidations),
                                )
                            else:
                                sent = self.notifier.send_raw(
                                    self._render_intraday_revalidations(unseen_revalidations)
                                )
                                if sent:
                                    for alert in unseen_revalidations:
                                        await self._mark_intraday_revalidation(alert)
                                    logger.info(
                                        "Intraday revalidation: %d alerta(s) enviadas",
                                        len(unseen_revalidations),
                                    )

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "Portfolio live loop error (reintentara en %ds): %s",
                    PORTFOLIO_LIVE_POLL_SECONDS,
                    exc,
                    exc_info=True,
                )
            finally:
                try:
                    await db.close()
                except Exception:
                    pass

            await asyncio.sleep(PORTFOLIO_LIVE_POLL_SECONDS)

    async def _compute_intraday_revalidations(
        self,
        pool,
        latest_prices: list[dict],
    ) -> list[IntradayRevalidationAlert]:
        if not latest_prices:
            return []

        latest_by_ticker = {
            str(row.get("ticker") or "").upper(): row
            for row in latest_prices
            if str(row.get("ticker") or "").strip()
        }
        if not latest_by_ticker:
            return []

        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (ticker)
                    id,
                    decided_at,
                    ticker,
                    decision,
                    price_at_decision::float AS plan_price,
                    ABS(COALESCE(theoretical_amount_ars, executed_amount_ars, 0))::float AS target_amount_ars,
                    current_weight::float AS current_weight,
                    target_weight::float AS target_weight,
                    layers->>'reason' AS reason
                FROM decision_log
                WHERE decided_at >= NOW() - ($1::int * INTERVAL '1 day')
                  AND COALESCE(source, layers->>'source') = 'execution_plan'
                  AND status IN ('APPROVED', 'EXECUTED')
                  AND decision_type = 'executable'
                  AND decision IN ('BUY', 'SELL')
                  AND price_at_decision IS NOT NULL
                  AND price_at_decision > 0
                  AND (
                    (decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::time >= TIME '17:00'
                    OR (decided_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::time < TIME '10:30'
                  )
                ORDER BY ticker, decided_at DESC, id DESC
                """,
                INTRADAY_REVALIDATION_LOOKBACK_DAYS,
            )

        now = _now_art()
        alerts: list[IntradayRevalidationAlert] = []
        for row in rows:
            ticker = str(row["ticker"] or "").upper()
            latest = latest_by_ticker.get(ticker)
            if not latest:
                continue

            current_price = _safe_float(latest.get("last_price"))
            plan_price = _safe_float(row["plan_price"])
            if not current_price or not plan_price or plan_price <= 0:
                continue

            price_ts = self._parse_price_ts(latest.get("ts"))
            if price_ts is None:
                continue
            age_seconds = (now - price_ts.astimezone(ART_TZ)).total_seconds()
            if age_seconds < 0 or age_seconds > INTRADAY_REVALIDATION_MAX_PRICE_AGE_SECONDS:
                continue

            change_pct = (float(current_price) / float(plan_price)) - 1.0
            if abs(change_pct) < INTRADAY_REVALIDATION_PCT:
                continue

            alerts.append(
                IntradayRevalidationAlert(
                    decision_id=int(row["id"]),
                    ticker=ticker,
                    decision=str(row["decision"] or "").upper(),
                    decided_at=row["decided_at"],
                    plan_price=float(plan_price),
                    current_price=float(current_price),
                    change_pct=float(change_pct),
                    target_amount_ars=float(row["target_amount_ars"] or 0),
                    current_weight=(
                        float(row["current_weight"])
                        if row["current_weight"] is not None
                        else None
                    ),
                    target_weight=(
                        float(row["target_weight"])
                        if row["target_weight"] is not None
                        else None
                    ),
                    reason=str(row["reason"] or "").strip() or None,
                    price_ts=price_ts,
                )
            )

        return sorted(alerts, key=lambda alert: abs(alert.change_pct), reverse=True)

    @staticmethod
    def _parse_price_ts(value) -> datetime | None:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=ART_TZ)
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=ART_TZ)
        except Exception:
            return None

    async def _intraday_revalidation_seen(self, alert: IntradayRevalidationAlert) -> bool:
        return bool(await _redis_get(self._intraday_revalidation_key(alert)))

    async def _mark_intraday_revalidation(self, alert: IntradayRevalidationAlert) -> None:
        await _redis_set(
            self._intraday_revalidation_key(alert),
            f"{alert.change_pct:+.6f}",
            ex=INTRADAY_REVALIDATION_TTL_SECONDS,
        )

    @staticmethod
    def _intraday_revalidation_key(alert: IntradayRevalidationAlert) -> str:
        business_day = _now_art().strftime("%Y%m%d")
        threshold = max(INTRADAY_REVALIDATION_PCT, 0.0001)
        bucket = int(abs(alert.change_pct) / threshold)
        direction = "UP" if alert.change_pct > 0 else "DOWN"
        return (
            f"{INTRADAY_REVALIDATION_KEY_PREFIX}:{business_day}:"
            f"{alert.decision_id}:{direction}:{bucket}"
        )

    def _render_intraday_revalidations(
        self,
        alerts: list[IntradayRevalidationAlert],
    ) -> str:
        lines = tg_header(
            "🔄 Revalidación intradía",
            subtitle="official=False | no modifica auditoría ni performance",
        ) + [
            "Lectura: el precio cambió contra un plan previo. Sirve para decidir si mirar, esperar o revalidar.",
            "",
        ]

        for alert in alerts[:5]:
            action = self._intraday_action_text(alert)
            plan = self._plan_label(alert)
            price_time = (
                alert.price_ts.astimezone(ART_TZ).strftime("%H:%M")
                if alert.price_ts
                else "N/A"
            )
            lines += [
                tg_section(str(alert.ticker)),
                f"Cambio: <b>{alert.change_pct:+.2%}</b> desde plan EOD",
                (
                    f"Plan original: <b>{escape(plan)}</b> en "
                    f"<b>{self._fmt_price(alert.plan_price)}</b>"
                ),
                f"Monto plan: <b>{self._fmt_ars(alert.target_amount_ars)}</b>",
                (
                    f"Precio actual: <b>{self._fmt_price(alert.current_price)}</b> "
                    f"({price_time} ART)"
                ),
                f"Acción sugerida: <b>{escape(action)}</b>",
            ]
            if alert.reason:
                lines.append(f"Motivo plan: {escape(self._clean_reason(alert.reason))[:180]}")
            lines.append("")

        lines.append(tg_note("Contexto operativo, no nueva decisión oficial. Si se ejecuta, requiere fill real para entrar a auditoría."))
        return "\n".join(lines)

    @staticmethod
    def _fmt_price(value: float) -> str:
        return f"${value:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")

    @staticmethod
    def _fmt_ars(value: float) -> str:
        return f"${value:,.0f} ARS".replace(",", ".")

    @staticmethod
    def _clean_reason(value: str) -> str:
        return (
            str(value or "")
            .replace("posici?n", "posicion")
            .replace("exposici?n", "exposicion")
            .replace(" ? ", " -> ")
            .replace("?", "")
        )

    @staticmethod
    def _plan_label(alert: IntradayRevalidationAlert) -> str:
        if alert.decision == "SELL":
            if (
                alert.target_weight is not None
                and alert.current_weight is not None
                and alert.target_weight <= 0.001
            ):
                return "SELL total"
            return "SELL parcial"
        return "BUY"

    @staticmethod
    def _intraday_action_text(alert: IntradayRevalidationAlert) -> str:
        if alert.decision == "SELL":
            if alert.change_pct < 0:
                return "Evaluar si ejecutar ahora o esperar cierre"
            return "Evaluar ejecutar recorte si el motivo sigue vigente"

        if alert.change_pct > 0:
            return "No perseguir precio automaticamente; revalidar entrada"
        return "Evaluar si el pullback mejora entrada o esperar cierre"

    @staticmethod
    async def _resolve_pool(db: PortfolioDatabase):
        """Intenta obtener el pool asyncpg desde la instancia de DB."""
        if hasattr(db, "get_pool"):
            pool = await db.get_pool()
            if pool is not None:
                return pool
        return getattr(db, "_db_pool", None) or getattr(db, "_pool", None)

    async def _compute_risk_alerts(self, pool) -> list[RiskAlert]:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                WITH latest_prices AS (
                    SELECT DISTINCT ON (ticker)
                        ticker,
                        last_price
                    FROM market_prices
                    WHERE last_price IS NOT NULL
                    ORDER BY ticker, ts DESC
                ),
                latest_buys AS (
                    SELECT DISTINCT ON (ticker)
                        ticker,
                        price_at_decision,
                        stop_loss_pct,
                        stop_loss_price,
                        target_price
                    FROM decision_log
                    WHERE decision = 'BUY'
                      AND price_at_decision IS NOT NULL
                      AND outcome_5d IS NULL
                      AND closed_at IS NULL
                      AND COALESCE(was_stopped, FALSE) IS FALSE
                      AND COALESCE(status, '') IN ('EXECUTED', 'EXECUTED_MANUAL')
                    ORDER BY ticker, decided_at DESC
                )
                SELECT
                    b.ticker,
                    b.price_at_decision,
                    b.stop_loss_pct,
                    b.stop_loss_price,
                    b.target_price,
                    p.last_price
                FROM latest_buys b
                JOIN latest_prices p ON p.ticker = b.ticker
                """
            )

        alerts: list[RiskAlert] = []
        for row in rows:
            ticker = str(row["ticker"]).upper()
            entry = _safe_float(row["price_at_decision"])
            current = _safe_float(row["last_price"])
            stop_price = _safe_float(row["stop_loss_price"])
            target_price = _safe_float(row["target_price"])
            stop_pct = _safe_float(row["stop_loss_pct"])

            if entry is None or current is None or entry == 0:
                continue

            # Derivar stop_price desde stop_pct si no viene explícito
            if stop_price is None and stop_pct is not None:
                pct = stop_pct / 100.0 if abs(stop_pct) > 1 else stop_pct
                pct = -abs(pct)  # siempre negativo
                stop_price = entry * (1.0 + pct)

            pnl_pct = (current / entry) - 1.0

            if stop_price is not None and current <= stop_price:
                level = "STOP_TRIGGERED"
            elif (
                stop_price is not None
                and stop_price > 0
                and 0 < (current - stop_price) / stop_price <= STOP_NEAR_PCT
            ):
                level = "STOP_NEAR"
            elif pnl_pct <= CRITICAL_PCT:
                level = "CRITICAL"
            elif pnl_pct <= WARNING_PCT:
                level = "WARNING"
            else:
                continue

            alerts.append(RiskAlert(
                ticker=ticker,
                level=level,
                current_price=current,
                entry_price=entry,
                pnl_pct=pnl_pct,
                stop_loss_price=stop_price,
                target_price=target_price,
            ))

        return alerts

    async def _should_send_alert(self, alert: RiskAlert) -> bool:
        key = f"{alert.ticker}:{alert.level}"
        last = self._last_alert_sent.get(key)
        ttl = self._alert_ttl(alert)
        if last is None:
            return await _redis_get(self._alert_key(alert)) is None

        elapsed = (datetime.now(tz=UTC) - last).total_seconds()
        if elapsed < ttl:
            return False

        return await _redis_get(self._alert_key(alert)) is None

    async def _mark_alert_sent(self, alert: RiskAlert) -> None:
        self._last_alert_sent[f"{alert.ticker}:{alert.level}"] = datetime.now(tz=UTC)
        await _redis_set(self._alert_key(alert), "1", ex=self._alert_ttl(alert))

    @staticmethod
    def _alert_key(alert: RiskAlert) -> str:
        entry = round(alert.entry_price, 4)
        stop = round(alert.stop_loss_price or 0.0, 4)
        return f"{RISK_ALERT_KEY_PREFIX}:{alert.ticker}:{alert.level}:{entry}:{stop}"

    @staticmethod
    def _alert_ttl(alert: RiskAlert) -> int:
        if alert.level == "STOP_TRIGGERED":
            return STOP_TRIGGERED_ALERT_TTL_SECONDS
        return RISK_ALERT_TTL_SECONDS

    def _send_alert(self, alert: RiskAlert) -> bool:
        pnl = alert.pnl_pct * 100.0
        stop_txt = f"\nStop: <b>${alert.stop_loss_price:,.2f}</b>" if alert.stop_loss_price else ""
        target_txt = f"\nTarget: <b>${alert.target_price:,.2f}</b>" if alert.target_price else ""
        icons = {
            "STOP_TRIGGERED": "🚨",
            "STOP_NEAR":      "⚠️",
            "CRITICAL":       "🔴",
            "WARNING":        "🟡",
        }
        icon = icons.get(alert.level, "⚠️")

        msg = (
            f"{icon} <b>Alerta de riesgo — {alert.ticker}</b>\n"
            f"Estado: <code>{alert.level}</code>\n"
            f"Precio: <b>${alert.current_price:,.2f}</b> · Entrada ejecutada: <b>${alert.entry_price:,.2f}</b>"
            f"{stop_txt}{target_txt}\n"
            f"PNL: <b>{pnl:+.2f}%</b>\n"
            f"{tg_note('Alerta sobre posición ejecutada; no dispara órdenes automáticas.')}"
        )
        try:
            self.notifier.send_raw(msg)
            logger.warning(
                "Risk alert enviada: %s %s (PNL %.2f%%)",
                alert.level, alert.ticker, pnl,
            )
            return True
        except Exception as e:
            logger.warning("No se pudo enviar alerta Telegram: %s", e)
            return False

    async def _portfolio_alert_seen(self, alert: PortfolioMoveAlert) -> bool:
        return bool(await _redis_get(self._portfolio_alert_key(alert)))

    async def _mark_portfolio_alert(self, alert: PortfolioMoveAlert) -> None:
        await _redis_set(
            self._portfolio_alert_key(alert),
            f"{alert.change_pct_1d:+.6f}",
            ex=PORTFOLIO_ALERT_TTL_SECONDS,
        )

    @staticmethod
    def _portfolio_alert_key(alert: PortfolioMoveAlert) -> str:
        business_day = _now_art().strftime("%Y%m%d")
        return (
            f"{PORTFOLIO_ALERT_KEY_PREFIX}:{business_day}:"
            f"{alert.ticker}:{alert.direction}:{alert.level}"
        )


# ─── Wrappers de start/stop para APScheduler ───────────────────────────────────

async def start_intraday_loops() -> None:
    global _intraday_manager
    if _intraday_manager is None:
        _intraday_manager = IntradayManager()
    await _intraday_manager.start()


async def stop_intraday_loops() -> None:
    global _intraday_manager
    if _intraday_manager is not None:
        await _intraday_manager.stop()


# ─── Scheduler principal ───────────────────────────────────────────────────────

async def run_opening_portfolio_report_then_start_intraday() -> None:
    """
    Apertura coordinada: una sola sesion de scraping para mercado + portfolio,
    y el loop intradia arranca apenas termina esa foto inicial.
    """
    result = await run_opening_portfolio_report("10:31_OPENING_PORTFOLIO")
    if result.get("skipped") == "market_closed":
        logger.info("Apertura intradia omitida: %s", result.get("reason"))
        return
    await start_intraday_loops()


async def _scheduler_main() -> None:
    if not HAS_APSCHEDULER:
        raise ImportError("apscheduler no instalado: pip install apscheduler>=3.10")

    scheduler = AsyncIOScheduler(timezone=TIMEZONE)

    scheduler.add_job(
        run_opening_portfolio_report_then_start_intraday,
        _business_day_cron(hour=10, minute=31),
        id="opening_portfolio_report",
        name="Opening portfolio + intraday 10:31 ART",
        misfire_grace_time=300,
        replace_existing=True,
    )
    scheduler.add_job(
        run_post_open_portfolio_report,
        _business_day_cron(hour=10, minute=45),
        id="post_open_portfolio_report",
        name="Post-open portfolio mark 10:45 ART",
        misfire_grace_time=300,
        replace_existing=True,
    )
    scheduler.add_job(
        run_full,
        _business_day_cron(hour=17, minute=2),
        args=["17:02_FULL"],
        id="portfolio_eod",
        name="Full 17:02 ART",
        misfire_grace_time=300,
        replace_existing=True,
    )
    scheduler.add_job(
        stop_intraday_loops,
        _business_day_cron(hour=16, minute=59),
        id="intraday_stop",
        name="Intraday stop 16:59 ART",
        misfire_grace_time=300,
        replace_existing=True,
    )
    scheduler.add_job(
        run_build_daily_candles,
        _business_day_cron(hour=17, minute=5),
        id="build_daily_candles",
        name="Build daily internal candles 17:05 ART",
        misfire_grace_time=600,
        replace_existing=True,
    )
    scheduler.add_job(
        run_verify_daily_candles,
        _business_day_cron(hour=17, minute=10),
        id="verify_daily_candles",
        name="Verify daily internal candles 17:10 ART",
        misfire_grace_time=600,
        replace_existing=True,
    )
    scheduler.add_job(
        run_daily_analysis,
        _business_day_cron(hour=17, minute=12),
        id="daily_analysis",
        name="Daily analysis 17:12 ART",
        misfire_grace_time=900,
        replace_existing=True,
    )
    scheduler.add_job(
        run_update_outcomes,
        _business_day_cron(hour=21, minute=30),
        id="update_outcomes_daily",
        name="Update outcomes 21:30 ART",
        misfire_grace_time=600,
        replace_existing=True,
    )

    heartbeat_task = asyncio.create_task(
        _scheduler_heartbeat_loop(),
        name="scheduler_heartbeat",
    )
    scheduler.start()
    logger.info(
        "Scheduler activo: 10:31 apertura portfolio + intraday on; 10:45 post-open; 16:59 intraday off; 17:02 full; 17:05 candles; 17:10 verify; 17:12 analysis; 21:30 outcomes"
    )

    # Si arrancamos durante rueda, iniciar loops de inmediato
    now = _now_art()
    if _is_market_window(now):
        logger.info(
            "Scheduler arrancó dentro de rueda (%s ART) — iniciando loops intradía",
            now.strftime("%H:%M:%S"),
        )
        await start_intraday_loops()

    stop_event = asyncio.Event()

    def _handle_signal() -> None:
        logger.info("Señal recibida — iniciando apagado limpio...")
        scheduler.shutdown(wait=False)
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except (NotImplementedError, OSError):
            pass

    await stop_event.wait()
    heartbeat_task.cancel()
    await stop_intraday_loops()
    logger.info("Scheduler apagado limpiamente")


def start_scheduler() -> None:
    try:
        asyncio.run(_scheduler_main())
    except KeyboardInterrupt:
        logger.info("Scheduler detenido por usuario")


if __name__ == "__main__":
    start_scheduler()

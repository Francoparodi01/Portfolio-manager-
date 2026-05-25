"""
src/scheduler/runner.py

Scheduler principal para Cocos Copilot.

QuÃ© hace:
  - 10:31 ART â†’ scrape mercado + portfolio, envia apertura e inicia loops intradia
  - 17:00 ART â†’ run_full("17:00_FULL")
  - 17:01 ART â†’ detiene loops intradÃ­a
  - 21:30 ART â†’ run_update_outcomes()
  - Si arranca durante horario de mercado, inicia loops de inmediato.

DiseÃ±o intradÃ­a:
  Un Ãºnico loop de scraping (sin competencia, sin login doble):
    - Mercado cada 90s.
    - Portfolio cada ~10min (dentro del mismo login).
  Risk guard separado: solo lee DB, sin Playwright.

Redis:
  Completamente opcional. Si falla, el sistema sigue funcionando.
  Se usa solo para heartbeats y flags de estado (fire-and-forget).

CoordinaciÃ³n de scraper:
  asyncio.Lock en proceso. Confiable, sin dependencia de red.
  run_scrape / run_full respetan el lock sin bloquear: si estÃ¡ ocupado, abortan
  con log honesto â€” nunca reportan Ã©xito cuando abortaron.
"""

from __future__ import annotations

import asyncio
import os
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
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
from src.collector.live_portfolio import (
    PortfolioMoveAlert,
    build_live_portfolio,
    render_live_portfolio_alert,
    render_opening_portfolio_report,
    select_portfolio_move_alerts,
)
from src.collector.notifier import TelegramNotifier

logger = get_logger(__name__)

# â”€â”€â”€ Constantes â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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
COCOS_SYNC_FILLS = os.getenv("COCOS_SYNC_FILLS", "false").lower() == "true"
FILL_REFRESH_SECONDS = int(os.getenv("FILL_REFRESH_SECONDS", "3600"))
PORTFOLIO_CACHE_TTL_SECONDS = int(os.getenv("PORTFOLIO_CACHE_TTL_SECONDS", "600"))
PORTFOLIO_LIVE_POLL_SECONDS = int(os.getenv("PORTFOLIO_LIVE_POLL_SECONDS", "60"))
PORTFOLIO_ALERT_MAJOR_PCT = float(os.getenv("PORTFOLIO_ALERT_MAJOR_PCT", "0.03"))
PORTFOLIO_ALERT_WEIGHTED_PCT = float(os.getenv("PORTFOLIO_ALERT_WEIGHTED_PCT", "0.02"))
PORTFOLIO_ALERT_MIN_WEIGHT = float(os.getenv("PORTFOLIO_ALERT_MIN_WEIGHT", "0.10"))
PORTFOLIO_ALERT_TTL_SECONDS = int(os.getenv("PORTFOLIO_ALERT_TTL_SECONDS", "86400"))
RISK_ALERT_TTL_SECONDS = int(os.getenv("RISK_ALERT_TTL_SECONDS", "1800"))
STOP_TRIGGERED_ALERT_TTL_SECONDS = int(os.getenv("STOP_TRIGGERED_ALERT_TTL_SECONDS", "86400"))

WARNING_PCT = -0.04
CRITICAL_PCT = -0.06
STOP_NEAR_PCT = 0.02

# Redis keys (todos opcionales)
SCRAPER_LOCK_KEY = "cocos:lock:scraper"      # soft-lock cross-process: bot lo lee para saber si el runner estÃ¡ scrapando
MARKET_HEARTBEAT_KEY = "cocos:monitor:market:last_tick"
RISK_HEARTBEAT_KEY = "cocos:monitor:risk:last_check"
MONITOR_STATE_KEY = "cocos:monitor:state"
SCHEDULER_HEARTBEAT_KEY = "cocos:scheduler:last_heartbeat"
BOT_BUSY_KEY = "cocos:bot:busy"
PORTFOLIO_ALERT_KEY_PREFIX = "cocos:portfolio:alert"
RISK_ALERT_KEY_PREFIX = "cocos:risk:alert"

# Lock en proceso: garantiza un Ãºnico scraper activo a la vez.
# Se crea la primera vez que se usa (dentro del event loop).
_scraper_lock: asyncio.Lock | None = None
_intraday_manager: "IntradayManager | None" = None


# â”€â”€â”€ Helpers generales â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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


# â”€â”€â”€ Redis helpers (fire-and-forget, nunca rompen el flujo) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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
    """Retorna False si Redis no responde â€” no bloquear alertas por falla de infra."""
    return bool(await _redis_get(BOT_BUSY_KEY))


async def _cache_snapshot(snapshot) -> None:
    await cache_portfolio_snapshot(
        snapshot.to_dict(),
        ttl_seconds=PORTFOLIO_CACHE_TTL_SECONDS,
    )


# â”€â”€â”€ Jobs programados â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

async def run_scrape(run_type: str = "SCHEDULED") -> dict:
    """
    Scrape de portfolio.
    Si el scraper estÃ¡ ocupado (loop intradÃ­a activo), aborta y lo dice claramente.
    Nunca reporta Ã©xito si abortÃ³.
    """
    cfg = get_config()
    notifier = TelegramNotifier(cfg.scraper.telegram_bot_token, cfg.scraper.telegram_chat_id)
    db = PortfolioDatabase(cfg.database.url)

    logger.info("=== run_scrape [%s] iniciando ===", run_type)
    result: dict = {"success": False, "run_type": run_type}

    lock = _get_scraper_lock()
    if lock.locked():
        logger.warning(
            "run_scrape [%s]: scraper ocupado por loop intradÃ­a â€” abortando (no es error, es coordinaciÃ³n normal)",
            run_type,
        )
        result["aborted"] = "scraper_busy"
        return result

    async with lock:
        # Soft-lock Redis: seÃ±aliza a otros procesos (bot) que el scraper estÃ¡ activo.
        # Fire-and-forget â€” si Redis falla, el scraping continÃºa igual.
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
                "run_scrape ok: %d posiciones Â· confianza %.2f Â· total %s ARS",
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
            logger.error("run_scrape [%s] fallÃ³: %s", run_type, e, exc_info=True)
            notifier.notify_critical_error(run_type, str(e))
            result["error"] = str(e)
        finally:
            await _redis_delete(SCRAPER_LOCK_KEY)
            await db.close()

    return result


async def run_full(run_type: str = "FULL") -> dict:
    """
    Scrape completo: portfolio + mercado + anÃ¡lisis tÃ©cnico.
    Si el scraper estÃ¡ ocupado, aborta con log honesto.
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
            "run_full [%s]: scraper ocupado â€” abortando (el loop intradÃ­a corre en paralelo)",
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

            result.update(
                success=True,
                positions=len(snapshot.positions),
                acciones=len(acciones),
                cedears=len(cedears),
            )
            logger.info(
                "run_full ok: %d posiciones Â· %d acciones Â· %d cedears",
                len(snapshot.positions), len(acciones), len(cedears),
            )
            notifier.notify_scrape_complete(
                total_ars=float(snapshot.total_value_ars),
                positions_count=len(snapshot.positions),
                confidence=snapshot.confidence_score,
                cash_ars=float(snapshot.cash_ars),
            )
            notifier.send_raw(
                f"ðŸ“Š Mercado EOD: {len(acciones)} acciones Â· {len(cedears)} CEDEARs guardados."
            )

            # AnÃ¡lisis tÃ©cnico â€” no crÃ­tico, fallo no afecta el resultado principal
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
                    logger.info("AnÃ¡lisis tÃ©cnico: %d seÃ±ales enviadas", len(signals))
                except Exception as e:
                    logger.warning("AnÃ¡lisis tÃ©cnico fallÃ³ (no crÃ­tico): %s", e)

        except Exception as e:
            logger.error("run_full [%s] fallÃ³: %s", run_type, e, exc_info=True)
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
            "run_opening_portfolio_report [%s]: scraper ocupado â€” abortando",
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

            latest_prices = await db.get_latest_market_prices()
            live_portfolio = build_live_portfolio(snapshot_payload, latest_prices)
            await cache_live_portfolio(
                live_portfolio,
                ttl_seconds=PORTFOLIO_CACHE_TTL_SECONDS,
            )

            notifier.send_raw(render_opening_portfolio_report(live_portfolio))
            result.update(
                success=True,
                positions=len(snapshot.positions),
                acciones=len(acciones),
                cedears=len(cedears),
                price_coverage=live_portfolio.get("price_coverage_count", 0),
            )
            logger.info(
                "opening portfolio ok: %d posiciones Â· cobertura %s/%s Â· %dA + %dC",
                len(snapshot.positions),
                live_portfolio.get("price_coverage_count", 0),
                live_portfolio.get("positions_count", 0),
                len(acciones),
                len(cedears),
            )
        except Exception as e:
            logger.error("run_opening_portfolio_report [%s] fallÃ³: %s", run_type, e, exc_info=True)
            notifier.notify_critical_error(run_type, str(e))
            result["error"] = str(e)
        finally:
            await _redis_delete(SCRAPER_LOCK_KEY)
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
        logger.error("update_outcomes fallÃ³: %s", e, exc_info=True)
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
        if len(frame) >= 60:
            frames[ticker] = frame
    return frames


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


# â”€â”€â”€ Daily analysis health checks â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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


# â”€â”€â”€ Risk alert â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@dataclass
class RiskAlert:
    ticker: str
    level: str          # WARNING | CRITICAL | STOP_NEAR | STOP_TRIGGERED
    current_price: float
    entry_price: float
    pnl_pct: float
    stop_loss_price: float | None = None
    target_price: float | None = None


# â”€â”€â”€ Intraday Manager â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

class IntradayManager:
    """
    Dos loops independientes durante horario de mercado:

    1. _scraper_loop:
       Un Ãºnico loop de scraping â€” un login por iteraciÃ³n, sin competencia.
       - Mercado (ACCIONES + CEDEARS) cada MARKET_POLL_SECONDS (90s).
       - Portfolio cada PORTFOLIO_REFRESH_SECONDS (~10min), dentro del mismo login.
       Esto resuelve el problema de dos scrapers peleÃ¡ndose y logins dobles.

    2. _risk_guard_loop:
       Solo lee DB. Sin Playwright. Sin scraper.
       Emite alertas por Telegram segÃºn umbrales de PNL / stop loss.
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
        logger.info("IntradayManager: loops intradÃ­a iniciados")
        try:
            self.notifier.send_raw(
                "ðŸŸ¢ <b>Monitoreo intradÃ­a iniciado</b>\n"
                "Mercado cada 90s Â· Portfolio cada 5min Â· Live cache cada 60s Â· Risk guard cada 60s."
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
        logger.info("IntradayManager: loops intradÃ­a detenidos")
        try:
            self.notifier.send_raw("ðŸ”´ <b>Monitoreo intradÃ­a detenido</b>")
        except Exception as e:
            logger.warning("No se pudo notificar fin de monitoreo: %s", e)

    # â”€â”€ Loop Ãºnico de scraping â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    async def _scraper_loop(self) -> None:
        """
        Un Ãºnico loop de scraping.

        - En rueda (dÃ­as hÃ¡biles 10:30-17:00 ART):
        * scrapea mercado cada MARKET_POLL_SECONDS
        * scrapea portfolio cada PORTFOLIO_REFRESH_SECONDS

        - Fuera de rueda / fines de semana:
        * NO scrapea mercado
        * SÃ scrapea portfolio cada PORTFOLIO_OFFHOURS_REFRESH_SECONDS
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

                            # â”€â”€ Mercado solo en rueda â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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
                                    logger.info("Scraper loop: mercado sin filas en esta iteraciÃ³n")

                            # â”€â”€ Portfolio siempre permitido â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
                            if should_refresh_portfolio:
                                snapshot = await scraper.scrape_portfolio()
                                await db.save_snapshot(snapshot)
                                await _cache_snapshot(snapshot)
                                last_portfolio_ts = time.monotonic()
                                logger.info(
                                    "Scraper loop: portfolio guardado Â· %d posiciones Â· conf %.2f",
                                    len(snapshot.positions),
                                    snapshot.confidence_score,
                                )

                            if should_refresh_fills:
                                fills = await scraper.scrape_broker_fills()
                                movements = await scraper.scrape_portfolio_movements()
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
                    "Scraper loop error (reintentarÃ¡ luego): %s",
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

    # â”€â”€ Risk guard (solo DB) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    async def _risk_guard_loop(self) -> None:
        """
        Lee DB. Calcula PNL contra entries de decision_log. EnvÃ­a alertas.
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
                    # Silenciar alertas no crÃ­ticas si el bot estÃ¡ procesando algo manual
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
                    logger.info("Risk guard: todo dentro de parÃ¡metros")

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("Risk guard error (reintentarÃ¡ en %ds): %s", RISK_POLL_SECONDS, e, exc_info=True)
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

                latest_prices = await db.get_latest_market_prices()
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
                      AND COALESCE(status, 'APPROVED') IN ('APPROVED', 'EXECUTED')
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

            # Derivar stop_price desde stop_pct si no viene explÃ­cito
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
            "STOP_TRIGGERED": "ðŸš¨",
            "STOP_NEAR":      "âš ï¸",
            "CRITICAL":       "ðŸ”´",
            "WARNING":        "ðŸŸ¡",
        }
        icon = icons.get(alert.level, "âš ï¸")

        msg = (
            f"{icon} <b>{alert.level} â€” {alert.ticker}</b>\n"
            f"Precio: <b>${alert.current_price:,.2f}</b> Â· Entrada: <b>${alert.entry_price:,.2f}</b>"
            f"{stop_txt}{target_txt}\n"
            f"PNL: <b>{pnl:+.2f}%</b>"
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


# â”€â”€â”€ Wrappers de start/stop para APScheduler â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

async def start_intraday_loops() -> None:
    global _intraday_manager
    if _intraday_manager is None:
        _intraday_manager = IntradayManager()
    await _intraday_manager.start()


async def stop_intraday_loops() -> None:
    global _intraday_manager
    if _intraday_manager is not None:
        await _intraday_manager.stop()


# â”€â”€â”€ Scheduler principal â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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
        run_full,
        _business_day_cron(hour=17, minute=0),
        args=["17:00_FULL"],
        id="portfolio_eod",
        name="Full 17:00 ART",
        misfire_grace_time=300,
        replace_existing=True,
    )
    scheduler.add_job(
        stop_intraday_loops,
        _business_day_cron(hour=17, minute=1),
        id="intraday_stop",
        name="Intraday stop 17:01 ART",
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
        "Scheduler activo: 10:31 apertura portfolio + intraday on; 17:00 full; 17:01 intraday off; 17:05 candles; 17:10 verify; 17:12 analysis; 21:30 outcomes"
    )

    # Si arrancamos durante rueda, iniciar loops de inmediato
    now = _now_art()
    if _is_market_window(now):
        logger.info(
            "Scheduler arrancÃ³ dentro de rueda (%s ART) â€” iniciando loops intradÃ­a",
            now.strftime("%H:%M:%S"),
        )
        await start_intraday_loops()

    stop_event = asyncio.Event()

    def _handle_signal() -> None:
        logger.info("SeÃ±al recibida â€” iniciando apagado limpio...")
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


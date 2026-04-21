from __future__ import annotations

import asyncio
import contextlib
import signal

try:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.cron import CronTrigger
    HAS_APSCHEDULER = True
except ImportError:
    HAS_APSCHEDULER = False

from src.core.config import get_config
from src.core.logger import get_logger
from src.collector.cocos_scraper import CocosCapitalScraper
from src.collector.db import PortfolioDatabase
from src.collector.notifier import TelegramNotifier
from src.collector.db import PortfolioDatabase as _PDB
from src.services.market_listener import MarketListener
from src.services.risk_guard import RiskGuard

logger = get_logger(__name__)

TIMEZONE = "America/Argentina/Buenos_Aires"


# ══════════════════════════════════════════════════════════════════════════════
# JOBS EXISTENTES
# ══════════════════════════════════════════════════════════════════════════════

async def run_scrape(run_type: str = "SCHEDULED") -> dict:
    """Job de scrape de portfolio: login → scrape → DB → Telegram."""
    cfg = get_config()
    notifier = TelegramNotifier(cfg.scraper.telegram_bot_token, cfg.scraper.telegram_chat_id)
    db = PortfolioDatabase(cfg.database.url)

    logger.info(f"=== Iniciando run [{run_type}] ===")
    notifier.notify_run_start(run_type)

    result: dict = {"success": False}

    try:
        await db.connect()

        async with CocosCapitalScraper(cfg.scraper) as scraper:
            try:
                await scraper.login()
                notifier.notify_login_ok()
            except Exception as e:
                notifier.notify_login_error(str(e))
                raise

            snapshot = await scraper.scrape_portfolio()
            sid = await db.save_snapshot(snapshot)

            result.update(success=True, snapshot_id=str(sid), positions=len(snapshot.positions))

            notifier.notify_scrape_complete(
                total_ars=float(snapshot.total_value_ars),
                positions_count=len(snapshot.positions),
                confidence=snapshot.confidence_score,
                cash_ars=float(snapshot.cash_ars),
            )

            if snapshot.positions:
                notifier.send_snapshot_json(snapshot.to_dict())

            logger.info(
                f"Run ok: {len(snapshot.positions)} posiciones, "
                f"confianza={snapshot.confidence_score:.2f}, "
                f"total={snapshot.total_value_ars:,.0f} ARS"
            )

    except Exception as e:
        logger.error(f"Run fallido [{run_type}]: {e}", exc_info=True)
        notifier.notify_critical_error(run_type, str(e))
        result["error"] = str(e)
    finally:
        await db.close()

    return result


async def run_full(run_type: str = "FULL") -> dict:
    """Job extendido 17:00: portfolio + precios de mercado."""
    cfg = get_config()
    notifier = TelegramNotifier(cfg.scraper.telegram_bot_token, cfg.scraper.telegram_chat_id)
    db = PortfolioDatabase(cfg.database.url)

    logger.info(f"=== Iniciando run FULL [{run_type}] ===")
    notifier.notify_run_start(run_type)
    result: dict = {"success": False}

    try:
        await db.connect()

        async with CocosCapitalScraper(cfg.scraper) as scraper:
            await scraper.login()
            notifier.notify_login_ok()

            snapshot = await scraper.scrape_portfolio()
            await db.save_snapshot(snapshot)

            acciones = await scraper.scrape_market("ACCIONES")
            cedears = await scraper.scrape_market("CEDEARS")
            await db.save_market_prices((acciones or []) + (cedears or []))

            result.update(
                success=True,
                positions=len(snapshot.positions),
                acciones=len(acciones or []),
                cedears=len(cedears or []),
            )

            notifier.notify_scrape_complete(
                total_ars=float(snapshot.total_value_ars),
                positions_count=len(snapshot.positions),
                confidence=snapshot.confidence_score,
                cash_ars=float(snapshot.cash_ars),
            )
            notifier.send_raw(
                f"Mercado: {len(acciones or [])} acciones, {len(cedears or [])} CEDEARs guardados"
            )

        if snapshot and snapshot.positions:
            try:
                from src.analysis.technical import analyze_portfolio, build_telegram_report

                tickers = [p.ticker for p in snapshot.positions]
                logger.info(f"Ejecutando analisis tecnico: {tickers}")
                signals = analyze_portfolio(tickers, period="3mo")
                report = build_telegram_report(signals, float(snapshot.total_value_ars))
                notifier.send_raw(report, parse_mode=None)
                logger.info(f"Analisis tecnico: {len(signals)} señales enviadas")
            except Exception as e:
                logger.warning(f"Analisis tecnico falló (no crítico): {e}")

    except Exception as e:
        logger.error(f"Run FULL fallido: {e}", exc_info=True)
        notifier.notify_critical_error(run_type, str(e))
        result["error"] = str(e)
    finally:
        await db.close()

    return result


async def run_update_outcomes() -> None:
    """Job diario: rellena outcome_5d/10d/20d y was_correct en decision_log."""
    cfg = get_config()
    db = _PDB(cfg.database.url)
    try:
        await db.connect()
        updated = await db.update_outcomes(lookback_days=60)
        logger.info(f"update_outcomes: {updated} decisiones actualizadas")
    except Exception as e:
        logger.error(f"update_outcomes falló: {e}", exc_info=True)
    finally:
        with contextlib.suppress(Exception):
            await db.close()


# ══════════════════════════════════════════════════════════════════════════════
# TASKS INTRADÍA
# ══════════════════════════════════════════════════════════════════════════════

async def _run_market_listener_forever() -> None:
    """
    Listener intradiario:
    - mantiene sesión
    - scrapea mercado durante rueda
    - duerme fuera de horario
    """
    listener = MarketListener(
        poll_seconds=90,
        retry_seconds=30,
        market_open_hour=10,
        market_open_minute=30,
        market_close_hour=17,
        market_close_minute=0,
    )
    await listener.run_forever()


async def _run_risk_guard_forever() -> None:
    """
    Guardia intradiaria:
    - lee snapshot + decision_log + market_prices
    - alerta WARNING / CRITICAL / STOP_NEAR / STOP_TRIGGERED
    - duerme fuera de horario
    """
    guard = RiskGuard(
        poll_seconds=60,
        market_open_hour=10,
        market_open_minute=30,
        market_close_hour=17,
        market_close_minute=0,
        warning_pct=-0.04,
        critical_pct=-0.06,
        stop_near_pct=0.02,
        dedup_minutes=30,
    )
    await guard.run_forever()


# ══════════════════════════════════════════════════════════════════════════════
# SCHEDULER PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

async def _scheduler_main():
    """
    Orquestador único del sistema.

    Mantiene:
      - APScheduler para jobs puntuales
      - tasks persistentes intradía para market listener y risk guard

    Modelo:
      - 10:30 → portfolio
      - intradía → market listener siempre vivo durante rueda
      - intradía → risk guard siempre vivo durante rueda
      - 17:00 → portfolio + mercado completo
      - 21:30 → update outcomes
    """
    if not HAS_APSCHEDULER:
        raise ImportError("apscheduler no instalado: pip install apscheduler")

    scheduler = AsyncIOScheduler(timezone=TIMEZONE)

    # 10:30 ART — snapshot de portfolio
    scheduler.add_job(
        run_scrape,
        CronTrigger(hour=10, minute=30, timezone=TIMEZONE),
        args=["10:30_PORTFOLIO"],
        id="portfolio_morning",
        name="Portfolio 10:30 ART",
        misfire_grace_time=300,
        replace_existing=True,
        max_instances=1,
    )

    # 17:00 ART — snapshot + mercado + análisis técnico
    scheduler.add_job(
        run_full,
        CronTrigger(hour=17, minute=0, timezone=TIMEZONE),
        args=["17:00_FULL"],
        id="portfolio_eod",
        name="Portfolio + Mercado 17:00 ART",
        misfire_grace_time=300,
        replace_existing=True,
        max_instances=1,
    )

    # 21:30 ART — update outcomes
    scheduler.add_job(
        run_update_outcomes,
        CronTrigger(hour=21, minute=30, timezone=TIMEZONE),
        id="update_outcomes_daily",
        name="Update Outcomes 21:30 ART",
        misfire_grace_time=600,
        replace_existing=True,
        max_instances=1,
    )

    scheduler.start()
    logger.info(
        "Scheduler activo — jobs: 10:30 (portfolio) | 17:00 (portfolio + mercado) | "
        "21:30 (update outcomes) ART"
    )

    # Tasks intradía persistentes
    market_task = asyncio.create_task(_run_market_listener_forever(), name="market_listener")
    risk_task = asyncio.create_task(_run_risk_guard_forever(), name="risk_guard")

    logger.info("Tasks intradía activadas: market_listener + risk_guard")

    stop_event = asyncio.Event()

    def _handle_signal():
        logger.info("Señal recibida — apagando scheduler y tasks...")
        with contextlib.suppress(Exception):
            scheduler.shutdown(wait=False)
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except (NotImplementedError, OSError):
            pass

    await stop_event.wait()

    for task in (market_task, risk_task):
        task.cancel()

    for task in (market_task, risk_task):
        with contextlib.suppress(asyncio.CancelledError):
            await task

    logger.info("Scheduler apagado correctamente")


def start_scheduler():
    """Punto de entrada público."""
    try:
        asyncio.run(_scheduler_main())
    except KeyboardInterrupt:
        logger.info("Scheduler detenido por usuario")


if __name__ == "__main__":
    start_scheduler()
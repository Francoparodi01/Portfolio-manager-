"""
src/scheduler/runner.py
Scheduler principal — CORREGIDO para Python 3.12.

Bug original: APScheduler 3.x llama asyncio.get_running_loop() en .start()
pero no hay event loop activo todavía cuando se invoca desde __main__.

Fix: envolver todo en asyncio.run() para que el loop exista antes
de que APScheduler intente agarrarlo.
"""
from __future__ import annotations

import asyncio
import logging
import signal

try:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.cron import CronTrigger
    HAS_APSCHEDULER = True
except ImportError:
    HAS_APSCHEDULER = False

from src.collector import notifier
from src.core.config import get_config
from src.core.logger import get_logger
from src.collector.cocos_scraper import CocosCapitalScraper
from src.collector.db import PortfolioDatabase
from src.collector.notifier import TelegramNotifier
from src.collector.db import PortfolioDatabase as _PDB

logger = get_logger(__name__)

TIMEZONE = "America/Argentina/Buenos_Aires"


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
            await db.save_market_prices(acciones + cedears)

            result.update(
                success=True,
                positions=len(snapshot.positions),
                acciones=len(acciones),
                cedears=len(cedears),
            )

            notifier.notify_scrape_complete(
                total_ars=float(snapshot.total_value_ars),
                positions_count=len(snapshot.positions),
                confidence=snapshot.confidence_score,
                cash_ars=float(snapshot.cash_ars),
            )
            notifier.send_raw(
                f"Mercado: {len(acciones)} acciones, {len(cedears)} CEDEARs guardados"
            )
        # Analisis tecnico post-scrape (no critico si falla)
        if snapshot and snapshot.positions:
            try:
                from src.analysis.technical import analyze_portfolio, build_telegram_report
                tickers = [p.ticker for p in snapshot.positions]
                logger.info(f"Ejecutando analisis tecnico: {tickers}")
                signals = analyze_portfolio(tickers, period="3mo")
                report  = build_telegram_report(signals, float(snapshot.total_value_ars))
                notifier.send_raw(report, parse_mode=None)  # texto plano, sin parsing                logger.info(f"Analisis tecnico: {len(signals)} senales enviadas")
            except Exception as e:
                logger.warning(f"Analisis tecnico fallo (no critico): {e}")


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
    db  = _PDB(cfg.database.url)
    try:
        await db.connect()
        updated = await db.update_outcomes(lookback_days=60)
        logger.info(f"update_outcomes: {updated} decisiones actualizadas")
    except Exception as e:
        logger.error(f"update_outcomes falló: {e}", exc_info=True)
    finally:
        try:
            await db.close()
        except Exception:
            pass

async def _scheduler_main():
    """
    Corre el scheduler DENTRO de un event loop activo.
    APScheduler 3.x requiere que asyncio.get_running_loop() funcione
    en el momento de llamar .start() — por eso se envuelve en asyncio.run().
    """
    if not HAS_APSCHEDULER:
        raise ImportError("apscheduler no instalado: pip install apscheduler")

    scheduler = AsyncIOScheduler(timezone=TIMEZONE)

    # 10:30 ART — solo portfolio
    scheduler.add_job(
        run_scrape,
        CronTrigger(hour=10, minute=30, timezone=TIMEZONE),
        args=["10:30_PORTFOLIO"],
        id="portfolio_morning",
        name="Portfolio 10:30 ART",
        misfire_grace_time=300,
        replace_existing=True,
    )

    # 17:00 ART — portfolio + mercado completo
    scheduler.add_job(
        run_full,
        CronTrigger(hour=17, minute=0, timezone=TIMEZONE),
        args=["17:00_FULL"],
        id="portfolio_eod",
        name="Portfolio + Mercado 17:00 ART",
        misfire_grace_time=300,
        replace_existing=True,
    )

    # 21:30 ART — actualizar outcomes de decisiones pasadas
    scheduler.add_job(
        run_update_outcomes,
        CronTrigger(hour=21, minute=30, timezone=TIMEZONE),
        id="update_outcomes_daily",
        name="Update Outcomes 21:30 ART",
        misfire_grace_time=600,
        replace_existing=True,
    )

    scheduler.start()
    logger.info("Scheduler activo — jobs: 10:30 (portfolio) | 17:00 (portfolio + mercado) | 21:30 (update outcomes) ART")

    # Esperar señal de shutdown
    stop_event = asyncio.Event()

    def _handle_signal():
        logger.info("Señal recibida — apagando scheduler...")
        scheduler.shutdown(wait=False)
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except (NotImplementedError, OSError):
            # Windows no soporta add_signal_handler — no es crítico
            pass

    await stop_event.wait()


def start_scheduler():
    """Punto de entrada público. Envuelve el scheduler en asyncio.run()."""
    try:
        asyncio.run(_scheduler_main())
    except KeyboardInterrupt:
        logger.info("Scheduler detenido por usuario")


if __name__ == "__main__":
    start_scheduler()
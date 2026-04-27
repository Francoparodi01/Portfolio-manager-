"""
src/scheduler/runner.py

Scheduler principal + loops intradía coordinados con Redis.

Qué hace:
- 10:30 ART -> scrape portfolio
- 10:31 ART -> arranca listener intradía de mercado
- 10:31 ART -> arranca guard intradía de riesgo
- 17:00 ART -> portfolio + mercado completo
- 17:01 ART -> apaga loops intradía
- 21:30 ART -> update_outcomes

Coordinación con bot:
- lock global de scraper vía Redis
- heartbeats de mercado y riesgo
- pausa temporal del monitor
- bot_busy para silenciar alertas menores mientras se ejecutan acciones manuales
"""

from __future__ import annotations

import asyncio
import signal
from dataclasses import dataclass
from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo

try:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.cron import CronTrigger
    HAS_APSCHEDULER = True
except ImportError:
    HAS_APSCHEDULER = False

from src.core.config import get_config
from src.core.logger import get_logger
from src.core.redis_client import client as redis_client
from src.collector.cocos_scraper import CocosCapitalScraper
from src.collector.db import PortfolioDatabase
from src.collector.notifier import TelegramNotifier
from src.collector.db import PortfolioDatabase as _PDB

logger = get_logger(__name__)

TIMEZONE = "America/Argentina/Buenos_Aires"
ART_TZ = ZoneInfo(TIMEZONE)
UTC = timezone.utc

MARKET_OPEN = time(hour=10, minute=30)
MARKET_CLOSE = time(hour=17, minute=0)

MARKET_POLL_SECONDS = 90
RISK_POLL_SECONDS = 60

WARNING_PCT = -0.04
CRITICAL_PCT = -0.06
STOP_NEAR_PCT = 0.02

# Redis keys
SCRAPER_LOCK_KEY = "cocos:lock:scraper"
MONITOR_STATE_KEY = "cocos:monitor:state"
MONITOR_PAUSED_UNTIL_KEY = "cocos:monitor:paused_until"
MARKET_HEARTBEAT_KEY = "cocos:monitor:market:last_tick"
RISK_HEARTBEAT_KEY = "cocos:monitor:risk:last_check"
BOT_BUSY_KEY = "cocos:bot:busy"

_intraday_manager: "IntradayManager | None" = None


@dataclass
class RiskAlert:
    ticker: str
    level: str
    current_price: float
    entry_price: float
    pnl_pct: float
    stop_loss_price: float | None = None
    target_price: float | None = None


def _now_art() -> datetime:
    return datetime.now(tz=ART_TZ)


def _is_market_window(now: datetime | None = None) -> bool:
    now = now or _now_art()
    t = now.timetz().replace(tzinfo=None)
    return MARKET_OPEN <= t < MARKET_CLOSE


def _safe_float(value, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


async def _get_pool(db: PortfolioDatabase):
    if hasattr(db, "get_pool"):
        pool = await db.get_pool()
        if pool is not None:
            return pool
    pool = getattr(db, "_db_pool", None) or getattr(db, "_pool", None)
    return pool


async def _heartbeat(key: str) -> None:
    try:
        await redis_client.set(key, str(int(datetime.now(tz=UTC).timestamp())), ex=3600)
    except Exception as e:
        logger.warning("Heartbeat redis falló [%s]: %s", key, e)


async def _set_monitor_state(state: str) -> None:
    try:
        await redis_client.set(MONITOR_STATE_KEY, state, ex=3600)
    except Exception as e:
        logger.warning("Monitor state redis falló: %s", e)


async def _is_monitor_paused() -> bool:
    try:
        raw = await redis_client.get(MONITOR_PAUSED_UNTIL_KEY)
        if not raw:
            return False
        paused_until = int(raw.decode() if isinstance(raw, bytes) else raw)
        now_ts = int(datetime.now(tz=UTC).timestamp())
        return now_ts < paused_until
    except Exception as e:
        logger.warning("Monitor pause check redis falló: %s", e)
        return False


async def _acquire_scraper_lock(owner: str, ttl: int = 300) -> bool:
    try:
        ok = await redis_client.set(SCRAPER_LOCK_KEY, owner, ex=ttl, nx=True)
        return bool(ok)
    except Exception as e:
        logger.warning("Acquire scraper lock falló: %s", e)
        return False


async def _release_scraper_lock(owner: str) -> None:
    try:
        current = await redis_client.get(SCRAPER_LOCK_KEY)
        if not current:
            return
        current_str = current.decode() if isinstance(current, bytes) else str(current)
        if current_str == owner:
            await redis_client.delete(SCRAPER_LOCK_KEY)
    except Exception as e:
        logger.warning("Release scraper lock falló: %s", e)


async def _is_bot_busy() -> bool:
    try:
        value = await redis_client.get(BOT_BUSY_KEY)
        return bool(value)
    except Exception:
        return False


async def run_scrape(run_type: str = "SCHEDULED") -> dict:
    cfg = get_config()
    notifier = TelegramNotifier(cfg.scraper.telegram_bot_token, cfg.scraper.telegram_chat_id)
    db = PortfolioDatabase(cfg.database.url)

    logger.info("=== Iniciando run [%s] ===", run_type)
    notifier.notify_run_start(run_type)

    result: dict = {"success": False}
    owner = f"run_scrape:{int(datetime.now(tz=UTC).timestamp())}"

    try:
        got_lock = await _acquire_scraper_lock(owner, ttl=300)
        if not got_lock:
            logger.warning("run_scrape: lock ocupado, abortando run [%s]", run_type)
            result["error"] = "scraper lock ocupado"
            return result

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
                "Run ok: %d posiciones, confianza=%.2f, total=%s ARS",
                len(snapshot.positions),
                snapshot.confidence_score,
                f"{snapshot.total_value_ars:,.0f}",
            )

    except Exception as e:
        logger.error("Run fallido [%s]: %s", run_type, e, exc_info=True)
        notifier.notify_critical_error(run_type, str(e))
        result["error"] = str(e)
    finally:
        await _release_scraper_lock(owner)
        await db.close()

    return result


async def run_full(run_type: str = "FULL") -> dict:
    cfg = get_config()
    notifier = TelegramNotifier(cfg.scraper.telegram_bot_token, cfg.scraper.telegram_chat_id)
    db = PortfolioDatabase(cfg.database.url)

    logger.info("=== Iniciando run FULL [%s] ===", run_type)
    notifier.notify_run_start(run_type)
    result: dict = {"success": False}
    owner = f"run_full:{int(datetime.now(tz=UTC).timestamp())}"

    try:
        got_lock = await _acquire_scraper_lock(owner, ttl=600)
        if not got_lock:
            logger.warning("run_full: lock ocupado, abortando run [%s]", run_type)
            result["error"] = "scraper lock ocupado"
            return result

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

        if snapshot and snapshot.positions:
            try:
                from src.analysis.technical import analyze_portfolio, build_telegram_report

                tickers = [p.ticker for p in snapshot.positions]
                logger.info("Ejecutando analisis tecnico: %s", tickers)
                signals = analyze_portfolio(tickers, period="3mo")
                report = build_telegram_report(signals, float(snapshot.total_value_ars))
                notifier.send_raw(report)
                logger.info("Analisis tecnico: %d señales enviadas", len(signals))
            except Exception as e:
                logger.warning("Analisis tecnico falló (no crítico): %s", e)

    except Exception as e:
        logger.error("Run FULL fallido: %s", e, exc_info=True)
        notifier.notify_critical_error(run_type, str(e))
        result["error"] = str(e)
    finally:
        await _release_scraper_lock(owner)
        await db.close()

    return result


async def run_update_outcomes() -> None:
    cfg = get_config()
    db = _PDB(cfg.database.url)
    try:
        await db.connect()
        updated = await db.update_outcomes(lookback_days=60)
        logger.info("update_outcomes: %s decisiones actualizadas", updated)
    except Exception as e:
        logger.error("update_outcomes falló: %s", e, exc_info=True)
    finally:
        try:
            await db.close()
        except Exception:
            pass


class IntradayManager:
    def __init__(self):
        self.cfg = get_config()
        self.notifier = TelegramNotifier(
            self.cfg.scraper.telegram_bot_token,
            self.cfg.scraper.telegram_chat_id,
        )
        self._market_task: asyncio.Task | None = None
        self._risk_task: asyncio.Task | None = None
        self._running = False
        self._last_alert_keys: dict[str, datetime] = {}

    @property
    def running(self) -> bool:
        return self._running

    async def start(self) -> None:
        if self._running:
            logger.info("IntradayManager: loops ya activos")
            return

        self._running = True
        self._market_task = asyncio.create_task(self._market_listener_loop(), name="market_listener_loop")
        self._risk_task = asyncio.create_task(self._risk_guard_loop(), name="risk_guard_loop")
        await _set_monitor_state("running")
        logger.info("IntradayManager: loops intradía iniciados")

        try:
            self.notifier.send_raw(
                "🟢 <b>Monitoreo intradía iniciado</b>\n"
                "Escuchando mercado y riesgo entre 10:30 y 17:00 ART."
            )
        except Exception as e:
            logger.warning("No se pudo enviar aviso de inicio de monitoreo: %s", e)

    async def stop(self) -> None:
        self._running = False
        tasks = [t for t in [self._market_task, self._risk_task] if t is not None]
        for task in tasks:
            task.cancel()

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        self._market_task = None
        self._risk_task = None
        await _set_monitor_state("stopped")
        logger.info("IntradayManager: loops intradía detenidos")

        try:
            self.notifier.send_raw(
                "🔴 <b>Monitoreo intradía detenido</b>\n"
                "Se frenó el listener de mercado y el guard de riesgo."
            )
        except Exception as e:
            logger.warning("No se pudo enviar aviso de fin de monitoreo: %s", e)

    async def _market_listener_loop(self) -> None:
        while self._running:
            now = _now_art()
            if not _is_market_window(now):
                await asyncio.sleep(30)
                continue

            if await _is_monitor_paused():
                logger.info("Market listener: monitor pausado")
                await asyncio.sleep(30)
                continue

            owner = f"market_listener:{int(datetime.now(tz=UTC).timestamp())}"
            got_lock = await _acquire_scraper_lock(owner, ttl=240)
            if not got_lock:
                logger.info("Market listener: scraper lock ocupado, salteando iteración")
                await asyncio.sleep(20)
                continue

            db = PortfolioDatabase(self.cfg.database.url)
            try:
                await db.connect()
                async with CocosCapitalScraper(self.cfg.scraper) as scraper:
                    await scraper.login()
                    acciones = await scraper.scrape_market("ACCIONES")
                    cedears = await scraper.scrape_market("CEDEARS")
                    total = len(acciones) + len(cedears)

                    if total > 0:
                        await db.save_market_prices(acciones + cedears)
                        await _heartbeat(MARKET_HEARTBEAT_KEY)
                        logger.info(
                            "Market listener: guardados %d precios (%d acciones, %d cedears)",
                            total, len(acciones), len(cedears)
                        )
                    else:
                        logger.info("Market listener: sin filas de mercado en esta iteración")

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("Market listener loop falló (no crítico): %s", e, exc_info=True)
            finally:
                await _release_scraper_lock(owner)
                try:
                    await db.close()
                except Exception:
                    pass

            await asyncio.sleep(MARKET_POLL_SECONDS)

    async def _risk_guard_loop(self) -> None:
        while self._running:
            now = _now_art()
            if not _is_market_window(now):
                await asyncio.sleep(30)
                continue

            if await _is_monitor_paused():
                logger.info("Risk guard: monitor pausado")
                await asyncio.sleep(30)
                continue

            db = PortfolioDatabase(self.cfg.database.url)
            try:
                await db.connect()
                pool = await _get_pool(db)
                if pool is None:
                    logger.warning("Risk guard: no se pudo obtener pool DB")
                    await asyncio.sleep(RISK_POLL_SECONDS)
                    continue

                bot_busy = await _is_bot_busy()
                alerts = await self._compute_risk_alerts(pool)

                for alert in alerts:
                    if bot_busy and alert.level not in ("CRITICAL", "STOP_TRIGGERED"):
                        logger.info(
                            "Risk guard: bot busy, alerta no crítica silenciada [%s %s]",
                            alert.level, alert.ticker
                        )
                        continue

                    if self._should_send_alert(alert):
                        self._send_alert(alert)
                        self._last_alert_keys[f"{alert.ticker}:{alert.level}"] = datetime.now(tz=UTC)

                await _heartbeat(RISK_HEARTBEAT_KEY)

                if not alerts:
                    logger.info("Risk guard: sin alertas")

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("Risk guard loop falló (no crítico): %s", e, exc_info=True)
            finally:
                try:
                    await db.close()
                except Exception:
                    pass

            await asyncio.sleep(RISK_POLL_SECONDS)

    async def _compute_risk_alerts(self, pool) -> list[RiskAlert]:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                WITH latest_prices AS (
                    SELECT DISTINCT ON (ticker)
                        ticker,
                        last_price,
                        ts
                    FROM market_prices
                    WHERE last_price IS NOT NULL
                    ORDER BY ticker, ts DESC
                ),
                latest_buys AS (
                    SELECT DISTINCT ON (ticker)
                        id,
                        ticker,
                        price_at_decision,
                        stop_loss_pct,
                        stop_loss_price,
                        target_price,
                        decided_at,
                        decision
                    FROM decision_log
                    WHERE decision = 'BUY'
                      AND price_at_decision IS NOT NULL
                    ORDER BY ticker, decided_at DESC
                )
                SELECT
                    b.id,
                    b.ticker,
                    b.price_at_decision,
                    b.stop_loss_pct,
                    b.stop_loss_price,
                    b.target_price,
                    p.last_price
                FROM latest_buys b
                JOIN latest_prices p
                  ON p.ticker = b.ticker
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

            if entry is None or current is None:
                continue

            if stop_price is None and stop_pct is not None:
                stop_pct_dec = stop_pct / 100.0 if abs(stop_pct) > 1 else stop_pct
                if stop_pct_dec > 0:
                    stop_pct_dec = -abs(stop_pct_dec)
                stop_price = entry * (1.0 + stop_pct_dec)

            pnl_pct = (current / entry) - 1.0

            if stop_price is not None and current <= stop_price:
                alerts.append(RiskAlert(
                    ticker=ticker,
                    level="STOP_TRIGGERED",
                    current_price=current,
                    entry_price=entry,
                    pnl_pct=pnl_pct,
                    stop_loss_price=stop_price,
                    target_price=target_price,
                ))
                continue

            if stop_price is not None:
                dist_to_stop = (current - stop_price) / stop_price
                if 0 < dist_to_stop <= STOP_NEAR_PCT:
                    alerts.append(RiskAlert(
                        ticker=ticker,
                        level="STOP_NEAR",
                        current_price=current,
                        entry_price=entry,
                        pnl_pct=pnl_pct,
                        stop_loss_price=stop_price,
                        target_price=target_price,
                    ))
                    continue

            if pnl_pct <= CRITICAL_PCT:
                alerts.append(RiskAlert(
                    ticker=ticker,
                    level="CRITICAL",
                    current_price=current,
                    entry_price=entry,
                    pnl_pct=pnl_pct,
                    stop_loss_price=stop_price,
                    target_price=target_price,
                ))
            elif pnl_pct <= WARNING_PCT:
                alerts.append(RiskAlert(
                    ticker=ticker,
                    level="WARNING",
                    current_price=current,
                    entry_price=entry,
                    pnl_pct=pnl_pct,
                    stop_loss_price=stop_price,
                    target_price=target_price,
                ))

        return alerts

    def _should_send_alert(self, alert: RiskAlert) -> bool:
        key = f"{alert.ticker}:{alert.level}"
        last = self._last_alert_keys.get(key)
        if last is None:
            return True

        elapsed = (datetime.now(tz=UTC) - last).total_seconds()
        if alert.level == "STOP_TRIGGERED":
            return elapsed >= 60
        return elapsed >= 30 * 60

    def _send_alert(self, alert: RiskAlert) -> None:
        pnl = alert.pnl_pct * 100.0
        stop_txt = (
            f"\nStop: <b>${alert.stop_loss_price:,.2f}</b>"
            if alert.stop_loss_price is not None else ""
        )
        target_txt = (
            f"\nTarget: <b>${alert.target_price:,.2f}</b>"
            if alert.target_price is not None else ""
        )

        msg = (
            f"⚠️ <b>{alert.level} — {alert.ticker}</b>\n"
            f"Precio actual: <b>${alert.current_price:,.2f}</b>\n"
            f"Entrada: <b>${alert.entry_price:,.2f}</b>{stop_txt}{target_txt}\n"
            f"PNL: <b>{pnl:+.2f}%</b>"
        )
        try:
            self.notifier.send_raw(msg)
            logger.warning("Risk alert enviada: %s %s", alert.level, alert.ticker)
        except Exception as e:
            logger.warning("No se pudo enviar alerta Telegram: %s", e)


async def start_intraday_loops() -> None:
    global _intraday_manager
    if _intraday_manager is None:
        _intraday_manager = IntradayManager()
    await _intraday_manager.start()


async def stop_intraday_loops() -> None:
    global _intraday_manager
    if _intraday_manager is not None:
        await _intraday_manager.stop()


async def _scheduler_main():
    if not HAS_APSCHEDULER:
        raise ImportError("apscheduler no instalado: pip install apscheduler")

    scheduler = AsyncIOScheduler(timezone=TIMEZONE)

    scheduler.add_job(
        run_scrape,
        CronTrigger(hour=10, minute=30, timezone=TIMEZONE),
        args=["10:30_PORTFOLIO"],
        id="portfolio_morning",
        name="Portfolio 10:30 ART",
        misfire_grace_time=300,
        replace_existing=True,
    )

    scheduler.add_job(
        start_intraday_loops,
        CronTrigger(hour=10, minute=31, timezone=TIMEZONE),
        id="intraday_start",
        name="Start intraday loops 10:31 ART",
        misfire_grace_time=300,
        replace_existing=True,
    )

    scheduler.add_job(
        run_full,
        CronTrigger(hour=17, minute=0, timezone=TIMEZONE),
        args=["17:00_FULL"],
        id="portfolio_eod",
        name="Portfolio + Mercado 17:00 ART",
        misfire_grace_time=300,
        replace_existing=True,
    )

    scheduler.add_job(
        stop_intraday_loops,
        CronTrigger(hour=17, minute=1, timezone=TIMEZONE),
        id="intraday_stop",
        name="Stop intraday loops 17:01 ART",
        misfire_grace_time=300,
        replace_existing=True,
    )

    scheduler.add_job(
        run_update_outcomes,
        CronTrigger(hour=21, minute=30, timezone=TIMEZONE),
        id="update_outcomes_daily",
        name="Update Outcomes 21:30 ART",
        misfire_grace_time=600,
        replace_existing=True,
    )

    scheduler.start()
    logger.info(
        "Scheduler activo — jobs: 10:30 scrape | 10:31 intraday on | 17:00 full | 17:01 intraday off | 21:30 update outcomes"
    )

    now = _now_art()
    if _is_market_window(now):
        logger.info(
            "Scheduler arrancó dentro de rueda (%s ART) — iniciando loops intradía inmediatamente",
            now.strftime("%H:%M:%S"),
        )
        await start_intraday_loops()

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
            pass

    await stop_event.wait()
    await stop_intraday_loops()


def start_scheduler():
    try:
        asyncio.run(_scheduler_main())
    except KeyboardInterrupt:
        logger.info("Scheduler detenido por usuario")


if __name__ == "__main__":
    start_scheduler()
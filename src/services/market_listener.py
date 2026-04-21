from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from src.core.config import get_config
from src.core.logger import get_logger
from src.collector.cocos_scraper import CocosCapitalScraper
from src.collector.db import PortfolioDatabase

logger = get_logger(__name__)

ART_TZ = ZoneInfo("America/Argentina/Buenos_Aires")


@dataclass
class ListenerStats:
    cycles_ok: int = 0
    cycles_error: int = 0
    last_success_at: datetime | None = None
    last_error_at: datetime | None = None
    last_error: str = ""


class MarketListener:
    """
    Listener intradiario de mercado para Cocos Copilot.

    Responsabilidades:
      - Mantenerse activo solo en horario de mercado argentino
      - Loguearse en Cocos
      - Scrapear ACCIONES y CEDEARS en loop
      - Persistir precios en market_prices
      - Reintentar si la sesión cae
    """

    def __init__(
        self,
        poll_seconds: int = 90,
        retry_seconds: int = 30,
        market_open_hour: int = 10,
        market_open_minute: int = 30,
        market_close_hour: int = 17,
        market_close_minute: int = 0,
    ) -> None:
        self.cfg = get_config()
        self.poll_seconds = poll_seconds
        self.retry_seconds = retry_seconds
        self.market_open = time(hour=market_open_hour, minute=market_open_minute)
        self.market_close = time(hour=market_close_hour, minute=market_close_minute)
        self.stats = ListenerStats()
        self._running = False
        self._db: PortfolioDatabase | None = None

    async def run_forever(self) -> None:
        errors = self.cfg.scraper.validate()
        if errors:
            raise RuntimeError(f"Configuración inválida: {errors}")

        self._running = True
        self._db = PortfolioDatabase(self.cfg.database.url)
        await self._db.connect()

        logger.info(
            "MarketListener iniciado | horario %s-%s ART | poll=%ss",
            self.market_open.strftime("%H:%M"),
            self.market_close.strftime("%H:%M"),
            self.poll_seconds,
        )

        try:
            while self._running:
                now = datetime.now(tz=ART_TZ)

                if not self._is_market_open(now):
                    sleep_for = self._seconds_until_next_open(now)
                    logger.info(
                        "Mercado cerrado | ahora=%s | próximo inicio en %ss",
                        now.strftime("%Y-%m-%d %H:%M:%S"),
                        sleep_for,
                    )
                    await asyncio.sleep(sleep_for)
                    continue

                await self._run_market_session()
        finally:
            if self._db:
                await self._db.close()
            logger.info("MarketListener detenido")

    def stop(self) -> None:
        self._running = False

    async def run_once(self) -> None:
        """
        Útil para testing manual del servicio.
        Hace una sola captura de ACCIONES + CEDEARS si el mercado está abierto.
        """
        errors = self.cfg.scraper.validate()
        if errors:
            raise RuntimeError(f"Configuración inválida: {errors}")

        now = datetime.now(tz=ART_TZ)
        if not self._is_market_open(now):
            logger.warning(
                "run_once abortado: mercado cerrado (%s ART)",
                now.strftime("%Y-%m-%d %H:%M:%S"),
            )
            return

        db = PortfolioDatabase(self.cfg.database.url)
        await db.connect()
        try:
            async with CocosCapitalScraper(self.cfg.scraper) as scraper:
                await scraper.login()
                acciones = await scraper.scrape_market("ACCIONES")
                cedears = await scraper.scrape_market("CEDEARS")

                all_rows = (acciones or []) + (cedears or [])
                if all_rows:
                    await db.save_market_prices(all_rows)

                logger.info(
                    "run_once OK | acciones=%d | cedears=%d | total=%d",
                    len(acciones or []),
                    len(cedears or []),
                    len(all_rows),
                )
        finally:
            await db.close()

    async def _run_market_session(self) -> None:
        """
        Mantiene una sesión viva mientras el mercado esté abierto.
        Si se cae, reintenta solo.
        """
        session_started_at = datetime.now(tz=ART_TZ)
        logger.info(
            "Abriendo sesión de mercado | %s ART",
            session_started_at.strftime("%Y-%m-%d %H:%M:%S"),
        )

        try:
            async with CocosCapitalScraper(self.cfg.scraper) as scraper:
                await scraper.login()
                logger.info("Login OK en Cocos")

                while self._running:
                    now = datetime.now(tz=ART_TZ)
                    if not self._is_market_open(now):
                        logger.info("Fin de sesión: mercado cerrado")
                        return

                    await self._poll_market(scraper)
                    await asyncio.sleep(self.poll_seconds)

        except Exception as e:
            self.stats.cycles_error += 1
            self.stats.last_error_at = datetime.now(tz=ART_TZ)
            self.stats.last_error = str(e)
            logger.error("Sesión de mercado caída: %s", e, exc_info=True)
            await asyncio.sleep(self.retry_seconds)

    async def _poll_market(self, scraper: CocosCapitalScraper) -> None:
        if not self._db:
            raise RuntimeError("DB no inicializada")

        poll_started = datetime.now(tz=ART_TZ)

        acciones = await scraper.scrape_market("ACCIONES")
        cedears = await scraper.scrape_market("CEDEARS")
        rows = (acciones or []) + (cedears or [])

        if rows:
            await self._db.save_market_prices(rows)

        self.stats.cycles_ok += 1
        self.stats.last_success_at = poll_started

        logger.info(
            "Poll OK | %s ART | acciones=%d | cedears=%d | total=%d",
            poll_started.strftime("%H:%M:%S"),
            len(acciones or []),
            len(cedears or []),
            len(rows),
        )

    def _is_market_open(self, now: datetime) -> bool:
        t = now.timetz().replace(tzinfo=None)
        return self.market_open <= t < self.market_close

    def _seconds_until_next_open(self, now: datetime) -> int:
        today_open = datetime.combine(now.date(), self.market_open, tzinfo=ART_TZ)

        if now < today_open:
            delta = today_open - now
            return max(30, int(delta.total_seconds()))

        next_day = now.date() + timedelta(days=1)
        next_open = datetime.combine(next_day, self.market_open, tzinfo=ART_TZ)
        delta = next_open - now
        return max(30, int(delta.total_seconds()))
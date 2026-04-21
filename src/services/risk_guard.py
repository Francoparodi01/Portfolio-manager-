from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from src.core.config import get_config
from src.core.logger import get_logger
from src.collector.db import PortfolioDatabase
from src.collector.notifier import TelegramNotifier

logger = get_logger(__name__)

ART_TZ = ZoneInfo("America/Argentina/Buenos_Aires")


@dataclass
class GuardDecision:
    ticker: str
    decided_at: datetime
    entry_price: float
    stop_loss_pct: float
    target_pct: float
    horizon_days: int
    size_pct: float
    regime: str | None = None
    score: float | None = None
    confidence: float | None = None

    @property
    def stop_price(self) -> float:
        return self.entry_price * (1 + self.stop_loss_pct)

    @property
    def target_price(self) -> float:
        return self.entry_price * (1 + self.target_pct)


@dataclass
class GuardState:
    ticker: str
    current_price: float
    entry_price: float
    pnl_pct: float
    stop_price: float
    target_price: float
    distance_to_stop_pct: float
    distance_to_target_pct: float
    days_held: int
    severity: str
    title: str
    message: str


class RiskGuard:
    """
    Guardia intradiaria de riesgo.

    Vigila posiciones compradas activas usando:
      - snapshot más reciente del portfolio
      - decision_log (última BUY por ticker)
      - market_prices (último precio por ticker)

    Genera alertas:
      - WARNING
      - CRITICAL
      - STOP_NEAR
      - STOP_TRIGGERED

    Además:
      - persiste alertas en stop_alerts
      - deduplica por ticker+severity en memoria
      - intenta deduplicar también en DB
    """

    def __init__(
        self,
        poll_seconds: int = 60,
        market_open_hour: int = 10,
        market_open_minute: int = 30,
        market_close_hour: int = 17,
        market_close_minute: int = 0,
        warning_pct: float = -0.04,
        critical_pct: float = -0.06,
        stop_near_pct: float = 0.02,
        dedup_minutes: int = 30,
    ) -> None:
        self.cfg = get_config()
        self.poll_seconds = poll_seconds
        self.market_open = time(hour=market_open_hour, minute=market_open_minute)
        self.market_close = time(hour=market_close_hour, minute=market_close_minute)
        self.warning_pct = warning_pct
        self.critical_pct = critical_pct
        self.stop_near_pct = stop_near_pct
        self.dedup_minutes = dedup_minutes

        self._running = False
        self._db: PortfolioDatabase | None = None
        self._notifier = TelegramNotifier(
            self.cfg.scraper.telegram_bot_token,
            self.cfg.scraper.telegram_chat_id,
        )
        self._last_sent: dict[str, datetime] = {}

    async def run_forever(self) -> None:
        self._running = True
        self._db = PortfolioDatabase(self.cfg.database.url)
        await self._db.connect()

        logger.info(
            "RiskGuard iniciado | horario %s-%s ART | poll=%ss",
            self.market_open.strftime("%H:%M"),
            self.market_close.strftime("%H:%M"),
            self.poll_seconds,
        )

        try:
            while self._running:
                now = datetime.now(tz=ART_TZ)

                if self._is_market_open(now):
                    await self._run_cycle(now)
                    await asyncio.sleep(self.poll_seconds)
                else:
                    sleep_for = self._seconds_until_next_open(now)
                    logger.info(
                        "Mercado cerrado | RiskGuard duerme hasta próxima apertura en %ss",
                        sleep_for,
                    )
                    await asyncio.sleep(sleep_for)
        finally:
            if self._db:
                await self._db.close()
            logger.info("RiskGuard detenido")

    def stop(self) -> None:
        self._running = False

    async def run_once(self) -> None:
        now = datetime.now(tz=ART_TZ)
        self._db = PortfolioDatabase(self.cfg.database.url)
        await self._db.connect()
        try:
            await self._run_cycle(now, force=True)
        finally:
            await self._db.close()
            self._db = None

    async def _run_cycle(self, now: datetime, force: bool = False) -> None:
        if not self._db:
            raise RuntimeError("DB no inicializada")

        snapshot = await self._db.get_latest_snapshot()
        if not snapshot:
            logger.warning("RiskGuard: sin snapshot de portfolio")
            return

        positions = snapshot.get("positions", []) or []
        if not positions:
            logger.info("RiskGuard: sin posiciones activas en snapshot")
            return

        tickers = [str(p.get("ticker", "")).upper() for p in positions if p.get("ticker")]
        tickers = [t for t in tickers if t]

        decision_map = await self._load_latest_buy_decisions(tickers)
        if not decision_map:
            logger.warning("RiskGuard: no encontró decisiones BUY activas/relevantes para monitorear")
            return

        price_map = await self._load_latest_market_prices(list(decision_map.keys()))
        if not price_map:
            logger.warning("RiskGuard: no encontró precios recientes en market_prices")
            return

        alerts_to_send: list[GuardState] = []

        for ticker, decision in decision_map.items():
            current_price = price_map.get(ticker)
            if current_price is None:
                continue

            state = self._evaluate_position(
                ticker=ticker,
                decision=decision,
                current_price=current_price,
            )
            if not state:
                continue

            if force or self._should_send_memory(state):
                alerts_to_send.append(state)

        if not alerts_to_send:
            logger.info("RiskGuard: sin alertas | %s ART", now.strftime("%H:%M:%S"))
            return

        for state in alerts_to_send:
            inserted = await self._persist_alert_if_new(state)
            if not inserted and not force:
                logger.info("RiskGuard dedup DB | %s | %s", state.ticker, state.severity)
                continue

            logger.warning(
                "RiskGuard alerta %s | %s | px=%.2f | pnl=%.2f%%",
                state.severity,
                state.ticker,
                state.current_price,
                state.pnl_pct * 100,
            )

            self._send_alert(state)
            self._last_sent[f"{state.ticker}:{state.severity}"] = now

    async def _load_latest_buy_decisions(self, tickers: list[str]) -> dict[str, GuardDecision]:
        if not self._db:
            raise RuntimeError("DB no inicializada")

        pool = await self._db.get_pool()
        if not pool or not tickers:
            return {}

        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (ticker)
                    ticker,
                    decided_at,
                    price_at_decision,
                    stop_loss_pct,
                    target_pct,
                    horizon_days,
                    size_pct,
                    regime,
                    final_score,
                    confidence
                FROM decision_log
                WHERE ticker = ANY($1::text[])
                  AND decision = 'BUY'
                  AND price_at_decision IS NOT NULL
                  AND stop_loss_pct IS NOT NULL
                  AND target_pct IS NOT NULL
                ORDER BY ticker, decided_at DESC
                """,
                tickers,
            )

        out: dict[str, GuardDecision] = {}
        for r in rows:
            try:
                ticker = str(r["ticker"]).upper()
                decided_at = r["decided_at"]
                if decided_at is None:
                    continue

                out[ticker] = GuardDecision(
                    ticker=ticker,
                    decided_at=decided_at,
                    entry_price=float(r["price_at_decision"]),
                    stop_loss_pct=float(r["stop_loss_pct"]),
                    target_pct=float(r["target_pct"]),
                    horizon_days=int(r["horizon_days"] or 20),
                    size_pct=float(r["size_pct"] or 0.0),
                    regime=r["regime"],
                    score=float(r["final_score"]) if r["final_score"] is not None else None,
                    confidence=float(r["confidence"]) if r["confidence"] is not None else None,
                )
            except Exception as e:
                logger.warning("No se pudo parsear decision_log row: %s", e)

        return out

    async def _load_latest_market_prices(self, tickers: list[str]) -> dict[str, float]:
        if not self._db:
            raise RuntimeError("DB no inicializada")

        pool = await self._db.get_pool()
        if not pool or not tickers:
            return {}

        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (ticker)
                    ticker,
                    last_price,
                    ts
                FROM market_prices
                WHERE ticker = ANY($1::text[])
                  AND last_price IS NOT NULL
                ORDER BY ticker, ts DESC
                """,
                tickers,
            )

        out: dict[str, float] = {}
        for r in rows:
            try:
                out[str(r["ticker"]).upper()] = float(r["last_price"])
            except Exception:
                continue

        return out

    def _evaluate_position(self, ticker: str, decision: GuardDecision, current_price: float) -> GuardState | None:
        pnl_pct = (current_price / decision.entry_price) - 1.0
        distance_to_stop_pct = (current_price / decision.stop_price) - 1.0
        distance_to_target_pct = (decision.target_price / current_price) - 1.0
        days_held = (datetime.now(tz=ART_TZ).date() - decision.decided_at.date()).days

        if current_price <= decision.stop_price:
            severity = "STOP_TRIGGERED"
            title = f"STOP ACTIVADO — {ticker}"
            msg = (
                f"🚨 <b>STOP ACTIVADO — {ticker}</b>\n\n"
                f"Precio actual: <b>${current_price:,.2f}</b>\n"
                f"Entry: <b>${decision.entry_price:,.2f}</b>\n"
                f"Stop: <b>${decision.stop_price:,.2f}</b>\n"
                f"PNL: <b>{pnl_pct * 100:+.2f}%</b>\n"
                f"Días: <b>{days_held}</b>\n\n"
                f"<b>Acción:</b> revisar salida inmediata."
            )
        elif 0 <= distance_to_stop_pct <= self.stop_near_pct:
            severity = "STOP_NEAR"
            title = f"STOP NEAR — {ticker}"
            msg = (
                f"⚠️ <b>STOP NEAR — {ticker}</b>\n\n"
                f"Precio actual: <b>${current_price:,.2f}</b>\n"
                f"Stop: <b>${decision.stop_price:,.2f}</b>\n"
                f"Distancia al stop: <b>{distance_to_stop_pct * 100:.2f}%</b>\n"
                f"PNL: <b>{pnl_pct * 100:+.2f}%</b>\n\n"
                f"<b>Acción:</b> preparar salida / vigilar de cerca."
            )
        elif pnl_pct <= self.critical_pct:
            severity = "CRITICAL"
            title = f"CRITICAL — {ticker}"
            msg = (
                f"🔴 <b>CRITICAL — {ticker}</b>\n\n"
                f"Precio actual: <b>${current_price:,.2f}</b>\n"
                f"Entry: <b>${decision.entry_price:,.2f}</b>\n"
                f"PNL: <b>{pnl_pct * 100:+.2f}%</b>\n"
                f"Stop: <b>${decision.stop_price:,.2f}</b>\n"
                f"Target: <b>${decision.target_price:,.2f}</b>\n\n"
                f"<b>Acción:</b> pérdida relevante, revisar tesis."
            )
        elif pnl_pct <= self.warning_pct:
            severity = "WARNING"
            title = f"WARNING — {ticker}"
            msg = (
                f"🟡 <b>WARNING — {ticker}</b>\n\n"
                f"Precio actual: <b>${current_price:,.2f}</b>\n"
                f"Entry: <b>${decision.entry_price:,.2f}</b>\n"
                f"PNL: <b>{pnl_pct * 100:+.2f}%</b>\n"
                f"Stop: <b>${decision.stop_price:,.2f}</b>\n"
                f"Target: <b>${decision.target_price:,.2f}</b>\n\n"
                f"<b>Acción:</b> no aumentar, monitorear."
            )
        else:
            return None

        return GuardState(
            ticker=ticker,
            current_price=current_price,
            entry_price=decision.entry_price,
            pnl_pct=pnl_pct,
            stop_price=decision.stop_price,
            target_price=decision.target_price,
            distance_to_stop_pct=distance_to_stop_pct,
            distance_to_target_pct=distance_to_target_pct,
            days_held=days_held,
            severity=severity,
            title=title,
            message=msg,
        )

    def _should_send_memory(self, state: GuardState) -> bool:
        key = f"{state.ticker}:{state.severity}"
        last = self._last_sent.get(key)
        if not last:
            return True
        return (datetime.now(tz=ART_TZ) - last) >= timedelta(minutes=self.dedup_minutes)

    async def _persist_alert_if_new(self, state: GuardState) -> bool:
        """
        Inserta la alerta en stop_alerts si no hay una igual reciente.
        Dedupe por ticker + alert_type + ventana de minutos.
        """
        if not self._db:
            raise RuntimeError("DB no inicializada")

        pool = await self._db.get_pool()
        if not pool:
            return False

        cutoff = datetime.utcnow() - timedelta(minutes=self.dedup_minutes)

        async with pool.acquire() as conn:
            existing = await conn.fetchval(
                """
                SELECT id
                FROM stop_alerts
                WHERE ticker = $1
                  AND alert_type = $2
                  AND created_at >= $3
                ORDER BY created_at DESC
                LIMIT 1
                """,
                state.ticker,
                state.severity,
                cutoff,
            )

            if existing:
                return False

            await conn.execute(
                """
                INSERT INTO stop_alerts (
                    ticker,
                    alert_type,
                    created_at,
                    message,
                    title,
                    severity,
                    price_at_alert
                )
                VALUES ($1, $2, NOW(), $3, $4, $5, $6)
                """,
                state.ticker,
                state.severity,
                state.message,
                state.title,
                state.severity,
                state.current_price,
            )

        return True

    def _send_alert(self, state: GuardState) -> None:
        try:
            self._notifier.send_raw(state.message)
        except Exception as e:
            logger.error(
                "No se pudo enviar alerta Telegram para %s: %s",
                state.ticker,
                e,
                exc_info=True,
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
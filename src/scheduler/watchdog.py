"""Scheduler watchdog: did every scheduled job of today leave its mark in the DB?

APScheduler fires the jobs, but a job can still skip silently (portfolio
refresh down, scraper timeout, misfire). The database is the source of truth:
each slot names the artifact its job must produce. Every check, a slot whose
deadline passed without an artifact is re-run once and reported on Telegram.

Pure decision logic (``missing_slots``) is separate from the DB reads and
the job calls, so it can be tested without a scheduler.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Awaitable, Callable, Iterable, Optional
from zoneinfo import ZoneInfo

import asyncpg

ART = ZoneInfo("America/Argentina/Buenos_Aires")


@dataclass(frozen=True)
class Slot:
    name: str
    label: str
    artifact: str  # plans | market | portfolio | radar | candles
    start: time  # scheduled time
    due: time  # no artifact by then = missed
    last_retry: time  # do not re-run after this (market closed, data stale, etc.)


SLOTS: tuple[Slot, ...] = (
    Slot("open_1031", "snapshot de apertura 10:31", "portfolio", time(10, 31), time(11, 0), time(16, 30)),
    Slot("market_1040", "mercado 10:40", "market", time(10, 40), time(11, 0), time(16, 55)),
    Slot("analysis_11", "análisis completo 11:00", "plans", time(11, 0), time(11, 45), time(12, 40)),
    Slot("market_1200", "mercado 12:00", "market", time(12, 0), time(12, 20), time(16, 55)),
    Slot("analysis_13", "análisis completo 13:00", "plans", time(13, 0), time(13, 45), time(15, 40)),
    Slot("analysis_16", "análisis completo 16:00", "plans", time(16, 0), time(16, 40), time(16, 50)),
    Slot("market_1640", "mercado 16:40", "market", time(16, 40), time(16, 58), time(16, 59)),
    Slot("radar_1650", "radar 16:50", "radar", time(16, 50), time(17, 30), time(20, 0)),
    Slot("eod_1702", "cierre de cartera 17:02", "portfolio", time(17, 2), time(17, 35), time(20, 0)),
    Slot("candles_1705", "velas del día 17:05", "candles", time(17, 5), time(17, 40), time(20, 0)),
)


def missing_slots(
    now: datetime,
    artifacts: dict[str, list[datetime]],
    attempted: Iterable[str] = (),
) -> list[Slot]:
    """Slots due by ``now`` with no artifact since their start, not yet retried, still retriable."""
    local = now.astimezone(ART)
    today = local.date()
    done = set(attempted)
    out = []
    for slot in SLOTS:
        if slot.name in done or not (slot.due <= local.time() <= slot.last_retry):
            continue
        if not has_artifact(slot, artifacts, today):
            out.append(slot)
    return out


def has_artifact(slot: Slot, artifacts: dict[str, list[datetime]], day: date) -> bool:
    start = datetime.combine(day, slot.start, ART)
    return any(ts.astimezone(ART) >= start for ts in artifacts.get(slot.artifact, []))


ARTIFACT_SQL = {
    "plans": "SELECT created_at AS ts FROM execution_plans WHERE created_at >= $1",
    "market": "SELECT MAX(ts) AS ts FROM market_prices WHERE ts >= $1 GROUP BY date_trunc('minute', ts)",
    "portfolio": "SELECT scraped_at AS ts FROM portfolio_snapshots WHERE scraped_at >= $1",
    "radar": "SELECT decided_at AS ts FROM decision_log WHERE decided_at >= $1 AND (source = 'radar' OR metric_scope = 'radar_audit')",
    "candles": (
        "SELECT MAX(scraped_at) AS ts FROM market_candles WHERE scraped_at >= $1 "
        "AND interval = '1d' AND source = 'internal_snapshot' AND ts::date = ($1 AT TIME ZONE 'America/Argentina/Buenos_Aires')::date "
        "HAVING COUNT(*) > 0"
    ),
}


async def fetch_artifacts(database_url: str, day: date) -> dict[str, list[datetime]]:
    start = datetime.combine(day, time(0), ART)
    conn = await asyncpg.connect(
        database_url.replace("postgresql+asyncpg://", "postgresql://"),
        server_settings={"default_transaction_read_only": "on"},
    )
    out: dict[str, list[datetime]] = {}
    try:
        for kind, sql in ARTIFACT_SQL.items():
            try:
                out[kind] = [r["ts"] for r in await conn.fetch(sql, start) if r["ts"] is not None]
            except Exception:  # noqa: BLE001 - a broken query must not hide the others
                out[kind] = []
    finally:
        await conn.close()
    return out


class Watchdog:
    """Keeps which slots were already retried today (in memory; a restart retries again once)."""

    def __init__(
        self,
        *,
        database_url: str,
        jobs: dict[str, Callable[[], Awaitable[object]]],
        notify: Callable[[str], None],
        now: Callable[[], datetime],
        is_business_day: Callable[[datetime], bool],
    ):
        self.database_url = database_url
        self.jobs = jobs
        self.notify = notify
        self.now = now
        self.is_business_day = is_business_day
        self.attempted: dict[date, set[str]] = {}

    async def check(self) -> list[str]:
        now = self.now()
        if not self.is_business_day(now):
            return []
        today = now.astimezone(ART).date()
        attempted = self.attempted.setdefault(today, set())
        for old in [d for d in self.attempted if d != today]:
            self.attempted.pop(old, None)
        artifacts = await fetch_artifacts(self.database_url, today)
        report = []
        for slot in missing_slots(now, artifacts, attempted):
            job = self.jobs.get(slot.name)
            if job is None:
                continue
            attempted.add(slot.name)
            self.notify(f"⏰ <b>Vigilante</b>: no hay rastro de <b>{slot.label}</b>. Lo vuelvo a correr ahora.")
            error: Optional[str] = None
            try:
                await job()
            except Exception as exc:  # noqa: BLE001
                error = f"{type(exc).__name__}: {exc}"[:300]
            after = await fetch_artifacts(self.database_url, today)
            if has_artifact(slot, after, today) and error is None:
                self.notify(f"✅ <b>Vigilante</b>: {slot.label} recuperado.")
                report.append(f"{slot.name}:recovered")
            else:
                detail = f" Error: <code>{error}</code>" if error else " El reintento tampoco dejó rastro."
                self.notify(f"❌ <b>Vigilante</b>: {slot.label} sigue faltando.{detail} Revisá el scheduler.")
                report.append(f"{slot.name}:failed")
        return report

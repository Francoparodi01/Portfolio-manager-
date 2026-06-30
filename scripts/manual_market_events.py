#!/usr/bin/env python
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import date, datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.manual_market_events import (
    ART_TZ,
    BLOCK_NEW_BUYS,
    default_active_window,
    normalize_action_policy,
    normalize_csv,
    normalize_event_time_hint,
    normalize_severity,
)
from src.collector.db import PortfolioDatabase
from src.core.config import get_config


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ART_TZ)
    return parsed


async def _cmd_add(args) -> None:
    cfg = get_config()
    event_date = _parse_date(args.event_date)
    hint = normalize_event_time_hint(args.time_hint)
    default_from, default_until = default_active_window(event_date, hint)
    active_from = _parse_dt(args.active_from) or default_from
    active_until = _parse_dt(args.active_until) or default_until

    db = PortfolioDatabase(cfg.database.url)
    await db.connect()
    try:
        row_id = await db.upsert_manual_market_event(
            event_date=event_date,
            event_time_hint=hint,
            ticker=args.ticker,
            title=args.title,
            impact_scope=list(normalize_csv(args.scope)),
            related_tickers=list(normalize_csv(args.related, ticker=True)),
            severity=normalize_severity(args.severity),
            active_from=active_from,
            active_until=active_until,
            action_policy=normalize_action_policy(args.policy),
            notes=args.notes,
        )
        print(f"manual_market_event inserted id={row_id}")
        print(f"active_from={active_from.isoformat()} active_until={active_until.isoformat()}")
    finally:
        await db.close()


async def _cmd_list(args) -> None:
    cfg = get_config()
    db = PortfolioDatabase(cfg.database.url)
    await db.connect()
    try:
        if args.active:
            events = await db.get_active_manual_market_events()
        else:
            await db.ensure_manual_market_events_schema()
            pool = await db.get_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT
                        id,
                        event_date,
                        event_time_hint,
                        ticker,
                        title,
                        impact_scope,
                        related_tickers,
                        severity,
                        active_from,
                        active_until,
                        action_policy,
                        notes,
                        is_active
                    FROM manual_market_events
                    ORDER BY active_from DESC, id DESC
                    LIMIT $1
                    """,
                    int(args.limit),
                )
            from src.analysis.manual_market_events import manual_market_event_from_row

            events = [manual_market_event_from_row(dict(row)) for row in rows]

        if not events:
            print("manual_market_events: none")
            return

        for event in events:
            impacted = ",".join(event.impacted_tickers)
            scope = ",".join(event.impact_scope)
            print(
                f"#{event.id} active={event.is_active} {event.event_date} "
                f"{event.event_time_hint} {event.ticker or '-'} | "
                f"{event.title} | sev={event.severity} policy={event.action_policy} | "
                f"scope={scope or '-'} related={impacted or '-'} | "
                f"{event.active_from.isoformat()} -> {event.active_until.isoformat()}"
            )
            if event.notes:
                print(f"   notes={event.notes}")
    finally:
        await db.close()


async def _cmd_deactivate(args) -> None:
    cfg = get_config()
    db = PortfolioDatabase(cfg.database.url)
    await db.connect()
    try:
        ok = await db.deactivate_manual_market_event(args.id)
        print(f"manual_market_event id={args.id} deactivated={ok}")
    finally:
        await db.close()


async def main() -> None:
    parser = argparse.ArgumentParser(description="Manage manual market events/catalysts")
    sub = parser.add_subparsers(dest="cmd", required=True)

    add = sub.add_parser("add", help="insert a manual market event")
    add.add_argument("--event-date", required=True, help="YYYY-MM-DD")
    add.add_argument("--time-hint", default="unknown", choices=["before_open", "during_market", "after_close", "unknown"])
    add.add_argument("--ticker", default="")
    add.add_argument("--title", required=True)
    add.add_argument("--scope", default="", help="comma-separated impact scope")
    add.add_argument("--related", default="", help="comma-separated related tickers")
    add.add_argument("--severity", default="high", choices=["low", "medium", "high"])
    add.add_argument("--policy", default=BLOCK_NEW_BUYS, choices=["warn_only", "block_new_buys", "no_action"])
    add.add_argument("--active-from", default=None, help="ISO datetime; naive means ART")
    add.add_argument("--active-until", default=None, help="ISO datetime; naive means ART")
    add.add_argument("--notes", default="")

    list_cmd = sub.add_parser("list", help="list manual market events")
    list_cmd.add_argument("--active", action="store_true")
    list_cmd.add_argument("--limit", type=int, default=30)

    deactivate = sub.add_parser("deactivate", help="deactivate one event")
    deactivate.add_argument("id", type=int)

    args = parser.parse_args()
    if args.cmd == "add":
        await _cmd_add(args)
    elif args.cmd == "list":
        await _cmd_list(args)
    elif args.cmd == "deactivate":
        await _cmd_deactivate(args)


if __name__ == "__main__":
    asyncio.run(main())

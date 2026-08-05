"""Render upcoming portfolio earnings from persisted issuer observations."""
from __future__ import annotations

import argparse
import asyncio
from datetime import date, timedelta
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.upcoming_earnings import (
    DEFAULT_UPCOMING_EARNINGS_DAYS,
    render_upcoming_earnings_report,
    upcoming_earnings_from_rows,
)
from src.collector.db import PortfolioDatabase
from src.collector.notifier import TelegramNotifier
from src.core.config import get_config


async def run(
    *,
    days: int,
    tickers: list[str],
    owner_chat_id: int | None,
) -> str:
    cfg = get_config()
    db = PortfolioDatabase(cfg.database.url)
    await db.connect()
    try:
        selected_tickers = [str(ticker).upper().strip() for ticker in tickers if str(ticker).strip()]
        if not selected_tickers:
            snapshot = await db.get_latest_snapshot(owner_chat_id=owner_chat_id)
            selected_tickers = sorted({
                str(position.get("ticker") or "").upper().strip()
                for position in (snapshot or {}).get("positions") or []
                if str(position.get("ticker") or "").strip()
            })
        today = date.today()
        rows = await db.get_upcoming_earnings_events(
            from_date=today,
            to_date=today + timedelta(days=max(1, int(days))),
            tickers=selected_tickers,
            limit=100,
        )
    finally:
        await db.close()
    return render_upcoming_earnings_report(
        upcoming_earnings_from_rows(rows),
        today=today,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Proximos balances de la cartera")
    parser.add_argument("--days", type=int, default=DEFAULT_UPCOMING_EARNINGS_DAYS)
    parser.add_argument("--tickers", nargs="*", default=[])
    parser.add_argument("--owner-chat-id", type=int, default=None)
    parser.add_argument("--no-telegram", action="store_true")
    args = parser.parse_args()
    report = asyncio.run(
        run(
            days=max(1, int(args.days)),
            tickers=args.tickers,
            owner_chat_id=args.owner_chat_id,
        )
    )
    print(report)
    if not args.no_telegram:
        cfg = get_config()
        TelegramNotifier(
            cfg.scraper.telegram_bot_token,
            cfg.scraper.telegram_chat_id,
        ).send_raw(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

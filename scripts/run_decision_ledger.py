"""
Decision Ledger.

Read-only economic attribution layer:
- real execution PnL in ARS;
- bot vs human economic delta;
- radar candidate and swap comparison;
- mark-to-latest for pending decisions.

It does not change analysis, thresholds, planner, fills or outcomes.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

import asyncpg

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.decision_ledger import fetch_decision_ledger, render_decision_ledger
from src.collector.notifier import TelegramNotifier
from src.core.config import get_config


async def async_main(args: argparse.Namespace) -> int:
    cfg = get_config()
    dsn = cfg.database.url.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    try:
        data = await fetch_decision_ledger(
            conn,
            days=args.days,
            match_window_days=args.match_window_days,
            owner_chat_id=args.owner_chat_id,
        )
    finally:
        await conn.close()

    report = render_decision_ledger(data)
    print(report)

    if not args.no_telegram and cfg.scraper.telegram_enabled:
        TelegramNotifier(cfg.scraper.telegram_bot_token, cfg.scraper.telegram_chat_id).send_raw(report)

    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Decision Ledger economico")
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--match-window-days", type=int, default=2)
    parser.add_argument("--owner-chat-id", type=int, default=None)
    parser.add_argument("--no-telegram", action="store_true")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(async_main(args)))


if __name__ == "__main__":
    main()

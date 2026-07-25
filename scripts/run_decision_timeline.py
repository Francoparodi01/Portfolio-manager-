"""
Read-only Decision Timeline CLI.

Prints a normalized lifecycle view to stdout. It does not write to DB and does
not send Telegram messages.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys

import asyncpg

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.decision_timeline import fetch_decision_timeline, render_decision_timeline
from src.core.config import get_config


async def async_main(args: argparse.Namespace) -> int:
    cfg = get_config()
    dsn = cfg.database.url.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    try:
        data = await fetch_decision_timeline(
            conn,
            days=args.days,
            run_id=args.run_id,
            ticker=args.ticker,
            owner_chat_id=args.owner_chat_id,
            limit=args.limit,
        )
    finally:
        await conn.close()

    if args.json:
        print(json.dumps(data, ensure_ascii=False, indent=2))
    else:
        print(render_decision_timeline(data))
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Decision Timeline read-only")
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--ticker", default=None)
    parser.add_argument("--owner-chat-id", type=int, default=None)
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(async_main(args)))


if __name__ == "__main__":
    main()

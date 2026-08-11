"""Read-only net performance report by analysis run and decision."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

import asyncpg

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.net_decision_report import (
    fetch_net_decision_report,
    render_net_decision_report,
    write_decision_csv,
    write_run_csv,
)
from src.collector.notifier import TelegramNotifier
from src.core.config import get_config


async def async_main(args: argparse.Namespace) -> int:
    cfg = get_config()
    dsn = cfg.database.url.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    try:
        data = await fetch_net_decision_report(
            conn,
            days=args.days,
            owner_chat_id=args.owner_chat_id,
        )
    finally:
        await conn.close()

    report = render_net_decision_report(data, limit_runs=args.limit_runs)
    print(report)
    if args.csv_out:
        csv_path = write_decision_csv(data["rows"], args.csv_out)
        print(f"[csv] {csv_path}")
    if args.runs_csv_out:
        runs_csv_path = write_run_csv(data["runs"], args.runs_csv_out)
        print(f"[runs_csv] {runs_csv_path}")

    if not args.no_telegram and cfg.scraper.telegram_enabled:
        TelegramNotifier(cfg.scraper.telegram_bot_token, cfg.scraper.telegram_chat_id).send_raw(report)
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Resultado neto por corrida y decision")
    parser.add_argument("--days", type=int, default=180)
    parser.add_argument("--limit-runs", type=int, default=6)
    parser.add_argument("--owner-chat-id", type=int, default=None)
    parser.add_argument("--csv-out")
    parser.add_argument("--runs-csv-out")
    parser.add_argument("--no-telegram", action="store_true")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(async_main(args)))


if __name__ == "__main__":
    main()

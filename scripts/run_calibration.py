from __future__ import annotations

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.dcl.report_generator import render_calibration_report
from src.analysis.dcl.run_calibration import run_calibration_cycle
from src.collector.notifier import TelegramNotifier
from src.core.config import get_config
from src.core.logger import get_logger


logger = get_logger(__name__)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Decision Calibration Layer audit")
    parser.add_argument("--days", type=int, default=180)
    parser.add_argument("--owner-chat-id", type=int, default=None)
    parser.add_argument("--min-n", type=int, default=20)
    parser.add_argument(
        "--quality-mode",
        choices=("strict", "relaxed", "all"),
        default="relaxed",
        help="strict=clean, relaxed=clean+mixed, all=diagnostico",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Alias de --quality-mode strict",
    )
    parser.add_argument(
        "--relaxed",
        action="store_true",
        help="Alias de --quality-mode relaxed",
    )
    parser.add_argument("--no-telegram", action="store_true")
    args = parser.parse_args()
    quality_mode = "strict" if args.strict else "relaxed" if args.relaxed else args.quality_mode

    cfg = get_config()
    report = await run_calibration_cycle(
        cfg.database.url,
        days=args.days,
        owner_chat_id=args.owner_chat_id,
        quality_mode=quality_mode,
        min_n=args.min_n,
        dry_run=True,
    )
    text = render_calibration_report(report)
    print(text)

    if args.no_telegram or not cfg.scraper.telegram_enabled:
        return

    try:
        TelegramNotifier(
            cfg.scraper.telegram_bot_token,
            cfg.scraper.telegram_chat_id,
        ).send_raw(text)
    except Exception as exc:
        logger.warning("No pude enviar DCL a Telegram: %s", exc)


if __name__ == "__main__":
    asyncio.run(main())

"""Live read-only bridge for Telegram; canonical analytics CLI remains offline."""
import argparse
import asyncio
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import asyncpg
from src.analysis.analytics_v2_live import capture, render_telegram, summarize, write_archive
from src.core.config import get_config
from src.core.telegram_format import validate_telegram_html


async def async_main(args):
    cfg = get_config()
    configured_owner = str(cfg.scraper.telegram_chat_id or "").strip()
    multiuser = bool(cfg.multiuser_enabled)
    legacy_null = not multiuser and configured_owner == str(args.owner_chat_id)
    conn = await asyncpg.connect(cfg.database.url.replace("postgresql+asyncpg://", "postgresql://"), command_timeout=60)
    try:
        snapshot = await capture(conn, owner_chat_id=args.owner_chat_id, days=args.days, allow_legacy_null=legacy_null)
    finally:
        await conn.close()
    package = summarize(snapshot)
    text = render_telegram(package)
    valid, errors = validate_telegram_html(text)
    if not valid:
        raise ValueError(f"Analytics report HTML validation failed: {errors}")
    if args.archive_out:
        write_archive(package, args.archive_out)
    print(text)


def main():
    parser = argparse.ArgumentParser(description="Owner-scoped Analytics v2 observational report")
    parser.add_argument("--owner-chat-id", required=True, type=int)
    parser.add_argument("--days", default=180, type=int)
    parser.add_argument("--archive-out", type=Path)
    args = parser.parse_args()
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()

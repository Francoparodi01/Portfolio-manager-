"""Read-only ticker technical analysis runner.

Usage:
  python scripts/run_ticker_analysis.py NVDA --no-telegram --chart-out /tmp/nvda.png
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

from src.analysis.ticker_technical_report import (
    DEFAULT_CANDLE_LIMIT,
    build_ticker_technical_report_from_db,
    normalize_ticker,
    render_ticker_technical_charts,
    render_ticker_telegram_report,
)
from src.collector.db import PortfolioDatabase
from src.core.config import get_config
from src.core.logger import get_logger


logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ticker technical report")
    parser.add_argument("ticker", help="Ticker a analizar, por ejemplo NVDA")
    parser.add_argument("--chart-out", help="Ruta PNG de salida")
    parser.add_argument("--owner-chat-id", type=int, default=None)
    parser.add_argument("--limit", type=int, default=DEFAULT_CANDLE_LIMIT)
    parser.add_argument("--no-telegram", action="store_true", help="Compatibilidad CLI")
    parser.add_argument(
        "--no-yfinance-fallback",
        action="store_true",
        help="Fallar si market_candles no tiene velas suficientes",
    )
    return parser.parse_args()


async def main_async() -> int:
    args = parse_args()
    ticker = normalize_ticker(args.ticker)

    cfg = get_config()
    db = PortfolioDatabase(cfg.database.url)
    await db.connect()
    try:
        report = await build_ticker_technical_report_from_db(
            db,
            ticker,
            owner_chat_id=args.owner_chat_id,
            candle_limit=args.limit,
            allow_yfinance_fallback=not args.no_yfinance_fallback,
        )
    finally:
        await db.close()

    if args.chart_out:
        for chart_path in render_ticker_technical_charts(report, args.chart_out):
            print(f"[chart] {chart_path}")

    print(render_ticker_telegram_report(report))
    return 0


def main() -> None:
    try:
        raise SystemExit(asyncio.run(main_async()))
    except Exception as exc:
        logger.exception("ticker analysis fallo")
        print(f"Error en ticker analysis: {exc}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()

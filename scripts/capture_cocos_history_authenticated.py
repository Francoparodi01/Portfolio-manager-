from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.capture_cocos_history import capture_history_from_page
from scripts.import_cocos_history import _load_candles
from src.collector.cocos_scraper import CocosCapitalScraper
from src.collector.db import PortfolioDatabase
from src.core.config import get_config


async def capture_authenticated(
    *,
    market: str,
    ticker: str,
    wait_ms: int,
    chart_range: str | None,
    range_wait_ms: int,
    interval: str,
) -> list[dict]:
    cfg = get_config()
    async with CocosCapitalScraper(cfg.scraper) as scraper:
        await scraper.login()
        page = scraper._page
        await page.set_viewport_size({"width": 2048, "height": 1150})
        return await capture_history_from_page(
            page,
            market=market,
            ticker=ticker,
            wait_ms=wait_ms,
            chart_range=chart_range,
            range_wait_ms=range_wait_ms,
            interval=interval,
        )


async def _main() -> None:
    parser = argparse.ArgumentParser(
        description="Captura manual/excepcional de velas Cocos usando credenciales configuradas"
    )
    parser.add_argument("market", choices=["ACCIONES", "CEDEARS"])
    parser.add_argument("ticker")
    parser.add_argument("--wait-ms", type=int, default=12000)
    parser.add_argument("--chart-range", help="Rango visible del chart, por ejemplo 5y")
    parser.add_argument("--range-wait-ms", type=int, default=18000)
    parser.add_argument("--interval", default="1d", help="Intervalo a persistir. Default: 1d")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--import-db", action="store_true")
    args = parser.parse_args()

    candles = await capture_authenticated(
        market=args.market,
        ticker=args.ticker,
        wait_ms=args.wait_ms,
        chart_range=args.chart_range,
        range_wait_ms=args.range_wait_ms,
        interval=args.interval,
    )

    output = json.dumps(candles, ensure_ascii=False, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output, encoding="utf-8")
    else:
        print(output)

    if args.import_db and candles:
        if not args.output:
            raise RuntimeError("--import-db requiere --output para validar el payload importado")
        db = PortfolioDatabase(get_config().database.url)
        await db.connect()
        try:
            saved = await db.save_market_candles(_load_candles(args.output))
            print(f"{saved} velas importadas")
        finally:
            await db.close()


if __name__ == "__main__":
    asyncio.run(_main())

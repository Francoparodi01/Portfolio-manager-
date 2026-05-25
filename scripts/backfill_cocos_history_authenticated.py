from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)

from scripts.backfill_cocos_history import (
    _history_output_path,
    _market_from_asset_type,
    _missing_history_assets,
)
from scripts.capture_cocos_history import capture_history_from_page
from scripts.import_cocos_history import _load_candles
from src.collector.cocos_scraper import CocosCapitalScraper
from src.collector.db import PortfolioDatabase
from src.core.config import get_config


SKIP_TICKERS = {"C.I."}


async def _capture_with_existing_session(
    scraper: CocosCapitalScraper,
    *,
    market: str,
    ticker: str,
    wait_ms: int,
    chart_range: str | None,
    range_wait_ms: int,
    interval: str,
) -> list[dict]:
    if scraper._context is None:
        raise RuntimeError("Cocos scraper context no inicializado")

    page = await scraper._context.new_page()
    try:
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
    finally:
        await page.close()


async def _main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill autenticado de velas historicas Cocos en lote"
    )
    parser.add_argument("--wait-ms", type=int, default=12000)
    parser.add_argument("--chart-range", default="5y")
    parser.add_argument("--range-wait-ms", type=int, default=18000)
    parser.add_argument("--interval", default="1d")
    parser.add_argument("--pause-ms", type=int, default=8000)
    parser.add_argument("--min-rows", type=int, default=900)
    parser.add_argument("--output-dir", type=Path, default=Path("logs/history_5y"))
    parser.add_argument("--all", action="store_true", help="Recapturar aunque ya haya historia suficiente")
    parser.add_argument("--import-db", action="store_true")
    parser.add_argument("--limit", type=int, help="Limitar cantidad de activos a capturar")
    parser.add_argument("--ticker", action="append", default=[], help="Ticker puntual. Se puede repetir.")
    parser.add_argument("--continue-on-error", action="store_true", default=True)
    args = parser.parse_args()

    cfg = get_config()
    db = PortfolioDatabase(cfg.database.url)
    await db.connect()

    try:
        assets = await db.get_cocos_universe_assets()
        tickers = {ticker.upper().strip() for ticker in args.ticker if ticker.strip()}
        if tickers:
            assets = [asset for asset in assets if str(asset["ticker"]).upper() in tickers]
        else:
            assets = [
                asset for asset in assets
                if str(asset["ticker"]).upper() not in SKIP_TICKERS
            ]

        targets = assets if args.all else await _missing_history_assets(
            db,
            assets,
            min_rows=args.min_rows,
            interval=args.interval,
        )
        if args.limit:
            targets = targets[: args.limit]

        args.output_dir.mkdir(parents=True, exist_ok=True)
        print(f"targets={len(targets)} min_rows={args.min_rows} range={args.chart_range}")

        if not targets:
            return

        async with CocosCapitalScraper(cfg.scraper) as scraper:
            await scraper.login()

            for index, asset in enumerate(targets, start=1):
                market = _market_from_asset_type(asset["asset_type"])
                ticker = str(asset["ticker"]).upper()
                path = _history_output_path(args.output_dir, asset)
                try:
                    candles = await _capture_with_existing_session(
                        scraper,
                        market=market,
                        ticker=ticker,
                        wait_ms=args.wait_ms,
                        chart_range=args.chart_range,
                        range_wait_ms=args.range_wait_ms,
                        interval=args.interval,
                    )
                    path.write_text(
                        json.dumps(candles, ensure_ascii=False, indent=2),
                        encoding="utf-8",
                    )

                    imported = 0
                    if args.import_db and candles:
                        imported = await db.save_market_candles(_load_candles(path))

                    print(
                        f"{index}/{len(targets)} {asset['asset_type']} {ticker}: "
                        f"{len(candles)} velas capturadas"
                        + (f" | {imported} importadas" if args.import_db else "")
                    )
                except Exception as exc:
                    print(f"{index}/{len(targets)} {asset['asset_type']} {ticker}: ERROR {exc}")
                    if not args.continue_on_error:
                        raise

                if index < len(targets) and args.pause_ms > 0:
                    await asyncio.sleep(args.pause_ms / 1000)
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(_main())

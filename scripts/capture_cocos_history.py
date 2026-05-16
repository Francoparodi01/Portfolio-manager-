from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

from playwright.async_api import async_playwright

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.collector.cocos_history import (
    asset_type_from_market,
    currency_from_long_ticker,
    long_ticker_from_history_url,
    merge_candle_batches,
    parse_history_payload,
)


def _is_rate_limited_page(html: str) -> bool:
    return "Error 1015" in html or "You are being rate limited" in html


def _market_url(market: str, ticker: str) -> str:
    return f"https://app.cocos.capital/market/{market.upper()}/{ticker.upper()}"


async def capture_history(
    *,
    market: str,
    ticker: str,
    cdp_url: str,
    wait_ms: int,
) -> list[dict]:
    asset_type = asset_type_from_market(market)
    batches = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.connect_over_cdp(cdp_url)
        context = browser.contexts[0]
        page = await context.new_page()

        async def on_response(response):
            if "historic-data-extended" not in response.url:
                return
            payload = await response.json()
            long_ticker = long_ticker_from_history_url(response.url)
            batches.append(
                parse_history_payload(
                    payload,
                    ticker=ticker,
                    long_ticker=long_ticker,
                    asset_type=asset_type,
                    currency=currency_from_long_ticker(long_ticker),
                )
            )

        page.on("response", on_response)
        await page.goto(_market_url(market, ticker), wait_until="domcontentloaded")
        await page.wait_for_timeout(wait_ms)
        if _is_rate_limited_page(await page.content()):
            raise RuntimeError("Cocos rate limit detectado (Cloudflare 1015)")
        await page.close()
        await browser.close()

    return [candle.to_dict() for candle in merge_candle_batches(batches)]


async def _main() -> None:
    parser = argparse.ArgumentParser(description="Captura velas historicas Cocos desde Chrome real")
    parser.add_argument("market", choices=["ACCIONES", "CEDEARS"])
    parser.add_argument("ticker")
    parser.add_argument("--cdp-url", default="http://127.0.0.1:9222")
    parser.add_argument("--wait-ms", type=int, default=12000)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    candles = await capture_history(
        market=args.market,
        ticker=args.ticker,
        cdp_url=args.cdp_url,
        wait_ms=args.wait_ms,
    )
    output = json.dumps(candles, ensure_ascii=False, indent=2)
    if args.output:
        args.output.write_text(output, encoding="utf-8")
    else:
        print(output)


if __name__ == "__main__":
    asyncio.run(_main())

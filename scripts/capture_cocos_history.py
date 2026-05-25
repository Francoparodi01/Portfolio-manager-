from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

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


async def _click_chart_range(page, label: str) -> None:
    aliases = {
        "5y": ("5y", "5Y", "5a", "5A", "5 años", "5 anos"),
        "1y": ("1y", "1Y", "1a", "1A", "1 año", "1 ano"),
        "all": ("Todos", "Todo", "MAX", "MÁX", "Max", "All"),
        "max": ("Todos", "Todo", "MAX", "MÁX", "Max", "All"),
    }
    candidates = aliases.get(str(label).strip().lower(), (label,))

    for frame in page.frames:
        if "charting_library" not in frame.url and "tv-chart" not in frame.url:
            continue
        for candidate in candidates:
            locator = frame.get_by_text(candidate, exact=True)
            if await locator.count() > 0:
                await locator.first.click(timeout=10_000, force=True)
                return
    raise RuntimeError(f"No se encontro el selector de rango del grafico: {label}")


async def capture_history_from_page(
    page,
    *,
    market: str,
    ticker: str,
    wait_ms: int,
    chart_range: str | None = None,
    range_wait_ms: int = 18000,
    interval: str = "1d",
) -> list[dict]:
    asset_type = asset_type_from_market(market)
    batches = []
    collect_enabled = {"value": chart_range is None}

    async def on_response(response):
        if not collect_enabled["value"]:
            return
        if "historic-data-extended" not in response.url:
            return
        try:
            payload = await response.json()
            long_ticker = long_ticker_from_history_url(response.url)
            if not long_ticker.upper().startswith(f"{ticker.upper()}-"):
                return
            batches.append(
                parse_history_payload(
                    payload,
                    ticker=ticker,
                    long_ticker=long_ticker,
                    asset_type=asset_type,
                    currency=currency_from_long_ticker(long_ticker),
                    interval=interval,
                )
            )
        except Exception:
            return

    page.on("response", on_response)
    await page.goto(_market_url(market, ticker), wait_until="domcontentloaded")

    if chart_range:
        await page.wait_for_timeout(wait_ms)
        batches.clear()
        collect_enabled["value"] = True
        await _click_chart_range(page, chart_range)
        await page.wait_for_timeout(range_wait_ms)
    else:
        await page.wait_for_timeout(wait_ms)

    if _is_rate_limited_page(await page.content()):
        raise RuntimeError("Cocos rate limit detectado (Cloudflare 1015)")

    return [candle.to_dict() for candle in merge_candle_batches(batches)]


async def capture_history(
    *,
    market: str,
    ticker: str,
    cdp_url: str,
    wait_ms: int,
    chart_range: str | None = None,
    range_wait_ms: int = 18000,
    interval: str = "1d",
) -> list[dict]:
    from playwright.async_api import async_playwright

    async with async_playwright() as playwright:
        browser = await playwright.chromium.connect_over_cdp(cdp_url)
        context = browser.contexts[0]
        page = await context.new_page()
        candles = await capture_history_from_page(
            page,
            market=market,
            ticker=ticker,
            wait_ms=wait_ms,
            chart_range=chart_range,
            range_wait_ms=range_wait_ms,
            interval=interval,
        )
        await page.close()
        await browser.close()

    return candles


async def _main() -> None:
    parser = argparse.ArgumentParser(
        description="Captura manual/excepcional de velas historicas Cocos desde Chrome real"
    )
    parser.add_argument("market", choices=["ACCIONES", "CEDEARS"])
    parser.add_argument("ticker")
    parser.add_argument("--cdp-url", default="http://127.0.0.1:9222")
    parser.add_argument("--wait-ms", type=int, default=12000)
    parser.add_argument("--chart-range", help="Rango visible del chart, por ejemplo 5y")
    parser.add_argument("--range-wait-ms", type=int, default=18000)
    parser.add_argument("--interval", default="1d", help="Intervalo a persistir. Default: 1d")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    candles = await capture_history(
        market=args.market,
        ticker=args.ticker,
        cdp_url=args.cdp_url,
        wait_ms=args.wait_ms,
        chart_range=args.chart_range,
        range_wait_ms=args.range_wait_ms,
        interval=args.interval,
    )
    output = json.dumps(candles, ensure_ascii=False, indent=2)
    if args.output:
        args.output.write_text(output, encoding="utf-8")
    else:
        print(output)


if __name__ == "__main__":
    asyncio.run(_main())

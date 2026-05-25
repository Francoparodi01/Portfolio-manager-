"""
scripts/validate_byma_prices.py

Read-only validation of latest Cocos market_prices against TradingView/BYMA.

The default mapping is intentionally simple:
    Cocos ticker -> BYMA:{ticker}

This keeps ACCIONES and CEDEARs comparable in ARS. Do not compare CEDEARs
against NYSE/NASDAQ here; that mixes ARS Cocos prices with USD source prices.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import asyncpg
from playwright.async_api import Browser, BrowserContext, Page, async_playwright

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.config import get_config


ART = ZoneInfo("America/Argentina/Buenos_Aires")
DEFAULT_TOLERANCE_PCT = 3.0
DEFAULT_DELAY_SECONDS = 1.5

SYMBOL_ALIASES = {
    "BA.C": "BYMA:BAC",
    "BRKB": "BYMA:BRKB",
}


@dataclass
class DbPrice:
    ticker: str
    asset_type: str
    last_price: float
    ts: datetime


@dataclass
class ValidationResult:
    ticker: str
    asset_type: str
    tv_symbol: str
    cocos_price: float
    cocos_ts: datetime
    tv_price: Optional[float] = None
    diff_pct: Optional[float] = None
    matched: bool = False
    selector_used: Optional[str] = None
    error: Optional[str] = None


def tv_symbol_for_ticker(ticker: str) -> str:
    ticker = ticker.upper().strip()
    return SYMBOL_ALIASES.get(ticker, f"BYMA:{ticker}")


def tv_symbol_url(tv_symbol: str) -> str:
    exchange, ticker = tv_symbol.split(":", 1)
    return f"https://www.tradingview.com/symbols/{exchange}-{ticker.replace('.', '-')}/"


def parse_price(raw: str) -> Optional[float]:
    if not raw:
        return None

    text = str(raw).strip()
    match = re.search(r"([-+]?\d[\d.,]*)\s*([KMB])?", text, flags=re.IGNORECASE)
    if not match:
        return None

    value = match.group(1)
    suffix = (match.group(2) or "").upper()
    if re.search(r"\d\.\d{3},", value):
        value = value.replace(".", "").replace(",", ".")
    elif "," in value and "." not in value:
        if re.search(r",\d{3}$", value):
            value = value.replace(",", "")
        elif re.search(r",\d{1,2}$", value):
            value = value.replace(",", ".")
        else:
            value = value.replace(",", "")
    else:
        value = value.replace(",", "")

    try:
        parsed = float(value)
    except ValueError:
        return None
    if suffix == "K":
        parsed *= 1_000
    elif suffix == "M":
        parsed *= 1_000_000
    elif suffix == "B":
        parsed *= 1_000_000_000
    return parsed if parsed > 0 else None


async def _try_selector(page: Page, selector: str, label: str) -> tuple[Optional[float], Optional[str]]:
    try:
        element = page.locator(selector).first
        if await element.is_visible(timeout=2_500):
            price = parse_price(await element.inner_text(timeout=2_500))
            if price:
                return price, label
    except Exception:
        return None, None
    return None, None


async def scrape_tv_price(page: Page, tv_symbol: str) -> tuple[Optional[float], Optional[str]]:
    try:
        await page.goto(tv_symbol_url(tv_symbol), wait_until="domcontentloaded", timeout=30_000)
        await page.wait_for_timeout(3_000)
    except Exception as exc:
        return None, f"goto-error: {exc}"

    selectors = [
        ('[data-field="last_price"]', "data-field"),
        ('.js-symbol-last', "js-symbol-last"),
        ('[class*="priceValue"]', "priceValue"),
        ('[class*="last-"]', "class-last"),
        ('[class*="price-"]', "class-price"),
        ('[data-name="legend-source-item"] [class*="value"]', "legend-value"),
    ]

    for selector, label in selectors:
        price, used = await _try_selector(page, selector, label)
        if price:
            return price, used

    try:
        title = await page.title()
        for number in re.findall(r"[\d][,.\d]*", title):
            price = parse_price(number)
            if price and not (1900 < price < 2100):
                return price, "title"
    except Exception:
        pass

    try:
        raw = await page.evaluate(
            """
            () => {
                const selectors = [
                    '[data-field="last_price"]',
                    '.js-symbol-last',
                    '[class*="priceValue"]',
                    '[class*="last-"]',
                    '[class*="price-"]'
                ];
                for (const selector of selectors) {
                    const el = document.querySelector(selector);
                    if (el && el.textContent) return el.textContent.trim();
                }
                const og = document.querySelector('meta[property="og:description"]');
                return og ? og.getAttribute('content') : null;
            }
            """
        )
        if raw:
            price = parse_price(str(raw))
            if price:
                return price, "js-eval"
    except Exception:
        pass

    return None, None


async def fetch_latest_prices(
    conn: asyncpg.Connection,
    asset_type: str,
    tickers: list[str],
    limit: Optional[int],
) -> list[DbPrice]:
    filters = ["last_price IS NOT NULL", "last_price > 0"]
    args: list[object] = []

    if asset_type != "ALL":
        args.append(asset_type)
        filters.append(f"asset_type = ${len(args)}")

    if tickers:
        args.append(tickers)
        filters.append(f"ticker = ANY(${len(args)}::text[])")

    limit_sql = ""
    if limit:
        args.append(limit)
        limit_sql = f"LIMIT ${len(args)}"

    query = f"""
        WITH latest AS (
            SELECT DISTINCT ON (ticker)
                ticker,
                COALESCE(asset_type, 'UNKNOWN') AS asset_type,
                last_price,
                ts
            FROM market_prices
            WHERE {' AND '.join(filters)}
            ORDER BY ticker, ts DESC
        )
        SELECT *
        FROM latest
        ORDER BY asset_type, ticker
        {limit_sql}
    """

    rows = await conn.fetch(query, *args)
    return [
        DbPrice(
            ticker=str(row["ticker"]).upper(),
            asset_type=str(row["asset_type"]).upper(),
            last_price=float(row["last_price"]),
            ts=row["ts"],
        )
        for row in rows
    ]


def _fmt_dt(value: datetime) -> str:
    dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return dt.astimezone(ART).strftime("%d/%m %H:%M")


def _print_results(results: list[ValidationResult], tolerance: float) -> None:
    widths = [8, 8, 14, 12, 12, 9, 22]

    def row(values: list[str]) -> str:
        return "  " + "  ".join(str(v)[:w].ljust(w) for v, w in zip(values, widths))

    print()
    print("=" * 96)
    print("  BYMA PRICE VALIDATION - COCOS DB vs TRADINGVIEW")
    print("=" * 96)
    print(row(["ticker", "type", "tv_symbol", "cocos", "tv", "diff", "status"]))
    print("  " + "-" * 94)

    ok = warn = error = 0
    for result in results:
        if result.error:
            status = f"ERROR: {result.error}"
            tv_price = "-"
            diff = "-"
            error += 1
        elif result.tv_price is None:
            status = "ERROR: TV no price"
            tv_price = "-"
            diff = "-"
            error += 1
        elif result.matched:
            status = f"MATCH [{result.selector_used}]"
            tv_price = f"{result.tv_price:,.2f}"
            diff = f"{result.diff_pct:.2f}%"
            ok += 1
        else:
            status = f"DIFF > {tolerance:.1f}%"
            tv_price = f"{result.tv_price:,.2f}"
            diff = f"{result.diff_pct:.2f}%"
            warn += 1

        print(
            row(
                [
                    result.ticker,
                    result.asset_type,
                    result.tv_symbol,
                    f"{result.cocos_price:,.2f}",
                    tv_price,
                    diff,
                    status,
                ]
            )
        )

    print("=" * 96)
    print(f"  Checked: {len(results)} | match: {ok} | divergence: {warn} | error: {error}")
    print("=" * 96)
    print()


async def run(
    asset_type: str,
    tickers: list[str],
    limit: Optional[int],
    tolerance: float,
    delay: float,
) -> None:
    cfg = get_config()
    dsn = cfg.database.url.replace("postgresql+asyncpg://", "postgresql://")

    conn = await asyncpg.connect(dsn)
    try:
        db_prices = await fetch_latest_prices(conn, asset_type, tickers, limit)
    finally:
        await conn.close()

    if not db_prices:
        print("No market_prices rows found for the selected filters.")
        return

    print()
    print("=" * 72)
    print("  BYMA PRICE VALIDATION")
    print("=" * 72)
    print(f"  Source DB  : market_prices latest per ticker")
    print(f"  TV mapping : BYMA:<ticker> by default")
    print(f"  Asset type : {asset_type}")
    print(f"  Tickers    : {len(db_prices)}")
    print(f"  Tolerance  : {tolerance:.1f}%")
    print("-" * 72)

    results: list[ValidationResult] = []
    async with async_playwright() as playwright:
        browser: Browser = await playwright.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context: BrowserContext = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="es-AR",
            extra_http_headers={"Accept-Language": "es-AR,es;q=0.9,en;q=0.8"},
        )

        async def block_heavy(route, request):
            if request.resource_type in {"image", "media", "font"}:
                await route.abort()
            else:
                await route.continue_()

        page = await context.new_page()
        await page.route("**/*", block_heavy)

        for index, db_price in enumerate(db_prices, 1):
            tv_symbol = tv_symbol_for_ticker(db_price.ticker)
            result = ValidationResult(
                ticker=db_price.ticker,
                asset_type=db_price.asset_type,
                tv_symbol=tv_symbol,
                cocos_price=db_price.last_price,
                cocos_ts=db_price.ts,
            )
            print(
                f"  [{index:02d}/{len(db_prices)}] {db_price.ticker:<8} "
                f"{db_price.asset_type:<7} Cocos {db_price.last_price:>12,.2f} "
                f"@ {_fmt_dt(db_price.ts)} -> {tv_symbol} ...",
                end="",
                flush=True,
            )

            try:
                tv_price, selector = await scrape_tv_price(page, tv_symbol)
                result.tv_price = tv_price
                result.selector_used = selector
            except Exception as exc:
                result.error = str(exc)[:80]
                results.append(result)
                print(f" ERROR {result.error}")
                continue

            if tv_price is None:
                result.error = "TV no price"
                results.append(result)
                print(" ERROR TV no price")
                continue

            result.diff_pct = abs(tv_price - result.cocos_price) / result.cocos_price * 100
            result.matched = result.diff_pct <= tolerance
            label = "MATCH" if result.matched else "DIFF"
            print(f" {label} TV {tv_price:>12,.2f} diff={result.diff_pct:.2f}% [{selector}]")
            results.append(result)

            if index < len(db_prices) and delay > 0:
                await asyncio.sleep(delay)

        await browser.close()

    _print_results(results, tolerance)


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only BYMA price validation against TradingView")
    parser.add_argument("--asset-type", choices=["ALL", "ACCION", "CEDEAR"], default="ALL")
    parser.add_argument("--ticker", action="append", default=[], help="Ticker to validate. Can be repeated.")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of latest tickers.")
    parser.add_argument("--tolerance", type=float, default=DEFAULT_TOLERANCE_PCT)
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY_SECONDS)
    args = parser.parse_args()

    asyncio.run(
        run(
            asset_type=args.asset_type,
            tickers=[ticker.upper().strip() for ticker in args.ticker if ticker.strip()],
            limit=args.limit,
            tolerance=args.tolerance,
            delay=args.delay,
        )
    )


if __name__ == "__main__":
    main()

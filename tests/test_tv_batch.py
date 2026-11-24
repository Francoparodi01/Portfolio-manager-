"""
tests/test_tv_batch.py

Valida precios de CEDEARs en Cocos DB contra TradingView BYMA.

IMPORTANTE:
- Este script corre SOLO CEDEARs.
- Compara precio Cocos en ARS vs TradingView BYMA en ARS.
- No usa exchanges de origen tipo NASDAQ/NYSE, porque eso sería USD y no comparable directo.
- Sirve para auditar si market_prices de Cocos está alineado con BYMA/TradingView.

Uso:
    docker compose run --rm scheduler python tests/test_tv_batch.py
    docker compose run --rm scheduler python tests/test_tv_batch.py --tolerance 5.0
    docker compose run --rm scheduler python tests/test_tv_batch.py --ticker NVDA
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import asyncpg
from dotenv import load_dotenv
from playwright.async_api import Browser, BrowserContext, Page, async_playwright

load_dotenv()


# ── CEDEAR ticker map ─────────────────────────────────────────────────────────
# Cocos ticker -> TradingView BYMA symbol.
#
# Nota:
# - Para CEDEARs queremos BYMA:<ticker>, no NASDAQ/NYSE.
# - Algunos tickers tienen nombres especiales entre Cocos y TradingView.
# - Si alguno falla en TradingView, ajustar este mapa puntual.
# ──────────────────────────────────────────────────────────────────────────────

TICKER_TV_MAP: dict[str, str] = {
    "AAPL":  "BYMA:AAPL",
    "ABNB":  "BYMA:ABNB",
    "AMD":   "BYMA:AMD",
    "AMZN":  "BYMA:AMZN",
    "ARKK":  "BYMA:ARKK",
    "BABA":  "BYMA:BABA",
    "CVX":   "BYMA:CVX",
    "DOW":   "BYMA:DOW",
    "EA":    "BYMA:EA",
    "GM":    "BYMA:GM",
    "GOOGL": "BYMA:GOOGL",
    "JMIA":  "BYMA:JMIA",
    "JNJ":   "BYMA:JNJ",
    "KO":    "BYMA:KO",
    "MELI":  "BYMA:MELI",
    "META":  "BYMA:META",
    "MSFT":  "BYMA:MSFT",
    "MU":    "BYMA:MU",
    "NFLX":  "BYMA:NFLX",
    "NU":    "BYMA:NU",
    "NVDA":  "BYMA:NVDA",
    "PBR":   "BYMA:PBR",
    "PFE":   "BYMA:PFE",
    "PLTR":  "BYMA:PLTR",
    "QCOM":  "BYMA:QCOM",
    "QQQ":   "BYMA:QQQ",
    "SPGI":  "BYMA:SPGI",
    "TEN":   "BYMA:TEN",
    "TSLA":  "BYMA:TSLA",
    "TSM":   "BYMA:TSM",
    "UBER":  "BYMA:UBER",
    "VALE":  "BYMA:VALE",
    "VIST":  "BYMA:VIST",
    "WMT":   "BYMA:WMT",

    # Especiales / aliases potenciales.
    # En Cocos puede aparecer BA.C, pero TradingView suele usar BAC.
    "BA.C":  "BYMA:BAC",

    # Berkshire puede variar según proveedor.
    # Si falla, probar BYMA:BRKB o BYMA:BRK.B manualmente.
    "BRKB":  "BYMA:BRKB",
}


DEFAULT_TOLERANCE_PCT = 3.0
DELAY_BETWEEN_TICKERS = 2.0


# ── Resultado por ticker ──────────────────────────────────────────────────────

@dataclass
class TickerResult:
    ticker: str
    tv_symbol: str
    cocos_price: Optional[float] = None
    tv_price: Optional[float] = None
    diff_pct: Optional[float] = None
    matched: Optional[bool] = None
    error: Optional[str] = None
    selector_used: Optional[str] = None


# ── Helpers TradingView ───────────────────────────────────────────────────────

def tv_symbol_url(tv_symbol: str) -> str:
    if ":" not in tv_symbol:
        raise ValueError(f"TradingView symbol inválido: {tv_symbol}. Esperado formato EXCHANGE:TICKER")

    exchange, ticker = tv_symbol.split(":", 1)
    ticker_for_url = ticker.replace(".", "-")
    return f"https://www.tradingview.com/symbols/{exchange}-{ticker_for_url}/"


def parse_price(raw: str) -> Optional[float]:
    if not raw:
        return None

    raw = str(raw).strip()

    # Dejar solo números y separadores comunes.
    match = re.search(r"[-+]?\d[\d.,]*", raw)
    if not match:
        return None

    raw = match.group(0)

    # Formato tipo 1.234,56
    if re.search(r"\d\.\d{3},", raw):
        raw = raw.replace(".", "").replace(",", ".")

    # Formato tipo 1234,56
    elif "," in raw and "." not in raw:
        if re.search(r",\d{3}$", raw):
            raw = raw.replace(",", "")
        elif re.search(r",\d{1,2}$", raw):
            raw = raw.replace(",", ".")
        else:
            raw = raw.replace(",", "")

    # Formato tipo 1,234.56
    else:
        raw = raw.replace(",", "")

    try:
        value = float(raw)
        if value <= 0:
            return None
        return value
    except ValueError:
        return None


async def _try_selector(page: Page, selector: str) -> tuple[Optional[float], Optional[str]]:
    try:
        el = page.locator(selector).first
        if await el.is_visible(timeout=2_500):
            text = await el.inner_text(timeout=2_500)
            price = parse_price(text)
            if price and price > 0:
                return price, selector
    except Exception:
        pass

    return None, None


async def scrape_tv_price(page: Page, tv_symbol: str) -> tuple[Optional[float], Optional[str]]:
    """
    Retorna (precio, selector_usado).

    TradingView cambia bastante el HTML, por eso se prueban varias estrategias.
    """
    url = tv_symbol_url(tv_symbol)

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        await page.wait_for_timeout(3_500)
    except Exception as e:
        return None, f"goto-error: {e}"

    # Estrategia 1: data-field.
    price, selector = await _try_selector(page, '[data-field="last_price"]')
    if price:
        return price, "data-field"

    # Estrategia 2: selectores comunes.
    candidates = [
        ('[class*="last-"]', "class*last"),
        ('.js-symbol-last', "js-symbol-last"),
        ('[class*="priceValue"]', "priceValue"),
        ('[class*="price-"]', "class*price"),
        ('[data-name="legend-source-item"] [class*="value"]', "legend-value"),
    ]

    for selector, label in candidates:
        price, _ = await _try_selector(page, selector)
        if price:
            return price, label

    # Estrategia 3: título de página.
    try:
        title = await page.title()
        numbers = re.findall(r"[\d][,.\d]*", title)
        for number in numbers:
            price = parse_price(number)
            if price and price > 0 and not (1900 < price < 2100):
                return price, "title"
    except Exception:
        pass

    # Estrategia 4: JS evaluate.
    try:
        result = await page.evaluate(
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
                    if (el && el.textContent) {
                        return el.textContent.trim();
                    }
                }

                const og = document.querySelector('meta[property="og:description"]');
                if (og) return og.getAttribute('content');

                return null;
            }
            """
        )

        if result:
            price = parse_price(str(result))
            if price and price > 0:
                return price, "js-eval"
    except Exception:
        pass

    return None, None


# ── DB ────────────────────────────────────────────────────────────────────────

def get_database_url() -> str:
    raw = (
        os.environ.get("DATABASE_URL")
        or os.environ.get("DATABASE_URL_CLOUD")
        or os.environ.get("DATABASE_URL_LOCAL")
        or ""
    )

    raw = raw.strip().replace("postgresql+asyncpg://", "postgresql://")

    if not raw:
        print("❌ No encontré DATABASE_URL, DATABASE_URL_CLOUD ni DATABASE_URL_LOCAL en .env")
        sys.exit(1)

    return raw


async def fetch_all_prices(
    conn: asyncpg.Connection,
    tickers: list[str],
) -> dict[str, tuple[float, datetime]]:
    """
    Trae el último precio de cada CEDEAR en market_prices.

    Se filtra asset_type = CEDEAR para evitar mezclar con ACCION.
    """
    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (ticker)
            ticker,
            last_price,
            ts
        FROM market_prices
        WHERE ticker = ANY($1)
          AND asset_type = 'CEDEAR'
        ORDER BY ticker, ts DESC
        """,
        tickers,
    )

    return {
        row["ticker"]: (float(row["last_price"]), row["ts"])
        for row in rows
    }


# ── Tabla resumen ─────────────────────────────────────────────────────────────

def print_summary(results: list[TickerResult], tolerance: float) -> None:
    col = {
        "ticker": 8,
        "tv": 16,
        "cocos": 14,
        "tv_p": 14,
        "diff": 10,
        "status": 24,
    }

    total_w = sum(col.values()) + len(col) * 3 + 1
    hr = "═" * total_w
    div = "─" * total_w

    def row(ticker: str, tv: str, cocos: str, tv_p: str, diff: str, status: str) -> str:
        return (
            f"  {ticker:<{col['ticker']}}  "
            f"{tv:<{col['tv']}}  "
            f"{cocos:>{col['cocos']}}  "
            f"{tv_p:>{col['tv_p']}}  "
            f"{diff:>{col['diff']}}  "
            f"{status}"
        )

    print()
    print(hr)
    print("  RESUMEN — VALIDACIÓN DE PRECIOS CEDEARs COCOS DB vs TRADINGVIEW BYMA")
    print(hr)
    print(row("TICKER", "TV SYMBOL", "COCOS ARS", "TV ARS", "DIFF %", "ESTADO"))
    print(div)

    match_count = 0
    fail_count = 0
    error_count = 0

    for result in results:
        if result.error:
            status = f"❌ ERROR: {result.error[:40]}"
            cocos_str = f"{result.cocos_price:,.2f}" if result.cocos_price else "—"
            tv_str = f"{result.tv_price:,.2f}" if result.tv_price else "—"
            diff_str = "—"
            error_count += 1

        elif result.tv_price is None:
            status = "❌ TV sin precio"
            cocos_str = f"{result.cocos_price:,.2f}" if result.cocos_price else "—"
            tv_str = "—"
            diff_str = "—"
            error_count += 1

        elif result.matched:
            status = f"✅ MATCH [{result.selector_used}]"
            cocos_str = f"{result.cocos_price:,.2f}"
            tv_str = f"{result.tv_price:,.2f}"
            diff_str = f"{result.diff_pct:.2f}%"
            match_count += 1

        else:
            status = f"⚠️ DIFF > {tolerance:.1f}%"
            cocos_str = f"{result.cocos_price:,.2f}"
            tv_str = f"{result.tv_price:,.2f}"
            diff_str = f"{result.diff_pct:.2f}%"
            fail_count += 1

        print(row(result.ticker, result.tv_symbol, cocos_str, tv_str, diff_str, status))

    print(hr)
    print(f"  CEDEARs testeados : {len(results)}")
    print(f"  ✅ Match           : {match_count}")
    print(f"  ⚠️ Divergencia     : {fail_count}")
    print(f"  ❌ Error           : {error_count}")
    print(hr)
    print()


# ── Runner principal ──────────────────────────────────────────────────────────

async def run(tolerance: float, only_ticker: Optional[str]) -> None:
    if only_ticker:
        ticker = only_ticker.upper().strip()
        if ticker not in TICKER_TV_MAP:
            print(f"❌ Ticker no está en TICKER_TV_MAP: {ticker}")
            print(f"Disponibles: {', '.join(sorted(TICKER_TV_MAP.keys()))}")
            sys.exit(1)

        tickers = [ticker]
    else:
        tickers = sorted(TICKER_TV_MAP.keys())

    print()
    print("═" * 72)
    print("  BATCH TEST — CEDEARs TRADINGVIEW BYMA vs COCOS DB")
    print("═" * 72)
    print(f"  Tickers    : {len(tickers)}")
    print("  Modo       : Solo CEDEARs — comparación directa ARS contra BYMA")
    print(f"  Tolerancia : {tolerance:.1f}%")
    print("─" * 72)

    db_url = get_database_url()

    try:
        conn = await asyncpg.connect(db_url)
        db_prices = await fetch_all_prices(conn, tickers)
        await conn.close()
    except Exception as e:
        print(f"❌ Error conectando a la base: {e}")
        sys.exit(1)

    print(f"  DB: {len(db_prices)}/{len(tickers)} tickers con precio CEDEAR")
    print()

    results: list[TickerResult] = []

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
            extra_http_headers={
                "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )

        async def block_heavy(route, request):
            if request.resource_type in {"image", "media", "font"}:
                await route.abort()
            else:
                await route.continue_()

        page: Page = await context.new_page()
        await page.route("**/*", block_heavy)

        for index, ticker in enumerate(tickers, 1):
            tv_symbol = TICKER_TV_MAP[ticker]
            result = TickerResult(ticker=ticker, tv_symbol=tv_symbol)

            if ticker in db_prices:
                result.cocos_price, _ = db_prices[ticker]
            else:
                result.error = "sin precio CEDEAR en DB"
                results.append(result)
                print(f"  [{index:02d}/{len(tickers)}] {ticker:<6} ❌ sin precio CEDEAR en DB")
                continue

            print(
                f"  [{index:02d}/{len(tickers)}] {ticker:<6} scrapeando {tv_symbol} ...",
                end="",
                flush=True,
            )

            try:
                tv_price, selector = await scrape_tv_price(page, tv_symbol)
                result.tv_price = tv_price
                result.selector_used = selector
            except Exception as e:
                result.error = str(e)[:80]
                results.append(result)
                print(f" ❌ {result.error}")
                continue

            if tv_price is None:
                result.error = "TV sin precio"
                results.append(result)
                print(" ❌ sin precio en TV")
                continue

            diff_pct = abs(tv_price - result.cocos_price) / result.cocos_price * 100
            result.diff_pct = diff_pct
            result.matched = diff_pct <= tolerance

            icon = "✅" if result.matched else "⚠️ "
            print(
                f" {icon} Cocos {result.cocos_price:>10,.2f} → "
                f"TV {tv_price:>10,.2f} diff={diff_pct:.2f}% [{selector}]"
            )

            results.append(result)

            if index < len(tickers):
                await asyncio.sleep(DELAY_BETWEEN_TICKERS)

        await browser.close()

    print_summary(results, tolerance)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Valida precios de CEDEARs Cocos DB contra TradingView BYMA"
    )

    parser.add_argument(
        "--tolerance",
        type=float,
        default=DEFAULT_TOLERANCE_PCT,
        help=f"Tolerancia máxima en %% entre Cocos y TradingView. Default: {DEFAULT_TOLERANCE_PCT}",
    )

    parser.add_argument(
        "--ticker",
        type=str,
        default=None,
        help="Correr un solo ticker. Ejemplo: --ticker NVDA",
    )

    args = parser.parse_args()

    asyncio.run(
        run(
            tolerance=args.tolerance,
            only_ticker=args.ticker,
        )
    )


if __name__ == "__main__":
    main()

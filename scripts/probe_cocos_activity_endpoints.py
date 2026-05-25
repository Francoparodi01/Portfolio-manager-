from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path
from pprint import pformat

from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)

from src.collector.cocos_scraper import CocosCapitalScraper
from src.core.config import get_config


KEYWORDS = (
    "activity",
    "activit",
    "movement",
    "movim",
    "transaction",
    "orden",
    "order",
    "operac",
    "trade",
    "fill",
    "account",
    "portfolio",
)


def _interesting_api_url(url: str) -> bool:
    lower = url.lower()
    return (
        "api.cocos.capital" in lower
        and any(keyword in lower for keyword in KEYWORDS)
    )


async def _main() -> None:
    parser = argparse.ArgumentParser(
        description="Sonda read-only de endpoints Cocos relacionados con movimientos/fills"
    )
    parser.add_argument(
        "--paths",
        nargs="*",
        default=[
            "/capital-portfolio",
            "/activity",
            "/activities",
            "/movements",
            "/movimientos",
            "/transactions",
            "/operaciones",
            "/orders",
            "/ordenes",
            "/account",
            "/cuenta",
        ],
    )
    parser.add_argument("--wait-ms", type=int, default=5000)
    args = parser.parse_args()

    cfg = get_config()
    seen: set[str] = set()

    async with CocosCapitalScraper(cfg.scraper) as scraper:
        await scraper.login()
        page = scraper._page
        if page is None:
            raise RuntimeError("Cocos page no inicializada")

        async def on_response(response):
            url = response.url
            lower = url.lower()
            if not any(keyword in lower for keyword in KEYWORDS):
                return
            if url in seen:
                return
            seen.add(url)
            try:
                content_type = response.headers.get("content-type", "")
            except Exception:
                content_type = ""
            print(f"{response.status} {content_type[:40]:<40} {url}")
            if response.status < 400 and _interesting_api_url(url):
                try:
                    payload = await response.json()
                    print(pformat(payload, width=120)[:2000])
                except Exception:
                    pass

        page.on("response", on_response)

        for path in args.paths:
            url = f"https://app.cocos.capital{path}"
            print(f"\nNAV {url}")
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=45_000)
                await page.wait_for_timeout(args.wait_ms)
                title = await page.title()
                body = await page.inner_text("body", timeout=5_000)
                compact = " ".join(body.split())[:500]
                print(f"TITLE {title}")
                print(f"BODY  {compact}")
            except Exception as exc:
                print(f"ERROR {exc}")


if __name__ == "__main__":
    asyncio.run(_main())

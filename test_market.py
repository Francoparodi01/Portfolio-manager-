import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.core.config import get_config
from src.collector.cocos_scraper import CocosCapitalScraper
from src.collector.db import PortfolioDatabase

async def main():
    cfg = get_config()
    db = PortfolioDatabase(cfg.database.url)
    await db.connect()
    try:
        async with CocosCapitalScraper(cfg.scraper) as scraper:
            await scraper.login()
            acciones = await scraper.scrape_market("ACCIONES")
            cedears = await scraper.scrape_market("CEDEARS")
            print("acciones=", len(acciones), "cedears=", len(cedears))
            await db.save_market_prices(acciones + cedears)
    finally:
        await db.close()

asyncio.run(main())
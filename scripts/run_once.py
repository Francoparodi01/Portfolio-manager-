"""
scripts/run_once.py
Ejecucion manual de un scrape. Util para testing y depuracion.

Uso:
    python scripts/run_once.py                    # solo portfolio
    python scripts/run_once.py --full             # portfolio + mercado
    python scripts/run_once.py --no-db            # sin guardar en DB
    python scripts/run_once.py --json output.json # guardar snapshot en archivo
"""
import argparse
import asyncio
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.config import get_config
from src.core.logger import get_logger
from src.collector.cocos_scraper import CocosCapitalScraper
from src.collector.db import PortfolioDatabase
from src.collector.notifier import TelegramNotifier

logger = get_logger(__name__)


async def main(full: bool = False, no_db: bool = False, json_output: str = None):
    cfg = get_config()

    errors = cfg.scraper.validate()
    if errors:
        logger.error(f"Configuracion invalida: {errors}")
        sys.exit(1)

    notifier = TelegramNotifier(cfg.scraper.telegram_bot_token, cfg.scraper.telegram_chat_id)
    db = PortfolioDatabase(cfg.database.url) if not no_db else None

    try:
        if db:
            await db.connect()

        async with CocosCapitalScraper(cfg.scraper) as scraper:
            logger.info("Iniciando scrape manual...")
            await scraper.login()

            snapshot = await scraper.scrape_portfolio()

            logger.info(f"Portfolio scrapeado:")
            logger.info(f"  Total ARS:   ${snapshot.total_value_ars:,.2f}")
            logger.info(f"  Cash ARS:    ${snapshot.cash_ars:,.2f}")
            logger.info(f"  Posiciones:  {len(snapshot.positions)}")
            logger.info(f"  Confianza:   {snapshot.confidence_score:.2%}")

            for p in snapshot.positions:
                logger.info(
                    f"  {p.ticker:8s} x{p.quantity:8.2f} "
                    f"@ ${p.current_price:>12,.2f} "
                    f"= ${p.market_value:>14,.2f}"
                )

            if db:
                sid = await db.save_snapshot(snapshot)
                logger.info(f"Snapshot guardado: {sid}")

            if json_output:
                with open(json_output, "w") as f:
                    json.dump(snapshot.to_dict(), f, indent=2, ensure_ascii=False)
                logger.info(f"JSON guardado en: {json_output}")

            if full:
                logger.info("Scrapeando mercado...")
                acciones = await scraper.scrape_market("ACCIONES")
                cedears = await scraper.scrape_market("CEDEARS")
                logger.info(f"  Acciones: {len(acciones)}")
                logger.info(f"  CEDEARs:  {len(cedears)}")
                if db:
                    await db.save_market_prices(acciones + cedears)

    except Exception as e:
        logger.error(f"Error en run manual: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if db:
            await db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape manual de Cocos Capital")
    parser.add_argument("--full", action="store_true", help="Incluir scrape de mercado")
    parser.add_argument("--no-db", action="store_true", help="No guardar en base de datos")
    parser.add_argument("--json", dest="json_output", metavar="FILE", help="Guardar snapshot en archivo JSON")
    args = parser.parse_args()

    asyncio.run(main(full=args.full, no_db=args.no_db, json_output=args.json_output))

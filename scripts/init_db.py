"""
scripts/init_db.py
Inicializa el schema de TimescaleDB.
Ejecutar UNA vez antes del primer run.

Uso:
    python scripts/init_db.py
"""
import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.config import get_config
from src.core.logger import get_logger
from src.collector.db import PortfolioDatabase

logger = get_logger(__name__)


async def main():
    cfg = get_config()

    errors = cfg.scraper.validate()
    if errors:
        logger.warning(f"Config warnings: {errors}")

    logger.info(f"Conectando a: {cfg.database.url[:50]}...")
    db = PortfolioDatabase(cfg.database.url)

    try:
        await db.connect()
        await db.init_schema()
        logger.info("Schema inicializado correctamente")
    except Exception as e:
        logger.error(f"Error inicializando schema: {e}")
        sys.exit(1)
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())

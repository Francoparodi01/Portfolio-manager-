"""
Recalcula outcomes históricos desde market_candles canónicas.

Uso:
  python scripts/recompute_outcomes.py
  python scripts/recompute_outcomes.py --days 180
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.collector.db import PortfolioDatabase
from src.core.config import get_config
from src.core.logger import get_logger

logger = get_logger(__name__)


async def main(lookback_days: int | None = None) -> None:
    cfg = get_config()
    db = PortfolioDatabase(cfg.database.url)
    scope = "todo el histórico" if lookback_days is None else f"últimos {lookback_days} días"

    try:
        await db.connect()
        logger.info("Recalculando outcomes sobre %s...", scope)
        updated = await db.recompute_outcomes(lookback_days=lookback_days)
        logger.info("Outcomes recalculados: %s", updated)
        print(f"✅ {updated} outcomes recalculados ({scope})")
    except Exception as exc:
        logger.error("Error recomputando outcomes: %s", exc, exc_info=True)
        print(f"❌ Error: {exc}")
        raise
    finally:
        await db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Recalcula outcomes históricos canónicos")
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="Limitar el recálculo a los últimos N días; por defecto procesa todo el histórico",
    )
    args = parser.parse_args()
    asyncio.run(main(lookback_days=args.days))

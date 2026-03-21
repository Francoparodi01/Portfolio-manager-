"""
scripts/update_outcomes.py
──────────────────────────
Rellena outcome_5d / outcome_10d / outcome_20d / was_correct
para todas las decisiones guardadas donde ya pasaron ≥5 días.

Correr:
  - Manualmente:   python scripts/update_outcomes.py
  - Via cron/APScheduler: diario a cierre de mercado (21:00 ART)
  - Via Telegram:  /update_outcomes (si lo agregás al bot)

Uso:
  python scripts/update_outcomes.py
  python scripts/update_outcomes.py --days 60   # solo últimos 60 días
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.config import get_config
from src.core.logger import get_logger
from src.collector.db import PortfolioDatabase

logger = get_logger(__name__)


async def main(lookback_days: int = 30) -> None:
    cfg = get_config()
    db  = PortfolioDatabase(cfg.database.url)

    try:
        await db.connect()
        logger.info(f"Actualizando outcomes (últimos {lookback_days} días)...")
        updated = await db.update_outcomes(lookback_days=lookback_days)
        logger.info(f"✅ {updated} decisiones actualizadas")
        print(f"✅ {updated} outcomes actualizados")
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        print(f"❌ Error: {e}")
        sys.exit(1)
    finally:
        await db.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Actualiza outcomes de decisiones pasadas")
    p.add_argument("--days", type=int, default=30,
                   help="Lookback en días (default: 30)")
    args = p.parse_args()
    asyncio.run(main(lookback_days=args.days))
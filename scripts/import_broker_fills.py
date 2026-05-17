from __future__ import annotations

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.collector.broker_fills import load_broker_fills_csv
from src.collector.db import PortfolioDatabase
from src.core.config import get_config


async def main(path: str, *, source: str, max_age_days: int) -> None:
    cfg = get_config()
    fills = load_broker_fills_csv(path, source=source)
    db = PortfolioDatabase(cfg.database.url)

    try:
        await db.connect()
        saved = await db.save_broker_fills(fills)
        reconciled = await db.reconcile_broker_fills(max_age_days=max_age_days)
    finally:
        await db.close()

    print(f"broker fills guardados: {saved}")
    print(f"broker fills reconciliados: {reconciled}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Importa fills reales del broker desde CSV y reconcilia decision_log"
    )
    parser.add_argument("path", help="CSV con fills ejecutados")
    parser.add_argument(
        "--source",
        default="manual_import",
        help="Etiqueta de origen para los fills importados",
    )
    parser.add_argument(
        "--max-age-days",
        type=int,
        default=3,
        help="Ventana maxima entre decision aprobada y fill",
    )
    args = parser.parse_args()
    asyncio.run(
        main(
            args.path,
            source=args.source,
            max_age_days=args.max_age_days,
        )
    )

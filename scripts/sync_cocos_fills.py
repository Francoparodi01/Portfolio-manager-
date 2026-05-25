from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)

from src.collector.cocos_scraper import CocosCapitalScraper
from src.collector.db import PortfolioDatabase
from src.collector.broker_movements import broker_fills_from_movements
from src.core.config import get_config


async def _main() -> None:
    parser = argparse.ArgumentParser(
        description="Sincroniza fills/ordenes ejecutadas desde Cocos y reconcilia decision_log"
    )
    parser.add_argument("--no-db", action="store_true", help="Solo muestra conteo, no guarda")
    parser.add_argument("--wait-ms", type=int, default=4000, help="Espera por vista sondeada")
    parser.add_argument("--max-age-days", type=int, default=3, help="Ventana de reconciliacion")
    parser.add_argument(
        "--no-materialize-manual",
        action="store_true",
        help="No crear decision_log EXECUTED_MANUAL para fills sin plan aprobado",
    )
    args = parser.parse_args()

    cfg = get_config()
    db = None if args.no_db else PortfolioDatabase(cfg.database.url)

    if db:
        await db.connect()

    try:
        async with CocosCapitalScraper(cfg.scraper) as scraper:
            movements = await scraper.scrape_portfolio_movements(wait_ms=args.wait_ms)

        fills = broker_fills_from_movements(movements)

        print(f"fills derivados de movimientos: {len(fills)}")
        for fill in fills[:20]:
            print(
                f"- {fill.executed_at.isoformat()} {fill.side} "
                f"{fill.ticker} x{fill.quantity:g} @ {fill.avg_fill_price:g} "
                f"({fill.external_fill_id})"
            )
        print(f"movimientos detectados: {len(movements)}")
        for movement in movements[:20]:
            qty = "" if movement.quantity is None else f" x{movement.quantity:g}"
            price = "" if movement.price is None else f" @ {movement.price:g}"
            print(
                f"- {movement.executed_at.date()} {movement.movement_type} "
                f"{movement.ticker or movement.currency}{qty}{price} "
                f"amount={movement.amount}"
            )

        if db:
            saved_movements = await db.save_broker_movements(movements)
            saved = await db.save_broker_fills(fills)
            reconciled = await db.reconcile_broker_fills(max_age_days=args.max_age_days)
            materialized = 0
            if not args.no_materialize_manual:
                materialized = await db.materialize_unmatched_broker_fills()
            print(f"broker movements guardados: {saved_movements}")
            print(f"broker fills guardados: {saved}")
            print(f"broker fills reconciliados: {reconciled}")
            print(f"broker fills materializados manuales: {materialized}")
    finally:
        if db:
            await db.close()


if __name__ == "__main__":
    asyncio.run(_main())

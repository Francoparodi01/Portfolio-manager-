from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.collector.data.models import AssetType, Currency, MarketCandle
from src.collector.db import PortfolioDatabase
from src.core.config import get_config


def _load_candles(path: Path) -> list[MarketCandle]:
    rows = json.loads(path.read_text(encoding="utf-8"))
    return [
        MarketCandle(
            ticker=row["ticker"],
            long_ticker=row["long_ticker"],
            asset_type=AssetType(row["asset_type"]),
            currency=Currency(row["currency"]),
            venue=row["venue"],
            interval=row["interval"],
            ts=datetime.fromisoformat(row["ts"]),
            open_price=float(row["open_price"]),
            high_price=float(row["high_price"]),
            low_price=float(row["low_price"]),
            close_price=float(row["close_price"]),
            volume=float(row["volume"]),
            source=row.get("source", "COCOS"),
        )
        for row in rows
    ]


async def _main() -> None:
    parser = argparse.ArgumentParser(description="Importa velas Cocos JSON a market_candles")
    parser.add_argument("path", type=Path)
    args = parser.parse_args()

    db = PortfolioDatabase(get_config().database.url)
    await db.connect()
    try:
        saved = await db.save_market_candles(_load_candles(args.path))
        print(f"{saved} velas importadas")
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(_main())

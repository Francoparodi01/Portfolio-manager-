from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import asyncpg


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.core.config import get_config


async def main() -> None:
    cfg = get_config()
    dsn = cfg.database.url.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_market_candles_ticker_interval_ts
            ON market_candles(ticker, interval, ts DESC)
            """
        )
    finally:
        await conn.close()
    print("idx_market_candles_ticker_interval_ts OK")


if __name__ == "__main__":
    asyncio.run(main())

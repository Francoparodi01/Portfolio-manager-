"""
scripts/backfill_prices.py
Rellena price_at_decision para decisiones que no tienen precio de entrada.
Usa yfinance para obtener el precio histórico en la fecha de la decisión.

Uso:
    python scripts/backfill_prices.py
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.config import get_config
from src.core.logger import get_logger
from src.collector.db import PortfolioDatabase

logger = get_logger(__name__)


async def main():
    import yfinance as yf

    cfg = get_config()
    db  = PortfolioDatabase(cfg.database.url)
    await db.connect()
    pool = await db.get_pool()

    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, ticker, decided_at
            FROM decision_log
            WHERE price_at_decision IS NULL AND decision != 'HOLD'
            ORDER BY decided_at ASC
        """)

    print(f"{len(rows)} decisiones sin precio de entrada")

    updated = 0
    for row in rows:
        ticker = str(row["ticker"]).upper()
        date   = row["decided_at"].strftime("%Y-%m-%d")
        try:
            df = yf.download(
                ticker, start=date, period="5d",
                progress=False, auto_adjust=True
            )["Close"].squeeze()

            if df is None or df.empty:
                print(f"  {ticker} {date}: sin datos en yfinance")
                continue

            price = float(df.iloc[0])
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE decision_log SET price_at_decision = $1 WHERE id = $2",
                    price, row["id"]
                )
            print(f"  {ticker} {date}: ${price:.2f}")
            updated += 1
        except Exception as e:
            print(f"  {ticker} {date}: error — {e}")

    print(f"\nBackfill completado: {updated}/{len(rows)} actualizados")
    await db.close()


if __name__ == "__main__":
    asyncio.run(main())
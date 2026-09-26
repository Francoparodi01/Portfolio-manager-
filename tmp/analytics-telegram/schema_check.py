import asyncio
import json
import asyncpg
from src.core.config import get_config


async def main():
    cfg = get_config()
    owner = int(cfg.scraper.telegram_chat_id)
    conn = await asyncpg.connect(cfg.database.url.replace('postgresql+asyncpg://', 'postgresql://'))
    try:
        async with conn.transaction(readonly=True, isolation='repeatable_read'):
            tables = ['decision_log', 'broker_fills', 'plan_execution_attributions', 'portfolio_snapshots', 'market_candles', 'radar_discovery_snapshots', 'radar_exploratory_candidates']
            rows = await conn.fetch('SELECT table_name, column_name FROM information_schema.columns WHERE table_schema=$1 AND table_name=ANY($2::text[]) ORDER BY table_name, ordinal_position', 'public', tables)
            print(json.dumps([dict(r) for r in rows]))
            rows = await conn.fetch("SELECT COALESCE(source,layers->>'source') AS source, status, metric_scope, COUNT(*) AS n FROM decision_log WHERE owner_chat_id=$1 AND decided_at >= NOW()-INTERVAL '180 days' GROUP BY 1,2,3 ORDER BY 1,2,3", owner)
            print(json.dumps([dict(r) for r in rows]))
    finally:
        await conn.close()

asyncio.run(main())

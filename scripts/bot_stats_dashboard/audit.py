"""Repeat the owner-only raw audit without printing owner IDs or credentials."""
import argparse
import asyncio
from datetime import timedelta
import json
from pathlib import Path
import os
import asyncpg
from dotenv import load_dotenv
from server import stats, owner_chat_id


async def audit():
    owner=int(owner_chat_id())
    result=await stats(owner)
    dsn=os.environ['DATABASE_URL'].replace('postgresql+asyncpg://','postgresql://')
    db=await asyncpg.connect(dsn,server_settings={'default_transaction_read_only':'on','statement_timeout':'15000'})
    try:
        async with db.transaction(readonly=True,isolation='repeatable_read'):
            legacy=bool(result['legacy_owner_inferred'])
            profile=await db.fetch("""
                SELECT m.source,m.currency,m.venue,m.interval,count(*) AS n,
                       min(m.ts) AS first,max(m.ts) AS last,
                       array_agg(DISTINCT extract(hour FROM m.ts AT TIME ZONE 'UTC')) AS utc_hours
                FROM market_candles m
                WHERE m.ticker IN (SELECT DISTINCT i.ticker
                    FROM order_intents i JOIN execution_plans p ON p.id=i.execution_plan_id
                    WHERE p.owner_chat_id=$1 OR ($2::boolean AND p.owner_chat_id IS NULL))
                GROUP BY m.source,m.currency,m.venue,m.interval ORDER BY n DESC
            """,owner,legacy)
            bot=await db.fetchval("""SELECT count(*) FROM execution_plans
                WHERE owner_chat_id=$1 OR ($2::boolean AND owner_chat_id IS NULL)""",owner,legacy)
            result['independent_audit']={
                'all_owner_plan_rows':bot,
                'market_scope':'Tickers in explicit plus strictly verified legacy plans, all history.',
                'owner_scope':result['owner_scope'],
                'market_profile':[dict(x) for x in profile],
                'transaction_read_only':await db.fetchval("SELECT current_setting('transaction_read_only')"),
                'audited_at':str(await db.fetchval('SELECT NOW()'))}
    finally:
        await db.close()
    return result


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--env-file',type=Path,required=True)
    parser.add_argument('--output',type=Path,default=Path('output/bot-stats/raw-audit.json'))
    args=parser.parse_args()
    load_dotenv(args.env_file,override=False)
    data=asyncio.run(audit())
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(data,ensure_ascii=False,default=str,allow_nan=False,indent=2),encoding='utf-8')
    print(json.dumps({'output':str(args.output),'plans':data['plans'],'counts':data['sql_counts'],'readonly':data['readonly']},default=str))

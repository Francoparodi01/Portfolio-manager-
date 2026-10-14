"""Source reconciliation. Immutable recovery differs from mutable current views."""
from datetime import datetime, timezone
from .delivery import append_event
from .hooks import event


async def recover_observations(conn, *, owner, start, limit=100):
    result={}
    async with conn.transaction():
        await conn.execute("SELECT set_config('qeef.owner',$1,true)",str(owner))
        for table,kind,key,clock in (
            ('portfolio_snapshots','PORTFOLIO','snapshot_id','scraped_at'),
            ('market_candle_observations','MARKET','observation_id','available_at')):
            if not await conn.fetchval('SELECT to_regclass($1)', 'public.'+table):
                result[kind]={'status':'SOURCE_UNAVAILABLE'};continue
            # Identifiers above are constants, never caller-controlled SQL.
            rows=await conn.fetch(f'''SELECT s.* FROM {table} s WHERE s.owner_chat_id=$1 AND s.{clock}>=$2
                AND NOT EXISTS(SELECT 1 FROM qeef_events e WHERE e.owner_chat_id=$1 AND e.kind=$3 AND e.source_id=s.{key}::text)
                ORDER BY s.{clock},s.{key} LIMIT $4''',owner,start,kind,limit)
            for row in rows:
                p=dict(row)
                if kind=='PORTFOLIO':
                    p['positions']=[dict(r) for r in await conn.fetch('SELECT * FROM positions WHERE snapshot_id=$1',row[key])]
                    p.pop('raw_html',None)
                await append_event(conn,event(kind,owner,row[key],p,
                    effective_at=row['scraped_at' if kind=='PORTFOLIO' else 'candle_timestamp'],
                    available_at=row[clock],provenance='RECOVERED_IMMUTABLE'))
            result[kind]={'recovered':len(rows),'batch_limit':limit}
        # broker_fills is mutable. Reobserve current versions with NOW availability;
        # never backdate a fee/link correction to its execution time.
        if await conn.fetchval("SELECT to_regclass('public.broker_fills')"):
            rows=await conn.fetch('''SELECT f.id,f.source,f.external_fill_id,f.executed_at,f.executed_at_precision,f.executed_at_source,
                f.ticker,f.side,f.quantity,f.avg_fill_price,f.gross_amount_ars,f.fees_ars,f.decision_log_id,f.owner_chat_id,
                (SELECT jsonb_agg(jsonb_build_object('plan_id',i.execution_plan_id,'order_intent_id',i.id) ORDER BY i.id)
                 FROM order_intents i JOIN execution_plans p ON p.id=i.execution_plan_id
                 WHERE i.decision_log_id=f.decision_log_id AND p.owner_chat_id=$1) AS recorded_plan_links
                FROM broker_fills f WHERE f.owner_chat_id=$1 AND f.created_at >=$2 ORDER BY f.id LIMIT 5001''',owner,start)
            recovered=0
            for row in rows[:5000]:
                item=event('FILL',owner,str(row['source'])+':'+str(row['external_fill_id']),dict(row),
                    effective_at=row['executed_at'],available_at=datetime.now(timezone.utc),provenance='RECOVERED_CURRENT')
                if not await conn.fetchval('SELECT 1 FROM qeef_events WHERE owner_chat_id=$1 AND event_id=$2',owner,item.event_id):
                    await append_event(conn,item);recovered+=1
            result['FILL']={'recovered':recovered,'scanned':min(len(rows),5000),'truncated':len(rows)>5000}
        result['MOVEMENTS']={'status':'UNRECOVERABLE_FROM_UNSCOPED_SOURCE','remediation':'REOBSERVE_OWNER_SESSION',
            'reason':'Existing broker_movements has no owner; absence cannot certify complete flows.'}
    return result

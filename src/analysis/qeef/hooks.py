"""Fail-open capture hooks. Disabled means no serialization, I/O or DB access."""
from __future__ import annotations

import asyncio
from datetime import datetime,timezone
import logging
import os
from pathlib import Path
from src.decision_lab.capture import clean
from src.decision_lab.models import canonical,digest
from .prospective import CaptureEvent
from .delivery import Outbox

log=logging.getLogger(__name__)


def enabled():
    return os.getenv('QEEF_CAPTURE_ENABLED','false').lower()=='true'


def settings():
    owner=int(os.environ['TELEGRAM_CHAT_ID'])
    start=datetime.fromisoformat(os.environ['QEEF_CAPTURE_START_AT'])
    path=Path(os.environ['QEEF_SPOOL_PATH'])
    if not owner or start.tzinfo is None or not path.is_absolute():
        raise ValueError('explicit owner, activation cutoff and absolute durable spool required')
    return owner,start,path


async def capture(factory):
    if not enabled():
        return {'status':'DISABLED'}
    try:
        owner,start,path=settings()
        events=factory()
        if asyncio.iscoroutine(events): events=await asyncio.wait_for(events,timeout=1)
        events=list(events)
        if any(e.owner!=owner for e in events): raise ValueError('owner mismatch')
        accepted=[e for e in events if e.available_at>=start and (e.kind!='PLAN' or e.effective_at>=start)]
        def persist():
            outbox=Outbox(path)
            return sum(outbox.append(e) for e in accepted)
        # SQLite has bounded lock wait. A durable append, not fire-and-forget:
        # no successful receipt before filesystem commit.
        inserted=await asyncio.wait_for(asyncio.to_thread(persist),timeout=1)
        return {'status':'CAPTURED','expected':len(accepted),'inserted':inserted}
    except Exception as exc:
        log.error('qeef_capture_failed code=%s recovery=canonical_source_scan',type(exc).__name__)
        return {'status':'FAILED','error_code':type(exc).__name__,'recovery':'CANONICAL_SOURCE_SCAN_REQUIRED'}


def event(kind,owner,source_id,payload,*,effective_at,available_at=None,provenance='RECORDED',parent_id=None):
    value=clean(payload)
    text=canonical(value)
    return CaptureEvent(owner=owner,kind=kind,source_id=str(source_id),effective_at=effective_at,
        available_at=available_at or datetime.now(timezone.utc),source_version=digest(value),
        payload_json=text,provenance=provenance,parent_id=parent_id)


async def capture_plan(conn,*,owner,plan_id,capture_hash,bundle=None):
    async def factory():
        import json
        row=await conn.fetchrow('SELECT payload FROM decision_lab_plan_captures WHERE owner_chat_id=$1 AND capture_hash=$2 AND plan_id=$3::uuid',owner,capture_hash,str(plan_id))
        if not row: raise ValueError('canonical full-context capture missing')
        p=json.loads(row['payload']) if isinstance(row['payload'],str) else dict(row['payload'])
        p['source_references']=['decision_lab_plan_captures:'+capture_hash]
        p['decision_links']=[dict(r) for r in await conn.fetch('SELECT id,decision_log_id FROM order_intents WHERE execution_plan_id=$1::uuid',str(plan_id))]
        if bundle is not None: p['qeef_bundle']=bundle.model_dump(mode='json') if hasattr(bundle,'model_dump') else bundle
        return [event('PLAN',owner,plan_id,p,effective_at=p['decision_at'],available_at=p['captured_at'])]
    return await capture(factory)


async def capture_snapshot(snapshot):
    return await capture(lambda:[event('PORTFOLIO',snapshot.owner_chat_id,snapshot.snapshot_id,
        snapshot.to_dict(),effective_at=snapshot.scraped_at,available_at=snapshot.scraped_at)])


async def capture_fills(conn,*,owner,external_ids):
    async def factory():
        rows=await conn.fetch('''SELECT f.id,f.source,f.external_fill_id,f.executed_at,f.executed_at_precision,f.executed_at_source,
            f.ticker,f.side,f.quantity,f.avg_fill_price,f.gross_amount_ars,f.fees_ars,f.decision_log_id,f.owner_chat_id,
            (SELECT jsonb_agg(jsonb_build_object('plan_id',i.execution_plan_id,'order_intent_id',i.id) ORDER BY i.id)
             FROM order_intents i JOIN execution_plans p ON p.id=i.execution_plan_id
             WHERE i.decision_log_id=f.decision_log_id AND p.owner_chat_id=$1) AS recorded_plan_links
            FROM broker_fills f WHERE f.owner_chat_id=$1 AND f.external_fill_id=ANY($2::text[])''',owner,list(external_ids))
        return [event('FILL',owner,str(r['source'])+':'+str(r['external_fill_id']),dict(r),effective_at=r['executed_at']) for r in rows]
    return await capture(factory)


async def capture_movements(movements,*,owner):
    def factory():
        payload=[]
        for movement in movements:
            row=clean(movement)
            row.pop('raw_payload',None)
            payload.append(row)
        return [event('MOVEMENTS',owner,digest(payload),{'movements':payload,'owner':owner,
            'coverage':'UNKNOWN','owner_basis':'CONFIGURED_SCRAPER_SESSION_NOT_INFERRED_FROM_UNSCOPED_DB'},
            effective_at=max((m.executed_at for m in movements),default=datetime.now(timezone.utc)))]
    return await capture(factory)


async def capture_market(observations):
    return await capture(lambda:[event('MARKET',o.owner_chat_id,o.observation_id,o.to_dict(),
        effective_at=o.candle_timestamp,available_at=o.available_at) for o in observations])


async def capture_broker_batch(db,movements,*,owner):
    if not enabled(): return {'status':'DISABLED'}
    try:
        if not owner: raise ValueError('exact session owner required')
        await capture_movements(movements,owner=owner)
        pool=await db.get_pool()
        async with pool.acquire() as conn:
            return await capture_fills(conn,owner=owner,external_ids=[m.external_movement_id for m in movements])
    except Exception as exc:
        log.error('qeef_broker_capture_failed code=%s',type(exc).__name__)
        return {'status':'FAILED','error_code':type(exc).__name__}

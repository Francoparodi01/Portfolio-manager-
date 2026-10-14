"""Owner-scoped durable consumer, recovery and deterministic evaluation products."""
from __future__ import annotations

from datetime import datetime,timezone,timedelta
import json
from src.decision_lab.models import Dataset,canonical,digest
from .prospective import CaptureEvent,case_from_plan,evaluate_case,lifecycle,PIPELINE_VERSION
from .delivery import append_event,deliver
from .hooks import event
from .contracts import Thesis
from .store import append_artifact
from .accounting import AccountSnapshot,AccountEvent,Coverage,FXQuote,reconcile
from .market import from_observation


def decode(value):
    return json.loads(value) if isinstance(value,str) else value


async def events_of(conn,owner,kind,*,as_of=None):
    rows=await conn.fetch('''SELECT payload FROM qeef_events WHERE owner_chat_id=$1 AND kind=$2
        AND ($3::timestamptz IS NULL OR available_at<=$3) ORDER BY effective_at,event_id''',owner,kind,as_of)
    return [CaptureEvent.model_validate(decode(r['payload'])) for r in rows]


async def product(conn,parent,kind,payload):
    value=event(kind,parent.owner,parent.event_id,payload,effective_at=parent.effective_at,
        available_at=parent.available_at,provenance='DERIVED',parent_id=parent.event_id)
    await append_event(conn,value,enqueue=False)
    return value


async def process_event(conn,item,*,code_version):
    if item.kind=='PLAN':
        case=case_from_plan(item)
        if case['thesis']:
            thesis=Thesis.model_validate(case['thesis'])
            await append_artifact(conn,owner=item.owner,kind='THESIS',artifact_id=thesis.thesis_id,
                payload=thesis.model_dump(mode='json'),code_version=PIPELINE_VERSION,data_as_of=thesis.recorded_at)
        await product(conn,item,'CASE',case)
    elif item.kind=='MARKET':
        fact=from_observation(item.payload)
        await product(conn,item,'EVALUATION',{'owner':item.owner,'source_event_id':item.event_id,
            'result_kind':'MARKET_CONTRACT','market':fact.model_dump(mode='json'),**fact.assess(item.available_at)})
    elif item.kind=='EVALUATION_INPUT':
        p=item.payload
        candidates=[e for e in await events_of(conn,item.owner,'CASE') if e.payload['case_id']==p['case_id']]
        if len(candidates)!=1: raise ValueError('exact frozen case missing or ambiguous')
        ds=Dataset.model_validate(p['dataset'])
        if any(r.owner is not None and r.owner!=item.owner for r in ds.records):
            raise ValueError('outcome dataset owner mismatch')
        as_of=datetime.fromisoformat(p['evaluated_as_of'])
        if as_of>item.available_at: raise ValueError('evaluation not yet available')
        result=evaluate_case(candidates[0].payload,ds,as_of=as_of,code_version=code_version)
        from .transitions import advance
        result['thesis_lifecycle']=await advance(conn,candidates[0].payload,ds,as_of=as_of)
        result['evaluation_id']=digest({k:v for k,v in result.items() if k!='evaluation_id'})
        await product(conn,item,'EVALUATION',result)
    elif item.kind=='ACCOUNTING_INPUT':
        p=item.payload
        start,end=AccountSnapshot.model_validate(p['start']),AccountSnapshot.model_validate(p['end'])
        if start.owner!=item.owner: raise ValueError('accounting owner mismatch')
        result=reconcile(start,end,tuple(AccountEvent.model_validate(e) for e in p['events']),
            Coverage.model_validate(p['coverage']),as_of=item.available_at,
            quotes=tuple(FXQuote.model_validate(q) for q in p.get('quotes',[])))
        await product(conn,item,'RECONCILIATION',result)
    elif item.kind=='THESIS_EVENT':
        p=item.payload
        raw=await conn.fetchval("SELECT payload FROM qeef_artifacts WHERE owner_chat_id=$1 AND kind='THESIS' AND artifact_id=$2",item.owner,p['thesis_id'])
        if not raw: raise ValueError('thesis parent not yet captured')
        thesis=Thesis.model_validate(decode(raw))
        previous=await conn.fetch('''SELECT e.payload FROM qeef_events e JOIN qeef_jobs j USING(owner_chat_id,event_id)
            WHERE e.owner_chat_id=$1 AND e.kind='THESIS_EVENT' AND j.status='DONE'
            AND e.payload->>'payload_json' IS NOT NULL''',item.owner)
        transitions=[CaptureEvent.model_validate(decode(r['payload'])).payload for r in previous]
        transitions=[r for r in transitions if r.get('thesis_id')==thesis.thesis_id]+[p]
        cutoff=max([item.available_at]+[datetime.fromisoformat(r['available_at']) for r in transitions])
        projection=lifecycle(thesis,transitions,as_of=cutoff)
        await product(conn,item,'LIFECYCLE',projection)
    elif item.kind in {'PORTFOLIO','FILL','MOVEMENTS'}:
        pass  # immutable observations consumed together by reconciliation, not fill-PnL
    else:
        raise ValueError('unsupported input event')


async def process_jobs(conn,*,owner,code_version,limit=100,force_retry=False):
    result={'processed':0,'failed':0}
    for _ in range(limit):
        selected=None
        try:
            async with conn.transaction():
                await conn.execute("SELECT set_config('qeef.owner',$1,true)",str(owner))
                selected=await conn.fetchrow('''SELECT j.event_id,e.payload FROM qeef_jobs j JOIN qeef_events e USING(owner_chat_id,event_id)
                    WHERE j.owner_chat_id=$1 AND j.status IN ('PENDING','RETRY') AND ($2 OR j.next_attempt_at<=now())
                    ORDER BY e.available_at,j.event_id FOR UPDATE OF j SKIP LOCKED LIMIT 1''',owner,force_retry)
                if not selected: break
                item=CaptureEvent.model_validate(decode(selected['payload']))
                await process_event(conn,item,code_version=code_version)
                await conn.execute("UPDATE qeef_jobs SET status='DONE',attempts=attempts+1,last_error_code=NULL,processed_at=now() WHERE owner_chat_id=$1 AND event_id=$2",owner,item.event_id)
                result['processed']+=1
        except Exception as exc:
            if selected is None: raise
            async with conn.transaction():
                await conn.execute("SELECT set_config('qeef.owner',$1,true)",str(owner))
                await conn.execute('''UPDATE qeef_jobs SET attempts=attempts+1,last_error_code=$3,
                    status=CASE WHEN attempts+1>=10 THEN 'DEAD_LETTER' ELSE 'RETRY' END,
                    next_attempt_at=now()+least(3600,power(2,least(attempts+1,12))) * interval '1 second'
                    WHERE owner_chat_id=$1 AND event_id=$2''',owner,selected['event_id'],type(exc).__name__)
            result['failed']+=1
            if force_retry: break  # do not spin on the same unavailable dependency
    return result


async def recover_plans(conn,*,owner,start,limit=100):
    """Only immutable FULL_CONTEXT captures after explicit activation; no backfill claim."""
    async with conn.transaction():
        await conn.execute("SELECT set_config('qeef.owner',$1,true)",str(owner))
        if not await conn.fetchval("SELECT to_regclass('public.decision_lab_plan_captures')"):
            return {'recovered':0,'source_status':'UNAVAILABLE'}
        rows=await conn.fetch('''SELECT c.capture_hash,c.plan_id,c.payload,c.captured_at FROM decision_lab_plan_captures c
            WHERE c.owner_chat_id=$1 AND c.captured_at >=$2 AND c.payload->>'capture_kind'='FULL_CONTEXT'
            AND (c.payload->>'decision_at')::timestamptz >=$2
            AND NOT EXISTS(SELECT 1 FROM qeef_events e WHERE e.owner_chat_id=$1 AND e.kind='PLAN' AND e.source_id=c.plan_id::text)
            ORDER BY c.captured_at,c.capture_hash LIMIT $3''',owner,start,limit)
        for row in rows:
            p=decode(row['payload'])
            p['source_references']=['decision_lab_plan_captures:'+row['capture_hash']]
            p['decision_links']=[dict(r) for r in await conn.fetch('SELECT id,decision_log_id FROM order_intents WHERE execution_plan_id=$1::uuid',str(row['plan_id']))]
            await append_event(conn,event('PLAN',owner,row['plan_id'],p,effective_at=p['decision_at'],
                available_at=p['captured_at'],provenance='RECOVERED_IMMUTABLE'))
        return {'recovered':len(rows),'source_status':'AVAILABLE','bounded_batch':limit}


async def schedule_evaluations(conn,*,owner,as_of):
    """Use an explicit outcome Dataset if present, otherwise preserve pending state.

    Native candle observations alone do not certify calendar/actions/ratio.
    The typed dataset is ingested as EVALUATION_INPUT by the same durable hook.
    """
    cases=await events_of(conn,owner,'CASE',as_of=as_of)
    from .adapters import dataset_from_market
    inputs=await events_of(conn,owner,'EVALUATION_INPUT',as_of=as_of)
    market=await events_of(conn,owner,'MARKET',as_of=as_of)
    count=0
    for item in cases:
        case=item.payload
        # Explicit evidence inputs are separately versioned; automatic native
        # collection must not overwrite or downgrade a supplied complete dataset.
        if any(e.payload.get('case_id')==case['case_id'] and not e.payload.get('automatic') for e in inputs):
            continue
        ds,gaps=dataset_from_market(case,market,as_of=as_of)
        p={'case_id':case['case_id'],'dataset':ds.model_dump(mode='json'),
           'evaluated_as_of':as_of.isoformat(),'market_contract_gaps':gaps,'automatic':True}
        if any(e.payload==p for e in inputs): continue
        await append_event(conn,event('EVALUATION_INPUT',owner,case['case_id'],p,effective_at=as_of,available_at=as_of,parent_id=item.event_id))
        count+=1
    return count


async def schedule_reconciliation(conn,*,owner,as_of):
    from .adapters import accounting_from_native
    snapshots=await events_of(conn,owner,'PORTFOLIO',as_of=as_of)
    fills=await events_of(conn,owner,'FILL',as_of=as_of)
    movements=await events_of(conn,owner,'MOVEMENTS',as_of=as_of)
    # Only a source's latest visible version; preserve every old version in events.
    def latest(items):
        by_id={}
        for item in sorted(items,key=lambda e:(e.available_at,e.event_id)): by_id[item.source_id]=item
        return list(by_id.values())
    snapshots=sorted(latest(snapshots),key=lambda e:e.effective_at)
    if len(snapshots)<2:return 0
    count=0
    previous=await events_of(conn,owner,'ACCOUNTING_INPUT',as_of=as_of)
    for start,end in zip(snapshots,snapshots[1:]):
        if start.effective_at==end.effective_at: continue
        payload=accounting_from_native(start,end,latest(fills),latest(movements),as_of=as_of)
        # Ignore only the capture timestamp when testing input equivalence.
        def signature(p):
            p={**p,'coverage':{**p['coverage'],'available_at':None}}
            return digest(p)
        if any(signature(e.payload)==signature(payload) for e in previous): continue
        await append_event(conn,event('ACCOUNTING_INPUT',owner,start.source_id+':'+end.source_id,payload,
            effective_at=end.effective_at,available_at=as_of))
        count+=1
        if count>=100: break
    return count


async def cycle(conn,outbox,*,owner,start,as_of,code_version,limit=100):
    delivered=await deliver(conn,outbox,owner=owner,limit=limit)
    recovered=await recover_plans(conn,owner=owner,start=start,limit=limit)
    from .recovery import recover_observations
    recovered['observations']=await recover_observations(conn,owner=owner,start=start,limit=limit)
    processed=await process_jobs(conn,owner=owner,code_version=code_version,limit=limit)
    async with conn.transaction():
        await conn.execute("SELECT set_config('qeef.owner',$1,true)",str(owner))
        scheduled=await schedule_evaluations(conn,owner=owner,as_of=as_of)
        reconciliations=await schedule_reconciliation(conn,owner=owner,as_of=as_of)
    evaluated=await process_jobs(conn,owner=owner,code_version=code_version,limit=limit)
    return {'delivery':delivered,'recovery':recovered,'processing':processed,'scheduled':scheduled,'reconciliations':reconciliations,'evaluation':evaluated}


async def safe_cycle(pool):
    from .hooks import enabled,settings,log
    from .delivery import Outbox
    if not enabled(): return {'status':'DISABLED'}
    try:
        owner,start,path=settings()
        async with pool.acquire() as conn:
            from pathlib import Path
            import hashlib
            root=Path(__file__).resolve().parents[2]
            files=sorted((root/'analysis/qeef').glob('*.py'))+sorted((root/'analysis/analytics_v2').glob('*.py'))+sorted((root/'decision_lab').glob('*.py'))
            version=PIPELINE_VERSION+':source:'+digest({p.relative_to(root).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in files})
            return await cycle(conn,Outbox(path),owner=owner,start=start,as_of=datetime.now(timezone.utc),code_version=version)
    except Exception as exc:
        log.error('qeef_worker_failed code=%s',type(exc).__name__)
        return {'status':'FAILED','error_code':type(exc).__name__}

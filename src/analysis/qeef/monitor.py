"""Read-only evidence metrics with explicit denominators, not performance charts."""
from collections import Counter
from datetime import datetime,timezone
from .prospective import AUTHORITY,PIPELINE_VERSION
from .worker import events_of


async def report(conn,*,owner,start=None,as_of=None,outbox=None):
    if not owner: raise ValueError('exact owner required')
    as_of=as_of or datetime.now(timezone.utc)
    result={**AUTHORITY,'pipeline_version':PIPELINE_VERSION,'owner':owner,'as_of':as_of.isoformat(),
        'activation_start':start.isoformat() if start else None,'status':'INSUFFICIENT_DATA'}
    async with conn.transaction(readonly=True):
        await conn.execute("SELECT set_config('qeef.owner',$1,true)",str(owner))
        if not await conn.fetchval("SELECT to_regclass('public.qeef_events')"):
            return {**result,'reasons':['MIGRATION_NOT_INSTALLED'],'economic_pnl_net':None}
        cases=await events_of(conn,owner,'CASE',as_of=as_of)
        evaluations=await events_of(conn,owner,'EVALUATION',as_of=as_of)
        reconciliations=await events_of(conn,owner,'RECONCILIATION',as_of=as_of)
        lifecycle=await events_of(conn,owner,'LIFECYCLE',as_of=as_of)
        def latest(items,key):
            rows={}
            for e in sorted(items,key=lambda e:(e.available_at,e.event_id)):
                if e.payload.get(key): rows[e.payload[key]]=e.payload
            return rows
        cf=latest([e for e in evaluations if e.payload.get('result_kind')=='COUNTERFACTUAL' or e.payload.get('case_id')],'case_id')
        th=latest(lifecycle,'thesis_id')
        for c in cases:
            if c.payload.get('thesis_id') and c.payload['thesis_id'] not in th:
                th[c.payload['thesis_id']]={**c.payload['thesis'],'lifecycle_status':'OPENED','closed_at':None}
        jobs=[dict(r) for r in await conn.fetch('''SELECT status,count(*) AS count,sum(duplicate_deliveries) AS duplicates,
            max(extract(epoch FROM ($2::timestamptz-e.available_at))) FILTER(WHERE status!='DONE') AS lag_seconds
            FROM qeef_jobs j JOIN qeef_events e USING(owner_chat_id,event_id) WHERE j.owner_chat_id=$1 AND e.available_at<=$2 GROUP BY status''',owner,as_of)]
        for row in jobs:
            if row['lag_seconds'] is not None: row['lag_seconds']=float(row['lag_seconds'])
        counts={r['kind']:r['count'] for r in await conn.fetch('''SELECT kind,count(*) AS count FROM qeef_events
            WHERE owner_chat_id=$1 AND available_at<=$2 GROUP BY kind''',owner,as_of)}
        source={}
        for table,kind,key,clock,extra in (
            ('decision_lab_plan_captures','PLAN','plan_id','captured_at',"AND s.payload->>'capture_kind'='FULL_CONTEXT'"),
            ('portfolio_snapshots','PORTFOLIO','snapshot_id','scraped_at',''),
            ('market_candle_observations','MARKET','observation_id','available_at',''),
            ('broker_fills','FILL',"source || ':' || s.external_fill_id",'created_at','')):
            exists=await conn.fetchval('SELECT to_regclass($1)','public.'+table)
            if not start or not exists:
                source[kind]={'expected':None,'missing':None,'status':'UNKNOWN_ACTIVATION_OR_SOURCE'};continue
            expr='s.'+key if kind!='FILL' else 's.'+key
            row=await conn.fetchrow(f'''SELECT count(DISTINCT {expr}) AS expected,
                count(DISTINCT {expr}) FILTER(WHERE NOT EXISTS(SELECT 1 FROM qeef_events e WHERE e.owner_chat_id=$1 AND e.kind=$4 AND e.source_id=({expr})::text AND e.available_at<=$3)) AS missing
                FROM {table} s WHERE s.owner_chat_id=$1 AND s.{clock}>=$2 AND s.{clock}<=$3 {extra}''',owner,start,as_of,kind)
            source[kind]=dict(row)
        reasons=Counter(reason for c in cases for reason in c.payload.get('missing',[]))
        for e in cf.values():
            reasons.update(e.get('reasons',[]))
            for r in e.get('outcomes',[]): reasons.update(r.get('reasons',[]))
        for e in reconciliations: reasons.update(e.payload.get('reasons',[]))
        market=[e.payload for e in evaluations if e.payload.get('result_kind')=='MARKET_CONTRACT']
        result.update(status='AVAILABLE',events_by_kind=counts,source_capture=source,
            decisions_captured=len(cases),opportunities_identified=len({c.payload['opportunity_id'] for c in cases if c.payload.get('opportunity_id')}),
            active_theses=sum(not t.get('closed_at') for t in th.values()),
            open_opportunities=len({t['opportunity_id'] for t in th.values() if not t.get('closed_at')}),
            alternatives_registered=sum(len(c.payload.get('alternatives',[])) for c in cases),
            cases_complete=sum(c.payload['status']=='VERIFIED' for c in cases),
            cases_incomplete=sum(c.payload['status']!='VERIFIED' for c in cases),
            unresolved_thesis_links=sum(not c.payload.get('thesis_id') for c in cases),
            evaluations_latest_by_case=dict(Counter(e['status'] for e in cf.values())),
            evaluations_pending=sum(c.payload['case_id'] not in cf or cf[c.payload['case_id']]['status']!='VERIFIED' for c in cases),
            evaluations_mature=sum(e['status']=='VERIFIED' for e in cf.values()),
            pit_contracts={'denominator':len(market),'verified':sum(e['status']=='VERIFIED' for e in market)},
            reconciliation_versions=dict(Counter(e.payload['status'] for e in reconciliations)),
            blockers=dict(sorted(reasons.items())),jobs=jobs,
            failed_events=sum(r['count'] for r in jobs if r['status'] in {'RETRY','DEAD_LETTER'}),
            duplicate_deliveries=sum(r['duplicates'] or 0 for r in jobs),
            processing_lag_seconds=max([float(r['lag_seconds'] or 0) for r in jobs]+[0]),
            movements_recovery='UNKNOWN_UNSCOPED_CANONICAL_SOURCE',
            economic_pnl_net=None,performance_claim='NO_AGGREGATE_ACCOUNT_PERFORMANCE_FROM_DECISION_RETURNS')
    if outbox: result['local_delivery']=outbox.status(owner)
    return result


def markdown(payload):
    rows=['# QEEF — evidencia prospectiva','',
          'RESEARCH_ONLY · affects_analysis=false · affects_execution=false · promotion_allowed=false','',
          '| Indicador | Valor |','|---|---|']
    for key in ('status','as_of','decisions_captured','opportunities_identified','active_theses','open_opportunities',
                'alternatives_registered','cases_complete','cases_incomplete','evaluations_pending','evaluations_mature',
                'failed_events','processing_lag_seconds','duplicate_deliveries','unresolved_thesis_links'):
        rows.append(f'| {key} | {payload.get(key,"UNKNOWN")} |')
    rows.extend(['','Los casos completos y los contrafactuales verificables no certifican PnL realizado ni alpha.','',
        '## Bloqueos','',*['- '+k+': '+str(v) for k,v in payload.get('blockers',{}).items()],
        *['- '+k for k in payload.get('reasons',[])],
        '', '## Denominadores de captura','', '| Fuente | Esperados | Ausentes |','|---|---|---|'])
    for name,value in payload.get('source_capture',{}).items():
        rows.append(f'| {name} | {value.get("expected")} | {value.get("missing")} |')
    rows.extend(['','Los movimientos sin owner no tienen denominador recuperable: UNKNOWN.',''])
    return '\n'.join(rows)

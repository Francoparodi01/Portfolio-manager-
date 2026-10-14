"""Phase 2 real PostgreSQL; unique disposable container, never operational DB."""
import asyncio
import json
from pathlib import Path
import asyncpg
import pytest
from tests.test_qeef_postgres import disposable
from tests.test_qeef_phase2 import plan_event,account_fixture,T
from src.analysis.qeef.hooks import event,capture_plan
from src.analysis.qeef.delivery import Outbox,append_event,deliver
from src.analysis.qeef.worker import process_jobs,events_of,schedule_evaluations,schedule_reconciliation
from src.analysis.qeef.prospective import case_from_plan
from src.analysis.qeef.monitor import report,markdown
from src.decision_lab.models import canonical

ROOT=Path(__file__).resolve().parents[1]


def test_postgres_end_to_end_retry_immutable_rls_monitor(disposable,tmp_path,monkeypatch):
    async def run():
        conn=await asyncpg.connect(disposable)
        try:
            await conn.execute("CREATE TABLE original_source(id int PRIMARY KEY,decision text); INSERT INTO original_source VALUES(1,'HOLD')")
            for name in ('20261014_qeef_v1.sql','20261014_qeef_prospective.sql'):
                migration=(ROOT/'migrations'/name).read_text(encoding='utf-8')
                await conn.execute(migration);await conn.execute(migration)
            plan,ds,cut=plan_event();case=case_from_plan(plan)
            outcome=event('EVALUATION_INPUT',1,'test-outcome',{'case_id':case['case_id'],'dataset':ds.model_dump(mode='json'),
                'evaluated_as_of':cut.isoformat()},effective_at=cut,available_at=cut,provenance='SYNTHETIC_TEST')
            # Arrival before its parent: durable retry, no lost input.
            await append_event(conn,outcome)
            assert (await process_jobs(conn,owner=1,code_version='test',force_retry=True))['failed']==1
            box=Outbox(tmp_path/'outbox.db');assert box.append(plan)
            assert (await deliver(conn,box,owner=1))['delivered']==1
            assert (await process_jobs(conn,owner=1,code_version='test'))['processed']==1
            result=await process_jobs(conn,owner=1,code_version='test',force_retry=True)
            assert result['failed']==0
            values=await events_of(conn,1,'EVALUATION')
            assert values[0].payload['status']=='VERIFIED'
            assert values[0].payload['economic_pnl_net'] is None
            assert values[0].payload['thesis_lifecycle']['closure_reason']=='EXPIRED'
            a,b,ev,c,t=account_fixture(dividend=20)
            accounting=event('ACCOUNTING_INPUT',1,'test-account',{'start':a.model_dump(mode='json'),'end':b.model_dump(mode='json'),
                'events':[e.model_dump(mode='json') for e in ev],'coverage':c.model_dump(mode='json')},effective_at=t,available_at=t)
            await append_event(conn,accounting)
            assert (await process_jobs(conn,owner=1,code_version='test'))['failed']==0
            account=(await events_of(conn,1,'RECONCILIATION'))[0].payload
            assert account['status']=='VERIFIED' and account['economic_pnl_net']=='178'
            before=await conn.fetchval('SELECT count(*) FROM qeef_events')
            assert not await append_event(conn,plan)
            assert await conn.fetchval('SELECT count(*) FROM qeef_events')==before
            # Explicit dataset cannot be overwritten by empty native observations.
            async with conn.transaction():
                assert await schedule_evaluations(conn,owner=1,as_of=cut)==0
            status=await report(conn,owner=1,start=T,as_of=cut,outbox=box)
            assert status['decisions_captured']==1 and status['evaluations_mature']==1
            assert status['duplicate_deliveries']>=1 and status['active_theses']==0
            assert 'RESEARCH_ONLY' in markdown(status)
            assert (await report(conn,owner=2,as_of=cut))['decisions_captured']==0
            for sql in ('UPDATE qeef_events SET source_id=source_id','DELETE FROM qeef_events','TRUNCATE qeef_jobs,qeef_events'):
                with pytest.raises(asyncpg.RaiseError,match='append-only'): await conn.execute(sql)
            await conn.execute('CREATE ROLE phase2_reader; GRANT USAGE ON SCHEMA public TO phase2_reader; GRANT SELECT ON qeef_events,qeef_jobs,qeef_artifacts TO phase2_reader')
            await conn.execute('SET ROLE phase2_reader')
            async with conn.transaction():
                await conn.execute("SELECT set_config('qeef.owner','2',true)")
                assert await conn.fetchval('SELECT count(*) FROM qeef_events')==0
                assert await conn.fetchval('SELECT count(*) FROM qeef_jobs')==0
            await conn.execute('RESET ROLE')
            assert await conn.fetchval('SELECT decision FROM original_source')=='HOLD'
        finally: await conn.close()
    asyncio.run(run())


def test_canonical_hook_recovery_and_automatic_native_outcomes(disposable,tmp_path,monkeypatch):
    async def run():
        from src.analysis.qeef.worker import recover_plans
        conn=await asyncpg.connect(disposable)
        try:
            await conn.execute('''CREATE TABLE decision_lab_plan_captures(capture_hash text,plan_id uuid,owner_chat_id bigint,payload jsonb,captured_at timestamptz);
                CREATE TABLE order_intents(id int,execution_plan_id uuid,decision_log_id int);
                CREATE TABLE execution_plans(id uuid,owner_chat_id bigint)''')
            pid='00000000-0000-0000-0000-000000000002'
            plan,ds,cut=plan_event(plan_id=pid);p=plan.payload
            p.update(capture_kind='FULL_CONTEXT',captured_at=T.isoformat())
            await conn.execute('INSERT INTO decision_lab_plan_captures VALUES($1,$2,1,$3::jsonb,$4)','capture2',pid,canonical(p),T)
            monkeypatch.setenv('QEEF_CAPTURE_ENABLED','true');monkeypatch.setenv('TELEGRAM_CHAT_ID','1')
            monkeypatch.setenv('QEEF_CAPTURE_START_AT',T.isoformat());monkeypatch.setenv('QEEF_SPOOL_PATH',str(tmp_path/'hook.db'))
            assert (await capture_plan(conn,owner=1,plan_id=pid,capture_hash='capture2'))['status']=='CAPTURED'
            box=Outbox(tmp_path/'hook.db')
            assert len(box.pending(1))==1
            # Simulate lost delivery acknowledgement / source scan before spool delivery.
            assert (await recover_plans(conn,owner=1,start=T))['recovered']==1
            assert (await deliver(conn,box,owner=1))['failed']==0
            assert (await process_jobs(conn,owner=1,code_version='test'))['failed']==0
            actions=[r.model_dump(mode='json') for r in ds.records if r.kind=='ACTION_COVERAGE']
            for bar in (r for r in ds.records if r.kind=='BAR'):
                data=bar.payload;s=next(s for s in ds.sessions if s.session_id==data['session_id'])
                p=dict(observation_id=bar.record_id,owner_chat_id=1,instrument_id='SYNTHETIC:AAA',ticker='AAA',market='BYMA',
                    currency='ARS',price_unit='ARS_PER_CEDEAR',volume_unit='CEDEAR',asset_type='CEDEAR',source=bar.source,
                    candle_timestamp=s.close_at.isoformat(),scraped_at=s.close_at.isoformat(),available_at=s.close_at.isoformat(),is_closed=True,
                    open_price=data['open'],close_price=data['close'],high_price=data['high'],low_price=data['low'],volume=data['volume'],
                    provenance=dict(calendar_version=ds.calendar_version,session_id=s.session_id,session_open=s.open_at.isoformat(),
                        session_close=s.close_at.isoformat(),calendar_available_at=T.isoformat(),corporate_action_version='fixture-v1',
                        ratio_version='fixture-v1',depositary_ratio=10,price_mode='RAW_AS_TRADED',corporate_action_records=actions))
                await append_event(conn,event('MARKET',1,bar.record_id,p,effective_at=s.close_at,available_at=s.close_at,provenance='SYNTHETIC_TEST'))
            assert (await process_jobs(conn,owner=1,code_version='test'))['failed']==0
            async with conn.transaction():
                assert await schedule_evaluations(conn,owner=1,as_of=cut)==1
            result=await process_jobs(conn,owner=1,code_version='test')
            assert result['failed']==0
            case=next(e.payload for e in await events_of(conn,1,'CASE') if e.payload['plan_id']==pid)
            results=[e.payload for e in await events_of(conn,1,'EVALUATION') if e.payload.get('case_id')==case['case_id']]
            assert results[-1]['status']=='VERIFIED'
            status=await report(conn,owner=1,start=T,as_of=cut)
            assert status['source_capture']['PLAN']=={'expected':1,'missing':0}
            assert status['pit_contracts']=={'denominator':21,'verified':21}
        finally: await conn.close()
    asyncio.run(run())


@pytest.mark.parametrize('failure',[False,True])
def test_actual_formal_plan_persistence_survives_qeef_failure(disposable,monkeypatch,failure):
    from copy import deepcopy
    from tests.test_high_confidence_evidence_v2 import evidence_database,save_formal_plan,formal_plan,bundle_counts
    from src.analysis.qeef import hooks
    async def broken(*args,**kwargs): raise RuntimeError('private simulated failure')
    async def run():
        async with evidence_database(disposable,monkeypatch) as (conn,url):
            monkeypatch.setenv('QEEF_CAPTURE_ENABLED','false')
            if failure: monkeypatch.setattr(hooks,'capture_plan',broken)
            plan=formal_plan();before=deepcopy(plan)
            ids=await save_formal_plan(url,execution_plan=plan)
            assert plan==before and len(ids)==4
            assert await bundle_counts(conn)==[1,4,4,2,1]
    asyncio.run(run())


def test_observation_recovery_mutable_fee_clock_and_snapshot_gaps(disposable):
    from src.analysis.qeef.recovery import recover_observations
    from datetime import datetime,timezone,timedelta
    async def run():
        conn=await asyncpg.connect(disposable)
        try:
            await conn.execute('''CREATE TABLE portfolio_snapshots(snapshot_id text,owner_chat_id bigint,scraped_at timestamptz,total_value_ars numeric,cash_ars numeric);
                CREATE TABLE positions(snapshot_id text,ticker text,currency text,quantity numeric,current_price numeric,avg_cost numeric);
                CREATE TABLE broker_fills(id int,source text,external_fill_id text,owner_chat_id bigint,created_at timestamptz,
                    executed_at timestamptz,executed_at_precision text,executed_at_source text,ticker text,side text,quantity numeric,
                    avg_fill_price numeric,gross_amount_ars numeric,fees_ars numeric,decision_log_id int)''')
            for i,qty,cash in ((0,10,1000),(1,6,1438),(2,6,1438)):
                sid='native-'+str(i)
                await conn.execute('INSERT INTO portfolio_snapshots VALUES($1,1,$2,$3,$4)',sid,T+timedelta(days=i),qty*100,cash)
                await conn.execute("INSERT INTO positions VALUES($1,'AAA','ARS',$2,100,80)",sid,qty)
            await conn.execute("INSERT INTO broker_fills VALUES(1,'fixture','fill',1,$1,$1,'exact','fixture','AAA','SELL',4,110,440,NULL,NULL)",T+timedelta(hours=1))
            now=datetime.now(timezone.utc)
            result=await recover_observations(conn,owner=1,start=T)
            assert result['PORTFOLIO']['recovered']==3 and result['FILL']['recovered']==1
            recovered=(await events_of(conn,1,'FILL'))[-1]
            assert recovered.provenance=='RECOVERED_CURRENT' and recovered.available_at>=now
            assert (await recover_observations(conn,owner=1,start=T))['FILL']['recovered']==0
            await conn.execute('UPDATE broker_fills SET fees_ars=2')  # only disposable synthetic source
            assert (await recover_observations(conn,owner=1,start=T))['FILL']['recovered']==1
            versions=sorted(await events_of(conn,1,'FILL'),key=lambda e:e.available_at)
            assert len(versions)==2 and versions[0].payload['fees_ars'] is None
            now=datetime.now(timezone.utc)
            await process_jobs(conn,owner=1,code_version='test')
            async with conn.transaction():
                assert await schedule_reconciliation(conn,owner=1,as_of=now)==2
            assert (await process_jobs(conn,owner=1,code_version='test'))['failed']==0
            results=await events_of(conn,1,'RECONCILIATION')
            assert len([r for r in results if r.payload['status']=='NOT_IDENTIFIABLE'])==2
            async with conn.transaction():
                assert await schedule_reconciliation(conn,owner=1,as_of=now)==0
            status=await report(conn,owner=1,start=T,as_of=now)
            assert status['source_capture']['PORTFOLIO']=={'expected':3,'missing':0}
            assert status['source_capture']['FILL']=={'expected':1,'missing':0}
        finally: await conn.close()
    asyncio.run(run())

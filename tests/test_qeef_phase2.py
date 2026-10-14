"""Synthetic fixtures validate software, never historical economic performance."""
import asyncio
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal as D
import json
from types import SimpleNamespace
import pytest
from tests.test_qeef import T,fixture,thesis
from src.decision_lab.models import canonical,digest,Dataset
from src.analysis.qeef.prospective import (DecisionBundle,case_from_plan,evaluate_case,lifecycle,thesis_from_declaration)
from src.analysis.qeef.hooks import event,capture
from src.analysis.qeef.delivery import Outbox,deliver
from src.analysis.qeef.accounting import (AccountSnapshot,Holding,AccountEvent,Coverage,FXQuote,reconcile,value_snapshot,convert)
from src.analysis.qeef.market import MarketFact,history_window


def plan_event(*,complete=True,side='SELL',plan_id='plan1'):
    ep,ds,cutoff=fixture(side=side)
    th=thesis().model_copy(update={'opportunity_id':ep.state.opportunity_id,'ticker':'AAA'})
    bundle=DecisionBundle(owner=1,plan_id=plan_id,defined_at=T,episode=ep,thesis=th,source_references=('fixture',))
    p={'owner':1,'plan_id':plan_id,'decision_at':T.isoformat(),
       'portfolio':{'snapshot_id':'portfolio','cash_ars':1000,'positions':[r.model_dump(mode='json') for r in ep.state.positions]},
       'plan':{'feasible':True,'decisions':[{'ticker':'AAA','action':side}],
               ('sell_orders' if side=='SELL' else 'buy_orders'):[{'ticker':'AAA','side':side,'quantity':4}]},
       'signals':[{'ticker':'AAA','available_at':T.isoformat()}]}
    if complete:p['qeef_bundle']=bundle.model_dump(mode='json')
    return event('PLAN',1,plan_id,p,effective_at=T,available_at=T,provenance='SYNTHETIC_TEST'),ds,cutoff


def declaration():
    th=thesis()
    return {'source_thesis_key':'case-1','source_id':'SYNTHETIC_TEST','hypothesis':'Declared fixture target',
        'opened_at':T.isoformat(),'available_at':T.isoformat(),'ticker':'MU','strategy_version':'fixture',
        'intended_horizon':20,'horizon_unit':'BYMA_SESSIONS',
        'invalidation_conditions':[c.model_dump(mode='json') for c in th.invalidation_conditions],
        'target_conditions':[c.model_dump(mode='json') for c in th.target_conditions],
        'source_references':['fixture']}


def transition(th,kind,*,at=T,event_id='e',**kw):
    return dict(owner=th.owner,thesis_id=th.thesis_id,thesis_hash=th.content_hash,event_id=event_id,
        effective_at=at.isoformat(),available_at=at.isoformat(),transition=kind,source_references=['fixture'],**kw)


def account_fixture(*,fee=2,dividend=0,split=1,flow=0,side='SELL'):
    end_at=T+timedelta(days=5)
    def hold(q,price,cost,at):
        return Holding(instrument_id='ARS:AAA',ticker='AAA',currency='ARS',quantity=q,price=price,
            average_cost=cost,valuation_at=at,available_at=at,source_reference='fixture')
    start=AccountSnapshot(owner=1,snapshot_id='start',at=T,available_at=T,cash={'ARS':1000},
        positions=(hold(10,100,80,T),),observed_nav=2000,source_reference='fixture')
    events=[]
    if split!=1:
        events.append(AccountEvent(owner=1,event_id='split',kind='SPLIT',at=T+timedelta(days=1),available_at=T+timedelta(days=1),
            currency='ARS',instrument_id='ARS:AAA',quantity_factor=split,source_reference='fixture',timestamp_precision='exact'))
    q=4*split
    sell=side=='SELL'
    events.append(AccountEvent(owner=1,event_id='trade',kind=side,at=T+timedelta(days=2),available_at=T+timedelta(days=2),
        currency='ARS',instrument_id='ARS:AAA',quantity=q,price=D(110)/split,fees=fee,taxes=0,other_costs=0,
        source_reference='fixture',timestamp_precision='exact',plan_id='plan1',order_id='order1'))
    if dividend:
        events.append(AccountEvent(owner=1,event_id='dividend',kind='DIVIDEND',at=T+timedelta(days=3),available_at=T+timedelta(days=3),
            currency='ARS',amount=dividend,source_reference='fixture',timestamp_precision='exact'))
    if flow:
        events.append(AccountEvent(owner=1,event_id='flow',kind='DEPOSIT',at=end_at,available_at=end_at,
            currency='ARS',amount=flow,source_reference='fixture',timestamp_precision='exact'))
    cash=1000+(440 if sell else -440)-(fee or 0)+dividend+flow
    qty=(6 if sell else 14)*split
    price=D(120)/split
    cost=D(80)/split if sell else D(1240)/14/split
    end=AccountSnapshot(owner=1,snapshot_id='end',at=end_at,available_at=end_at,cash={'ARS':cash},
        positions=(hold(qty,price,cost,end_at),),observed_nav=D(cash)+qty*price,source_reference='fixture')
    coverage=Coverage(owner=1,start=T,end=end_at,available_at=end_at,fills=True,external_flows=True,
        corporate_actions=True,costs=True,cost_basis=True,financing=0,source_references=('fixture',))
    return start,end,tuple(events),coverage,end_at


def test_net_zero_intraperiod_flows_do_not_manufacture_twr():
    a,b,ev,c,t=account_fixture()
    flows=tuple(AccountEvent(owner=1,event_id=kind,kind=kind,at=T+timedelta(days=i),
        available_at=T+timedelta(days=i),currency='ARS',amount=500,source_reference='fixture',
        timestamp_precision='exact') for i,kind in ((1,'DEPOSIT'),(4,'WITHDRAWAL')))
    result=reconcile(a,b,ev+flows,c,as_of=t)
    assert result['status']=='VERIFIED' and result['economic_pnl_net']=='158'
    assert result['flow_adjusted_return'] is None


def test_end_period_flow_adjusted_return():
    a,b,ev,c,t=account_fixture(flow=200)
    result=reconcile(a,b,ev,c,as_of=t)
    assert result['status']=='VERIFIED' and result['flow_adjusted_return']==.079


@pytest.mark.parametrize('kind',['FEE','TAX'])
def test_standalone_observed_charge_not_fabricated_as_fill(kind):
    a,b,ev,c,t=account_fixture()
    charge=AccountEvent(owner=1,event_id='charge',kind=kind,at=t,available_at=t,
        currency='ARS',amount=3,source_reference='fixture',timestamp_precision='exact')
    b=b.model_copy(update={'cash':{'ARS':b.cash['ARS']-3},'observed_nav':b.observed_nav-3})
    result=reconcile(a,b,ev+(charge,),c,as_of=t)
    assert result['status']=='VERIFIED' and result['economic_pnl_net']=='155'


def test_bundle_cannot_replace_source_portfolio_or_thesis_identity():
    e,ds,t=plan_event();p=e.payload
    p['portfolio']['snapshot_id']='different'
    with pytest.raises(ValueError,match='portfolio identity'):
        case_from_plan(event('PLAN',1,e.source_id,p,effective_at=T,available_at=T))
    p=e.payload;p['qeef_bundle']['thesis']['opportunity_id']='other'
    with pytest.raises(ValueError,match='opportunity identity'):
        case_from_plan(event('PLAN',1,e.source_id,p,effective_at=T,available_at=T))


def test_monitor_refuses_cross_owner_before_database_access(monkeypatch):
    from aiohttp import web
    from src.monitor.qeef import prospective_view
    monkeypatch.setenv('TELEGRAM_CHAT_ID','1')
    request=SimpleNamespace(query={'owner_chat_id':'2'},app={})
    with pytest.raises(web.HTTPForbidden): asyncio.run(prospective_view(request))


def test_telegram_optional_read_is_exact_owner_and_disabled_by_default(monkeypatch):
    from tests.test_telegram_output_quality import telegram_bot as bot
    calls=[]
    async def allowed(*args): return True
    async def send(*args,**kwargs): calls.append(args[2])
    monkeypatch.setattr(bot,'ensure_allowed_chat',allowed);monkeypatch.setattr(bot,'send_text',send)
    monkeypatch.setenv('TELEGRAM_CHAT_ID','1');monkeypatch.delenv('QEEF_TELEGRAM_ENABLED',raising=False)
    update=SimpleNamespace(effective_chat=SimpleNamespace(id=2))
    asyncio.run(bot.qeef_status_handler(update,None));assert not calls
    monkeypatch.setenv('QEEF_TELEGRAM_ENABLED','true')
    monkeypatch.setattr(bot,'get_config',lambda:pytest.fail('cross-owner DB access'))
    asyncio.run(bot.qeef_status_handler(update,None));assert len(calls)==1 and 'otra cuenta' in calls[0]


def test_thesis_generation_and_explicit_recurrence_identity():
    d=declaration()
    a=thesis_from_declaration(d,owner=1,decision_at=T)
    b=thesis_from_declaration(d,owner=1,decision_at=T+timedelta(days=1))
    assert a.content_hash==b.content_hash and a.opportunity_id==b.opportunity_id
    c=thesis_from_declaration({**d,'source_thesis_key':'new'},owner=1,decision_at=T)
    assert c.opportunity_id!=a.opportunity_id
    assert thesis_from_declaration(None,owner=1,decision_at=T) is None


def test_append_only_thesis_and_out_of_order_updates():
    th=thesis()
    rows=[transition(th,'UPDATED',at=T+timedelta(days=i),event_id=str(i),decision_id=str(i)) for i in (3,1,2)]
    result=lifecycle(th,rows+rows[:1],as_of=T+timedelta(days=4))
    assert result['decision_count']==3 and result['opportunity_count']==1
    assert result['lifecycle_status']=='UPDATED'
    with pytest.raises(ValueError): lifecycle(th,[{**rows[0],'thesis_hash':'changed'}],as_of=T+timedelta(days=4))


@pytest.mark.parametrize('kind,fields,status',[
    ('INVALIDATED',{'condition_id':'stop','observed_value':89,'observed_unit':'ARS','observed_source':'BYMA'},'INVALIDATED'),
    ('CONFIRMED',{'condition_id':'target','observed_value':121,'observed_unit':'ARS','observed_source':'BYMA'},'CONFIRMED'),
    ('CLOSED',{'execution_id':'fill1'},'CLOSED')])
def test_lifecycle_closure_and_invalidation(kind,fields,status):
    th=thesis();r=lifecycle(th,[transition(th,kind,**fields)],as_of=T)
    assert r['lifecycle_status']==status
    assert th.content_hash==thesis().content_hash


def test_future_thesis_and_future_confirmation_rejected_or_hidden():
    with pytest.raises(ValueError,match='future'):
        thesis_from_declaration({**declaration(),'available_at':(T+timedelta(days=1)).isoformat()},owner=1,decision_at=T)
    th=thesis()
    r=lifecycle(th,[transition(th,'CONFIRMED',at=T+timedelta(days=1),condition_id='target',observed_value=130,observed_unit='ARS',observed_source='BYMA')],as_of=T)
    assert r['lifecycle_status']=='OPENED'


def test_frozen_alternatives_and_complete_synthetic_counterfactual():
    e,ds,cut=plan_event();case=case_from_plan(e);original=deepcopy(case)
    result=evaluate_case(case,ds,as_of=cut,code_version='fixture')
    assert case==original
    assert result['status']=='VERIFIED' and result['economic_pnl_net'] is None
    assert {r['horizon'] for r in result['outcomes']}=={5,10,20}
    assert {r['alternative'] for r in result['outcomes']} >= {'PLAN','NO_TRADE','HOLD_EXISTING','CASH'}
    assert result==evaluate_case(case,ds,as_of=cut,code_version='fixture')


def test_incomplete_native_plan_keeps_basic_alternatives_and_missing_thesis():
    e,ds,cut=plan_event(complete=False);case=case_from_plan(e)
    assert case['thesis_id'] is None and case['status']=='PARTIAL'
    assert [r for r in case['alternatives'] if r['name']=='NO_TRADE'][0]['status']=='FROZEN_PARTIAL'
    assert evaluate_case(case,ds,as_of=cut,code_version='fixture')['status']=='NOT_IDENTIFIABLE'


def test_buy_without_execution_never_becomes_realized():
    e,ds,cut=plan_event(side='BUY');r=evaluate_case(case_from_plan(e),ds,as_of=cut,code_version='fixture')
    assert r['result_kind']=='COUNTERFACTUAL' and r['economic_pnl_net'] is None


def test_future_signal_marks_case_invalid():
    e,ds,cut=plan_event();p=e.payload;p['signals'][0]['available_at']=cut.isoformat()
    bad=event('PLAN',1,e.source_id,p,effective_at=T,available_at=T)
    assert case_from_plan(bad)['status']=='INVALID'


def test_invalid_counterfactual_benchmark_not_selected_after_outcome():
    e,ds,cut=plan_event();p=e.payload
    p['qeef_bundle']['defined_at']=cut.isoformat()
    with pytest.raises(ValueError): case_from_plan(event('PLAN',1,e.source_id,p,effective_at=T,available_at=T))


@pytest.mark.parametrize('side',['SELL','BUY'])
def test_partial_sell_or_buy_accounting_preserves_nav_identity(side):
    a,b,ev,c,t=account_fixture(side=side)
    r=reconcile(a,b,ev,c,as_of=t)
    assert r['status']=='VERIFIED' and r['quantity_gaps']=={} and r['cash_gaps']=={}
    assert D(r['economic_pnl_net'])==b.observed_nav-a.observed_nav
    assert r['canonical_accounting']['periods'][0]['quality_gate']=='VERIFIED'


def test_hold_without_operation_has_mark_to_market_not_trade_pnl():
    a,b,ev,c,t=account_fixture()
    b=AccountSnapshot.model_validate({**a.model_dump(mode='json'),'snapshot_id':'hold','at':t.isoformat(),'available_at':t.isoformat()})
    r=reconcile(a,b,(),c,as_of=t)
    assert r['status']=='VERIFIED' and r['economic_pnl_net']=='0'


def test_duplicate_fill_exact_is_idempotent_conflicting_is_invalid():
    a,b,ev,c,t=account_fixture();r=reconcile(a,b,ev+ev,c,as_of=t)
    assert r['duplicate_events_rejected']==1 and r['economic_pnl_net']=='158'
    changed=AccountEvent.model_validate({**ev[0].model_dump(mode='json'),'quantity':3})
    with pytest.raises(ValueError,match='conflicting'):reconcile(a,b,ev+(changed,),c,as_of=t)


def test_unknown_fees_cannot_be_zero_or_simulated():
    a,b,ev,c,t=account_fixture(fee=None);r=reconcile(a,b,ev,c,as_of=t)
    assert r['fee_status']=='UNKNOWN' and r['economic_pnl_net'] is None and r['cash_gaps'] is None
    changed=AccountEvent.model_validate({**ev[0].model_dump(mode='json'),'source_kind':'SIMULATED'})
    with pytest.raises(ValueError,match='simulated'):reconcile(a,b,(changed,),c,as_of=t)


@pytest.mark.parametrize('kind',['SPLIT','RATIO_CHANGE'])
def test_corporate_rebase_preserves_value(kind):
    a,b,ev,c,t=account_fixture(split=2)
    ev=(AccountEvent.model_validate({**ev[0].model_dump(mode='json'),'kind':kind}),*ev[1:])
    r=reconcile(a,b,ev,c,as_of=t)
    assert r['status']=='VERIFIED' and r['economic_pnl_net']=='158'


def test_dividend_enters_cash_and_realized_component_once():
    a,b,ev,c,t=account_fixture(dividend=20);r=reconcile(a,b,ev,c,as_of=t)
    assert r['status']=='VERIFIED' and r['economic_pnl_net']=='178' and r['realized_pnl_gross']=='140'


def test_nav_mismatch_and_missing_flow_coverage_block_certification():
    a,b,ev,c,t=account_fixture()
    bad=AccountSnapshot.model_validate({**b.model_dump(mode='json'),'observed_nav':b.observed_nav+1})
    r=reconcile(a,bad,ev,c,as_of=t)
    assert r['economic_pnl_net'] is None and 'NAV_COMPONENT_MISMATCH' in r['reasons']
    c=Coverage.model_validate({**c.model_dump(mode='json'),'external_flows':False})
    assert reconcile(a,b,ev,c,as_of=t)['economic_pnl_net'] is None


def test_real_cash_flows_are_not_pnl():
    a,b,ev,c,t=account_fixture(flow=200);r=reconcile(a,b,ev,c,as_of=t)
    assert r['status']=='VERIFIED' and r['economic_pnl_net']=='158' and r['net_external_flows']=='200'


def test_fx_requires_quote_known_at_valuation_and_does_not_impute_ccl():
    q=FXQuote(currency='USD',reporting_currency='ARS',rate=1000,effective_at=T,available_at=T+timedelta(days=1),source_reference='fixture')
    assert convert(D(1),'USD','ARS',at=T,available_cutoff=T+timedelta(days=2),quotes=(q,)) is None
    known=FXQuote.model_validate({**q.model_dump(mode='json'),'available_at':T.isoformat()})
    assert convert(D(1),'USD','ARS',at=T,available_cutoff=T,quotes=(known,))==1000


def market_fact(**kw):
    return MarketFact(instrument_id='BYMA:CEDEAR:ARS:AAA',ticker='AAA',exchange='BYMA',currency='ARS',
        price_unit='ARS_PER_CEDEAR',volume_unit='CEDEAR_UNITS',calendar_version='SYNTHETIC_BYMA',session_id='s1',
        observed_at=T,available_at=T,source_timestamp=T,corporate_action_version='fixture',ratio_version='fixture',
        depositary_ratio=10,asset_type='CEDEAR',source_id='fixture',source_reference='fixture',price_basis='RAW_AS_TRADED',
        is_closed=True,close=100,**kw)


def test_market_contract_and_future_revision():
    f=market_fact();assert f.assess(T)['status']=='VERIFIED'
    revised=MarketFact.model_validate({**f.model_dump(mode='json'),'revision_at':(T+timedelta(days=1)).isoformat()})
    assert revised.assess(T)['status']=='INVALID'


@pytest.mark.parametrize('field',['calendar_version','volume_unit','ratio_version','exchange'])
def test_market_missing_metadata_not_filled(field):
    f=MarketFact.model_validate({**market_fact().model_dump(mode='json'),field:'UNKNOWN'})
    assert f.assess(T)['status']=='PARTIAL'


def test_short_history_and_other_instrument_do_not_fill_sndk_window():
    f=market_fact();r=history_window([f],instrument_id='BYMA:CEDEAR:ARS:SNDK',decision_at=T,required_sessions=20)
    assert r['status']=='INSUFFICIENT_DATA' and r['n_available_sessions']==0


def test_owner_isolation_in_accounting_and_bundle():
    a,b,ev,c,t=account_fixture()
    wrong=AccountSnapshot.model_validate({**b.model_dump(mode='json'),'owner':2})
    with pytest.raises(ValueError,match='owner'):reconcile(a,wrong,ev,c,as_of=t)
    e,ds,cut=plan_event();p=e.payload;p['qeef_bundle']['owner']=2
    with pytest.raises(ValueError):case_from_plan(event('PLAN',1,e.source_id,p,effective_at=T,available_at=T))


def test_outbox_restart_idempotence_and_owner_scope(tmp_path):
    e,_,_=plan_event();path=tmp_path/'spool.sqlite3';box=Outbox(path)
    assert box.append(e) and not box.append(e)
    assert len(Outbox(path).pending(1))==1 and Outbox(path).pending(2)==[]
    assert box.status(1)['duplicates']==1
    p=e.payload;p['plan']['feasible']=False
    bad=type(e).model_validate({**e.model_dump(mode='json'),'payload_json':canonical(p)})
    with pytest.raises(ValueError,match='identity conflict'):box.append(bad)


def test_temporary_connection_loss_retains_original_event(tmp_path):
    class Offline:
        def transaction(self):raise ConnectionError('do not leak connection details')
    e,_,_=plan_event();box=Outbox(tmp_path/'spool.sqlite3');box.append(e)
    result=asyncio.run(deliver(Offline(),box,owner=1))
    assert result['failed']==1 and box.pending(1)==[e]
    assert box.status(1)['failed']==1


def test_capture_disabled_does_not_invoke_producer_or_touch_files(monkeypatch,tmp_path):
    monkeypatch.delenv('QEEF_CAPTURE_ENABLED',raising=False)
    def factory():raise AssertionError('must not execute')
    assert asyncio.run(capture(factory))=={'status':'DISABLED'}
    assert list(tmp_path.iterdir())==[]


def test_collector_failure_preserves_recommendation_and_sanitizes_logs(monkeypatch,tmp_path,caplog):
    monkeypatch.setenv('QEEF_CAPTURE_ENABLED','true');monkeypatch.setenv('TELEGRAM_CHAT_ID','1')
    monkeypatch.setenv('QEEF_CAPTURE_START_AT',T.isoformat());monkeypatch.setenv('QEEF_SPOOL_PATH',str(tmp_path/'spool.sqlite3'))
    recommendation={'action':'BUY','quantity':4};before=deepcopy(recommendation)
    def fail():raise ConnectionError('SECRET_CREDENTIAL')
    result=asyncio.run(capture(fail))
    assert result['status']=='FAILED' and recommendation==before and 'SECRET_CREDENTIAL' not in caplog.text


def test_capture_enabled_persists_before_acknowledgement(monkeypatch,tmp_path):
    e,_,_=plan_event();path=tmp_path/'spool.sqlite3'
    for k,v in {'QEEF_CAPTURE_ENABLED':'true','TELEGRAM_CHAT_ID':'1','QEEF_CAPTURE_START_AT':T.isoformat(),'QEEF_SPOOL_PATH':str(path)}.items():monkeypatch.setenv(k,v)
    assert asyncio.run(capture(lambda:[e]))['status']=='CAPTURED'
    assert Outbox(path).pending(1)==[e]


def test_accounting_event_arrival_order_does_not_change_result():
    a,b,ev,c,t=account_fixture(dividend=20,split=2)
    x=reconcile(a,b,ev,c,as_of=t);y=reconcile(a,b,tuple(reversed(ev)),c,as_of=t)
    assert x['economic_pnl_net']==y['economic_pnl_net']=='178'
    assert x['quantity_gaps']==y['quantity_gaps']=={}

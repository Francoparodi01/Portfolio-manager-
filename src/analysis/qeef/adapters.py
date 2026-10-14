"""Loss-aware adapters over captured native facts. No data repair or owner inference."""
from datetime import datetime
from decimal import Decimal
import json
from src.decision_lab.models import Dataset,Evidence,Session,canonical,digest
from .accounting import AccountSnapshot,Holding,AccountEvent,Coverage
from .market import from_observation


def source_instrument(ticker,currency):
    # Source-local key, not a verified security-master identifier.
    return 'BROKER_TICKER:'+currency+':'+ticker


def snapshot_from_native(payload,*,owner,available_at):
    if payload.get('owner_chat_id') is not None and int(payload['owner_chat_id'])!=owner:
        raise ValueError('snapshot owner mismatch')
    at=payload['scraped_at']
    positions=tuple(Holding(instrument_id=source_instrument(p['ticker'],p['currency']),ticker=p['ticker'],
        currency=p['currency'],quantity=p['quantity'],price=p['current_price'],average_cost=p.get('avg_cost'),
        valuation_at=at,available_at=available_at,source_reference='positions:'+str(payload['snapshot_id']))
        for p in payload.get('positions',[]) if Decimal(str(p['quantity']))>0)
    return AccountSnapshot(owner=owner,snapshot_id=str(payload['snapshot_id']),at=at,available_at=available_at,
        cash={'ARS':Decimal(str(payload['cash_ars']))},positions=positions,
        # Repository PortfolioSnapshot defines total_value_ars as invested value.
        observed_nav=Decimal(str(payload['total_value_ars']))+Decimal(str(payload['cash_ars'])),
        source_reference='portfolio_snapshots:'+str(payload['snapshot_id']))


def accounting_from_native(start_event,end_event,fill_events,movement_events,*,as_of):
    owner=start_event.owner
    start=snapshot_from_native(start_event.payload,owner=owner,available_at=start_event.available_at)
    end=snapshot_from_native(end_event.payload,owner=owner,available_at=end_event.available_at)
    events=[]
    for item in fill_events:
        p=item.payload
        if item.owner!=owner: raise ValueError('fill owner mismatch')
        events.append(AccountEvent(owner=owner,event_id=item.source_id,kind=p['side'],at=p['executed_at'],
            available_at=item.available_at,currency='ARS',instrument_id=source_instrument(p['ticker'],'ARS'),
            quantity=p['quantity'],price=p['avg_fill_price'],fees=p.get('fees_ars'),taxes=None,other_costs=None,
            source_reference='broker_fills:'+str(p['id']),decision_id=str(p['decision_log_id']) if p.get('decision_log_id') else None,
            plan_id=p.get('execution_plan_id'),order_id=p.get('order_intent_id'),
            timestamp_precision=p.get('executed_at_precision') or 'UNKNOWN'))
    unresolved=[]
    for item in movement_events:
        if item.owner!=owner: raise ValueError('movement owner mismatch')
        for p in item.payload.get('movements',[]):
            kind=str(p.get('movement_type','')).upper()
            if kind in {'BUY','SELL','COMPRA','VENTA'}: continue  # recorded as fills, never double count cash
            if kind not in {'DEPOSIT','WITHDRAWAL','DIVIDEND','TAX','FEE'} or p.get('amount') is None:
                unresolved.append(str(p.get('external_movement_id','UNKNOWN')));continue
            events.append(AccountEvent(owner=owner,event_id='movement:'+str(p['external_movement_id']),kind=kind,
                at=p['executed_at'],available_at=item.available_at,currency=p['currency'],amount=abs(Decimal(str(p['amount']))),
                source_reference='broker_movements:'+str(p['external_movement_id']),timestamp_precision=p.get('executed_at_precision') or 'UNKNOWN'))
    coverage=Coverage(owner=owner,start=start.at,end=end.at,available_at=as_of,
        source_references=(start.source_reference,end.source_reference))
    return {'start':start.model_dump(mode='json'),'end':end.model_dump(mode='json'),
        'events':[e.model_dump(mode='json') for e in events],'coverage':coverage.model_dump(mode='json'),
        'unresolved_movements':unresolved,'source_identity_status':'BROKER_TICKER_NOT_SECURITY_MASTER',
        'coverage_status':'UNATTESTED_VISIBLE_ROWS_ARE_NOT_COMPLETE_LEDGER'}


def dataset_from_market(case,market_events,*,as_of):
    """Compile only explicit session clocks, calendars and action coverage.

    No date->session guessing, no synthetic opens, no CEDEAR/underlying conversion.
    A native collector can supply these fields in its versioned provenance;
    until then the evaluator reports the actual gap.
    """
    records={}
    sessions={}
    calendars=set()
    reasons=[]
    frozen=case.get('frozen')
    if frozen:
        for raw in frozen['episode']['state']['records']:
            e=Evidence.model_validate(raw);records[e.content_hash]=e
    for item in market_events:
        if item.owner!=case['owner']: raise ValueError('market owner mismatch')
        if item.available_at>as_of: continue
        p=item.payload
        provenance=json.loads(p['provenance']) if isinstance(p.get('provenance'),str) else p.get('provenance') or {}
        try:
            fact=from_observation(p)
            assessment=fact.assess(as_of)
        except (ValueError,KeyError):
            reasons.append('INVALID_MARKET_CONTRACT');continue
        required=('session_open','session_close','calendar_available_at')
        if assessment['status']!='VERIFIED' or any(not provenance.get(k) for k in required):
            reasons.extend(assessment.get('reasons',[])+['EXPLICIT_SESSION_CONTRACT_REQUIRED']);continue
        if datetime.fromisoformat(provenance['calendar_available_at'])>datetime.fromisoformat(case['decision_at']):
            reasons.append('CALENDAR_NOT_KNOWN_AT_DECISION');continue
        session=Session(session_id=fact.session_id,open_at=provenance['session_open'],close_at=provenance['session_close'])
        if session.close_at>as_of: continue
        if session.session_id in sessions and sessions[session.session_id]!=session:
            raise ValueError('conflicting session calendar')
        sessions[session.session_id]=session;calendars.add(fact.calendar_version)
        bar=Evidence(kind='BAR',record_id=str(p['observation_id']),owner=item.owner,source=fact.source_id,
            effective_at=session.close_at,available_at=fact.available_at,revision_at=fact.revision_at,
            quality='POINT_IN_TIME_SAFE',payload_json=canonical({'ticker':fact.ticker,'session_id':fact.session_id,
                'open':p['open_price'],'close':p['close_price'],'high':p['high_price'],'low':p['low_price'],
                'volume':p.get('volume'),'currency':fact.currency,'venue':fact.exchange,'asset_type':fact.asset_type,
                'depositary_ratio':fact.depositary_ratio,'price_mode':fact.price_basis,'is_closed':fact.is_closed}))
        records[bar.content_hash]=bar
        for raw in provenance.get('corporate_action_records',[]):
            action=Evidence.model_validate(raw)
            if action.kind not in {'CORPORATE_ACTION','ACTION_COVERAGE'} or action.owner not in {None,item.owner}:
                raise ValueError('invalid corporate action provenance')
            if action.available_at<=as_of: records[action.content_hash]=action
    if len(calendars)>1: raise ValueError('incompatible calendar versions')
    dataset=Dataset(records=tuple(records.values()),sessions=tuple(sessions.values()),
        calendar_version=next(iter(calendars),'UNKNOWN'))
    return dataset,sorted(set(reasons))

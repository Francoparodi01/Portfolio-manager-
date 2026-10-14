"""Prospective contracts: incomplete source declarations remain incomplete."""
from __future__ import annotations

import json
from decimal import Decimal
from datetime import datetime
from typing import Literal
from pydantic import AwareDatetime, Field, model_validator
from src.decision_lab.models import Frozen, Episode, Dataset, canonical, digest
from .contracts import Thesis, ThesisEvent, Benchmark
from .counterfactuals import freeze_alternatives, evaluate_counterfactuals
from .lifecycle import project_thesis

PIPELINE_VERSION = 'qeef-prospective-v1'
AUTHORITY = {'mode': 'RESEARCH_ONLY', 'affects_analysis': False,
             'affects_execution': False, 'promotion_allowed': False}


class CaptureEvent(Frozen):
    owner: int
    kind: Literal['PLAN', 'PORTFOLIO', 'FILL', 'MOVEMENTS', 'MARKET', 'EVALUATION_INPUT', 'ACCOUNTING_INPUT',
                  'THESIS_EVENT', 'CASE', 'RECONCILIATION', 'EVALUATION', 'LIFECYCLE']
    source_id: str = Field(min_length=1)
    effective_at: AwareDatetime
    available_at: AwareDatetime
    source_version: str = Field(min_length=1)
    payload_json: str
    parent_id: str | None = None
    provenance: Literal['RECORDED', 'RECOVERED_IMMUTABLE', 'RECOVERED_CURRENT', 'DERIVED', 'SYNTHETIC_TEST'] = 'RECORDED'

    @model_validator(mode='after')
    def valid(self):
        if not self.owner:
            raise ValueError('exact owner required')
        value = json.loads(self.payload_json)
        if not isinstance(value, dict) or canonical(value) != self.payload_json:
            raise ValueError('canonical object payload required')
        for key in ('owner','owner_chat_id'):
            if value.get(key) is not None and int(value[key])!=self.owner:
                raise ValueError('event payload owner mismatch')
        return self

    @property
    def event_id(self):
        return digest([PIPELINE_VERSION, self.owner, self.kind, self.source_id, self.source_version])

    @property
    def content_hash(self):
        return digest(self)

    @property
    def payload(self):
        return json.loads(self.payload_json)


class DecisionBundle(Frozen):
    """Optional complete evidence furnished by a producer at recommendation time.

    Existing producers lacking these fields still create a CASE with missingness;
    QEEF does not generate investment targets or a strategy on their behalf.
    """
    owner: int
    plan_id: str
    defined_at: AwareDatetime
    episode: Episode
    thesis: Thesis | None = None
    benchmarks: tuple[Benchmark, ...] = ()
    source_references: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode='after')
    def valid(self):
        state = self.episode.state
        if self.owner != state.owner or self.defined_at > state.as_of:
            raise ValueError('bundle owner or ex-ante clock mismatch')
        if any(not e.known_at(state.as_of) for e in state.records):
            raise ValueError('future decision input')
        if self.thesis and (self.thesis.owner != self.owner or self.thesis.recorded_at > state.as_of):
            raise ValueError('thesis owner or availability mismatch')
        if self.thesis and self.thesis.opportunity_id != state.opportunity_id:
            raise ValueError('thesis opportunity identity mismatch')
        freeze_alternatives(self.episode, self.benchmarks)
        return self


def freeze_bundle(bundle: DecisionBundle):
    episode = freeze_alternatives(bundle.episode, bundle.benchmarks)
    alternatives = json.loads(episode.alternatives_json)
    return {'episode': episode.model_dump(mode='json'),
            'alternatives': [{**a, 'alternative_id': digest([episode.episode_id, a]),
                              'defined_at': bundle.defined_at.isoformat(),
                              'state_id': episode.state.state_id,
                              'cost_hash': digest(episode.cost_model),
                              'execution_policy': 'NEXT_SESSION_OPEN_LOTS_AND_CASH',
                              'status': 'NOT_IDENTIFIABLE' if a.get('reason') else 'FROZEN'} for a in alternatives]}


def recorded_alternatives(payload,*,recorded_at):
    """Freeze basic policies over recorded facts even when pricing is incomplete.

    The policy is fixed in this code before activation; no ticker selection occurs
    after a recommendation. Feasibility is only attested to the source's extent.
    """
    state=payload.get('portfolio')
    plan=payload.get('plan') or {}
    output=[]
    for name in ('PLAN','NO_TRADE','HOLD_EXISTING','CASH','REBALANCE_BASELINE','ELIGIBLE_ALTERNATIVE'):
        eligible=bool(state) and (name in {'NO_TRADE','HOLD_EXISTING'} or name=='PLAN' and plan.get('feasible') is True)
        row={'name':name,'defined_at':payload['decision_at'],'recorded_at':recorded_at,
            'policy_version':PIPELINE_VERSION+':recorded-basic-alternatives',
            'definition_basis':'PREDECLARED_POLICY_OVER_RECORDED_STATE',
            'initial_portfolio':state,'state_hash':digest(state) if state else None,
            'cash_available':plan.get('cash_before'),'constraints':{'gate':plan.get('gate'),'warnings':plan.get('warnings')},
            'orders':(plan.get('sell_orders',[])+plan.get('buy_orders',[])) if name=='PLAN' and eligible else [],
            'cost_status':'UNKNOWN','costs':None,'horizons_sessions':[5,10,20],
            'execution_policy':'NEXT_SESSION_OPEN_LOTS_AND_CASH',
            'status':'FROZEN_PARTIAL' if eligible else 'NOT_IDENTIFIABLE',
            'reason':'PRICING_COST_AND_MARKET_CONTRACT_REQUIRED' if eligible else 'EX_ANTE_FEASIBILITY_OR_SELECTION_NOT_RECORDED'}
        row['alternative_id']=digest(row)
        output.append(row)
    return output


def thesis_from_declaration(declaration,*,owner,decision_at):
    """Identity comes from an explicit producer key, never ticker similarity."""
    if not declaration:
        return None
    required=('source_thesis_key','source_id','hypothesis','opened_at','available_at',
              'ticker','strategy_version','intended_horizon','horizon_unit',
              'invalidation_conditions','target_conditions','source_references')
    if any(not declaration.get(k) for k in required):
        raise ValueError('incomplete explicit thesis declaration')
    if datetime.fromisoformat(declaration['available_at'])>decision_at:
        raise ValueError('future thesis declaration')
    key=[owner,declaration['source_id'],declaration['source_thesis_key']]
    return Thesis(owner=owner,opportunity_id=digest(['opportunity',key]),thesis_id=digest(['thesis',key]),
        ticker=declaration['ticker'],strategy_version=declaration['strategy_version'],
        thesis_opened_at=declaration['opened_at'],recorded_at=declaration['available_at'],
        intended_horizon=declaration['intended_horizon'],horizon_unit=declaration['horizon_unit'],
        invalidation_conditions=declaration['invalidation_conditions'],target_conditions=declaration['target_conditions'],
        source_references=declaration['source_references'],evidence_quality=declaration.get('evidence_quality','UNKNOWN'))


def case_from_plan(event: CaptureEvent):
    if event.kind != 'PLAN':
        raise ValueError('plan event required')
    p = event.payload
    if int(p['owner']) != event.owner or str(p['plan_id']) != event.source_id:
        raise ValueError('source plan identity mismatch')
    at = datetime.fromisoformat(p['decision_at'])
    if at != event.effective_at:
        raise ValueError('source decision time mismatch')
    bundle = DecisionBundle.model_validate(p['qeef_bundle']) if p.get('qeef_bundle') else None
    if bundle and (bundle.owner != event.owner or bundle.plan_id != event.source_id or bundle.episode.state.as_of != at):
        raise ValueError('bundle does not match canonical plan')
    portfolio = p.get('portfolio')
    plan = p.get('plan', {})
    if bundle:
        if bundle.thesis and bundle.thesis.ticker not in {d.get('ticker') for d in plan.get('decisions',[])}:
            raise ValueError('thesis instrument absent from canonical plan')
        # Producer must attest the exact source snapshot. A typed counterfactual
        # cannot silently replace the portfolio in the original recommendation.
        if not portfolio or bundle.episode.state.portfolio_snapshot_id != str(portfolio.get('snapshot_id')):
            raise ValueError('bundle portfolio identity mismatch')
        state=bundle.episode.state
        if Decimal(str(portfolio.get('cash_ars'))) != state.cash_ars:
            raise ValueError('bundle cash differs from canonical snapshot')
        source_positions={r['ticker']:(Decimal(str(r['quantity'])),Decimal(str(r.get('current_price',r.get('mark_ars'))))) for r in portfolio.get('positions',[])}
        if source_positions!={r.ticker:(r.quantity,r.mark_ars) for r in state.positions}:
            raise ValueError('bundle positions differ from canonical snapshot')
        source_orders=plan.get('sell_orders',[])+plan.get('buy_orders',[])
        actual=sorted((r['ticker'],r['side'],Decimal(str(r.get('planned_qty',r.get('quantity'))))) for r in source_orders if r.get('status')!='BLOCKED')
        frozen_orders=sorted((r.ticker,r.side,r.quantity) for r in bundle.episode.plan.orders if r.executable and not r.blocked)
        if actual!=frozen_orders:
            raise ValueError('bundle orders differ from canonical plan')
    missing = []
    thesis = bundle.thesis if bundle and bundle.thesis else thesis_from_declaration(p.get('thesis_declaration'),owner=event.owner,decision_at=at)
    if not thesis:
        missing.append('THESIS_DECLARATION_NOT_FROZEN')
    if not bundle:
        missing.extend(['TYPED_DECISION_STATE_MISSING', 'COST_AND_EXECUTION_CONTRACT_MISSING'])
    if not portfolio:
        missing.append('INITIAL_PORTFOLIO_MISSING')
    frozen = freeze_bundle(bundle) if bundle else None
    # Same ticker/score does not establish an opportunity identity. Only an
    # explicit immutable thesis reference can link subsequent recommendations.
    declaration = thesis.model_dump(mode='json') if thesis else None
    signals = p.get('signals') or []
    clocks = []
    for signal in signals:
        available = signal.get('available_at')
        clocks.append('UNKNOWN' if not available else 'INVALID' if datetime.fromisoformat(available) > at else 'RECORDED_AS_OF')
    if 'INVALID' in clocks:
        missing.append('FUTURE_SIGNAL_INPUT')
    if 'UNKNOWN' in clocks:
        missing.append('SIGNAL_AVAILABILITY_UNKNOWN')
    context={name:{'status':'RECORDED' if p.get(name) is not None else 'UNKNOWN','value':p.get(name)}
             for name in ('instrument_identity','discovery_source','market_state','technical_context','liquidity',
                          'macro','fundamentals','catalysts','risks','system_version')}
    return {**AUTHORITY, 'pipeline_version': PIPELINE_VERSION, 'owner': event.owner,
        'case_id': digest(['case', event.event_id]), 'source_event_id': event.event_id,
        'plan_id': event.source_id, 'decision_at': p['decision_at'], 'recorded_at': event.available_at.isoformat(),
        'source_references': p.get('source_references', []), 'source_digest': event.content_hash,
        'opportunity_id': thesis.opportunity_id if thesis else None,
        'thesis_id': thesis.thesis_id if thesis else None, 'thesis': declaration,
        'thesis_identity': 'EXPLICIT_FROZEN_REFERENCE' if thesis else 'UNRESOLVED_NO_HEURISTIC_PROMOTION',
        'decisions': plan.get('decisions', []), 'decision_links': p.get('decision_links', []),
        'allocation': {'decisions': plan.get('decisions', []), 'cash_before': plan.get('cash_before'),
                       'cash_after': plan.get('cash_after'), 'feasible': plan.get('feasible')},
        'initial_portfolio': portfolio, 'recorded_signals': signals, 'macro': p.get('macro'),
        'evidence_context':context,
        'events': p.get('events'), 'confidence_definition': 'SOURCE_SCORE_NOT_CALIBRATED_SUCCESS_PROBABILITY',
        'thesis_declaration': p.get('thesis_declaration'),
        'frozen': frozen, 'bundle': bundle.model_dump(mode='json') if bundle else None,
        'alternatives': frozen['alternatives'] if frozen else recorded_alternatives(p,recorded_at=event.available_at.isoformat()),
        'signal_clock_coverage': clocks, 'missing': missing,
        'status': 'INVALID' if 'FUTURE_SIGNAL_INPUT' in missing else 'PARTIAL' if missing else 'VERIFIED',
        'economic_pnl_net': None, 'result_kind': 'RECORDED_RECOMMENDATION'}


def evaluate_case(case, dataset: Dataset, *, as_of, code_version):
    if as_of.tzinfo is None or as_of < datetime.fromisoformat(case['decision_at']):
        raise ValueError('invalid evaluation cutoff')
    if not case.get('frozen') or case['status'] == 'INVALID':
        result = {**AUTHORITY, 'case_id': case['case_id'], 'owner': case['owner'],
            'status': 'INVALID' if case['status'] == 'INVALID' else 'NOT_IDENTIFIABLE',
            'reasons': case['missing'], 'outcomes': [], 'economic_pnl_net': None}
    else:
        episode = Episode.model_validate(case['frozen']['episode'])
        if episode.state.owner != case['owner']:
            raise ValueError('frozen owner mismatch')
        # Never reselect alternatives at maturity. The stored frozen Episode is
        # used directly, including benchmark policy hashes and quantities.
        horizons = {5,10,20}
        if case.get('thesis') and case['thesis']['horizon_unit'] == 'BYMA_SESSIONS':
            horizons.add(case['thesis']['intended_horizon'])
        rows = evaluate_counterfactuals(episode, dataset, evaluated_as_of=as_of, horizons=tuple(sorted(horizons)))
        pairs = []
        for h in sorted(horizons):
            group = {r['alternative']:r for r in rows if r['horizon']==h}
            for name in ('NO_TRADE','HOLD_EXISTING','CASH','REBALANCE_BASELINE','ELIGIBLE_ALTERNATIVE'):
                a,b=group['PLAN'],group[name]
                delta=a['net_return']-b['net_return'] if a.get('net_return') is not None and b.get('net_return') is not None else None
                pairs.append({'horizon_sessions':h,'benchmark':name,'incremental_return':delta,
                    'incremental_money_ars': delta*float(episode.state.capital_base_ars) if delta is not None else None})
        plan_rows=[r for r in rows if r['alternative']=='PLAN']
        status='VERIFIED' if plan_rows and all(r['status']=='VERIFIED' for r in plan_rows) else 'PARTIAL' if any(r.get('net_return') is not None for r in plan_rows) else 'INSUFFICIENT_DATA' if any(r['status']=='INSUFFICIENT_DATA' for r in plan_rows) else 'NOT_IDENTIFIABLE'
        result={**AUTHORITY,'case_id':case['case_id'],'owner':case['owner'],'status':status,
            'outcomes':rows,'pairs':pairs,'result_kind':'COUNTERFACTUAL','economic_pnl_net':None,
            'incremental_account_risk':None,'risk_reason':'NO_RECONCILED_CONTINUOUS_ACCOUNT_CURVE'}
    result.update({'pipeline_version':PIPELINE_VERSION,'evaluated_at':as_of.isoformat(),
        'code_version':code_version,'evidence_digest':digest([case,dataset]),'frozen_case_digest':digest(case)})
    result['evaluation_id']=digest(result)
    return result


def lifecycle(thesis, events, *, as_of):
    """Vocabulary adapter; objective checks remain in the v1 projection."""
    mapping={'UPDATED':'UPDATE','CONFIRMED':'TARGET','INVALIDATED':'INVALIDATION',
             'CLOSED':'POSITION_CLOSED','REGIME_CHANGE':'REGIME_CHANGE','EXPIRED':'EXPIRED'}
    mapped=[]
    confirmations=[]
    for raw in events:
        row=dict(raw)
        name=row.pop('transition')
        if name=='OPENED':
            if row.get('thesis_hash') != thesis.content_hash or row.get('owner') != thesis.owner:
                raise ValueError('opening identity mismatch')
            continue  # the immutable Thesis is the opening event
        if name not in mapping:
            raise ValueError('unknown lifecycle transition')
        event=ThesisEvent.model_validate({**row,'event_type':mapping[name]})
        if max(event.effective_at,event.available_at)>as_of:
            continue
        if name=='CONFIRMED':
            project_thesis(thesis,(event,),as_of=as_of)  # validate frozen target
            confirmations.append(event.event_id)
            event=ThesisEvent.model_validate({**event.model_dump(mode='json'),'event_type':'UPDATE'})
        mapped.append(event)
    projection=project_thesis(thesis,tuple(mapped),as_of=as_of)
    projection['confirmed_event_ids']=confirmations
    projection['lifecycle_status']='INVALIDATED' if projection['closure_reason']=='INVALIDATION' else 'CLOSED' if projection['closed_at'] else 'CONFIRMED' if confirmations else 'UPDATED' if mapped else 'OPENED'
    return projection

"""Observed portfolio reconciliation, using Decimal and canonical Analytics PnL.

Broker rows are inputs, never repaired. Missing coverage/costs/basis blocks a net
claim even when the visible balance happens to match. Research costs are excluded.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Literal
from pydantic import AwareDatetime, Field, model_validator
from src.decision_lab.models import Frozen, digest
from src.analysis.analytics_v2.models import Dataset, EconomicPeriod, ExecutionFact, NavPoint, ObservedAccountCost
from .metrics import realized_value
from .prospective import AUTHORITY, PIPELINE_VERSION

D=Decimal
ZERO=D(0)
TOL=D('0.01')


class Holding(Frozen):
    instrument_id: str
    ticker: str
    currency: str
    quantity: Decimal = Field(ge=0)
    price: Decimal = Field(gt=0)
    average_cost: Decimal | None = Field(default=None,ge=0)
    valuation_at: AwareDatetime
    available_at: AwareDatetime
    source_reference: str


class AccountSnapshot(Frozen):
    owner: int
    snapshot_id: str
    at: AwareDatetime
    available_at: AwareDatetime
    reporting_currency: str = 'ARS'
    cash: dict[str, Decimal]
    positions: tuple[Holding,...]
    observed_nav: Decimal = Field(gt=0)
    source_reference: str

    @model_validator(mode='after')
    def valid(self):
        if not self.owner or len({p.instrument_id for p in self.positions})!=len(self.positions):
            raise ValueError('owner and unique instrument positions required')
        if any(p.valuation_at>self.at or p.available_at>self.available_at for p in self.positions):
            raise ValueError('future snapshot valuation')
        return self


class AccountEvent(Frozen):
    owner: int
    event_id: str
    kind: Literal['BUY','SELL','DEPOSIT','WITHDRAWAL','DIVIDEND','SPLIT','RATIO_CHANGE','TAX','FEE']
    at: AwareDatetime
    available_at: AwareDatetime
    currency: str
    instrument_id: str | None = None
    quantity: Decimal | None = Field(default=None,gt=0)
    price: Decimal | None = Field(default=None,gt=0)
    amount: Decimal | None = Field(default=None,ge=0)
    fees: Decimal | None = Field(default=None,ge=0)
    taxes: Decimal | None = Field(default=None,ge=0)
    other_costs: Decimal | None = Field(default=None,ge=0)
    quantity_factor: Decimal | None = Field(default=None,gt=0)
    source_reference: str
    source_kind: Literal['OBSERVED','SIMULATED'] = 'OBSERVED'
    plan_id: str | None = None
    order_id: str | None = None
    decision_id: str | None = None
    timestamp_precision: str = 'UNKNOWN'

    @model_validator(mode='after')
    def valid(self):
        if not self.owner or not self.event_id or not self.source_reference:
            raise ValueError('account event identity required')
        if self.kind in {'BUY','SELL'} and (not self.instrument_id or self.quantity is None or self.price is None):
            raise ValueError('trade quantities and prices required')
        if self.kind in {'SPLIT','RATIO_CHANGE'} and (not self.instrument_id or self.quantity_factor is None):
            raise ValueError('corporate action quantity basis required')
        if self.kind in {'DEPOSIT','WITHDRAWAL','DIVIDEND','TAX','FEE'} and self.amount is None:
            raise ValueError('observed movement amount required')
        return self


class FXQuote(Frozen):
    currency: str
    reporting_currency: str
    rate: Decimal = Field(gt=0)
    effective_at: AwareDatetime
    available_at: AwareDatetime
    source_reference: str


class Coverage(Frozen):
    owner: int
    start: AwareDatetime
    end: AwareDatetime
    available_at: AwareDatetime
    fills: bool = False
    external_flows: bool = False
    corporate_actions: bool = False
    costs: bool = False
    cost_basis: bool = False
    financing: Decimal | None = Field(default=None,ge=0)
    source_references: tuple[str,...] = ()


def convert(amount, currency, reporting_currency, *, at, available_cutoff, quotes):
    if currency==reporting_currency:
        return amount
    eligible=[q for q in quotes if q.currency==currency and q.reporting_currency==reporting_currency
              and max(q.available_at,q.effective_at)<=at and q.available_at<=available_cutoff]
    if not eligible:
        return None
    latest=max(eligible,key=lambda q:(q.effective_at,q.available_at,q.source_reference))
    return amount*latest.rate


def value_snapshot(snapshot, *, quotes=()):
    parts=[(v,c) for c,v in snapshot.cash.items()]
    parts.extend((p.quantity*p.price,p.currency) for p in snapshot.positions)
    converted=[convert(v,c,snapshot.reporting_currency,at=snapshot.at,available_cutoff=snapshot.available_at,quotes=quotes) for v,c in parts]
    nav=sum(converted,ZERO) if all(x is not None for x in converted) else None
    residual=nav-snapshot.observed_nav if nav is not None else None
    return {'calculated_nav':str(nav) if nav is not None else None,
            'observed_nav':str(snapshot.observed_nav),'residual':str(residual) if residual is not None else None,
            'status':'VERIFIED' if residual is not None and abs(residual)<=TOL else 'NOT_IDENTIFIABLE' if nav is None else 'INVALID',
            'reasons':[] if residual is not None and abs(residual)<=TOL else ['FX_MISSING'] if nav is None else ['NAV_COMPONENT_MISMATCH']}


def reconcile(start: AccountSnapshot, end: AccountSnapshot, events: tuple[AccountEvent,...],
              coverage: Coverage, *, as_of, quotes=()):
    if start.owner!=end.owner or coverage.owner!=start.owner or any(e.owner!=start.owner for e in events):
        raise ValueError('owner mismatch')
    if not start.at<end.at<=as_of or max(start.available_at,end.available_at,coverage.available_at)>as_of:
        raise ValueError('invalid reconciliation cutoff')
    if start.reporting_currency!=end.reporting_currency:
        raise ValueError('reporting currency mismatch')
    reasons=[]
    if coverage.start>start.at or coverage.end<end.at or not coverage.source_references:
        reasons.append('COVERAGE_PERIOD_NOT_ATTESTED')
    for name in ('fills','external_flows','corporate_actions','costs','cost_basis'):
        if not getattr(coverage,name): reasons.append('COVERAGE_'+name.upper()+'_UNKNOWN')
    if coverage.financing is None: reasons.append('FINANCING_UNKNOWN')
    unique={}
    duplicates=0
    for event in events:
        if event.source_kind!='OBSERVED':
            raise ValueError('simulated cost or fill cannot enter observed accounting')
        if event.event_id in unique:
            if unique[event.event_id]!=event: raise ValueError('conflicting duplicate broker event')
            duplicates+=1
        unique[event.event_id]=event
    selected=[e for e in unique.values() if start.at<e.at<=end.at and e.available_at<=as_of]
    if any(start.at<e.at<=end.at and e.available_at>as_of for e in unique.values()):
        reasons.append('LATE_EVENT_NOT_VISIBLE')
    selected.sort(key=lambda e:(e.at,0 if e.kind in {'SPLIT','RATIO_CHANGE'} else 1,e.event_id))
    quantities={p.instrument_id:p.quantity for p in start.positions}
    basis={p.instrument_id:p.average_cost for p in start.positions}
    currencies={p.instrument_id:p.currency for p in start.positions}
    cash=dict(start.cash)
    realized=ZERO
    fees=taxes=other=external=ZERO
    known_cash=True
    reported=start.reporting_currency
    for e in selected:
        if e.timestamp_precision!='exact': reasons.append('EVENT_TIME_NOT_EXACT')
        if e.currency!=reported: reasons.append('FX_COMPONENT_ATTRIBUTION_REQUIRED')
        instrument=e.instrument_id
        if instrument and instrument in currencies and currencies[instrument]!=e.currency:
            raise ValueError('instrument currency mismatch')
        if e.kind in {'SPLIT','RATIO_CHANGE'}:
            if instrument not in quantities: reasons.append('ACTION_POSITION_UNKNOWN'); continue
            quantities[instrument]*=e.quantity_factor
            if basis.get(instrument) is not None: basis[instrument]/=e.quantity_factor
        elif e.kind in {'BUY','SELL'}:
            q=quantities.get(instrument,ZERO)
            cost=basis.get(instrument,ZERO if not q else None)
            gross=e.quantity*e.price
            known=all(x is not None for x in (e.fees,e.taxes,e.other_costs))
            if not known:
                reasons.append('OBSERVED_COSTS_UNKNOWN'); known_cash=False
            else:
                fees+=e.fees;taxes+=e.taxes;other+=e.other_costs
            cash[e.currency]=cash.get(e.currency,ZERO)+(gross if e.kind=='SELL' else -gross)-(e.fees+e.taxes+e.other_costs if known else ZERO)
            if e.kind=='BUY':
                basis[instrument]=(q*cost+gross)/(q+e.quantity) if cost is not None else None
                quantities[instrument]=q+e.quantity
            else:
                if e.quantity>q: reasons.append('SELL_EXCEEDS_OBSERVED_QUANTITY')
                if cost is None: reasons.append('COST_BASIS_UNKNOWN')
                else: realized+=e.quantity*(e.price-cost)
                quantities[instrument]=q-e.quantity
            currencies[instrument]=e.currency
        else:
            signed=-e.amount if e.kind in {'WITHDRAWAL','TAX','FEE'} else e.amount
            cash[e.currency]=cash.get(e.currency,ZERO)+signed
            if e.kind in {'DEPOSIT','WITHDRAWAL'}:
                converted=convert(signed,e.currency,reported,at=e.at,available_cutoff=as_of,quotes=quotes)
                if converted is None: reasons.append('FLOW_FX_MISSING')
                else: external+=converted
            elif e.kind=='DIVIDEND': realized+=e.amount
            elif e.kind=='TAX': taxes+=e.amount
            elif e.kind=='FEE': fees+=e.amount
    if coverage.financing is not None:
        cash[reported]=cash.get(reported,ZERO)-coverage.financing
    qend={p.instrument_id:p.quantity for p in end.positions}
    quantity_gaps={k:str(qend.get(k,ZERO)-quantities.get(k,ZERO)) for k in sorted(set(qend)|set(quantities)) if qend.get(k,ZERO)!=quantities.get(k,ZERO)}
    cash_gaps={k:str(end.cash.get(k,ZERO)-cash.get(k,ZERO)) for k in sorted(set(cash)|set(end.cash)) if abs(end.cash.get(k,ZERO)-cash.get(k,ZERO))>TOL} if known_cash else None
    if quantity_gaps: reasons.append('QUANTITY_RECONCILIATION_FAILED')
    if cash_gaps: reasons.append('CASH_RECONCILIATION_FAILED')
    values=[value_snapshot(s,quotes=quotes) for s in (start,end)]
    for value in values: reasons.extend(value['reasons'])
    if any(p.currency!=reported for p in start.positions+end.positions) or any(c!=reported and v for s in (start,end) for c,v in s.cash.items()):
        reasons.append('FX_COMPONENT_ATTRIBUTION_REQUIRED')
    initial_unreal=sum((p.quantity*(p.price-p.average_cost) for p in start.positions if p.average_cost is not None),ZERO)
    if any(p.average_cost is None for p in start.positions): reasons.append('COST_BASIS_UNKNOWN')
    final_unreal=ZERO
    for p in end.positions:
        if basis.get(p.instrument_id) is None: reasons.append('COST_BASIS_UNKNOWN')
        else: final_unreal+=p.quantity*(p.price-basis[p.instrument_id])
    economic=end.observed_nav-start.observed_nav-external
    components=realized+final_unreal-initial_unreal-fees-taxes-other-(coverage.financing or ZERO)
    if not reasons and abs(economic-components)>TOL: reasons.append('PNL_COMPONENT_RECONCILIATION_FAILED')
    result={**AUTHORITY,'pipeline_version':PIPELINE_VERSION,'owner':start.owner,'result_kind':'REALIZED_ACCOUNT',
        'status':'NOT_IDENTIFIABLE' if reasons else 'VERIFIED','reasons':sorted(set(reasons)),
        'start':start.at.isoformat(),'end':end.at.isoformat(),'evaluated_at':as_of.isoformat(),
        'quantity_gaps':quantity_gaps,'cash_gaps':cash_gaps,'valuation':values,
        'n_events_supplied':len(events),'n_unique_events':len(unique),'n_events_in_period_visible':len(selected),
        'duplicate_events_rejected':duplicates,'fee_status':'UNKNOWN' if any(e.kind in {'BUY','SELL'} and e.fees is None for e in selected) else 'KNOWN' if coverage.costs else 'UNKNOWN',
        'economic_pnl_net':str(economic) if not reasons else None,
        'realized_pnl_gross':str(realized) if not reasons else None,
        'unrealized_pnl_change':str(final_unreal-initial_unreal) if not reasons else None,
        'net_external_flows':str(external) if coverage.external_flows and 'FLOW_FX_MISSING' not in reasons else None,
        'fees':str(fees) if coverage.costs and known_cash else None,
        'flow_adjusted_return':None,'return_reason':'INTRAPERIOD_FLOWS_REQUIRE_SUBPERIOD_VALUATIONS',
        'evidence_digest':digest([start,end,events,coverage,quotes])}
    if not reasons:
        flow_events=[e for e in selected if e.kind in {'DEPOSIT','WITHDRAWAL'}]
        flow_timing='NONE' if not flow_events else 'PERIOD_END' if all(e.at==end.at for e in flow_events) else 'UNKNOWN'
        # Final certification also passes existing Analytics v2 identities.
        common=dict(account_id=str(start.owner),currency=reported,available_at=as_of,ingested_at=as_of,sealed_at=as_of,source_id='qeef-reconciled')
        executions=tuple(ExecutionFact(**common,broker_fill_id=e.event_id,instrument_id=e.instrument_id,quantity=float(e.quantity),side=e.kind,fill_price=float(e.price),fees=float(e.fees),taxes=float(e.taxes),other_costs=float(e.other_costs),fill_at=e.at) for e in selected if e.kind in {'BUY','SELL'})
        charges=tuple(ObservedAccountCost(**common,event_id=e.event_id,at=e.at,
            component='fees' if e.kind=='FEE' else 'taxes',amount=float(e.amount)) for e in selected if e.kind in {'FEE','TAX'})
        period=EconomicPeriod(**common,start=start.at,end=end.at,beginning_nav=float(start.observed_nav),ending_nav=float(end.observed_nav),net_external_flows=float(external),realized_pnl=float(realized),unrealized_pnl_change=float(final_unreal-initial_unreal),fees=float(fees),taxes=float(taxes),financing=float(coverage.financing),other_costs=float(other),coverage_confirmed=True,evidence_ids=coverage.source_references)
        points=tuple(NavPoint(**common,at=s.at,nav=float(s.observed_nav),gross_notional=float(sum((p.quantity*p.price for p in s.positions),ZERO)),net_notional=float(sum((p.quantity*p.price for p in s.positions),ZERO)),sizing_policy='OBSERVED_ACCOUNT',kind='ACTUAL',reconciled=True,net_external_flow=0 if s is start else float(external),flow_timing='NONE' if s is start else flow_timing,flow_adjusted_return=None if s is start or flow_timing=='UNKNOWN' else float(economic/start.observed_nav)) for s in (start,end))
        validated=realized_value(Dataset(label=PIPELINE_VERSION,executions=executions,account_costs=charges,economic_periods=(period,),nav_points=points),owner=start.owner,evaluated_as_of=as_of)
        if validated['periods'][0]['quality_gate']!='VERIFIED':
            result.update(status='INVALID',economic_pnl_net=None,reasons=['CANONICAL_ANALYTICS_RECONCILIATION_FAILED'])
        result['canonical_accounting']=validated
        if flow_timing!='UNKNOWN' and result['status']=='VERIFIED':
            result['flow_adjusted_return']=float(economic/start.observed_nav)
            result['return_reason']=None
    result['reconciliation_id']=digest(result)
    return result

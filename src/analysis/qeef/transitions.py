"""Automatic objective observations use only frozen conditions and explicit sessions."""
from datetime import timedelta
from operator import lt,le,gt,ge,eq
from .contracts import Thesis
from .prospective import lifecycle
from .hooks import event
from .delivery import append_event
from src.decision_lab.models import digest


async def advance(conn,case,dataset,*,as_of):
    from .worker import events_of
    if not case.get('thesis'): return None
    thesis=Thesis.model_validate(case['thesis'])
    prior=[e.payload for e in await events_of(conn,thesis.owner,'THESIS_EVENT',as_of=as_of)
           if e.payload.get('thesis_id')==thesis.thesis_id]
    projection=lifecycle(thesis,prior,as_of=as_of)
    if projection['closed_at']: return projection
    confirmed={e.get('condition_id') for e in prior if e.get('transition')=='CONFIRMED'}
    sessions=sorted((s for s in dataset.sessions if thesis.thesis_opened_at<s.close_at<=as_of),key=lambda s:s.close_at)
    expiry=None
    if thesis.horizon_unit=='BYMA_SESSIONS' and dataset.calendar_version!='UNKNOWN' and len(sessions)>=thesis.intended_horizon:
        expiry=sessions[thesis.intended_horizon-1].close_at
    if thesis.horizon_unit=='CALENDAR_DAYS' and thesis.thesis_opened_at+timedelta(days=thesis.intended_horizon)<=as_of:
        expiry=thesis.thesis_opened_at+timedelta(days=thesis.intended_horizon)
    candidates=[]
    for bar in sorted(dataset.records,key=lambda b:(b.effective_at,b.record_id)):
        if bar.kind!='BAR' or bar.available_at>as_of or (bar.revision_at and bar.revision_at>as_of): continue
        p=bar.payload
        if p.get('ticker')!=thesis.ticker or not p.get('is_closed') or bar.effective_at<thesis.recorded_at: continue
        if expiry and bar.effective_at>expiry: continue
        if p.get('price_mode')!='RAW_AS_TRADED': continue
        for transition,conditions in (('INVALIDATED',thesis.invalidation_conditions),('CONFIRMED',thesis.target_conditions)):
            for condition in conditions:
                if condition.field not in {'price','close'} or condition.source!=bar.source or condition.unit!=p.get('currency'): continue
                if transition=='CONFIRMED' and condition.condition_id in confirmed: continue
                if not {'LT':lt,'LE':le,'GT':gt,'GE':ge,'EQ':eq}[condition.operator](p['close'],condition.value): continue
                row=dict(owner=thesis.owner,thesis_id=thesis.thesis_id,thesis_hash=thesis.content_hash,
                    event_id=digest([thesis.content_hash,transition,bar.content_hash,condition.condition_id]),
                    transition=transition,effective_at=bar.effective_at.isoformat(),
                    available_at=max(bar.available_at,bar.revision_at or bar.available_at).isoformat(),
                    condition_id=condition.condition_id,observed_value=p['close'],observed_unit=condition.unit,
                    observed_source=bar.source,source_references=[bar.content_hash])
                candidates.append(row)
                if transition=='CONFIRMED': confirmed.add(condition.condition_id)
                if transition=='INVALIDATED': break
        if any(r['transition']=='INVALIDATED' for r in candidates): break
    # Same-bar invalidation wins; no invented intraday target-before-stop ordering.
    invalid_times={r['effective_at'] for r in candidates if r['transition']=='INVALIDATED'}
    candidates=[r for r in candidates if r['transition']!='CONFIRMED' or r['effective_at'] not in invalid_times]
    if not any(r['transition']=='INVALIDATED' for r in candidates):
        if expiry:
            candidates=[r for r in candidates if r['effective_at']<=expiry.isoformat()]
            candidates.append(dict(owner=thesis.owner,thesis_id=thesis.thesis_id,thesis_hash=thesis.content_hash,
                event_id=digest([thesis.content_hash,'EXPIRED',expiry,dataset.calendar_version]),transition='EXPIRED',
                effective_at=expiry.isoformat(),available_at=as_of.isoformat(),
                elapsed_sessions=len(sessions),calendar_reference=dataset.calendar_version,
                source_references=[digest(dataset)]))
    for row in candidates:
        if any(r['event_id']==row['event_id'] for r in prior): continue
        # Verify against existing history before persisting a new immutable event.
        lifecycle(thesis,prior+[row],as_of=as_of)
        await append_event(conn,event('THESIS_EVENT',thesis.owner,row['event_id'],row,
            effective_at=row['effective_at'],available_at=row['available_at'],provenance='DERIVED'))
        prior.append(row)
    return lifecycle(thesis,prior,as_of=as_of)

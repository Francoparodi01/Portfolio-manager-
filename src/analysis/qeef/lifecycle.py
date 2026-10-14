"""Append-only thesis projection; objectives cannot be revised by later events."""
from operator import lt, le, gt, ge, eq
from .contracts import Thesis, ThesisEvent
from src.decision_lab.models import digest


def project_thesis(thesis: Thesis, events: tuple[ThesisEvent, ...], *, as_of):
    if as_of.tzinfo is None or as_of < thesis.recorded_at:
        raise ValueError('thesis was not available at cutoff')
    unique = {}
    for event in events:
        if event.owner != thesis.owner or event.thesis_id != thesis.thesis_id or event.thesis_hash != thesis.content_hash:
            raise ValueError('owner, thesis or frozen-objective mismatch')
        if event.effective_at < thesis.recorded_at or event.available_at < thesis.recorded_at:
            raise ValueError('event predates thesis registration')
        if event.event_id in unique and unique[event.event_id] != event:
            raise ValueError('conflicting immutable event')
        unique[event.event_id] = event
    rows = sorted((e for e in unique.values() if e.available_at <= as_of and e.effective_at <= as_of),
                  key=lambda e: (e.effective_at, e.event_id))
    closed = None
    for event in rows:
        if closed:
            raise ValueError('new thesis required after closure')
        if event.event_type in {'TARGET', 'INVALIDATION'}:
            conditions = thesis.target_conditions if event.event_type == 'TARGET' else thesis.invalidation_conditions
            c = next((c for c in conditions if c.condition_id == event.condition_id), None)
            if c is None or event.observed_value is None or event.observed_unit != c.unit or event.observed_source != c.source:
                raise ValueError('condition evidence does not match frozen definition')
            if not {'LT': lt, 'LE': le, 'GT': gt, 'GE': ge, 'EQ': eq}[c.operator](event.observed_value, c.value):
                raise ValueError('closure condition not satisfied')
        if event.event_type == 'POSITION_CLOSED' and not event.execution_id:
            raise ValueError('position closure requires execution evidence')
        if event.event_type == 'EXPIRED' and thesis.horizon_unit == 'CALENDAR_DAYS':
            if (event.effective_at - thesis.thesis_opened_at).total_seconds() < thesis.intended_horizon * 86400:
                raise ValueError('premature expiry')
        if event.event_type == 'EXPIRED' and thesis.horizon_unit == 'BYMA_SESSIONS':
            if event.elapsed_sessions is None or event.elapsed_sessions < thesis.intended_horizon or not event.calendar_reference:
                raise ValueError('session expiry requires calendar evidence and mature horizon')
        if event.event_type == 'EXPIRED' and thesis.horizon_unit == 'EVENT':
            raise ValueError('event horizon must close against a frozen target or invalidation condition')
        # Expiry does not declare economic success.
        if event.event_type != 'UPDATE':
            closed = event
    return {**thesis.model_dump(mode='json'), 'as_of': as_of.isoformat(),
            'current_status': 'CLOSED' if closed else 'OPEN',
            'closed_at': closed.effective_at.isoformat() if closed else None,
            'closure_reason': closed.event_type if closed else None,
            'censoring': 'NONE' if closed else 'RIGHT_CENSORED_AT_AS_OF',
            'decision_count': len({e.decision_id for e in rows if e.decision_id}),
            'opportunity_count': 1, 'thesis_count': 1,
            'event_ids': [e.event_id for e in rows], 'evidence_digest': digest([thesis, rows])}

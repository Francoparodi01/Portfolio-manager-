"""Lossless semantic adapter: raw history is evidence, never canonical new facts."""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta
import json
import math
from statistics import mean
from src.decision_lab.models import digest


def obj(value):
    return json.loads(value) if isinstance(value, str) else value or {}


def adapt_history(capture):
    if capture.get('schema_version') != 'qeef-capture-v1' or capture.get('transaction_read_only') != 'on' or not capture.get('owner_token'):
        raise ValueError('verified owner-scoped QEEF capture required')
    as_of = datetime.fromisoformat(capture['captured_at'])
    tables = capture['tables']
    plans_by_id = {str(p['id']): p for p in tables.get('execution_plans', [])}
    plans_by_run = defaultdict(list)
    intents = defaultdict(list)
    for p in plans_by_id.values():
        if p.get('run_id'):
            plans_by_run[str(p['run_id'])].append(p)
    for intent in tables.get('order_intents', []):
        if intent.get('decision_log_id') is not None:
            intents[str(intent['decision_log_id'])].append(intent)
    fills = defaultdict(list)
    for f in tables.get('broker_fills', []):
        if f.get('decision_log_id') is not None:
            fills[str(f['decision_log_id'])].append(f)
    snapshots = {str(p['snapshot_id']): p for p in tables.get('portfolio_snapshots', [])}
    contexts = defaultdict(list)
    for c in tables.get('decision_lab_plan_captures', []):
        contexts[str(c['plan_id'])].append(c)
    raw = [('decision_log', r) for r in tables.get('decision_log', [])]
    raw += [('position_hold_observations', r) for r in tables.get('position_hold_observations', [])]
    rows = []
    seen = set()
    for table, r in raw:
        source_ref = f'{table}:{r["id"]}'
        if source_ref in seen:
            raise ValueError('duplicate source identity')
        seen.add(source_ref)
        at = r.get('decided_at') or r.get('observed_at')
        if not at or datetime.fromisoformat(at) > as_of:
            raise ValueError('source record after capture cutoff')
        layers = obj(r.get('layers'))
        versions = layers.get('versions') or {}
        item_links = intents[str(r['id'])] if table == 'decision_log' else []
        exact_ids = {str(i['execution_plan_id']) for i in item_links}
        plan_id = next(iter(exact_ids)) if len(exact_ids) == 1 else str(r['execution_plan_id']) if r.get('execution_plan_id') else None
        candidates = plans_by_run[str(r.get('run_id'))]
        plan_link_kind = 'EXACT_ITEM_OR_HOLD_LINK' if plan_id else 'RUN_ONLY' if len(candidates) == 1 else 'AMBIGUOUS_OR_MISSING'
        if not plan_id and len(candidates) == 1:
            plan_id = str(candidates[0]['id'])
        plan = plans_by_id.get(plan_id)
        if plan and str(plan.get('run_id')) != str(r.get('run_id')):
            raise ValueError('decision and plan run lineage conflict')
        linked_fills = fills[str(r['id'])] if table == 'decision_log' else []
        raw_contexts = [obj(c['payload']) for c in contexts.get(plan_id, [])]
        full = [c for c in raw_contexts if c.get('capture_kind') == 'FULL_CONTEXT']
        snapshot_id = r.get('portfolio_snapshot_id')
        if not snapshot_id and full:
            portfolio = full[-1].get('portfolio') or {}
            snapshot_id = portfolio.get('snapshot_id')
        snapshot = snapshots.get(str(snapshot_id)) if snapshot_id else None
        # Do not infer a nearest snapshot and promote that guess to exact lineage.
        reasons = ['THESIS_NOT_FROZEN_IN_SOURCE', 'EXECUTABLE_PRICE_AND_ACTION_COVERAGE_NOT_LINKED']
        if not snapshot:
            reasons.append('EXACT_INITIAL_PORTFOLIO_LINK_MISSING')
        if not full:
            reasons.append('FULL_CONTEXT_CAPTURE_MISSING')
        if not linked_fills:
            reasons.append('NO_EXACT_EXECUTION_LINK')
        elif any(f.get('fees_ars') is None for f in linked_fills):
            reasons.append('OBSERVED_FEES_UNKNOWN')
        outcomes = {}
        for h in (5, 10, 20, 40):
            value = r.get(f'outcome_{h}d')
            executable = r.get(f'executable_outcome_{h}d')
            filled = r.get('outcome_filled_at')
            known = filled and datetime.fromisoformat(filled) <= as_of
            values = (value, executable)
            if any(v is not None and not math.isfinite(float(v)) for v in values):
                raise ValueError('nonfinite legacy outcome')
            outcomes[str(h)] = {'legacy_recorded_directional_return': float(value) if value is not None and known else None,
                'legacy_executable_directional_return': float(executable) if executable is not None and known else None,
                'recorded_value': value, 'available_at': filled,
                'status': 'RECORDED_NOT_PIT_REVALIDATED' if value is not None and known else 'UNAVAILABLE',
                'horizon_unit': 'CALENDAR_DAYS_FIRST_AVAILABLE_BAR' if table == 'decision_log' else 'SOURCE_OUTCOME_VERSION',
                'source_outcome_version': r.get('outcome_version'),
                'qeef_net_return': None, 'incremental_vs_no_trade': None}
        quality = 'PARTIAL_CAPTURE' if full else 'LEGACY_OR_INCOMPLETE'
        rows.append({'source_reference': source_ref, 'source_digest': digest(r),
            'owner_token': capture['owner_token'], 'decision_id': source_ref,
            'recommendation_id': source_ref, 'plan_id': plan_id,
            'order_intent_ids': [str(i['id']) for i in item_links], 'plan_link_kind': plan_link_kind,
            'execution_ids': [str(f['id']) for f in linked_fills],
            'ticker': r['ticker'], 'decision_at': at, 'action': r.get('decision') or r.get('action'),
            'source': r.get('source') or table, 'metric_scope': r.get('metric_scope') or 'UNKNOWN',
            'source_status': r.get('status') or 'UNKNOWN', 'regime': r.get('regime') or 'UNKNOWN',
            'strategy_version': versions.get('synthesis_version') or 'UNKNOWN',
            'evidence_quality': quality, 'source_references': [source_ref] + ([f'execution_plans:{plan_id}'] if plan_id else []),
            'initial_evidence': {'layers': layers, 'reference_price': r.get('price_at_decision') or r.get('reference_price')},
            'portfolio_state': snapshot, 'thesis': None, 'thesis_id': None, 'opportunity_id': None,
            'recommended_action': r.get('decision') or r.get('action'),
            'executed_action': [{'execution_id': str(f['id']), 'side': f['side'], 'quantity': f['quantity'],
                'price': f['avg_fill_price'], 'fees_ars': f.get('fees_ars'), 'executed_at': f['executed_at']} for f in linked_fills],
            'feasible_alternative': {'name': 'NO_TRADE', 'status': 'NOT_IDENTIFIABLE', 'reason': 'COMMON_INITIAL_STATE_AND_OUTCOME_EVIDENCE_REQUIRED'},
            'outcomes': outcomes, 'economic_pnl_net': None, 'counterfactual_delta': None,
            'quality_gate': 'NOT_IDENTIFIABLE', 'limitations': reasons,
            'execution_status': 'OBSERVED_LINKED_FILL' if linked_fills else 'UNOBSERVED_EXECUTION',
            'censoring': 'OUTCOME_MISSING_OR_PENDING' if all(o['status'] == 'UNAVAILABLE' for o in outcomes.values()) else 'PARTIAL_HORIZON_COVERAGE'})
    # Explicitly inferred recurrence groups. Stable anchor, no outcome-dependent
    # selection and no claim that these are 30 independent economic opportunities.
    active = {}
    for r in sorted(rows, key=lambda r: (r['decision_at'], r['source_reference'])):
        key = (r['owner_token'], r['ticker'], r['source'], r['metric_scope'], r['source_status'], r['strategy_version'], r['evidence_quality'])
        prior = active.get(key)
        if prior is None or datetime.fromisoformat(r['decision_at'])-datetime.fromisoformat(prior['last_at']) > timedelta(days=7):
            prior = {'proxy_id': digest(['qeef-recurrence-v1', key, r['source_reference']]), 'last_at': r['decision_at']}
            active[key] = prior
        r['recurrence_proxy_id'] = prior['proxy_id']
        r['recurrence_method'] = 'SAME_PARTITION_GAP_LE_7_CALENDAR_DAYS_NOT_CANONICAL_OPPORTUNITY'
        prior['last_at'] = r['decision_at']
    return sorted(rows, key=lambda r: (r['decision_at'], r['source_reference']))


def compare_history(cases):
    groups = defaultdict(list)
    keys = ('source', 'source_status', 'metric_scope', 'action', 'regime', 'strategy_version', 'evidence_quality')
    for c in cases:
        groups[tuple(c[k] for k in keys)].append(c)
    result = []
    for key, group in sorted(groups.items()):
        for h in ('5', '10', '20', '40'):
            values = [c['outcomes'][h]['legacy_recorded_directional_return'] for c in group
                      if c['outcomes'][h]['legacy_recorded_directional_return'] is not None]
            result.append({**dict(zip(keys, key)), 'horizon_label': h+'D',
                'horizon_unit': group[0]['outcomes'][h]['horizon_unit'],
                'n_decisions': len(group), 'n_recorded_outcomes': len(values),
                'n_recurrence_proxies': len({c['recurrence_proxy_id'] for c in group}),
                'n_identified_opportunities': 0, 'n_valid_economic_pairs': 0,
                'legacy_mean_recorded_outcome': mean(values) if values else None,
                'legacy_positive_outcome_rate': sum(v > 0 for v in values)/len(values) if values else None,
                'qeef_economic_net_mean': None, 'qeef_incremental_value_mean': None,
                'qeef_interpretation': 'RECORDED_DIRECTIONAL_DIAGNOSTIC_ONLY',
                'qeef_confidence_interval': None, 'status': 'NOT_IDENTIFIABLE'})
    return result


def legacy_lab_comparison(capture):
    objects = capture['tables'].get('decision_lab_objects', [])
    episodes = [obj(r['payload']) for r in objects if r['kind'] == 'episode']
    outcomes = [obj(r['payload']) for r in objects if r['kind'] == 'outcomes']
    rows = []
    for ep in episodes:
        ep_outcomes = [o for o in outcomes if o['episode_id'] == ep['episode_id']]
        for horizon in (5, 10, 20):
            selected = {o['alternative']: o for o in ep_outcomes if o['horizon'] == horizon}
            plan, hold = selected.get('PLAN', {}), selected.get('HOLD', {})
            delta = plan['net_return']-hold['net_return'] if plan.get('net_return') is not None and hold.get('net_return') is not None else None
            rows.append({'episode_id': ep['episode_id'], 'decision_at': ep['state']['as_of'], 'horizon_sessions': horizon,
                'recorded_quality': ep['quality']['level'], 'recorded_primary_eligible': ep['quality']['primary_eligible'],
                'legacy_plan_net': plan.get('net_return'), 'legacy_hold_net': hold.get('net_return'),
                'legacy_paired_delta': delta, 'qeef_verified_delta': None,
                'status': 'NOT_IDENTIFIABLE', 'reasons': ['LEGACY_RECONSTRUCTION_NOT_PRIMARY', 'OUTCOME_PRICE_HASHES_NOT_REPLAYED'],
                'unchanged_legacy_cost_model': ep['cost_model']})
    return rows

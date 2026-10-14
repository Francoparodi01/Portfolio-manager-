"""Separate estimands for detection, allocation, execution and confidence.

No universal score. Missing evidence remains missing. Statistical intervals are
conditional research diagnostics, never permission to promote a policy.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
import math
from types import SimpleNamespace
import numpy as np
from src.analysis.analytics_v2.pnl import economic_pnl
from src.analysis.date_block_statistics import interval, benjamini_hochberg
from src.decision_lab.models import digest


def detection_quality(universe, *, decision_at, evaluated_at, definition, selected, seed=42):
    """Labels must implement a frozen risk/cost/liquidity criterion, not price-up.

Rows contain ticker, available_at, score, label, label_available_at. Missing labels
remain in the coverage denominator; recall uses the full attested eligible set.
"""
    if definition.get('registered_at') is None or datetime.fromisoformat(definition['registered_at']) > decision_at:
        return {'status': 'INVALID', 'reasons': ['OPPORTUNITY_DEFINITION_NOT_FROZEN']}
    required = ('risk_limit', 'cost_policy', 'liquidity_rule', 'horizon', 'universe_ref')
    if any(definition.get(k) is None for k in required) or not definition.get('universe_complete'):
        return {'status': 'NOT_IDENTIFIABLE', 'reasons': ['DEFENSIBLE_OPPORTUNITY_UNIVERSE_REQUIRED']}
    keys = [r['ticker'] for r in universe]
    if len(set(keys)) != len(keys) or not set(selected).issubset(keys) or len(set(selected)) != len(selected):
        raise ValueError('unique eligible universe and selections required')
    if any(datetime.fromisoformat(r['available_at']) > decision_at for r in universe):
        return {'status': 'INVALID', 'reasons': ['FUTURE_UNIVERSE_OR_RANKING']}
    known = [r for r in universe if isinstance(r.get('label'), bool) and r.get('label_available_at')
             and decision_at < datetime.fromisoformat(r['label_available_at']) <= evaluated_at]
    sample = [r for r in known if r['ticker'] in selected]
    tp = sum(r['label'] for r in sample)
    positives = sum(r['label'] for r in known)
    complete = len(known) == len(universe) and bool(universe)
    random_selection = list(np.random.default_rng(seed).choice(sorted(keys), size=len(selected), replace=False)) if keys else []
    random_rows = [r for r in known if r['ticker'] in random_selection]
    ranked = sorted(universe, key=lambda r: (-r['score'], r['ticker'])) if all(isinstance(r.get('score'), (int, float)) and math.isfinite(r['score']) for r in universe) else []
    k = len(selected)
    ndcg = None
    if complete and ranked and k:
        dcg = sum(float(r['label'])/math.log2(i+2) for i,r in enumerate(ranked[:k]))
        ideal = sum(1/math.log2(i+2) for i in range(min(k, positives)))
        ndcg = dcg/ideal if ideal else None
    return {'status': 'VERIFIED' if complete else 'PARTIAL',
            'reasons': [] if complete else ['OUTCOME_LABEL_COVERAGE_INCOMPLETE'],
            'n_eligible': len(universe), 'n_selected': len(selected), 'n_labelled': len(known),
            'label_coverage': len(known)/len(universe) if universe else None,
            'precision': tp/len(sample) if len(sample) == len(selected) and sample else None,
            'recall': tp/positives if complete and positives else None,
            'false_positives': len(sample)-tp if len(sample) == len(selected) else None,
            'missed_opportunities': positives-tp if complete else None,
            'ranking_ndcg_at_selection_size': ndcg,
            'random_selection': random_selection, 'random_seed': seed,
            'random_precision': sum(r['label'] for r in random_rows)/len(random_rows) if complete and random_rows else None,
            'executable_return': None, 'definition_digest': digest(definition)}


def thesis_quality(thesis, evidence):
    """Audit relevant support and contradiction; never count indicators as alpha."""
    selected = [r for r in evidence if r.get('reference') in thesis.source_references]
    future = [r['reference'] for r in selected if not r.get('available_at') or
              datetime.fromisoformat(r['available_at']) > thesis.thesis_opened_at]
    missing = sorted(set(thesis.source_references)-{r['reference'] for r in selected})
    support = [r['reference'] for r in selected if r.get('relation') == 'SUPPORTS']
    contradictions = [r['reference'] for r in selected if r.get('relation') == 'CONTRADICTS']
    return {'DATA_QUALITY': {'status': 'INVALID' if future else 'PARTIAL' if missing else 'VERIFIED',
                             'future_or_unclocked': future, 'missing_references': missing},
            'THESIS_COHERENCE': {'support': support, 'contradictions': contradictions,
                                 'status': 'PARTIAL', 'reason': 'RELATIONS_RECORDED_NOT_CAUSALLY_VALIDATED'},
            'PREDICTIVE_QUALITY': None, 'ECONOMIC_PERFORMANCE': None}


def allocation_quality(weights, *, covariance=None, sectors=None, max_weight=None):
    w = np.asarray(list(weights.values()), dtype=float)
    if not len(w) or not np.isfinite(w).all() or np.any(w < 0) or not math.isclose(float(w.sum()), 1, abs_tol=1e-8):
        raise ValueError('fully reconciled long-only weights including CASH required')
    names = list(weights)
    sector_weights = defaultdict(float)
    for name, value in weights.items():
        sector_weights[(sectors or {}).get(name, 'UNKNOWN' if name != 'CASH' else 'CASH')] += value
    result = {'status': 'PARTIAL', 'concentration_hhi': float(w @ w),
              'max_weight': float(w.max()), 'capital_utilization': 1-weights.get('CASH', 0),
              'sector_weights': dict(sector_weights),
              'limit_breaches': [t for t in names if t != 'CASH' and max_weight is not None and weights[t] > max_weight],
              'covariance_risk': None, 'cash_drag': None,
              'reasons': ['CASH_DRAG_REQUIRES_FROZEN_BENCHMARK', 'FACTOR_FX_LIQUIDITY_NOT_SUPPLIED']}
    if covariance is not None:
        c = np.asarray(covariance, dtype=float)
        if c.shape != (len(w), len(w)) or not np.isfinite(c).all() or not np.allclose(c, c.T) or np.linalg.eigvalsh(c).min() < -1e-10:
            raise ValueError('aligned finite positive-semidefinite covariance required')
        sigma = math.sqrt(max(0, float(w @ c @ w)))
        result['covariance_risk'] = {'volatility_per_covariance_period': sigma,
            'marginal_risk': dict(zip(names, (c @ w / sigma).tolist())) if sigma else None,
            'risk_contribution': dict(zip(names, (w*(c @ w)/sigma).tolist())) if sigma else None}
    else:
        result['reasons'].append('ALIGNED_PIT_COVARIANCE_MISSING')
    return result


def compare_allocations(initial, target, *, covariance=None, sectors=None, max_weight=None):
    if set(initial) != set(target):
        raise ValueError('same complete instrument universe required for allocation comparison')
    target = {k: target[k] for k in initial}
    return {'initial': allocation_quality(initial, covariance=covariance, sectors=sectors, max_weight=max_weight),
        'target': allocation_quality(target, covariance=covariance, sectors=sectors, max_weight=max_weight),
        'weight_changes': {k: target[k]-initial[k] for k in initial},
        'one_way_reallocation_including_cash': .5*sum(abs(target[k]-initial[k]) for k in initial),
        'asset_trade_notional_over_nav': sum(abs(target[k]-initial[k]) for k in initial if k != 'CASH'),
        'incremental_net_return': None, 'reason': 'WEIGHT_DIAGNOSTIC_REQUIRES_PRICED_COUNTERFACTUAL_FOR_VALUE'}


def realized_value(dataset, *, owner, evaluated_as_of):
    """Canonical Analytics v2 NAV/flow/component reconciliation, not fill sums."""
    if not owner or evaluated_as_of.tzinfo is None:
        raise ValueError('exact account owner and aware cutoff required')
    account_rows = dataset.recommendations + dataset.executions + dataset.economic_periods + dataset.nav_points + dataset.account_costs
    if any(r.account_id != str(owner) for r in account_rows):
        raise ValueError('economic dataset contains another owner')
    dataset = dataset.as_of(evaluated_as_of)
    if any(p.end > evaluated_as_of for p in dataset.economic_periods) or any(p.at > evaluated_as_of for p in dataset.nav_points) or any(f.fill_at > evaluated_as_of for f in dataset.executions):
        raise ValueError('economic evidence effective after evaluation cutoff')
    periods, curves = economic_pnl(dataset)
    counts = Counter((p.account_id,p.kind,p.sizing_policy,p.currency,p.cohort,p.horizon_days) for p in dataset.nav_points)
    for row in curves:
        key = (row['account_id'], row['kind'], row['sizing_policy'], row['currency'], row.get('cohort'), row.get('horizon_days'))
        n = counts[key]-1
        annual = (1+row['time_weighted_return'])**(row['periods_per_year']/n)-1 if n >= row['periods_per_year'] and row.get('time_weighted_return') is not None else None
        row['calmar'] = annual/abs(row['max_drawdown']) if annual is not None and row.get('max_drawdown',0) < 0 else None
        row['calmar_reason'] = None if row['calmar'] is not None else 'ONE_YEAR_RECONCILED_CURVE_WITH_NONZERO_DRAWDOWN_REQUIRED'
    for row in periods:
        row['result_kind'] = 'REALIZED_ACCOUNT'
        row['quality_gate'] = 'VERIFIED' if row['status'] == 'COMPLETE' else 'NOT_IDENTIFIABLE'
        row['estimated_alpha'] = None
        row['alpha_reason'] = 'NO_FROZEN_FACTOR_MODEL_OR_SAMPLING_CONTRACT'
    return {'periods': periods, 'curves': curves}


def execution_quality(*, owner, decision_at, order_id, ticker, side, requested_quantity, reference_price, fills, coverage_confirmed):
    """Observed fills only, with explicit partial execution and missing fees."""
    if not owner or side not in {'BUY', 'SELL'} or requested_quantity <= 0 or reference_price <= 0 or not all(math.isfinite(float(x)) for x in (requested_quantity, reference_price)):
        raise ValueError('valid owner and frozen order required')
    unique = {}
    for f in fills:
        if f['owner'] != owner or f['order_id'] != order_id or f['ticker'] != ticker or f['side'] != side:
            raise ValueError('fill attribution mismatch')
        if datetime.fromisoformat(f['executed_at']) < decision_at:
            raise ValueError('fill predates decision')
        if f['quantity'] <= 0 or f['price'] <= 0 or not all(math.isfinite(float(f[k])) for k in ('quantity', 'price')):
            raise ValueError('invalid observed execution')
        if f['execution_id'] in unique and unique[f['execution_id']] != f:
            raise ValueError('conflicting fill identity')
        unique[f['execution_id']] = f
    rows = list(unique.values())
    quantity = sum(r['quantity'] for r in rows)
    if quantity > requested_quantity:
        raise ValueError('fills exceed frozen requested quantity')
    if any(r.get('fees') is not None and (not math.isfinite(float(r['fees'])) or r['fees'] < 0) for r in rows):
        raise ValueError('invalid fee evidence')
    fees = sum(r['fees'] for r in rows) if rows and all(r.get('fees') is not None for r in rows) else None
    shortfall = sum((1 if side == 'BUY' else -1)*r['quantity']*(r['price']-reference_price) for r in rows) if rows else None
    return {'result_kind': 'OBSERVED_EXECUTION', 'status': 'VERIFIED' if coverage_confirmed and fees is not None else 'PARTIAL' if rows else 'INSUFFICIENT_DATA',
        'execution_state': 'FULL' if quantity == requested_quantity else 'PARTIAL' if quantity else 'NOT_EXECUTED' if coverage_confirmed else 'UNOBSERVED',
        'observed_quantity': quantity if rows or coverage_confirmed else None,
        'fill_ratio': quantity/requested_quantity if rows or coverage_confirmed else None,
        'fees': fees, 'shortfall_cost_vs_frozen_reference': shortfall,
        'economic_pnl_net': None, 'reasons': [] if coverage_confirmed and fees is not None else ['EXECUTION_COVERAGE_OR_FEES_INCOMPLETE']}


def attribution(variants):
    """Controlled model contrasts require same inputs, costs and sample; no causal %.

Each row names an ablated component, full and ablated net returns, a common input
digest, and separately frozen policy hashes. This estimates a model contrast only.
"""
    components = ('Collector', 'Radar', 'Technical', 'Fundamental', 'Macro', 'Risk',
                  'DecisionEngine', 'MetaPolicy', 'Optimizer', 'Planner', 'Execution')
    result = []
    for component in components:
        rows = [r for r in variants if r.get('component') == component]
        valid = bool(rows) and all(r.get('controlled') is True and r.get('same_initial_state') is True
            and r.get('same_costs') is True and r.get('same_horizon') is True and r.get('input_digest')
            and r.get('full_policy_hash') and r.get('ablated_policy_hash')
            and r.get('full_net_return') is not None and r.get('ablated_net_return') is not None
            and r.get('registered_at') and r.get('decision_at')
            and datetime.fromisoformat(r['registered_at']) <= datetime.fromisoformat(r['decision_at']) for r in rows)
        result.append({'component': component, 'status': 'PARTIAL' if valid else 'NOT_IDENTIFIABLE',
                       'model_contrast_mean': float(np.mean([r['full_net_return']-r['ablated_net_return'] for r in rows])) if valid else None,
                       'causal_share': None, 'reason': 'CONTROLLED_MODEL_CONTRAST_NOT_REAL_WORLD_CAUSAL_EFFECT' if valid else 'MATCHED_ABLATION_NOT_AVAILABLE'})
    return result


def paired_confidence(rows, *, horizon, minimum_n=30, seed=42, resamples=2000):
    """One paired value/opportunity; non-overlap starts at decision, not next open.

Temporal and ticker-cluster bootstrap intervals are reported separately. The
effective n is a conservative *proxy*, not proof of independent observations.
"""
    if minimum_n < 2 or resamples < 100 or horizon <= 0:
        raise ValueError('invalid sampling specification')
    cohort_keys = ('owner', 'strategy_version', 'cost_hash', 'benchmark_hash', 'horizon', 'quality', 'regime')
    if any(any(r.get(k) is None for k in cohort_keys) for r in rows):
        raise ValueError('complete cohort identity required')
    if len({tuple(r[k] for k in cohort_keys) for r in rows}) > 1:
        raise ValueError('incompatible cohorts must not be pooled')
    unique = {}
    for r in rows:
        if r['horizon'] != horizon:
            raise ValueError('mixed horizons')
        key = r['opportunity_id']
        if key in unique and digest(unique[key]) != digest(r):
            raise ValueError('pseudoreplication: conflicting opportunity outcomes')
        unique[key] = r
    accepted = [r for r in unique.values() if r.get('status') == 'MATURE' and r.get('delta') is not None]
    for r in accepted:
        if not math.isfinite(r['delta']) or datetime.fromisoformat(r['exit_at']) <= datetime.fromisoformat(r['decision_at']):
            raise ValueError('invalid paired outcome')
    disjoint, end = [], None
    for r in sorted(accepted, key=lambda r: (r['exit_at'], r['decision_at'], r['opportunity_id'])):
        start = datetime.fromisoformat(r['decision_at'])
        if end is None or start > end:
            disjoint.append(r)
            end = datetime.fromisoformat(r['exit_at'])
    values = [r['delta'] for r in accepted]
    tickers = sorted({r['ticker'] for r in accepted})
    result = {'status': 'INSUFFICIENT_DATA', 'reasons': ['INSUFFICIENT_INDEPENDENT_PRIMARY_EVIDENCE'],
              'n_raw': len(rows), 'n_opportunities': len(unique), 'n_mature': len(accepted),
              'n_nonoverlapping_proxy': len(disjoint), 'n_ticker_clusters': len(tickers),
              'coverage': len(accepted)/len(unique) if unique else None,
              'mean_incremental_return': float(np.mean(values)) if values else None,
              'win_rate_incremental': sum(v > 0 for v in values)/len(values) if values else None,
              'temporal_ci95': None, 'ticker_cluster_ci95': None,
              'promotion_allowed': False, 'regime': rows[0]['regime'] if rows else None}
    if len(disjoint) >= minimum_n and len(tickers) >= 2 and result['coverage'] >= .9 and all(r['quality'] == 'HIGH' for r in accepted):
        policy = SimpleNamespace(bootstrap_resamples=resamples, bootstrap_seed=seed, confidence_level=.95)
        result['temporal_ci95'] = interval([r['decision_at'][:10] for r in accepted], np.array(values), policy, max(horizon, 20))
        clusters = [[r['delta'] for r in accepted if r['ticker'] == ticker] for ticker in tickers]
        rng = np.random.default_rng(seed)
        draws = [float(np.mean([v for i in rng.integers(0, len(clusters), len(clusters)) for v in clusters[i]])) for _ in range(resamples)]
        result['ticker_cluster_ci95'] = list(map(float, np.quantile(draws, [.025, .975])))
        result['status'] = 'PARTIAL'
        result['reasons'] = ['CONDITIONAL_RESEARCH_INTERVALS_NOT_CAUSAL_OR_PROMOTION_EVIDENCE']
    return result


def multiple_testing(p_values):
    """Reuse the repository's BH adjustment; absent tests remain absent."""
    return benjamini_hochberg(p_values)

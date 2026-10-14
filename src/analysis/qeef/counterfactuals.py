"""QEEF boundary around the canonical Decision Lab accounting kernel.

No strategy generation or execution: supplied plans and alternatives are frozen
before this evaluator sees outcome prices. QEEF CASH liquidates the portfolio;
Decision Lab's original affected-sleeve CASH remains unchanged.
"""
from __future__ import annotations

import json
from decimal import Decimal
from src.decision_lab.models import Episode, Order, canonical, digest
from src.decision_lab.counterfactuals import evaluate_episode
from src.decision_lab.state import EvidenceIndex, InsufficientEvidence
from .contracts import Benchmark


def freeze_alternatives(episode: Episode, benchmarks: tuple[Benchmark, ...] = ()) -> Episode:
    state = episode.state
    if len({b.name for b in benchmarks}) != len(benchmarks):
        raise ValueError('duplicate benchmark definition')
    existing = {a['name']: a for a in json.loads(episode.alternatives_json)}
    alternatives = [existing['PLAN']]
    for name in ('NO_TRADE', 'HOLD_EXISTING'):
        alternatives.append({'name': name, 'orders': [], 'fraction': '0', 'reason': None})
    orders = [Order(ticker=p.ticker, side='SELL', quantity=p.quantity,
                    reference_price=p.mark_ars, target_amount_ars=p.quantity*p.mark_ars,
                    executable=True, action='SELL_FULL').model_dump(mode='json')
              for p in state.positions if p.quantity]
    alternatives.append({'name': 'CASH', 'orders': orders, 'fraction': '1', 'reason': None})
    for name in ('REBALANCE_BASELINE', 'ELIGIBLE_ALTERNATIVE'):
        b = next((b for b in benchmarks if b.name == name), None)
        if b:
            if b.owner != state.owner or b.state_id != state.state_id:
                raise ValueError('benchmark owner/state mismatch')
            if max(b.defined_at, b.selection_available_at) > state.as_of:
                raise ValueError('benchmark selected with future information')
            if any(o.side == 'BUY' and o.ticker not in b.eligible_tickers for o in b.orders):
                raise ValueError('benchmark outside frozen feasible set')
            if any(not o.executable or o.blocked for o in b.orders):
                raise ValueError('benchmark contains blocked order')
        alternatives.append({'name': name, 'orders': [o.model_dump(mode='json') for o in b.orders] if b else [],
                             'fraction': '1', 'reason': None if b else 'EX_ANTE_BENCHMARK_NOT_RECORDED'})
    payload = {'parent_episode': episode.episode_id, 'alternatives': alternatives,
               'cost_model': episode.cost_model.model_dump(mode='json'),
               'benchmark_definitions': [b.model_dump(mode='json') for b in benchmarks]}
    # Revalidate the complete immutable model, rather than relying on model_copy.
    return Episode.model_validate({**episode.model_dump(mode='json'),
                                   'episode_id': digest(payload), 'alternatives_json': canonical(alternatives)})


def _basis_reasons(episode, index, evaluated_as_of, horizon):
    state = episode.state
    reasons = []
    if episode.plan.state_id != state.state_id:
        reasons.append('PLAN_STATE_MISMATCH')
    if episode.plan.mode == 'HISTORICAL_POLICY_REPLAY' and max(episode.plan.strategy.available_at, episode.plan.strategy.config_available_at) > state.as_of:
        reasons.append('FUTURE_POLICY_VERSION')
    if episode.plan.strategy.training_end and episode.plan.strategy.training_end >= state.as_of:
        reasons.append('TRAINING_REACHES_DECISION')
    portfolio = [e for e in state.records if e.kind == 'PORTFOLIO' and e.record_id == state.portfolio_snapshot_id and e.owner == state.owner]
    if len(portfolio) != 1:
        reasons.append('EXACT_PORTFOLIO_EVIDENCE_MISSING')
    else:
        p = portfolio[0].payload
        actual = sorted((x.ticker, x.quantity, x.mark_ars) for x in state.positions)
        recorded = sorted((x['ticker'], Decimal(str(x['quantity'])), Decimal(str(x['mark_ars']))) for x in p.get('positions', []))
        if p.get('cash_ars') is None or Decimal(str(p['cash_ars'])) != state.cash_ars or actual != recorded:
            reasons.append('PORTFOLIO_STATE_DIFFERS_FROM_FROZEN_EVIDENCE')
    if state.cash_ars + sum((p.quantity*p.mark_ars for p in state.positions), Decimal(0)) != state.capital_base_ars:
        reasons.append('INITIAL_CAPITAL_NOT_RECONCILED')
    for e in state.records:
        if not e.known_at(state.as_of):
            reasons.append('FUTURE_DECISION_INPUT')
        if e.owner is not None and e.owner != state.owner:
            reasons.append('OWNER_MISMATCH')
    future = [s for s in index.ordered_sessions if s.open_at > state.as_of][:horizon]
    if len(future) < horizon or future[-1].close_at > evaluated_as_of:
        return reasons  # kernel reports censoring or missing calendar
    tickers = {p.ticker for p in state.positions if p.quantity} | {
        o['ticker'] for a in json.loads(episode.alternatives_json) for o in a['orders']}
    bars = index.bars(evaluated_as_of, tickers, owner=state.owner)
    for ticker in sorted(tickers):
        for session in future:
            e = bars.get((ticker, session.session_id))
            if e is None:
                reasons.append(f'MISSING_BAR:{ticker}:{session.session_id}')
                continue
            p = e.payload
            if p.get('currency') != 'ARS' or p.get('venue') != 'BYMA':
                reasons.append(f'CURRENCY_OR_VENUE_UNVERIFIED:{ticker}')
            if p.get('asset_type') not in {'CEDEAR', 'ACCION'}:
                reasons.append(f'INSTRUMENT_BASIS_UNKNOWN:{ticker}')
            if p.get('asset_type') == 'CEDEAR' and (p.get('depositary_ratio') is None or Decimal(str(p['depositary_ratio'])) <= 0):
                reasons.append(f'CEDEAR_RATIO_UNKNOWN:{ticker}')
            if p.get('price_mode') != 'RAW_AS_TRADED':
                reasons.append(f'SHARE_BASIS_NOT_RAW:{ticker}')
            if e.available_at < session.close_at or p.get('is_closed') is not True:
                reasons.append(f'BAR_CLOSURE_NOT_VERIFIED:{ticker}')
            if e.quality != 'POINT_IN_TIME_SAFE':
                reasons.append(f'PRICE_LINEAGE_NOT_VERIFIED:{ticker}')
        # Ratio changes require an explicit quantity rebase in the same window.
        observed = [(s, bars.get((ticker, s.session_id))) for s in future]
        previous = None
        for session, e in observed:
            if e is None or e.payload.get('depositary_ratio') is None:
                continue
            ratio = Decimal(str(e.payload['depositary_ratio']))
            if previous and ratio != previous[1]:
                actions = [a for a in index.visible('CORPORATE_ACTION', evaluated_as_of, state.owner)
                           if a.payload.get('ticker') == ticker and a.payload.get('type') == 'RATIO_CHANGE'
                           and previous[0].close_at < a.effective_at <= session.open_at]
                if len(actions) != 1 or Decimal(str(actions[0].payload.get('quantity_factor', 0))) != ratio/previous[1]:
                    reasons.append(f'RATIO_CHANGE_UNRECONCILED:{ticker}')
            previous = session, ratio
    return sorted(set(reasons))


def evaluate_counterfactuals(episode: Episode, dataset, *, evaluated_as_of, horizons=(5, 10, 20)):
    if evaluated_as_of.tzinfo is None or evaluated_as_of < episode.state.as_of:
        raise ValueError('invalid evaluation cutoff')
    if not horizons or any(h <= 0 for h in horizons) or len(set(horizons)) != len(horizons):
        raise ValueError('unique positive session horizons required')
    index = EvidenceIndex(dataset)
    output = []
    for horizon in horizons:
        try:
            reasons = _basis_reasons(episode, index, evaluated_as_of, horizon)
        except InsufficientEvidence as exc:
            reasons = list(exc.reasons)
        if reasons:
            for alt in json.loads(episode.alternatives_json):
                output.append({'alternative': alt['name'], 'horizon': horizon, 'status': 'NOT_IDENTIFIABLE',
                               'reasons': reasons, 'net_return': None, 'economic_pnl_net': None,
                               'result_kind': 'COUNTERFACTUAL', 'censoring': 'UNKNOWN'})
            continue
        rows = evaluate_episode(episode, index, evaluated_as_of=evaluated_as_of, horizons=(horizon,))
        for row in rows:
            status = {'MATURE': 'VERIFIED' if row.primary_eligible else 'PARTIAL',
                      'PENDING': 'INSUFFICIENT_DATA', 'UNAVAILABLE': 'NOT_IDENTIFIABLE',
                      'NOT_APPLICABLE': 'NOT_IDENTIFIABLE', 'INVALID': 'INVALID'}[row.status]
            output.append({**row.model_dump(mode='json'), 'kernel_status': row.status, 'status': status,
                           'reasons': [row.reason] if row.reason else list(row.warnings) or
                           ([] if status == 'VERIFIED' else ['STATE_NOT_PRIMARY_ELIGIBLE']),
                           'result_kind': 'COUNTERFACTUAL', 'economic_pnl_net': None,
                           'censoring': 'RIGHT_CENSORED_AT_AS_OF' if row.status == 'PENDING' else
                           'NONE' if row.status == 'MATURE' else 'UNKNOWN'})
    return output


def sensitivity(episode, dataset, *, cost_models, benchmarks=(), evaluated_as_of, horizons=(5, 10, 20)):
    """Predeclared research cost scenarios; never rank or promote a winning cost."""
    if not cost_models or len({c.version for c in cost_models}) != len(cost_models):
        raise ValueError('distinct explicitly named cost scenarios required')
    result = []
    for costs in cost_models:
        candidate = Episode.model_validate({**episode.model_dump(mode='json'), 'cost_model': costs.model_dump(mode='json')})
        frozen = freeze_alternatives(candidate, tuple(benchmarks))
        result.append({'cost_model': costs.model_dump(mode='json'), 'cost_hash': digest(costs),
            'outcomes': evaluate_counterfactuals(frozen, dataset, evaluated_as_of=evaluated_as_of, horizons=horizons)})
    return result

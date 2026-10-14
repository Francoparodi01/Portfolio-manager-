"""Synthetic contract/accounting tests; never economic profitability evidence."""
import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from types import SimpleNamespace

import numpy as np
import pytest

from src.decision_lab.models import (CostModel, Dataset, Evidence, Experiment, FrozenPlan,
    HistoricalState, Order, Position, QualityAssessment, Session, StrategySpec, canonical, digest)
from src.decision_lab.counterfactuals import freeze_episode
from src.analysis.qeef.contracts import Benchmark, Condition, Gate, Thesis, ThesisEvent
from src.analysis.qeef.counterfactuals import freeze_alternatives, evaluate_counterfactuals
from src.analysis.qeef.historical import adapt_history, compare_history
from src.analysis.qeef.lifecycle import project_thesis
from src.analysis.qeef.metrics import (allocation_quality, attribution, detection_quality,
    multiple_testing, paired_confidence, realized_value, thesis_quality)
from src.analysis.qeef.service import evaluate, write_report

T = datetime(2026, 3, 2, 21, tzinfo=timezone.utc)  # after BYMA close


def ev(kind, payload, *, at=T, available=None, owner=None, rid=None):
    return Evidence(kind=kind, record_id=rid or digest([kind, payload, at]), effective_at=at,
        available_at=available or at, source='SYNTHETIC_TEST', quality='POINT_IN_TIME_SAFE',
        owner=owner, payload_json=canonical(payload))


def fixture(*, side='SELL', quantity=4, cost=0, close=110, split=None, ratio_change=False):
    """Common capital 2,000 ARS, 10 shares at 100 and 1,000 cash; explicit sessions."""
    positions = (Position(ticker='AAA', quantity=10, mark_ars=100),)
    portfolio = ev('PORTFOLIO', {'positions': [p.model_dump(mode='json') for p in positions], 'cash_ars': 1000}, owner=1, rid='portfolio')
    universe = ev('UNIVERSE', {'instruments': [{'ticker': 'AAA', 'lot_size': 1, 'enabled': True, 'operable': True}], 'universe_quality': 'EXACT'})
    quality = QualityAssessment(level='HIGH', primary_eligible=True, components_json='{}')
    state = HistoricalState(as_of=T, owner=1, state_id='state1', opportunity_id='opp1', records=(portfolio, universe),
        positions=positions, cash_ars=1000, capital_base_ars=2000, portfolio_snapshot_id='portfolio',
        market_snapshot_id='market', feature_snapshot_id=None, universe_snapshot_id='universe',
        universe_quality='EXACT', input_hash=digest([portfolio, universe]), quality=quality)
    spec = StrategySpec(strategy_version='SYNTHETIC_TEST', adapter='recorded_plan_v1', available_at=T,
        code_version='fixture', config_hash='config', config_available_at=T, planner_version='fixture',
        optimizer_version='fixture', risk_policy_version='fixture', implementation_hash='fixture')
    order = Order(ticker='AAA', side=side, quantity=quantity, reference_price=100,
        target_amount_ars=quantity*100, executable=True, action=side)
    plan = FrozenPlan(strategy=spec, mode='RECORDED_PLAN_EVALUATION', state_id=state.state_id, orders=(order,),
        affected_tickers=('AAA',), mechanism_json='{}', feasible=True, plan_hash=digest(order))
    episode = freeze_episode(state, plan, CostModel(fee_bps=cost, return_basis='TOTAL_RETURN'),
        Experiment(experiment_id='fixture', registered_at=T))
    sessions, records = [], []
    for i in range(1, 22):
        opened = T + timedelta(days=i, hours=-6)
        session = Session(session_id=str(i), open_at=opened, close_at=opened+timedelta(hours=5))
        sessions.append(session)
        factor = split if split and i >= 3 else 1
        records.append(ev('BAR', {'ticker': 'AAA', 'session_id': str(i), 'open': 100/factor,
            'close': close/factor, 'high': 120/factor, 'low': 90/factor, 'volume': 1000,
            'price_mode': 'RAW_AS_TRADED', 'currency': 'ARS', 'venue': 'BYMA', 'asset_type': 'CEDEAR',
            'depositary_ratio': 10*factor if ratio_change else 10, 'is_closed': True}, at=session.close_at))
    if split:
        records.append(ev('CORPORATE_ACTION', {'ticker': 'AAA', 'type': 'RATIO_CHANGE' if ratio_change else 'SPLIT',
            'quantity_factor': split}, at=sessions[2].open_at))
    records.append(ev('ACTION_COVERAGE', {'ticker': 'AAA', 'complete': True, 'from': T.isoformat(),
        'to': sessions[-1].close_at.isoformat()}, at=sessions[-1].close_at))
    return episode, Dataset(records=tuple([portfolio, universe, *records]), sessions=tuple(sessions), calendar_version='SYNTHETIC_TEST'), sessions[-1].close_at


def outcomes(ep, ds, cutoff, horizons=(5,)):
    return {r['alternative']: r for r in evaluate_counterfactuals(freeze_alternatives(ep), ds,
        evaluated_as_of=cutoff, horizons=horizons)}


def thesis():
    return Thesis(owner=1, opportunity_id='opp', thesis_id='thesis', ticker='MU', strategy_version='test',
        thesis_opened_at=T, recorded_at=T, intended_horizon=20, horizon_unit='BYMA_SESSIONS',
        invalidation_conditions=(Condition(condition_id='stop', field='price', operator='LE', value=90, unit='ARS', source='BYMA'),),
        target_conditions=(Condition(condition_id='target', field='price', operator='GE', value=120, unit='ARS', source='BYMA'),),
        source_references=('source1',), evidence_quality='HIGH')


def test_repeated_holds_then_buy_remain_one_frozen_thesis():
    t = thesis()
    events = tuple(ThesisEvent(owner=1, thesis_id=t.thesis_id, thesis_hash=t.content_hash, event_id=str(i),
        effective_at=T+timedelta(days=i), available_at=T+timedelta(days=i), event_type='UPDATE',
        decision_id='hold'+str(i) if i < 10 else 'buy10', recommendation_id=str(i), source_references=('r',)) for i in range(1, 11))
    result = project_thesis(t, events+events[:1], as_of=T+timedelta(days=11))
    assert result['decision_count'] == 10 and result['opportunity_count'] == 1
    assert result['censoring'] == 'RIGHT_CENSORED_AT_AS_OF'
    assert t.content_hash == thesis().content_hash


def test_thesis_cannot_retroactively_change_target_or_cross_owner():
    t = thesis()
    args = dict(owner=1, thesis_id=t.thesis_id, thesis_hash=t.content_hash, event_id='e', effective_at=T,
        available_at=T, event_type='TARGET', condition_id='target', observed_value=119,
        observed_unit='ARS', observed_source='BYMA', source_references=('r',))
    with pytest.raises(ValueError, match='not satisfied'):
        project_thesis(t, (ThesisEvent(**args),), as_of=T)
    args['observed_value'] = 120
    result = project_thesis(t, (ThesisEvent(**args),), as_of=T)
    assert result['closure_reason'] == 'TARGET'
    args['owner'] = 2
    with pytest.raises(ValueError, match='mismatch'):
        project_thesis(t, (ThesisEvent(**args),), as_of=T)


def test_buy_without_execution_is_counterfactual_not_realized():
    ep, ds, cutoff = fixture(side='BUY')
    report = evaluate(episode=ep, dataset=ds, evaluated_as_of=cutoff, code_version='fixture', horizons=(5,))
    assert report['dimensions']['E_REALIZED_ECONOMIC_VALUE']['economic_pnl_net'] is None
    assert all(r['result_kind'] == 'COUNTERFACTUAL' and r['economic_pnl_net'] is None for r in report['outcomes'])
    assert not report['affects_execution'] and not report['promotion_allowed']


def test_partial_fill_uses_actual_quantity_and_preserves_capital_identity():
    ep, ds, cutoff = fixture(quantity=2, cost=100)
    result = outcomes(ep, ds, cutoff)['PLAN']
    assert Decimal(result['ending_nav_ars']) == Decimal('2078.00')
    assert result['net_return'] == pytest.approx(.039)
    assert result['gross_return']-result['net_return'] == pytest.approx(result['cost_drag'])
    # This tests a supplied partial plan, not an inferred actual broker fill.
    assert json.loads(result['executed_orders_json'])[0]['quantity'] == '2'


def test_after_close_uses_next_session_open():
    ep, ds, cutoff = fixture()
    result = outcomes(ep, ds, cutoff)['PLAN']
    assert datetime.fromisoformat(result['entry_at']) > T
    assert datetime.fromisoformat(result['entry_at']) == ds.sessions[0].open_at


def test_future_input_rejected_even_if_state_claims_high_quality():
    ep, ds, cutoff = fixture()
    leaked = ev('MACRO', {'vix': 15}, available=T+timedelta(days=1))
    state = ep.state.model_copy(update={'records': ep.state.records+(leaked,)})
    result = outcomes(ep.model_copy(update={'state': state}), ds, cutoff)
    assert result['PLAN']['net_return'] is None
    assert 'FUTURE_DECISION_INPUT' in result['PLAN']['reasons']
    with pytest.raises(ValueError, match='future outcome'):
        ev('FEATURES', {'nested': {'outcome_5d': .2}}, owner=1)


@pytest.mark.parametrize('missing_session', ['1', '2', '5'])
def test_new_or_short_history_never_fills_missing_prices(missing_session):
    ep, ds, cutoff = fixture()
    short = ds.model_copy(update={'records': tuple(e for e in ds.records if e.kind != 'BAR' or e.payload['session_id'] != missing_session)})
    result = outcomes(ep, short, cutoff)['PLAN']
    assert result['status'] == 'NOT_IDENTIFIABLE' and result['net_return'] is None
    assert any('MISSING_BAR' in r for r in result['reasons'])


@pytest.mark.parametrize('ratio_change', [False, True])
def test_split_and_cedear_ratio_do_not_create_profit(ratio_change):
    ep, ds, cutoff = fixture(split=2, ratio_change=ratio_change, close=100)
    result = outcomes(ep, ds, cutoff)
    assert result['PLAN']['net_return'] == pytest.approx(0)
    assert result['NO_TRADE']['net_return'] == pytest.approx(0)


def test_unexplained_ratio_change_not_identifiable():
    ep, ds, cutoff = fixture(split=2, ratio_change=True, close=100)
    ds = ds.model_copy(update={'records': tuple(e for e in ds.records if e.kind != 'CORPORATE_ACTION')})
    assert 'RATIO_CHANGE_UNRECONCILED:AAA' in outcomes(ep, ds, cutoff)['PLAN']['reasons']


@pytest.mark.parametrize('field,value', [('currency', 'USD'), ('venue', 'NASDAQ'), ('depositary_ratio', None), ('price_mode', 'CURRENTLY_ADJUSTED_HISTORY')])
def test_price_basis_changes_cannot_create_artificial_returns(field, value):
    ep, ds, cutoff = fixture()
    records = tuple(e.model_copy(update={'payload_json': canonical({**e.payload, field: value})}) if e.kind == 'BAR' else e for e in ds.records)
    result = outcomes(ep, ds.model_copy(update={'records': records}), cutoff)['PLAN']
    assert result['status'] == 'NOT_IDENTIFIABLE' and result['net_return'] is None


def test_missing_corporate_action_coverage_blocks_total_return():
    ep, ds, cutoff = fixture()
    ds = ds.model_copy(update={'records': tuple(e for e in ds.records if e.kind != 'ACTION_COVERAGE')})
    assert outcomes(ep, ds, cutoff)['PLAN']['net_return'] is None


def test_hold_no_trade_identical_and_cash_sells_whole_portfolio():
    ep, ds, cutoff = fixture()
    result = outcomes(ep, ds, cutoff)
    assert result['HOLD_EXISTING']['net_return'] == result['NO_TRADE']['net_return'] == pytest.approx(.05)
    assert result['PLAN']['net_return'] == pytest.approx(.03)
    assert result['CASH']['net_return'] == pytest.approx(0)
    assert json.loads(result['CASH']['executed_orders_json'])[0]['quantity'] == '10'


def test_costs_exceed_gross_gain_and_cost_monotonicity():
    values = []
    for cost in (0, 25, 100, 500):
        ep, ds, cutoff = fixture(side='BUY', close=100.1, cost=cost)
        values.append(outcomes(ep, ds, cutoff)['PLAN']['net_return'])
    assert values == sorted(values, reverse=True) and values[-1] < 0 < values[0]


def pair(i=0, **changes):
    return {'owner': 1, 'strategy_version': 'v1', 'cost_hash': 'c', 'benchmark_hash': 'b', 'horizon': 5,
            'quality': 'HIGH', 'regime': 'UP', 'opportunity_id': str(i), 'ticker': 'MU',
            'status': 'MATURE', 'decision_at': (T+timedelta(days=i)).isoformat(),
            'exit_at': (T+timedelta(days=i+7)).isoformat(), 'delta': .01, **changes}


def test_overlapping_decisions_and_small_samples_have_no_fake_interval():
    result = paired_confidence([pair(i) for i in range(9)], horizon=5)
    assert result['n_nonoverlapping_proxy'] == 2 and result['n_mature'] == 9
    assert result['temporal_ci95'] is None and not result['promotion_allowed']
    single = paired_confidence([pair()], horizon=5)
    assert single['temporal_ci95'] is None
    with pytest.raises(ValueError, match='pseudoreplication'):
        paired_confidence([pair(), pair(delta=-.5)], horizon=5)


@pytest.mark.parametrize('change', [{'owner': 2}, {'regime': 'DOWN'}, {'horizon': 10}, {'quality': 'LOW'}, {'strategy_version': 'v2'}])
def test_owners_regimes_and_cohorts_cannot_be_pooled(change):
    with pytest.raises(ValueError, match='incompatible'):
        paired_confidence([pair(), pair(1, **change)], horizon=5)


def test_future_benchmark_rejected_and_missing_benchmark_unknown():
    ep, ds, cutoff = fixture()
    kwargs = dict(name='REBALANCE_BASELINE', owner=1, state_id=ep.state.state_id,
        defined_at=T, selection_available_at=T, policy_version='frozen', policy_hash='hash',
        source_references=('policy',), eligible_tickers=('AAA',), orders=ep.plan.orders)
    b = Benchmark(**kwargs)
    frozen = freeze_alternatives(ep, (b,))
    rows = evaluate_counterfactuals(frozen, ds, evaluated_as_of=cutoff, horizons=(5,))
    lookup = {r['alternative']: r for r in rows}
    assert lookup['REBALANCE_BASELINE']['net_return'] == lookup['PLAN']['net_return']
    assert lookup['ELIGIBLE_ALTERNATIVE']['status'] == 'NOT_IDENTIFIABLE'
    kwargs['selection_available_at'] = T+timedelta(seconds=1)
    with pytest.raises(ValueError, match='future'):
        freeze_alternatives(ep, (Benchmark(**kwargs),))


def test_open_horizons_are_censored_and_not_losses():
    ep, ds, cutoff = fixture()
    row = outcomes(ep, ds, T+timedelta(days=2))['PLAN']
    assert row['censoring'] == 'RIGHT_CENSORED_AT_AS_OF'
    assert row['net_return'] is None


def test_identical_report_rerun_idempotent_and_mutation_rejected(tmp_path):
    ep, ds, cutoff = fixture()
    a = evaluate(episode=ep, dataset=ds, evaluated_as_of=cutoff, code_version='test', horizons=(5,))
    b = evaluate(episode=ep, dataset=ds, evaluated_as_of=cutoff, code_version='test', horizons=(5,))
    assert a == b
    write_report(a, tmp_path)
    write_report(b, tmp_path)
    assert len(list(tmp_path.iterdir())) == 2
    with pytest.raises(FileExistsError):
        write_report({**b, 'code_version': 'different'}, tmp_path)


def test_legacy_owner_unknown_cannot_be_promoted():
    with pytest.raises(ValueError, match='owner-scoped'):
        adapt_history({'schema_version': 'qeef-capture-v1', 'transaction_read_only': 'on', 'owner_token': None})
    with pytest.raises(ValueError, match='cannot be HIGH'):
        Thesis.model_validate({**thesis().model_dump(), 'formation_kind': 'HISTORICAL_INFERENCE'})


def test_allocation_risk_contributions_sum_to_volatility():
    result = allocation_quality({'A': .5, 'B': .3, 'CASH': .2}, covariance=[[.04, .01, 0], [.01, .09, 0], [0, 0, 0]], max_weight=.4)
    risk = result['covariance_risk']
    assert sum(risk['risk_contribution'].values()) == pytest.approx(risk['volatility_per_covariance_period'])
    assert result['limit_breaches'] == ['A'] and result['capital_utilization'] == .8
    with pytest.raises(ValueError):
        allocation_quality({'A': .8, 'CASH': .3})
    from src.analysis.qeef.metrics import compare_allocations
    compared = compare_allocations({'A': .5, 'CASH': .5}, {'A': .8, 'CASH': .2})
    assert compared['one_way_reallocation_including_cash'] == pytest.approx(.3)
    assert compared['incremental_net_return'] is None


def test_opportunity_recall_requires_complete_universe_and_frozen_risk_cost_rule():
    definition = {'registered_at': T.isoformat(), 'risk_limit': .1, 'cost_policy': 'cost1', 'liquidity_rule': 'liq1',
        'horizon': 5, 'universe_ref': 'u1', 'universe_complete': True}
    universe = [{'ticker': t, 'available_at': T.isoformat(), 'label': label, 'score': score,
        'label_available_at': (T+timedelta(days=5)).isoformat()} for t, label, score in [('A', True, 3), ('B', True, 2), ('C', False, 1)]]
    result = detection_quality(universe, decision_at=T, evaluated_at=T+timedelta(days=6), definition=definition, selected=['A'])
    assert result['precision'] == 1 and result['recall'] == .5 and result['missed_opportunities'] == 1
    assert result['ranking_ndcg_at_selection_size'] == 1
    universe[-1]['label'] = None
    assert detection_quality(universe, decision_at=T, evaluated_at=T+timedelta(days=6), definition=definition, selected=['A'])['recall'] is None


def test_component_attribution_unknown_without_controlled_ablations():
    assert all(r['causal_share'] is None and r['status'] == 'NOT_IDENTIFIABLE' for r in attribution([]))
    assert multiple_testing({'a': .01, 'b': None}) == {'a': .02, 'b': None}


def test_thesis_relevance_is_not_indicator_count():
    result = thesis_quality(thesis(), [{'reference': 'source1', 'available_at': T.isoformat(), 'relation': 'SUPPORTS'}])
    assert result['DATA_QUALITY']['status'] == 'VERIFIED'
    assert result['PREDICTIVE_QUALITY'] is None and result['ECONOMIC_PERFORMANCE'] is None


def test_realized_nav_reuses_accounting_and_refuses_unknown_costs():
    from examples.analytics_v2.sample import sample
    dataset, policy = sample()
    owner = dataset.recommendations[0].account_id
    results = realized_value(dataset, owner=owner, evaluated_as_of=policy.evaluated_as_of)
    assert results['periods'] and all(r['result_kind'] == 'REALIZED_ACCOUNT' for r in results['periods'])
    empty = dataset.model_copy(update={'economic_periods': ()})
    assert realized_value(empty, owner=owner, evaluated_as_of=policy.evaluated_as_of)['periods'][0]['economic_pnl_net'] is None
    with pytest.raises(ValueError, match='another owner'):
        realized_value(empty, owner='different', evaluated_as_of=policy.evaluated_as_of)


def test_verified_defect_singleton_legacy_ci_is_degenerate():
    from src.analysis.dcl.statistical_auditor import _bootstrap_mean_ci
    assert _bootstrap_mean_ci([.02]) == (.02, .02)
    assert paired_confidence([pair()], horizon=5)['temporal_ci95'] is None


def test_verified_defect_legacy_drawdown_depends_on_order_of_decisions():
    from src.analysis.viability_audit import _max_drawdown
    assert _max_drawdown([-.1, -.1, .2]) != _max_drawdown([-.1, .2, -.1])


def test_gate_requires_reason_and_no_unknown_means_zero():
    with pytest.raises(ValueError, match='requires reasons'):
        Gate(status='NOT_IDENTIFIABLE')
    assert Gate(status='NOT_IDENTIFIABLE', reasons=('MISSING_PRICE',)).status == 'NOT_IDENTIFIABLE'


def test_monitor_rejects_owner_override_before_any_db_access(monkeypatch):
    from aiohttp import web
    from src.monitor.qeef import qeef_view
    monkeypatch.setenv('TELEGRAM_CHAT_ID', '1')
    request = SimpleNamespace(query={'owner_chat_id': '2'}, app={})
    with pytest.raises(web.HTTPForbidden):
        asyncio.run(qeef_view(request))


def test_observed_partial_execution_does_not_impute_remainder():
    from src.analysis.qeef.metrics import execution_quality
    fill = {'execution_id': 'f', 'owner': 1, 'order_id': 'o', 'ticker': 'MU', 'side': 'BUY',
        'quantity': 2, 'price': 102, 'fees': 3, 'executed_at': (T+timedelta(days=1)).isoformat()}
    kwargs = dict(owner=1, decision_at=T, order_id='o', ticker='MU', side='BUY', requested_quantity=10,
        reference_price=100, fills=[fill, fill], coverage_confirmed=True)
    result = execution_quality(**kwargs)
    assert result['execution_state'] == 'PARTIAL' and result['fill_ratio'] == .2
    assert result['shortfall_cost_vs_frozen_reference'] == 4 and result['economic_pnl_net'] is None
    assert execution_quality(**{**kwargs, 'fills': [], 'coverage_confirmed': False})['fill_ratio'] is None
    with pytest.raises(ValueError, match='mismatch'):
        execution_quality(**{**kwargs, 'owner': 2})


def test_source_sndk_history_is_never_extended_with_another_ticker():
    from src.decision_lab.state import EvidenceIndex
    ep, ds, cutoff = fixture()
    # All bars are AAA: a newly listed SNDK must have no borrowed history.
    assert EvidenceIndex(ds).bars(cutoff, {'SNDK'}, owner=1) == {}


def test_currency_unit_scaling_preserves_return_property():
    for scale in (Decimal('.01'), Decimal('1'), Decimal('100')):
        ep, ds, cutoff = fixture()
        state = ep.state.model_copy(update={'positions': tuple(p.model_copy(update={'mark_ars': p.mark_ars*scale}) for p in ep.state.positions),
            'cash_ars': ep.state.cash_ars*scale, 'capital_base_ars': ep.state.capital_base_ars*scale})
        state_records = tuple(e.model_copy(update={'payload_json': canonical({'cash_ars': str(state.cash_ars),
            'positions': [p.model_dump(mode='json') for p in state.positions]})}) if e.kind == 'PORTFOLIO' else e for e in state.records)
        state = state.model_copy(update={'records': state_records})
        orders = tuple(o.model_copy(update={'reference_price': o.reference_price*scale, 'target_amount_ars': o.target_amount_ars*scale}) for o in ep.plan.orders)
        scaled_ep = ep.model_copy(update={'state': state, 'plan': ep.plan.model_copy(update={'orders': orders})})
        records = tuple(e.model_copy(update={'payload_json': canonical({**e.payload, **{k: str(Decimal(str(e.payload[k]))*scale) for k in ('open','close','high','low')}})}) if e.kind == 'BAR' else e for e in ds.records)
        assert outcomes(scaled_ep, ds.model_copy(update={'records': records}), cutoff)['PLAN']['net_return'] == pytest.approx(.03)


def test_named_cost_sensitivity_and_benchmark_sensitivity_use_common_state():
    from src.analysis.qeef.counterfactuals import sensitivity
    ep, ds, cutoff = fixture()
    result = sensitivity(ep, ds, evaluated_as_of=cutoff, horizons=(5,),
        cost_models=(CostModel(version='zero', fee_bps=0, return_basis='TOTAL_RETURN'), CostModel(version='cost100', fee_bps=100, return_basis='TOTAL_RETURN')))
    plan = [next(r for r in s['outcomes'] if r['alternative']=='PLAN') for s in result]
    assert plan[0]['net_return'] > plan[1]['net_return']
    assert plan[0]['capital_base_ars'] == plan[1]['capital_base_ars']


def test_portfolio_capital_shortfall_does_not_invent_leverage():
    ep, ds, cutoff = fixture(side='BUY', quantity=100)
    row = outcomes(ep, ds, cutoff)['PLAN']
    assert row['net_return'] is None and 'FROZEN_PLAN_CASH_SHORTFALL_AT_NEXT_OPEN' in row['reasons']


def test_history_same_rows_preserve_raw_directional_outcomes_and_missingness():
    row = {'id': 1, 'ticker': 'MU', 'decision': 'BUY', 'decided_at': T.isoformat(), 'source': 'radar',
        'metric_scope': 'radar_audit', 'status': 'THEORETICAL', 'outcome_5d': .1,
        'outcome_filled_at': (T+timedelta(days=7)).isoformat()}
    capture = {'schema_version': 'qeef-capture-v1', 'transaction_read_only': 'on', 'owner_token': 'token',
        'captured_at': (T+timedelta(days=10)).isoformat(), 'tables': {'decision_log': [row]}}
    cases = adapt_history(capture)
    comparisons = compare_history(cases)
    assert comparisons[0]['legacy_mean_recorded_outcome'] == .1
    assert all(r['qeef_economic_net_mean'] is None for r in comparisons)
    assert row['outcome_5d'] == .1 and cases[0]['opportunity_id'] is None
    capture['captured_at'] = (T+timedelta(days=6)).isoformat()
    assert adapt_history(capture)[0]['outcomes']['5']['legacy_recorded_directional_return'] is None


def test_verified_legacy_episode_partition_omits_cohort_but_qeef_keeps_scopes():
    from examples.analytics_v2.sample import sample
    from src.analysis.analytics_v2.episodes import build_episodes
    data, policy = sample()
    rec = data.recommendations[0]
    second = rec.model_copy(update={'recommendation_id': 'changed-cohort', 'cohort': 'different-cohort',
        'decision_as_of': rec.decision_as_of+timedelta(minutes=1)})
    episodes, _ = build_episodes([rec, second], policy.dedup_policy_version)
    assert len(episodes) == 1 and episodes[0]['recommendation_count'] == 2
    base = {'ticker': 'MU', 'decision': 'BUY', 'decided_at': T.isoformat(), 'source': 'execution_plan', 'status': 'APPROVED'}
    capture = {'schema_version': 'qeef-capture-v1', 'transaction_read_only': 'on', 'owner_token': 'owner',
        'captured_at': T.isoformat(), 'tables': {'decision_log': [{**base, 'id': 1, 'metric_scope': 'primary'}, {**base, 'id': 2, 'metric_scope': 'planner_audit'}]}}
    rows = adapt_history(capture)
    assert len({r['recurrence_proxy_id'] for r in rows}) == 2

"""Cash-only entry planning, installed exclusively in the isolated replay process."""
from contextvars import ContextVar
from functools import wraps

CASH_START_POLICY = 'ELIGIBLE_RADAR_V1'


def prepare_spec(spec):
    """Version new runs, including requests queued by an older Telegram process."""
    prepared = dict(spec)
    prepared.update(cash_start_policy=CASH_START_POLICY,
                    variant='FULL_CASH_START',
                    methodology='CURRENT_FULL_ENGINE_ON_HISTORICAL_CAPTURE_V2')
    return prepared


def install_cash_bridge(engine):
    """Complete the missing cash-only plan before native guards and rendering.

    No source rewriting: existing plans and portfolios with holdings stay native.
    Each replay cut has its own context; candidates never carry forward.
    """
    from src.analysis.optimizer_core import _get_risk_gate_state

    context = ContextVar('backtest_cash_start', default=None)
    native_main = engine.main
    native_opportunity = engine.run_opportunity_analysis
    native_events = engine.active_event_risk_by_ticker
    native_finish = engine._ensure_corporate_action_blocks_in_plan

    @wraps(native_opportunity)
    def opportunity(*args, **kwargs):
        report = native_opportunity(*args, **kwargs)
        state = context.get()
        if state is not None:
            state.update(report=report, macro_snap=kwargs['macro_snap'],
                         macro_regime=kwargs['macro_regime'])
        return report

    @wraps(native_events)
    def events(*args, **kwargs):
        blocks = native_events(*args, **kwargs)
        state = context.get()
        if state is not None:
            state['event_blocks'] = dict(blocks)
        return blocks

    @wraps(native_finish)
    def finish(plan, *, blocked_by_ticker, positions, cash_ars):
        state = context.get()
        if (state is not None and plan is None and not positions and cash_ars > 0
                and 'report' in state):
            candidates = engine._radar_buys_for_execution(state['report'], cash_ars)
            gate, reason = _get_risk_gate_state(
                state['macro_snap'].vix, 0.0, state['macro_regime'])
            state['audit'].update(attempted=True, candidates=len(candidates),
                                  gate=gate, gate_reason=reason)
            plan = engine.reconcile_funding(
                decisions=[], current_positions={}, cash_before=cash_ars,
                portfolio_value_ars=cash_ars, gate=gate, external_buys=candidates,
                blocked_buy_tickers=state['event_blocks'],
                blocked_trade_tickers=blocked_by_ticker)
            engine.validate_execution_plan(plan)
            state['audit']['buy_orders'] = len(plan.buy_orders)
        return native_finish(plan, blocked_by_ticker=blocked_by_ticker,
                             positions=positions, cash_ars=cash_ars)

    @wraps(native_main)
    async def main(*args, **kwargs):
        if (kwargs.get('no_persist') is not True or kwargs.get('no_telegram') is not True
                or kwargs.get('run_intent') != 'exploratory'):
            raise RuntimeError('CASH_START_REQUIRES_ISOLATED_BACKTEST')
        state = dict(event_blocks={}, audit=dict(policy=CASH_START_POLICY,
                                                attempted=False, candidates=0, buy_orders=0))
        enabled = not kwargs.get('no_optimizer', False) and not kwargs.get('skip_radar', False)
        token = context.set(state if enabled else None)
        try:
            result = await native_main(*args, **kwargs)
            result['backtest_cash_start'] = state['audit']
            return result
        finally:
            context.reset(token)

    engine.run_opportunity_analysis = opportunity
    engine.active_event_risk_by_ticker = events
    engine._ensure_corporate_action_blocks_in_plan = finish
    engine.main = main

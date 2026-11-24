"""Optional rotation policies installed only inside the isolated replay process."""
import copy
import inspect
from functools import wraps
from datetime import datetime
from zoneinfo import ZoneInfo

from src.backtesting.spec import ROTATION_DEFAULTS, rotation_overrides


def risk_exempt(decision, signal=None, *, gate=None, blocked_trade_tickers=None):
    """Preserve native safety decisions; never turn a blocked trade into an order."""
    action = getattr(decision.action, 'value', decision.action)
    if action == 'BLOCKED' or gate in ('BLOCKED','CAUTIOUS'):
        return True
    if decision.ticker in (blocked_trade_tickers or {}):
        return True
    if signal and (getattr(signal,'stop_triggered',False)
                   or getattr(signal,'structural_break_confirmed',False)):
        return True
    quality = getattr(decision,'data_quality',{}) or {}
    if quality.get('is_operable') is False or quality.get('price_status') in (
            'INVALID','BLOCKED','STALE','MISSING','UNKNOWN'):
        return True
    text = ' '.join(str(getattr(decision,k,'') or '') for k in
                    ('reason_primary','reason_secondary','sell_cause','block_code')).casefold()
    return any(word in text for word in ('stop','riesgo','risk','defensiv','ruptura',
                'concentraci','corporat','inválid','invalid','data_quality','blocked','precio no comparable'))


def install_rotation_policy(engine, spec, book, clock):
    options = rotation_overrides(spec)
    if options == ROTATION_DEFAULTS:
        return None
    from src.analysis.execution_planner import MIN_WEIGHT_DELTA, derive_decision_intents, reconcile_funding
    from src.analysis.enums import DecisionType
    sessions = {day:i for i,day in enumerate(spec['sessions'])}
    first_cut = spec['cutoffs'][0]
    art = ZoneInfo('America/Argentina/Buenos_Aires')
    audit = []
    signals = {}
    native_derive = engine.derive_decision_intents
    native_funding = engine.reconcile_funding
    # Replay's observation wrappers deliberately have generic signatures.
    derive_signature = inspect.signature(derive_decision_intents)
    funding_signature = inspect.signature(reconcile_funding)

    def startup():
        return clock() == datetime.fromisoformat(first_cut)

    def since(ticker, side):
        for f in reversed(book.fills):
            if f['ticker']==ticker and f['side']==side:
                day=f['filled_at'][:10]
                return sessions[clock().astimezone(art).date().isoformat()]-sessions[day]
        return None

    def record(ticker, policy, elapsed=None):
        audit.append(dict(cutoff=clock().isoformat(),ticker=ticker,policy=policy,
                          elapsed_sessions=elapsed))

    @wraps(native_derive)
    def derive(*args, **kwargs):
        bound = derive_signature.bind(*args,**kwargs)
        signals.clear()
        signals.update(bound.arguments.get('signals_by_ticker',{}))
        if not startup() and options['delta_band_mult'] != 1:
            bound.arguments['min_weight_delta'] = MIN_WEIGHT_DELTA*options['delta_band_mult']
        return native_derive(*bound.args,**bound.kwargs)

    @wraps(native_funding)
    def funding(*args, **kwargs):
        if startup():
            return native_funding(*args,**kwargs)
        bound = funding_signature.bind(*args,**kwargs)
        values = bound.arguments
        decisions=[]
        for d in values.get('decisions',[]):
            side=getattr(d.action,'value',d.action)
            policy=None; elapsed=None
            if side=='BUY':
                elapsed=since(d.ticker,'SELL')
                if elapsed is not None and elapsed < options['redeploy_cooldown_sessions']:
                    policy='redeploy_cooldown_sessions'
            elif side in ('SELL_FULL','SELL_PARTIAL') and not risk_exempt(
                    d, signals.get(d.ticker),gate=values.get('gate'),
                    blocked_trade_tickers=values.get('blocked_trade_tickers')):
                elapsed=since(d.ticker,'BUY')
                if elapsed is not None and elapsed < options['min_hold_sessions']:
                    policy='min_hold_sessions'
            if policy:
                d=copy.copy(d)
                d.action=DecisionType.WATCH if side=='BUY' else DecisionType.HOLD
                d.reason_primary=f'BACKTEST_WAIT: {policy}'
                d.reason_secondary=f'{elapsed} sesiones desde último fill opuesto'
                record(d.ticker,policy,elapsed)
            decisions.append(d)
        values['decisions']=decisions
        if values.get('external_buys') is not None:
            buys=[]
            for candidate in values['external_buys']:
                reason=candidate.get('reason','')
                tier=options['radar_buy_tiers']
                excluded=(tier=='SIN_ESPERAR_FUNDING' and 'Esperar funding' in reason
                          or tier=='SOLO_ABRIR_POSICION' and not reason.startswith('Radar elegible: Abrir posición'))
                elapsed=since(candidate['ticker'],'SELL')
                cooldown=elapsed is not None and elapsed<options['redeploy_cooldown_sessions']
                if excluded or cooldown:
                    record(candidate['ticker'],'radar_buy_tiers' if excluded else 'redeploy_cooldown_sessions',elapsed)
                else:
                    buys.append(candidate)
            values['external_buys']=buys
        return native_funding(*bound.args,**bound.kwargs)

    engine.derive_decision_intents=derive
    engine.reconcile_funding=funding
    return audit

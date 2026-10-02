from datetime import datetime, date, timedelta, timezone
import itertools
import math
import pytest
from metrics import compute, sessions_after, trading_day

UTC = timezone.utc
AS_OF = datetime(2026, 10, 2, 18, tzinfo=UTC)


def signal(**overrides):
    r = dict(plan_id='test-plan', intent_id=1, created_at=datetime(2026, 9, 1, 18, tzinfo=UTC),
             source='execution_plan', feasible=True, is_executable=True, was_blocked=False,
             ticker='ABC', side='BUY', run_id=None)
    return dict(r, **overrides)


def prices(start=date(2026, 9, 1), n=20, **overrides):
    return [dict(dict(ticker='ABC', long_ticker='ABC-ARS', source='COCOS', currency='ARS',
                     venue='BYMA', interval='1d', ts=datetime.combine(d, datetime.min.time(),UTC),
                     scraped_at=AS_OF-timedelta(hours=1), open_price=100, close_price=110),**overrides)
            for d in sessions_after(start, n)]


def run(rows=None, candles=None, **kw):
    return compute([signal()] if rows is None else rows, prices() if candles is None else candles, as_of=AS_OF, **kw)


def status(result,h='5'):
    return result['signals'][0]['details'][h]['status']


def test_known_buy_sell_cost_and_fixed_dates():
    buy=run()
    assert buy['metrics']['5']['mean_pct'] == pytest.approx(9.25)
    assert buy['signals'][0]['entry_date']=='2026-09-02'
    assert buy['signals'][0]['details']['5']['exit_date']=='2026-09-08'
    assert run([signal(side='SELL')])['metrics']['5']['mean_pct']==pytest.approx(-10.75)
    assert run(cost_bps=0)['metrics']['5']['mean_pct']==pytest.approx(10)
    assert status(buy,'40')=='immature'


def test_empty_preserves_missing_metrics():
    r=run([],[])
    for m in r['metrics'].values():
        assert m['n']==0
        assert m['coverage_pct'] is m['mean_pct'] is m['median_pct'] is m['win_pct'] is None


@pytest.mark.parametrize('change',[{'source':'radar'},{'source':'manual'},{'source':None},
    {'feasible':False},{'is_executable':False},{'was_blocked':True},{'was_blocked':None},{'side':'HOLD'},{'ticker':' '}])
def test_ineligible_never_enter_population(change):
    r=run([signal(**change)])
    assert r['quality']['unique_signals']==0
    assert r['quality']['ineligible_intents']==1


def test_dedup_first_eligible_and_art_midnight():
    rows=[signal(intent_id=1,was_blocked=True),signal(intent_id=3),signal(intent_id=2),
          signal(intent_id=4,created_at=datetime(2026,9,2,1,tzinfo=UTC))]
    r=run(rows)
    assert r['quality']['same_day_duplicates']==2
    assert r['signals'][0]['intent_id']==2
    assert r['quality']['unique_signals']==1


def test_opposite_sides_are_distinct_signals():
    r=run([signal(),signal(intent_id=2,side='SELL')])
    assert r['metrics']['5']['n']==2
    assert r['metrics']['5']['mean_pct']==pytest.approx(-.75)
    assert r['metrics']['5']['median_pct']==pytest.approx(-.75)
    assert r['metrics']['5']['win_pct']==50


@pytest.mark.parametrize('missing_index',[0,2,4])
def test_missing_session_does_not_shift_entry_or_exit(missing_index):
    p=prices();p.pop(missing_index)
    r=run(candles=p)
    assert status(r)=='missing_or_conflicting_prices'
    assert r['metrics']['5']['mean_pct'] is None


@pytest.mark.parametrize('source',['internal_snapshot','YAHOO_BYMA','UNKNOWN'])
def test_reconstructed_or_unverified_source_excluded(source):
    assert status(run(candles=prices(source=source)))=='missing_or_conflicting_prices'


def test_fallback_is_whole_series_not_mixed_endpoints():
    cocos=prices()[:3]
    tv=prices(source='TRADINGVIEW_BYMA',long_ticker='TV:BYMA:ABC')
    r=run(candles=cocos+tv)
    assert r['signals'][0]['details']['5']['source']=='TRADINGVIEW_BYMA'
    assert status(run(candles=cocos+tv[3:]))=='missing_or_conflicting_prices'


def test_conflict_cannot_be_overwritten_by_row_order():
    p=prices(); a=p[4];b=dict(a,close_price=112)
    for tail in itertools.permutations([a,a,b]):
        r=run(candles=p[:4]+list(tail)+p[5:])
        assert r['quality']['conflicting_candle_days']==1
        assert r['metrics']['5']['n']==0


@pytest.mark.parametrize('value',[None,0,-1,float('nan'),float('inf')])
def test_invalid_prices_not_zero_returns(value):
    assert status(run(candles=prices(open_price=value)))=='missing_or_conflicting_prices'


@pytest.mark.parametrize('cost',[float('nan'),float('inf'),-1,401])
def test_invalid_cost_rejected(cost):
    with pytest.raises(ValueError): run(cost_bps=cost)


def test_utc_candle_date_not_previous_art_day():
    r=run()
    assert r['metrics']['5']['n']==1
    assert r['signals'][0]['details']['5']['entry_price']==100


def test_holidays_and_trading_without_settlement():
    assert not trading_day(date(2026,7,9))
    assert trading_day(date(2026,7,10))
    assert sessions_after(date(2026,7,8),2)==[date(2026,7,10),date(2026,7,13)]


def test_current_day_and_future_data_do_not_mature():
    row=signal(created_at=datetime(2026,9,25,18,tzinfo=UTC))
    r=run([row],prices(start=date(2026,9,25)))
    assert status(r)=='immature'  # fifth session Oct 2, still the current date
    p=prices();p[0]['scraped_at']=AS_OF+timedelta(days=1)
    assert status(run(candles=p))=='missing_or_conflicting_prices'


def test_partial_historical_candle_rejected():
    p=prices();p[0]['scraped_at']=p[0]['ts']+timedelta(hours=16)
    assert status(run(candles=p))=='missing_or_conflicting_prices'


def test_corporate_event_and_registry_failure_exclude():
    event=dict(ticker='ABC',effective_at=datetime(2026,9,4,15,tzinfo=UTC),lifecycle_status='CONFIRMED')
    assert status(run(events=[event]))=='corporate_event'
    assert status(run(events=[dict(event,lifecycle_status='DISMISSED')]))=='evaluated'
    assert status(run(events_available=False))=='corporate_registry_unavailable'


def test_large_unverified_discontinuity_excluded():
    p=prices();p[2]['open_price']=50;p[2]['close_price']=55
    assert status(run(candles=p))=='unverified_discontinuity'


def test_wrong_currency_and_ambiguous_instrument():
    assert status(run(candles=prices(currency='USD')))=='missing_or_conflicting_prices'
    assert status(run(candles=prices()+prices(long_ticker='ABC-OTHER',close_price=111)))=='ambiguous_instrument'


def test_calendar_outside_verified_range_fails_closed():
    r=run([signal(created_at=datetime(2025,9,1,18,tzinfo=UTC))],[])
    assert status(r)=='calendar_unverified'


def test_missing_denominator_not_included_in_mean():
    r=run([signal(),signal(intent_id=2,ticker='MISSING')])
    assert r['metrics']['5']['coverage_pct']==50
    assert r['metrics']['5']['mean_pct']==pytest.approx(9.25)
    assert r['metrics']['5']['win_pct']==100


def test_all_four_horizons_recompute_from_distinct_raw_exits():
    row=signal(created_at=datetime(2026,6,1,18,tzinfo=UTC))
    p=prices(start=date(2026,6,1),n=40)
    for index,c in enumerate(p):
        c['open_price']=100+index*.1
        c['close_price']=100+(index+1)*.1
    r=run([row],p,cost_bps=75)
    for h in (5,10,20,40):
        assert r['metrics'][str(h)]['n']==1
        # Independent hand formula: 0.1 percentage points/session minus 0.75 pp.
        assert r['metrics'][str(h)]['mean_pct']==pytest.approx(h*.1-.75)
        assert r['metrics'][str(h)]['win_pct']==(0 if h==5 else 100)

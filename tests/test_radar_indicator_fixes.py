"""Independent boundary calculations for the four fixes; synthetic inputs only."""
from dataclasses import replace
import math
import pandas as pd
import pytest
from src.analysis.technical import compute_indicators, generate_signals
from src.analysis.opportunity_screener import _compute_screener_metrics, _compute_asymmetry


def bars(values=None):
    close = values if values is not None else [100+(i%5)*2+i*.1 for i in range(220)]
    return pd.DataFrame(dict(Open=close,High=[x+1 for x in close],Low=[x-1 for x in close],
                             Close=close,Volume=[1000.]*len(close)),
                        index=pd.date_range('2026-01-02',periods=len(close),freq='B',tz='UTC'))


@pytest.mark.parametrize('fault',['missing_last','missing_middle','infinite','zero','negative',
                                   'missing_high','impossible','duplicate','unsorted','after_cutoff'])
def test_invalid_evidence_never_becomes_finite_rsi_or_atr(fault):
    f=bars()
    if fault=='missing_last': f.loc[f.index[-1],'Close']=float('nan')
    if fault=='missing_middle': f.loc[f.index[40],'Close']=float('nan')
    if fault=='infinite': f.loc[f.index[-1],'Close']=float('inf')
    if fault=='zero': f.loc[f.index[-1],'Close']=0
    if fault=='negative': f.loc[f.index[-1],'Close']=-1
    if fault=='missing_high': f=f.drop(columns='High')
    if fault=='impossible': f.loc[f.index[-1],'Low']=10000
    if fault=='duplicate': f=pd.concat([f,f.tail(1)])
    if fault=='unsorted': f=f.iloc[::-1]
    if fault=='after_cutoff': f.attrs['cutoff']=f.index[-2].to_pydatetime()
    original=f.copy(deep=True)
    m=_compute_screener_metrics('FIXTURE',f,None,None)
    a=_compute_asymmetry('FIXTURE',f,m)
    assert not m.passes_screen and m.rsi is None and m.price is None
    assert not a.rr_valid and a.atr_pct is None and a.stop_loss_pct is None
    pd.testing.assert_frame_equal(f,original)


@pytest.mark.parametrize('n',[0,1,14,15,39])
def test_short_frame_has_no_measured_rsi_placeholder(n):
    m=_compute_screener_metrics('FIXTURE',bars().iloc[:n],None,None)
    assert m.rsi is None and m.price is None and not m.passes_screen


@pytest.mark.parametrize('kind',['up','down','flat','mixed'])
def test_screener_and_motor_match_independent_price_change_rsi(kind):
    close=([100+i for i in range(60)] if kind=='up' else
           [200-i for i in range(60)] if kind=='down' else
           [100]*60 if kind=='flat' else [100+i*.1+(i%5)*3 for i in range(60)])
    changes=[b-a for a,b in zip(close[-15:],close[-14:])]
    gain=sum(max(d,0) for d in changes);loss=sum(max(-d,0) for d in changes)
    expected=100-100/(1+gain/loss) if loss else 100 if gain else 50
    f=bars(close)
    assert _compute_screener_metrics('FIXTURE',f,None,None).rsi==pytest.approx(expected)
    assert compute_indicators(f,'FIXTURE').rsi_14==pytest.approx(expected)


def test_gap_changes_stop_without_changing_its_existing_limits():
    close=[200-i for i in range(45)]+[100]*10+[150]*5
    f=bars(close);m=_compute_screener_metrics('FIXTURE',f,None,None)
    a=_compute_asymmetry('FIXTURE',f,m)
    # Last 14 TRs contain one 51-point gap and thirteen 2-point ranges.
    expected_atr=(13*2+51)/14
    assert a.atr_pct==pytest.approx(expected_atr/150)
    assert a.stop_loss_pct==pytest.approx(min(max(expected_atr/150*1.5,.05),.18))
    assert a.stop_loss_pct > .05  # The old High-Low proxy hit the 5% floor.
    assert a.risk_reward==pytest.approx((200/150-1)/a.stop_loss_pct)


@pytest.mark.parametrize('field_a,field_b,weight',[
    ('ema_12','ema_26',.7),('sma_20','sma_50',1.2),('close','sma_200',.8)])
def test_equal_averages_are_neutral_and_strict_relations_retain_weights(field_a,field_b,weight):
    base=compute_indicators(bars([100.]*220),'FIXTURE')
    flat=generate_signals(base)
    assert flat.score_raw==0 and flat.signal=='HOLD'
    values={field_a:100.,field_b:100.}
    neutral=generate_signals(replace(base,**values))
    values[field_a]=101.
    up=generate_signals(replace(base,**values))
    values[field_a]=99.
    down=generate_signals(replace(base,**values))
    assert up.score_raw-neutral.score_raw==pytest.approx(weight)
    assert down.score_raw-neutral.score_raw==pytest.approx(-weight)
    assert not any('Cross' in reason for reason in up.reasons+down.reasons)

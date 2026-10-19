"""Independent arithmetic and edge cases, including the four corrected V1 defects.

Synthetic data only. No DB, feeds, scheduler, execution or new shadow system.
"""
from copy import deepcopy
from dataclasses import replace
import math
from pathlib import Path
import random
import sys
import pandas as pd
import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from technical_reference import indicators as reference, rsi, ema, bollinger, true_ranges
from audit_technical_validation import old_rsi, equal
from src.analysis.technical import compute_indicators, generate_signals, _rsi, _atr, _macd, _ema, _sma, _bollinger
from src.analysis.opportunity_screener import (_compute_screener_metrics, _compute_asymmetry,
    opportunity_rank_score, OpportunityCandidate, CandidateStatus, TradeType, AsymmetryMetrics,
    EdgeMetrics, EdgeLabel, STATUS_PRIORITY)


def frame(kind='mixed',n=220):
    rng=random.Random(640)
    closes=[100.]
    for i in range(1,n):
        change={'mixed':rng.uniform(-2,2),'up':.5,'down':-.25,'flat':0.,'gap':15. if i==n-5 else 0.}[kind]
        closes.append(closes[-1]+change)
    return pd.DataFrame({'Open':closes,'High':[x+1 for x in closes],
        'Low':[x-1 for x in closes],'Close':closes,'Volume':[100.+(i%7)*20 for i in range(n)]},
        index=pd.date_range('2025-01-01',periods=n,freq='B',tz='UTC'))


@pytest.mark.parametrize('kind',['mixed','up','down','flat','gap'])
@pytest.mark.parametrize('n',[60,199,200,220])
def test_all_core_fields_against_scalar_reference(kind,n):
    f=frame(kind,n);saved=f.copy(deep=True);actual=compute_indicators(f,'SYNTHETIC')
    assert actual is not None
    for name,expected in reference(f).items():
        assert equal(expected,getattr(actual,name)),(kind,n,name,expected,getattr(actual,name))
    pd.testing.assert_frame_equal(f,saved)


@pytest.mark.parametrize('n',[0,1,13,14])
def test_rsi_insufficient_lookback_remains_missing(n):
    assert _rsi(pd.Series([100.+i for i in range(n)],dtype=float)).isna().all()
    assert rsi([100.+i for i in range(n)]) is None


@pytest.mark.parametrize('kind,expected',[('up',100),('down',0),('flat',50)])
def test_first_complete_rsi_boundary(kind,expected):
    values=frame(kind,15).Close
    assert math.isnan(_rsi(values).iloc[-2])
    assert _rsi(values).iloc[-1]==expected


def test_missing_price_breaks_lookback_until_fourteen_new_changes():
    s=pd.Series([100.+i for i in range(40)]);s.iloc[18]=float('nan')
    assert _rsi(s).iloc[18:33].isna().all()
    assert _rsi(s).iloc[33]==100


def test_ema_and_macd_first_price_seed_hand_computed():
    s=pd.Series([10.,12.,9.])
    expected12=10*(11/13)**2+12*(2/13)*(11/13)+9*(2/13)
    assert _ema(s,12).iloc[-1]==pytest.approx(expected12)
    fast,slow=ema(s.tolist(),12),ema(s.tolist(),26)
    line=[a-b for a,b in zip(fast,slow)]
    sig=ema(line,9)
    a,b,c=_macd(s)
    assert a.tolist()==pytest.approx(line)
    assert b.tolist()==pytest.approx(sig)
    assert c.tolist()==pytest.approx([x-y for x,y in zip(line,sig)])


def test_bollinger_sample_vs_population_is_explicit_methodology():
    values=list(range(1,21));upper,mid,lower=_bollinger(pd.Series(values,dtype=float))
    sample=bollinger(values,ddof=1);population=bollinger(values,ddof=0)
    assert (upper.iloc[-1],mid.iloc[-1],lower.iloc[-1])==pytest.approx(sample)
    assert (sample[0]-sample[2])/(population[0]-population[2])==pytest.approx(math.sqrt(20/19))


def test_atr_includes_gap_from_previous_close():
    f=frame('gap',60)
    assert _atr(f).iloc[-1]==pytest.approx((13*2+16)/14)


@pytest.mark.parametrize('n',[0,1,14,39,40,59])
def test_technical_public_minimum_is_sixty(n):
    assert compute_indicators(frame(n=max(n,1)).iloc[:n],'SYNTHETIC') is None


def test_screener_short_frame_is_rejected_despite_placeholder_fifty():
    metric=_compute_screener_metrics('SYNTHETIC',frame(n=39),None,None)
    assert metric.rsi is None and not metric.passes_screen
    assert metric.fail_reason=='datos insuficientes'


def test_screener_uses_canonical_price_delta_rsi():
    f=frame('mixed',60);m=_compute_screener_metrics('SYNTHETIC',f,None,None)
    assert m.rsi==pytest.approx(rsi(f.Close.tolist()))
    assert abs(m.rsi-rsi(f.Close.tolist(),percentage=True))>0.01


@pytest.mark.parametrize('value',[None,float('nan'),float('inf'),0.,-1.])
def test_volume_missingness_is_not_zero_or_one(value):
    f=frame();f.loc[f.index[-1],'Volume']=value
    actual=compute_indicators(f,'SYNTHETIC')
    assert actual.vol_ratio is None and actual.obv is None


def test_volume_inclusive_twenty_is_not_previous_twenty():
    f=frame();f.Volume=100.;f.loc[f.index[-1],'Volume']=200.
    assert compute_indicators(f,'SYNTHETIC').vol_ratio==pytest.approx(200/105)
    assert compute_indicators(f,'SYNTHETIC').vol_ratio!=pytest.approx(200/100)


@pytest.mark.parametrize('fault',['nan','negative','impossible','duplicate','unsorted'])
def test_invalid_prices_rejected(fault):
    f=frame()
    if fault=='nan':f.loc[f.index[-1],'Close']=float('nan')
    if fault=='negative':f.loc[f.index[-1],'Close']=-1.
    if fault=='impossible':f.loc[f.index[-1],'High']=1.
    if fault=='duplicate':f=pd.concat([f,f.tail(1)])
    if fault=='unsorted':f=f.iloc[::-1]
    assert compute_indicators(f,'SYNTHETIC') is None


@pytest.mark.parametrize('edge',[-.2,0.,.05,.5])
@pytest.mark.parametrize('valid_rr',[False,True])
def test_radar_rank_matches_independent_formula_and_ignores_outcomes(edge,valid_rr):
    c=OpportunityCandidate(ticker='SYNTHETIC',status=CandidateStatus.VIGILANCIA_A,
        trade_type=TradeType.NEW_ENTRY,final_score=.23,conviction=.6,
        asymmetry=AsymmetryMetrics(ticker='SYNTHETIC',asymmetry_ratio=2.1,rr_valid=valid_rr),
        edge=EdgeMetrics(raw=edge,label=EdgeLabel.MODERADO,vs_ticker='',vs_score=0.,explanation=''))
    expected=30+.23*.6*(1+(.7 if valid_rr else 0))*(.4 if edge<0 else 1+2*min(edge,.15))
    assert opportunity_rank_score(c)==pytest.approx(expected)
    c.prior_radar_outcomes={5:999,10:-999};assert opportunity_rank_score(c)==pytest.approx(expected)


def test_v1_replay_refuses_a_changed_engine():
    # V1 is pinned to its audited motor; run it in the PR29 worktree.
    with pytest.raises(ValueError,match='Working technical module differs'):
        old_rsi()



def test_screener_flat_price_should_be_neutral_under_declared_boundary_policy():
    assert _compute_screener_metrics('SYNTHETIC',frame('flat'),None,None).rsi==50


def test_screener_atr_label_should_include_true_range():
    f=frame('gap',60);m=_compute_screener_metrics('SYNTHETIC',f,None,None)
    assert _compute_asymmetry('SYNTHETIC',f,m).atr_pct==pytest.approx(_atr(f).iloc[-1]/m.price)


def test_screener_missing_last_price_should_not_emit_finite_rsi():
    f=frame();f.loc[f.index[-1],'Close']=float('nan')
    assert _compute_screener_metrics('SYNTHETIC',f,None,None).rsi is None


def test_equal_averages_should_not_claim_strict_bearish_inequalities():
    signal=generate_signals(compute_indicators(frame('flat'),'SYNTHETIC'))
    assert not any('EMA12 < EMA26' in r or 'SMA20 < SMA50' in r for r in signal.reasons)

"""Offline paired corrections vs PR29, on the exact V1 candle bundles.

No DB, feeds, saved-decision replay or full Radar ranking with assumed context.
"""
from __future__ import annotations
import argparse
from collections import Counter, defaultdict
import hashlib
import json
import logging
import math
from pathlib import Path
import subprocess
import sys
import types

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from audit_technical_validation import backup_frames, linked_frames, file_hash, save
from src.analysis import technical, opportunity_screener as screener

BASE = '810836f5b80a0c28b0392a4d8776c039292adb5e'


def load_before(name, relative):
    source = subprocess.check_output(['git', 'show', BASE+':'+relative], cwd=ROOT).decode('utf8')
    module = types.ModuleType(name)
    module.__file__ = str(ROOT/relative)
    sys.modules[name] = module
    exec(compile(source, str(ROOT/relative)+'@'+BASE, 'exec'), module.__dict__)
    return module


def finite(value):
    return float(value) if value is not None and math.isfinite(float(value)) else None


def different(a, b):
    if a is None or b is None: return a != b
    if isinstance(a, (bool, str)): return a != b
    return not math.isclose(a, b, rel_tol=1e-10, abs_tol=1e-8)


def technical_view(module, frame, ticker):
    indicator = module.compute_indicators(frame, ticker)
    if indicator is None: return None
    signal = module.generate_signals(indicator)
    return dict(rsi=finite(indicator.rsi_14), score=signal.score_raw, signal=signal.signal,
                regime=signal.technical_regime, strength=signal.strength)


def screen_view(module, frame, ticker, asset_type):
    metric = module._compute_screener_metrics(ticker, frame, None, None, asset_type)
    asym = module._compute_asymmetry(ticker, frame, metric)
    return dict(price=finite(metric.price), rsi=finite(metric.rsi), passes_screen=metric.passes_screen,
                fail_reason=metric.fail_reason, atr_pct=finite(asym.atr_pct),
                stop_loss_pct=finite(asym.stop_loss_pct), rr=finite(asym.risk_reward), rr_valid=asym.rr_valid)


def sensitivity(frames, old):
    endpoints = 0; rows = []; counts = Counter(); tickers = defaultdict(set)
    for meta, frame, _ in frames:
        if meta['cohort'] != 'BACKUP_LAST_260_BY_SERIES': continue
        if technical.compute_indicators(frame, meta['ticker']) is None: continue
        close = frame.Close
        endpoints += len(frame)-59
        # Only equal averages can alter technical scores: all indicator formulas
        # and other signal contributions are unchanged from PR29.
        equal_ma = ((old._ema(close,12)==old._ema(close,26)) |
                    (old._sma(close,20)==old._sma(close,50)) |
                    (close==old._sma(close,200)))
        changes = close.pct_change(fill_method=None)
        gains = changes.clip(lower=0).rolling(14).mean()
        losses = (-changes.clip(upper=0)).rolling(14).mean()
        prior_rsi = (100-100/(1+gains/losses)).mask(losses==0,100.)
        fixed_rsi = technical._rsi(close)
        def rsi_term(x):
            return -.10 if x>80 else -.05 if x>70 else .05 if x<40 else 0.
        prior_atr = (frame.High-frame.Low).rolling(14).mean()/close
        fixed_atr = technical._atr(frame)/close
        for i in range(59,len(frame)):
            for key, a, b in [
                ('screener_rsi', prior_rsi.iloc[i], fixed_rsi.iloc[i]),
                ('screener_rsi_momentum_term', rsi_term(prior_rsi.iloc[i]), rsi_term(fixed_rsi.iloc[i])),
                ('atr_pct', prior_atr.iloc[i], fixed_atr.iloc[i]),
                ('stop_loss_pct', min(max(prior_atr.iloc[i]*1.5,.05),.18),
                                  min(max(fixed_atr.iloc[i]*1.5,.05),.18)),
            ]:
                if different(float(a),float(b)):
                    counts[key]+=1; tickers[key].add(meta['ticker'])
            if not equal_ma.iloc[i]: continue
            prefix = frame.iloc[:i+1].copy()
            # Same indicators in both versions; only generate_signals differs.
            ind = technical.compute_indicators(prefix, meta['ticker'])
            a, b = old.generate_signals(ind), technical.generate_signals(ind)
            changed = {key:different(getattr(a,key),getattr(b,key))
                       for key in ('score_raw','signal','technical_regime','strength')}
            for key, changed_value in changed.items():
                if changed_value:
                    counts[key]+=1; tickers[key].add(meta['ticker'])
            if any(changed.values()):
                rows.append(dict(ticker=meta['ticker'],series_id=meta['id'],source=meta['identity']['source'],
                                 date=str(prefix.index[-1]),before={k:getattr(a,k) for k in changed},
                                 after={k:getattr(b,k) for k in changed}))
    transitions = Counter(r['before']['signal']+' -> '+r['after']['signal'] for r in rows
                          if r['before']['signal']!=r['after']['signal'])
    return rows, dict(endpoints=endpoints, changes={key:dict(endpoints=counts[key],tickers=sorted(tickers[key]))
                         for key in sorted(counts)},signal_transitions=dict(transitions))


def audit(args):
    args.output.mkdir(parents=True,exist_ok=False)
    before_tech = load_before('_radar_audit_before_technical','src/analysis/technical.py')
    before_screen = load_before('_radar_audit_before_screener','src/analysis/opportunity_screener.py')
    frames = list(backup_frames(args.bundle))+list(linked_frames(args.context_capture))
    rows=[]
    for meta, frame, _ in frames:
        row=dict(cohort=meta['cohort'],id=meta['id'],ticker=meta['ticker'],identity=meta['identity'],
                 bars=len(frame),last_bar=str(frame.index[-1]),frame_hash=hashlib.sha256(
                     frame.to_json(date_format='iso',orient='split').encode()).hexdigest())
        row['technical_before']=technical_view(before_tech,frame,meta['ticker'])
        row['technical_after']=technical_view(technical,frame,meta['ticker'])
        row['screener_before']=screen_view(before_screen,frame,meta['ticker'],meta['identity']['asset_type'])
        row['screener_after']=screen_view(screener,frame,meta['ticker'],meta['identity']['asset_type'])
        rows.append(row)
    cohorts={}
    for cohort in sorted({r['cohort'] for r in rows}):
        cohort_rows=[r for r in rows if r['cohort']==cohort]
        measured={}
        for group, fields in [('technical',('rsi','score','signal','regime','strength')),
                              ('screener',('rsi','passes_screen','atr_pct','stop_loss_pct','rr','rr_valid'))]:
            eligible=[r for r in cohort_rows if r[group+'_before'] is not None and r[group+'_after'] is not None]
            for field in fields:
                changes=[r for r in eligible if different(r[group+'_before'][field],r[group+'_after'][field])]
                measured[group+'_'+field]=dict(frames=len(changes),tickers=sorted({r['ticker'] for r in changes}))
        cohorts[cohort]=dict(frames=len(cohort_rows),technical_eligible=sum(r['technical_after'] is not None for r in cohort_rows),
                            changes=measured)
    historical, history_summary=sensitivity(frames,before_tech)
    save(args.output/'comparison-private.json',rows)
    save(args.output/'historical-private.json',historical)
    summary=dict(baseline=BASE,context_sha256=file_hash(args.context_capture),
                 market_manifest=json.loads((args.bundle/'manifest.json').read_text()),cohorts=cohorts,
                 historical_sensitivity=history_summary,
                 full_radar_ranking='NOT_VERIFIABLE_MISSING_EXACT_PRODUCTIVE_INPUTS',
                 scope='Paired technical signals and screener components, not saved decisions or full Radar scores. Screen-only diagnostics omit benchmarks in both arms.',
                 files={name:file_hash(args.output/name) for name in ['comparison-private.json','historical-private.json']},
                 code_sha256_utf8_lf={name:hashlib.sha256((ROOT/name).read_text(encoding='utf-8-sig').encode()).hexdigest()
                        for name in ['src/analysis/technical.py','src/analysis/opportunity_screener.py','scripts/audit_radar_corrections.py']})
    save(args.output/'summary.json',summary)
    print(json.dumps(summary))


if __name__=='__main__':
    logging.disable(logging.CRITICAL)
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--bundle',type=Path,required=True)
    p.add_argument('--context-capture',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    audit(p.parse_args())

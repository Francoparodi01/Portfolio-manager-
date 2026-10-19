"""Frozen-input mathematical and RSI A/B audit. No DB, network, orders or writes to history.

Extract a private market-only bundle from the supplied SQL dump as DATA, or replay
that bundle and explicitly linked contextual candles. Original versions stay apart.
Only audit output is written; the output directory must not already exist.
"""
from __future__ import annotations
import argparse
import ast
from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import datetime
import gzip
import hashlib
import json
import logging
import math
from pathlib import Path
import subprocess
import sys
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from measurement_baseline import copy_blocks, file_hash
from technical_reference import indicators as reference_indicators
from src.analysis import technical
from src.analysis.contextual_contracts import frame_quality
from src.analysis.opportunity_screener import _compute_screener_metrics
from src.collector.cocos_history import candles_to_frame
from src.collector.contextual_g2 import observation_rows_to_candles
import pandas as pd

BASE='1687cdcce50895a9dfacb6dae04188fd8d8d5626'
FIX='935f7b156fccad5d88a0c4968188c858f003a299'


def json_object(value):
    return json.loads(value) if isinstance(value,str) else value


def save(path, value):
    with path.open('x',encoding='utf8',newline='\n') as stream:
        json.dump(value,stream,ensure_ascii=True,sort_keys=True,indent=2,allow_nan=False)
        stream.write('\n')


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def extract(backup,output):
    output.mkdir(parents=True,exist_ok=False)
    target=output/'market-candles.jsonl.gz';counts=Counter();databases=set();n=0
    with target.open('xb') as raw, gzip.GzipFile(fileobj=raw,mode='wb',mtime=0,filename='') as stream:
        for database,table,columns,row in copy_blocks(backup):
            if table!='market_candles' or row is None:continue
            databases.add(database)
            if len(databases)>1:raise ValueError('Multiple source databases')
            stream.write((json.dumps(row,sort_keys=True,separators=(',',':'))+'\n').encode())
            counts[row['source']+'|'+row['interval']]+=1;n+=1
    if not n:raise ValueError('No candle rows')
    manifest={'schema':'technical-frozen-market-v1','input_sha256':file_hash(backup),
        'output_sha256':file_hash(target),'rows':n,'source_interval_counts':dict(counts),
        'sql_executed':False,'extraction_changes':'NONE; COPY field escaping decoded; original values retained'}
    save(output/'manifest.json',manifest)
    print(json.dumps(manifest))


def old_rsi():
    """Execute only the audited old RSI helper, not an old application runtime."""
    def source(ref):
        return subprocess.check_output(['git','show',ref+':src/analysis/technical.py'],cwd=ROOT).decode('utf8')
    before,after=source(BASE),source(FIX)
    aa,bb=ast.parse(before),ast.parse(after)
    def members(tree):
        return {node.name:ast.dump(node,include_attributes=False) for node in tree.body if isinstance(node,(ast.FunctionDef,ast.ClassDef))}
    a,b=members(aa),members(bb)
    if {k for k in a.keys()|b.keys() if a.get(k)!=b.get(k)}!={'_rsi'}:
        raise ValueError('Base/fix change exceeds RSI helper')
    if technical.__file__ is None or Path(technical.__file__).read_text(encoding='utf8')!=after:
        raise ValueError('Working technical module differs from audited fix')
    node=next(n for n in aa.body if isinstance(n,ast.FunctionDef) and n.name=='_rsi')
    namespace={};exec(compile(ast.Module(body=[node],type_ignores=[]),'<audited-base-rsi>','exec'),namespace)
    return namespace['_rsi'],{'base_commit':BASE,'fix_commit':FIX,
        'base_technical_sha256_utf8_lf':hashlib.sha256(before.encode()).hexdigest(),
        'fix_technical_sha256_utf8_lf':hashlib.sha256(after.encode()).hexdigest()}


def backup_frames(bundle):
    manifest=json.loads((bundle/'manifest.json').read_text())
    if file_hash(bundle/'market-candles.jsonl.gz')!=manifest['output_sha256']:raise ValueError('Bundle hash mismatch')
    groups=defaultdict(list)
    with gzip.open(bundle/'market-candles.jsonl.gz','rt',encoding='utf8') as stream:
        for line in stream:
            row=json.loads(line)
            if row['interval']!='1d':continue
            key=tuple(row[k] for k in ['ticker','long_ticker','asset_type','currency','venue','interval','source'])
            groups[key].append(row)
    for key,rows in sorted(groups.items()):
        frame=candles_to_frame(rows)
        # A bounded trailing window, not a claim to recreate a particular decision.
        frame=frame.tail(260).copy()
        yield {'cohort':'BACKUP_LAST_260_BY_SERIES','id':digest(key),'ticker':key[0],
            'identity':dict(zip(['ticker','provider_symbol','asset_type','currency','venue','interval','source'],key)),
            'original_rows':len(rows),'original_records_sha256':digest(rows)},frame,None


def linked_frames(capture):
    data=json.loads(capture.read_text(encoding='utf8'))
    if not data.get('owner_token'):raise ValueError('Owner-scoped capture required')
    observations={}
    for row in data['tables']['market_candle_observations']:
        key=row['observation_id']
        if key in observations:raise ValueError('Repeated observation identity')
        observations[key]=row
    links=defaultdict(list)
    for row in data['tables']['contextual_snapshot_candles']:
        if row['input_role']=='ASSET':links[row['snapshot_id']].append(row)
    seen=set()
    for snapshot in sorted(data['tables']['contextual_market_snapshots'],key=lambda r:r['snapshot_id']):
        sid=snapshot['snapshot_id']
        if sid in seen:raise ValueError('Repeated snapshot identity')
        seen.add(sid);ordered=sorted(links[sid],key=lambda r:r['ordinal'])
        if not ordered or len({r['ordinal'] for r in ordered})!=len(ordered):raise ValueError('Missing/duplicate ordinal')
        if len({r['observation_id'] for r in ordered})!=len(ordered):raise ValueError('Duplicate linked observation')
        rows=[observations[r['observation_id']] for r in ordered]
        if any(r['instrument_id']!=snapshot['instrument_id'] for r in rows):raise ValueError('Cross-instrument link')
        frame=candles_to_frame(observation_rows_to_candles(rows))
        frame.attrs['cutoff']=pd.Timestamp(snapshot['cutoff']).to_pydatetime()
        stored=json_object(snapshot['feature_snapshot_v3']).get('payload',{})
        quality=stored.get('data_quality',{})
        yield {'cohort':'LINKED_CONTEXTUAL_ASSET','id':sid,'ticker':rows[0]['ticker'],
            'identity':frame.attrs['series_identity'],'cutoff':snapshot['cutoff'],
            'original_rows':len(rows),'original_records_sha256':digest(rows),
            'saved_mode':snapshot['mode'],'saved_code_version':snapshot['code_version'],
            'productive_frame_digest':quality.get('series_digest'),
            'linked_frame_digest':frame_quality(frame,cutoff=frame.attrs['cutoff'])['series_digest']},frame,stored


def projection(ind):
    signal=technical.generate_signals(ind)
    return {'rsi':ind.rsi_14,'score_raw':signal.score_raw,'signal':signal.signal,
        'regime':signal.technical_regime,'overbought':signal.overbought_momentum}


def equal(a,b):
    if a is None or b is None:return a is None and b is None
    return math.isclose(a,b,rel_tol=1e-10,abs_tol=1e-8)


def sensitivity(frames, eligible_ids, old):
    """All prefixes >=60 bars of eligible backup windows, never saved decisions.

    Only endpoints where RSI changes require both signal projections: the AST
    gate establishes that RSI is the sole changed engine function. Unchanged
    RSI cannot change any deterministic input to generate_signals.
    """
    rows=[];endpoints=0;tickers=set();changed_points=Counter();changed_tickers=defaultdict(set)
    for meta,frame,_ in frames:
        if meta['cohort']!='BACKUP_LAST_260_BY_SERIES' or meta['id'] not in eligible_ids:continue
        endpoints+=len(frame)-59;tickers.add(meta['ticker'])
        before=old(frame.Close);after=technical._rsi(frame.Close)
        for i in range(59,len(frame)):
            a,b=before.iloc[i],after.iloc[i]
            if (pd.isna(a) and pd.isna(b)) or a==b:continue
            prefix=frame.iloc[:i+1].copy()
            with patch.object(technical,'_rsi',old):aa=technical.compute_indicators(prefix,meta['ticker'])
            bb=technical.compute_indicators(prefix,meta['ticker'])
            if aa is None or bb is None:raise ValueError('Unexpected invalid eligible prefix')
            pre,post=projection(aa),projection(bb)
            changes={k:pre[k]!=post[k] for k in pre}
            for key,changed in changes.items():
                if changed:changed_points[key]+=1;changed_tickers[key].add(meta['ticker'])
            rows.append({'series_id':meta['id'],'ticker':meta['ticker'],'source':meta['identity']['source'],
                'date':str(prefix.index[-1]),'prefix_bars':len(prefix),'before':pre,'after':post,'changed':changes})
    transitions=Counter(r['before']['signal']+' -> '+r['after']['signal'] for r in rows if r['changed']['signal'])
    return rows,{'endpoints':endpoints,'unique_tickers':len(tickers),
        'changes':{k:{'endpoints':changed_points[k],'tickers':sorted(changed_tickers[k])} for k in ['rsi','score_raw','signal','regime','overbought']},
        'rsi_to_100':sum(r['after']['rsi']==100 for r in rows),'rsi_to_50':sum(r['after']['rsi']==50 for r in rows),
        'signal_transitions':dict(transitions),'scope':'retrospective prefixes of eligible backup windows; not historical decisions or PIT backtest'}


def audit(bundle,capture,output):
    output.mkdir(parents=True,exist_ok=False)
    old,versions=old_rsi();results=[];totals={};checks=Counter();numeric_checks=Counter();maximum=defaultdict(float)
    frames=list(backup_frames(bundle))+list(linked_frames(capture))
    stored_counts=Counter()
    for meta,frame,stored in frames:
        if stored is not None:
            values=stored.get('data_quality',{}).get('indicator_values',{})
            stored_counts['payloads']+=1
            stored_counts['finite_original_rsi']+=isinstance(values.get('rsi_14'),(int,float)) and math.isfinite(values['rsi_14'])
        quality=frame_quality(frame,cutoff=frame.attrs.get('cutoff'))
        result={**meta,'bars':len(frame),'first_bar':str(frame.index[0]),'last_bar':str(frame.index[-1]),
            'quality':quality,'frame_sha256':hashlib.sha256(frame.to_json(date_format='iso',orient='split').encode()).hexdigest()}
        with patch.object(technical,'_rsi',old):before=technical.compute_indicators(frame,meta['ticker'])
        after=technical.compute_indicators(frame,meta['ticker'])
        if (before is None)!=(after is None):raise ValueError('Eligibility changed unexpectedly')
        result['eligible']=after is not None
        if after:
            result['before']=projection(before);result['after']=projection(after)
            result['changed']={k:result['before'][k]!=result['after'][k] for k in result['before']}
            result['indicator_values_before']=asdict(before);result['indicator_values_after']=asdict(after)
            oracle=reference_indicators(frame);failures=[]
            for name,value in oracle.items():
                actual=getattr(after,name);checks[name]+=1
                if not equal(value,actual):failures.append({'field':name,'reference':value,'actual':actual})
                if value is not None and actual is not None:
                    numeric_checks[name]+=1;maximum[name]=max(maximum[name],abs(value-actual))
            result['reference_failures']=failures
            metric=_compute_screener_metrics(meta['ticker'],frame,None,None,meta['identity']['asset_type'])
            result['screener']={'rsi':metric.rsi,'passes_screen':metric.passes_screen,'fail_reason':metric.fail_reason,
                'difference_from_technical':metric.rsi-after.rsi_14 if after.rsi_14 is not None else None}
        results.append(result)
    for cohort in sorted({r['cohort'] for r in results}):
        rows=[r for r in results if r['cohort']==cohort];ok=[r for r in rows if r['eligible']]
        changed={k:{'frames':sum(r['changed'][k] for r in ok),'tickers':sorted({r['ticker'] for r in ok if r['changed'][k]})}
                 for k in ['rsi','score_raw','signal','regime','overbought']}
        transitions=Counter(r['before']['signal']+' -> '+r['after']['signal'] for r in ok if r['changed']['signal'])
        totals[cohort]={'frames':len(rows),'unique_tickers':len({r['ticker'] for r in rows}),
            'eligible_frames':len(ok),'eligible_tickers':len({r['ticker'] for r in ok}),
            'excluded_frames':len(rows)-len(ok),'changes':changed,'signal_transitions':dict(transitions),
            'excluded_reasons':dict(Counter('INSUFFICIENT_LOOKBACK' if r['bars']<60 else '|'.join(r['quality']['price_reasons']) for r in rows if not r['eligible'])),
            'first_endpoint':min(r['last_bar'] for r in rows),'last_endpoint':max(r['last_bar'] for r in rows),
            'screener_rsi_differs':sum(not equal(r['before']['rsi'],r['screener']['rsi']) for r in ok),
            'screener_vs_corrected_rsi_differs':sum(not equal(r['after']['rsi'],r['screener']['rsi']) for r in ok),
            'reference_failures':sum(len(r.get('reference_failures',[])) for r in rows),
            'productive_frame_digest_matches':sum(bool(r.get('productive_frame_digest')) and r.get('productive_frame_digest')==r.get('linked_frame_digest') for r in rows)}
    sensitive,sensitivity_summary=sensitivity(frames,{r['id'] for r in results if r['eligible']},old)
    save(output/'sensitivity-private.json',sensitive)
    save(output/'comparison-private.json',results)
    summary={'schema':'technical-validation-v1','versions':versions,'context_capture_sha256':file_hash(capture),
        'market_manifest':json.loads((bundle/'manifest.json').read_text()),'cohorts':totals,
        'independent_checks':dict(checks),'finite_numeric_checks':dict(numeric_checks),'maximum_absolute_error':dict(maximum),
        'historical_sensitivity':sensitivity_summary,'original_productive_indicator_coverage':dict(stored_counts),
        'sensitivity_private_sha256':file_hash(output/'sensitivity-private.json'),
        'comparison_private_sha256':file_hash(output/'comparison-private.json'),
        'runtime':{'python':sys.version.split()[0],'pandas':pd.__version__},
        'scope':'Offline technical signal projections; no stored decisions recalculated or overwritten',
        'radar_final_classification_and_ranking':'NOT_EVALUABLE_MISSING_FULL_FROZEN_CONTEXT',
        'code_sha256_utf8_lf':{name:hashlib.sha256((ROOT/name).read_text(encoding='utf8').encode()).hexdigest()
            for name in ['scripts/audit_technical_validation.py','scripts/technical_reference.py']}}
    save(output/'summary.json',summary);print(json.dumps(summary))


def main():
    logging.disable(logging.CRITICAL)
    parser=argparse.ArgumentParser(description=__doc__);sub=parser.add_subparsers(dest='command',required=True)
    p=sub.add_parser('extract');p.add_argument('--backup',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    p=sub.add_parser('compare');p.add_argument('--bundle',type=Path,required=True);p.add_argument('--context-capture',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if args.command=='extract':extract(args.backup,args.output)
    else:audit(args.bundle,args.context_capture,args.output)


if __name__=='__main__':main()

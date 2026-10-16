"""Offline receipts for frozen captures, CSV exports, schema and valuation gaps."""
import argparse
from collections import Counter,defaultdict
import csv
from datetime import datetime
from decimal import Decimal
import gzip
import json
from pathlib import Path
import re
import sqlite3

from measurement_baseline import digest,file_hash,normalize,owner_scope,write_new


def schema_check(path,manifest):
    schemas={}; active=None
    with gzip.open(path,'rt',encoding='utf-8') as f:
        for line in f:
            match=re.fullmatch(r'CREATE TABLE public\.([a-z_][a-z0-9_]*) \(\n',line)
            if match:
                active=match[1];schemas[active]={};continue
            if active:
                if line.startswith(');'):
                    active=None;continue
                match=re.match(r'    "?([a-z_][a-z0-9_]*)"? (.*)',line)
                if match and match[1] not in {'CONSTRAINT'}:
                    typ=re.split(r' DEFAULT | NOT NULL| COLLATE | GENERATED | CHECK ',match[2])[0].rstrip(',')
                    typ=re.sub(r'\(\d+(?:,\d+)?\)','',typ)
                    schemas[active][match[1]]=typ
    differences=[];checked=0
    aliases={'double precision':'double precision','timestamp with time zone':'timestamp with time zone',
             'timestamp without time zone':'timestamp without time zone'}
    for table,spec in manifest['tables'].items():
        for col,typ in spec['types'].items():
            original=schemas.get(table,{}).get(col)
            # SQL array text representation is compared literally, as documented.
            compatible=(typ=='ARRAY' and original and original.endswith('[]')) or aliases.get(original,original)==typ
            if not compatible:
                differences.append({'table':table,'column':col,'backup_type':original,'local_type':typ})
            checked+=1
    return {'checked_columns':checked,'type_differences':differences,
            'types_from_backup_ddl_not_executed':True}


def exported_csv(path,spec,owner,db):
    counter=Counter(); keys=Counter();times={};nulls=Counter()
    with path.open(encoding='utf-8-sig',newline='') as f:
        reader=csv.DictReader(f)
        columns=reader.fieldnames
        for row in reader:
            # CSV empty has ambiguous NULL/empty semantics. Key coverage only.
            if None in row:raise ValueError('CSV width mismatch')
            row={k:(None if v=='' else v) for k,v in row.items()}
            counter[owner_scope(row,owner)]+=1
            key=digest([normalize(row[k],spec['types'][k]) for k in spec['keys']])
            keys[key]+=1
            for c,v in row.items():
                if v is None:
                    nulls[c]+=1
                elif spec['types'].get(c) in {'timestamp with time zone','timestamp without time zone','date'}:
                    val=normalize(v,spec['types'][c]);old=times.get(c,{'min':val,'max':val})
                    times[c]={'min':min(old['min'],val),'max':max(old['max'],val)}
    current={r[0] for r in db.execute('SELECT key FROM fingerprints WHERE tbl=? AND source=?',(path.stem,'local'))}
    backup={r[0] for r in db.execute('SELECT key FROM fingerprints WHERE tbl=? AND source=?',(path.stem,'backup'))}
    return {'file':path.name,'sha256':file_hash(path),'n':sum(counter.values()),'columns':columns,
            'scope_counts':dict(counter),'null_or_empty_counts':dict(nulls),'time_ranges':times,
            'duplicate_key_groups':sum(v>1 for v in keys.values()),'keys_missing_local':len(keys.keys()-current),
            'keys_missing_backup':len(keys.keys()-backup),'source_as_of':'UNKNOWN',
            'limit':'CSV origin/extraction clock not attested; NULL versus empty ambiguous; keys only.'}


def valuations(capture):
    positions=defaultdict(list)
    for p in capture['tables']['positions']:positions[p['snapshot_id']].append(p)
    results=Counter();by_month=defaultdict(Counter)
    for s in capture['tables']['portfolio_snapshots']:
        pp=positions.get(s['snapshot_id'],[])
        if not pp or s['total_value_ars'] is None or any(p['quantity'] is None or p['current_price'] is None for p in pp):
            results['MISSING_INPUT']+=1;continue
        if any(p['currency']!='ARS' for p in pp):
            results['FX_REQUIRED']+=1;continue
        delta=abs(sum((Decimal(str(p['quantity']))*Decimal(str(p['current_price'])) for p in pp),Decimal(0))-Decimal(str(s['total_value_ars'])))
        bucket=next((label for ceiling,label in [(Decimal('.01'),'LE_0_01'),(Decimal('1'),'GT_0_01_LE_1'),(Decimal('100'),'GT_1_LE_100'),(Decimal('10000'),'GT_100_LE_10000')] if delta<=ceiling),'GT_10000')
        results[bucket]+=1;by_month[s['scraped_at'][:7]][bucket]+=1
    return {'denominator':len(capture['tables']['portfolio_snapshots']),'absolute_gap_buckets_ARS':dict(results),
            'by_month':{k:dict(v) for k,v in sorted(by_month.items())},'interpretation':'Arithmetic residuals, not financial losses.'}


def capture_changes(old,new,catalog):
    if old['owner_token']!=new['owner_token']:raise ValueError('Different capture owners')
    result={}
    for table,rows in old['tables'].items():
        schema=catalog['tables'][table]
        keys=next((k['columns'] for k in schema['unique_keys'] if k['kind']=='PRIMARY KEY'),None)
        if not keys:continue
        def val(row,col):
            v=row[col];typ=schema['types'][col]
            if v is None:return None
            if isinstance(v,(dict,list)):v=json.dumps(v)
            elif isinstance(v,bool):v='true' if v else 'false'
            else:v=str(v)
            return normalize(v,typ)
        def keyed(rr):
            data={digest([val(r,k) for k in keys]):r for r in rr}
            if len(data)!=len(rr):raise ValueError('Capture duplicate keys')
            return data
        aa=keyed(rows);bb=keyed(new['tables'].get(table,[]));changed=Counter();different=0
        for key in aa.keys()&bb.keys():
            a,b=aa[key],bb[key];diff=[c for c in a.keys()&b.keys() if val(a,c)!=val(b,c)]
            different+=bool(diff);changed.update(diff)
        result[table]={'old_n':len(aa),'new_n':len(bb),'shared_keys':len(aa.keys()&bb.keys()),
                       'missing_new':len(aa.keys()-bb.keys()),'new_only':len(bb.keys()-aa.keys()),
                       'changed_rows':different,'changed_fields':dict(changed)}
    return result


def main(a):
    manifest=json.loads(Path(a.manifest).read_text());catalog=json.loads(Path(a.catalog).read_text())
    if file_hash(a.backup)!=manifest['source_sha256']:raise ValueError('Backup mismatch')
    old=json.loads(Path(a.old_capture).read_text());new=json.loads(Path(a.new_capture).read_text())
    db=sqlite3.connect(Path(a.comparison_db).resolve().as_uri()+'?mode=ro',uri=True)
    result={'schema_check':schema_check(a.backup,manifest),
            'old_capture_sha256':file_hash(a.old_capture),'old_capture_at':old['captured_at'],
            'new_capture_sha256':file_hash(a.new_capture),'new_capture_at':new['captured_at'],
            'capture_changes':capture_changes(old,new,catalog),
            'current_valuation':valuations(new),
            'csv_exports':[exported_csv(p,manifest['tables'][p.stem],manifest['owner_digest'],db) for p in sorted(Path(a.csv_dir).glob('*.csv'))]}
    db.close();write_new(a.output,result)
    print(json.dumps({'path':a.output,'sha256':file_hash(a.output),'schema':result['schema_check'],
                      'valuation':result['current_valuation'],
                      'csv':[{k:v for k,v in x.items() if k not in ['columns','null_or_empty_counts','time_ranges']} for x in result['csv_exports']]}))


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for name in ['manifest','catalog','backup','old-capture','new-capture','comparison-db','csv-dir','output']:
        p.add_argument('--'+name,required=True)
    main(p.parse_args())

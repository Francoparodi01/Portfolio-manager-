"""Synthetic, isolated diagnostics tests; never connect to a database or Docker."""
import gzip
import importlib.util
import json
from pathlib import Path
import sys
import pytest

SPEC=importlib.util.spec_from_file_location('baseline',Path(__file__).parents[1]/'scripts'/'measurement_baseline.py')
m=importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(m)


def test_decimal_precision_and_zero():
    assert m.decimal_text('123456789012345678901234567890.12345678000')=='123456789012345678901234567890.12345678'
    assert m.decimal_text('-0.000')=='0'


def test_type_aware_normalization():
    assert m.normalize('01','text') != m.normalize('1','text')
    assert m.normalize('1.00','numeric') == m.normalize('1','numeric')
    assert m.normalize('{"x":1.0}','jsonb') == m.normalize('{"x":1}','jsonb')
    assert m.normalize('{"x":"1"}','jsonb') != m.normalize('{"x":1}','jsonb')
    assert m.normalize('{"x":null}','jsonb') != m.normalize('{}','jsonb')


def test_timestamp_offsets_and_naive_rejection():
    assert m.normalize('2026-10-15 21:30:00-03','timestamp with time zone') == m.normalize('2026-10-16T00:30:00+00:00','timestamp with time zone')
    with pytest.raises(ValueError, match='Timezone'):
        m.normalize('2026-10-15 21:30:00','timestamp with time zone')


def test_copy_escaping_and_null():
    assert m.decode_copy(r'\N') is None
    assert m.decode_copy(r'\\N')==r'\N'
    assert m.decode_copy(r'a\tb\n\101\x42')=='a\tb\nAB'


def test_owner_cohorts_never_coalesce():
    exact=m.digest('123')
    rows=[{'owner_chat_id':x} for x in ['123',None,'0','456','789']]
    scopes=[m.owner_scope(r,exact) for r in rows]
    assert len(set(scopes))==5
    assert scopes[:3]==['EXACT_OWNER','OWNER_NULL','OWNER_ZERO']
    assert m.owner_scope({},exact)=='NO_OWNER_COLUMN'


def test_copy_parser_does_not_execute_sql_and_keeps_empty_tables(tmp_path):
    path=tmp_path/'fixture.gz'
    with gzip.open(path,'wt') as f:
        f.write('DROP DATABASE operational;\n\\connect postgres\nCOPY public.empty (id) FROM stdin;\n\\.\nCOPY public.items (id, value) FROM stdin;\n1\t\\N\n\\.\n')
    rows=list(m.copy_blocks(path))
    assert len(rows)==3 and rows[0][1]=='empty'
    assert rows[2][-1]=={'id':'1','value':None}


@pytest.mark.parametrize('text',[
    'COPY public.items (id) FROM stdin;\n1\n',
    'COPY public.items (id) FROM stdin;\n1\t2\n\\.\n',
    'COPY public.items (id) FROM stdin;\n\\.\nCOPY public.items (id) FROM stdin;\n\\.\n',
])
def test_rejects_incomplete_width_and_repeated_copy(tmp_path,text):
    path=tmp_path/'bad.gz'
    with gzip.open(path,'wt') as f:f.write(text)
    with pytest.raises(ValueError):list(m.copy_blocks(path))


def test_crc_corruption_fails(tmp_path):
    path=tmp_path/'broken.gz'
    data=gzip.compress(b'COPY public.items (id) FROM stdin;\n1\n\\.\n')
    path.write_bytes(data[:-4])
    with pytest.raises((EOFError,gzip.BadGzipFile)):list(m.copy_blocks(path))


def test_identifiers_cannot_inject_sql():
    with pytest.raises(ValueError):m.identifier('x; DELETE FROM x')


def test_immutable_output(tmp_path):
    path=tmp_path/'receipt.json'
    m.write_new(path,{'old':True})
    with pytest.raises(FileExistsError):m.write_new(path,{'old':False})
    assert json.loads(path.read_text())=={'old':True}


def fixture_compare(tmp_path,duplicate=False,complete=True):
    backup=tmp_path/'backup';backup.mkdir()
    spec={'columns':['id','value'],'keys':['id'],'key_kind':'PRIMARY KEY'}
    manifest={'source_sha256':'synthetic','tables':{'items':spec},'profiles':{'items':{'n':2}}}
    m.write_new(backup/'manifest.json',manifest)
    with gzip.open(backup/'fingerprints.jsonl.gz','wt') as f:
        for row in [['items','k1','v1','OWNER_NULL'],['items','k2','v2','EXACT_OWNER']]:
            f.write(json.dumps(row)+'\n')
    live=tmp_path/'live.gz'
    rows=[{'meta':{'manifest_digest':m.digest(manifest),'source_as_of':'fixture'}},
          ['items','k1','changed','EXACT_OWNER'],['items','k3','v3','EXACT_OWNER']]
    if duplicate:rows.append(['items','k1','another','EXACT_OWNER'])
    rows.append({'profile':'items','values':{'n':3 if duplicate else 2}})
    if complete:rows.append({'complete':True})
    with gzip.open(live,'wt') as f:
        for row in rows:f.write(json.dumps(row)+'\n')
    return backup,live


def test_compare_detects_missing_changed_and_owner_changed(tmp_path):
    backup,live=fixture_compare(tmp_path)
    m.compare(backup,live,tmp_path/'out')
    r=json.loads((tmp_path/'out'/'reconciliation.json').read_text())['tables']['items']
    assert (r['shared_keys'],r['changed_content'],r['changed_scope'],r['backup_missing_local'],r['local_only'])==(1,1,1,1,1)


def test_compare_never_overwrites_duplicate_versions(tmp_path):
    backup,live=fixture_compare(tmp_path,duplicate=True)
    m.compare(backup,live,tmp_path/'out')
    r=json.loads((tmp_path/'out'/'reconciliation.json').read_text())['tables']['items']
    assert r['duplicate_keys']['local']=={'groups':1,'extra_rows':1}
    assert 'shared_keys' not in r


def test_compare_rejects_partial_capture(tmp_path):
    backup,live=fixture_compare(tmp_path,complete=False)
    with pytest.raises(ValueError,match='Incomplete'):
        m.compare(backup,live,tmp_path/'out')


def test_unsupported_copy_is_not_silently_ignored(tmp_path):
    path=tmp_path/'quoted.gz'
    with gzip.open(path,'wt') as f:f.write('COPY "public"."items" (id) FROM stdin;\n1\n\\.\n')
    with pytest.raises(ValueError,match='Unsupported COPY'):
        list(m.copy_blocks(path))


def test_tampered_fingerprint_rejected(tmp_path):
    backup,live=fixture_compare(tmp_path)
    path=backup/'manifest.json'
    manifest=json.loads(path.read_text());manifest['fingerprints_sha256']='incorrect'
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError,match='hash mismatch'):
        m.compare(backup,live,tmp_path/'out')


sys.path.insert(0,str(Path(__file__).parents[1]/'scripts'))
from audit_measurement_offline import valuations,schema_check,capture_changes


def test_valuation_does_not_zero_fill_unknowns_or_convert_fx():
    s=[{'snapshot_id':str(i),'scraped_at':'2026-10-01','total_value_ars':'10'} for i in range(4)]
    p=[{'snapshot_id':'0','quantity':'2','current_price':'5','currency':'ARS'},
       {'snapshot_id':'1','quantity':None,'current_price':'5','currency':'ARS'},
       {'snapshot_id':'2','quantity':'2','current_price':'5','currency':'USD'}]
    r=valuations({'tables':{'portfolio_snapshots':s,'positions':p}})
    assert r['absolute_gap_buckets_ARS']=={'LE_0_01':1,'MISSING_INPUT':2,'FX_REQUIRED':1}


def test_quoted_column_ddl_type(tmp_path):
    path=tmp_path/'ddl.gz'
    with gzip.open(path,'wt') as f:f.write('CREATE TABLE public.market_candles (\n    "interval" text NOT NULL,\n    close_price numeric(20,4)\n);\n')
    manifest={'tables':{'market_candles':{'types':{'interval':'text','close_price':'numeric'}}}}
    assert schema_check(path,manifest)['type_differences']==[]


def test_capture_comparison_rejects_owner_mixing():
    with pytest.raises(ValueError,match='Different capture owners'):
        capture_changes({'owner_token':'one'},{'owner_token':'two'}, {})

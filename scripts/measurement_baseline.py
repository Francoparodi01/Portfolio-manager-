"""Read-only measurement census and offline COPY reconciliation (no app imports).

Private artifacts belong under outputs/. The dump is parsed, never executed.
Run --help for the separate catalog, backup, live and compare stages.
"""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
from urllib.parse import urlsplit

CONFIG = None  # Replaced only in the ephemeral stdin copy sent to the container.
VERSION = 'measurement-baseline-v1'
ESCAPE = re.compile(r'\\([0-7]{1,3}|x[0-9a-fA-F]{1,2}|.)')
IDENT = re.compile(r'^[a-z_][a-z0-9_]*$')
TIME_TYPES = {'timestamp with time zone', 'timestamp without time zone', 'date'}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                     ensure_ascii=True, allow_nan=False).encode()).hexdigest()


def file_hash(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def write_new(path, obj):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x', encoding='utf-8', newline='\n') as f:
        json.dump(obj, f, indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False)
        f.write('\n')


def identifier(name):
    if not IDENT.fullmatch(name):
        raise ValueError('Unsupported SQL identifier')
    return '"' + name + '"'


def decode_copy(value):
    if value == r'\N':
        return None
    def replace(m):
        s = m[1]
        if s[0] in '01234567':
            return chr(int(s, 8))
        if s.startswith('x') and len(s) > 1:
            return chr(int(s[1:], 16))
        return {'b': '\b', 'f': '\f', 'n': '\n', 'r': '\r', 't': '\t',
                'v': '\v', '\\': '\\'}.get(s, s)
    return ESCAPE.sub(replace, value)


def decimal_text(value):
    # No Decimal.normalize(): it rounds to the active decimal context precision.
    d = Decimal(str(value))
    if not d.is_finite():
        return str(d)
    s = format(d, 'f')
    if '.' in s:
        s = s.rstrip('0').rstrip('.')
    return '0' if d == 0 else s


def json_typed(v):
    if v is None:
        return ['null']
    if isinstance(v, bool):
        return ['bool', v]
    if isinstance(v, (int, Decimal)):
        return ['number', decimal_text(v)]
    if isinstance(v, str):
        return ['string', v]
    if isinstance(v, list):
        return ['array', [json_typed(x) for x in v]]
    return ['object', [[k, json_typed(x)] for k, x in sorted(v.items())]]


def normalize(v, typ):
    if v is None:
        return None
    if typ in {'smallint', 'integer', 'bigint', 'numeric', 'real', 'double precision'}:
        return decimal_text(v)
    if typ in {'json', 'jsonb'}:
        return json_typed(json.loads(v, parse_float=Decimal))
    if typ == 'boolean':
        if v not in ('t', 'f', 'true', 'false'):
            raise ValueError('Invalid boolean')
        return v in ('t', 'true')
    if typ in TIME_TYPES:
        if v in {'infinity', '-infinity'}:
            return v
        dt = datetime.fromisoformat(v)
        if typ == 'timestamp with time zone':
            if dt.tzinfo is None:
                raise ValueError('Timezone missing')
            dt = dt.astimezone(timezone.utc)
        elif dt.tzinfo is not None:
            raise ValueError('Unexpected timezone')
        return dt.date().isoformat() if typ == 'date' else dt.isoformat()
    # Text, UUID, enums, arrays and bytea preserve PostgreSQL text representation.
    return v


def owner_scope(row, owner):
    col = 'owner_chat_id' if 'owner_chat_id' in row else 'chat_id' if 'chat_id' in row else None
    if not col:
        return 'NO_OWNER_COLUMN'
    v = row[col]
    if v is None:
        return 'OWNER_NULL'
    if str(v) == '0':
        return 'OWNER_ZERO'
    if digest(str(v)) == owner:
        return 'EXACT_OWNER'
    return 'OTHER_OWNER_' + digest(str(v))[:16]


def fingerprint(row, spec, owner):
    values = {c: normalize(row[c], spec['types'][c]) for c in spec['columns']}
    key = [values[c] for c in spec['keys']]
    scope = owner_scope(row, owner)
    # Keys do not imply account ownership; scope travels separately and is compared.
    return [digest(key) if key else None, digest(values), scope]


class Profile:
    def __init__(self, spec):
        self.spec = spec
        self.n = 0
        self.nulls = Counter()
        self.scopes = Counter()
        self.times = {}
        self.nonpositive = Counter()

    def add(self, row, scope):
        self.n += 1
        self.scopes[scope] += 1
        for c, value in row.items():
            if value is None:
                self.nulls[c] += 1
                continue
            typ = self.spec['types'].get(c)
            if typ in TIME_TYPES:
                val = normalize(value, typ)
                prev = self.times.get(c, {'min': val, 'max': val})
                self.times[c] = {'min': min(prev['min'], val), 'max': max(prev['max'], val)}
            if c in {'quantity', 'current_price', 'last_price', 'avg_fill_price', 'open_price',
                     'high_price', 'low_price', 'close_price', 'price_at_decision'}:
                d = Decimal(value)
                if d.is_finite() and d <= 0:
                    self.nonpositive[c] += 1

    def result(self):
        return {'n': self.n, 'scope_counts': dict(self.scopes),
                'nulls': {c: self.nulls[c] for c in self.spec['columns']},
                'time_ranges': self.times, 'nonpositive': dict(self.nonpositive)}


def copy_blocks(path):
    """Yield public COPY rows only; validate all blocks and gzip CRC to EOF."""
    database = None
    active = None
    seen = set()
    with gzip.open(path, 'rt', encoding='utf-8', newline='') as f:
        for line in f:
            line = line.rstrip('\r\n')
            if active:
                if line == r'\.':
                    active = None
                    continue
                schema, table, columns = active
                raw = line.split('\t')
                if len(raw) != len(columns):
                    raise ValueError('COPY width mismatch')
                if schema == 'public':
                    yield database, table, columns, dict(zip(columns, map(decode_copy, raw)))
                continue
            if line.startswith('\\connect '):
                database = line.split(' ', 1)[1].strip('"')
            m = re.fullmatch(r'COPY ([a-z_][a-z0-9_]*)\.([a-z_][a-z0-9_]*) \((.*)\) FROM stdin;', line)
            if line.startswith('COPY ') and m is None:
                raise ValueError('Unsupported COPY syntax; do not silently omit a relation')
            if m:
                schema, table = m[1], m[2]
                columns = [c.strip().strip('"') for c in m[3].split(',')]
                for c in columns:
                    identifier(c)
                if (database, schema, table) in seen:
                    raise ValueError('Repeated COPY block')
                seen.add((database, schema, table))
                active = schema, table, columns
                if schema == 'public':
                    yield database, table, columns, None
    if active:
        raise ValueError('Truncated COPY block')


def build_backup(path, catalog_path, output):
    catalog = json.loads(Path(catalog_path).read_text())
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    specs, profiles, database_names = {}, {}, set()
    raw_hash = file_hash(path)
    with gzip.open(output / 'fingerprints.jsonl.gz', 'xt', encoding='utf-8') as out:
        for database, table, columns, row in copy_blocks(path):
            database_names.add(database)
            if len(database_names) != 1:
                raise ValueError('Multiple public databases; keep separate sources')
            if row is None:
                local = catalog['tables'].get(table)
                if local is None or any(c not in local['types'] for c in columns):
                    raise ValueError('Schema not comparable; explicit handling required: ' + table)
                choices = [x for x in local['unique_keys'] if all(c in columns for c in x['columns'])]
                choices.sort(key=lambda k: (k['kind'] != 'PRIMARY KEY', len(k['columns']), k['columns']))
                spec = {'columns': columns, 'types': {c: local['types'][c] for c in columns},
                        'keys': choices[0]['columns'] if choices else [],
                        'key_kind': choices[0]['kind'] if choices else 'NO_DECLARED_KEY'}
                specs[table] = spec
                profiles[table] = Profile(spec)
                continue
            fp = fingerprint(row, specs[table], catalog['owner_digest'])
            profiles[table].add(row, fp[2])
            out.write(json.dumps([table, *fp], separators=(',', ':')) + '\n')
    if raw_hash != file_hash(path):
        raise ValueError('Source changed while reading')
    result = {'version': VERSION, 'source_name': Path(path).name, 'source_sha256': raw_hash,
              'fingerprints_sha256': file_hash(output / 'fingerprints.jsonl.gz'),
              'catalog_sha256': file_hash(catalog_path), 'owner_digest': catalog['owner_digest'],
              'source_as_of': 'UNKNOWN', 'gzip_crc': 'FULL_STREAM_PASSED',
              'dump_sql_executed': False, 'tables': specs,
              'profiles': {t: p.result() for t, p in profiles.items()}}
    write_new(output / 'manifest.json', result)
    print(json.dumps({'tables': len(specs), 'rows': sum(p.n for p in profiles.values()),
                      'source_sha256': raw_hash}))


async def connect_readonly(config):
    import asyncpg
    dsn = config.get('dsn') or os.environ['DATABASE_URL']
    dsn = dsn.replace('postgresql+asyncpg://', 'postgresql://')
    host = urlsplit(dsn).hostname
    if config.get('source_kind', 'local') == 'local' and host not in {'db', 'cocos_db', 'localhost', '127.0.0.1'}:
        raise ValueError('Expected local connection')
    opts = {'timeout': 15, 'command_timeout': 180, 'server_settings': {
        'default_transaction_read_only': 'on', 'statement_timeout': '180000',
        'lock_timeout': '3000', 'TimeZone': 'UTC', 'application_name': 'measurement_baseline_readonly_v1'}}
    if config.get('source_kind') == 'remote':
        opts['ssl'] = 'require'
    return await asyncpg.connect(dsn, **opts)


async def catalog_runtime(config):
    conn = await connect_readonly(config)
    try:
        async with conn.transaction(readonly=True, isolation='repeatable_read'):
            columns = await conn.fetch("SELECT c.table_name,c.column_name,c.data_type,c.is_nullable FROM information_schema.columns c JOIN information_schema.tables t USING(table_catalog,table_schema,table_name) WHERE c.table_schema='public' AND t.table_type='BASE TABLE' ORDER BY c.table_name,c.ordinal_position")
            tables = {}
            for r in columns:
                t = tables.setdefault(r['table_name'], {'types': {}, 'nullable': {}, 'unique_keys': []})
                t['types'][r['column_name']] = r['data_type']
                t['nullable'][r['column_name']] = r['is_nullable']
            keys = await conn.fetch("SELECT t.table_name,t.constraint_type,array_agg(k.column_name::text ORDER BY k.ordinal_position) columns FROM information_schema.table_constraints t JOIN information_schema.key_column_usage k USING(constraint_catalog,constraint_schema,constraint_name) WHERE t.table_schema='public' AND t.constraint_type IN ('PRIMARY KEY','UNIQUE') GROUP BY t.table_name,t.constraint_name,t.constraint_type")
            for r in keys:
                tables[r['table_name']]['unique_keys'].append({'kind': r['constraint_type'], 'columns': r['columns']})
            owner = str(config.get('owner') or os.environ['TELEGRAM_CHAT_ID'])
            if owner == '0':
                raise ValueError('Exact nonzero owner required')
            for table, spec in tables.items():
                q = identifier(table)
                spec['n'] = await conn.fetchval(f'SELECT count(*) FROM public.{q}')
                col = next((c for c in ('owner_chat_id', 'chat_id') if c in spec['types']), None)
                if col:
                    rr = await conn.fetch(f'SELECT {identifier(col)}::text owner,count(*) n FROM public.{q} GROUP BY {identifier(col)}')
                    spec['scope_counts'] = {owner_scope({col: r['owner']}, digest(owner)): r['n'] for r in rr}
            foreign = await conn.fetch("SELECT c.conrelid::regclass::text table_name,c.conname,pg_get_constraintdef(c.oid) definition,c.convalidated FROM pg_constraint c JOIN pg_namespace n ON n.oid=c.connamespace WHERE n.nspname='public' AND c.contype='f'")
            return {'version': VERSION, 'source_kind': config.get('source_kind','local'),
                    'source_as_of': str(await conn.fetchval('SELECT CURRENT_TIMESTAMP')),
                    'transaction_read_only': await conn.fetchval('SHOW transaction_read_only'),
                    'isolation': await conn.fetchval('SHOW transaction_isolation'),
                    'timezone': await conn.fetchval('SHOW timezone'),
                    'server_version': await conn.fetchval('SHOW server_version'),
                    'owner_digest': digest(owner), 'tables': tables,
                    'foreign_keys': [dict(r) for r in foreign]}
    finally:
        await conn.close()


async def live_runtime(config):
    manifest = config['manifest']
    conn = await connect_readonly(config)
    try:
        async with conn.transaction(readonly=True, isolation='repeatable_read'):
            if await conn.fetchval('SHOW transaction_read_only') != 'on':
                raise ValueError('Read-only guard failed')
            owner = digest(os.environ['TELEGRAM_CHAT_ID'])
            if owner != manifest['owner_digest']:
                raise ValueError('Owner mismatch')
            out = gzip.GzipFile(fileobj=sys.stdout.buffer, mode='wb', mtime=0)
            def emit(obj):
                out.write((json.dumps(obj, separators=(',', ':'), default=str) + '\n').encode())
            emit({'meta': {'source_as_of': await conn.fetchval('SELECT CURRENT_TIMESTAMP'),
                          'transaction_read_only': 'on', 'isolation': 'repeatable read',
                          'manifest_digest': digest(manifest)}})
            for table, spec in sorted(manifest['tables'].items()):
                cols = ','.join(identifier(c) + '::text AS ' + identifier(c) for c in spec['columns'])
                profile = Profile(spec)
                async for record in conn.cursor(f'SELECT {cols} FROM public.{identifier(table)}', prefetch=1000):
                    row = dict(record)
                    fp = fingerprint(row, spec, owner)
                    profile.add(row, fp[2])
                    emit([table, *fp])
                emit({'profile': table, 'values': profile.result()})
            emit({'complete': True})
            out.close()
    finally:
        await conn.close()


async def checks_runtime(config):
    conn = await connect_readonly(config)
    try:
        async with conn.transaction(readonly=True, isolation='repeatable_read'):
            owner = int(os.environ['TELEGRAM_CHAT_ID'])
            result = {'source_as_of': str(await conn.fetchval('SELECT CURRENT_TIMESTAMP')),
                      'transaction_read_only': await conn.fetchval('SHOW transaction_read_only'),
                      'sql_sha256': hashlib.sha256(config['sql'].encode()).hexdigest(), 'checks': {}}
            for name, sql in re.findall(r'-- name: (\w+)\n(.*?)(?=-- name:|\Z)', config['sql'], re.S):
                # Fixed reviewed SELECT statements; fail closed if someone adds mutations.
                if not sql.lstrip().upper().startswith('SELECT') or re.search(r'\b(INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|COPY|CALL|TRUNCATE)\b',sql,re.I):
                    raise ValueError('Only aggregate SELECT checks allowed')
                rows = await conn.fetch(sql, *([owner] if '$1' in sql else []))
                result['checks'][name] = [dict(r) for r in rows]
            fks = await conn.fetch("""SELECT c.conname,ct.relname child,pt.relname parent,
                array_agg(ca.attname::text ORDER BY k.ord) child_cols,
                array_agg(pa.attname::text ORDER BY k.ord) parent_cols
                FROM pg_constraint c JOIN pg_class ct ON ct.oid=c.conrelid
                JOIN pg_class pt ON pt.oid=c.confrelid JOIN pg_namespace ns ON ns.oid=ct.relnamespace
                CROSS JOIN LATERAL unnest(c.conkey,c.confkey) WITH ORDINALITY k(ca,pa,ord)
                JOIN pg_attribute ca ON ca.attrelid=ct.oid AND ca.attnum=k.ca
                JOIN pg_attribute pa ON pa.attrelid=pt.oid AND pa.attnum=k.pa
                WHERE c.contype='f' AND ns.nspname='public'
                GROUP BY c.conname,ct.relname,pt.relname ORDER BY 2,1""")
            result['foreign_key_checks'] = []
            for fk in fks:
                where = ' AND '.join('c.'+identifier(c)+' IS NOT NULL' for c in fk['child_cols'])
                match = ' AND '.join('c.'+identifier(a)+'=p.'+identifier(b) for a,b in zip(fk['child_cols'],fk['parent_cols']))
                n = await conn.fetchval(f'SELECT count(*) FROM public.{identifier(fk["child"])} c WHERE {where} AND NOT EXISTS (SELECT 1 FROM public.{identifier(fk["parent"])} p WHERE {match})')
                result['foreign_key_checks'].append({'constraint':fk['conname'],'child':fk['child'],'parent':fk['parent'],'dangling':n})
            return result
    finally:
        await conn.close()


async def details_runtime(config):
    conn = await connect_readonly(config)
    try:
        async with conn.transaction(readonly=True, isolation='repeatable_read'):
            if await conn.fetchval('SHOW transaction_read_only') != 'on':
                raise ValueError('Read-only guard failed')
            owner = digest(os.environ['TELEGRAM_CHAT_ID'])
            if owner != config['manifest']['owner_digest']:
                raise ValueError('Owner mismatch')
            result = {'source_as_of': str(await conn.fetchval('SELECT CURRENT_TIMESTAMP')),
                      'transaction_read_only': 'on', 'tables': {}}
            for table, expected in config['changed'].items():
                spec = config['manifest']['tables'][table]
                cols = ','.join(identifier(c)+'::text AS '+identifier(c) for c in spec['columns'])
                found = {}
                async for record in conn.cursor(f'SELECT {cols} FROM public.{identifier(table)}', prefetch=1000):
                    row = dict(record)
                    key, content, scope = fingerprint(row,spec,owner)
                    if key in expected:
                        found[key] = {'scope':scope,'same_as_compared_snapshot':content==expected[key],
                                      'fields':{c:digest(normalize(row[c],spec['types'][c])) for c in spec['columns']}}
                result['tables'][table] = found
            return result
    finally:
        await conn.close()


def explain_changes(backup, manifest_path, details_path, output):
    manifest = json.loads(Path(manifest_path).read_text())
    if file_hash(backup) != manifest['source_sha256']:
        raise ValueError('Backup hash mismatch')
    details = json.loads(Path(details_path).read_text())
    result = {'source_as_of':details['source_as_of'], 'backup_sha256':manifest['source_sha256'],
              'details_sha256':file_hash(details_path),'tables':{}}
    for _,table,_,row in copy_blocks(backup):
        if row is None or table not in details['tables']:
            continue
        spec = manifest['tables'][table]
        key,_,scope = fingerprint(row,spec,manifest['owner_digest'])
        current = details['tables'][table].get(key)
        if current is None:
            continue
        item=result['tables'].setdefault(table,{}).setdefault(scope,{'rows':0,'later_version_conflicts':0,'changed_fields':Counter()})
        item['rows']+=1
        if not current['same_as_compared_snapshot']:
            item['later_version_conflicts']+=1
            continue
        item['changed_fields'].update(c for c in spec['columns'] if digest(normalize(row[c],spec['types'][c]))!=current['fields'][c])
    write_new(output,result)
    print(json.dumps(result))


def run_container(config, container, destination, binary=False):
    dest = Path(destination)
    if dest.exists():
        raise FileExistsError('Choose a new artifact path')
    source = Path(__file__).read_text(encoding='utf-8').replace(
        'CONFIG = None  # Replaced only in the ephemeral stdin copy sent to the container.',
        'CONFIG = ' + repr(config), 1)
    dest.parent.mkdir(parents=True, exist_ok=True)
    # Streaming output, no credentials in argv and no stderr propagation.
    with dest.open('xb') as out:
        proc = subprocess.run(['docker', 'exec', '-i', container, 'python', '-', '--runtime'],
                              input=source.encode(), stdout=out, stderr=subprocess.PIPE)
    if proc.returncode:
        try:
            safe = json.loads(proc.stderr)
            print(json.dumps({k: safe.get(k) for k in ('status','error_type','sqlstate')}))
        except (ValueError, UnicodeDecodeError):
            print(json.dumps({'status': 'FAILED', 'error_type': 'RUNTIME_STARTUP_ERROR'}))
        raise RuntimeError('Capture failed; incomplete artifact retained, do not use')
    if binary:
        with gzip.open(dest, 'rt', encoding='utf-8') as f:
            tail = None
            for line in f:
                tail = json.loads(line)
        if tail != {'complete': True}:
            raise ValueError('Incomplete live artifact')
    print(json.dumps({'path': str(dest), 'sha256': file_hash(dest)}))


def compare(backup_dir, live_path, output):
    """Multisets on declared keys: duplicate keys are reported, never overwritten."""
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    manifest = json.loads((Path(backup_dir) / 'manifest.json').read_text())
    fingerprint_hash = file_hash(Path(backup_dir) / 'fingerprints.jsonl.gz')
    if manifest.get('fingerprints_sha256',fingerprint_hash) != fingerprint_hash:
        raise ValueError('Backup fingerprint hash mismatch')
    db = sqlite3.connect(output / 'comparison.sqlite')
    db.execute('CREATE TABLE fingerprints (source TEXT, tbl TEXT, key TEXT, content TEXT, scope TEXT)')
    profiles, meta, complete = {}, {}, False
    for source, path in [('backup', Path(backup_dir) / 'fingerprints.jsonl.gz'), ('local', Path(live_path))]:
        batch = []
        with gzip.open(path, 'rt', encoding='utf-8') as f:
            for line in f:
                row = json.loads(line)
                if isinstance(row, dict):
                    if 'profile' in row:
                        profiles[row['profile']] = row['values']
                    if 'meta' in row:
                        meta = row['meta']
                    if row.get('complete'):
                        complete = True
                    continue
                batch.append((source, *row))
                if len(batch) >= 5000:
                    db.executemany('INSERT INTO fingerprints VALUES (?,?,?,?,?)', batch)
                    batch.clear()
        db.executemany('INSERT INTO fingerprints VALUES (?,?,?,?,?)', batch)
        db.commit()
    if not complete or meta.get('manifest_digest') != digest(manifest):
        raise ValueError('Incomplete capture or manifest mismatch')
    db.execute('CREATE INDEX fp_index ON fingerprints(tbl,source,key)')
    result = {'version': VERSION, 'backup_sha256': manifest['source_sha256'],
              'backup_fingerprints_sha256': fingerprint_hash,
              'live_sha256': file_hash(live_path), 'live_meta': meta, 'tables': {}}
    for table, spec in manifest['tables'].items():
        info = {'key_columns': spec['keys'], 'key_kind': spec['key_kind'],
                'compared_columns': spec['columns'], 'backup': manifest['profiles'][table],
                'local': profiles[table]}
        if spec['keys']:
            dup = {}
            for source in ['backup', 'local']:
                dup[source] = db.execute('SELECT count(*),coalesce(sum(n-1),0) FROM (SELECT count(*) n FROM fingerprints WHERE tbl=? AND source=? GROUP BY key HAVING count(*)>1)', (table,source)).fetchone()
            info['duplicate_keys'] = {k: {'groups': v[0], 'extra_rows': v[1]} for k,v in dup.items()}
            if any(v[0] for v in dup.values()):
                info['comparison_status'] = 'DUPLICATE_KEYS_REQUIRES_MULTIVERSION_REVIEW'
            else:
                counts = db.execute('SELECT count(*),coalesce(sum(a.content<>b.content),0),coalesce(sum(a.scope<>b.scope),0) FROM fingerprints a JOIN fingerprints b ON a.tbl=b.tbl AND a.key=b.key WHERE a.tbl=? AND a.source=? AND b.source=?', (table,'backup','local')).fetchone()
                info.update(shared_keys=counts[0], changed_content=counts[1], changed_scope=counts[2],
                            backup_missing_local=info['backup']['n']-counts[0],
                            local_only=info['local']['n']-counts[0], comparison_status='COMPARED_SHARED_COLUMNS')
        else:
            info['comparison_status'] = 'NO_DECLARED_KEY'
        result['tables'][table] = info
    db.close()
    write_new(output / 'reconciliation.json', result)
    print(json.dumps({'tables': len(result['tables']), 'source_as_of': meta['source_as_of'],
                      'missing_keys': {t:r['backup_missing_local'] for t,r in result['tables'].items() if r.get('backup_missing_local')},
                      'uncompared': [t for t,r in result['tables'].items() if r['comparison_status']!='COMPARED_SHARED_COLUMNS']}))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest='command', required=True)
    for name in ('catalog', 'live', 'checks', 'details'):
        s = sub.add_parser(name)
        s.add_argument('--container', default='cocos_scheduler')
        s.add_argument('--output', required=True)
        if name in ('live','details'):
            s.add_argument('--manifest', required=True)
        if name == 'details':
            s.add_argument('--comparison-db', required=True)
        if name == 'checks':
            s.add_argument('--sql', default=str(Path(__file__).with_name('measurement_checks.sql')))
    s = sub.add_parser('backup')
    s.add_argument('--input', required=True)
    s.add_argument('--catalog', required=True)
    s.add_argument('--output-dir', required=True)
    s = sub.add_parser('explain')
    s.add_argument('--backup',required=True)
    s.add_argument('--manifest',required=True)
    s.add_argument('--details',required=True)
    s.add_argument('--output',required=True)
    s = sub.add_parser('compare')
    s.add_argument('--backup-dir', required=True)
    s.add_argument('--live', required=True)
    s.add_argument('--output-dir', required=True)
    a = p.parse_args()
    if a.command == 'backup':
        build_backup(a.input, a.catalog, a.output_dir)
    elif a.command == 'compare':
        compare(a.backup_dir, a.live, a.output_dir)
    elif a.command == 'explain':
        explain_changes(a.backup,a.manifest,a.details,a.output)
    else:
        config = {'command': a.command}
        if a.command in ('live','details'):
            config['manifest'] = json.loads(Path(a.manifest).read_text())
        if a.command == 'details':
            db=sqlite3.connect(Path(a.comparison_db).resolve().as_uri()+'?mode=ro',uri=True)
            selected=('market_candles','decision_log','radar_discovery_outcomes','plan_execution_attributions','corporate_event_applications','price_quality_flags')
            config['changed']={}
            for table in selected:
                rows=db.execute('SELECT b.key,b.content FROM fingerprints a JOIN fingerprints b ON a.tbl=b.tbl AND a.key=b.key WHERE a.tbl=? AND a.source=? AND b.source=? AND a.content<>b.content',(table,'backup','local')).fetchall()
                config['changed'][table]=dict(rows)
            db.close()
        if a.command == 'checks':
            config['sql'] = Path(a.sql).read_text(encoding='utf-8')
        run_container(config, a.container, a.output, binary=a.command == 'live')


if __name__ == '__main__':
    if '--runtime' in sys.argv:
        try:
            if CONFIG['command'] == 'catalog':
                print(json.dumps(asyncio.run(catalog_runtime(CONFIG)), sort_keys=True))
            elif CONFIG['command'] == 'checks':
                print(json.dumps(asyncio.run(checks_runtime(CONFIG)), sort_keys=True,default=str))
            elif CONFIG['command'] == 'details':
                print(json.dumps(asyncio.run(details_runtime(CONFIG)),sort_keys=True))
            else:
                asyncio.run(live_runtime(CONFIG))
        except Exception as exc:
            # SQL or connection errors may contain private data: emit class only.
            print(json.dumps({'status': 'FAILED', 'error_type': type(exc).__name__,
                              'sqlstate': getattr(exc, 'sqlstate', None)}), file=sys.stderr)
            raise SystemExit(1)
    else:
        main()

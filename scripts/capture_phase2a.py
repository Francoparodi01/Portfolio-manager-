"""Private Phase 2A SELECT capture. No application imports or schema bootstrap.

Exact-owner holdings; unscoped broker/legacy evidence remains quarantined.
Uses the existing container connection without exporting its credentials.
"""
from __future__ import annotations

import argparse
import asyncio
import gzip
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlsplit


QUERIES = {
    'portfolio_snapshots': 'SELECT t.* FROM public.portfolio_snapshots t WHERE owner_chat_id=$1',
    'positions': 'SELECT t.* FROM public.positions t JOIN public.portfolio_snapshots p USING(snapshot_id) WHERE p.owner_chat_id=$1',
    'raw_snapshots': 'SELECT t.* FROM public.raw_snapshots t JOIN public.portfolio_snapshots p USING(snapshot_id) WHERE p.owner_chat_id=$1',
}
# These are separate provenance/identity investigations, never a combined account.
QUARANTINE = ('broker_fills', 'broker_movements', 'decision_log', 'execution_plans',
              'position_hold_observations', 'plan_execution_attributions',
              'plan_execution_attribution_plans', 'plan_execution_attribution_movements',
              'corporate_event_applications', 'corporate_events')


async def capture():
    import asyncpg
    owner = int(os.environ['TELEGRAM_CHAT_ID'])
    if not owner:
        raise ValueError('Explicit owner required')
    dsn = os.environ['DATABASE_URL'].replace('postgresql+asyncpg://', 'postgresql://')
    if urlsplit(dsn).hostname not in {'db', 'cocos_db', 'localhost', '127.0.0.1'}:
        raise ValueError('This collector is restricted to the local source')
    conn = await asyncpg.connect(dsn, timeout=15, server_settings={
        'default_transaction_read_only': 'on', 'statement_timeout': '60000',
        'application_name': 'phase2a_readonly'})
    try:
        async with conn.transaction(readonly=True, isolation='repeatable_read'):
            result = {
                'schema_version': 'phase2a-capture-v1',
                'captured_at': str(await conn.fetchval('SELECT CURRENT_TIMESTAMP')),
                'transaction_read_only': await conn.fetchval('SHOW transaction_read_only'),
                'owner_digest': hashlib.sha256(json.dumps(str(owner), separators=(',', ':')).encode()).hexdigest(),
                'tables': {}, 'schema': {}, 'scope': {},
            }
            for table in (*QUERIES, *QUARANTINE):
                columns = await conn.fetch('''SELECT column_name,data_type FROM information_schema.columns
                    WHERE table_schema='public' AND table_name=$1 ORDER BY ordinal_position''', table)
                if not columns:
                    raise ValueError('Required table missing')
                result['schema'][table] = {r['column_name']: r['data_type'] for r in columns}
                sql = QUERIES.get(table, f'SELECT t.* FROM public.{table} t')
                args = (owner,) if table in QUERIES else ()
                count = await conn.fetchval(f'SELECT count(*) FROM ({sql}) q', *args)
                if count > 100000:
                    raise ValueError('Capture bound exceeded; no partial artifact')
                # PostgreSQL text representations preserve numeric and JSON digits.
                projection = ','.join('q."{0}"::text AS "{0}"'.format(r['column_name']) for r in columns)
                rows = [dict(r) for r in await conn.fetch(f'SELECT {projection} FROM ({sql}) q', *args)]
                rows.sort(key=lambda r: json.dumps(r, sort_keys=True))
                result['tables'][table] = rows
                result['scope'][table] = 'EXACT_OWNER_OR_PARENT' if table in QUERIES else 'PARTITION_BEFORE_USE'
    finally:
        await conn.close()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--container', default='cocos_scheduler')
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError('Use a new immutable capture path')
    proc = subprocess.run(['docker', 'exec', '-i', args.container, 'python', '-', '--runtime'],
                          input=Path(__file__).read_bytes(), capture_output=True)
    if proc.returncode:
        raise RuntimeError('Read-only capture failed; private exception suppressed')
    result = json.loads(proc.stdout)
    if result['transaction_read_only'] != 'on':
        raise ValueError('Read-only guard failed')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('xb') as f:
        f.write(gzip.compress(proc.stdout, mtime=0))
    print(json.dumps({'captured_at': result['captured_at'],
                      'sha256': hashlib.sha256(args.output.read_bytes()).hexdigest(),
                      'rows': {k: len(v) for k, v in result['tables'].items()}}))


if __name__ == '__main__':
    if '--runtime' in sys.argv:
        print(json.dumps(asyncio.run(capture()), sort_keys=True, allow_nan=False))
    else:
        main()

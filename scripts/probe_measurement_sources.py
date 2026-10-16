"""Sanitized source-access receipts. Never prints DSNs, passwords or owner IDs."""
import argparse
import asyncio
import json
from pathlib import Path
import subprocess
from urllib.parse import quote, urlsplit

from measurement_baseline import catalog_runtime, file_hash, write_new


def env_file(path):
    # Literal dotenv values only, no interpolation or execution.
    from dotenv import dotenv_values
    return dotenv_values(path, interpolate=False)


async def main(args):
    owner = env_file(args.owner_env)['TELEGRAM_CHAT_ID']
    results = []
    for file in args.remote_env:
        values = env_file(file)
        dsn = values.get('DATABASE_URL')
        host = urlsplit(dsn or '').hostname
        if not host or not (host.endswith('.supabase.com') or host.endswith('.supabase.co')):
            raise ValueError('Expected an explicit Supabase configuration')
        receipt = {'source': 'SUPABASE_ORIGINAL_CANDIDATE', 'configuration_file': Path(file).name,
                   'host': host, 'status': 'NO VERIFICADA'}
        try:
            data = await catalog_runtime({'dsn': dsn, 'owner': owner, 'source_kind': 'remote'})
            receipt.update(status='VERIFIED_ACCESS', catalog=data)
        except Exception as exc:
            receipt.update(error_type=type(exc).__name__, sqlstate=getattr(exc, 'sqlstate', None))
        results.append(receipt)
    for container in args.legacy_container:
        obj = json.loads(subprocess.check_output(['docker','inspect',container]))[0]
        values = dict(s.split('=',1) for s in obj['Config']['Env'] if '=' in s)
        port = obj['NetworkSettings']['Ports']['5432/tcp'][0]['HostPort']
        user = values['POSTGRES_USER']
        database = values.get('POSTGRES_DB',user)
        dsn = 'postgresql://{}:{}@127.0.0.1:{}/{}'.format(quote(user,safe=''),quote(values['POSTGRES_PASSWORD'],safe=''),port,quote(database,safe=''))
        receipt = {'source': container, 'host': '127.0.0.1', 'port': port, 'status': 'NO VERIFICADA'}
        try:
            receipt.update(status='VERIFIED_ACCESS', catalog=await catalog_runtime({'dsn': dsn,'owner': owner}))
        except Exception as exc:
            receipt.update(status='NO VERIFICADA',error_type=type(exc).__name__,sqlstate=getattr(exc,'sqlstate',None))
        results.append(receipt)
    write_new(args.output, {'sources': results})
    print(json.dumps({'path':args.output,'sha256':file_hash(args.output),
                      'sources':[{k:v for k,v in r.items() if k!='catalog'} for r in results]}))


if __name__ == '__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--owner-env',required=True)
    p.add_argument('--remote-env',action='append',default=[])
    p.add_argument('--legacy-container',action='append',default=[])
    p.add_argument('--output',required=True)
    asyncio.run(main(p.parse_args()))

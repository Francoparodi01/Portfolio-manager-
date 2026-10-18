"""Capture original historical Cocos statements using an authenticated browser.

Only GET routes observed in the UI; no DB, application bootstrap or email sends.
Raw evidence is private. Outputs/checkpoints are immutable; resume uses hashes.
"""
from __future__ import annotations
import argparse
from datetime import datetime
import gzip
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
from uuid import uuid4

from cocos_activity_audit import context, save_capture
from measurement_baseline import file_hash, digest


def request_plan(dates, retry, account, context_hash):
    if len(set(dates)) != len(dates) or len(dates) > 31:
        raise ValueError('Unique bounded report dates required')
    for date in dates:
        if datetime.strptime(date, '%Y%m%d').strftime('%Y%m%d') != date:
            raise ValueError('Expected YYYYMMDD')
    origin = 'https://api.cocos.capital/api/'
    plan = [{'key': 'portfolio-current', 'url': origin+'v2/portfolio?period=MAX&domain=BROKER&include=costBasis,allocation,cash', 'binary': False},
            {'key': 'orders-current', 'url': origin+'orders', 'binary': False}]
    for currency in ['ARS', 'USD']:
        for kind in ['history', 'balance']:
            url = origin+f'portfolio/{kind}?currency={currency}&period=MAX'+('&metric=balance' if kind == 'history' else '')
            plan.append({'key': f'{kind}-{currency}', 'url': url, 'binary': False})
    for date in sorted(dates):
        for kind in ['PDF', 'CSV']:
            plan.append({'key': f'holdings-{date}.{kind.lower()}',
                         'url': origin+f'portfolio/reports/holdings?type={kind}&date={date}', 'binary': True})
    if retry is not None:
        if (retry.get('schema_version') != 'cocos-activity-capture-v1' or
                str(retry.get('account')) != account or retry.get('owner_context_sha256') != context_hash):
            raise ValueError('Retry source account/context mismatch')
        ids = sorted({str(t['ticket_id']) for t in retry['tickets'] if t['status'] != 200})
        if len(ids) > 100 or any(not x.isdigit() or int(x) <= 0 for x in ids):
            raise ValueError('Unsafe or unbounded retry plan')
        plan.extend({'key': 'retry-ticket-'+digest(t)+'.pdf',
                     'url': origin+f'v1/wallet/tickets/{t}/download', 'binary': True} for t in ids)
    return plan


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--context-capture', type=Path, required=True)
    p.add_argument('--retry-source', type=Path)
    p.add_argument('--dates', nargs='+', required=True)
    p.add_argument('--session', default='quantia-cocos-phase2b')
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    if args.output.exists(): raise FileExistsError('Immutable capture already exists')
    account, _ = context(args.context_capture)
    context_hash = file_hash(args.context_capture)
    retry = json.load(gzip.open(args.retry_source, 'rt', encoding='utf8')) if args.retry_source else None
    plan = request_plan(args.dates, retry, account, context_hash)
    source = Path(__file__).with_suffix('.js').read_text(encoding='utf8')
    code_hash = hashlib.sha256(source.encode()).hexdigest()
    parts = args.output.with_name(args.output.name+'.parts'); parts.mkdir(parents=True, exist_ok=True)
    npx = shutil.which('npx.cmd') or shutil.which('npx')
    if not npx: raise RuntimeError('npx required')
    batches = []
    for start in range(0, len(plan), 8):
        cfg = {'expected_account': account, 'requests': plan[start:start+8]}
        key = digest([cfg, context_hash, code_hash]); path = parts/(key+'.json.gz')
        if path.exists():
            batch = json.load(gzip.open(path, 'rt', encoding='utf8'))
            if batch.get('batch_key') != key: raise ValueError('Checkpoint mismatch')
        else:
            runner = parts/(str(uuid4())+'.runner-private.js')
            runner.write_text(source.replace('/* AUDIT_CONFIG */ {}', json.dumps(cfg)), encoding='utf8')
            try:
                proc = subprocess.run([npx, '--yes', '--package', '@playwright/cli', 'playwright-cli',
                    '-s='+args.session, 'run-code', '--filename', str(runner.resolve()), '--json'],
                    capture_output=True, timeout=300)
            finally: runner.unlink(missing_ok=True)
            if proc.returncode: raise RuntimeError('Browser capture failed; earlier parts retained')
            batch = json.loads(json.loads(proc.stdout)['result'])
            batch.update(batch_key=key, collector_code_sha256_utf8_lf=code_hash)
            save_capture(path, batch)
        batches.append((path, batch))
        print(json.dumps({'batch':len(batches),'records':len(batch['records']),'status':batch['status']}), flush=True)
        if batch['status'] == 'SOURCE_BLOCKED' or len(batch['records']) != len(cfg['requests']): break
    result = {'schema_version':'phase2b-capture-v1', 'account':account,
        'owner_context_sha256':context_hash, 'retry_source_sha256':file_hash(args.retry_source) if args.retry_source else None,
        'collector_code_sha256_utf8_lf':code_hash, 'requests_planned':plan,
        'started_at':min(b['started_at'] for _,b in batches), 'finished_at':max(b['finished_at'] for _,b in batches),
        'batches':[{'sha256':file_hash(path),'status':b['status'],'account_verification':b['account_verification']} for path,b in batches],
        'records':[r for _,b in batches for r in b['records']],
        'errors':[e for _,b in batches for e in b['errors']]}
    result['status'] = ('CAPTURED' if len(result['records']) == len(plan) and not result['errors'] and
                        all(b['status']=='CAPTURED' for _,b in batches) and
                        all(b['account_verification']=='ACTIVE_UI_ACCOUNT_MATCH' for _,b in batches) else 'PARTIAL')
    save_capture(args.output, result)
    print(json.dumps({'status':result['status'],'records':len(result['records']),'input_sha256':file_hash(args.output)}))


if __name__ == '__main__': main()

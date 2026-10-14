"""Private, bounded, repeatable-read QEEF evidence capture; no application bootstrap.

Runs this same file via stdin in an existing container to use its connection
without exporting credentials. Only SELECTs and read-only transaction control.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid


def encode(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (Decimal, uuid.UUID)):
        return str(value)
    raise TypeError(type(value).__name__)


async def capture():
    import asyncpg

    owner = int(os.environ['TELEGRAM_CHAT_ID'])
    if not owner:
        raise ValueError('exact configured owner required')
    dsn = os.environ['DATABASE_URL'].replace('postgresql+asyncpg://', 'postgresql://')
    conn = await asyncpg.connect(dsn, server_settings={
        'default_transaction_read_only': 'on', 'statement_timeout': '30000',
        'application_name': 'qeef_research_readonly_v1'})
    try:
        async with conn.transaction(readonly=True, isolation='repeatable_read'):
            async def rows(sql, *args):
                return [dict(r) for r in await conn.fetch(sql, *args)]

            schema = {}
            for r in await rows("SELECT table_name,column_name FROM information_schema.columns WHERE table_schema='public' ORDER BY table_name,ordinal_position"):
                schema.setdefault(r['table_name'], []).append(r['column_name'])
            result = {'schema_version': 'qeef-capture-v1', 'captured_at': await conn.fetchval('SELECT CURRENT_TIMESTAMP'),
                      'transaction_read_only': await conn.fetchval('SHOW transaction_read_only'),
                      'owner_token': hashlib.sha256(str(owner).encode()).hexdigest()[:16],
                      'schema': schema, 'tables': {}, 'profiles': {}, 'limitations': []}
            # Fixed identifiers, never supplied by a user or a data row.
            direct = ('decision_log', 'execution_plans', 'portfolio_snapshots', 'broker_fills',
                      'position_hold_observations', 'contextual_market_snapshots',
                      'learning_shadow_metric_snapshots', 'learning_shadow_metric_snapshots_v2',
                      'decision_lab_runs', 'decision_lab_plan_captures', 'decision_lab_objects',
                      'radar_discovery_runs', 'shadow_thesis_forecasts', 'learning_shadow_cases',
                      'market_candle_observations', 'plan_execution_attributions')
            for table in direct:
                if table not in schema:
                    result['profiles'][table] = {'status': 'MISSING_TABLE'}
                    continue
                if 'owner_chat_id' not in schema[table]:
                    result['profiles'][table] = {'status': 'OWNER_SCOPE_UNAVAILABLE'}
                    continue
                count = await conn.fetchval(f'SELECT count(*) FROM {table} WHERE owner_chat_id=$1', owner)
                unscoped = await conn.fetchval(f'SELECT count(*) FROM {table} WHERE owner_chat_id IS NULL')
                result['profiles'][table] = {'n': count, 'legacy_unscoped_n': unscoped}
                if count > 100000:
                    result['limitations'].append(f'CAPTURE_LIMIT:{table}:{count}')
                    continue
                # Private output excludes raw broker payloads; keeps the exact recorded links.
                columns = [c for c in schema[table] if c not in {'raw_payload', 'owner_chat_id'}]
                data = await rows(f'SELECT {",".join(columns)} FROM {table} WHERE owner_chat_id=$1', owner)
                result['tables'][table] = sorted(data, key=lambda r: json.dumps(r, default=encode, sort_keys=True))
            children = {
                'order_intents': 'SELECT c.* FROM order_intents c JOIN execution_plans p ON p.id=c.execution_plan_id WHERE p.owner_chat_id=$1',
                'positions': 'SELECT c.* FROM positions c JOIN portfolio_snapshots p ON p.snapshot_id=c.snapshot_id WHERE p.owner_chat_id=$1',
                'raw_snapshots': 'SELECT c.snapshot_id,c.scraped_at FROM raw_snapshots c JOIN portfolio_snapshots p ON p.snapshot_id=c.snapshot_id WHERE p.owner_chat_id=$1',
                'radar_discovery_snapshots': 'SELECT c.* FROM radar_discovery_snapshots c JOIN radar_discovery_runs p ON p.run_id=c.run_id WHERE p.owner_chat_id=$1',
                'contextual_snapshot_candles': 'SELECT c.* FROM contextual_snapshot_candles c JOIN contextual_market_snapshots p ON p.snapshot_id=c.snapshot_id WHERE p.owner_chat_id=$1',
                'broker_movements': 'SELECT c.id,c.external_movement_id,c.executed_at,c.currency,c.movement_type FROM broker_movements c WHERE EXISTS (SELECT 1 FROM broker_fills p WHERE p.owner_chat_id=$1 AND p.external_fill_id=c.external_movement_id)',
                'radar_discovery_outcomes': 'SELECT c.* FROM radar_discovery_outcomes c JOIN radar_discovery_snapshots s ON s.id=c.snapshot_id JOIN radar_discovery_runs p ON p.run_id=s.run_id WHERE p.owner_chat_id=$1',
            }
            for table, sql in children.items():
                if table not in schema:
                    result['profiles'][table] = {'status': 'MISSING_TABLE'}
                    continue
                count = await conn.fetchval(f'SELECT count(*) FROM ({sql}) scoped', owner)
                result['profiles'][table] = {'n': count, 'scope': 'OWNER_PARENT_JOIN'}
                if count > 100000:
                    result['limitations'].append(f'CAPTURE_LIMIT:{table}:{count}')
                    continue
                data = await rows(sql, owner)
                result['tables'][table] = sorted(data, key=lambda r: json.dumps(r, default=encode, sort_keys=True))
            if 'market_candles' in schema:
                checks = ','.join(f'count({c}) AS {c}_known' for c in
                                  ('available_at', 'bar_end', 'calendar', 'adjustment_policy', 'depositary_ratio')
                                  if c in schema['market_candles'])
                result['market_quality'] = await rows(f'''SELECT source,currency,asset_type,venue,interval,count(*) n,
                    min(ts) first_ts,max(ts) last_ts{',' + checks if checks else ''}
                    FROM market_candles GROUP BY 1,2,3,4,5 ORDER BY 1,2,3,4,5''')
            result['lineage'] = (await rows('''SELECT count(*) FILTER(WHERE d.run_id IS NULL) missing_run,
                count(*) FILTER(WHERE d.source='execution_plan' AND NOT EXISTS
                  (SELECT 1 FROM execution_plans p WHERE p.run_id=d.run_id AND p.owner_chat_id=d.owner_chat_id)) orphan_formal_run,
                count(*) FILTER(WHERE d.outcome_20d IS NOT NULL AND d.outcome_filled_at IS NULL) outcome_without_availability
                FROM decision_log d WHERE d.owner_chat_id=$1''', owner))[0]
    finally:
        await conn.close()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True)
    parser.add_argument('--container', default='cocos_scheduler')
    args = parser.parse_args()
    path = Path(args.output)
    if path.exists():
        raise FileExistsError('immutable capture: select a new output path')
    source = Path(__file__).read_bytes()
    proc = subprocess.run(['docker', 'exec', '-i', args.container, 'python', '-', '--runtime'],
                          input=source, capture_output=True)
    if proc.returncode:
        # Connection exceptions may contain connection information: never print them.
        raise RuntimeError('read-only capture failed; no capture published')
    payload = json.loads(proc.stdout)
    if payload['transaction_read_only'] != 'on':
        raise ValueError('read-only guard not confirmed')
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('xb') as stream:
        stream.write(proc.stdout)
    print(json.dumps({'path': str(path), 'sha256': hashlib.sha256(proc.stdout).hexdigest(),
                      'captured_at': payload['captured_at'], 'profiles': payload['profiles'],
                      'limitations': payload['limitations']}))


if __name__ == '__main__':
    if '--runtime' in sys.argv:
        print(json.dumps(asyncio.run(capture()), default=encode, sort_keys=True, allow_nan=False))
    else:
        main()

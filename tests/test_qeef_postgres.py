"""Real PostgreSQL integration in a unique disposable Docker container only."""
import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import time
import uuid

import asyncpg
import pytest

from src.analysis.qeef.contracts import Condition, Thesis, ThesisEvent
from src.analysis.qeef.store import append_artifact, latest_evaluation
from src.analysis.qeef.service import metadata
from src.decision_lab.models import digest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope='module')
def disposable():
    name = 'qeef-test-'+uuid.uuid4().hex[:12]
    subprocess.run(['docker', 'run', '-d', '--rm', '--name', name, '--label', 'quantia.qeef.disposable=true',
        '-p', '127.0.0.1::5432', '-e', 'POSTGRES_HOST_AUTH_METHOD=trust', '-e', 'POSTGRES_DB=qeef_disposable',
        'postgres:17-alpine'], check=True, capture_output=True)
    try:
        port = subprocess.check_output(['docker', 'port', name, '5432'], text=True).strip().split(':')[-1]
        dsn = f'postgresql://postgres@127.0.0.1:{port}/qeef_disposable'
        async def ready():
            conn = await asyncpg.connect(dsn, timeout=2)
            try:
                return await conn.fetchval('SELECT current_database()') == 'qeef_disposable'
            finally:
                await conn.close()
        for _ in range(60):
            # The image starts a temporary Unix-socket server during init.
            # Wait for the published TCP endpoint used by the actual tests.
            try:
                if asyncio.run(ready()):
                    break
            except (OSError, ConnectionError, asyncpg.PostgresError, TimeoutError):
                pass
            time.sleep(.25)
        else:
            raise RuntimeError('disposable PostgreSQL unavailable')
        yield dsn
    finally:
        labels = json.loads(subprocess.check_output(['docker', 'inspect', '--format', '{{json .Config.Labels}}', name], text=True))
        if name.startswith('qeef-test-') and labels.get('quantia.qeef.disposable') == 'true':
            subprocess.run(['docker', 'rm', '-f', name], check=True, capture_output=True)


def test_migration_idempotence_compatibility_immutable_store_and_owner_rls(disposable):
    async def run():
        conn = await asyncpg.connect(disposable)
        try:
            # A preexisting source record must remain exactly unchanged.
            await conn.execute("CREATE TABLE decision_log(id int PRIMARY KEY, decision text); INSERT INTO decision_log VALUES(1,'HOLD')")
            migration = (ROOT/'migrations/20261014_qeef_v1.sql').read_text(encoding='utf-8')
            await conn.execute(migration)
            await conn.execute(migration)
            assert await conn.fetchval('SELECT decision FROM decision_log WHERE id=1') == 'HOLD'
            now = datetime(2026, 10, 14, tzinfo=timezone.utc)
            thesis = Thesis(owner=1, opportunity_id='opp', thesis_id='t', ticker='MU', strategy_version='test',
                thesis_opened_at=now, recorded_at=now, intended_horizon=20, horizon_unit='BYMA_SESSIONS',
                invalidation_conditions=(Condition(condition_id='s', field='price', operator='LT', value=10, unit='ARS', source='TEST'),),
                target_conditions=(Condition(condition_id='t', field='price', operator='GT', value=20, unit='ARS', source='TEST'),),
                source_references=('synthetic-test',), evidence_quality='HIGH')
            kwargs = dict(owner=1, kind='THESIS', artifact_id='t', payload=thesis.model_dump(mode='json'), code_version='test', data_as_of=now)
            assert (await append_artifact(conn, **kwargs))['inserted']
            assert not (await append_artifact(conn, **kwargs))['inserted']
            event = ThesisEvent(owner=1, thesis_id='t', thesis_hash=thesis.content_hash, event_id='target',
                effective_at=now, available_at=now, event_type='TARGET', condition_id='t', observed_value=21,
                observed_unit='ARS', observed_source='TEST', source_references=('synthetic-test',))
            event_args = dict(owner=1, kind='THESIS_EVENT', artifact_id='target', payload=event.model_dump(mode='json'), code_version='test', data_as_of=now)
            assert (await append_artifact(conn, **event_args))['inserted']
            assert not (await append_artifact(conn, **event_args))['inserted']
            with pytest.raises(ValueError, match='after closure'):
                await append_artifact(conn, **{**event_args, 'artifact_id': 'zupdate',
                    'payload': {**event.model_dump(mode='json'), 'event_id': 'zupdate', 'event_type': 'UPDATE'}})
            with pytest.raises(ValueError, match='identity conflict'):
                await append_artifact(conn, **{**kwargs, 'code_version': 'rewrite'})
            evaluation = metadata(code_version='test', evidence_digest='synthetic', data_as_of=now.isoformat(),
                cohort={'owner': 1}, benchmarks={}, costs={}, references=['synthetic'])
            evaluation.update({'owner': 1, 'schema_version': 'qeef-evaluation-v1'})
            evaluation['evaluation_id'] = digest(evaluation)
            eval_args = dict(owner=1, kind='EVALUATION', artifact_id=evaluation['evaluation_id'], payload=evaluation, code_version='test', data_as_of=now)
            assert (await append_artifact(conn, **eval_args))['inserted']
            assert not (await append_artifact(conn, **eval_args))['inserted']
            assert await latest_evaluation(conn, owner=1) == evaluation
            with pytest.raises(ValueError, match='owner mismatch'):
                await append_artifact(conn, **{**eval_args, 'owner': 2})
            for sql in ("UPDATE qeef_artifacts SET code_version='bad'", 'DELETE FROM qeef_artifacts', 'TRUNCATE qeef_artifacts'):
                with pytest.raises(asyncpg.RaiseError, match='append-only'):
                    await conn.execute(sql)
            # Superusers bypass RLS; exercise actual application permissions.
            await conn.execute('CREATE ROLE qeef_test_reader; GRANT USAGE ON SCHEMA public TO qeef_test_reader; GRANT SELECT,INSERT ON qeef_artifacts TO qeef_test_reader')
            await conn.execute('SET ROLE qeef_test_reader')
            async with conn.transaction():
                await conn.execute("SELECT set_config('qeef.owner','2',true)")
                assert await conn.fetchval('SELECT count(*) FROM qeef_artifacts') == 0
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    async with conn.transaction():
                        await conn.execute("INSERT INTO qeef_artifacts SELECT * FROM qeef_artifacts WHERE false")
                        await conn.execute("INSERT INTO qeef_artifacts(owner_chat_id,artifact_id,kind,methodology_version,code_version,evidence_digest,data_as_of,evaluated_at,payload,content_hash) VALUES(1,'intruder','THESIS','v','v','d',now(),now(),'{}','h')")
            async with conn.transaction():
                await conn.execute("SELECT set_config('qeef.owner','1',true)")
                assert await conn.fetchval('SELECT count(*) FROM qeef_artifacts') == 3
            assert (await latest_evaluation(conn, owner=2))['status'] == 'INSUFFICIENT_DATA'
            await conn.execute('RESET ROLE')
            # Non-destructive rollback disables application use, preserving all evidence.
            await conn.execute('REVOKE ALL ON qeef_artifacts FROM qeef_test_reader')
            assert await conn.fetchval('SELECT count(*) FROM qeef_artifacts') == 3
            assert await conn.fetchval('SELECT count(*) FROM decision_log') == 1
        finally:
            await conn.close()
    asyncio.run(run())

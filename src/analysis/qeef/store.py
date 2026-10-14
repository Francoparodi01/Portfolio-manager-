"""Optional research store. Schema setup never runs implicitly."""
import hashlib
import json
from datetime import datetime
from src.decision_lab.models import canonical, digest
from . import METHODOLOGY_VERSION
from .contracts import Thesis, ThesisEvent
from .lifecycle import project_thesis


async def append_artifact(conn, *, owner, kind, artifact_id, payload, code_version, data_as_of):
    if not owner or kind not in {'THESIS', 'THESIS_EVENT', 'EVALUATION'}:
        raise ValueError('explicit owner and known artifact kind required')
    if not code_version or data_as_of.tzinfo is None:
        raise ValueError('code version and aware cutoff required')
    if kind == 'THESIS':
        item = Thesis.model_validate(payload)
        if item.owner != owner or item.thesis_id != artifact_id:
            raise ValueError('thesis identity mismatch')
    elif kind == 'THESIS_EVENT':
        item = ThesisEvent.model_validate(payload)
        if item.owner != owner or item.event_id != artifact_id:
            raise ValueError('event identity mismatch')
    else:
        token = hashlib.sha256(str(owner).encode()).hexdigest()[:16]
        if payload.get('owner') != owner and payload.get('owner_token') != token:
            raise ValueError('evaluation owner mismatch')
        if payload.get('evaluation_id') != artifact_id or payload.get('mode') != 'RESEARCH_ONLY':
            raise ValueError('evaluation identity or authority mismatch')
        if payload.get('schema_version') not in {'qeef-history-v1', 'qeef-evaluation-v1'} or any(payload.get(k) is not False for k in ('affects_analysis','affects_execution','promotion_allowed')):
            raise ValueError('evaluation research authority contract required')
        body = {k: v for k, v in payload.items() if k != 'evaluation_id'}
        if digest(body) != artifact_id:
            raise ValueError('evaluation content identity mismatch')
        if payload.get('code_version') != code_version or payload.get('methodology_version') != METHODOLOGY_VERSION or datetime.fromisoformat(payload['data_as_of']) != data_as_of:
            raise ValueError('evaluation provenance columns disagree with payload')
    if kind == 'THESIS' and item.recorded_at > data_as_of:
        raise ValueError('thesis unavailable at storage cutoff')
    if kind == 'THESIS_EVENT' and item.available_at > data_as_of:
        raise ValueError('event unavailable at storage cutoff')
    content = digest(payload)
    evidence = payload.get('evidence_digest', content)
    async with conn.transaction():
        await conn.execute("SELECT set_config('qeef.owner', $1, true)", str(owner))
        if kind == 'THESIS_EVENT':
            # Serialize events for the same thesis, including concurrent closures.
            await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))", str(owner)+':'+item.thesis_id)
            parent = await conn.fetchrow("SELECT payload FROM qeef_artifacts WHERE owner_chat_id=$1 AND kind='THESIS' AND artifact_id=$2", owner, item.thesis_id)
            if not parent:
                raise ValueError('owner-scoped thesis parent missing')
            thesis = Thesis.model_validate(json.loads(parent['payload']) if isinstance(parent['payload'], str) else parent['payload'])
            if thesis.content_hash != item.thesis_hash:
                raise ValueError('frozen thesis hash mismatch')
            prior = await conn.fetch("SELECT payload FROM qeef_artifacts WHERE owner_chat_id=$1 AND kind='THESIS_EVENT' AND payload->>'thesis_id'=$2", owner, item.thesis_id)
            events = tuple(ThesisEvent.model_validate(json.loads(r['payload']) if isinstance(r['payload'], str) else r['payload']) for r in prior)
            project_thesis(thesis, events+(item,), as_of=max([data_as_of, item.available_at]+[e.available_at for e in events]))
        result = await conn.execute('''INSERT INTO qeef_artifacts(owner_chat_id,artifact_id,kind,methodology_version,
            code_version,evidence_digest,data_as_of,evaluated_at,payload,content_hash)
            VALUES($1,$2,$3,$4,$5,$6,$7,$7,$8::jsonb,$9) ON CONFLICT DO NOTHING''',
            owner, artifact_id, kind, METHODOLOGY_VERSION, code_version, evidence, data_as_of, canonical(payload), content)
        existing = await conn.fetchrow('''SELECT content_hash,code_version,data_as_of FROM qeef_artifacts
            WHERE owner_chat_id=$1 AND kind=$2 AND artifact_id=$3''', owner, kind, artifact_id)
        if not existing or existing['content_hash'] != content or existing['code_version'] != code_version or existing['data_as_of'] != data_as_of:
            raise ValueError('immutable artifact identity conflict')
    return {'inserted': result == 'INSERT 0 1', 'artifact_id': artifact_id, 'content_hash': content}


async def latest_evaluation(conn, *, owner):
    if not owner:
        raise ValueError('exact owner required')
    async with conn.transaction(readonly=True):
        await conn.execute("SELECT set_config('qeef.owner', $1, true)", str(owner))
        if not await conn.fetchval("SELECT to_regclass('public.qeef_artifacts')"):
            return {'status': 'INSUFFICIENT_DATA', 'reasons': ['QEEF_SCHEMA_NOT_INSTALLED'], 'mode': 'RESEARCH_ONLY'}
        row = await conn.fetchrow('''SELECT payload FROM qeef_artifacts WHERE owner_chat_id=$1 AND kind='EVALUATION'
            ORDER BY data_as_of DESC, artifact_id DESC LIMIT 1''', owner)
        if row is None:
            return {'status': 'INSUFFICIENT_DATA', 'reasons': ['NO_QEEF_EVALUATION_FOR_OWNER'], 'mode': 'RESEARCH_ONLY'}
        return json.loads(row['payload']) if isinstance(row['payload'], str) else row['payload']

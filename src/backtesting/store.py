"""Durable filesystem queue shared by bot and dedicated single worker."""
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import uuid

ACTIVE = {'QUEUED', 'CAPTURING', 'RUNNING', 'EXPORTING'}
TERMINAL = {'COMPLETE', 'PARTIAL', 'NOT_EVALUABLE', 'FAILED', 'CANCELLED', 'INTERRUPTED'}


def root() -> Path:
    path = Path(os.environ.get('BACKTEST_ROOT', '/app/outputs/backtests'))
    path.mkdir(parents=True, exist_ok=True)
    return path


def stamp():
    return datetime.now(timezone.utc).isoformat()


def write(path: Path, value):
    temp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf8')
    os.replace(temp, path)


@contextmanager
def queue_lock():
    import fcntl
    with (root()/'.queue.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def jobs(owner: int | None = None):
    result = []
    for p in root().glob('*/job.json'):
        doc = json.loads(p.read_text())
        if owner is None or doc['owner'] == int(owner):
            result.append(doc)
    return sorted(result, key=lambda j: j['created_at'], reverse=True)


def directory(owner: int, job_id: str) -> Path:
    if not re.fullmatch(r'[0-9a-f]{32}', job_id):
        raise ValueError('ID de corrida inválido.')
    path = root()/job_id
    if not (path/'job.json').exists():
        raise ValueError('Corrida no encontrada.')
    if json.loads((path/'job.json').read_text())['owner'] != int(owner):
        raise ValueError('Corrida no encontrada.')
    return path


def enqueue(owner: int, request_key: str, spec: dict, *, notify=True):
    owner = int(owner)
    if owner <= 0:
        raise ValueError('Propietario inválido.')
    job_id = uuid.uuid5(uuid.NAMESPACE_URL, f'quantia-backtest:{owner}:{request_key}').hex
    with queue_lock():
        path = root()/job_id
        if (path/'job.json').exists():
            return json.loads((path/'job.json').read_text())
        running = [j for j in jobs() if j['status'] in ACTIVE]
        existing = [j for j in running if j['owner'] == owner]
        if existing:
            raise ValueError('Ya tenés una corrida pendiente. Consultá /backtest estado o cancelala antes de iniciar otra.')
        if len(running) >= 5:
            raise ValueError('La cola está llena. Volvé a intentar cuando termine una corrida.')
        path.mkdir()
        job = dict(id=job_id, owner=owner, spec=spec, status='QUEUED', created_at=stamp(), updated_at=stamp(), notify=notify)
        write(path/'job.json', job)
        return job


def update(path: Path, **values):
    job = json.loads((path/'job.json').read_text())
    job.update(values, updated_at=stamp())
    write(path/'job.json', job)
    return job


def cancel(owner: int, job_id: str):
    path = directory(owner, job_id)
    with queue_lock():
        job = json.loads((path/'job.json').read_text())
        if job['status'] in TERMINAL:
            return job
        (path/'cancel').touch()
        if job['status'] == 'QUEUED':
            job = update(path, status='CANCELLED')
        return job

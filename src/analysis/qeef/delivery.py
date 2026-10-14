"""Durable local outbox and owner-scoped PostgreSQL delivery. No schema bootstrap."""
from __future__ import annotations

import json
from pathlib import Path
import sqlite3
from contextlib import contextmanager
from src.decision_lab.models import canonical, digest
from .prospective import CaptureEvent


def same_source(a,b):
    # A repeated observation of identical facts keeps its first availability.
    # Neither a retry nor recovery is allowed to backdate that first capture.
    keys=('owner','kind','source_id','effective_at','source_version','payload_json','parent_id')
    return all(getattr(a,k)==getattr(b,k) for k in keys)


class Outbox:
    def __init__(self,path):
        self.path=Path(path)

    @contextmanager
    def connect(self):
        self.path.parent.mkdir(parents=True,exist_ok=True)
        conn=sqlite3.connect(self.path,timeout=.1)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=FULL')
        conn.execute('''CREATE TABLE IF NOT EXISTS events(owner INTEGER,event_id TEXT,payload TEXT,
            delivered INTEGER NOT NULL DEFAULT 0,duplicates INTEGER NOT NULL DEFAULT 0,
            attempts INTEGER NOT NULL DEFAULT 0,error_code TEXT,
            PRIMARY KEY(owner,event_id))''')
        conn.execute('''CREATE TRIGGER IF NOT EXISTS immutable_payload BEFORE UPDATE OF owner,event_id,payload ON events
            BEGIN SELECT RAISE(ABORT,'immutable evidence'); END''')
        conn.execute('''CREATE TRIGGER IF NOT EXISTS immutable_delete BEFORE DELETE ON events
            BEGIN SELECT RAISE(ABORT,'immutable evidence'); END''')
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    def append(self,event):
        with self.connect() as conn:
            row=conn.execute('SELECT payload FROM events WHERE owner=? AND event_id=?',(event.owner,event.event_id)).fetchone()
            if row:
                if not same_source(CaptureEvent.model_validate_json(row[0]),event):
                    raise ValueError('immutable outbox identity conflict')
                conn.execute('UPDATE events SET duplicates=duplicates+1 WHERE owner=? AND event_id=?',(event.owner,event.event_id))
                return False
            conn.execute('INSERT INTO events(owner,event_id,payload) VALUES(?,?,?)',(event.owner,event.event_id,canonical(event)))
            return True

    def pending(self,owner,limit=100):
        with self.connect() as conn:
            rows=conn.execute('SELECT payload FROM events WHERE owner=? AND delivered=0 ORDER BY rowid LIMIT ?',(owner,limit)).fetchall()
        return [CaptureEvent.model_validate_json(r[0]) for r in rows]

    def receipt(self,event,*,success,code=None):
        with self.connect() as conn:
            conn.execute('UPDATE events SET delivered=?,attempts=attempts+1,error_code=? WHERE owner=? AND event_id=?',
                         (int(success),code,event.owner,event.event_id))

    def status(self,owner):
        if not self.path.exists():
            return {'expected':0,'captured':0,'pending':0,'failed':0,'duplicates':0}
        conn=sqlite3.connect(self.path.resolve().as_uri()+'?mode=ro',uri=True,timeout=.1)
        try:
            row=conn.execute('SELECT count(*),coalesce(sum(delivered),0),coalesce(sum(1-delivered),0),coalesce(sum(error_code IS NOT NULL),0),coalesce(sum(duplicates),0) FROM events WHERE owner=?',(owner,)).fetchone()
        finally:
            conn.close()
        return dict(zip(('expected','captured','pending','failed','duplicates'),row))


async def append_event(conn,event,*,enqueue=True):
    async with conn.transaction():
        await conn.execute("SELECT set_config('qeef.owner',$1,true)",str(event.owner))
        inserted=await conn.fetchval('''INSERT INTO qeef_events(owner_chat_id,event_id,kind,source_id,source_version,effective_at,
            available_at,parent_id,content_hash,payload) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb)
            ON CONFLICT DO NOTHING RETURNING event_id''',event.owner,event.event_id,event.kind,event.source_id,
            event.source_version,event.effective_at,event.available_at,event.parent_id,event.content_hash,canonical(event))
        if not inserted:
            raw=await conn.fetchval('SELECT payload FROM qeef_events WHERE owner_chat_id=$1 AND event_id=$2',event.owner,event.event_id)
            existing=CaptureEvent.model_validate(json.loads(raw) if isinstance(raw,str) else raw)
            if not same_source(existing,event): raise ValueError('immutable database identity conflict')
        if enqueue:
            await conn.execute('''INSERT INTO qeef_jobs(owner_chat_id,event_id) VALUES($1,$2)
                ON CONFLICT(owner_chat_id,event_id) DO UPDATE SET duplicate_deliveries=qeef_jobs.duplicate_deliveries+1''',event.owner,event.event_id)
        return bool(inserted)


async def deliver(conn,outbox,*,owner,limit=100):
    result={'delivered':0,'failed':0}
    for event in outbox.pending(owner,limit):
        try:
            await append_event(conn,event)
        except Exception as exc:
            # Class name only: connection errors may contain credentials/payloads.
            outbox.receipt(event,success=False,code=type(exc).__name__)
            result['failed']+=1
            break
        else:
            outbox.receipt(event,success=True)
            result['delivered']+=1
    return result

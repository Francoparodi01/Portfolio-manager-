"""Local SQLite insert-only evidence, transactional children and a global hash chain."""
from datetime import datetime, timezone
from contextlib import closing
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sqlite3

from .evidence_capture import canonical, content_hash

ENTITIES = ("runs", "attempts", "hypotheses", "evaluations")
ATTEMPT_FIELDS = {"requested_model", "resolved_model", "responding_provider", "parameters", "prompt_version",
                  "input_tokens", "output_tokens", "reasoning_tokens", "calculated_cost_usd",
                  "billed_cost_usd", "billing_status", "status"}


def _finite_cost(value):
    if value is None:
        return
    number = Decimal(str(value))
    if isinstance(value,bool) or not number.is_finite() or number < 0:
        raise ValueError("NONNEGATIVE_FINITE_COST_REQUIRED")


def _attempt_metadata(metadata):
    if set(metadata) != ATTEMPT_FIELDS:
        raise ValueError("EXACT_ATTEMPT_METADATA_REQUIRED")
    for key in ("input_tokens","output_tokens","reasoning_tokens"):
        v = metadata[key]
        if v is not None and (type(v) is not int or v < 0):
            raise ValueError("TOKEN_COUNTS_MUST_BE_UNKNOWN_OR_NONNEGATIVE_INTEGERS")
    for key in ("calculated_cost_usd","billed_cost_usd"):
        _finite_cost(metadata[key])
    if metadata["billing_status"] not in {"BILLED_COST_UNKNOWN","BILLED_COST_REPORTED"}:
        raise ValueError("INVALID_BILLING_STATUS")
    if (metadata["billed_cost_usd"] is None) != (metadata["billing_status"] == "BILLED_COST_UNKNOWN"):
        raise ValueError("BILLING_VALUE_STATUS_MISMATCH")
    if metadata["status"] not in {"PLANNED","RECEIVED","FAILED"}:
        raise ValueError("INVALID_ATTEMPT_STATUS")
    for key in ("requested_model","resolved_model","responding_provider"):
        if metadata[key] is not None and (not isinstance(metadata[key],str) or not metadata[key].strip()):
            raise ValueError("MODEL_PROVIDER_MUST_BE_UNKNOWN_OR_NONEMPTY")
    if not isinstance(metadata["parameters"],dict) or not isinstance(metadata["prompt_version"],str):
        raise ValueError("INVALID_ATTEMPT_CONFIGURATION")


class LocalLedger:
    def __init__(self, path, *, output_root):
        root, target = Path(output_root).resolve(), Path(path).resolve()
        if ("output" not in {part.lower() for part in root.parts} or target.suffix != ".sqlite3"
                or not target.is_relative_to(root) or target == root):
            raise ValueError("LEDGER_MUST_BE_A_LOCAL_OUTPUT_SQLITE_FILE")
        target.parent.mkdir(parents=True,exist_ok=True)
        self.path = target
        with closing(self._connect()) as connection, connection:
            connection.execute("CREATE TABLE IF NOT EXISTS events (seq INTEGER PRIMARY KEY, entity TEXT NOT NULL, entity_id TEXT NOT NULL, payload TEXT NOT NULL, prev_hash TEXT NOT NULL, event_hash TEXT NOT NULL UNIQUE, UNIQUE(entity,entity_id))")
            for entity in ENTITIES:
                connection.execute(f"CREATE TABLE IF NOT EXISTS {entity} (entity_id TEXT PRIMARY KEY, event_seq INTEGER NOT NULL UNIQUE REFERENCES events(seq), payload TEXT NOT NULL)")
            for table in ("events",) + ENTITIES:
                for operation in ("UPDATE", "DELETE"):
                    connection.execute(f"CREATE TRIGGER IF NOT EXISTS deny_{table}_{operation.lower()} BEFORE {operation} ON {table} BEGIN SELECT RAISE(ABORT,'APPEND_ONLY'); END")

    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=10)
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA journal_mode=DELETE")
        return connection

    def _write_event(self, connection, entity, identity, payload):
        if entity not in ENTITIES or not isinstance(identity,str) or not identity:
            raise ValueError("INVALID_ENTITY_IDENTITY")
        prior = connection.execute("SELECT seq,event_hash FROM events ORDER BY seq DESC LIMIT 1").fetchone()
        seq, previous = (prior[0]+1,prior[1]) if prior else (1,"0"*64)
        raw = canonical(payload).decode("utf-8")
        digest = content_hash(dict(seq=seq,entity=entity,entity_id=identity,payload=payload,prev_hash=previous))
        connection.execute("INSERT INTO events VALUES (?,?,?,?,?,?)",(seq,entity,identity,raw,previous,digest))
        connection.execute(f"INSERT INTO {entity} VALUES (?,?,?)",(identity,seq,raw))
        return digest

    def _record(self, entity, identity, payload, children=()):
        # The timestamp is assigned only on first insertion; identical retries reuse the exact entry.
        stable = dict(payload)
        batch_hash = content_hash(dict(payload=stable,children=list(children)))
        connection = None
        try:
            connection = self._connect()
            connection.execute("BEGIN IMMEDIATE")
            if entity == "attempts" and connection.execute("SELECT 1 FROM runs WHERE entity_id=?",(payload["run_id"],)).fetchone() is None:
                raise ValueError("RUN_MUST_BE_RECORDED_FIRST")
            prior = connection.execute(f"SELECT payload FROM {entity} WHERE entity_id=?",(identity,)).fetchone()
            if prior:
                if json.loads(prior[0])["batch_hash"] != batch_hash:
                    connection.rollback()
                    return dict(status="CONFLICT",reason="REQUEST_ID_CONTENT_MISMATCH",recorded=False)
                connection.rollback()
                return dict(status="ALREADY_RECORDED",recorded=True,entity_id=identity)
            enriched = dict(stable,batch_hash=batch_hash,recorded_at=datetime.now(timezone.utc).isoformat())
            event_hash = self._write_event(connection,entity,identity,enriched)
            for child_entity,child_id,child_payload in children:
                self._write_event(connection,child_entity,child_id,dict(child_payload,attempt_id=identity))
            connection.commit()
            return dict(status="RECORDED",recorded=True,entity_id=identity,event_hash=event_hash)
        except (OSError,sqlite3.Error,ValueError,TypeError,RuntimeError):
            if connection is not None:
                connection.rollback()
            return dict(status="UNRECORDED",recorded=False,reason="ATOMIC_LOCAL_WRITE_FAILED")
        finally:
            if connection is not None:
                connection.close()

    def record_run(self, run_id, payload):
        return self._record("runs",run_id,payload)

    def record_attempt(self, *, run_id, request, response, metadata, hypotheses=(), evaluations=()):
        _attempt_metadata(metadata)
        if not isinstance(request,dict) or not isinstance(request.get("request_id"),str):
            raise ValueError("COMPLETE_REQUEST_WITH_ID_REQUIRED")
        if response is not None and not isinstance(response,str):
            raise ValueError("EXACT_RESPONSE_TEXT_OR_NONE_REQUIRED")
        if metadata["status"] == "RECEIVED" and response is None:
            raise ValueError("RECEIVED_ATTEMPT_REQUIRES_RESPONSE_TEXT")
        for key in ("requested_model","parameters","prompt_version"):
            if key in request and request[key] != metadata[key]:
                raise ValueError("REQUEST_METADATA_MISMATCH")
        if metadata["status"] == "PLANNED" and (response is not None or hypotheses or evaluations or
                any(metadata[key] is not None for key in ("resolved_model","responding_provider","input_tokens",
                                                         "output_tokens","reasoning_tokens","calculated_cost_usd","billed_cost_usd"))):
            raise ValueError("PLANNED_ATTEMPT_CANNOT_INVENT_OBSERVATIONS")
        identity = request["request_id"]
        children = [(entity,f"{identity}:{entity}:{i}",value)
                    for entity,items in (("hypotheses",hypotheses),("evaluations",evaluations)) for i,value in enumerate(items)]
        payload = dict(run_id=run_id,request=request,request_hash=content_hash(request),response=response,
                       response_hash=None if response is None else hashlib.sha256(response.encode("utf-8")).hexdigest(),
                       metadata=metadata)
        return self._record("attempts",identity,payload,children)

    def verify(self):
        with closing(self._connect()) as connection, connection:
            previous = "0"*64
            counts = {entity:0 for entity in ENTITIES}
            rows = connection.execute("SELECT seq,entity,entity_id,payload,prev_hash,event_hash FROM events ORDER BY seq").fetchall()
            for expected,(seq,entity,identity,raw,prior,digest) in enumerate(rows,1):
                payload = json.loads(raw)
                calculated = content_hash(dict(seq=seq,entity=entity,entity_id=identity,payload=payload,prev_hash=prior))
                if seq != expected or prior != previous or calculated != digest or entity not in ENTITIES:
                    raise ValueError("LEDGER_CHAIN_CORRUPT")
                stored = connection.execute(f"SELECT event_seq,payload FROM {entity} WHERE entity_id=?",(identity,)).fetchone()
                if stored != (seq,raw):
                    raise ValueError("LEDGER_ENTITY_CORRUPT")
                if entity == "attempts":
                    if content_hash(payload["request"]) != payload["request_hash"]:
                        raise ValueError("LEDGER_REQUEST_HASH_CORRUPT")
                    response = payload["response"]
                    if (None if response is None else hashlib.sha256(response.encode("utf-8")).hexdigest()) != payload["response_hash"]:
                        raise ValueError("LEDGER_RESPONSE_HASH_CORRUPT")
                counts[entity] += 1
                previous = digest
            if any(connection.execute(f"SELECT COUNT(*) FROM {entity}").fetchone()[0] != count for entity,count in counts.items()):
                raise ValueError("LEDGER_ORPHAN_ENTITY")
            return dict(status="VERIFIED",counts=counts,event_count=len(rows),head_hash=previous)

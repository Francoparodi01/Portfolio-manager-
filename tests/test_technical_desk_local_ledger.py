from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import hashlib
import json
import sqlite3

import pytest

from src.technical_desk.local_ledger import LocalLedger
from src.technical_desk.evidence_capture import content_hash


def metadata(status="PLANNED"):
    return dict(requested_model=None,resolved_model=None,responding_provider=None,parameters={"temperature":0},
        prompt_version="test-v1",input_tokens=None,output_tokens=None,reasoning_tokens=None,
        calculated_cost_usd=None,billed_cost_usd=None,billing_status="BILLED_COST_UNKNOWN",status=status)


@pytest.fixture
def ledger(tmp_path):
    instance=LocalLedger(tmp_path/"output/ledger.sqlite3",output_root=tmp_path/"output")
    assert instance.record_run("run-1",dict(mode="DRY_RUN",status="NOT_STARTED"))["status"]=="RECORDED"
    return instance


def record(ledger, **changes):
    args=dict(run_id="run-1",request=dict(request_id="req-1",messages=[{"content":"synthetic market data"}],parameters={"temperature":0}),
              response=None,metadata=metadata())
    args.update(changes)
    return ledger.record_attempt(**args)


def test_full_request_exact_response_hash_and_linked_entities(ledger):
    raw=' {"text": "á", "data": [1, 2]} \n'
    result=record(ledger,response=raw,metadata=metadata("RECEIVED"),hypotheses=[{"state":"ABSTAIN"}],evaluations=[{"status":"VALID"}])
    assert result["recorded"]
    verified=ledger.verify()
    assert verified["counts"]==dict(runs=1,attempts=1,hypotheses=1,evaluations=1)
    with sqlite3.connect(ledger.path) as connection:
        row=json.loads(connection.execute("SELECT payload FROM attempts").fetchone()[0])
    assert row["request_hash"]==content_hash(row["request"])
    assert row["response"]==raw and row["response_hash"]==hashlib.sha256(raw.encode()).hexdigest()
    assert row["metadata"]["billing_status"]=="BILLED_COST_UNKNOWN"


@pytest.mark.parametrize("table", ["events","runs","attempts","hypotheses","evaluations"])
@pytest.mark.parametrize("operation", ["UPDATE","DELETE"])
def test_sql_update_delete_are_rejected_for_every_entity(ledger,table,operation):
    record(ledger,response="{}",metadata=metadata("RECEIVED"),hypotheses=[{}],evaluations=[{}])
    sql=f"UPDATE {table} SET payload='{{}}'" if operation=="UPDATE" else f"DELETE FROM {table}"
    with sqlite3.connect(ledger.path) as connection, pytest.raises(sqlite3.IntegrityError,match="APPEND_ONLY"):
        connection.execute(sql)
    assert ledger.verify()["event_count"]==4


def test_idempotency_conflict_and_concurrent_duplicate_do_not_append(ledger):
    with ThreadPoolExecutor(max_workers=4) as pool:
        results=list(pool.map(lambda _:record(ledger),range(4)))
    assert sum(r["status"]=="RECORDED" for r in results)==1
    assert sum(r["status"]=="ALREADY_RECORDED" for r in results)==3
    changed_metadata = metadata()
    changed_metadata["parameters"] = {"temperature": 1}
    assert record(ledger,request=dict(request_id="req-1",parameters={"temperature":1}),
                  metadata=changed_metadata)["status"]=="CONFLICT"
    assert ledger.verify()["counts"]["attempts"]==1


def test_atomic_failure_rolls_back_attempt_hypothesis_and_chain(ledger,monkeypatch):
    original=ledger._write_event
    def fail(connection,entity,identity,payload):
        result=original(connection,entity,identity,payload)
        if entity=="evaluations":
            raise RuntimeError("synthetic storage failure")
        return result
    monkeypatch.setattr(ledger,"_write_event",fail)
    result=record(ledger,response="{}",metadata=metadata("RECEIVED"),hypotheses=[{}],evaluations=[{}])
    assert result["status"]=="UNRECORDED" and not result["recorded"]
    assert ledger.verify()["counts"]==dict(runs=1,attempts=0,hypotheses=0,evaluations=0)
    monkeypatch.setattr(ledger,"_write_event",original)
    assert record(ledger)["status"]=="RECORDED"


def test_detects_chain_tampering_after_administrator_removes_trigger(ledger):
    record(ledger)
    with sqlite3.connect(ledger.path) as connection:
        connection.execute("DROP TRIGGER deny_events_update")
        connection.execute("UPDATE events SET prev_hash=? WHERE seq=2",("f"*64,))
    with pytest.raises(ValueError,match="CHAIN_CORRUPT"):
        ledger.verify()


def test_no_unregistered_run_or_outside_output_path(ledger,tmp_path):
    assert record(ledger,run_id="missing")["status"]=="UNRECORDED"
    with pytest.raises(ValueError,match="LOCAL_OUTPUT"):
        LocalLedger(tmp_path/"production.sqlite3",output_root=tmp_path/"output")


@pytest.mark.parametrize("patch", [{"input_tokens":True},{"output_tokens":-1},{"calculated_cost_usd":"NaN"},
    {"billed_cost_usd":"1"},{"billing_status":"PAID"}])
def test_invalid_usage_or_cost_never_enters_ledger(ledger,patch):
    values=metadata("RECEIVED")
    values.update(patch)
    with pytest.raises(ValueError):
        record(ledger,response="{}",metadata=values)
    assert ledger.verify()["counts"]["attempts"]==0


def test_planned_rows_do_not_fabricate_response_or_usage(ledger):
    with pytest.raises(ValueError,match="CANNOT_INVENT"):
        record(ledger,response="{}")
    values=metadata()
    values["input_tokens"]=10
    with pytest.raises(ValueError,match="CANNOT_INVENT"):
        record(ledger,metadata=values)


def test_attempt_metadata_matches_full_request_and_survives_reopen(ledger):
    values=metadata()
    values["parameters"]={"temperature":1}
    with pytest.raises(ValueError,match="REQUEST_METADATA_MISMATCH"):
        record(ledger,metadata=values)
    with pytest.raises(ValueError,match="REQUIRES_RESPONSE"):
        record(ledger,metadata=metadata("RECEIVED"))
    assert record(ledger)["recorded"]
    reopened=LocalLedger(ledger.path,output_root=ledger.path.parent)
    assert reopened.verify()==ledger.verify()

import importlib.util
import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.technical_desk.dry_dispatch import abstention_fixture
from src.technical_desk.local_ledger import LocalLedger
from tests.test_technical_desk_compact_prompts import full_fixture

SPEC = importlib.util.spec_from_file_location("desk_free_smoke_runner", Path(__file__).parents[1] / "scripts/run_technical_desk_free_smoke.py")
smoke = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(smoke)


class MemoryLedger:
    def __init__(self, fail_intent=False):
        self.intents, self.attempts = [], []
        self.fail_intent = fail_intent

    def record_run(self, *args):
        return {"status": "RECORDED"}

    def _record(self, entity, identity, payload):
        self.intents.append(payload)
        return {"status": "UNRECORDED" if self.fail_intent else "RECORDED"}

    def record_attempt(self, **kwargs):
        self.attempts.append(kwargs)
        return {"status": "RECORDED"}

    def verify(self):
        return {"status": "VERIFIED"}


@pytest.fixture
def inputs(tmp_path, monkeypatch):
    import socket
    def blocked(*args, **kwargs):
        raise AssertionError("NETWORK_FORBIDDEN_IN_RUNNER_TESTS")
    monkeypatch.setattr(socket.socket, "connect", blocked)
    monkeypatch.setattr(socket, "getaddrinfo", blocked)
    manifest = json.loads((smoke.PHASE / "model-manifest.json").read_bytes())
    view, context = full_fixture()
    assert view["instrument_id"] == "BYMA:CEDEAR:AMD:ARS"
    directory = tmp_path / "output"
    directory.mkdir()
    return manifest, view, context, directory, {"verified_at_utc": datetime.now(timezone.utc).isoformat()}


def answer(body, view, **overrides):
    role = json.loads(body["messages"][1]["content"])["role"]
    content = json.dumps(abstention_fixture(role, view))
    payload = dict(model=body["model"], provider="Nvidia", choices=[dict(finish_reason="stop", message={"content": content})])
    result = dict(status="RECEIVED", abort=False, retry_eligible=False, http_status=200,
                  raw_response=json.dumps(payload).encode(), raw_generation=None, response_json=payload,
                  selected_headers={}, finish_reason="stop", latency_ms=2,
                  identity_usage=dict(resolved_model=body["model"], responding_provider="Nvidia", resolved_endpoint=None,
                    tokens=dict(input=200, output=20, reasoning=None, cache_read=None, cache_write=None), usage_cost=0, cost_details=None))
    result.update(overrides)
    return result


def call(inputs, ledger, send, sleeps=None):
    manifest, view, context, directory, fresh = inputs
    return smoke.execute(manifest, view, context, directory, ledger, send, fresh,
                         sleep=(sleeps if sleeps is not None else []).append, progress=lambda *a, **k: None)


def test_exact_five_roles_intent_before_send_and_atomic_local_ledger(inputs):
    manifest, view, _, directory, _ = inputs
    ledger = LocalLedger(directory / "fixture.sqlite3", output_root=directory)
    bodies = []
    def send(body):
        # Every POST is preceded by a durable intent; completed attempts are fewer.
        counts = ledger.verify()["counts"]
        assert counts["evaluations"] == 2 * len(bodies) + 1
        bodies.append(deepcopy(body))
        return answer(body, view)
    result = call(inputs, ledger, send)
    assert result["model_requests"] == result["logical_requests_attempted"] == 5
    assert result["ledger"]["counts"]["attempts"] == 5
    assert all(row["status"] == "ABSTAIN" for row in result["results"])
    assert len(list(directory.glob("*.response.raw.json"))) == 5
    moderator = json.loads(bodies[-1]["messages"][1]["content"])
    assert "ohlcv" not in moderator and "features" not in moderator
    assert all(body["provider"]["max_price"] == {"prompt": 0, "completion": 0, "request": 0} for body in bodies)


def test_failed_preregistration_prevents_any_send(inputs):
    with pytest.raises(RuntimeError, match="UNRECORDED"):
        call(inputs, MemoryLedger(fail_intent=True), lambda body: pytest.fail("sent without intent"))


def test_one_identical_retry_for_invalid_json_no_prompt_changes(inputs):
    view = inputs[1]
    bodies, waits = [], []
    def send(body):
        bodies.append(deepcopy(body))
        if len(bodies) == 1:
            bad = answer(body, view)
            bad["response_json"]["choices"][0]["message"]["content"] = "{"
            bad["raw_response"] = json.dumps(bad["response_json"]).encode()
            return bad
        return answer(body, view)
    result = call(inputs, MemoryLedger(), send, waits)
    assert len(bodies) == 6 and bodies[0] == bodies[1]
    assert waits == [5]
    assert result["results"][0]["attempts"] == 2


def test_429_honors_header_and_stops_after_single_retry(inputs):
    waits, calls = [], []
    def send(body):
        calls.append(deepcopy(body))
        return dict(status="HTTP_ERROR", abort=False, retry_eligible=True, http_status=429,
                    raw_response=b'{"error":{"code":429}}', selected_headers={"retry-after": "2"}, error_code="HTTP_STATUS_ERROR")
    result = call(inputs, MemoryLedger(), send, waits)
    assert calls[0] == calls[1] and len(calls) == 2 and waits == [2]
    assert result["stop_reason"] == "RATE_LIMIT_AFTER_SINGLE_RETRY"
    assert all(r["attempts"] == 0 for r in result["results"][1:])


def test_long_rate_limit_defers_without_another_request(inputs):
    result = call(inputs, MemoryLedger(), lambda body: dict(status="HTTP_ERROR", abort=False, retry_eligible=True,
        http_status=429, raw_response=b'{}', selected_headers={"retry-after": "3600"}))
    assert result["model_requests"] == 1 and result["stop_reason"] == "RETRY_DEFERRED_BY_LIMIT_HEADERS"


def test_semantic_invalid_output_does_not_retry_or_invent_moderator_inputs(inputs):
    calls = []
    def send(body):
        calls.append(body)
        bad = answer(body, inputs[1])
        bad["response_json"]["choices"][0]["message"]["content"] = "{}"
        bad["raw_response"] = json.dumps(bad["response_json"]).encode()
        return bad
    result = call(inputs, MemoryLedger(), send)
    assert len(calls) == 4 and result["results"][-1]["status"] == "NOT_STARTED"
    assert all(row["attempts"] == 1 for row in result["results"][:-1])
    assert result["status"] == "COMPLETED_WITH_FAILURES"


def test_nonfree_response_aborts_remaining_roles(inputs):
    result = call(inputs, MemoryLedger(), lambda body: dict(status="ABORTED", abort=True, retry_eligible=False,
        http_status=200, raw_response=b'{"model":"paid"}', error_code="RESOLVED_FREE_MODEL_MISMATCH"))
    assert result["model_requests"] == 1 and result["stop_reason"] == "RESOLVED_FREE_MODEL_MISMATCH"


def test_paid_hypothetical_is_unknown_without_usage_and_does_not_zero_missing_cache(inputs):
    manifest = inputs[0]
    assert smoke.hypothetical_cost({}, manifest) is None
    assert smoke.hypothetical_cost({"tokens": {"input": 100, "output": 10, "cache_read": None}}, manifest) == "0.00001970"
    assert smoke.hypothetical_cost({"tokens": {"input": 100, "output": 10, "cache_write": 1}}, manifest) is None


def test_key_loader_reads_only_exact_variable_without_mutating_environment(tmp_path, monkeypatch):
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    monkeypatch.setenv("openrouter", "not-an-authorized-alias")
    env = tmp_path / ".env"
    env.write_text('UNRELATED_SECRET=do-not-load\nOPENROUTER_API_KEY="synthetic-key"\n', encoding="utf-8")
    assert smoke.key_from_environment(env) == "synthetic-key"
    assert "UNRELATED_SECRET" not in smoke.os.environ
    assert "OPENROUTER_API_KEY" not in smoke.os.environ


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-1", "invalid"])
def test_invalid_rate_headers_block_retry(value, inputs):
    assert smoke.retry_wait_seconds({"selected_headers": {"retry-after": value}, "http_status": 429},
        inputs[0]["retry_policy"], datetime.now(timezone.utc)) is None


def test_non_utf8_response_is_reversible_and_hashable_without_replacement():
    raw = b'\xff\xfe\x00{broken-json'
    text, encoding = smoke.ledger_response(raw)
    assert encoding == "BASE64_HTTP_BYTES"
    assert smoke.base64.b64decode(text) == raw


def test_relative_runpy_filename_is_resolved_before_recording_intent(inputs, monkeypatch):
    monkeypatch.chdir(smoke.ROOT)
    monkeypatch.setattr(smoke, "__file__", "scripts/run_technical_desk_free_smoke.py")
    result = call(inputs, MemoryLedger(), lambda body: answer(body, inputs[1]))
    assert result["status"] == "COMPLETED" and result["model_requests"] == 5

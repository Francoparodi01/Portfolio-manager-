from copy import deepcopy
from datetime import datetime, timezone
import json
import socket

import pytest

from scripts import diagnose_technical_desk_reasoning as diagnostic
from tests.test_technical_desk_compact_prompts import full_fixture
from tests.test_technical_desk_free_smoke import MemoryLedger


@pytest.fixture
def inputs(tmp_path, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("NETWORK_FORBIDDEN")
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)
    manifest = json.loads((diagnostic.smoke.PHASE / "model-manifest.json").read_bytes())
    view, context = full_fixture()
    return diagnostic.make_plan(manifest, view, context), view, context, tmp_path


def response(body, content, *, reasoning=0, output=180, cost=0):
    finish = "length" if content is None else "stop"
    payload = dict(model=body["model"], provider="Nvidia", choices=[dict(
        finish_reason=finish, native_finish_reason=finish, message=dict(content=content))])
    return dict(status="RECEIVED", abort=False, http_status=200, response_json=payload,
                raw_response=json.dumps(payload).encode(), finish_reason=finish, latency_ms=1,
                identity_usage=dict(resolved_model=body["model"], responding_provider="Nvidia",
                                    tokens=dict(input=100, output=output, reasoning=reasoning,
                                                cache_read=None, cache_write=None), usage_cost=cost))


def run(inputs, send, ledger=None, preflight=None):
    plan, view, context, directory = inputs
    return diagnostic.execute(plan, view, context, directory, ledger or MemoryLedger(), send,
        preflight or (lambda *args: dict(verified_at_utc=datetime.now(timezone.utc).isoformat())),
        progress=lambda *args, **kwargs: None)


def test_only_reasoning_changes_between_controls_and_original_elliott(inputs):
    plan, view, context, _ = inputs
    baseline, disabled, elliott = plan["requests"]
    assert {**baseline["body"], "reasoning": {"enabled": False}} == disabled["body"]
    original = diagnostic.smoke.build_free_request(
        diagnostic.smoke.render_compact_prompt("elliott", view, context=context, replica_id=1),
        manifest=baseline["manifest"], role="elliott", max_output_tokens=4096)
    assert {**original, "reasoning": {"enabled": False}} == elliott["body"]
    assert elliott["body"]["response_format"] == baseline["body"]["response_format"]


def test_three_calls_only_after_minimum_pass_and_intents_recorded(inputs):
    plan, _, _, _ = inputs
    ledger = MemoryLedger()
    calls = []
    def send(body, manifest):
        assert len(ledger.intents) == len(calls) + 1
        calls.append(body)
        if len(calls) == 1:
            return response(body, None, reasoning=512, output=512)
        return response(body, json.dumps(plan["minimum_expected"]))
    result = run(inputs, send, ledger)
    assert result["model_posts"] == 3
    assert result["attempts"][0]["evaluation"]["exhausted_in_reasoning"] is True
    assert result["attempts"][1]["evaluation"]["elliott_gate_passed"] is True
    assert result["moderator_started"] is False and result["pilot_started"] is False
    assert [len(a["hypotheses"]) for a in ledger.attempts] == [0, 0, 1]


@pytest.mark.parametrize("reasoning,content_ok", [(1, True), (None, True), (0, False)])
def test_elliott_blocked_if_disabled_control_not_verified(inputs, reasoning, content_ok):
    plan = inputs[0]
    def send(body, manifest):
        return response(body, json.dumps(plan["minimum_expected"]) if content_ok else "{}", reasoning=reasoning)
    result = run(inputs, send)
    assert result["model_posts"] == 2
    assert result["stop_reason"] == "MINIMUM_DISABLED_DID_NOT_PASS"


def test_unrecorded_intent_prevents_send(inputs):
    with pytest.raises(RuntimeError, match="UNRECORDED"):
        run(inputs, lambda *args: pytest.fail("must not send"), MemoryLedger(fail_intent=True))


def test_expired_preflight_prevents_send(inputs):
    with pytest.raises(ValueError, match="FRESH_CATALOG_REQUIRED"):
        run(inputs, lambda *args: pytest.fail("must not send"),
            preflight=lambda *args: dict(verified_at_utc="2000-01-01T00:00:00+00:00"))


def test_unknown_cost_stops_without_more_requests(inputs):
    result = run(inputs, lambda body, manifest: response(body, None, reasoning=512, output=512, cost=None))
    assert result["model_posts"] == 1 and result["stop_reason"] == "COST_UNKNOWN"
    assert result["attempts"][0]["usage_cost"] is None


def test_scope_and_body_tampering_block_send(inputs):
    changed = deepcopy(inputs[0])
    changed["requests"][2]["case"] = "moderator"
    with pytest.raises(ValueError, match="EXACT_DIAGNOSTIC_SCOPE_REQUIRED"):
        run((changed, *inputs[1:]), lambda *args: pytest.fail("must not send"))
    changed = deepcopy(inputs[0])
    changed["requests"][0]["body"]["max_tokens"] += 1
    with pytest.raises(ValueError, match="FROZEN_REQUEST_CHANGED"):
        run((changed, *inputs[1:]), lambda *args: pytest.fail("must not send"))


def test_rate_limit_stops_without_retry(inputs):
    def send(body, manifest):
        result = response(body, None, reasoning=None, cost=None)
        result.update(status="HTTP_ERROR", http_status=429, retry_eligible=True,
                      selected_headers={"retry-after": "60"}, identity_usage=None)
        return result
    result = run(inputs, send)
    assert result["model_posts"] == 1
    assert result["attempts"][0]["selected_headers"] == {"retry-after": "60"}


def test_elliott_prompt_binds_existing_contract_metadata(inputs):
    plan = inputs[0]
    request = plan["requests"][2]
    assert request["prompt_version"] == "technical-desk-prompt-elliott-v2.3"
    assert "evaluation_cohort=OTHER_ROLES and volume_basis=NOT_USED" in request["body"]["messages"][0]["content"]
    instructions = request["body"]["messages"][0]["content"]
    assert "Copy packet_id and packet_digest verbatim" in instructions
    assert "exactly one PRIMARY" in instructions and "role=origin" in instructions
    assert "requires ABSTAIN" in instructions and "RULES_COMPATIBLE" in instructions
    expected = plan["minimum_expected"]
    assert expected["evaluation_cohort"] == "OTHER_ROLES" and expected["volume_basis"] == "NOT_USED"


def test_single_elliott_followup_requires_verified_dependency(inputs):
    plan = deepcopy(inputs[0])
    plan.update(requests=plan["requests"][2:], maximum_posts=1)
    with pytest.raises(ValueError, match="EXACT_DIAGNOSTIC_SCOPE_REQUIRED"):
        run((plan, *inputs[1:]), lambda *args: pytest.fail("must not send"))
    plan["verified_minimum_dependency"] = {"path": "fixture-only", "sha256": "0" * 64}
    result = run((plan, *inputs[1:]), lambda body, manifest: response(body, json.dumps(plan["minimum_expected"])))
    assert result["model_posts"] == 1 and not result["moderator_started"]

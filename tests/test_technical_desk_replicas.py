"""Hermetic tests for the preregistered replica experiment: no network, no output/ files, no real key."""
from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone
import json
import socket
import subprocess

import pytest

from scripts import run_technical_desk_replicas as rep
from scripts.technical_desk_replica_control import amd_compatible_dates, build_control, identifies_expected_count
from src.technical_desk.dry_dispatch import abstention_fixture
from src.technical_desk.free_smoke_transport import FREE_SMOKE_MODEL


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def blocked(*args, **kwargs):
        raise AssertionError("NETWORK_FORBIDDEN_IN_REPLICA_TESTS")
    monkeypatch.setattr(socket.socket, "connect", blocked)
    monkeypatch.setattr(socket, "getaddrinfo", blocked)


@pytest.fixture(scope="module")
def control():
    return build_control()


@pytest.fixture
def manifest():
    return dict(model_id=FREE_SMOKE_MODEL, endpoint="nvidia", provider_name="Nvidia",
                allowed_resolved_models=[FREE_SMOKE_MODEL], pricing={"prompt": "0", "completion": "0"},
                reasoning=None, context_length=1_000_000, max_prompt_tokens=900_000,
                max_completion_tokens=131_072,
                hypothetical_paid_pricing={"pricing": {"prompt": "0.00000015", "completion": "0.00000047"}})


def plan_for(manifest, control, reasoning=None):
    view, context, expected = control
    return rep.prepare_plan(manifest, {"AMD": view, "CONTROL": view}, {"AMD": context, "CONTROL": context},
                            expected, {"arm_b_reasoning": reasoning or {"enabled": True}})


# --- Phase 1 commit gate -------------------------------------------------------------------------

def git(root, *args):
    subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t", *args], cwd=root, check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def test_phase1_commit_may_be_an_ancestor_of_head(tmp_path):
    git(tmp_path, "init", "-q")
    (tmp_path / "a.txt").write_text("1")
    git(tmp_path, "add", "."), git(tmp_path, "commit", "-q", "-m", "phase1")
    phase1 = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=tmp_path, text=True).strip()
    assert rep.phase1_commit_in_history(tmp_path, phase1)
    (tmp_path / "a.txt").write_text("2")
    git(tmp_path, "commit", "-q", "-am", "later work")
    assert rep.phase1_commit_in_history(tmp_path, phase1)


def test_unknown_or_unrelated_commit_is_rejected(tmp_path):
    git(tmp_path, "init", "-q")
    (tmp_path / "a.txt").write_text("1")
    git(tmp_path, "add", "."), git(tmp_path, "commit", "-q", "-m", "only")
    assert not rep.phase1_commit_in_history(tmp_path, "0" * 40)
    assert not rep.phase1_commit_in_history(tmp_path / "missing", "0" * 40)


# --- synthetic control ---------------------------------------------------------------------------

def correct_output(expected, scale="P2"):
    ids = expected["daily"][scale]["pivot_ids"]
    scenario = dict(role="PRIMARY", family="IMPULSE", direction="BULLISH",
                    causal_anchors=[dict(role="origin", anchor_id=ids[0])],
                    structure_anchors=[dict(anchor_id=i) for i in ids[1:]])
    return dict(results=[dict(timeframe="D", state="VALID", scenarios=[scenario]),
                         dict(timeframe="W", state="ABSTAIN", scenarios=[])])


def test_control_geometry_is_one_bullish_impulse_on_both_scales(control):
    _, _, expected = control
    assert set(expected["daily"]) == {"P2", "P5"}
    assert all(len(v["pivot_ids"]) == 6 for v in expected["daily"].values())
    assert expected["market_hypothesis_allowed"] is False and expected["evidence_class"] == "SYNTHETIC_CONTROL_ONLY"


@pytest.mark.parametrize("scale", ["P2", "P5"])
def test_expected_count_is_recognised_on_either_scale(control, scale):
    assert identifies_expected_count(correct_output(control[2], scale), control[2]) is True


def mutate(parsed, fn):
    parsed = deepcopy(parsed)
    fn(parsed["results"][0]["scenarios"][0])
    return parsed


@pytest.mark.parametrize("name,change", [
    ("bearish", lambda s: s.update(direction="BEARISH")),
    ("correction", lambda s: s.update(family="CORRECTION")),
    ("alternative", lambda s: s.update(role="ALTERNATIVE")),
    ("no_origin", lambda s: s.update(causal_anchors=[])),
    ("extra_structure_anchor", lambda s: s["structure_anchors"].append(dict(anchor_id="P2:D:extra"))),
    ("missing_structure_anchor", lambda s: s["structure_anchors"].pop()),
    ("wrong_origin", lambda s: s["causal_anchors"][0].update(anchor_id="P2:D:other")),
])
def test_near_misses_do_not_count_as_the_expected_count(control, name, change):
    assert identifies_expected_count(mutate(correct_output(control[2]), change), control[2]) is False


def test_abstention_or_empty_output_does_not_identify_the_count(control):
    _, _, expected = control
    assert identifies_expected_count(None, expected) is False
    abstain = dict(results=[dict(timeframe="D", state="ABSTAIN", scenarios=[]), dict(timeframe="W", state="ABSTAIN", scenarios=[])])
    assert identifies_expected_count(abstain, expected) is False


def test_a_confirmed_window_can_never_end_on_the_last_bar_so_recency_is_reported_in_sessions(control):
    view = control[0]
    windows = amd_compatible_dates(view)
    assert windows and not any(w["ends_on_last_daily_bar"] for w in windows)
    # The control's own expected impulse ends 14 sessions before the last bar: there is no recency rule.
    assert {w["sessions_after_end"] for w in windows} == {14}


# --- preregistered plan --------------------------------------------------------------------------

def test_plan_is_16_balanced_cases_and_arms_differ_only_in_reasoning_and_cap(manifest, control):
    plan = plan_for(manifest, control)
    cases = plan["cases"]
    assert len(cases) == 16 and plan["maximum_posts"] == 16 and plan["automatic_retries"] == 0
    assert plan["stop_on_429"] is True and plan["moderator"] is False and plan["pilot"] is False
    assert Counter((c["dataset"], c["arm"]) for c in cases) == Counter({("AMD", "A"): 5, ("AMD", "B"): 5, ("CONTROL", "A"): 3, ("CONTROL", "B"): 3})
    assert len({c["case_id"] for c in cases}) == 16
    for case in cases:
        reasoning, cap = (({"enabled": False}, 4096) if case["arm"] == "A" else ({"enabled": True}, 24576))
        assert case["body"]["reasoning"] == reasoning and case["body"]["max_tokens"] == cap
        assert case["body"]["temperature"] == 0
        assert case["body"]["provider"]["max_price"] == {"prompt": 0, "completion": 0, "request": 0}
        assert case["body"]["provider"]["allow_fallbacks"] is False
    orders = [tuple(c["arm"] for c in cases[i:i + 2]) for i in range(0, 16, 2)]
    assert orders == [("A", "B"), ("B", "A")] * 4


def test_plan_states_the_limits_of_what_the_experiment_can_conclude(manifest, control):
    definition = plan_for(manifest, control)["metrics_definition"]
    assert "temperature 0".replace(" ", "_").upper() in definition["replica_independence"].upper().replace(" ", "_")
    assert definition["amd_expected_answer"].startswith("NONE_PREREGISTERED")


@pytest.mark.parametrize("reasoning", [{"enabled": False}, {"effort": "low"}, {"enabled": True, "effort": "high"},
                                       {"enabled": True, "exclude": True}, {}, None])
def test_arm_b_accepts_only_the_two_preregistered_reasoning_values(manifest, control, reasoning):
    view, context, expected = control
    with pytest.raises(ValueError, match="EXACT_ARM_B_REQUIRED"):
        rep.prepare_plan(manifest, {"AMD": view, "CONTROL": view}, {"AMD": context, "CONTROL": context}, expected,
                         {"arm_b_reasoning": reasoning})


def test_arm_b_low_effort_is_accepted_when_explicitly_chosen(manifest, control):
    plan = plan_for(manifest, control, {"enabled": True, "effort": "low"})
    assert next(c for c in plan["cases"] if c["arm"] == "B")["body"]["reasoning"] == {"enabled": True, "effort": "low"}


def test_context_or_output_limits_block_the_plan(manifest, control):
    manifest["max_completion_tokens"] = 8192
    with pytest.raises(ValueError, match="MODEL_CONTEXT_OR_OUTPUT_LIMIT"):
        plan_for(manifest, control)


# --- execution -----------------------------------------------------------------------------------

class MemoryLedger:
    def __init__(self):
        self.intents, self.attempts = [], []

    def record_run(self, *args):
        return {"status": "RECORDED"}

    def _record(self, entity, identity, payload):
        self.intents.append(identity)
        return {"status": "RECORDED"}

    def record_attempt(self, **kwargs):
        self.attempts.append(kwargs)
        return {"status": "RECORDED"}

    def verify(self):
        return {"status": "VERIFIED"}


def answer(body, view, **overrides):
    payload = dict(model=body["model"], provider="Nvidia", choices=[dict(finish_reason="stop",
                   message=dict(content=json.dumps(abstention_fixture("elliott", view))))])
    result = dict(status="RECEIVED", abort=False, http_status=200, raw_response=json.dumps(payload).encode(),
                  raw_generation=None, response_json=payload, selected_headers={}, finish_reason="stop", latency_ms=2,
                  identity_usage=dict(resolved_model=body["model"], responding_provider="Nvidia", resolved_endpoint=None,
                                      tokens=dict(input=200, output=20, reasoning=0), usage_cost=0, cost_details=None))
    result.update(overrides)
    return result


def run(plan, control, tmp_path, send, ledger=None):
    view, context, _ = control
    ledger = ledger or MemoryLedger()
    fresh = lambda variant, target: {"verified_at_utc": datetime.now(timezone.utc).isoformat()}
    return rep.execute(plan, {"AMD": context, "CONTROL": context}, tmp_path, ledger, send, fresh,
                       progress=lambda *a, **k: None), ledger


def test_a_429_stops_the_run_after_recording_it_and_never_retries(manifest, control, tmp_path):
    plan, sent = plan_for(manifest, control), []
    ledger = MemoryLedger()

    def send(body, variant):
        assert len(ledger.intents) == len(sent) + 1, "intent must be persisted before every send"
        sent.append(deepcopy(body))
        if len(sent) == 2:
            return dict(status="HTTP_ERROR", abort=False, http_status=429, raw_response=b'{"error":"rate limited"}',
                        raw_generation=None, response_json=None, selected_headers={"retry-after": "5"},
                        finish_reason=None, latency_ms=1, identity_usage={}, error_code="HTTP_429")
        return answer(body, control[0])

    result, _ = run(plan, control, tmp_path, send, ledger)
    assert len(sent) == 2 and result["status"] == "STOPPED" and result["stop_reason"] == "RATE_LIMIT_429_NO_RETRY"
    assert result["model_posts"] == 2 and len(ledger.attempts) == 2
    assert result["moderator_started"] is False and result["pilot_started"] is False


def test_a_full_run_sends_exactly_sixteen_requests_in_preregistered_order(manifest, control, tmp_path):
    plan, order = plan_for(manifest, control), []

    def send(body, variant):
        order.append(("A" if body["reasoning"] == {"enabled": False} else "B", body["max_tokens"]))
        return answer(body, control[0])

    result, _ = run(plan, control, tmp_path, send)
    assert result["status"] == "COMPLETED" and result["model_posts"] == 16
    assert order == [(c["arm"], c["body"]["max_tokens"]) for c in plan["cases"]]
    by_group = {(g["dataset"], g["arm"]): g for g in result["metrics"]}
    assert by_group[("AMD", "A")]["attempted"] == 5 and by_group[("CONTROL", "B")]["attempted"] == 3
    assert by_group[("AMD", "B")]["abstain"] == dict(count=5, denominator=5, percent=100.0)
    assert by_group[("CONTROL", "A")]["control_expected"]["count"] == 0


def test_a_changed_request_after_preregistration_is_refused_before_any_send(manifest, control, tmp_path):
    plan = plan_for(manifest, control)
    plan["cases"][0]["body"]["max_tokens"] += 1
    with pytest.raises(ValueError, match="FROZEN_REQUEST_CHANGED"):
        run(plan, control, tmp_path, lambda body, variant: pytest.fail("sent a modified request"))


def test_a_stale_catalog_check_stops_after_recording_intent_without_sending(manifest, control, tmp_path):
    plan, ledger = plan_for(manifest, control), MemoryLedger()
    view, context, _ = control
    stale = lambda variant, target: {"verified_at_utc": "2020-01-01T00:00:00+00:00"}
    result = rep.execute(plan, {"AMD": context, "CONTROL": context}, tmp_path, ledger,
                         lambda body, variant: pytest.fail("sent with a stale catalog"), stale, progress=lambda *a, **k: None)
    assert result["status"] == "STOPPED" and result["stop_reason"] == "FRESH_CATALOG_EXPIRED" and result["model_posts"] == 0

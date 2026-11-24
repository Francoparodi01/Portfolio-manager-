"""Bounded free format controls followed by frozen AMD Elliott; no moderator."""
import argparse
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import run_technical_desk_free_smoke as smoke
from src.technical_desk.dry_dispatch import abstention_fixture

DIRECTORY = smoke.PHASE / "reasoning-diagnostic-v1"
CASES = ("minimum_default", "minimum_disabled", "elliott_disabled")


def make_plan(manifest, view, context):
    if view["instrument_id"] != "BYMA:CEDEAR:AMD:ARS":
        raise ValueError("FROZEN_AMD_REQUIRED")
    fixture = abstention_fixture("elliott", view)
    for result in fixture["results"]:
        result["abstention_reason"] = "SYNTHETIC_FORMAT_CONTROL_ONLY"
    minimal = dict(prompt_version="technical-desk-minimum-format-control-v1", messages=[
        dict(role="system", content="Return exactly the supplied JSON object, without commentary. "
             "This is a synthetic output-format control, not a market analysis. SHADOW_ONLY."),
        dict(role="user", content=smoke.wire_json(fixture))])
    elliott = smoke.render_compact_prompt("elliott", view, context=context, replica_id=1)
    requests = []
    for name in CASES:
        variant = deepcopy(manifest)
        variant["reasoning"] = None if name == "minimum_default" else {"enabled": False}
        rendered = elliott if name == "elliott_disabled" else minimal
        cap = manifest["parameters"]["output_caps"]["elliott"] if name == "elliott_disabled" else 512
        body = smoke.build_free_request(rendered, manifest=variant, role="elliott", max_output_tokens=cap)
        bound = smoke.byte_ceiling(body, role="elliott")
        if (bound + cap > manifest["context_length"] or bound > manifest["max_prompt_tokens"]
                or cap > manifest["max_completion_tokens"]):
            raise ValueError("CONTEXT_LIMIT_EXCEEDED")
        requests.append(dict(case=name, manifest=variant, body=body,
                             request_sha256=smoke.sha(smoke.serialized_request(body)),
                             prompt_version=rendered["prompt_version"], input_bound=bound))
    default_body = deepcopy(requests[0]["body"])
    default_body["reasoning"] = {"enabled": False}
    assert default_body == requests[1]["body"]
    original = smoke.build_free_request(elliott, manifest=manifest, role="elliott",
                                       max_output_tokens=manifest["parameters"]["output_caps"]["elliott"])
    original["reasoning"] = {"enabled": False}
    assert original == requests[2]["body"]
    return dict(version="technical-desk-reasoning-diagnostic-v1", mode="SHADOW_ONLY",
                instrument_id=view["instrument_id"], packet_digest=view["packet_digest"],
                maximum_posts=3, retries=0, moderator=False, pilot=False, maximum_cost_usd="0",
                minimum_expected=fixture, elliott_gate="VALID_MINIMUM_JSON_AND_ZERO_REASONING",
                requests=requests)


def evaluate(result, case, plan, view, context):
    payload = result.get("response_json") or {}
    choices = payload.get("choices") or []
    choice = choices[0] if choices else {}
    content = choice.get("message", {}).get("content")
    parsed = None
    evaluation = dict(status="NOT_EVALUABLE", reason=result.get("error_code"))
    if result.get("status") == "RECEIVED":
        evaluation, parsed = smoke.validate_answer(content, "elliott", view, context)
    tokens = (result.get("identity_usage") or {}).get("tokens") or {}
    output, reasoning = tokens.get("output"), tokens.get("reasoning")
    visible = output - reasoning if type(output) is int and type(reasoning) is int else None
    exhausted = (content in (None, "") and result.get("finish_reason") == "length"
                 and type(output) is int and output > 0 and reasoning == output)
    exact_control = parsed == plan["minimum_expected"] if case != "elliott_disabled" else None
    gate = (exact_control is True and reasoning == 0 and result.get("finish_reason") == "stop"
            and result.get("status") == "RECEIVED" and not result.get("abort"))
    return dict(validation=evaluation, content_is_null=content is None,
                content_utf8_bytes=len(content.encode("utf-8")) if isinstance(content, str) else None,
                native_finish_reason=choice.get("native_finish_reason"),
                reported_nonreasoning_output_tokens=visible, exhausted_in_reasoning=exhausted,
                exact_synthetic_control=exact_control, elliott_gate_passed=gate), parsed


def execute(plan, view, context, directory, ledger, send, preflight, *, progress=print):
    cases = tuple(item["case"] for item in plan["requests"])
    followup = cases == ("elliott_disabled",) and plan.get("verified_minimum_dependency") is not None
    if (cases != CASES and not followup or plan["maximum_posts"] != len(cases)
            or plan["moderator"] is not False or plan["pilot"] is not False):
        raise ValueError("EXACT_DIAGNOSTIC_SCOPE_REQUIRED")
    plan_hash = smoke.content_hash(plan)
    run_id = "reasoning-diagnostic-" + plan_hash[:20]
    smoke.require_recorded(ledger.record_run(run_id, dict(plan=plan, plan_hash=plan_hash)))
    rows = []
    stopped = None
    for item in plan["requests"]:
        name, body, manifest = item["case"], item["body"], item["manifest"]
        if name == "elliott_disabled" and not followup and not rows[-1]["evaluation"]["elliott_gate_passed"]:
            stopped = "MINIMUM_DISABLED_DID_NOT_PASS"
            break
        if smoke.sha(smoke.serialized_request(body)) != item["request_sha256"]:
            raise ValueError("FROZEN_REQUEST_CHANGED")
        fresh = preflight(manifest, directory)
        parameters = {k: v for k, v in body.items() if k not in ("model", "messages", "response_format")}
        request_id = run_id + ":" + name
        request = dict(request_id=request_id, requested_model=manifest["model_id"],
                       parameters=parameters, prompt_version=item["prompt_version"], body=body,
                       serialized_body_sha256=item["request_sha256"], case=name, role="elliott")
        smoke.require_recorded(ledger._record("evaluations", request_id + ":intent",
                               dict(request=request, fresh_catalog=fresh, status="PRE_SEND_INTENT")))
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(fresh["verified_at_utc"])).total_seconds()
        if not 0 <= age <= 60:
            raise ValueError("FRESH_CATALOG_REQUIRED")
        progress(json.dumps(dict(event="sending", case=name)), flush=True)
        result = send(body, manifest)
        raw = result.get("raw_response")
        if raw is not None:
            smoke.save_bytes(directory / (name + ".response.raw.json"), raw)
        if result.get("raw_generation") is not None:
            smoke.save_bytes(directory / (name + ".generation.raw.json"), result["raw_generation"])
        identity = result.get("identity_usage") or {}
        tokens = identity.get("tokens") or {}
        evaluation, parsed = evaluate(result, name, plan, view, context)
        row = dict(case=name, status=result.get("status"), http_status=result.get("http_status"),
                   finish_reason=result.get("finish_reason"), tokens=tokens,
                   input_bound=item["input_bound"], output_bound=body["max_tokens"],
                   latency_ms=result.get("latency_ms"), model_resolved=identity.get("resolved_model"),
                   provider_resolved=identity.get("responding_provider"), endpoint_requested=manifest["endpoint"],
                   endpoint_resolved=identity.get("resolved_endpoint"), usage_cost=identity.get("usage_cost"),
                   cost_details=identity.get("cost_details"), evaluation=evaluation,
                   hypothetical_paid_cost_usd=smoke.hypothetical_cost(identity, manifest),
                   hypothetical_paid_pricing=manifest["hypothetical_paid_pricing"],
                   request_sha256=item["request_sha256"], response_sha256=smoke.sha(raw) if raw is not None else None,
                   selected_headers=result.get("selected_headers"), error_code=result.get("error_code"))
        text, encoding = smoke.ledger_response(raw)
        row["ledger_response_encoding"] = encoding
        cost = identity.get("usage_cost")
        metadata = dict(requested_model=manifest["model_id"], resolved_model=identity.get("resolved_model"),
                        responding_provider=identity.get("responding_provider"), parameters=parameters,
                        prompt_version=item["prompt_version"], input_tokens=tokens.get("input"),
                        output_tokens=tokens.get("output"), reasoning_tokens=tokens.get("reasoning"),
                        calculated_cost_usd=None, billed_cost_usd=cost,
                        billing_status="BILLED_COST_UNKNOWN" if cost is None else "BILLED_COST_REPORTED",
                        status="RECEIVED" if raw is not None else "FAILED")
        # Synthetic format controls are never recorded as market hypotheses.
        hypotheses = (parsed,) if name == "elliott_disabled" and parsed is not None else ()
        smoke.require_recorded(ledger.record_attempt(run_id=run_id, request=request, response=text,
                               metadata=metadata, hypotheses=hypotheses, evaluations=(row,)))
        rows.append(row)
        progress(json.dumps(dict(event="recorded", case=name, evaluation=evaluation["validation"]["status"],
                                 tokens=tokens, usage_cost=cost)), flush=True)
        if result.get("abort") or cost is None:
            stopped = result.get("error_code") or "COST_UNKNOWN"
            break
    return dict(status="STOPPED" if stopped else "COMPLETED", stop_reason=stopped, attempts=rows,
                model_posts=len(rows), moderator_started=False, pilot_started=False, ledger=ledger.verify())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--elliott-only", action="store_true")
    parser.add_argument("--followup-version", type=int, choices=(2, 3), default=2)
    args = parser.parse_args()
    manifest, view, context, protected = smoke.load_inputs()
    plan = make_plan(manifest, view, context)
    target = DIRECTORY
    if args.elliott_only:
        prior_path = DIRECTORY / "execution/summary.json"
        prior = json.loads(prior_path.read_bytes())
        minimal = [r for r in prior["attempts"] if r["case"] == "minimum_disabled"]
        if len(minimal) != 1 or not minimal[0]["evaluation"]["elliott_gate_passed"] or minimal[0]["usage_cost"] != 0:
            raise ValueError("VERIFIED_MINIMUM_DEPENDENCY_REQUIRED")
        plan.update(version=f"technical-desk-reasoning-diagnostic-v{args.followup_version}", requests=plan["requests"][2:], maximum_posts=1,
                    verified_minimum_dependency=dict(path=str(prior_path.relative_to(ROOT)),
                                                     sha256=smoke.sha(prior_path.read_bytes())))
        target = smoke.PHASE / f"reasoning-diagnostic-v{args.followup_version}"
    if not args.execute:
        target.mkdir(parents=True, exist_ok=False)
        plan["preregistered_at"] = datetime.now(timezone.utc).isoformat()
        plan["code_sha256"] = {str(path.relative_to(ROOT)): smoke.sha(path.read_bytes()) for path in
                              (Path(__file__).resolve(), ROOT / "src/technical_desk/free_smoke_transport.py",
                               ROOT / "src/technical_desk/compact_prompts.py")}
        smoke.save_json(target / "plan.json", plan)
        smoke.save_json(target / "plan.sha256.json", dict(sha256=smoke.sha((target / "plan.json").read_bytes())))
        print(json.dumps(dict(status="PREREGISTERED", posts=0, cases=[r["case"] for r in plan["requests"]])))
        return
    frozen = json.loads((target / "plan.json").read_bytes())
    expected = json.loads((target / "plan.sha256.json").read_bytes())["sha256"]
    if smoke.sha((target / "plan.json").read_bytes()) != expected:
        raise ValueError("PREREGISTRATION_CHANGED")
    if any(plan[k] != frozen[k] for k in plan) or any(smoke.sha((ROOT / path).read_bytes()) != digest
            for path, digest in frozen["code_sha256"].items()):
        raise ValueError("INPUT_OR_CODE_CHANGED_AFTER_PREREGISTRATION")
    key = smoke.key_from_environment(None)
    directory = target / "execution"
    directory.mkdir(exist_ok=False)
    ledger_path = directory / "ledger.sqlite3"
    def guard(event, values):
        if event == "sqlite3.connect" and Path(values[0]).resolve() != ledger_path.resolve():
            raise RuntimeError("NONLOCAL_DATABASE_FORBIDDEN")
    sys.addaudithook(guard)
    ledger = smoke.LocalLedger(ledger_path, output_root=smoke.PHASE)
    result = execute(frozen, view, context, directory, ledger,
                     lambda body, variant: smoke.send_free_attempt(body, manifest=variant, api_key=key),
                     smoke.fresh_zero_catalog)
    result["protected_files_unchanged"] = all(smoke.sha((ROOT / path).read_bytes()) == digest
                                                for path, digest in protected.items())
    smoke.save_json(directory / "summary.json", result)
    key = None
    print(json.dumps({k: v for k, v in result.items() if k not in ("attempts", "ledger")}))


if __name__ == "__main__":
    main()

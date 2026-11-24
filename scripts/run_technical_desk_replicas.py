"""Preregistered 16-call free shadow comparison. No retries, moderator, or pilot."""
import argparse
from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
import statistics
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import run_technical_desk_free_smoke as smoke
from scripts.technical_desk_replica_control import build_control, identifies_expected_count, amd_compatible_dates
from src.technical_desk.role_contracts import RoleOutput

DIRECTORY = smoke.PHASE / "replica-experiment"
COMMIT = "ba9c4373c8ee1a1ea68146c24a6fede1339d4bb6"
BLOCKS = (("CONTROL", 1), ("AMD", 1), ("CONTROL", 2), ("AMD", 2),
          ("CONTROL", 3), ("AMD", 3), ("AMD", 4), ("AMD", 5))
CODE_FILES = ("scripts/run_technical_desk_replicas.py", "scripts/technical_desk_replica_control.py",
              "src/technical_desk/free_smoke_transport.py", "src/technical_desk/compact_prompts.py",
              "src/technical_desk/desk_transport.py", "src/technical_desk/local_ledger.py",
              "scripts/run_technical_desk_free_smoke.py")


def prepare_plan(manifest, packets, contexts, expected, choice):
    if choice["arm_b_reasoning"] not in ({"enabled": True}, {"enabled": True, "effort": "low"}):
        raise ValueError("EXACT_ARM_B_REQUIRED")
    arms = {"A": dict(reasoning={"enabled": False}, max_tokens=4096),
            "B": dict(reasoning=choice["arm_b_reasoning"], max_tokens=24576)}
    rendered = {name: smoke.render_compact_prompt("elliott", view, context=contexts[name], replica_id=1)
                for name, view in packets.items()}
    if any(r["prompt_version"] != "technical-desk-prompt-elliott-v2.3" for r in rendered.values()):
        raise ValueError("FROZEN_PROMPT_V23_REQUIRED")
    if rendered["AMD"]["messages"][0] != rendered["CONTROL"]["messages"][0]:
        raise ValueError("SYSTEM_MESSAGE_MUST_MATCH_ACROSS_DATASETS")
    cases = []
    for block_index, (dataset, replica) in enumerate(BLOCKS):
        for arm in (("A", "B") if block_index % 2 == 0 else ("B", "A")):
            variant = deepcopy(manifest)
            variant["reasoning"] = arms[arm]["reasoning"]
            body = smoke.build_free_request(rendered[dataset], manifest=variant, role="elliott",
                                             max_output_tokens=arms[arm]["max_tokens"])
            bound = smoke.byte_ceiling(body, role="elliott")
            if bound + body["max_tokens"] > manifest["context_length"] or bound > manifest["max_prompt_tokens"] or body["max_tokens"] > manifest["max_completion_tokens"]:
                raise ValueError("MODEL_CONTEXT_OR_OUTPUT_LIMIT")
            cases.append(dict(case_id=f"{dataset}-{arm}-{replica}", dataset=dataset, arm=arm, replica=replica,
                              body=body, manifest=variant, request_sha256=smoke.sha(smoke.serialized_request(body)),
                              input_bound=bound, prompt_version=rendered[dataset]["prompt_version"]))
    for dataset in packets:
        a, b = [next(c["body"] for c in cases if c["dataset"] == dataset and c["arm"] == arm) for arm in ("A", "B")]
        if {k: v for k, v in a.items() if k not in ("reasoning", "max_tokens")} != {k: v for k, v in b.items() if k not in ("reasoning", "max_tokens")}:
            raise ValueError("ARMS_MAY_DIFFER_ONLY_IN_REASONING_AND_MAX_TOKENS")
    return dict(version="technical-desk-replicas-v1", mode="SHADOW_ONLY", phase1_commit=COMMIT,
                maximum_posts=16, automatic_retries=0, stop_on_429=True, moderator=False, pilot=False,
                arms=arms, effort_choice=choice, cases=cases, packets=packets, expected_control=expected,
                amd_compatible_dates=amd_compatible_dates(packets["AMD"]),
                prompt_system_sha256=smoke.content_hash(rendered["AMD"]["messages"][0]),
                metrics_definition=dict(valid="parse_role_output plus frozen package validation; ABSTAIN is valid",
                    abstain="both D and W explicitly ABSTAIN; denominator all attempted requests",
                    rates="numerator divided by attempted per dataset/arm; planned counts shown separately",
                    control_success=expected["spec"]["success"], unknown="missing stays null; known-token denominators reported",
                    replica_independence="REPLICAS_OF_AN_ARM_ARE_IDENTICAL_REQUESTS_AT_TEMPERATURE_0; they measure provider-side nondeterminism, not sampling variance",
                    amd_expected_answer="NONE_PREREGISTERED; neither contract nor prompt defines recency, so AMD ABSTAIN rate is descriptive, not accuracy",
                    ordering="eight paired blocks with alternating AB/BA, controls interleaved; no adaptive changes"))


def validate_result(result, case, view, context, expected):
    choice = ((result.get("response_json") or {}).get("choices") or [{}])[0]
    content = choice.get("message", {}).get("content")
    parsed = None
    if result.get("status") != "RECEIVED":
        return dict(valid=False, abstain=False, rejection_type=result.get("error_code") or result.get("status"),
                    control_expected=False if case["dataset"] == "CONTROL" else None), None
    evaluation, parsed = smoke.validate_answer(content, "elliott", view, context)
    rejection = None
    details = []
    if parsed is None:
        rejection = evaluation.get("reason") or evaluation["status"]
        if rejection == "ROLE_OUTPUT_SCHEMA_VIOLATION":
            try:
                RoleOutput.model_validate(json.loads(content))
            except Exception as exc:
                if hasattr(exc, "errors"):
                    details = [{"location": e["loc"], "type": e["type"], "message": e["msg"]}
                               for e in exc.errors(include_input=False, include_url=False)]
                    rejection = "ROLE_SCHEMA:" + "|".join(sorted({e["message"].removeprefix("Value error, ") for e in details}))
    return dict(valid=parsed is not None, abstain=evaluation["status"] == "ABSTAIN",
                rejection_type=rejection, rejection_details=details, validation=evaluation,
                states_by_timeframe={r["timeframe"]: r["state"] for r in parsed["results"]} if parsed else {},
                control_expected=identifies_expected_count(parsed, expected) if case["dataset"] == "CONTROL" else None), parsed


def metrics(rows):
    groups = []
    for dataset in ("AMD", "CONTROL"):
        for arm in ("A", "B"):
            group = [r for r in rows if r["dataset"] == dataset and r["arm"] == arm]
            n = len(group)
            def rate(key):
                count = sum(r["evaluation"].get(key) is True for r in group)
                return dict(count=count, denominator=n, percent=100 * count / n if n else None)
            def stats(values):
                known = [v for v in values if type(v) in (int, float)]
                return dict(known=len(known), mean=statistics.mean(known) if known else None, maximum=max(known) if known else None)
            groups.append(dict(dataset=dataset, arm=arm, attempted=n, planned=5 if dataset == "AMD" else 3,
                               valid=rate("valid"), abstain=rate("abstain"),
                               control_expected=rate("control_expected") if dataset == "CONTROL" else None,
                               rejections=dict(Counter(r["evaluation"]["rejection_type"] for r in group if r["evaluation"].get("rejection_type"))),
                               finish_reasons=dict(Counter(r.get("finish_reason") or "UNKNOWN" for r in group)),
                               output_tokens=stats([r["tokens"].get("output") for r in group]),
                               reasoning_tokens=stats([r["tokens"].get("reasoning") for r in group]),
                               latency_ms=stats([r.get("latency_ms") for r in group])))
    return groups


def execute(plan, contexts, directory, ledger, send, preflight, *, progress=print):
    cases = plan["cases"]
    if len(cases) != 16 or Counter((c["dataset"], c["arm"]) for c in cases) != Counter({("AMD", "A"): 5, ("AMD", "B"): 5, ("CONTROL", "A"): 3, ("CONTROL", "B"): 3}):
        raise ValueError("EXACT_16_REQUEST_SCOPE_REQUIRED")
    run_id = "replicas-" + smoke.content_hash(plan)[:20]
    smoke.require_recorded(ledger.record_run(run_id, dict(plan=plan, plan_hash=smoke.content_hash(plan))))
    rows, stopped = [], None
    def summary():
        return dict(status="STOPPED" if stopped else "COMPLETED" if len(rows) == 16 else "RUNNING",
                    stop_reason=stopped, model_posts=len(rows), planned_posts=16, attempts=rows,
                    metrics=metrics(rows), moderator_started=False, pilot_started=False, ledger=ledger.verify())
    for case in cases:
        body, variant = case["body"], case["manifest"]
        if smoke.sha(smoke.serialized_request(body)) != case["request_sha256"]:
            raise ValueError("FROZEN_REQUEST_CHANGED")
        target = directory / case["case_id"]
        target.mkdir(exist_ok=False)
        try:
            fresh = preflight(variant, target)
        except Exception as exc:
            stopped = "PREFLIGHT_FAILED_" + type(exc).__name__
            break
        request_id = run_id + ":" + case["case_id"]
        parameters = {k: v for k, v in body.items() if k not in ("model", "messages", "response_format")}
        request = dict(request_id=request_id, body=body, requested_model=variant["model_id"], parameters=parameters,
                       prompt_version=case["prompt_version"], dataset=case["dataset"], arm=case["arm"], replica=case["replica"],
                       serialized_body_sha256=case["request_sha256"])
        smoke.require_recorded(ledger._record("evaluations", request_id + ":intent", dict(status="PRE_SEND_INTENT", request=request, fresh_catalog=fresh)))
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(fresh["verified_at_utc"])).total_seconds()
        if not 0 <= age <= 60:
            stopped = "FRESH_CATALOG_EXPIRED"
            break
        progress(json.dumps(dict(event="sending", case=case["case_id"], ordinal=len(rows) + 1)), flush=True)
        result = send(body, variant)
        raw = result.get("raw_response")
        if raw is not None:
            smoke.save_bytes(target / "response.raw.json", raw)
        if result.get("raw_generation") is not None:
            smoke.save_bytes(target / "generation.raw.json", result["raw_generation"])
        identity = result.get("identity_usage") or {}
        tokens = identity.get("tokens") or {}
        evaluation, parsed = validate_result(result, case, plan["packets"][case["dataset"]], contexts[case["dataset"]], plan["expected_control"])
        text, encoding = smoke.ledger_response(raw)
        cost = identity.get("usage_cost")
        row = dict(case_id=case["case_id"], dataset=case["dataset"], arm=case["arm"], replica=case["replica"],
                   http_status=result.get("http_status"), transport_status=result.get("status"), finish_reason=result.get("finish_reason"),
                   tokens=tokens, latency_ms=result.get("latency_ms"), usage_cost=cost, cost_details=identity.get("cost_details"),
                   model_resolved=identity.get("resolved_model"), provider_resolved=identity.get("responding_provider"),
                   endpoint_resolved=identity.get("resolved_endpoint"), endpoint_requested=variant["endpoint"],
                   input_bound=case["input_bound"], output_bound=body["max_tokens"], evaluation=evaluation,
                   request_sha256=case["request_sha256"], response_sha256=smoke.sha(raw) if raw is not None else None,
                   ledger_response_encoding=encoding, selected_headers=result.get("selected_headers"),
                   hypothetical_paid_cost_usd=smoke.hypothetical_cost(identity, variant), hypothetical_paid_pricing=variant["hypothetical_paid_pricing"])
        metadata = dict(requested_model=variant["model_id"], resolved_model=identity.get("resolved_model"),
                        responding_provider=identity.get("responding_provider"), parameters=parameters,
                        prompt_version=case["prompt_version"], input_tokens=tokens.get("input"), output_tokens=tokens.get("output"),
                        reasoning_tokens=tokens.get("reasoning"), calculated_cost_usd=None, billed_cost_usd=cost,
                        billing_status="BILLED_COST_UNKNOWN" if cost is None else "BILLED_COST_REPORTED",
                        status="RECEIVED" if raw is not None else "FAILED")
        hypotheses = (parsed,) if case["dataset"] == "AMD" and parsed is not None else ()
        smoke.require_recorded(ledger.record_attempt(run_id=run_id, request=request, response=text, metadata=metadata,
                               hypotheses=hypotheses, evaluations=(row,)))
        rows.append(row)
        progress(json.dumps(dict(event="recorded", case=case["case_id"], valid=evaluation["valid"], abstain=evaluation["abstain"],
                                 expected=evaluation["control_expected"], tokens=tokens, usage_cost=cost)), flush=True)
        if result.get("http_status") == 429:
            stopped = "RATE_LIMIT_429_NO_RETRY"
        elif result.get("abort"):
            stopped = result.get("error_code") or "TRANSPORT_GUARD_ABORTED"
        smoke.save_json(directory / f"summary-{len(rows):02d}.json", summary())
        if stopped:
            break
    result = summary()
    smoke.save_json(directory / "summary.json", result)
    return result


def phase1_commit_in_history(root=ROOT, commit=COMMIT):
    """The Phase 1 commit must be an ancestor of HEAD; later commits are allowed because
    CODE_FILES and the protected contracts are bound by SHA-256 in the preregistered plan."""
    try:
        subprocess.run(["git", "merge-base", "--is-ancestor", commit, "HEAD"], cwd=root, check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except (subprocess.CalledProcessError, OSError):
        return False
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    if not phase1_commit_in_history():
        raise ValueError("PHASE1_COMMIT_REQUIRED")
    manifest, amd, amd_context, protected = smoke.load_inputs()
    if not args.execute:
        control, control_context, expected = build_control()
        choice = json.loads((DIRECTORY / "pricing/reasoning-choice.json").read_bytes())
        plan = prepare_plan(manifest, {"AMD": amd, "CONTROL": control}, {"AMD": amd_context, "CONTROL": control_context}, expected, choice)
        plan.update(preregistered_at=datetime.now(timezone.utc).isoformat(), protected_sha256=protected,
                    code_sha256={name: smoke.sha((ROOT / name).read_bytes()) for name in CODE_FILES})
        smoke.save_json(DIRECTORY / "plan.json", plan)
        smoke.save_json(DIRECTORY / "plan.sha256.json", dict(sha256=smoke.sha((DIRECTORY / "plan.json").read_bytes())))
        print(json.dumps(dict(status="PREREGISTERED", model_posts=0, planned_posts=16)))
        return
    plan = json.loads((DIRECTORY / "plan.json").read_bytes())
    if smoke.sha((DIRECTORY / "plan.json").read_bytes()) != json.loads((DIRECTORY / "plan.sha256.json").read_bytes())["sha256"]:
        raise ValueError("FROZEN_PLAN_CHANGED")
    if any(smoke.sha((ROOT / path).read_bytes()) != expected for path, expected in {**plan["code_sha256"], **plan["protected_sha256"]}.items()):
        raise ValueError("FROZEN_CODE_OR_CONTRACT_CHANGED")
    if smoke.content_hash(amd) != smoke.content_hash(plan["packets"]["AMD"]):
        raise ValueError("FROZEN_AMD_CHANGED")
    contexts = {name: smoke.context_from_market_view(view) for name, view in plan["packets"].items()}
    directory = DIRECTORY / "execution"
    directory.mkdir(exist_ok=False)
    key = smoke.key_from_environment(None)
    ledger_path = directory / "ledger.sqlite3"
    def guard(event, args):
        if event == "sqlite3.connect" and Path(args[0]).resolve() != ledger_path.resolve():
            raise RuntimeError("NONLOCAL_DATABASE_FORBIDDEN")
    sys.addaudithook(guard)
    ledger = smoke.LocalLedger(ledger_path, output_root=smoke.PHASE)
    result = execute(plan, contexts, directory, ledger,
                     lambda body, variant: smoke.send_free_attempt(body, manifest=variant, api_key=key), smoke.fresh_zero_catalog)
    key = None
    print(json.dumps({k: v for k, v in result.items() if k not in ("attempts", "metrics", "ledger")}))


if __name__ == "__main__":
    main()

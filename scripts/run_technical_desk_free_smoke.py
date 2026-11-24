"""One preregistered free AMD smoke. No production database or generic transport."""
import argparse
import base64
import hashlib
import http.client
import io
import json
import math
import os
import re
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from email.utils import parsedate_to_datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.technical_desk.compact_dry_run import byte_ceiling, context_from_market_view
from src.technical_desk.compact_prompts import render_compact_prompt, validate_compact_response, wire_json
from src.technical_desk.desk_transport import serialized_request
from src.technical_desk.evidence_capture import content_hash
from src.technical_desk.free_smoke_transport import build_free_request, send_free_attempt
from src.technical_desk.local_ledger import LocalLedger

PHASE = ROOT / "output/technical-desk-stage1b1/free-smoke"
MANIFEST_HASH = "2de850080e367191b4de6793d4ad9861ff385ab8e66828bf75a5e3f02d73e092"
ROLES = ("elliott", "wyckoff", "structure", "momentum", "moderator")


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def save_bytes(path, raw, *, secret=None):
    if secret and secret.encode() in raw:
        raise RuntimeError("SECRET_ECHO_NOT_RETAINED")
    temporary = path.with_name(path.name + ".part")
    if path.exists():
        raise RuntimeError("EXISTING_EVIDENCE_NEVER_OVERWRITTEN")
    with temporary.open("xb") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def save_json(path, value):
    save_bytes(path, (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode())


def load_inputs():
    raw = (PHASE / "model-manifest.json").read_bytes()
    if sha(raw) != MANIFEST_HASH:
        raise RuntimeError("FROZEN_MANIFEST_MISMATCH")
    manifest = json.loads(raw)
    protected = json.loads((PHASE / "protected-baseline.json").read_bytes())
    if not all(sha((ROOT / path).read_bytes()) == value for path, value in protected.items()):
        raise RuntimeError("PROTECTED_CONTRACT_CHANGED")
    summary = json.loads((ROOT / "output/technical-desk-stage1b1/verified-v2.1/summary.json").read_bytes())
    source = ROOT / "output/technical-desk-stage1b/verified-preparation/dry-run.json"
    if sha(source.read_bytes()) != summary["source_dry_run_sha256"]:
        raise RuntimeError("FROZEN_SOURCE_CHANGED")
    view = None
    for item in json.loads(source.read_bytes())["requests"]:
        request = item["request"]
        if request["agent"] == "elliott" and request["replica_id"] == 1:
            candidate = json.loads(request["messages"][1]["content"])["market_packet"]
            if candidate["instrument_id"] == "BYMA:CEDEAR:AMD:ARS":
                if content_hash(request) != item["request_hash"] or view is not None:
                    raise RuntimeError("AMD_FROZEN_REQUEST_MISMATCH")
                view = candidate
    if view is None or content_hash(view) != summary["source_packet_view_hashes"]["AMD"]:
        raise RuntimeError("AMD_PACKET_REQUIRED")
    return manifest, view, context_from_market_view(view), protected


def key_from_environment(env_file):
    key = os.environ.get("OPENROUTER_API_KEY", "").strip()
    if not key and env_file:
        # Read only this variable; do not load other credentials or config.
        from dotenv import dotenv_values
        with Path(env_file).open(encoding="utf-8-sig") as handle:
            for line in handle:
                assignment = line.strip().removeprefix("export ").lstrip()
                if assignment.split("=", 1)[0].strip() == "OPENROUTER_API_KEY":
                    key = (dotenv_values(stream=io.StringIO(assignment), interpolate=False).get("OPENROUTER_API_KEY") or "").strip()
                    break
    if not key:
        raise RuntimeError("OPENROUTER_API_KEY_MISSING")
    return key


def fresh_zero_catalog(manifest, directory):
    when = datetime.now(timezone.utc)
    connection = http.client.HTTPSConnection("openrouter.ai", timeout=30)
    try:
        connection.request("GET", "/api/v1/models", headers={"Accept": "application/json"})
        response = connection.getresponse()
        raw = response.read()
        status = response.status
    finally:
        connection.close()
    stem = when.strftime("%Y%m%dT%H%M%SZ") + "-models-preflight"
    save_bytes(directory / (stem + ".raw.json"), raw)
    evidence = dict(verified_at_utc=when.isoformat(), url="https://openrouter.ai/api/v1/models",
                    sha256=sha(raw), http_status=status, authorization_header_sent=False)
    save_json(directory / (stem + ".manifest.json"), evidence)
    if status != 200:
        raise RuntimeError("FRESH_CATALOG_HTTP_FAILED")
    matches = [m for m in json.loads(raw)["data"] if m.get("id") == manifest["model_id"]]
    if len(matches) != 1 or not matches[0]["id"].endswith(":free"):
        raise RuntimeError("FRESH_FREE_MODEL_UNAVAILABLE")
    model = matches[0]
    prices = model.get("pricing", {})
    if any(Decimal(str(prices.get(k, "NaN"))) != 0 for k in ("prompt", "completion")):
        raise RuntimeError("FRESH_ZERO_PRICING_NOT_VERIFIED")
    criteria = json.loads((PHASE / "selection-criteria.json").read_bytes())
    if (model.get("context_length", 0) < criteria["bound_evidence"]["minimum_context"] or
            not {"response_format", "structured_outputs"} <= set(model.get("supported_parameters", []))):
        raise RuntimeError("FRESH_MODEL_REQUIREMENTS_CHANGED")
    return evidence


def retry_wait_seconds(result, policy, now):
    headers = {key.lower(): value for key, value in result.get("selected_headers", {}).items()}
    waits = []
    for key, value in headers.items():
        try:
            if key == "retry-after":
                try:
                    waits.append(float(value))
                except ValueError:
                    waits.append((parsedate_to_datetime(value) - now).total_seconds())
            elif "ratelimit" in key and "reset" in key:
                number = float(value)
                waits.append(number / 1000 - now.timestamp() if number > 1e12 else
                             number - now.timestamp() if number > 1e9 else number)
        except (ValueError, TypeError, OverflowError):
            return None
    default = policy["default_429_wait_seconds_without_headers"] if result.get("http_status") == 429 else policy["default_transport_wait_seconds"]
    if any(not math.isfinite(value) or value < 0 for value in waits):
        return None
    delay = max([0] + waits) if waits else default
    return delay if delay <= policy["maximum_synchronous_header_wait_seconds"] else None


def validate_answer(content, role, view, context):
    try:
        json.loads(content)
    except (TypeError, ValueError):
        return dict(status="INVALID_JSON", parse_role_output="NOT_EVALUABLE", anchor_validation="NOT_EVALUABLE"), None
    try:
        output = validate_compact_response(content, role=role, view=view, context=context)
    except ValueError as exc:
        # Store rule codes / error categories, never a potentially huge input echo.
        reason = str(exc).splitlines()[0][:120]
        if "validation error" in reason.lower():
            reason = "ROLE_OUTPUT_SCHEMA_VIOLATION"
        return dict(status="INVALID", parse_role_output="REJECTED", anchor_validation="REJECTED_OR_NOT_EVALUABLE", reason=reason), None
    parsed = json.loads(output.model_dump_json())
    evaluation = dict(status="ABSTAIN" if all(r.state == "ABSTAIN" for r in output.results) else "VALID",
                      parse_role_output="VALID", anchor_validation="VALID",
                      timeframes=[dict(timeframe=r.timeframe, state=r.state, scenarios=len(r.scenarios)) for r in output.results])
    if role == "elliott":
        evaluations = []
        for result in parsed["results"]:
            for scenario in result["scenarios"]:
                anchors = {a["anchor_id"] for a in scenario["causal_anchors"] + scenario["structure_anchors"]}
                matches = []
                for scale, source in view["features"]["elliott_scales"].get(result["timeframe"], {}).items():
                    for candidate in source.get("candidates", []):
                        if set(candidate["pivot_ids"]) == anchors:
                            matches.append(dict(scale=scale, status=candidate["status"], rules=candidate["rules"]))
                evaluations.append(dict(timeframe=result["timeframe"], family=scenario["family"],
                                        matches=matches, status="MATCHED_FULL_PRECISION_RULES" if matches else "NOT_EVALUABLE_NO_EXACT_CANDIDATE_MATCH"))
        evaluation["elliott_scenarios"] = evaluations
    return evaluation, parsed


def hypothetical_cost(identity, manifest):
    tokens = identity.get("tokens", {})
    if tokens.get("input") is None or tokens.get("output") is None or tokens.get("cache_write") not in (None, 0):
        return None
    prices = manifest["hypothetical_paid_pricing"]["pricing"]
    return str(Decimal(tokens["input"]) * Decimal(prices["prompt"]) + Decimal(tokens["output"]) * Decimal(prices["completion"]))


def require_recorded(receipt):
    if receipt.get("status") != "RECORDED":
        raise RuntimeError("UNRECORDED_OR_ALREADY_ATTEMPTED")


def ledger_response(raw):
    if raw is None:
        return None, None
    try:
        return raw.decode("utf-8"), "UTF8"
    except UnicodeDecodeError:
        return base64.b64encode(raw).decode("ascii"), "BASE64_HTTP_BYTES"


def execute(manifest, view, context, directory, ledger, send, fresh_evidence, *, sleep=time.sleep, progress=print):
    if view["instrument_id"] != "BYMA:CEDEAR:AMD:ARS" or manifest["scope"]["logical_requests"] != 5 or manifest["scope"]["replicas"] != 1 or manifest["scope"]["debate"]:
        raise RuntimeError("AUTHORIZED_AMD_SMOKE_SCOPE_REQUIRED")
    run_id = "free-smoke-" + MANIFEST_HASH[:20]
    code_hashes = {str(path.relative_to(ROOT)): sha(path.read_bytes()) for path in
                  (Path(__file__).resolve(), ROOT / "src/technical_desk/free_smoke_transport.py")}
    require_recorded(ledger.record_run(run_id, dict(manifest=manifest, manifest_sha256=MANIFEST_HASH,
        fresh_catalog=fresh_evidence, instrument_id=view["instrument_id"], mode="SHADOW_ONLY", scope=manifest["scope"], code_sha256=code_hashes)))
    results, responses, attempts = [], [], []
    stopped = None
    posts = 0
    for role in ROLES:
        if stopped or (role == "moderator" and len(responses) != 4):
            results.append(dict(role=role, status="NOT_STARTED", reason=stopped or "VALIDATED_SPECIALIST_DEPENDENCY_MISSING", attempts=0))
            continue
        rendered = render_compact_prompt(role, view, context=context,
            specialist_responses=responses if role == "moderator" else None, replica_id=1)
        body = build_free_request(rendered, manifest=manifest, role=role,
                                  max_output_tokens=manifest["parameters"]["output_caps"][role])
        bound = byte_ceiling(body, role=role)
        if bound + body["max_tokens"] > manifest["context_length"] or bound > manifest["max_prompt_tokens"] or body["max_tokens"] > manifest["max_completion_tokens"]:
            raise RuntimeError("FROZEN_CONTEXT_OR_OUTPUT_LIMIT_EXCEEDED")
        body_hash = sha(serialized_request(body))
        logical_id = content_hash(dict(run_id=run_id, role=role, replica=1, packet=view["packet_digest"]))
        role_attempts = []
        for attempt in (1, 2):
            if sha(serialized_request(body)) != body_hash or posts >= 10:
                raise RuntimeError("IMMUTABLE_REQUEST_OR_ATTEMPT_LIMIT")
            request_id = logical_id + ":" + str(attempt)
            parameters = {k: v for k, v in body.items() if k not in ("model", "messages", "response_format")}
            request = dict(request_id=request_id, logical_request_id=logical_id, role=role, replica_id=1,
                           attempt=attempt, requested_model=manifest["model_id"], prompt_version=rendered["prompt_version"],
                           parameters=parameters, body=body, serialized_body_sha256=body_hash,
                           input_bound=bound, output_bound=body["max_tokens"])
            require_recorded(ledger._record("evaluations", request_id + ":intent", dict(run_id=run_id, request=request, status="PRE_SEND_INTENT")))
            if posts == 0 and (datetime.now(timezone.utc) - datetime.fromisoformat(fresh_evidence["verified_at_utc"])).total_seconds() > manifest["fresh_catalog_max_age_seconds_before_first_post"]:
                raise RuntimeError("FRESH_CATALOG_TOO_OLD_BEFORE_FIRST_POST")
            progress(json.dumps(dict(event="sending", role=role, attempt=attempt)), flush=True)
            result = send(body)
            posts += 1
            raw = result.get("raw_response")
            response_path = None
            if raw is not None:
                response_path = f"{role}-attempt-{attempt}.response.raw.json"
                save_bytes(directory / response_path, raw)
            if result.get("raw_generation") is not None:
                save_bytes(directory / f"{role}-attempt-{attempt}.generation.raw.json", result["raw_generation"])
            response_text, response_encoding = ledger_response(raw)
            identity = result.get("identity_usage") or {}
            tokens = identity.get("tokens", {})
            evaluation = dict(status="INVALID", parse_role_output="NOT_EVALUABLE", anchor_validation="NOT_EVALUABLE", reason=result.get("error_code"))
            parsed = None
            content = None
            if result.get("status") == "RECEIVED":
                choices = (result.get("response_json") or {}).get("choices") or []
                content = choices[0].get("message", {}).get("content") if choices else None
                evaluation, parsed = validate_answer(content, role, view, context)
            retry_eligible = result.get("retry_eligible", False) or evaluation["status"] == "INVALID_JSON"
            row = dict(role=role, attempt=attempt, status=evaluation["status"], transport_status=result.get("status"),
                       http_status=result.get("http_status"), finish_reason=result.get("finish_reason"),
                       latency_ms=result.get("latency_ms"), tokens=tokens or None, input_bound=bound,
                       output_bound=body["max_tokens"], request_sha256=body_hash, response_sha256=sha(raw) if raw is not None else None,
                       response_path=response_path, model_resolved=identity.get("resolved_model"),
                       ledger_response_encoding=response_encoding,
                       generation_response_path=f"{role}-attempt-{attempt}.generation.raw.json" if result.get("raw_generation") is not None else None,
                       generation_response_sha256=sha(result["raw_generation"]) if result.get("raw_generation") is not None else None,
                       generation_http_status=result.get("generation_http_status"),
                       endpoint_requested=manifest["endpoint"], endpoint_resolved=identity.get("resolved_endpoint"),
                       provider_resolved=identity.get("responding_provider"), usage_cost=identity.get("usage_cost"),
                       cost_details=identity.get("cost_details"), hypothetical_paid_cost_usd=hypothetical_cost(identity, manifest),
                       hypothetical_paid_pricing=manifest["hypothetical_paid_pricing"], selected_headers=result.get("selected_headers"),
                       validation=evaluation, abort=result.get("abort", True), retry_eligible=retry_eligible)
            billed = identity.get("usage_cost")
            metadata = dict(requested_model=manifest["model_id"], resolved_model=identity.get("resolved_model"),
                responding_provider=identity.get("responding_provider"), parameters=parameters, prompt_version=rendered["prompt_version"],
                input_tokens=tokens.get("input"), output_tokens=tokens.get("output"), reasoning_tokens=tokens.get("reasoning"),
                calculated_cost_usd=None, billed_cost_usd=billed,
                billing_status="BILLED_COST_UNKNOWN" if billed is None else "BILLED_COST_REPORTED",
                status="RECEIVED" if raw is not None else "FAILED")
            require_recorded(ledger.record_attempt(run_id=run_id, request=request,
                response=response_text,
                metadata=metadata, hypotheses=() if parsed is None else (parsed,), evaluations=(row,)))
            attempts.append(row)
            role_attempts.append(row)
            progress(json.dumps(dict(event="recorded", role=role, attempt=attempt, status=row["status"], http_status=row["http_status"])), flush=True)
            if result.get("abort"):
                stopped = result.get("error_code") or "TRANSPORT_GUARD_ABORTED"
                break
            if parsed is not None:
                responses.append(dict(agent=role, replica_id=1, fixture_only=False, response_json=content))
                break
            if not retry_eligible:
                break
            if attempt == 2:
                if result.get("http_status") == 429:
                    stopped = "RATE_LIMIT_AFTER_SINGLE_RETRY"
                break
            delay = retry_wait_seconds(result, manifest["retry_policy"], datetime.now(timezone.utc))
            if delay is None:
                stopped = "RETRY_DEFERRED_BY_LIMIT_HEADERS"
                break
            progress(json.dumps(dict(event="retry_wait", role=role, seconds=delay)), flush=True)
            sleep(delay)
        results.append(dict(role=role, status=role_attempts[-1]["status"], attempts=len(role_attempts), last_attempt=role_attempts[-1]))
    verify = ledger.verify()
    all_valid = len(results) == 5 and all(row["status"] in ("VALID", "ABSTAIN") for row in results)
    return dict(status="ABORTED" if stopped else "COMPLETED" if all_valid else "COMPLETED_WITH_FAILURES", stop_reason=stopped, model_requests=posts,
                logical_requests_attempted=sum(r["attempts"] > 0 for r in results), results=results, attempts=attempts,
                ledger=verify, pilot_started=False, mode="SHADOW_ONLY")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--env-file")
    parser.add_argument("--execution-name", default="execution")
    args = parser.parse_args()
    manifest, view, context, protected = load_inputs()
    if not args.execute:
        print(json.dumps(dict(status="PREPARED_NOT_STARTED", model=manifest["model_id"], endpoint=manifest["endpoint"], protected=len(protected))))
        return
    if not re.fullmatch(r"execution(?:-\d+)?", args.execution_name):
        raise RuntimeError("INVALID_EXECUTION_DIRECTORY_NAME")
    directory = PHASE / args.execution_name
    directory.mkdir(exist_ok=False)
    ledger_path = (directory / "ledger.sqlite3").resolve()
    def local_db_guard(event, audit_args):
        if event == "sqlite3.connect" and Path(audit_args[0]).resolve() != ledger_path:
            raise RuntimeError("PRODUCTION_DATABASE_FORBIDDEN")
    sys.addaudithook(local_db_guard)
    key = None
    try:
        ledger = LocalLedger(ledger_path, output_root=directory)
        key = key_from_environment(args.env_file)
        fresh = fresh_zero_catalog(manifest, directory)
        result = execute(manifest, view, context, directory, ledger,
            lambda request: send_free_attempt(request, manifest=manifest, api_key=key), fresh)
        result["protected_files_unchanged"] = all(sha((ROOT / path).read_bytes()) == value for path, value in protected.items())
        save_json(directory / "summary.json", result)
        print(json.dumps({k: result[k] for k in ("status", "stop_reason", "model_requests", "logical_requests_attempted", "protected_files_unchanged")}))
    except Exception as exc:
        # No traceback or credential-bearing exception details in logs.
        reason = str(exc) if re.fullmatch(r"[A-Z0-9_]+", str(exc)) else "EXECUTION_STOPPED_NO_AUTOMATIC_RESUME"
        failure = dict(status="UNRECORDED_OR_ABORTED", exception_type=type(exc).__name__, reason=reason, pilot_started=False)
        save_json(directory / "failure.json", failure)
        print(json.dumps(failure))
        raise SystemExit(1)
    finally:
        key = None


if __name__ == "__main__":
    main()

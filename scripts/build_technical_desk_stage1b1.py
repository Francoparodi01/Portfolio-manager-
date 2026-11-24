"""Offline compact projection and captured-price budgets. Never writes a database."""
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from decimal import Decimal
import argparse
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def offline_guard(event, args):
    if event in {"socket.connect", "socket.getaddrinfo", "sqlite3.connect", "subprocess.Popen"}:
        raise RuntimeError("STAGE1B1_NO_NETWORK_OR_DATABASE")


def encode(value):
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(type(value).__name__)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selection", type=Path, required=True)
    parser.add_argument("--preregistration", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not args.output.resolve().is_relative_to((ROOT / "output").resolve()):
        raise ValueError("OUTPUT_DIRECTORY_REQUIRED")
    sys.addaudithook(offline_guard)
    from src.technical_desk.compact_prompts import projection_policy, EXPECTED_POLICY_SHA256, wire_json
    from src.technical_desk.compact_dry_run import context_from_market_view, prepare_compact_requests, evaluate_selection
    from src.technical_desk.evidence_capture import content_hash

    read = lambda path: json.loads(path.read_bytes())
    phase = ROOT / "output/technical-desk-stage1b1"
    prereg = read(args.preregistration)
    started_at = datetime.now(timezone.utc)
    if prereg["sha256"] != EXPECTED_POLICY_SHA256 or datetime.fromisoformat(prereg["registered_at_utc"]) > started_at:
        raise ValueError("PROJECTION_PREREGISTRATION_REQUIRED")
    policy = projection_policy()
    protected = read(phase / "preserved-sha256.json")
    def verify_protected():
        if any(hashlib.sha256((ROOT / p).read_bytes()).hexdigest() != h for p, h in protected.items()):
            raise ValueError("PROTECTED_CONTRACT_CHANGED")
    verify_protected()
    selection = read(args.selection)
    if selection["schema"] != "technical-desk-stage1b1-model-selection-v1" or len(selection["models"]) != 2:
        raise ValueError("EXPLICIT_TWO_MODEL_SELECTION_REQUIRED")
    source_path = ROOT / "output/technical-desk-stage1b/verified-preparation/dry-run.json"
    source_raw = source_path.read_bytes()
    previous = json.loads(source_raw)
    views, old_prompts = {}, {}
    for item in previous["requests"]:
        request = item["request"]
        if content_hash(request) != item["request_hash"]:
            raise ValueError("FROZEN_REQUEST_HASH_MISMATCH")
        view = json.loads(request["messages"][1]["content"])["market_packet"]
        ticker = view["instrument_id"].split(":")[2]
        old_prompts[(ticker, request["replica_id"], request["agent"])] = request["messages"]
        if request["agent"] == "elliott" and request["replica_id"] == 1:
            views[ticker] = view
    original_hashes = {ticker: content_hash(view) for ticker, view in views.items()}
    contexts = {ticker: context_from_market_view(view) for ticker, view in views.items()}
    rendered = prepare_compact_requests(views, contexts)
    comparisons = []
    for model in selection["models"]:
        explicit = dict(model, capture_manifest=str(ROOT / model["capture_manifest"]))
        comparisons.append(evaluate_selection(rendered, contexts, explicit, as_of=started_at))
    if {ticker: content_hash(view) for ticker, view in views.items()} != original_hashes:
        raise ValueError("FULL_PRECISION_SOURCE_WAS_MUTATED")
    sizes = {}
    for entry in rendered:
        key = (entry["ticker"], entry["replica"], entry["agent"])
        before = len(wire_json(old_prompts[key]).encode("utf-8"))
        after = len(wire_json(entry["rendered"]["messages"]).encode("utf-8"))
        counts = sizes.setdefault(entry["agent"], dict(before_total=0, after_total=0, before_max=0, after_max=0, requests=0))
        counts["before_total"] += before
        counts["after_total"] += after
        counts["before_max"] = max(counts["before_max"], before)
        counts["after_max"] = max(counts["after_max"], after)
        counts["requests"] += 1
    verify_protected()
    args.output.mkdir(parents=True, exist_ok=False)
    def save(name, data):
        with (args.output / name).open("x", encoding="utf-8", newline="\n") as handle:
            json.dump(data, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=encode)
            handle.write("\n")
    sources = [ROOT / "src/technical_desk" / f"{name}.py" for name in
               ("compact_prompts", "compact_dry_run", "desk_transport", "endpoint_pricing")] + [Path(__file__)]
    summary = dict(status="NOT_STARTED", started_at=started_at, preregistration=prereg,
        projection_policy_hash=EXPECTED_POLICY_SHA256, pricing_cutoff=started_at, source_packet_view_hashes=original_hashes,
        source_dry_run_sha256=hashlib.sha256(source_raw).hexdigest(), source_selection_sha256=hashlib.sha256(args.selection.read_bytes()).hexdigest(),
        source_sha256={str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sources},
        prompt_bytes_by_role=sizes, protected_sha256_unchanged=True, model_calls=0, database_writes=0,
        smoke_started=False, token_bound=policy["input_bound"], models=[])
    for result in comparisons:
        bounds = {role: max((r["input_byte_ceiling"] for r in result["requests"] if r["agent"] == role), default=None)
                  for role in policy["output_caps_tokens"]}
        summary["models"].append(dict(model_id=result["model_id"], endpoint=result["endpoint"],
            status=result["status"], feasibility=result.get("feasibility", "NOT_STARTED"), reasons=result["reasons"],
            pricing_status=result["pricing"]["status"], pricing_valid_until=result["pricing"].get("valid_until"),
            context_length=result["pricing"]["context_length"], context_pass=result.get("context_pass", False),
            input_ceiling_by_role=bounds, output_caps=policy["output_caps_tokens"],
            pilot_reservation_usd=(result["pilot"] or {}).get("required_reservation_usd"),
            smoke_reservation_usd=(result["smoke"] or {}).get("required_reservation_usd")))
    save("model-manifests.json", [result["pricing"]["manifest"] for result in comparisons])
    save("dry-run.json", comparisons)
    save("summary.json", summary)
    print(json.dumps(dict(status="NOT_STARTED", bytes_by_role=sizes, models=summary["models"],
                          model_calls=0, database_writes=0, smoke_started=False), default=encode))


if __name__ == "__main__":
    main()

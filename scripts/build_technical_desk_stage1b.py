"""Offline prompt/ledger preparation; never starts the AMD smoke test."""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import argparse

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))


def offline_guard(output_root):
    def guard(event,args):
        if event in {"socket.connect","socket.getaddrinfo","subprocess.Popen"}:
            raise RuntimeError("STAGE1B_EXTERNAL_IO_FORBIDDEN")
        if event=="sqlite3.connect":
            path=Path(args[0]).resolve()
            if not path.is_relative_to(output_root.resolve()) or path.suffix!=".sqlite3":
                raise RuntimeError("STAGE1B_ONLY_LOCAL_OUTPUT_LEDGER_ALLOWED")
    return guard


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output",type=Path,default=ROOT/"output/technical-desk-stage1b/preparation")
    args=parser.parse_args()
    if not args.output.resolve().is_relative_to((ROOT/"output").resolve()):
        raise ValueError("OUTPUT_MUST_STAY_OUTSIDE_SOURCE_TREE")
    sys.addaudithook(offline_guard(args.output))
    from scripts.build_technical_desk_stage1a import market_bars
    from src.technical_desk.evidence_capture import canonical, content_hash
    from src.technical_desk.elliott_control import extract_pivots
    from src.technical_desk.prompt_packets import project_packet
    from src.technical_desk.prompts import template_manifest
    from src.technical_desk.dry_dispatch import build_dry_run, MAX_OUTPUT_TOKENS, MAX_RESPONSE_UTF8_BYTES
    from src.technical_desk.local_ledger import LocalLedger
    packets_dir=ROOT/"output/technical-desk-stage0c/packets"
    features_dir=ROOT/"output/technical-desk-stage1a/verified-final"
    elliott_dir=ROOT/"output/technical-desk-stage1a1/elliott"
    packet_manifest,feature_manifest,elliott_manifest=read(packets_dir/"manifest.json"),read(features_dir/"manifest.json"),read(elliott_dir/"manifest.json")
    policy=read(ROOT/"docs/technical-desk-stage1a-policy.json")
    policy5=deepcopy(policy)
    policy5["pivots"].update(left_bars=5,right_bars=5,scale_by_timeframe={"D":"5L5R_D","W":"5L5R_W"})
    views,contexts,input_hashes={},{},{}
    feature_entries={p["ticker"]:p for p in feature_manifest["packets"]}
    elliott_entries={(p["ticker"],p["timeframe"]):p for p in elliott_manifest["frames"]}
    for entry in packet_manifest["packets"]:
        ticker=entry["ticker"]
        packet=read(packets_dir/entry["file"])
        feature_entry=feature_entries[ticker]
        feature=read(features_dir/feature_entry["file"])
        if content_hash(packet)!=entry["content_hash"] or content_hash(feature)!=feature_entry["content_hash"] or feature["source_packet_hash"]!=entry["content_hash"]:
            raise ValueError("FROZEN_PACKET_HASH_MISMATCH")
        if feature["policy_hash"]!=content_hash(policy):
            raise ValueError("FROZEN_FEATURE_POLICY_MISMATCH")
        secondary,secondary_evaluations={},{}
        for tf in ("D","W"):
            e=elliott_entries[(ticker,tf)]
            saved=read(elliott_dir/e["file"])
            if content_hash(saved)!=e["content_hash"] or saved["secondary_policy_hash"]!=content_hash(policy5):
                raise ValueError("FROZEN_SECONDARY_SCALE_HASH_MISMATCH")
            extracted=extract_pivots(market_bars(packet,tf),timeframe=tf,policy=policy5,pivot_window=5)
            if len(extracted["confirmed_pivots"])!=saved["secondary_observed_5_plus_5"]["confirmed_pivot_count"]:
                raise ValueError("SECONDARY_PIVOT_COUNT_MISMATCH")
            secondary[tf]=extracted["confirmed_pivots"]
            secondary_evaluations[tf]=saved["secondary_observed_5_plus_5"]
            input_hashes[f"{ticker}-{tf}-elliott"]=e["content_hash"]
        views[ticker],contexts[ticker]=project_packet(packet,feature,secondary,secondary_evaluations)
        input_hashes[ticker]=dict(packet=entry["content_hash"],features=feature_entry["content_hash"])
    as_of=datetime.now(timezone.utc)
    plan=build_dry_run(views,contexts,as_of=as_of)
    args.output.mkdir(parents=True,exist_ok=False)
    def save(filename,payload):
        with (args.output/filename).open("xb") as handle:
            handle.write(canonical(payload)+b"\n")
    templates=template_manifest()
    run_id="dry-"+content_hash(dict(requests=[item["request_hash"] for item in plan["requests"]],fixture=plan["fixture_pricing"]))
    sources=[ROOT/"src/technical_desk"/f"{name}.py" for name in ("prompts","prompt_packets","dry_dispatch","local_ledger")]+[Path(__file__)]
    source_hashes={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sources}
    smoke=dict(schema="technical-desk-amd-smoke-preregistration-v1",registered_at=as_of.isoformat(),status="NOT_STARTED",
        instrument_id=views["AMD"]["instrument_id"],packet_id=views["AMD"]["packet_id"],packet_digest=views["AMD"]["packet_digest"],
        roles=list(MAX_OUTPUT_TOKENS),replicas=1,debate=False,base_requests=5,budget_usd="0.50",
        requested_model=None,resolved_model=None,provider=None,pricing_verified=False,
        reasons=["MODEL_TO_BE_FIXED","PROVIDER_TO_BE_FIXED","VERIFIED_PRICING_REQUIRED","EXECUTION_NOT_AUTHORIZED"],
        prompt_templates=templates,max_output_tokens=MAX_OUTPUT_TOKENS,max_response_utf8_bytes=MAX_RESPONSE_UTF8_BYTES,
        source_sha256=source_hashes,retry_policy="NO_AUTOMATIC_SMOKE_RETRIES_WITHOUT_NEW_RESERVATION",
        moderator_dependencies="FOUR_SAME_REPLICA_SPECIALIST_RESPONSES",model_calls=0,dispatch_authorized=False)
    save("smoke-preregistration.json",smoke)
    save("prompt-manifest.json",dict(templates=templates,source_sha256=source_hashes,input_hashes=input_hashes,
        output_schema_hash=content_hash(views["AMD"]["output_schema"])))
    save("dry-run.json",plan)
    ledger=LocalLedger(args.output/"ledger.sqlite3",output_root=args.output)
    recorded=ledger.record_run(run_id,dict(mode="DRY_RUN",status="NOT_STARTED",request_count=50,
        plan_hash=content_hash(plan),fixture_budget=plan["fixture_budget"],actual_calculated_cost_usd=None,
        billed_cost_usd=None,billing_status="BILLED_COST_UNKNOWN",smoke_preregistration_hash=content_hash(smoke)))
    if not recorded["recorded"]:
        raise RuntimeError("UNRECORDED")
    for item in plan["requests"]:
        request=item["request"]
        metadata=dict(requested_model=None,resolved_model=None,responding_provider=None,parameters=request["parameters"],
            prompt_version=request["prompt_version"],input_tokens=None,output_tokens=None,reasoning_tokens=None,
            calculated_cost_usd=None,billed_cost_usd=None,billing_status="BILLED_COST_UNKNOWN",status="PLANNED")
        receipt=ledger.record_attempt(run_id=run_id,request=request,response=None,metadata=metadata)
        if not receipt["recorded"]:
            raise RuntimeError("UNRECORDED")
    verification=ledger.verify()
    summary=dict(status=plan["status"],run_id=run_id,request_count=50,template_count=5,
        source_sha256=source_hashes,fixture_budget=plan["fixture_budget"],ledger=verification,
        model_calls=0,productive_database_writes=0,smoke_started=False)
    save("summary.json",summary)
    print(json.dumps(dict(status=summary["status"],requests=50,templates=5,ledger_counts=verification["counts"],
        fixture_reservation_usd=str(plan["fixture_budget"]["required_reservation_usd"]),fixture_budget_status=plan["fixture_budget"]["status"],
        model_calls=0,smoke_started=False)))


if __name__=="__main__":
    main()

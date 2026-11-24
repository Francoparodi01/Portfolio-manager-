"""Offline deterministic features and contract checks; no stage 1b dispatch."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.build_technical_desk_stage0c import forbid_external_io
from src.technical_desk.evidence_capture import canonical, content_hash
from src.technical_desk.features import compute_features
from src.technical_desk.elliott_rules import RULE_VERSION, validate_impulse, validate_abc
from src.technical_desk.volume_scan import scan_volume
from src.technical_desk.pilot_protocol_v3 import AGENTS, PacketSpec
from src.technical_desk.role_contracts import AnchorContext, PacketContext, parse_role_output, role_output_schema


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def market_bars(packet, timeframe):
    daily = {}
    for row in packet["daily"]:
        fields = dict(row)
        if fields.pop("fact_hash") != content_hash(fields):
            raise ValueError("DAILY_FACT_HASH_MISMATCH")
        proof = row["close_proof"]
        closed_at = proof.get("confirmed_at",proof.get("safe_after"))
        if closed_at is None:
            raise ValueError("CLOSED_BAR_EVIDENCE_REQUIRED")
        daily[row["fact_hash"]] = dict(time=row["bar_start"],closed_at=closed_at,
            fact_hash=row["fact_hash"],available_at=row["available_at"],
            **{key:row[key] for key in ("open","high","low","close")})
    if timeframe == "D":
        return list(daily.values())
    weekly = []
    for row in packet["weekly"]:
        fields = dict(row)
        if fields.pop("fact_hash") != content_hash(fields) or row["volume"] is not None:
            raise ValueError("WEEKLY_FACT_OR_VOLUME_CONTRACT_MISMATCH")
        components = [daily[key] for key in row["component_hashes"]]
        weekly.append(dict(time=components[0]["time"],closed_at=max((r["closed_at"] for r in components),key=datetime.fromisoformat),
            fact_hash=row["fact_hash"],available_at=row["available_at"],
            **{key:row[key] for key in ("open","high","low","close")}))
    return weekly


def elliott_candidates(features, bars, tick, timeframe):
    chain = features["active_alternating_pivots"]
    result = {}
    for name, size, validator in (("ordinary_impulse",6,validate_impulse),("five_pivot_partial",5,validate_impulse),("abc_basic",4,validate_abc)):
        candidates = []
        for start in range(max(0,len(chain)-size+1)):
            pivots = chain[start:start+size]
            evaluated = validator(pivots,bars,tick_size=tick,scale=timeframe)
            candidates.append(dict(pivot_ids=[p["id"] for p in pivots],**evaluated))
        result[name] = dict(count=len(candidates),status_counts=dict(Counter(r["status"] for r in candidates)),
                            candidates=candidates)
    return result


def contract_smoke(feature_packet, digest, source, tick):
    """Validate fixture abstentions against real anchors; not model responses."""
    anchors = tuple(AnchorContext(p["id"],tf,datetime.fromisoformat(p["pivot_time"]),datetime.fromisoformat(p["confirmed_at"]))
                    for tf,result in feature_packet["features"].items() for p in result["confirmed_pivots"])
    packet = PacketSpec(packet_id=digest,digest=digest,instrument_id=source["instrument_id"],currency="ARS",
        tick_size=tick,anchor_ids=tuple(a.anchor_id for a in anchors),validated_timeframes=("D","W"),quality_status="READY",
        atr_by_timeframe=tuple((tf,Decimal(source["atr_by_timeframe"][tf]["value"])) for tf in ("D","W")))
    context = PacketContext(packet,datetime.fromisoformat(source["cutoff"]),anchors)
    checked = []
    for agent in AGENTS:
        raw = json.dumps(dict(packet_id=digest,packet_digest=digest,agent=agent,
            volume_basis="RELATIVE_UNVERIFIED" if agent=="wyckoff" else "NOT_USED",
            evaluation_cohort="WYCKOFF_RELATIVE_UNVERIFIED" if agent=="wyckoff" else "OTHER_ROLES",
            results=[dict(timeframe=tf,state="ABSTAIN",scenarios=[],uncertainty="UNKNOWN",
                          abstention_reason="SYNTHETIC_CONTRACT_CHECK_NO_MODEL_RUN") for tf in ("D","W")]))
        parsed = parse_role_output(raw,expected_agent=agent,packet_context=context)
        if parsed.affects_analysis or parsed.affects_execution:
            raise AssertionError("AUTHORITY_MUST_REMAIN_FALSE")
        checked.append(agent)
    return dict(status="PASS",fixture_only=True,models_invoked=False,agents=checked,
                context_anchor_count=len(anchors),response_count_for_pilot=0)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packets",type=Path,required=True)
    parser.add_argument("--policy",type=Path,required=True)
    parser.add_argument("--registration",type=Path,required=True)
    parser.add_argument("--output",type=Path,required=True)
    args=parser.parse_args()
    sys.addaudithook(forbid_external_io)
    policy, registration = read(args.policy), read(args.registration)
    if (hashlib.sha256(args.policy.read_bytes()).hexdigest()!=registration["policy_sha256"]
            or policy!=registration["policy"] or policy["elliott"]["version"]!=RULE_VERSION):
        raise ValueError("REGISTERED_POLICY_MISMATCH")
    manifest=read(args.packets/"manifest.json")
    entries=manifest["packets"]
    if manifest["status"]!="READY" or len(entries)!=5 or {p["ticker"] for p in entries}!=set(registration["tick_contracts"]):
        raise ValueError("FIVE_FROZEN_READY_PACKETS_REQUIRED")
    args.output.mkdir(parents=True,exist_ok=False)
    summaries=[]
    for entry in entries:
        packet=read(args.packets/entry["file"])
        if (content_hash(packet)!=entry["content_hash"] or packet["ticker"]!=entry["ticker"]
                or packet["operating_mode"]!="SHADOW_ONLY" or not packet["quality"]["ready"]):
            raise ValueError("FROZEN_PACKET_CONTRACT_MISMATCH")
        tick_contract=registration["tick_contracts"][entry["ticker"]]
        if tick_contract["capture_hash"]!=packet["source_capture_hash"]:
            raise ValueError("TICK_CAPTURE_LINEAGE_MISMATCH")
        tick=Decimal(tick_contract["tick_size"])
        features, validators, causal_checks = {}, {}, []
        for timeframe in ("D","W"):
            bars=market_bars(packet,timeframe)
            feature=compute_features(bars,timeframe=timeframe,tick_size=tick,policy=policy)
            if feature["snapshots"][-1]["indicators"]["atr"]["value"]!=Decimal(packet["atr_by_timeframe"][timeframe]["value"]):
                raise ValueError("ATR_DIFFERS_FROM_FROZEN_V3")
            # Fixed boundary cuts, declared before any outputs are inspected.
            cuts=sorted({k for k in (0,1,2,3,4,13,14,19,20,25,33,49,199,len(bars)//2,len(bars)-1) if k<len(bars)})
            for k in cuts:
                prefix=compute_features(bars[:k+1],timeframe=timeframe,tick_size=tick,policy=policy)
                if canonical(prefix["snapshots"][-1])!=canonical(feature["snapshots"][k]):
                    raise AssertionError("FUTURE_DATA_LEAK")
            causal_checks.append(dict(timeframe=timeframe,cuts=cuts,result="PASS"))
            features[timeframe]=feature
            validators[timeframe]=elliott_candidates(feature,bars,tick,timeframe)
        scan=scan_volume(packet["daily"],window=policy["volume_scan"]["pre_sessions"],threshold=Decimal(policy["volume_scan"]["threshold"]))
        payload=dict(schema="technical-desk-stage1a-v1",ticker=entry["ticker"],instrument_id=packet["instrument_id"],
            mode="SHADOW_ONLY",affects_analysis=False,affects_execution=False,
            source_packet_hash=entry["content_hash"],market_data_cutoff=packet["cutoff"],
            policy_hash=content_hash(policy),tick_contract=tick_contract,features=features,
            elliott=validators,volume_review=scan,
            temporal_semantics="CAUSAL_CLOSED_BAR_LOGIC_ON_CURRENT_CAPTURE_NOT_ORIGINAL_HISTORICAL_AVAILABILITY")
        digest=content_hash(payload)
        fixture_check=contract_smoke(payload,digest,packet,tick)
        filename=f"{entry['ticker']}-{digest[:16]}.json"
        with (args.output/filename).open("xb") as handle:
            handle.write(canonical(payload)+b"\n")
        summary=dict(ticker=entry["ticker"],file=filename,content_hash=digest,price_tick=str(tick),
            frames={tf:dict(bars=len(features[tf]["snapshots"]),confirmed_pivots=len(features[tf]["confirmed_pivots"]),
                active_alternating_pivots=len(features[tf]["active_alternating_pivots"]),
                five_pivot_windows=len(features[tf]["five_pivot_windows"]),
                open_candidates=len(features[tf]["snapshots"][-1]["open_extrema"]),
                missing_features=features[tf]["missing_features"],
                ordinary_impulse_statuses=validators[tf]["ordinary_impulse"]["status_counts"])
                    for tf in ("D","W")},
            volume_review=dict(flagged_count=scan["flagged_count"],flagged_dates=scan["flagged_dates"],
                               status_counts=scan["status_counts"],classification="CANDIDATE_FOR_REVIEW_NOT_ADJUSTMENT"),
            causal_checks=causal_checks,contract_fixture_check=fixture_check)
        summaries.append(summary)
    schema=role_output_schema()
    with (args.output/"role-output.schema.json").open("xb") as handle:
        handle.write(canonical(schema)+b"\n")
    final=dict(schema="technical-desk-stage1a-manifest-v1",built_at=datetime.now(timezone.utc).isoformat(),
        input_manifest_hash=content_hash(manifest),policy_hash=content_hash(policy),registration_hash=content_hash(registration),
        role_schema_hash=content_hash(schema),mode="SHADOW_ONLY",models_invoked=False,database_writes=False,
        network_connections=False,stage1b_started=False,packets=summaries)
    with (args.output/"manifest.json").open("xb") as handle:
        handle.write(canonical(final)+b"\n")
    print(json.dumps([dict(ticker=s["ticker"],frames=s["frames"],volume_flagged_count=s["volume_review"]["flagged_count"]) for s in summaries],indent=2))


if __name__=="__main__":
    main()

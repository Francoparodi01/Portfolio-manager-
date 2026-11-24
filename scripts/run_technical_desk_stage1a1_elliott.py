"""Freeze and run the offline 2+2 Elliott geometric null, without models or DB."""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import platform
import statistics
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.build_technical_desk_stage0c import forbid_external_io
from scripts.build_technical_desk_stage1a import market_bars
from src.technical_desk.evidence_capture import canonical, content_hash
from src.technical_desk.features import compute_features
from src.technical_desk.elliott_control import (PROCEDURE, SECONDARY_SCALE, REPLICATES, calibrate, compare_control,
    evaluate_series, extract_pivots, random_walk, replica_seed)


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def save(path, payload):
    with path.open("xb") as handle:
        handle.write(canonical(payload))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packets", type=Path, required=True)
    parser.add_argument("--features", type=Path, required=True)
    parser.add_argument("--registration", type=Path, required=True)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    sys.addaudithook(forbid_external_io)
    packet_manifest, feature_manifest = read(args.packets/"manifest.json"), read(args.features/"manifest.json")
    prior_registration, policy = read(args.registration), read(args.policy)
    if hashlib.sha256(args.policy.read_bytes()).hexdigest() != prior_registration["policy_sha256"] or policy != prior_registration["policy"]:
        raise ValueError("REGISTERED_POLICY_MISMATCH")
    entries = sorted(packet_manifest["packets"], key=lambda entry: entry["ticker"])
    if packet_manifest["status"] != "READY" or len(entries) != 5 or len({e["ticker"] for e in entries}) != 5:
        raise ValueError("EXACTLY_FIVE_READY_PACKETS_REQUIRED")
    features_by_ticker = {entry["ticker"]: entry for entry in feature_manifest["packets"]}
    prepared = []
    for entry in entries:
        packet, feature_entry = read(args.packets/entry["file"]), features_by_ticker[entry["ticker"]]
        feature_packet = read(args.features/feature_entry["file"])
        if content_hash(packet) != entry["content_hash"] or content_hash(feature_packet) != feature_entry["content_hash"]:
            raise ValueError("INPUT_HASH_MISMATCH")
        if feature_packet["source_packet_hash"] != entry["content_hash"] or feature_packet["policy_hash"] != content_hash(policy):
            raise ValueError("FEATURE_LINEAGE_MISMATCH")
        for timeframe in ("D", "W"):
            bars = market_bars(packet, timeframe)
            prepared.append(dict(ticker=entry["ticker"], timeframe=timeframe, bars=bars,
                tick_size=feature_entry["price_tick"], calibration=calibrate(bars),
                packet_hash=entry["content_hash"], features_hash=feature_entry["content_hash"],
                expected_features=feature_packet["features"][timeframe],
                expected_statuses=feature_packet["elliott"][timeframe]["ordinary_impulse"]["status_counts"]))
    root = Path(__file__).resolve().parents[1]
    source_files = ["src/technical_desk/elliott_control.py", "src/technical_desk/features.py",
                    "src/technical_desk/elliott_rules.py", "scripts/run_technical_desk_stage1a1_elliott.py"]
    registration = dict(registered_at=datetime.now(timezone.utc).isoformat(), before_pattern_and_control_calculation=True,
        procedure=PROCEDURE, procedure_hash=content_hash(PROCEDURE), policy_hash=content_hash(policy),
        secondary_scale=SECONDARY_SCALE,secondary_scale_hash=content_hash(SECONDARY_SCALE),
        prior_registration_hash=content_hash(prior_registration), python_version=platform.python_version(),
        source_sha256={name:hashlib.sha256((root/name).read_bytes()).hexdigest() for name in source_files},
        inputs=[{key:row[key] for key in ("ticker","timeframe","tick_size","calibration","packet_hash","features_hash")}
                for row in prepared], mode="SHADOW_ONLY", models_invoked=False, database_writes=False)
    args.output.mkdir(parents=True, exist_ok=False)
    save(args.output/"preregistration.json", registration)
    registration_hash = content_hash(registration)
    print(json.dumps(dict(event="PREREGISTERED",hash=registration_hash,inputs=len(prepared))), flush=True)
    summaries = []
    secondary_policy=deepcopy(policy)
    secondary_policy["pivots"].update(left_bars=5,right_bars=5,scale_by_timeframe={"D":"5L5R_D","W":"5L5R_W"})
    for item in prepared:
        ticker, timeframe, bars = item["ticker"], item["timeframe"], item["bars"]
        fast = extract_pivots(bars, timeframe=timeframe, policy=policy)
        # Equivalence is checked against the full feature implementation and the
        # frozen artifact, prior to interpreting any candidate or simulation.
        full = compute_features(bars, timeframe=timeframe, tick_size=item["tick_size"], policy=policy)
        for key in ("confirmed_pivots", "active_alternating_pivots"):
            if canonical(fast[key]) != canonical(full[key]) or canonical(full[key]) != canonical(item["expected_features"][key]):
                raise ValueError("FAST_DETECTOR_EQUIVALENCE_FAILED")
        observed = evaluate_series(bars, tick_size=item["tick_size"], timeframe=timeframe, policy=policy, include_candidates=True)
        if any(count != item["expected_statuses"].get(state, 0) for state, count in observed["status_counts"].items()):
            raise ValueError("REBASED_VALIDATOR_DIFFERS_FROM_FROZEN_STAGE1A")
        secondary_full=compute_features(bars,timeframe=timeframe,tick_size=item["tick_size"],policy=secondary_policy)
        secondary_fast=extract_pivots(bars,timeframe=timeframe,policy=secondary_policy,pivot_window=5)
        for key in ("confirmed_pivots","active_alternating_pivots"):
            if canonical(secondary_fast[key])!=canonical(secondary_full[key]):
                raise ValueError("SECONDARY_FAST_DETECTOR_EQUIVALENCE_FAILED")
        secondary=evaluate_series(bars,tick_size=item["tick_size"],timeframe=timeframe,
            policy=secondary_policy,pivot_window=5,include_candidates=True)
        simulations = []
        for replica in range(REPLICATES):
            seed = replica_seed(ticker, timeframe, replica)
            generated = random_walk(bars, calibration=item["calibration"], tick_size=item["tick_size"], seed=seed,return_diagnostics=True)
            outcome = evaluate_series(generated["bars"], tick_size=item["tick_size"], timeframe=timeframe, policy=policy)
            outcome.update(replica=replica,seed=str(seed),ohlc_hash=content_hash(generated["bars"]),volatility=generated["volatility"])
            simulations.append(outcome)
        comparison = compare_control(observed, simulations)
        volatility_summary={}
        for name in ("realized_sigma","absolute_relative_error"):
            values=[r["volatility"][name] for r in simulations if r["volatility"][name] is not None]
            volatility_summary[name]=dict(minimum=min(values),median=statistics.median(values),maximum=max(values))
        payload = dict(ticker=ticker,timeframe=timeframe,registration_hash=registration_hash,
                       observed=observed,simulations=simulations,comparison=comparison,
                       secondary_observed_5_plus_5=secondary,secondary_policy_hash=content_hash(secondary_policy),
                       volatility_summary=volatility_summary,
                       fast_detector_equivalence="PASS",models_invoked=False,database_writes=False)
        filename = f"{ticker}-{timeframe}.json"
        save(args.output/filename, payload)
        summary = dict(ticker=ticker,timeframe=timeframe,file=filename,content_hash=content_hash(payload),
            bars=len(bars),status_counts=observed["status_counts"],candidate_count=observed["candidate_count"],
            nonoverlap_all=observed["nonoverlap_all"],comparison=comparison,volatility_summary=volatility_summary,
            secondary_observed_5_plus_5={key:secondary[key] for key in ("bar_count","confirmed_pivot_count",
                "active_alternating_pivot_count","candidate_count","status_counts","nonoverlap_all","nonoverlap_compatible")})
        summaries.append(summary)
        print(json.dumps(dict(event="COMPLETED",ticker=ticker,timeframe=timeframe,walks=REPLICATES,
            observed_candidates=observed["candidate_count"],observed_compatible=observed["status_counts"]["RULES_COMPATIBLE"])),flush=True)
    save(args.output/"manifest.json",dict(completed_at=datetime.now(timezone.utc).isoformat(),
        preregistration_hash=registration_hash,frames=summaries,walks_total=REPLICATES*len(prepared),
        mode="SHADOW_ONLY",models_invoked=False,database_writes=False,network_connections=False,
        stage1b_started=False,predictive_validity="NOT_EVALUATED"))


if __name__ == "__main__":
    main()

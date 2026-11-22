"""Preregister, then group frozen volume flags; no network, database or models."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys


def offline_guard(event, args):
    if event in {"socket.connect", "socket.getaddrinfo", "sqlite3.connect", "subprocess.Popen"}:
        raise RuntimeError("OFFLINE_VOLUME_CLUSTERS_EXTERNAL_IO_PROHIBITED:" + event)


if __name__ == "__main__":
    sys.addaudithook(offline_guard)

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.technical_desk.evidence_capture import canonical, content_hash
from src.technical_desk.volume_clusters import POLICY, cluster_volume_flags


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_new(path, value):
    with path.open("xb") as handle:
        handle.write(canonical(value) + b"\n")


def child(directory, filename):
    if Path(filename).name != filename:
        raise ValueError("MANIFEST_FILENAME_MUST_BE_LOCAL")
    return directory / filename


def inputs(args):
    paths = [args.features / "manifest.json", args.packets / "manifest.json", args.calendar, args.feature_policy]
    for directory in (args.features, args.packets):
        entries = read(directory / "manifest.json")["packets"]
        if len(entries) != 5 or len({entry["ticker"] for entry in entries}) != 5:
            raise ValueError("FIVE_DISTINCT_FROZEN_PACKETS_REQUIRED")
        paths.extend(child(directory, entry["file"]) for entry in entries)
    return {str(path.resolve()): sha256(path) for path in paths}


def sources():
    return {str(path.relative_to(ROOT)): sha256(path) for path in
            (Path(__file__).resolve(), ROOT / "src/technical_desk/volume_clusters.py",
             ROOT / "src/technical_desk/packet_data.py", ROOT / "src/technical_desk/evidence_capture.py")}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--features", type=Path, default=ROOT / "output/technical-desk-stage1a/verified-final")
    parser.add_argument("--packets", type=Path, default=ROOT / "output/technical-desk-stage0c/packets")
    parser.add_argument("--calendar", type=Path, default=ROOT / "output/technical-desk-stage0b-remediation/calendar_audit_remediated.json")
    parser.add_argument("--feature-policy", type=Path, default=ROOT / "docs/technical-desk-stage1a-policy.json")
    parser.add_argument("--output", type=Path, default=ROOT / "output/technical-desk-stage1a1")
    parser.add_argument("--register", action="store_true")
    args = parser.parse_args()
    policy_path = args.output / "volume-policy.json"
    registration_path = args.output / "volume-preregistration.json"
    if args.register:
        evidence = inputs(args)
        args.output.mkdir(parents=True, exist_ok=True)
        write_new(policy_path, POLICY)
        registration = dict(schema="technical-desk-volume-clusters-registration-v1",
            registered_at=datetime.now(timezone.utc).isoformat(), policy=POLICY,
            policy_content_hash=content_hash(POLICY), policy_sha256=sha256(policy_path),
            input_file_sha256=evidence, source_file_sha256=sources(),
            calculations_started=False, mode="SHADOW_ONLY",
            runtime_guard="socket.connect/getaddrinfo, sqlite3.connect and subprocess.Popen prohibited")
        write_new(registration_path, registration)
        print(json.dumps(dict(status="REGISTERED", policy_hash=content_hash(POLICY),
                              registration_sha256=sha256(registration_path), input_files=len(evidence))))
        return
    policy, registration = read(policy_path), read(registration_path)
    if (policy != POLICY or registration["policy"] != POLICY
            or registration["policy_content_hash"] != content_hash(POLICY)
            or registration["policy_sha256"] != sha256(policy_path)
            or registration["input_file_sha256"] != inputs(args)
            or registration["source_file_sha256"] != sources()):
        raise ValueError("PREREGISTRATION_OR_FROZEN_INPUT_MISMATCH")
    feature_manifest, packet_manifest, calendar = read(args.features / "manifest.json"), read(args.packets / "manifest.json"), read(args.calendar)
    feature_policy = read(args.feature_policy)
    if feature_manifest["input_manifest_hash"] != content_hash(packet_manifest) or packet_manifest["calendar_hash"] != content_hash(calendar):
        raise ValueError("FROZEN_MANIFEST_CALENDAR_LINEAGE_MISMATCH")
    if (feature_manifest["policy_hash"] != content_hash(feature_policy)
            or feature_policy["indicators"]["atr_period"] != 14
            or feature_policy["indicators"]["atr_method"] != "WILDER_SMA_SEED_FIRST_TR_HIGH_MINUS_LOW"):
        raise ValueError("FROZEN_WILDER14_ATR_POLICY_REQUIRED")
    feature_entries = {entry["ticker"]: entry for entry in feature_manifest["packets"]}
    packet_entries = {entry["ticker"]: entry for entry in packet_manifest["packets"]}
    if set(feature_entries) != set(packet_entries):
        raise ValueError("FROZEN_COHORT_MISMATCH")
    packets = []
    for ticker in sorted(feature_entries):
        feature_entry, packet_entry = feature_entries[ticker], packet_entries[ticker]
        feature, packet = read(child(args.features, feature_entry["file"])), read(child(args.packets, packet_entry["file"]))
        if (content_hash(feature) != feature_entry["content_hash"] or content_hash(packet) != packet_entry["content_hash"]
                or feature["source_packet_hash"] != packet_entry["content_hash"]
                or feature["policy_hash"] != content_hash(feature_policy)
                or packet["calendar_hash"] != content_hash(calendar)
                or feature["mode"] != "SHADOW_ONLY" or packet["operating_mode"] != "SHADOW_ONLY"):
            raise ValueError("FROZEN_PACKET_LINEAGE_MISMATCH")
        snapshots = feature["features"]["D"]["snapshots"]
        if [row["time"] for row in snapshots] != [row["bar_start"] for row in packet["daily"]]:
            raise ValueError("DAILY_FEATURE_ALIGNMENT_MISMATCH")
        scan = feature["volume_review"]
        flags = scan["flagged_dates"]
        if (scan["flagged_count"] != len(flags)
                or scan["schema"] != "technical-desk-volume-scan-v1"
                or scan["window_sessions_each_side"] != 10 or str(scan["jump_factor_threshold"]) != "3"
                or flags != [row["session_date"] for row in scan["checks"] if row["level_jump"] is True]):
            raise ValueError("FROZEN_VOLUME_FLAG_MISMATCH")
        result = cluster_volume_flags(flags, packet["daily"], snapshots, calendar, policy=policy)
        result.update(ticker=ticker, source_packet_hash=packet_entry["content_hash"],
                      source_features_hash=feature_entry["content_hash"])
        packets.append(result)
    output = dict(schema="technical-desk-stage1a1-volume-v1", built_at=datetime.now(timezone.utc).isoformat(),
        mode="SHADOW_ONLY", affects_analysis=False, affects_execution=False, models_invoked=False,
        database_writes=False, network_connections=False, stage1b_started=False,
        runtime_guard="socket.connect/getaddrinfo, sqlite3.connect and subprocess.Popen prohibited",
        policy_hash=content_hash(policy), preregistration_sha256=sha256(registration_path),
        input_file_sha256=registration["input_file_sha256"], source_file_sha256=registration["source_file_sha256"],
        calendar_content_hash=content_hash(calendar), flagged_count=sum(p["flagged_count"] for p in packets),
        cluster_count=sum(p["cluster_count"] for p in packets), packets=packets)
    write_new(args.output / "volume-clusters.json", output)
    print(json.dumps(dict(status="COMPLETED", flagged_count=output["flagged_count"], cluster_count=output["cluster_count"],
        packets=[dict(ticker=p["ticker"], flagged_count=p["flagged_count"], cluster_count=p["cluster_count"],
                      movement_status_counts=p["movement_status_counts"]) for p in packets]), indent=2))


if __name__ == "__main__":
    main()

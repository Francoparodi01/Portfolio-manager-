"""Build stage 0c offline from frozen 0b evidence. Never starts stage 1."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.technical_desk.evidence_capture import canonical, content_hash
from src.technical_desk.stage0c import build_stage0c_packet, role_input


def forbid_external_io(event, args):
    if event in {"socket.connect", "socket.getaddrinfo", "sqlite3.connect", "subprocess.Popen"}:
        raise RuntimeError("OFFLINE_STAGE0C_EXTERNAL_IO_PROHIBITED:" + event)


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--original-packets", type=Path, required=True)
    parser.add_argument("--preregistration", type=Path, required=True)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--adjustments", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    sys.addaudithook(forbid_external_io)
    policy, registration = read(args.policy), read(args.preregistration)
    if (hashlib.sha256(args.policy.read_bytes()).hexdigest() != registration["policy_sha256"]
            or policy != registration["policy"]):
        raise ValueError("PREREGISTERED_POLICY_CHANGED")
    evidence = args.evidence
    source_manifest = read(evidence / "capture-20261115/manifest.json")
    old_manifest = read(evidence / "candidates-calendar-v2/manifest.json")
    original = read(args.original_packets / "rehash.json")
    calendar = read(evidence / "calendar_audit_remediated.json")
    adjustments = read(args.adjustments)
    if content_hash(calendar) != old_manifest["calendar_hash"]:
        raise ValueError("FROZEN_CALENDAR_CHANGED")
    if content_hash(source_manifest) != old_manifest["capture_manifest_hash"]:
        raise ValueError("FROZEN_SOURCE_MANIFEST_CHANGED")
    if content_hash(original) != old_manifest["original_rehash_hash"]:
        raise ValueError("FROZEN_HISTORY_SELECTION_CHANGED")
    starts = {item["ticker"]: item["first_bar"][:10] for item in original["packets"]}
    entries = source_manifest["captures"]
    if (source_manifest["status"] != "CAPTURED" or len(entries) != 5 or len(starts) != 5
            or {e["ticker"] for e in entries} != set(starts)
            or any(e["status"] != "CAPTURED" for e in entries)):
        raise ValueError("FIVE_ORIGINAL_CAPTURES_REQUIRED")
    args.output.mkdir(parents=True, exist_ok=False)
    packets = []
    cutoff = datetime.fromisoformat(old_manifest["cutoff"])
    built_at = datetime.now(timezone.utc).isoformat()
    for entry in entries:
        capture = read(evidence / "capture-20261115" / entry["file"])
        if capture["ticker"] != entry["ticker"]:
            raise ValueError("CAPTURE_IDENTITY_MISMATCH")
        packet = build_stage0c_packet(capture, calendar, original_start=starts[entry["ticker"]],
            cutoff=cutoff, source_hash=entry["content_hash"], policy=policy, adjustments=adjustments)
        packet["market_data_cutoff"] = packet["cutoff"]
        packet["diagnostic_evidence_captured_at"] = adjustments.get("captured_at")
        packet["wyckoff_input_hash"] = content_hash(role_input(packet, "wyckoff"))
        digest = content_hash(packet)
        filename = f"{entry['ticker']}-{digest[:16]}.json"
        with (args.output / filename).open("xb") as handle:
            handle.write(canonical(packet) + b"\n")
        packets.append(dict(ticker=entry["ticker"], file=filename, content_hash=digest,
                            history_start=packet["history_start"],
                            history_start_override=packet["history_start_override"], **packet["quality"]))
    ready = sum(p["ready"] for p in packets)
    manifest = dict(schema="technical-desk-stage0c-manifest-v1", operating_mode="SHADOW_ONLY",
        built_at=built_at, market_data_cutoff=cutoff.isoformat(),
        source_manifest_hash=content_hash(source_manifest), calendar_hash=content_hash(calendar),
        original_rehash_hash=content_hash(original), policy_hash=content_hash(policy),
        preregistration_hash=content_hash(registration), adjustment_evidence_hash=content_hash(adjustments),
        status="READY" if ready == 5 else "PARTIAL", ready_packets=ready, packets=packets,
        models_invoked=False, database_writes=False, network_connections=False,
        stage1_started=False, pilot_status="NOT_STARTED", pilot_cost_observed=None,
        pilot_blockers=["MODEL_NOT_SELECTED", "PRICING_NOT_SUPPLIED"],
        runtime_guard="socket.connect/getaddrinfo, sqlite3.connect and subprocess.Popen prohibited")
    with (args.output / "manifest.json").open("xb") as handle:
        handle.write(canonical(manifest) + b"\n")
    print(json.dumps(dict(status=manifest["status"], ready_packets=ready,
        packets=[{k:p[k] for k in ("ticker", "daily_rows", "weekly_rows", "status", "blockers", "relative_volume_available", "relative_volume_warmup")} for p in packets]), indent=2))


if __name__ == "__main__":
    main()

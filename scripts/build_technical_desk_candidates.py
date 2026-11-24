"""Build local D/W candidate packets from frozen evidence; never invoke agents."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.technical_desk.evidence_capture import canonical, content_hash
from src.technical_desk.packet_data import build_candidate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture", type=Path, required=True)
    parser.add_argument("--calendar", type=Path, required=True)
    parser.add_argument("--original-packets", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cutoff", type=datetime.fromisoformat,
                        help="Frozen aware ISO timestamp for exact offline reproduction")
    args = parser.parse_args()
    manifest = json.loads((args.capture / "manifest.json").read_text())
    original = json.loads((args.original_packets / "rehash.json").read_text())
    calendar = json.loads(args.calendar.read_text())
    starts = {p["ticker"]: p["first_bar"][:10] for p in original["packets"]}
    if manifest["status"] != "CAPTURED" or set(manifest["selected_tickers"]) != set(starts) or len(starts) != 5:
        raise ValueError("FIVE_ORIGINAL_CANDIDATES_REQUIRED")
    entries = manifest["captures"]
    if len(entries) != 5 or {entry["ticker"] for entry in entries} != set(starts):
        raise ValueError("FIVE_DISTINCT_CAPTURE_FILES_REQUIRED")
    if any(entry.get("status") != "CAPTURED" for entry in entries):
        raise ValueError("ALL_SOURCE_CAPTURES_REQUIRED")
    cutoff = args.cutoff or datetime.now(timezone.utc)
    if cutoff.tzinfo is None or cutoff.utcoffset() is None:
        raise ValueError("AWARE_CUTOFF_REQUIRED")
    packets = []
    args.output.mkdir(parents=True, exist_ok=False)
    for entry in entries:
        capture = json.loads((args.capture / entry["file"]).read_text())
        if capture["ticker"] != entry["ticker"]:
            raise ValueError("CAPTURE_MANIFEST_IDENTITY_MISMATCH")
        packet = build_candidate(capture, calendar, start_date=starts[entry["ticker"]],
                                 cutoff=cutoff, source_hash=entry["content_hash"])
        digest = content_hash(packet)
        filename = f"{entry['ticker']}-{digest[:16]}.json"
        with (args.output / filename).open("xb") as handle:
            handle.write(canonical(packet) + b"\n")
        packets.append(dict(ticker=entry["ticker"], file=filename, content_hash=digest, **packet["quality"]))
    summary = dict(schema="technical-desk-data-manifest-v1", cutoff=cutoff.isoformat(),
                   capture_manifest_hash=content_hash(manifest), original_rehash_hash=content_hash(original),
                   calendar_hash=content_hash(calendar), status="NOT_READY", ready_packets=0,
                   packets=packets, models_invoked=False, database_writes=False)
    with (args.output / "manifest.json").open("xb") as handle:
        handle.write(canonical(summary) + b"\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

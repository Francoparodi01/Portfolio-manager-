"""Reproduce stage-0b data readiness from saved captures, without network or SQL."""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime, timedelta
import hashlib
import json
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.audit_technical_desk_stage0b import digest


def summarize(root: Path):
    calendar_path = root / "calendar_audit.json"
    calendar = json.loads(calendar_path.read_text(encoding="utf-8"))
    packet_root = root / "packets"
    capture = json.loads((packet_root / "capture.json").read_text(encoding="utf-8"))
    rehash = json.loads((packet_root / "rehash.json").read_text(encoding="utf-8"))
    checks = {}

    def check(name, condition):
        checks[name] = bool(condition)
        if not condition:
            raise ValueError(f"AUDIT_INTEGRITY_FAILURE:{name}")

    check("read_only_snapshot", capture["identity"]["transaction_read_only"] == "on")
    check("repeatable_read", capture["identity"]["transaction_isolation"] == "repeatable read")
    check("capture_hash", digest(capture) == rehash["capture_hash"])
    selected = sorted({r["ticker"] for r in capture["results"]["portfolio"]})[:5]
    check("five_preregistered_tickers", selected == capture["selected_tickers"] and len(selected) == 5)
    expected = set(calendar["expected_sessions_verified_range"])
    verified = [(r["start"], r["end"]) for r in calendar["verified_ranges"]]
    timezone = ZoneInfo(calendar["timezone"])
    packet_results = []
    for manifest in rehash["packets"]:
        ticker = manifest["ticker"]
        packet = json.loads((packet_root / manifest["file"]).read_text(encoding="utf-8"))
        check(f"{ticker}_packet_hash", digest(packet) == manifest["packet_id"])
        check(f"{ticker}_capture_link", packet["source_capture_hash"] == rehash["capture_hash"])
        rows = packet["observations"]
        original = [r for r in capture["results"]["observations"] if r["ticker"] == ticker]
        check(f"{ticker}_rows_match_capture", rows == original)
        check(f"{ticker}_rehash_count", len(packet["rehash"]) == len(rows) == manifest["observations"])
        check(f"{ticker}_rehash_integrity", all(c["matches"] and c["stored_digest"] == c["computed_digest"] for c in packet["rehash"]))
        dates = [datetime.fromisoformat(r["bar_start"]).astimezone(timezone).date().isoformat() for r in rows]
        check(f"{ticker}_unique_dates", len(set(dates)) == len(dates))
        known = {d for d in dates if any(start <= d <= end for start, end in verified)}
        unknown = sorted(set(dates) - known)
        missing = sorted(expected - known)
        unexpected = sorted(known - expected)
        weekly_expected = defaultdict(set)
        for day in expected:
            parsed = date.fromisoformat(day)
            monday = parsed - timedelta(days=parsed.weekday())
            # A week straddling an uncovered year is not promoted to complete.
            friday = monday + timedelta(days=4)
            if any(start <= monday.isoformat() and friday.isoformat() <= end for start, end in verified):
                weekly_expected[monday.isoformat()].add(day)
        complete_weeks = sorted(w for w, days in weekly_expected.items() if days <= known)
        incomplete_weeks = {w: sorted(days - known) for w, days in sorted(weekly_expected.items()) if not days <= known}
        blockers = []
        lag = sorted(d for d in expected if d > max(dates))
        if lag:
            blockers.append("AS_OF_ARCHIVE_STALE")
        if missing or unexpected:
            blockers.append("CALENDAR_SESSION_COVERAGE_INCOMPLETE")
        if unknown:
            blockers.append("FULL_HISTORY_CALENDAR_UNKNOWN")
        if any(r["volume_unit"] is None for r in rows):
            blockers.append("VOLUME_UNIT_UNKNOWN")
        if any(r["provenance"].get("calendar") is None for r in rows):
            blockers.append("CALENDAR_SNAPSHOT_NOT_LINKED_TO_PACKET")
        # No D/W packet has been built: whole source candles are archived only.
        blockers.append("DAILY_WEEKLY_PACKET_NOT_BUILT_OR_VALIDATED")
        packet_results.append(dict(ticker=ticker, candidate_id=manifest["packet_id"],
            digest_matches=manifest["digest_matches"], observations=len(rows),
            first_session_label=min(dates), latest_closed_session_label=max(dates),
            expected_last_session=calendar["last_expected_complete_session"],
            missing_closed_sessions_after_latest=lag,
            current_calendar_verified_bars=len(known), calendar_unknown_bars=len(unknown),
            unknown_calendar_dates=unknown, expected_verified_sessions=len(expected),
            missing_verified_sessions=missing, unexpected_verified_dates=unexpected,
            complete_calendar_week_groups=len(complete_weeks),
            complete_calendar_week_labels=complete_weeks, incomplete_calendar_weeks=incomplete_weeks,
            weekly_ohlcv_built=False, ready=False, blockers=blockers))
    check("all_observations_accounted_for", sum(p["observations"] for p in packet_results) == len(capture["results"]["observations"]))
    check("same_five_packets", sorted(p["ticker"] for p in packet_results) == selected)
    return dict(schema="technical-desk-stage0b-readiness-v1", cutoff=capture["identity"]["captured_at"],
        status="NOT_READY", required_ready_packets=5, ready_packets=sum(p["ready"] for p in packet_results),
        rehashed_candles=sum(p["digest_matches"] for p in packet_results),
        audit_integrity_checks=checks, calendar_sha256=hashlib.sha256(calendar_path.read_bytes()).hexdigest(),
        calendar_vintage=calendar["vintage_status"], packets=packet_results,
        models_status="NOT_RUN", pilot_observed_cost_usd=None,
        note="Week groups are diagnostic calendar coverage, not weekly OHLCV. Original calendar vintage is unknown for historical replay; a current prospective packet may link a verified current calendar with a cutoff after all evidence was captured.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    result = summarize(args.root)
    target = args.root / "readiness.json"
    target.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({"path": str(target.resolve()), "status": result["status"],
        "ready_packets": result["ready_packets"], "rehashed_candles": result["rehashed_candles"],
        "checks_passed": sum(result["audit_integrity_checks"].values()),
        "by_ticker": [{k: p[k] for k in ("ticker", "observations", "calendar_unknown_bars", "complete_calendar_week_groups", "blockers")} for p in result["packets"]]}, indent=2))


if __name__ == "__main__":
    main()

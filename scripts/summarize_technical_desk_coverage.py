"""Reconcile the saved stage-0 capture without querying any live source."""
from __future__ import annotations

import argparse
from collections import defaultdict
import hashlib
import json
from pathlib import Path


def summarize(directory: Path) -> dict:
    capture = json.loads((directory / "coverage.json").read_text(encoding="utf-8"))
    details = json.loads((directory / "details.json").read_text(encoding="utf-8"))
    r, d = capture["results"], details["results"]
    checks = []

    def check(name, condition):
        if not condition:
            raise AssertionError(name)
        checks.append(name)

    for label, payload in (("coverage", capture), ("details", details)):
        check(label + "_read_only", payload["identity"]["transaction_read_only"] == "on")
        check(label + "_repeatable_read", payload["identity"]["transaction_isolation"] == "repeatable read")
    check("same_owner", capture["owner_token"] == details["owner_token"])
    latest = r["radar_latest"][0]
    check("same_latest_radar_run", {x["run_id"] for x in d["radar_latest_members"]} == {latest["run_id"]})
    obs = r["observations_version_ticker_date"]
    quality = r["observation_quality"][0]
    check("observations_reconcile_daily", sum(x["observations"] for x in obs) == quality["observations"])
    check("observations_reconcile_versions", sum(x["observations"] for x in r["observations_version_ticker"]) == quality["observations"])
    check("eligible_reconcile_daily", sum(x["candidate_observations_now"] for x in obs) == quality["lifecycle_eligible_now"])
    check("metadata_reconciles", sum(x["observations"] for x in d["observation_metadata"]) == quality["observations"])
    versions = defaultdict(lambda: dict(snapshots=0, with_v2=0, with_v3=0, tickers=set(), outcomes={}))
    for row in r["radar_version_ticker_date"]:
        v = versions[row["scoring_version"]]
        v["snapshots"] += row["snapshots"]
        v["with_v2"] += row["with_v2_score"]
        v["with_v3"] += row["with_v3_score"]
        v["tickers"].add(row["ticker"])
    for row in r["radar_versions"]:
        v = versions[row["scoring_version"]]
        v["runs"] = v.get("runs", 0) + row["runs"]
        v["first_session"] = min(v.get("first_session", row["first_session"]), row["first_session"])
        v["last_session"] = max(v.get("last_session", row["last_session"]), row["last_session"])
    for row in r["radar_outcomes_by_version"]:
        v = versions[row["scoring_version"]]
        key = str(row["horizon_sessions"])
        v["outcomes"][key] = v["outcomes"].get(key, 0) + row["outcomes"]
    for v in versions.values():
        v["tickers"] = len(v["tickers"])
    check("version_counts_reconcile", len(versions) == len({x["scoring_version"] for x in r["radar_versions"]}))
    latest_members = [x for x in d["radar_latest_members"] if x["rejection_reason"] != "benchmark_only"]
    check("latest_universe_reconciles", len(latest_members) == latest["universe_count"])
    identities = defaultdict(list)
    for row in obs:
        if row["candidate_observations_now"]:
            identities[row["instrument_id"]].append(row)
    portfolio = []
    for p in r["portfolio_latest"]:
        # No ticker-only identity inference: match explicit stored asset type/currency/market ID.
        rows = [x for key, values in identities.items()
                if key == f"BYMA:{p['asset_type']}:{p['ticker']}:{p['currency']}" for x in values]
        days = {x["candle_date_utc_label"] for x in rows}
        portfolio.append(dict(ticker=p["ticker"], currency=p["currency"],
                              candidate_daily_labels=len(days),
                              first_candle_label=min(days) if days else None,
                              last_candle_label=max(days) if days else None,
                              first_candidate_known_at=min((x["first_candidate_known_at"] for x in rows), default=None)))
    radar_with_obs = [x["ticker"] for x in latest_members
                      if f"BYMA:{x['asset_type']}:{x['ticker']}:ARS" in identities]
    followed = {x["ticker"] for x in r["followed_recent"]}
    observed_symbols = {x["ticker"] for x in obs if x["candidate_observations_now"]}
    linked = d["contextual_link_integrity"][0]
    check("contextual_link_count_reconciles", linked["links"] == sum(int(x["candle_links"]) for x in r["contextual_version_instrument_date"]))
    check("no_orphan_links", linked["orphan_links"] == 0)
    check("no_owner_mismatch", linked["owner_mismatch"] == 0)
    check("no_future_input_links", linked["evidence_after_cutoff"] == 0)
    check("no_open_input_links", linked["not_closed"] == 0)
    summary = dict(captured_at=capture["identity"]["captured_at"],
                   details_captured_at=details["identity"]["captured_at"],
                   portfolio=portfolio, observation_quality=quality,
                   observation_metadata=d["observation_metadata"],
                   radar_versions=dict(versions), latest_version=latest["scoring_version"],
                   radar_total_runs=sum(x["runs"] for x in r["radar_versions"]),
                   radar_total_snapshots=sum(x["snapshots"] for x in versions.values()),
                   radar_with_v2=sum(x["with_v2"] for x in versions.values()),
                   radar_with_v3=sum(x["with_v3"] for x in versions.values()),
                   latest_radar_universe=len(latest_members), latest_radar_with_ars_observations=len(radar_with_obs),
                   latest_radar_observed_tickers=sorted(radar_with_obs),
                   followed_symbols=len(followed), followed_without_observed_symbol=sorted(followed-observed_symbols),
                   contextual_snapshots=sum(x["snapshots"] for x in r["contextual_version_instrument_date"]),
                   contextual_link_integrity=linked,
                   market_candles_bars=sum(x["bars"] for x in r["market_candles_current"]),
                   market_candles_with_available_at=sum(x["with_available_at"] for x in r["market_candles_current"]),
                   market_candles_marked_closed=sum(x["closed"] for x in r["market_candles_current"]),
                   checks=checks)
    summary["artifact_sha256"] = {name: hashlib.sha256((directory/name).read_bytes()).hexdigest()
                                  for name in ("coverage.json", "details.json", "radar_runtime_review.json")}
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    summary = summarize(args.directory)
    target = args.directory / "summary.json"
    target.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"path": str(target.resolve()), "checks_passed": len(summary["checks"]),
                      "latest_radar_universe": summary["latest_radar_universe"],
                      "latest_radar_with_ars_observations": summary["latest_radar_with_ars_observations"]}))


if __name__ == "__main__":
    main()

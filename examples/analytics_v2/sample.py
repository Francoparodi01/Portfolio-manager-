"""Synthetic, hand-checkable evidence; NEVER actual Quantia performance."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.analysis.analytics_v2.models import AnalyticsPolicy, Dataset


def sample():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    cutoff = start + timedelta(days=121, hours=23)
    def evidence(at, source="synthetic"):
        return {"available_at": at, "ingested_at": at, "sealed_at": at, "source_id": source}
    sessions, bars, recs, actions = [], [], [], []
    for i in range(165):
        opened = start + timedelta(days=i, hours=14)
        closed = opened + timedelta(hours=6)
        sid = f"S{i:03}"
        sessions.append({**evidence(start - timedelta(days=1)), "session_id": sid, "open_at": opened, "close_at": closed})
        for j, ticker in enumerate(("AAA", "BBB", "CCC", "BENCH")):
            value = 100 + i * (.8 if ticker == "AAA" else -.3 if ticker == "BBB" else .2)
            bars.append({**evidence(closed + timedelta(minutes=1)), "instrument_id": ticker, "session_id": sid,
                         "series_id": "SYNTHETIC_ADJUSTED",
                         "revision": 0, "open": value, "close": value + .1 * (j + 1), "currency": "ARS", "adjustment_basis": "synthetic_split_adjusted"})
    for i in range(36):
        at = start + timedelta(days=i * 3, hours=21)
        ticker = ("AAA", "BBB", "CCC")[i % 3]
        rec = {**evidence(at), "recommendation_id": f"R{i:03}", "account_id": "SYNTHETIC",
               "source_module": "CORE", "cohort": "BOT", "instrument_id": ticker, "ticker": ticker,
               "direction": "BUY", "score": float(3 - i % 3), "decision_as_of": at,
               "expiry_at": at + timedelta(days=8), "benchmark_id": "BENCH", "price_series_id": "SYNTHETIC_ADJUSTED"}
        recs.append(rec)
        actions.append({**evidence(at), "recommendation_id": rec["recommendation_id"], "instrument_id": ticker,
                        "decision_as_of": at, "action": ("FOLLOW", "IGNORE", "CONTRARY")[i % 3]})
        if i % 4 == 0:
            later = at + timedelta(days=1)
            recs.append({**rec, **evidence(later), "recommendation_id": f"REPEAT{i}", "decision_as_of": later})
    for module, cohort in (("RADAR", "OPERABLE"), ("RADAR", "WATCHLIST"), ("SWAP", "PAIRED")):
        at = start + timedelta(days=4, hours=21)
        base = {**evidence(at), "recommendation_id": f"{module}-{cohort}", "account_id": "SYNTHETIC",
                "source_module": module, "cohort": cohort, "instrument_id": "AAA", "ticker": "AAA",
                "direction": "BUY", "score": 1., "decision_as_of": at, "benchmark_id": "BENCH", "price_series_id": "SYNTHETIC_ADJUSTED"}
        if module == "SWAP":
            base["original_instrument_id"] = "BBB"
        elif cohort == "WATCHLIST":
            base["instrument_id"] = base["ticker"] = "BBB"
        recs.append(base)
        recs.append({**base, **evidence(at + timedelta(days=1)), "recommendation_id": base["recommendation_id"] + "-REPEAT", "decision_as_of": at + timedelta(days=1)})
    at = start + timedelta(days=120, hours=21)
    recs.append({**recs[0], **evidence(at), "recommendation_id": "PENDING40", "decision_as_of": at, "expiry_at": None})
    recs.append({**recs[0], "recommendation_id": "AMBIGUOUS", "source_module": "MANUAL", "cohort": "MANUAL",
                 "ambiguous": True, "ambiguity_reason": "AMBIGUOUS_SAME_DAY"})
    fills = []
    for index, (side, price, reference) in enumerate((("BUY", 100.5, 100.), ("SELL", 120., 121.))):
        at = start + timedelta(days=index + 1, hours=15)
        fills.append({**evidence(at), "broker_fill_id": f"F{index}", "account_id": "SYNTHETIC",
                      "instrument_id": "AAA", "currency": "ARS", "recommendation_id": "R000",
                      "quantity": 1, "side": side, "fill_price": price, "reference_price": reference,
                      "reference_kind": "EXECUTION_MARKET",
                      "fees": .5, "taxes": 0., "other_costs": 0., "fill_at": at})
    end = start + timedelta(days=3)
    points = [{**evidence(at), "account_id": "SYNTHETIC", "currency": "ARS", "at": at, "nav": nav,
               "gross_notional": exposure, "net_notional": exposure, "sizing_policy": "synthetic-one-share",
               "kind": "ACTUAL", "flow_adjusted_return": ret, "reconciled": True}
              | {"net_external_flow": 0., "flow_timing": "NONE"}
              for at, nav, exposure, ret in ((start, 1000., 0., None), (start + timedelta(days=1), 1000., 0., 0.), (start + timedelta(days=2), 1009., 110., .009), (end, 1018.5, 0., 1018.5/1009-1))]
    economic = {**evidence(end), "account_id": "SYNTHETIC", "currency": "ARS", "start": start, "end": end,
                "beginning_nav": 1000., "ending_nav": 1018.5, "net_external_flows": 0., "realized_pnl": 19.5,
                "unrealized_pnl_change": 0., "fees": 1., "taxes": 0., "financing": 0., "other_costs": 0.,
                "average_capital_deployed": 100.5, "coverage_confirmed": True, "evidence_ids": ("F0", "F1", "synthetic-nav-flow-attestation")}
    return Dataset(label="SYNTHETIC EXAMPLE - NOT QUANTIA LIVE", recommendations=recs, sessions=sessions, bars=bars,
                   human_actions=actions, executions=fills, economic_periods=[economic], nav_points=points), AnalyticsPolicy(
                       experiment_id="synthetic-analytics-v2", evaluated_as_of=cutoff, bootstrap_resamples=200)


if __name__ == "__main__":
    folder = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "outputs" / "analytics_example_input"
    folder.mkdir(parents=True, exist_ok=True)
    data, policy = sample()
    (folder / "input.json").write_text(data.model_dump_json(indent=2), encoding="utf-8")
    (folder / "policy.json").write_text(policy.model_dump_json(indent=2), encoding="utf-8")
    print(folder)

"""Attach E2 shadow evidence to a recorded E1 capture and compare productive fields."""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime
import hashlib
import json
from pathlib import Path
from typing import Any

from src.analysis.contextual_market import build_contextual_snapshot
from src.collector.cocos_history import candles_to_frame


def _hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                         ensure_ascii=True, allow_nan=False).encode()).hexdigest()


def _productive(capture: dict) -> dict:
    plan = capture["plan"]
    signals = [{
        "ticker": signal.get("ticker"), "decision": signal.get("decision"),
        "final_score": signal.get("final_score"), "conviction": signal.get("conviction"),
    } for signal in capture.get("signals", [])]
    order_fields = ("ticker", "side", "action", "amount_ars", "quantity_est", "reference_price")
    orders = {
        name: [{key: order.get(key) for key in order_fields} for order in plan.get(name, [])]
        for name in ("sell_orders", "buy_orders", "blocked_orders")
    }
    return {
        "signals": signals,
        "decisions": plan.get("decisions", []),
        "orders": orders,
        "cash": {key: plan.get(key) for key in (
            "cash_before", "gross_sell_ars", "net_sell_ars", "gross_buy_ars",
            "fee_sell_ars", "fee_buy_ars", "cash_after",
        )},
    }


def _frame_for(rows: list[dict], cutoff: datetime):
    eligible = [row for row in rows if datetime.fromisoformat(str(row["ts"])) <= cutoff
                and datetime.fromisoformat(str(row["scraped_at"])) <= cutoff]
    counts: dict[str, int] = {}
    for row in eligible:
        counts[row["long_ticker"]] = counts.get(row["long_ticker"], 0) + 1
    if not counts:
        return None
    symbol = sorted(counts, key=lambda value: (-counts[value], value))[0]
    selected = []
    for row in eligible:
        if row["long_ticker"] != symbol:
            continue
        selected.append({
            **row, "bar_start": None, "bar_end": None, "available_at": None,
            "is_closed": None, "volume_unit": None, "calendar": None,
            "calendar_validation": None, "adjustment_policy": None,
            "depositary_ratio": None,
        })
    return candles_to_frame(selected)


def compare(recorded: dict) -> dict:
    captures = [item["payload"] for item in recorded["captures"] if item["payload"].get("signals")]
    if not captures:
        raise ValueError("FULL_CONTEXT_NOT_AVAILABLE")
    baseline = deepcopy(captures[0])
    shadow = deepcopy(baseline)
    cutoff = datetime.fromisoformat(str(baseline["decision_at"]))
    benchmark = _frame_for(recorded.get("candles", {}).get("SPY", []), cutoff)
    snapshot_status = {}
    for signal in shadow["signals"]:
        ticker = signal["ticker"]
        asset = _frame_for(recorded.get("candles", {}).get(ticker, []), cutoff)
        if asset is None:
            snapshot_status[ticker] = "NO_SERIES_KNOWN_AT_CUTOFF"
            continue
        snapshot = build_contextual_snapshot(
            asset, cutoff=cutoff, signal_action=signal.get("decision", "UNKNOWN"),
            benchmarks={"SPY": benchmark} if benchmark is not None else {},
        )
        signal["contextual_market_shadow"] = snapshot.to_dict()
        snapshot_status[ticker] = {
            "snapshot_id": snapshot.snapshot_id,
            "context_confidence": snapshot.components["context_confidence"],
            "missingness": snapshot.missingness,
        }
    baseline_productive = _productive(baseline)
    shadow_productive = _productive(shadow)
    return {
        "scope": "RECORDED_E1_PLAN_WITH_E2_SHADOW_ATTACHMENT_NOT_ECONOMIC_REPLAY",
        "plan_id": recorded["plan"]["id"],
        "run_id": recorded["plan"]["run_id"],
        "owner": recorded["plan"]["owner_chat_id"],
        "equal": {
            "scores": [item["final_score"] for item in baseline_productive["signals"]] == [item["final_score"] for item in shadow_productive["signals"]],
            "signals": [item["decision"] for item in baseline_productive["signals"]] == [item["decision"] for item in shadow_productive["signals"]],
            "orders": baseline_productive["orders"] == shadow_productive["orders"],
            "quantities": [order["quantity_est"] for values in baseline_productive["orders"].values() for order in values] == [order["quantity_est"] for values in shadow_productive["orders"].values() for order in values],
            "cash": baseline_productive["cash"] == shadow_productive["cash"],
            "decisions": baseline_productive["decisions"] == shadow_productive["decisions"],
        },
        "baseline_productive_hash": _hash(baseline_productive),
        "shadow_productive_hash": _hash(shadow_productive),
        "snapshot_status": snapshot_status,
        "baseline_capture_unchanged": captures[0] == baseline,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("evidence", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    raw = args.evidence.read_bytes()
    result = compare(json.loads(raw))
    result["evidence_sha256"] = hashlib.sha256(raw).hexdigest()
    payload = json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload + "\n", encoding="utf-8")
    print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

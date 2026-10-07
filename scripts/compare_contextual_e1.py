"""Compare E1 authority to a recorded proposal without repricing or modifying it."""
import argparse
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.analysis.contextual_contracts import evaluate_authority


def compare(recorded):
    plan = recorded["plan"]
    captures = [c["payload"] for c in recorded["captures"] if c["payload"].get("signals")]
    if not captures:
        raise ValueError("FULL_CONTEXT_NOT_AVAILABLE")
    capture = captures[0]
    if capture.get("owner") != plan["owner_chat_id"] or capture["plan_id"] != str(plan["id"]):
        raise ValueError("CAPTURE_IDENTITY_MISMATCH")
    if capture.get("run_id") not in (None, str(plan["run_id"])):
        raise ValueError("CAPTURE_RUN_MISMATCH")
    before = deepcopy(capture)
    signals = {s["ticker"]: s for s in capture["signals"]}
    comparisons = []
    for decision in capture["plan"]["decisions"]:
        signal = signals.get(decision["ticker"], {})
        shadow = evaluate_authority(
            signal_action=signal.get("decision", "UNKNOWN"),
            asset_view=signal.get("asset_view", "UNKNOWN"),
            current_weight=decision["current_weight"],
            target_weight=decision["theoretical_target_weight"],
            data_quality=signal.get("data_quality", {}), ticker=decision["ticker"],
        )
        comparisons.append({"ticker": decision["ticker"], "baseline_action": decision["action"], "shadow": shadow})
    assert capture == before
    return {
        "scope": "ELIGIBILITY_COMPARISON_NOT_ECONOMIC_REPLAY", "plan_id": plan["id"], "run_id": plan["run_id"],
        "plan_unchanged": True, "capture_run_id_recorded": capture.get("run_id") is not None,
        "comparisons": comparisons,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence", type=Path)
    args = parser.parse_args()
    raw = args.evidence.read_bytes()
    output = compare(json.loads(raw))
    output["evidence_sha256"] = hashlib.sha256(raw).hexdigest()
    print(json.dumps(output, indent=2, allow_nan=False))

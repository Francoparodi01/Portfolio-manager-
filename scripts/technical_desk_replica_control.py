"""Deterministic synthetic control; no network, database, or market hypotheses."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path

from src.technical_desk.compact_dry_run import context_from_market_view
from src.technical_desk.elliott_control import evaluate_series
from src.technical_desk.evidence_capture import canonical, content_hash
from src.technical_desk.features import compute_features
from src.technical_desk.role_contracts import role_output_schema


ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = ROOT / "docs/technical-desk-stage1a-policy.json"
POLICY_SHA256 = "b0b57ce27eb736fcb3eb267f3c1b5a0702f1f464e1bcdd55deb6b70e6c79a37e"
# Fixed before constructing any bars or calculating features. Freeze this object
# and its content_hash in the experiment registration before any model call.
CONTROL_SPEC = {
    "version": "technical-desk-synthetic-impulse-control-v1",
    "instrument_id": "SYNTHETIC:FIXTURE:IMPULSE:ARS",
    "purpose": "SYNTHETIC_METHOD_CONTROL_NOT_MARKET_EVIDENCE",
    "seed": None,
    "generation": "DETERMINISTIC_PIECEWISE_LINEAR_CLOSE_NEAREST_TICK_HALF_EVEN",
    "tick_size": "0.01",
    "start": "2026-01-05T13:30:00+00:00",
    "daily_bar_count": 70,
    "synthetic_sessions": "MONDAY_TO_FRIDAY_NO_HOLIDAY_CLAIM",
    "daily_duration_hours": 7,
    "knots": [[0, "110"], [5, "100"], [15, "120"], [25, "110"],
              [35, "160"], [45, "140"], [55, "180"], [69, "178"]],
    "open": "SAME_AS_CLOSE",
    "high_low": "CLOSE_PLUS_OR_MINUS_ONE_TICK",
    "volume": "NOT_SUPPLIED_NOT_USED",
    "weekly": "EXACT_AGGREGATION_OF_FIVE_SYNTHETIC_WEEKDAY_BARS",
    "scales": [2, 5],
    "feature_policy_sha256": POLICY_SHA256,
    "expected_daily_indices": [5, 15, 25, 35, 45, 55],
    "expected_daily_extremes": ["99.99", "120.01", "109.99", "160.01", "139.99", "180.01"],
    "expected_family": "IMPULSE",
    "expected_direction": "BULLISH",
    "weekly_expected": "NO_COMPLETE_SIX_ENDPOINT_CANDIDATE",
    "horizon_unit_note": "ROLE_CONTRACT_RETAINS_BYMA_TRADING_SESSIONS_NO_FORECAST_IS_TESTED",
    "success": "VALID_PRIMARY_D_IMPULSE_BULLISH_WITH_EXACT_ORDERED_ORIGIN_AND_REMAINING_FIVE_IDS_FROM_EITHER_SCALE",
}


def frozen_policy():
    raw = POLICY_PATH.read_bytes()
    if hashlib.sha256(raw).hexdigest() != POLICY_SHA256:
        raise ValueError("FROZEN_FEATURE_POLICY_CHANGED")
    return json.loads(raw)


def synthetic_bars():
    """Return full precision D and aggregated W bars from the fixed specification."""
    tick = Decimal(CONTROL_SPEC["tick_size"])
    moment = datetime.fromisoformat(CONTROL_SPEC["start"])
    knots, daily = CONTROL_SPEC["knots"], []
    for index in range(CONTROL_SPEC["daily_bar_count"]):
        while moment.weekday() > 4:
            moment += timedelta(days=1)
        left, right = next((a, b) for a, b in zip(knots, knots[1:]) if a[0] <= index <= b[0])
        close = (Decimal(left[1]) + (Decimal(right[1])-Decimal(left[1])) *
                 Decimal(index-left[0])/Decimal(right[0]-left[0])).quantize(tick)
        daily.append(dict(time=moment.isoformat(), closed_at=(moment+timedelta(hours=7)).isoformat(),
            session_date=moment.date().isoformat(), bar_start=moment.isoformat(),
            open=close, high=close+tick, low=close-tick, close=close, volume=None))
        moment += timedelta(days=1)
    weekly = []
    for index in range(0, len(daily), 5):
        group = daily[index:index+5]
        if len(group) != 5:
            raise ValueError("PARTIAL_SYNTHETIC_WEEK_PROHIBITED")
        weekly.append(dict(time=group[0]["time"], closed_at=group[-1]["closed_at"],
            monday=group[0]["session_date"], last_session=group[-1]["session_date"],
            open=group[0]["open"], high=max(row["high"] for row in group),
            low=min(row["low"] for row in group), close=group[-1]["close"], volume=None))
    return {"D": daily, "W": weekly}


def _replace_ids(value, mapping):
    if isinstance(value, dict):
        return {key: _replace_ids(item, mapping) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_replace_ids(item, mapping) for item in value]
    return mapping.get(value, value) if isinstance(value, str) else value


def build_control():
    """Return (market-shaped synthetic view, PacketContext, expected-count metadata)."""
    policy, bars = frozen_policy(), synthetic_bars()
    features = dict(price_tick=CONTROL_SPEC["tick_size"], atr_by_timeframe={}, elliott_scales={})
    registry, expected = {}, dict(spec=deepcopy(CONTROL_SPEC), spec_sha256=content_hash(CONTROL_SPEC),
        evidence_class="SYNTHETIC_CONTROL_ONLY", market_hypothesis_allowed=False, daily={})
    for tf, rows in bars.items():
        registry[tf], features["elliott_scales"][tf] = [], {}
        for scale in CONTROL_SPEC["scales"]:
            scale_policy = deepcopy(policy)
            scale_policy["pivots"].update(left_bars=scale, right_bars=scale,
                scale_by_timeframe={"D": f"{scale}L{scale}R_D", "W": f"{scale}L{scale}R_W"})
            computed = compute_features(rows, timeframe=tf, tick_size=CONTROL_SPEC["tick_size"], policy=scale_policy)
            mapping = {p["id"]: f"P{scale}:{tf}:"+p["id"] for p in computed["confirmed_pivots"]}
            pivots = _replace_ids(computed["confirmed_pivots"], mapping)
            registry[tf].extend(pivots)
            summary = evaluate_series(rows, timeframe=tf, tick_size=CONTROL_SPEC["tick_size"],
                policy=scale_policy, pivot_window=scale, include_candidates=True)
            features["elliott_scales"][tf][f"P{scale}"] = _replace_ids(summary, mapping)
            if scale == 2:
                features[tf] = dict(latest=_replace_ids(computed["snapshots"][-1], mapping),
                    missing=computed["missing_features"], swings=_replace_ids(computed["swings"], mapping))
                features["atr_by_timeframe"][tf] = computed["snapshots"][-1]["indicators"]["atr"]["value"]
                if features["atr_by_timeframe"][tf] is None:
                    raise ValueError("SYNTHETIC_ATR_REQUIRES_FULL_WINDOW")
            if tf == "D":
                if ([p["index"] for p in pivots] != CONTROL_SPEC["expected_daily_indices"] or
                        [str(p["price"]) for p in pivots] != CONTROL_SPEC["expected_daily_extremes"] or
                        summary["candidate_count"] != 1 or summary["status_counts"]["RULES_COMPATIBLE"] != 1):
                    raise ValueError("SYNTHETIC_GEOMETRY_DOES_NOT_MATCH_PREREGISTERED_CONTROL")
                expected["daily"][f"P{scale}"] = dict(direction="BULLISH", family="IMPULSE",
                    pivot_ids=[p["id"] for p in pivots], pivots=pivots)
            elif summary["candidate_count"]:
                raise ValueError("SYNTHETIC_WEEKLY_ABSENCE_CHANGED")
        if tf == "W":
            features[tf]["missing"]["ordinary_impulse"] = "FEWER_THAN_SIX_CONFIRMED_ALTERNATING_PIVOTS"
    payload = dict(spec=CONTROL_SPEC, bars=bars)
    digest = content_hash(payload)
    cutoff = bars["D"][-1]["closed_at"]
    view = dict(instrument_id=CONTROL_SPEC["instrument_id"], packet_id="synthetic-impulse-"+digest,
        packet_digest=digest, cuts=dict(cutoff=cutoff, history_start=bars["D"][0]["session_date"],
        market_data_cutoff=cutoff), ohlcv=bars, features=features, pivots=registry, output_schema=role_output_schema())
    # Decimal serialization is lossless, and no synthetic label changes the role contract.
    view, expected = json.loads(canonical(view)), json.loads(canonical(expected))
    expected.update(packet_digest=digest, view_sha256=content_hash(view), timeframe="D")
    return view, context_from_market_view(view), expected


def identifies_expected_count(parsed, expected):
    """Score only already-validated output; accepting either scale is preregistered."""
    if not parsed:
        return False
    result = next((r for r in parsed.get("results", []) if r.get("timeframe") == "D"), None)
    if not result or result.get("state") != "VALID":
        return False
    for scenario in result.get("scenarios", []):
        if (scenario.get("role"), scenario.get("family"), scenario.get("direction")) != ("PRIMARY", "IMPULSE", "BULLISH"):
            continue
        causal, structures = scenario.get("causal_anchors", []), scenario.get("structure_anchors", [])
        for match in expected["daily"].values():
            ids = match["pivot_ids"]
            if (causal == [dict(role="origin", anchor_id=ids[0])] and
                    len(structures) == 5 and {a["anchor_id"] for a in structures} == set(ids[1:])):
                return True
    return False


def amd_compatible_dates(view):
    """Return at most three frozen AMD compatible windows, with causal dates."""
    lookup = {p["id"]: p for p in view["pivots"]["D"]}
    last = view["ohlcv"]["D"][-1]["bar_start"]
    result = []
    for scale, source in view["features"]["elliott_scales"]["D"].items():
        for candidate in source["candidates"]:
            if candidate["status"] != "RULES_COMPATIBLE":
                continue
            pivots = [lookup[identity] for identity in candidate["pivot_ids"]]
            result.append(dict(scale=scale, start=pivots[0]["pivot_time"], end=pivots[-1]["pivot_time"],
                confirmed_at=pivots[-1]["confirmed_at"], last_daily_bar=last,
                ends_on_last_daily_bar=pivots[-1]["pivot_time"] == last,
                direction="BULLISH" if pivots[0]["kind"] == "LOW" else "BEARISH",
                pivots=[{k: p[k] for k in ("id", "kind", "price", "pivot_time", "confirmed_at")} for p in pivots]))
    return result

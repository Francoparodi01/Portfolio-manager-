from copy import deepcopy
from decimal import Decimal

import pytest

from src.technical_desk.elliott_control import (
    calibrate, compare_control, evaluate_series, extract_pivots,
    random_walk, replica_seed, summarize_candidates,
)
from src.technical_desk.elliott_rules import validate_impulse
from src.technical_desk.evidence_capture import canonical
from src.technical_desk.features import compute_features
from tests.test_technical_desk_features import POLICY, bars


@pytest.mark.parametrize("window", [2, 5])
@pytest.mark.parametrize("timeframe", ["D", "W"])
def test_fast_detector_matches_complete_features_and_rebased_validator(window, timeframe):
    policy = deepcopy(POLICY)
    policy["pivots"].update(left_bars=window, right_bars=window)
    source = bars(100)
    source = random_walk(source, calibration=calibrate(source), tick_size=Decimal("0.01"), seed=37)
    fast = extract_pivots(source, timeframe=timeframe, policy=policy, pivot_window=window)
    full = compute_features(source, timeframe=timeframe, tick_size=Decimal("0.01"), policy=policy)
    for key in ("confirmed_pivots", "active_alternating_pivots"):
        assert canonical(fast[key]) == canonical(full[key])
    result = evaluate_series(source, timeframe=timeframe, policy=policy, tick_size=Decimal("0.01"),
                             pivot_window=window, include_candidates=True)
    chain = fast["active_alternating_pivots"]
    assert result["candidate_count"] == max(0, len(chain) - 5)
    for candidate in result["candidates"]:
        i = candidate["candidate_index"]
        direct = validate_impulse(chain[i:i+6], source, tick_size=Decimal("0.01"), scale=timeframe)
        assert direct["status"] == candidate["status"]
        assert [(r["rule_id"], r["status"]) for r in direct["rules"]] == [(r["rule_id"], r["status"]) for r in candidate["rules"]]


def test_random_walk_is_reproducible_and_preserves_length_time_and_calibrated_sigma():
    source = bars(65)
    calibration = calibrate(source)
    seed = replica_seed("AMD", "D", 0)
    first = random_walk(source, calibration=calibration, tick_size=Decimal("0.01"), seed=seed, return_diagnostics=True)
    second = random_walk(source, calibration=calibration, tick_size=Decimal("0.01"), seed=seed, return_diagnostics=True)
    assert first == second
    assert seed != replica_seed("AMD", "W", 0) != replica_seed("AMD", "W", 1)
    assert len(first["bars"]) == len(source)
    assert [(r["time"], r["closed_at"]) for r in first["bars"]] == [(r["time"], r["closed_at"]) for r in source]
    assert first["volatility"]["pre_tick_sigma"] == pytest.approx(float(calibration["log_return_sigma"]), rel=1e-12)
    assert abs(first["volatility"]["pre_tick_mean"]) < 1e-12
    assert first["volatility"]["absolute_relative_error"] < .01
    assert all(r["low"] <= min(r["open"], r["close"]) <= max(r["open"], r["close"]) <= r["high"] for r in first["bars"])


def test_zero_volatility_has_no_invented_candidates_or_ratio():
    source = bars(15)
    for row in source:
        for key in ("open", "high", "low", "close"):
            row[key] = Decimal("100")
    walk = random_walk(source, calibration=calibrate(source), tick_size=1, seed=4)
    summary = evaluate_series(walk, tick_size=1, timeframe="D", policy=POLICY)
    assert summary["candidate_count"] == 0
    assert all(value == 0 for value in summary["status_counts"].values())
    control = compare_control(summary, [summary])
    assert control["observed"]["rate"] is None and control["control"]["pooled_rate"] is None
    assert control["rate_ratio"] is None and control["upper_empirical_p"]["value"] is None


def test_nonoverlap_rejects_shared_endpoint_and_keeps_compatible_cohort_separate():
    candidates = [dict(candidate_index=i, start_index=i, end_index=i+5,
                       status="RULES_COMPATIBLE" if i in (1, 6, 7) else "REJECTED") for i in range(10)]
    summary = summarize_candidates(candidates)
    assert summary["nonoverlap_all"]["candidate_indices"] == [0, 6]
    assert summary["nonoverlap_compatible"]["candidate_indices"] == [1, 7]
    assert summary["status_counts"] == dict(RULES_COMPATIBLE=3, REJECTED=7, INCOMPLETE=0, NOT_EVALUABLE=0)


def test_control_rate_pools_windows_instead_of_averaging_walk_percentages():
    def sample(n, compatible):
        return dict(candidate_count=n, status_counts={"RULES_COMPATIBLE": compatible},
                    nonoverlap_compatible={"count": int(compatible > 0)})
    result = compare_control(sample(10, 2), [sample(1, 1), sample(9, 0), sample(0, 0)])
    assert result["control"]["pooled_rate"] == .1
    assert result["control"]["eligible_walks"] == 2
    assert result["control"]["zero_candidate_walks"] == 1
    assert result["rate_ratio"] == 2
    assert result["upper_empirical_p"]["value"] == pytest.approx(2/3)

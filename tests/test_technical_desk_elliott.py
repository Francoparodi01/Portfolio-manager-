from copy import deepcopy
from datetime import date, timedelta
from decimal import Decimal

import pytest

from src.technical_desk.elliott_rules import validate_abc, validate_impulse


def candidate(values=(100, 120, 110, 145, 125, 155), *, bearish=False):
    values = [Decimal(str(value)) for value in values]
    if bearish:
        values = [Decimal(300) - value for value in values]
    bars = []
    for index in range(2 * len(values) + 1):
        value = values[min(index // 2, len(values) - 1)]
        if index % 2 and index // 2 + 1 < len(values):
            value = (value + values[index // 2 + 1]) / 2
        bars.append(dict(time=str(date(2026, 1, 1) + timedelta(days=index)),
                         open=value, high=value, low=value, close=value))
    pivots = [dict(id=f"p{number}", index=number * 2,
                   kind=("HIGH" if bearish else "LOW") if number % 2 == 0 else ("LOW" if bearish else "HIGH"),
                   price=value, pivot_time=bars[number * 2]["time"],
                   confirmed_at=bars[number * 2 + 1]["time"], status="CONFIRMED")
              for number, value in enumerate(values)]
    return pivots, bars


def rule(result, name):
    return next(item for item in result["rules"] if item["rule_id"] == name)


@pytest.mark.parametrize("scale", ["D", "W"])
@pytest.mark.parametrize("bearish", [False, True])
def test_valid_impulse_has_rule_results_and_no_authority(scale, bearish):
    pivots, bars = candidate(bearish=bearish)
    result = validate_impulse(pivots, bars, tick_size=Decimal(1), scale=scale)
    assert result["status"] == "RULES_COMPATIBLE"
    assert all(item["status"] == "RULES_COMPATIBLE" for item in result["rules"])
    assert result["mode"] == "SHADOW_ONLY"
    assert result["affects_analysis"] is result["affects_execution"] is False


@pytest.mark.parametrize("wave2", [100, 99])
@pytest.mark.parametrize("bearish", [False, True])
def test_wave2_exactly_100_percent_and_more_rejected(wave2, bearish):
    pivots, bars = candidate((100, 120, wave2, 145, 125, 155), bearish=bearish)
    result = validate_impulse(pivots, bars, tick_size=1)
    assert result["status"] == "REJECTED"
    assert rule(result, "wave2_less_than_100_percent")["status"] == "REJECTED"


@pytest.mark.parametrize("wave4", [120, 119])
@pytest.mark.parametrize("bearish", [False, True])
def test_wave4_touch_and_overlap_rejected(wave4, bearish):
    pivots, bars = candidate((100, 120, 110, 145, wave4, 155), bearish=bearish)
    result = validate_impulse(pivots, bars, tick_size=1)
    assert result["status"] == "REJECTED"
    assert rule(result, "wave4_no_wave1_overlap")["status"] == "REJECTED"


@pytest.mark.parametrize("bearish", [False, True])
def test_wave3_strictly_shortest_rejected(bearish):
    pivots, bars = candidate((100, 130, 120, 140, 135, 165), bearish=bearish)
    result = validate_impulse(pivots, bars, tick_size=1)
    assert result["status"] == "REJECTED"
    assert rule(result, "wave3_not_shortest")["status"] == "REJECTED"


@pytest.mark.parametrize("length", [30, 29])
def test_wave3_equal_or_within_fixed_tick_is_not_uniquely_shortest(length):
    pivots, bars = candidate((100, 130, 120, 120 + length, 135, 165))
    assert validate_impulse(pivots, bars, tick_size=1)["status"] == "RULES_COMPATIBLE"


def test_all_bars_inside_each_segment_are_checked():
    pivots, bars = candidate()
    bars[1]["high"] = Decimal(123)
    result = validate_impulse(pivots, bars, tick_size=1)
    assert result["status"] == "REJECTED"
    assert rule(result, "wave_1_segment_extremes")["status"] == "REJECTED"


def test_extremes_after_last_endpoint_do_not_affect_wave_geometry():
    pivots, bars = candidate()
    before = validate_impulse(pivots, bars, tick_size=1)
    bars[-1].update(high=Decimal(10000), low=Decimal(1))
    assert validate_impulse(pivots, bars, tick_size=1) == before


def test_five_endpoints_are_incomplete_and_wave3_is_not_checked_early():
    pivots, bars = candidate()
    result = validate_impulse(pivots[:5], bars, tick_size=1)
    assert result["status"] == "INCOMPLETE"
    assert rule(result, "wave3_not_shortest")["status"] == "INCOMPLETE"


def test_open_endpoint_does_not_count_as_completed_wave():
    pivots, bars = candidate()
    pivots[-1].update(status="CANDIDATE", confirmed_at=None)
    result = validate_impulse(pivots, bars, tick_size=1)
    assert result["status"] == "INCOMPLETE"
    assert rule(result, "completed_wave_rules")["status"] == "INCOMPLETE"


def test_confirmation_must_be_later_than_pivot_and_available_now():
    pivots, bars = candidate()
    pivots[-1]["confirmed_at"] = pivots[-1]["pivot_time"]
    assert validate_impulse(pivots, bars, tick_size=1)["status"] == "NOT_EVALUABLE"
    pivots[-1]["confirmed_at"] = "2099-01-01"
    assert validate_impulse(pivots, bars, tick_size=1)["status"] == "NOT_EVALUABLE"


@pytest.mark.parametrize("mutation", ["reversed", "same_kind", "bad_index", "bar_time", "duplicate_id"])
def test_invalid_endpoint_contract_or_geometry_is_never_compatible(mutation):
    pivots, bars = candidate()
    if mutation == "reversed":
        pivots[1], pivots[2] = pivots[2], pivots[1]
    elif mutation == "same_kind":
        pivots[1]["kind"] = pivots[0]["kind"]
    elif mutation == "bad_index":
        pivots[1]["index"] = 999
    elif mutation == "bar_time":
        pivots[1]["pivot_time"] = "2000-01-01"
    else:
        pivots[1]["id"] = pivots[0]["id"]
    assert validate_impulse(pivots, bars, tick_size=1)["status"] in ("REJECTED", "NOT_EVALUABLE")


@pytest.mark.parametrize("tick,scale,tolerance", [(0,"D",1),(-1,"D",1),(1.0,"D",1),
    ("NaN","D",1),(1,"M",1),(1,"D",2),(1,"D",True)])
def test_unknown_tick_scale_or_changed_policy_is_not_evaluable(tick, scale, tolerance):
    pivots, bars = candidate()
    assert validate_impulse(pivots, bars, tick_size=tick, scale=scale,
                            tolerance_ticks=tolerance)["status"] == "NOT_EVALUABLE"


def test_diagonals_explicitly_unsupported():
    pivots, bars = candidate()
    result = validate_impulse(pivots, bars, tick_size=1, pattern="DIAGONAL")
    assert result["status"] == "NOT_EVALUABLE"
    assert result["rules"][0]["reason"] == "UNSUPPORTED_PATTERN"


def test_abc_only_checks_basic_geometry_without_impulse_restrictions():
    pivots, bars = candidate((100, 120, 90, 130))
    result = validate_abc(pivots, bars, tick_size=1)
    assert result["status"] == "RULES_COMPATIBLE"
    assert not any(item["rule_id"].startswith("wave2_less") for item in result["rules"])
    assert validate_abc(pivots[:3], bars, tick_size=1)["status"] == "INCOMPLETE"


def test_abc_rejects_broken_chronology():
    pivots, bars = candidate((100, 120, 110, 125))
    pivots[1], pivots[2] = pivots[2], pivots[1]
    assert validate_abc(pivots, bars, tick_size=1)["status"] == "REJECTED"


def test_empty_candidate_is_incomplete_without_inventing_endpoints():
    _, bars = candidate()
    assert validate_impulse([], bars, tick_size=1)["status"] == "INCOMPLETE"


def test_input_is_not_mutated():
    pivots, bars = candidate()
    original = deepcopy((pivots, bars))
    validate_impulse(pivots, bars, tick_size=1)
    assert (pivots, bars) == original


def test_confirmation_can_be_known_at_final_bar_close_after_its_start():
    pivots, bars = candidate()
    bars = bars[:pivots[-1]["index"] + 1]
    bars[-1]["closed_at"] = "2026-01-12T22:00:00Z"
    pivots[-1]["confirmed_at"] = bars[-1]["closed_at"]
    assert validate_impulse(pivots, bars, tick_size=1)["status"] == "RULES_COMPATIBLE"


@pytest.mark.parametrize("wave,values,bar_index,boundary", [
    (2,(100,120,"100.5",145,125,155),4,100),
    (4,(100,120,110,145,"120.5",155),8,120),
])
def test_endpoint_tolerance_does_not_hide_intrabar_origin_or_overlap_breach(wave,values,bar_index,boundary):
    pivots,bars = candidate(values)
    bars[bar_index]["low"] = Decimal(boundary)
    result = validate_impulse(pivots,bars,tick_size=1)
    name = "wave2_less_than_100_percent" if wave == 2 else "wave4_no_wave1_overlap"
    assert rule(result,name)["status"] == "REJECTED"

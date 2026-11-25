"""Outcome resolver: sessions not calendar days, MISSING_BAR, ambiguity, ordering, targets, adjustments, W bars."""
from datetime import date, datetime, time, timedelta
from decimal import Decimal
import json
from zoneinfo import ZoneInfo

import pytest

from src.technical_desk import outcome_resolver as r

TZ = "America/Argentina/Buenos_Aires"
OPEN = time(11, 0)
HOLIDAYS = {date(2026, 11, 23), date(2026, 12, 8), date(2026, 12, 25), date(2027, 1, 1)}


def calendar(start=date(2026, 11, 2), end=date(2027, 1, 29)):
    days, cursor = [], start
    while cursor <= end:
        if cursor.weekday() < 5 and cursor not in HOLIDAYS:
            days.append(cursor)
        cursor += timedelta(days=1)
    return days


SESSIONS = calendar()
ORIGIN = datetime(2026, 11, 6, 21, 0, tzinfo=ZoneInfo(TZ))           # Friday after the close -> first session Mon 11-09
D = [date(2026, 11, 9), date(2026, 11, 10), date(2026, 11, 11), date(2026, 11, 12), date(2026, 11, 13)]


def bar(day, o, h, l, c):
    dec = lambda v: None if v is None else Decimal(str(v))
    return r.Bar(day, dec(o), dec(h), dec(l), dec(c))


def flat(day, price=100):
    return bar(day, price, price + 1, price - 1, price)


def scenario(direction="BULLISH", timeframe="D", conf=("CLOSE", "GT", "100", 1), inv=("CLOSE", "LT", "95", 1),
             targets=("110",), horizon=5):
    return r.EvalScenario(direction, timeframe,
                          (r.EvalCondition("CONFIRMATION", conf[0], conf[1], Decimal(conf[2]), conf[3]),
                           r.EvalCondition("INVALIDATION", inv[0], inv[1], Decimal(inv[2]), inv[3])),
                          tuple(Decimal(t) for t in targets), horizon)


def resolve(sc, bars, data_through=D[-1], **overrides):
    args = dict(origin=ORIGIN, expected_sessions=SESSIONS, bars=bars, data_through=data_through, tz=TZ, session_open_local=OPEN)
    args.update(overrides)
    return r.resolve_scenario(sc, **args)


# --- sessions, not calendar days ------------------------------------------------------------------

def test_first_evaluable_session_must_start_strictly_after_the_origin():
    zone = ZoneInfo(TZ)
    start = lambda o: r.evaluation_sessions(o, SESSIONS, tz=TZ, session_open_local=OPEN)[0]
    assert start(datetime(2026, 11, 9, 10, 59, tzinfo=zone)) == date(2026, 11, 9)
    assert start(datetime(2026, 11, 9, 11, 0, tzinfo=zone)) == date(2026, 11, 10)      # not strictly after: session already started
    assert start(datetime(2026, 11, 9, 12, 0, tzinfo=zone)) == date(2026, 11, 10)
    with pytest.raises(ValueError, match="AWARE_ORIGIN_REQUIRED"):
        r.evaluation_sessions(datetime(2026, 11, 9, 10), SESSIONS, tz=TZ, session_open_local=OPEN)


def test_horizon_counts_calendar_sessions_and_skips_the_holiday():
    origin = datetime(2026, 11, 20, 21, 0, tzinfo=ZoneInfo(TZ))                         # Friday; Monday 11-23 is a holiday
    result = resolve(scenario(horizon=3), [], data_through=date(2026, 11, 20), origin=origin)
    assert (result.first_session, result.last_session) == (date(2026, 11, 24), date(2026, 11, 26))


# --- event resolution --------------------------------------------------------------------------------

def test_confirmation_then_target_reached_reports_timing_in_sessions():
    bars = [bar(D[0], 99, 100, 98, 99), bar(D[1], 99, 102, 99, 101), bar(D[2], 101, 111, 100, 110), flat(D[3]), flat(D[4])]
    result = resolve(scenario(), bars)
    assert result.status == "TARGET_REACHED" and result.final
    assert result.confirmation["index"] == 2 and result.targets_reached[0]["index"] == 3
    assert result.targets_reached[0]["level"] == Decimal("110")


def test_a_target_touched_before_the_confirmation_is_recorded_but_not_scored():
    bars = [bar(D[0], 99, 111, 98, 99), bar(D[1], 99, 102, 99, 101), flat(D[2], 102), flat(D[3], 102), flat(D[4], 102)]
    result = resolve(scenario(), bars)
    assert result.status == "CONFIRMED_TARGET_NOT_REACHED" and not result.targets_reached
    assert [t["session"] for t in result.targets_touched_before_confirmation] == [D[0]]


def test_a_target_touched_in_the_confirming_bar_precedes_a_close_based_confirmation():
    bars = [bar(D[0], 99, 111, 98, 101), flat(D[1], 102), flat(D[2], 102), flat(D[3], 102), flat(D[4], 102)]
    result = resolve(scenario(), bars)
    assert result.status == "CONFIRMED_TARGET_NOT_REACHED" and len(result.targets_touched_before_confirmation) == 1


def test_a_target_touched_before_confirmation_can_still_score_when_touched_again_after_it():
    bars = [bar(D[0], 99, 111, 98, 99), bar(D[1], 99, 102, 99, 101), bar(D[2], 101, 112, 101, 111), flat(D[3]), flat(D[4])]
    result = resolve(scenario(), bars)
    assert result.status == "TARGET_REACHED" and result.targets_reached[0]["session"] == D[2]


def test_intrabar_invalidation_precedes_a_close_confirmation_in_the_same_bar():
    sc = scenario(inv=("LOW", "LE", "95", 1))
    result = resolve(sc, [bar(D[0], 99, 102, 94, 101), flat(D[1]), flat(D[2]), flat(D[3]), flat(D[4])])
    assert result.status == "INVALIDATED_BEFORE_CONFIRMATION" and result.confirmation is None


def test_two_intrabar_touches_in_one_bar_are_ambiguous_and_never_favour_the_thesis():
    sc = scenario(conf=("HIGH", "GE", "105", 1), inv=("LOW", "LE", "95", 1), targets=())
    result = resolve(sc, [bar(D[0], 100, 106, 94, 100), flat(D[1]), flat(D[2]), flat(D[3]), flat(D[4])])
    assert result.status == "AMBIGUOUS_SAME_BAR" and result.final
    assert {e["kind"] for e in result.ambiguous_events} == {"CONFIRMATION", "INVALIDATION"}
    assert result.confirmation is None and result.invalidation is None


def test_a_gap_at_the_open_reveals_the_order_of_two_touches():
    sc = scenario(conf=("HIGH", "GE", "105", 1), inv=("LOW", "LE", "95", 1), targets=("120",))
    result = resolve(sc, [bar(D[0], 106, 108, 94, 100), flat(D[1]), flat(D[2]), flat(D[3]), flat(D[4])])
    assert result.status == "INVALIDATED_AFTER_CONFIRMATION"
    assert result.gap_events and result.gap_events[0]["kind"] == "CONFIRMATION"


def test_contradictory_close_conditions_met_at_the_same_instant_are_ambiguous():
    sc = scenario(inv=("CLOSE", "GT", "98", 1), targets=())
    assert resolve(sc, [bar(D[0], 99, 102, 99, 101)] + [flat(d) for d in D[1:]]).status == "AMBIGUOUS_SAME_BAR"


def test_persistence_needs_consecutive_bars_and_resets_when_broken():
    sc = scenario(conf=("CLOSE", "GT", "100", 2), targets=())
    bars = [bar(D[0], 100, 102, 100, 101), bar(D[1], 100, 100, 98, 99), bar(D[2], 99, 102, 99, 101),
            bar(D[3], 101, 103, 101, 102), flat(D[4], 102)]
    result = resolve(sc, bars)
    assert result.status == "CONFIRMED_NO_TARGETS" and result.confirmation["index"] == 4


def test_unconfirmed_scenario_expires_at_the_end_of_its_horizon():
    result = resolve(scenario(), [flat(d) for d in D])
    assert result.status == "EXPIRED_UNCONFIRMED" and result.window_fully_observed and result.final


def test_bearish_target_is_a_low_touch():
    sc = scenario("BEARISH", conf=("CLOSE", "LT", "100", 1), inv=("CLOSE", "GT", "105", 1), targets=("90",))
    bars = [bar(D[0], 101, 101, 98, 99), bar(D[1], 99, 99, 89, 91), flat(D[2], 95), flat(D[3], 95), flat(D[4], 95)]
    result = resolve(sc, bars)
    assert result.status == "TARGET_REACHED" and result.targets_reached[0]["index"] == 2


def test_neutral_direction_does_not_score_targets_but_still_resolves_conditions():
    sc = scenario("NEUTRAL", targets=("110",))
    result = resolve(sc, [bar(D[0], 99, 102, 99, 101)] + [flat(d, 101) for d in D[1:]])
    assert result.status == "CONFIRMED_NO_TARGETS" and result.target_note == "TARGETS_NOT_EVALUATED_FOR_NEUTRAL_DIRECTION"


# --- missing data: never move the expiry ------------------------------------------------------------

def test_missing_bar_at_the_expiry_session_is_not_replaced_by_the_next_observation():
    bars = [flat(d) for d in D[:4]] + [flat(date(2026, 11, 16))]                       # 11-13 absent, next session present
    result = resolve(scenario(), bars, data_through=date(2026, 11, 16))
    assert result.status == "MISSING_BAR" and result.missing_sessions == (D[4],)


def test_events_before_the_first_missing_bar_still_count():
    result = resolve(scenario(), [flat(D[0]), bar(D[1], 100, 100, 93, 94), flat(D[3]), flat(D[4])])
    assert result.status == "INVALIDATED_BEFORE_CONFIRMATION"


def test_a_missing_bar_before_any_event_blocks_later_events():
    bars = [flat(D[0]), flat(D[2], 102), flat(D[3], 102), flat(D[4], 102)]            # 11-10 absent; later closes would confirm
    assert resolve(scenario(), bars).status == "MISSING_BAR"


def test_unobserved_sessions_are_immature_not_missing():
    result = resolve(scenario(), [flat(D[0]), flat(D[1]), flat(D[2])], data_through=D[2])
    assert result.status == "IMMATURE" and not result.final and result.provisional_state == "PENDING"
    assert result.observed_through == D[2] and not result.missing_sessions


def test_a_terminal_event_is_final_even_before_the_horizon_matures():
    result = resolve(scenario(), [flat(D[0]), bar(D[1], 100, 100, 93, 94)], data_through=D[1])
    assert result.status == "INVALIDATED_BEFORE_CONFIRMATION" and result.final


def test_no_look_ahead_bars_after_the_data_cutoff_are_ignored():
    past = [flat(D[0]), flat(D[1]), flat(D[2])]
    future = past + [bar(D[3], 100, 130, 100, 125)]
    assert resolve(scenario(), future, data_through=D[2]) == resolve(scenario(), past, data_through=D[2])


def test_bars_before_the_origin_are_never_used():
    stale = bar(date(2026, 11, 6), 100, 103, 100, 102)                               # would confirm, but precedes the origin
    result = resolve(scenario(), [stale] + [flat(d) for d in D])
    assert result.status == "EXPIRED_UNCONFIRMED"


def test_calendar_that_does_not_reach_the_horizon_blocks_the_evaluation():
    short = [d for d in SESSIONS if d <= D[3]]
    assert resolve(scenario(), [flat(d) for d in D], expected_sessions=short).reason == "CALENDAR_DOES_NOT_COVER_HORIZON"


def test_a_session_label_on_a_non_trading_day_inside_the_window_blocks_the_evaluation():
    origin = datetime(2026, 11, 17, 21, 0, tzinfo=ZoneInfo(TZ))                       # window 11-18 .. 11-25, holiday 11-23 inside
    result = resolve(scenario(), [flat(date(2026, 11, 23))], data_through=date(2026, 11, 25), origin=origin)
    assert (result.status, result.reason) == ("NOT_EVALUABLE", "UNEXPECTED_SESSION_LABEL")


# --- high/low and adjustments -----------------------------------------------------------------------

def test_an_intrabar_condition_is_not_evaluated_from_the_close_when_high_low_are_absent():
    sc = scenario(conf=("HIGH", "GE", "105", 1), targets=())
    bars = [bar(d, 100, None, None, 106) for d in D]                                   # closes far above, but no HIGH
    result = resolve(sc, bars)
    assert (result.status, result.reason) == ("NOT_EVALUABLE", "HIGH_LOW_REQUIRED_BY_CONDITION_OR_TARGET")


def test_close_only_conditions_work_without_high_low():
    sc = scenario(targets=())
    result = resolve(sc, [bar(D[0], 100, None, None, 101)] + [bar(d, 101, None, None, 101) for d in D[1:]])
    assert result.status == "CONFIRMED_NO_TARGETS"


def test_an_adjustment_event_inside_the_window_makes_the_case_not_comparable():
    bars = [flat(d) for d in D]
    assert resolve(scenario(), bars, adjustment_events=(D[2],)).status == "NOT_COMPARABLE"
    assert resolve(scenario(), bars, adjustment_events=(date(2026, 11, 16),)).status == "EXPIRED_UNCONFIRMED"


# --- weekly scenarios ---------------------------------------------------------------------------------

WEEK1 = [date(2026, 11, 9), date(2026, 11, 10), date(2026, 11, 11), date(2026, 11, 12), date(2026, 11, 13)]
WEEK2 = [date(2026, 11, 16), date(2026, 11, 17), date(2026, 11, 18), date(2026, 11, 19), date(2026, 11, 20)]


def test_weekly_bars_need_complete_weeks_and_resolve_on_the_last_session_of_the_week():
    sc = scenario(timeframe="W", targets=(), horizon=10)
    bars = [flat(d, 99) for d in WEEK1] + [flat(d, 99) for d in WEEK2[:4]] + [bar(WEEK2[4], 100, 102, 99, 101)]
    result = resolve(sc, bars, data_through=WEEK2[4])
    assert result.status == "CONFIRMED_NO_TARGETS" and result.confirmation["index"] == 10 and result.confirmation["session"] == WEEK2[4]


def test_a_weekly_scenario_without_a_complete_week_inside_the_horizon_is_not_evaluable():
    origin = datetime(2026, 11, 11, 21, 0, tzinfo=ZoneInfo(TZ))                       # Wednesday: first week is partial
    result = resolve(scenario(timeframe="W", horizon=5), [], data_through=date(2026, 11, 20), origin=origin)
    assert result.reason == "NO_COMPLETE_WEEKLY_BAR_IN_HORIZON"
    later = resolve(scenario(timeframe="W", targets=(), horizon=8), [flat(d, 99) for d in WEEK1 + WEEK2] + [flat(date(2026, 11, 24), 99)],
                    data_through=date(2026, 11, 24), origin=origin)
    assert later.status == "EXPIRED_UNCONFIRMED"


def test_a_missing_daily_bar_inside_a_week_blocks_the_weekly_bar():
    bars = [flat(d, 99) for d in WEEK1] + [flat(d, 99) for d in WEEK2 if d != date(2026, 11, 18)]
    result = resolve(scenario(timeframe="W", targets=(), horizon=10), bars, data_through=WEEK2[4])
    assert result.status == "MISSING_BAR" and result.missing_sessions == (date(2026, 11, 18),)


def test_an_incomplete_last_week_is_immature():
    bars = [flat(d, 99) for d in WEEK1 + WEEK2[:3]]
    result = resolve(scenario(timeframe="W", targets=(), horizon=10), bars, data_through=WEEK2[2])
    assert result.status == "IMMATURE"


# --- plumbing -----------------------------------------------------------------------------------------

def test_payload_is_json_safe_deterministic_and_versioned():
    bars = [bar(D[0], 99, 100, 98, 99), bar(D[1], 99, 102, 99, 101), bar(D[2], 101, 111, 100, 110), flat(D[3]), flat(D[4])]
    first, second = resolve(scenario(), bars).to_payload(), resolve(scenario(), bars).to_payload()
    assert first == second and json.loads(json.dumps(first))["resolver_version"] == r.RESOLVER_VERSION
    assert first["targets_reached"][0]["level"] == "110" and first["first_session"] == "2026-11-09"


def test_duplicate_bars_and_malformed_inputs_are_rejected():
    with pytest.raises(ValueError, match="DUPLICATE_BAR_SESSION"):
        resolve(scenario(), [flat(D[0]), flat(D[0])])
    with pytest.raises(ValueError, match="INCONSISTENT_OHLC"):
        bar(D[0], 100, 99, 98, 100)
    twice = r.EvalScenario("BULLISH", "D", (r.EvalCondition("CONFIRMATION", "CLOSE", "GT", Decimal(1), 1),) * 2, (), 5)
    with pytest.raises(ValueError, match="EXACTLY_ONE"):
        resolve(twice, [])


def test_role_output_scenarios_convert_without_losing_conditions_or_targets():
    from src.technical_desk.role_contracts import ConditionOutput, ScenarioOutput
    output = ScenarioOutput(
        method="STRUCTURE", timeframe="W", family="RANGE", causal_anchors=(), direction="BEARISH", role="PRIMARY",
        structure_state="BREAKDOWN", structure_anchors=(),
        conditions=(ConditionOutput(kind="CONFIRMATION", field="LOW", operator="LE", level="90.5", persistence_bars=2),
                    ConditionOutput(kind="INVALIDATION", field="CLOSE", operator="GT", level="101", persistence_bars=1)),
        targets=("85", "80.25"), horizon_sessions=20, uncertainty="HIGH", text="")
    converted = r.scenario_from_role_output(output)
    assert (converted.direction, converted.timeframe, converted.horizon_sessions) == ("BEARISH", "W", 20)
    assert converted.targets == (Decimal("85"), Decimal("80.25"))
    assert converted.conditions[0] == r.EvalCondition("CONFIRMATION", "LOW", "LE", Decimal("90.5"), 2)


def test_bars_after_the_data_cutoff_cannot_even_invalidate_the_evaluation():
    origin = datetime(2026, 11, 17, 21, 0, tzinfo=ZoneInfo(TZ))
    early = [flat(date(2026, 11, 18)), flat(date(2026, 11, 19))]
    stray = early + [flat(date(2026, 11, 23))]                                         # holiday label, but after the cut-off
    result = resolve(scenario(), stray, data_through=date(2026, 11, 19), origin=origin)
    assert result.status == "IMMATURE" and not result.unexpected_bar_sessions

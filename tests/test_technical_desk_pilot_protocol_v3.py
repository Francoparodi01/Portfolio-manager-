from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from src.technical_desk.pilot_protocol_v3 import (
    ACTION_BY_CHANGE, AGENTS, PROTOCOL_VERSION, SCENARIO_RULE_VERSION,
    CallEnvelope, Condition, ExperimentIdentity, PacketSpec, PricingEvidence,
    Replicate, Scenario, assess_pilot_completion, compare_replicates,
    plan_pilot_budget, replicate_ledger_record,
)


D = Decimal
NOW = datetime(2026, 11, 16, 12, tzinfo=timezone.utc)


def packet(number=1, **kwargs):
    return replace(PacketSpec(
        f"packet-{number}", f"{number:064x}", f"BYMA:CEDEAR:ASSET{number}:ARS", "ARS",
        D("0.01"), ("pivot-a", "pivot-b", "pivot-c"), ("D", "W"), "READY",
        atr_by_timeframe=(("D", D("2")), ("W", D("5"))),
    ), **kwargs)


def scenario(**kwargs):
    return replace(Scenario(
        "ELLIOTT", "D", "IMPULSE", (("origin", "pivot-a"),),
        "BULLISH", "PRIMARY", "WAVE_3", (("wave_1", "pivot-b"),),
        (Condition("CONFIRMATION", "CLOSE", "GE", D("100"), 2),
         Condition("INVALIDATION", "LOW", "LE", D("90"), 1)),
        (D("110"), D("120")), 20, "Principal rationale",
    ), **kwargs)


def replicate(identifier="r1", **kwargs):
    identity = ExperimentIdentity(packet(), "fixed-model", "a" * 64, "b" * 64, "c" * 64, "d" * 64)
    return replace(Replicate(identity, identifier, "elliott", "D", "VALID", (scenario(),)), **kwargs)


def envelopes():
    # Explicit synthetic tariff, never used as a real provider price.
    rate = PricingEvidence("fixed-model", "test-fixture://tariff", NOW - timedelta(hours=1),
                           NOW + timedelta(hours=1), D("1"), D("1"), D("0"), True)
    return tuple(CallEnvelope(agent, 1000, 1000, rate, "a" * 64) for agent in AGENTS)


def packets():
    return tuple(packet(i) for i in range(1, 6))


def budget(**kwargs):
    return plan_pilot_budget(packets(), envelopes(), as_of=NOW, **kwargs)


def kinds(result):
    return {change.kind for change in result.changes}


def test_v3_does_not_relabel_or_modify_v2_contract():
    from src.technical_desk import pilot_protocol as v2

    assert PROTOCOL_VERSION == "technical-desk-pilot-v3"
    assert SCENARIO_RULE_VERSION == "technical-desk-semantic-v3"
    assert v2.PROTOCOL_VERSION == "technical-desk-pilot-v2"
    assert v2.SCENARIO_RULE_VERSION == "technical-desk-semantic-v2"
    assert "LEVEL_CHANGE" in v2.ACTION_BY_CHANGE and "LEVEL_CHANGE" not in ACTION_BY_CHANGE


def test_exact_elliott_primary_alternative_same_family_and_anchors_does_not_retry():
    primary = scenario()
    alternative = replace(primary, role="ALTERNATIVE", structure_state="WAVE_5", targets=(D("200"),))
    left = replicate(scenarios=(primary, alternative))
    right = replicate("r2", scenarios=(replace(alternative, targets=(D("1000"),)), primary))
    result = compare_replicates(left, right)
    assert result.status == "STABLE" and result.matched_scenarios == 1
    assert result.errors == () and result.excluded_alternatives == (1, 1)
    assert not any("RETRY" in change.action for change in result.changes)
    assert all(all(value == 0 for value in metric.delta_ticks) for metric in result.level_dispersion)
    record = replicate_ledger_record(left)
    assert record["noise_scope"] == "PRIMARY_ONLY"
    assert record["response"] == asdict(left)
    assert record["response"]["scenarios"][1]["targets"] == (D("200"),)


@pytest.mark.parametrize("price_delta,ticks,atr_delta", [
    ("0", 0, "0"), ("0.01", 1, "0.005"), ("1", 100, "0.5"), ("-1", -100, "-0.5"),
])
def test_level_distances_never_create_binary_instability(price_delta, ticks, atr_delta):
    old = scenario()
    new = replace(old, conditions=(replace(old.conditions[0], level=D("100") + D(price_delta)), old.conditions[1]),
                  targets=(D("110") + D(price_delta), D("120") + D(price_delta)))
    result = compare_replicates(replicate(), replicate("r2", scenarios=(new,)))
    assert result.status == "STABLE" and kinds(result) == {"STABLE", "LEVEL_DISPERSION"}
    confirmation, invalidation, targets = result.level_dispersion
    assert confirmation.delta_ticks == (ticks,) and confirmation.delta_atr == (D(atr_delta),)
    assert confirmation.absolute_delta_ticks == (abs(ticks),)
    assert confirmation.absolute_delta_atr == (abs(D(atr_delta)),)
    assert invalidation.delta_ticks == (0,)
    assert targets.delta_ticks == (ticks, ticks)


def test_targets_cardinality_difference_preserves_sets_without_inventing_pairs():
    result = compare_replicates(replicate(), replicate("r2", scenarios=(scenario(targets=(D("115"),)),)))
    target = result.level_dispersion[-1]
    assert result.status == "STABLE"
    assert target.before_ticks == (11000, 12000) and target.after_ticks == (11500,)
    assert target.delta_ticks is None and target.delta_atr is None
    assert target.unpaired_reason == "TARGET_CARDINALITY_DIFFERS_NO_PAIRING"


@pytest.mark.parametrize("atr,reason", [
    ((), "PACKAGE_ATR_MISSING"),
    ((("D", D("0")),), "PACKAGE_ATR_MUST_BE_FINITE_AND_POSITIVE_DECIMAL"),
    ((("D", D("NaN")),), "PACKAGE_ATR_MUST_BE_FINITE_AND_POSITIVE_DECIMAL"),
    ((("D", D("Infinity")),), "PACKAGE_ATR_MUST_BE_FINITE_AND_POSITIVE_DECIMAL"),
    ((("D", 2.0),), "PACKAGE_ATR_MUST_BE_FINITE_AND_POSITIVE_DECIMAL"),
    ((("D", D("2")), ("D", D("3"))), "AMBIGUOUS_PACKAGE_ATR"),
])
def test_missing_or_invalid_atr_does_not_invent_numeric_denominator(atr, reason):
    left = replicate(identity=replace(replicate().identity, packet=packet(atr_by_timeframe=atr)))
    result = compare_replicates(left, replace(left, replicate_id="r2"))
    assert result.status == "STABLE"
    assert all(metric.delta_atr is None and metric.atr_reason == reason for metric in result.level_dispersion)
    assert result.level_dispersion[0].delta_ticks == (0,)


def test_weekly_dispersion_uses_weekly_atr():
    old = scenario(timeframe="W")
    new = replace(old, targets=(D("115"), D("125")))
    left = replicate(timeframe="W", scenarios=(old,))
    result = compare_replicates(left, replace(left, replicate_id="r2", scenarios=(new,)))
    assert result.level_dispersion[-1].delta_atr == (D("1"), D("1"))


@pytest.mark.parametrize("changes,expected", [
    ({"direction": "BEARISH"}, "DIRECTION_CHANGE"),
    ({"structure_state": "WAVE_5"}, "STRUCTURE_CHANGE"),
    ({"horizon_sessions": 25}, "STRUCTURE_CHANGE"),
    ({"causal_anchors": (("origin", "pivot-c"),)}, "ANCHOR_CHANGE"),
    ({"structure_anchors": (("wave_1", "pivot-c"),)}, "ANCHOR_CHANGE"),
])
def test_binary_axes_remain_visible(changes, expected):
    result = compare_replicates(replicate(), replicate("r2", scenarios=(scenario(**changes),)))
    assert result.status == "NOISE_UNSTABLE"
    assert kinds(result) == {expected, "LEVEL_DISPERSION"}


def test_primary_selection_change_is_measured_without_using_alternatives():
    primary = scenario()
    alternative = replace(primary, structure_state="WAVE_5", role="ALTERNATIVE")
    left = replicate(scenarios=(primary, alternative))
    right = replicate("r2", scenarios=(replace(primary, role="ALTERNATIVE"), replace(alternative, role="PRIMARY")))
    result = compare_replicates(left, right)
    assert result.status == "NOISE_UNSTABLE"
    assert kinds(result) == {"STRUCTURE_CHANGE", "LEVEL_DISPERSION"}
    assert "ROLE_CHANGE" in ACTION_BY_CHANGE  # Reserved, not independently observable.


@pytest.mark.parametrize("alternatives", [
    (),
    (scenario(role="ALTERNATIVE"),),
    (scenario(role="ALTERNATIVE", direction="BEARISH", structure_state="WAVE_5",
              causal_anchors=(("origin", "pivot-c"),), targets=(D("999"),), text="Changed alternative"),),
    (scenario(role="ALTERNATIVE"), scenario(role="ALTERNATIVE", structure_state="WAVE_5")),
])
def test_valid_alternative_add_remove_or_change_cannot_affect_comparison(alternatives):
    old_primary = scenario()
    new_primary = scenario(structure_state="WAVE_5", targets=(D("115"), D("125")))
    # Including each side's primary among the other's alternatives must not
    # create ROLE_CHANGE or influence any structural or numerical diagnostic.
    left = replicate(scenarios=(old_primary, replace(new_primary, role="ALTERNATIVE")))
    bare = compare_replicates(left, replicate("r2", scenarios=(new_primary,)))
    changed = compare_replicates(left, replicate("r2", scenarios=(new_primary,) + alternatives))
    assert replace(changed, excluded_alternatives=bare.excluded_alternatives) == bare
    assert changed.excluded_alternatives == (1, len(alternatives))


def test_alternative_prose_levels_and_addition_do_not_enter_primary_noise():
    right = replicate("r2", scenarios=(scenario(), scenario(role="ALTERNATIVE", targets=(D("999"),), text="Other")),
                      text="New alternative commentary")
    result = compare_replicates(replicate(), right)
    assert result.status == "STABLE" and result.excluded_alternatives == (0, 1)


def test_abstention_is_evidence_status_not_binary_semantic_axis():
    abstain = replicate("r2", state="ABSTAIN", scenarios=(), abstention_reason="MISSING_PATTERN")
    result = compare_replicates(replicate(), abstain)
    assert result.status == "INSUFFICIENT_EVIDENCE" and kinds(result) == {"ABSTENTION_CHANGE"}


def test_relative_volume_partial_is_ready_for_shadow_only():
    result = budget()
    assert result.status == "READY"
    assert result.base_calls == 50 and result.maximum_attempts == 100
    assert result.required_packages == 5 and result.replicas_per_package == 2
    assert result.required_reservation_usd == D("0.2") and result.budget_usd == D("2")
    assert result.dispatch_authorized is False


@pytest.mark.parametrize("change", [
    {"quality_status": "PARTIAL"}, {"operating_mode": "PRODUCTION"},
    {"volume_basis": "ABSOLUTE"}, {"validated_timeframes": ("D",)},
])
def test_bad_packet_contract_blocks_whole_pilot(change):
    altered = (replace(packet(), **change),) + packets()[1:]
    assert plan_pilot_budget(altered, envelopes(), as_of=NOW).status == "NOT_STARTED"


@pytest.mark.parametrize("change", [
    {"debate_enabled": True}, {"retry_limit": 0}, {"budget_usd": D("2.01")},
])
def test_fixed_pilot_limits_cannot_be_relaxed(change):
    assert budget(**change).status == "NOT_STARTED"


@pytest.mark.parametrize("mutation", ["no_tariff", "no_model", "no_digest", "expired", "expensive"])
def test_absent_model_or_tariff_and_unaffordable_plan_never_start(mutation):
    original = envelopes()[0]
    if mutation == "no_tariff":
        altered = replace(original, pricing=None)
    elif mutation == "no_model":
        altered = replace(original, pricing=replace(original.pricing, model_id=""))
    elif mutation == "no_digest":
        altered = replace(original, model_digest="")
    elif mutation == "expired":
        altered = replace(original, pricing=replace(original.pricing, valid_until=NOW - timedelta(seconds=1)))
    else:
        altered = replace(original, pricing=replace(original.pricing, extra_usd_per_attempt=D("1")))
    result = plan_pilot_budget(packets(), (altered,) + envelopes()[1:], as_of=NOW)
    assert result.status == "NOT_STARTED" and result.dispatch_authorized is False


def full_abstention_responses():
    responses = []
    for item in packets():
        identity = replace(replicate().identity, packet=item)
        for rep in ("r1", "r2"):
            for agent in AGENTS:
                for timeframe in ("D", "W"):
                    responses.append(Replicate(identity, rep, agent, timeframe, "ABSTAIN", abstention_reason="NO_PATTERN"))
    return tuple(responses)


def test_completion_reports_wyckoff_separately_without_combined_noise_count():
    result = assess_pilot_completion(packets(), full_abstention_responses(), budget=budget(), calculated_cost_usd=D("0.1"))
    assert result.status == "COMPLETE" and result.complete_packets == 5
    assert result.wyckoff.evaluation_cohort == "WYCKOFF_RELATIVE_UNVERIFIED"
    assert result.wyckoff.insufficient_evidence_pairs == 10
    assert result.other_roles.insufficient_evidence_pairs == 40
    assert not hasattr(result, "stable_pairs") and not hasattr(result, "insufficient_evidence_pairs")
    record = replicate_ledger_record(next(r for r in full_abstention_responses() if r.agent == "wyckoff"))
    assert record["evaluation_cohort"] == "WYCKOFF_RELATIVE_UNVERIFIED"


@pytest.mark.parametrize("fault", ["missing_weekly", "unreserved_model", "duplicate", "changed_cohort"])
def test_partial_or_changed_pilot_never_completes(fault):
    responses = full_abstention_responses()
    packet_set = packets()
    if fault == "missing_weekly":
        responses = responses[:-1]
    elif fault == "unreserved_model":
        responses = tuple(replace(r, identity=replace(r.identity, model_id="other-model")) for r in responses)
    elif fault == "duplicate":
        responses += (responses[0],)
    else:
        packet_set = tuple(packet(i) for i in range(2, 7))
    result = assess_pilot_completion(packet_set, responses, budget=budget(), calculated_cost_usd=D("0.1"))
    assert result.status == "INCOMPLETE"


@pytest.mark.parametrize("cost,status", [(None, "COST_UNVERIFIED"), (D("0.21"), "BUDGET_BREACH")])
def test_real_usage_cost_remains_required_and_bounded(cost, status):
    result = assess_pilot_completion(packets(), full_abstention_responses(), budget=budget(), calculated_cost_usd=cost)
    assert result.status == status

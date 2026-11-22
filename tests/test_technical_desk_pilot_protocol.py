from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, localcontext

import pytest

from src.technical_desk.pilot_protocol import (
    AGENTS,
    CallEnvelope,
    Condition,
    ExperimentIdentity,
    PacketSpec,
    PricingEvidence,
    Replicate,
    Scenario,
    assess_pilot_completion,
    compare_replicates,
    plan_pilot_budget,
    scenario_fingerprint,
    scenario_matching_key,
)


D = Decimal
NOW = datetime(2026, 11, 15, 12, tzinfo=timezone.utc)


def packet(number=1):
    return PacketSpec(
        f"packet-{number}", f"{number:064x}", f"TEST:CEDEAR:ASSET{number}:ARS", "ARS",
        D("0.01"), ("pivot-a", "pivot-b", "pivot-c"), ("D", "W"), "READY",
    )


def scenario(**kwargs):
    base = Scenario(
        "STRUCTURE", "D", "SWING_SEQUENCE", (("origin", "pivot-a"),),
        "BULLISH", "PRIMARY", "HH_HL", (("last_high", "pivot-b"),),
        (Condition("CONFIRMATION", "CLOSE", "GE", D("100"), 2),
         Condition("INVALIDATION", "LOW", "LE", D("90"), 1)),
        (D("110"), D("120")), 20, "First wording",
    )
    return replace(base, **kwargs)


def replicate(identifier="r1", **kwargs):
    identity = ExperimentIdentity(packet(), "fixed-model", "a" * 64, "b" * 64, "c" * 64, "d" * 64)
    return replace(Replicate(identity, identifier, "structure", "D", "VALID", (scenario(),)), **kwargs)


def kinds(result):
    return {change.kind for change in result.changes}


def envelopes(*, input_rate="1", output_rate="1", extra="0"):
    # Synthetic unit-test tariff supplied explicitly; not a provider price.
    rate = PricingEvidence("fixed-model", "test-fixture://tariff", NOW - timedelta(hours=1),
                           NOW + timedelta(hours=1), D(input_rate), D(output_rate), D(extra), True)
    return tuple(CallEnvelope(agent, 1000, 1000, rate, "a" * 64) for agent in AGENTS)


def budget(**kwargs):
    return plan_pilot_budget(tuple(packet(i) for i in range(1, 6)), envelopes(), as_of=NOW, **kwargs)


def test_decimal_spelling_and_text_do_not_change_scenario_semantics():
    old = scenario()
    new = replace(old, targets=(D("120.000"), D("110.00")),
                  conditions=tuple(reversed(old.conditions)), text="Different explanation")
    assert scenario_matching_key(old, packet()) == scenario_matching_key(new, packet())
    assert scenario_fingerprint(old, packet()) == scenario_fingerprint(new, packet())
    result = compare_replicates(replicate(), replicate("r2", scenarios=(new,)))
    assert result.status == "TEXT_ONLY"
    assert result.changes[0].action == "RETAIN_SEMANTICS_AND_ARCHIVE_BOTH_TEXTS"


def test_direction_flip_keeps_matching_key_and_cannot_hide_in_unmatched_scenarios():
    flipped = scenario(direction="BEARISH")
    assert scenario_matching_key(flipped, packet()) == scenario_matching_key(scenario(), packet())
    assert scenario_fingerprint(flipped, packet()) != scenario_fingerprint(scenario(), packet())
    result = compare_replicates(replicate(), replicate("r2", scenarios=(flipped,)))
    assert result.status == "NOISE_UNSTABLE"
    assert result.matched_scenarios == 1
    assert kinds(result) == {"DIRECTION_CHANGE"}
    assert not result.market_change_claim
    assert result.changes[0].action == "QUARANTINE_PAIR_AND_REPORT_UNSTABLE_DIRECTION"


def test_primary_alternative_swap_and_list_reorder_preserve_causal_matching():
    first = scenario()
    other = scenario(causal_anchors=(("origin", "pivot-c"),), role="ALTERNATIVE")
    left = replicate(scenarios=(first, other))
    reordered = replicate("r2", scenarios=(other, first))
    assert compare_replicates(left, reordered).status == "STABLE"
    swapped = replicate("r2", scenarios=(replace(other, role="PRIMARY"), replace(first, role="ALTERNATIVE")))
    result = compare_replicates(left, swapped)
    assert result.status == "NOISE_UNSTABLE"
    assert result.matched_scenarios == 2 and kinds(result) == {"ROLE_CHANGE"}


@pytest.mark.parametrize("changed,expected", [
    (scenario(targets=(D("110.01"), D("120"))), "LEVEL_CHANGE"),
    (scenario(structure_state="LH_LL"), "STRUCTURE_CHANGE"),
    (scenario(structure_anchors=(("last_high", "pivot-c"),)), "STRUCTURE_CHANGE"),
    (scenario(horizon_sessions=10), "STRUCTURE_CHANGE"),
    (scenario(conditions=(Condition("CONFIRMATION", "HIGH", "GT", D("100"), 2),
                          Condition("INVALIDATION", "LOW", "LE", D("90"), 1))), "STRUCTURE_CHANGE"),
])
def test_each_semantic_change_has_an_explicit_quarantine_action(changed, expected):
    result = compare_replicates(replicate(), replicate("r2", scenarios=(changed,)))
    assert result.status == "NOISE_UNSTABLE"
    assert expected in kinds(result)
    assert all(c.action.startswith("QUARANTINE_PAIR_") for c in result.changes)


def test_changed_causal_anchor_is_added_removed_structure_not_fuzzy_matched():
    changed = scenario(causal_anchors=(("origin", "pivot-c"),))
    result = compare_replicates(replicate(), replicate("r2", scenarios=(changed,)))
    assert result.status == "NOISE_UNSTABLE"
    assert result.matched_scenarios == 0
    assert len(result.added_keys) == len(result.removed_keys) == 1
    assert kinds(result) == {"STRUCTURE_CHANGE"}


def test_duplicate_semantic_keys_are_ambiguous_even_with_distinct_roles():
    duplicate = scenario(role="ALTERNATIVE", direction="BEARISH")
    result = compare_replicates(replicate(scenarios=(scenario(), duplicate)), replicate("r2"))
    assert result.status == "NOT_COMPARABLE"
    assert result.errors == ("ambiguous_duplicate_matching_key",)
    assert result.matched_scenarios == 0


@pytest.mark.parametrize("changed", [
    scenario(targets=(D("110.001"),)),
    scenario(targets=(float("nan"),)),
    scenario(targets=(D("Infinity"),)),
    scenario(causal_anchors=(("origin", "unknown-future-pivot"),)),
    scenario(conditions=()),
    scenario(horizon_sessions=True),
])
def test_invalid_inputs_fail_closed_without_silent_rounding(changed):
    result = compare_replicates(replicate(), replicate("r2", scenarios=(changed,)))
    assert result.status == "NOT_COMPARABLE"
    assert result.errors and not result.market_change_claim


@pytest.mark.parametrize("field,value", [
    ("packet", packet(2)), ("model_id", "different-model"),
    ("model_digest", "e" * 64), ("prompt_digest", "f" * 64),
    ("rules_digest", "e" * 64), ("parameters_digest", "f" * 64),
])
def test_frozen_experiment_changes_are_not_reported_as_model_noise(field, value):
    right = replicate("r2")
    right = replace(right, identity=replace(right.identity, **{field: value}))
    assert compare_replicates(replicate(), right).status == "INVALID_EXPERIMENT"


def test_comparison_rejects_reused_replica_cross_agent_cross_timeframe_and_missing():
    assert compare_replicates(replicate(), replicate()).status == "INVALID_EXPERIMENT"
    assert compare_replicates(replicate(), replicate("r2", agent="momentum")).status == "NOT_COMPARABLE"
    assert compare_replicates(replicate(), replicate("r2", timeframe="W")).status == "NOT_COMPARABLE"
    assert compare_replicates(replicate(), None).status == "NOT_COMPARABLE"
    assert compare_replicates(replicate(), replicate("r2", state="INVALID")).status == "NOT_COMPARABLE"


def test_abstention_transition_and_both_abstentions_are_different_outcomes():
    abstain = replicate("r2", state="ABSTAIN", scenarios=(), abstention_reason="MISSING_VOLUME_UNIT")
    transition = compare_replicates(replicate(), abstain)
    assert transition.status == "NOISE_UNSTABLE" and kinds(transition) == {"ABSTENTION_CHANGE"}
    both = compare_replicates(replace(abstain, replicate_id="r1"), abstain)
    assert both.status == "INSUFFICIENT_EVIDENCE"
    assert both.changes[0].action == "COLLECT_MISSING_EVIDENCE"


def test_budget_reserves_all_fifty_calls_and_all_fifty_possible_retries():
    result = budget()
    assert result.status == "READY"
    assert (result.required_packages, result.replicas_per_package, result.base_calls, result.maximum_attempts) == (5, 2, 50, 100)
    assert result.required_reservation_usd == D("0.2")
    assert result.observed_cost_usd is None and result.dispatch_authorized is False


def test_budget_rejects_full_pilot_even_when_first_replicas_would_fit():
    result = budget(budget_usd=D("0.15"))
    assert result.status == "NOT_STARTED"
    assert result.required_reservation_usd == D("0.2")
    assert result.required_packages == 5


def test_exact_two_dollar_limit_includes_extra_charges_without_decimal_rounding():
    packets = tuple(packet(i) for i in range(1, 6))
    with localcontext() as context:
        context.prec = 2
        fits = plan_pilot_budget(packets, envelopes(extra="0.018"), as_of=NOW)
        over = plan_pilot_budget(packets, envelopes(extra="0.018000001"), as_of=NOW)
    assert fits.status == "READY" and fits.required_reservation_usd == D("2")
    assert over.status == "NOT_STARTED" and over.required_reservation_usd == D("2.0000001")


@pytest.mark.parametrize("kwargs", [
    {"budget_usd": D("2.01")}, {"budget_usd": D("NaN")},
    {"budget_usd": 2.0}, {"retry_limit": 0}, {"retry_limit": True},
    {"debate_enabled": True},
])
def test_budget_cannot_relax_registered_sample_retry_or_cap(kwargs):
    assert budget(**kwargs).status == "NOT_STARTED"


def test_budget_rejects_reduced_duplicate_and_incomplete_packets():
    packets = tuple(packet(i) for i in range(1, 6))
    for invalid in (packets[:4], (packet(),) * 5,
                    (replace(packet(), validated_timeframes=("D",)),) + packets[1:],
                    (replace(packet(), quality_status="PARTIAL"),) + packets[1:]):
        result = plan_pilot_budget(invalid, envelopes(), as_of=NOW)
        assert result.status == "NOT_STARTED" and result.required_packages == 5


@pytest.mark.parametrize("changes", [
    {"source": ""}, {"covers_all_billable_usage": False},
    {"verified_at": NOW + timedelta(seconds=1)},
    {"valid_until": NOW - timedelta(seconds=1)},
    {"verified_at": NOW.replace(tzinfo=None)},
    {"input_usd_per_million": D("NaN")},
    {"output_usd_per_million": 1.0}, {"currency": "ARS"},
])
def test_unknown_stale_partial_or_invalid_pricing_blocks_whole_pilot(changes):
    plans = envelopes()
    altered = replace(plans[0], pricing=replace(plans[0].pricing, **changes))
    result = plan_pilot_budget(tuple(packet(i) for i in range(1, 6)), (altered,) + plans[1:], as_of=NOW)
    assert result.status == "NOT_STARTED" and result.required_reservation_usd is None


def test_missing_role_or_unbounded_billable_output_does_not_create_budget():
    packets = tuple(packet(i) for i in range(1, 6))
    plans = envelopes()
    assert plan_pilot_budget(packets, plans[:4], as_of=NOW).status == "NOT_STARTED"
    invalid = (replace(plans[0], max_billable_output_tokens=0),) + plans[1:]
    assert plan_pilot_budget(packets, invalid, as_of=NOW).status == "NOT_STARTED"


def test_semantic_matching_does_not_cross_instruments_or_currencies():
    assert scenario_matching_key(scenario(), packet()) != scenario_matching_key(scenario(), packet(2))
    usd = replace(packet(), instrument_id="TEST:CEDEAR:ASSET1:USD", currency="USD")
    assert scenario_matching_key(scenario(), packet()) != scenario_matching_key(scenario(), usd)


def test_five_snapshots_of_one_instrument_cannot_satisfy_sample_minimum():
    packets = tuple(replace(packet(i), instrument_id=packet().instrument_id) for i in range(1, 6))
    result = plan_pilot_budget(packets, envelopes(), as_of=NOW)
    assert result.status == "NOT_STARTED" and "five_distinct_instruments_required" in result.reasons


def complete_abstention_pilot():
    packets = tuple(packet(i) for i in range(1, 6))
    responses = []
    for item in packets:
        identity = replace(replicate().identity, packet=item)
        for rep in ("r1", "r2"):
            for agent in AGENTS:
                for timeframe in ("D", "W"):
                    responses.append(Replicate(identity, rep, agent, timeframe, "ABSTAIN",
                                               abstention_reason="NO_SUPPORTED_PATTERN"))
    return packets, tuple(responses)


def test_full_valid_abstentions_complete_protocol_but_not_evaluable_or_stable_pairs():
    packets, responses = complete_abstention_pilot()
    result = assess_pilot_completion(packets, responses, budget=budget(), calculated_cost_usd=D("0.1"))
    assert result.status == "COMPLETE" and result.complete_packets == 5
    assert result.insufficient_evidence_pairs == 50
    assert result.stable_pairs == result.unstable_pairs == 0


def test_missing_weekly_response_or_moderator_never_completes_five_packets():
    packets, responses = complete_abstention_pilot()
    for incomplete in (responses[:-1], tuple(r for r in responses if r.agent != "moderator")):
        result = assess_pilot_completion(packets, incomplete, budget=budget(), calculated_cost_usd=D("0.1"))
        assert result.status == "INCOMPLETE" and result.complete_packets < 5


@pytest.mark.parametrize("cost,status", [(None, "COST_UNVERIFIED"), (D("2.01"), "BUDGET_BREACH")])
def test_complete_sample_without_verified_cost_under_reserve_is_not_accepted(cost, status):
    packets, responses = complete_abstention_pilot()
    result = assess_pilot_completion(packets, responses, budget=budget(), calculated_cost_usd=cost)
    assert result.status == status and result.complete_packets == 5


def test_duplicates_changed_experiment_and_failed_reply_cannot_complete_pilot():
    packets, responses = complete_abstention_pilot()
    changed = replace(responses[-1], identity=replace(responses[-1].identity, model_digest="f" * 64))
    failed = replace(responses[-1], state="ERROR")
    for bad in (responses + (responses[-1],), responses[:-1] + (changed,), responses[:-1] + (failed,)):
        result = assess_pilot_completion(packets, bad, budget=budget(), calculated_cost_usd=D("0.1"))
        assert result.status == "INCOMPLETE"


def test_compound_conditions_cannot_hide_and_or_semantics_in_prose():
    base = scenario()
    compound = base.conditions + (Condition("CONFIRMATION", "HIGH", "GT", D("105"), 1),)
    left = replicate(scenarios=(replace(base, conditions=compound, text="Both conditions required"),))
    right = replicate("r2", scenarios=(replace(base, conditions=compound, text="Either condition suffices"),))
    result = compare_replicates(left, right)
    assert result.status == "NOT_COMPARABLE"
    assert result.errors == ("exactly_one_condition_per_kind_required",)


@pytest.mark.parametrize("identity", [
    "test:cedear:asset1:ars", "TEST:CEDEAR:ASSET1 :ARS",
    "TEST:CEDEAR:Asset1:ARS", "TEST:ASSET1:ARS",
])
def test_noncanonical_identity_aliases_cannot_satisfy_distinct_sample(identity):
    packets = (replace(packet(), instrument_id=identity),) + tuple(packet(i) for i in range(2, 6))
    result = plan_pilot_budget(packets, envelopes(), as_of=NOW)
    assert result.status == "NOT_STARTED"
    assert result.reasons == ("canonical_market_type_ticker_currency_required",)


def test_packet_currency_must_match_its_canonical_instrument_identity():
    packets = (replace(packet(), currency="USD"),) + tuple(packet(i) for i in range(2, 6))
    result = plan_pilot_budget(packets, envelopes(), as_of=NOW)
    assert result.status == "NOT_STARTED" and result.reasons == ("instrument_currency_mismatch",)


@pytest.mark.parametrize("model_changes", [
    {"model_id": "different-unpriced-model"}, {"model_digest": "e" * 64},
])
def test_consistent_replicas_of_an_unreserved_model_cannot_complete_pilot(model_changes):
    packets, responses = complete_abstention_pilot()
    altered = tuple(replace(r, identity=replace(r.identity, **model_changes)) for r in responses)
    result = assess_pilot_completion(packets, altered, budget=budget(), calculated_cost_usd=D("0.1"))
    assert result.status == "INCOMPLETE"
    assert result.reasons == ("response_model_differs_from_reserved_manifest",)


def test_budget_cannot_be_reused_for_another_five_packet_cohort():
    _, responses = complete_abstention_pilot()
    other_packets = tuple(packet(i) for i in range(6, 11))
    other_responses = tuple(replace(r, identity=replace(r.identity, packet=other_packets[int(r.identity.packet.packet_id.split("-")[-1]) - 1]))
                            for r in responses)
    result = assess_pilot_completion(other_packets, other_responses, budget=budget(), calculated_cost_usd=D("0.1"))
    assert result.status == "INCOMPLETE"
    assert result.reasons == ("pilot_packets_differ_from_reserved_manifest",)


def test_budget_requires_original_manifest_and_rejects_modified_reservation():
    packets, responses = complete_abstention_pilot()
    original = budget()
    changed_rate = replace(original.manifest.envelopes[0].pricing, output_usd_per_million=D("2"))
    changed_envelope = replace(original.manifest.envelopes[0], pricing=changed_rate)
    changed_manifest = replace(original.manifest, envelopes=(changed_envelope,) + original.manifest.envelopes[1:])
    for altered in (replace(original, manifest=None),
                    replace(original, manifest=changed_manifest),
                    replace(original, required_reservation_usd=D("1")),
                    replace(original, required_packages=4)):
        result = assess_pilot_completion(packets, responses, budget=altered, calculated_cost_usd=D("0.1"))
        assert result.status == "INCOMPLETE"
        assert result.reasons[0] in {"registered_budget_manifest_required", "budget_does_not_match_registered_manifest"}


def test_each_role_is_bound_to_its_own_resolved_model():
    packets, responses = complete_abstention_pilot()
    plans = envelopes()
    custom = replace(plans[0], model_digest="e" * 64,
                     pricing=replace(plans[0].pricing, model_id="elliott-specific-model"))
    reserved = plan_pilot_budget(packets, (custom,) + plans[1:], as_of=NOW)
    aligned = tuple(replace(r, identity=replace(r.identity, model_id="elliott-specific-model", model_digest="e" * 64))
                    if r.agent == "elliott" else r for r in responses)
    assert assess_pilot_completion(packets, aligned, budget=reserved, calculated_cost_usd=D("0.1")).status == "COMPLETE"
    assert assess_pilot_completion(packets, responses, budget=reserved, calculated_cost_usd=D("0.1")).status == "INCOMPLETE"


def test_model_digest_is_required_to_reserve_calls():
    plans = envelopes()
    result = plan_pilot_budget(tuple(packet(i) for i in range(1, 6)),
                               (replace(plans[0], model_digest=""),) + plans[1:], as_of=NOW)
    assert result.status == "NOT_STARTED" and result.reasons == ("resolved_model_digest_required",)

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from src.technical_desk import pilot_protocol_v3 as legacy
from src.technical_desk import pilot_protocol_v31 as protocol


D = Decimal
NOW = datetime(2026, 11, 16, 12, tzinfo=timezone.utc)


def packet(number=1):
    return protocol.PacketSpec(
        f"packet-{number}", f"{number:064x}", f"BYMA:CEDEAR:ASSET{number}:ARS", "ARS",
        D("0.25"), ("pivot-a", "pivot-b", "pivot-c"), ("D", "W"), "READY",
        atr_by_timeframe=(("D", D("2")), ("W", D("5"))),
    )


def scenario(**changes):
    return replace(protocol.Scenario(
        "ELLIOTT", "D", "IMPULSE", (("origin", "pivot-a"),), "BULLISH", "PRIMARY", "WAVE_3",
        (("wave_1", "pivot-b"),),
        (protocol.Condition("CONFIRMATION", "CLOSE", "GE", D("100"), 2),
         protocol.Condition("INVALIDATION", "LOW", "LE", D("90"), 1)),
        (), 20, "Principal rationale",
    ), **changes)


def replicate(identifier="r1", **changes):
    identity = protocol.ExperimentIdentity(packet(), "fixed-model", "a" * 64, "b" * 64, "c" * 64, "d" * 64)
    return replace(protocol.Replicate(identity, identifier, "elliott", "D", "VALID", (scenario(),)), **changes)


def envelopes():
    tariff = protocol.PricingEvidence("fixed-model", "test-fixture://tariff", NOW - timedelta(hours=1),
                                      NOW + timedelta(hours=1), D("1"), D("1"), D("0"), True)
    return tuple(protocol.CallEnvelope(agent, 1000, 1000, tariff, "a" * 64) for agent in protocol.AGENTS)


def packets():
    return tuple(packet(number) for number in range(1, 6))


def budget(**changes):
    return protocol.plan_pilot_budget(packets(), envelopes(), as_of=NOW, **changes)


def kinds(result):
    return {change.kind for change in result.changes}


def test_new_version_does_not_relabel_preserved_v3():
    assert protocol.PROTOCOL_VERSION == "technical-desk-pilot-v3.1"
    assert protocol.SCENARIO_RULE_VERSION == "technical-desk-semantic-v3.1"
    assert legacy.PROTOCOL_VERSION == "technical-desk-pilot-v3"
    assert legacy.SCENARIO_RULE_VERSION == "technical-desk-semantic-v3"
    assert "TARGET_PRESENCE_CHANGE" not in legacy.ACTION_BY_CHANGE


def test_both_without_targets_compare_conditions_in_ticks_atr_and_never_retry():
    conditions = (replace(scenario().conditions[0], level=D("100.25")), scenario().conditions[1])
    result = protocol.compare_replicates(replicate(), replicate("r2", scenarios=(scenario(conditions=conditions),)))
    assert result.status == "STABLE" and result.errors == () and result.target_state == "NONE"
    assert result.matched_scenarios == 1 and kinds(result) == {"STABLE", "LEVEL_DISPERSION"}
    assert tuple(item.kind for item in result.level_dispersion) == ("CONFIRMATION", "INVALIDATION")
    assert result.level_dispersion[0].delta_ticks == (1,)
    assert result.level_dispersion[0].delta_atr == (D("0.125"),)
    assert all("RETRY" not in change.action for change in result.changes)


@pytest.mark.parametrize("left_targets,right_targets", [((), (D("110"),)), ((D("110"),), ())])
def test_one_target_set_present_is_structural_change_without_target_dispersion(left_targets, right_targets):
    result = protocol.compare_replicates(
        replicate(scenarios=(scenario(targets=left_targets),)),
        replicate("r2", scenarios=(scenario(targets=right_targets),)),
    )
    assert result.status == "NOISE_UNSTABLE" and result.target_state == "PRESENCE_CHANGED"
    assert kinds(result) == {"TARGET_PRESENCE_CHANGE", "LEVEL_DISPERSION"}
    assert tuple(item.kind for item in result.level_dispersion) == ("CONFIRMATION", "INVALIDATION")
    assert all("RETRY" not in change.action for change in result.changes)
    change = next(item for item in result.changes if item.kind == "TARGET_PRESENCE_CHANGE")
    assert "STRUCTURAL" in change.action


def test_present_targets_equal_cardinality_keep_continuous_sorted_rank_pairing():
    left = replicate(scenarios=(scenario(targets=(D("120"), D("110"))),))
    right = replicate("r2", scenarios=(scenario(targets=(D("110.25"), D("121"))),))
    result = protocol.compare_replicates(left, right)
    target = result.level_dispersion[-1]
    assert result.status == "STABLE" and result.target_state == "BOTH_PRESENT"
    assert target.kind == "TARGETS" and target.pairing_rule == "SORTED_TARGET_RANK_EQUAL_CARDINALITY"
    assert target.delta_ticks == (1, 4) and target.delta_atr == (D("0.125"), D("0.5"))
    assert target.unpaired_reason is None


def test_present_targets_unequal_cardinality_preserve_sets_without_pairs():
    result = protocol.compare_replicates(
        replicate(scenarios=(scenario(targets=(D("110"), D("120"))),)),
        replicate("r2", scenarios=(scenario(targets=(D("115"),)),)),
    )
    target = result.level_dispersion[-1]
    assert result.status == "STABLE" and result.target_state == "BOTH_PRESENT"
    assert target.before_ticks == (440, 480) and target.after_ticks == (460,)
    assert target.delta_ticks is None and target.delta_atr is None
    assert target.unpaired_reason == "TARGET_CARDINALITY_DIFFERS_NO_PAIRING"


def test_alternative_target_presence_is_archived_but_excluded():
    alternate = scenario(role="ALTERNATIVE", targets=(D("1000"),))
    left = replicate(scenarios=(scenario(), alternate))
    right = replicate("r2", scenarios=(scenario(), replace(alternate, targets=())))
    result = protocol.compare_replicates(left, right)
    assert result.status == "STABLE" and result.target_state == "NONE"
    assert result.excluded_alternatives == (1, 1)
    assert protocol.replicate_ledger_record(left)["response"]["scenarios"][1]["targets"] == (D("1000"),)


@pytest.mark.parametrize("change,expected", [
    ({"direction": "BEARISH"}, "DIRECTION_CHANGE"),
    ({"structure_state": "WAVE_5"}, "STRUCTURE_CHANGE"),
    ({"causal_anchors": (("origin", "pivot-c"),)}, "ANCHOR_CHANGE"),
])
def test_binary_axes_remain_active_without_targets(change, expected):
    result = protocol.compare_replicates(replicate(), replicate("r2", scenarios=(scenario(**change),)))
    assert result.status == "NOISE_UNSTABLE" and expected in kinds(result)
    assert result.target_state == "NONE"


def test_missing_atr_is_explicit_without_fabricating_target_metric():
    identity = replace(replicate().identity, packet=replace(packet(), atr_by_timeframe=()))
    result = protocol.compare_replicates(replicate(identity=identity), replicate("r2", identity=identity))
    assert result.status == "STABLE" and len(result.level_dispersion) == 2
    assert all(metric.delta_atr is None and metric.atr_reason == "PACKAGE_ATR_MISSING" for metric in result.level_dispersion)


def test_budget_preserves_five_packets_two_replicas_five_roles_two_dollars_no_authority():
    result = budget()
    assert result.status == "READY" and result.base_calls == 50 and result.maximum_attempts == 100
    assert result.required_packages == 5 and result.replicas_per_package == 2 and result.budget_usd == D("2")
    assert result.dispatch_authorized is False


@pytest.mark.parametrize("change", [{"debate_enabled": True}, {"retry_limit": 0}, {"budget_usd": D("2.01")}])
def test_fixed_pilot_limits_cannot_be_relaxed(change):
    assert budget(**change).status == "NOT_STARTED"


@pytest.mark.parametrize("missing", ["model", "tariff"])
def test_missing_model_or_tariff_never_starts(missing):
    original = envelopes()[0]
    changed = replace(original, pricing=None if missing == "tariff" else replace(original.pricing, model_id=""))
    assert protocol.plan_pilot_budget(packets(), (changed,) + envelopes()[1:], as_of=NOW).status == "NOT_STARTED"


def test_full_targetless_pilot_completes_in_separate_cohorts():
    responses = []
    for item in packets():
        identity = replace(replicate().identity, packet=item)
        for replica in ("r1", "r2"):
            for agent in protocol.AGENTS:
                method = protocol.METHOD_BY_AGENT[agent]
                family, roles = next((family, roles) for (m, family), roles in protocol.CAUSAL_ROLES.items() if m == method)
                causal = tuple((role, item.anchor_ids[i]) for i, role in enumerate(sorted(roles)))
                for tf in ("D", "W"):
                    output = scenario(method=method, family=family, causal_anchors=causal, timeframe=tf)
                    responses.append(protocol.Replicate(identity, replica, agent, tf, "VALID", (output,)))
    result = protocol.assess_pilot_completion(packets(), tuple(responses), budget=budget(), calculated_cost_usd=D("0.1"))
    assert result.status == "COMPLETE" and result.complete_packets == 5
    assert result.wyckoff.stable_pairs == 10 and result.other_roles.stable_pairs == 40

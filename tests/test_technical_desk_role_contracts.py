import copy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json

import pytest

from src.technical_desk.pilot_protocol_v3 import AGENTS, METHOD_BY_AGENT, PacketSpec
from src.technical_desk.role_contracts import (
    AnchorContext, ContractViolation, PacketContext, adapt_scenario_to_v3,
    adapt_scenario_to_v31, packet_spec_for_v31, parse_role_output, role_output_schema,
)


NOW = datetime(2026, 11, 15, 23, tzinfo=timezone.utc)


def context():
    anchors = tuple(AnchorContext(f"{tf}-{number}", tf, NOW - timedelta(days=10 - number),
                                 NOW - timedelta(days=8 - number))
                    for tf in ("D", "W") for number in range(3))
    packet = PacketSpec("packet-1", "a" * 64, "BYMA:CEDEAR:AMD:ARS", "ARS", Decimal("0.25"),
                        tuple(a.anchor_id for a in anchors), ("D", "W"), "READY")
    return PacketContext(packet, NOW, anchors)


def scenario(agent, tf):
    family = {"elliott": "IMPULSE", "wyckoff": "RANGE", "structure": "SWING_SEQUENCE",
              "momentum": "TREND", "moderator": "CONSENSUS"}[agent]
    anchors = ([{"role": "range_start", "anchor_id": f"{tf}-0"},
                {"role": "range_end", "anchor_id": f"{tf}-1"}] if agent == "wyckoff"
               else [{"role": "origin", "anchor_id": f"{tf}-0"}])
    return {"method": METHOD_BY_AGENT[agent], "timeframe": tf, "family": family,
            "causal_anchors": anchors, "direction": "BULLISH", "role": "PRIMARY",
            "structure_state": "CANDIDATE", "structure_anchors": [{"role": "end", "anchor_id": f"{tf}-2"}],
            "conditions": [{"kind": "CONFIRMATION", "field": "CLOSE", "operator": "GE", "level": "100.25", "persistence_bars": 2},
                           {"kind": "INVALIDATION", "field": "LOW", "operator": "LE", "level": "90", "persistence_bars": 1}],
            "targets": ["110", "120.25"], "horizon_sessions": 20,
            "uncertainty": "HIGH", "text": "Candidate with documented evidence"}


def payload(agent="elliott"):
    return {"contract_version": "technical-desk-role-output-v1", "packet_id": "packet-1", "packet_digest": "a" * 64,
            "agent": agent, "mode": "SHADOW_ONLY", "affects_analysis": False, "affects_execution": False,
            "horizon_unit": "BYMA_TRADING_SESSIONS",
            "volume_basis": "RELATIVE_UNVERIFIED" if agent == "wyckoff" else "NOT_USED",
            "evaluation_cohort": "WYCKOFF_RELATIVE_UNVERIFIED" if agent == "wyckoff" else "OTHER_ROLES",
            "results": [{"timeframe": tf, "state": "VALID", "scenarios": [scenario(agent, tf)],
                         "abstention_reason": None, "uncertainty": "HIGH"} for tf in ("D", "W")]}


def parse(value=None, agent="elliott", packet_context=None):
    return parse_role_output(json.dumps(payload(agent) if value is None else value),
                             expected_agent=agent, packet_context=packet_context or context())


@pytest.mark.parametrize("agent", AGENTS)
def test_exact_five_role_outputs_and_round_trip(agent):
    output = parse(agent=agent)
    assert {r.timeframe for r in output.results} == {"D", "W"}
    assert output.mode == "SHADOW_ONLY" and output.affects_analysis is False and output.affects_execution is False
    assert output.results[1].scenarios[0].horizon_sessions == 20
    assert output.horizon_unit == "BYMA_TRADING_SESSIONS"
    assert parse_role_output(output.model_dump_json(), expected_agent=agent, packet_context=context()) == output


def test_fixed_authority_is_generated_when_omitted():
    value = payload()
    for field in ("contract_version", "mode", "affects_analysis", "affects_execution", "horizon_unit"):
        del value[field]
    output = parse(value)
    assert output.mode == "SHADOW_ONLY" and output.affects_execution is False and output.affects_analysis is False


@pytest.mark.parametrize("level", ["root", "timeframe", "scenario", "condition", "anchor"])
def test_extra_fields_forbidden_at_every_object_level(level):
    value = payload()
    targets = {"root": value, "timeframe": value["results"][0], "scenario": value["results"][0]["scenarios"][0],
               "condition": value["results"][0]["scenarios"][0]["conditions"][0],
               "anchor": value["results"][0]["scenarios"][0]["causal_anchors"][0]}
    targets[level]["probability"] = 0.9
    with pytest.raises(ContractViolation, match="Extra inputs"):
        parse(value)


@pytest.mark.parametrize("field,bad", [("role", " "), ("role", " end"), ("role", "end "),
                                      ("anchor_id", " D-2"), ("anchor_id", "D-2 ")])
def test_anchor_text_rejects_blank_or_edge_whitespace(field, bad):
    value = payload()
    value["results"][0]["scenarios"][0]["structure_anchors"][0][field] = bad
    with pytest.raises(ContractViolation, match="anchor_text_must_be_nonempty_without_edge_whitespace"):
        parse(value)


@pytest.mark.parametrize("patch", [{"affects_analysis": True}, {"affects_execution": 0},
                                  {"mode": "LIVE"}, {"horizon_unit": "WEEKLY_BARS"}])
def test_authority_and_horizon_units_cannot_be_overridden(patch):
    value = payload()
    value.update(patch)
    with pytest.raises(ContractViolation):
        parse(value)


@pytest.mark.parametrize("raw", ['{"agent":"elliott","agent":"momentum"}',
                                 '{"nested":{"level":"10","level":"20"}}',
                                 '{"number":NaN}', '{"number":Infinity}', '{"number":-Infinity}',
                                 '{} trailing data'])
def test_non_json_values_and_duplicate_keys_rejected(raw):
    with pytest.raises(ContractViolation):
        parse_role_output(raw, expected_agent="elliott", packet_context=context())


@pytest.mark.parametrize("field,bad", [("horizon_sessions", True), ("horizon_sessions", "20"),
                                      ("horizon_sessions", 20.0), ("uncertainty", 0.9),
                                      ("targets", [110.0])])
def test_no_numeric_coercion_or_numeric_uncertainty(field, bad):
    value = payload()
    value["results"][0]["scenarios"][0][field] = bad
    with pytest.raises(ContractViolation):
        parse(value)


@pytest.mark.parametrize("price", [100.25, "100.1", "0", "-1", "1e2", "NaN", "Infinity"])
def test_exact_decimal_string_positive_tick_prices(price):
    value = payload()
    value["results"][0]["scenarios"][0]["conditions"][0]["level"] = price
    with pytest.raises(ContractViolation):
        parse(value)


def test_optional_targets_preserve_absence_and_do_not_fabricate_v3_output():
    value = payload()
    del value["results"][0]["scenarios"][0]["targets"]
    value["results"][1]["scenarios"][0]["targets"] = []
    output = parse(value)
    for result in output.results:
        scenario_output = result.scenarios[0]
        assert scenario_output.targets == ()
        adapter = adapt_scenario_to_v3(scenario_output, packet_context=context())
        assert adapter.status == "NOT_COMPATIBLE" and adapter.scenario is None
        assert adapter.reason == "V3_REQUIRES_TARGETS_ABSENT_IN_ROLE_OUTPUT"


def test_present_targets_adapt_to_unchanged_v3():
    output = parse()
    result = adapt_scenario_to_v3(output.results[0].scenarios[0], packet_context=context())
    assert result.status == "COMPATIBLE" and result.reason is None
    assert result.scenario.targets == (Decimal("110"), Decimal("120.25"))


@pytest.mark.parametrize("targets", [[], ["110", "120.25"]])
def test_current_v31_adapter_accepts_optional_targets_without_legacy_rejection(targets):
    from src.technical_desk import pilot_protocol_v31 as protocol

    value = payload()
    value["results"][0]["scenarios"][0]["targets"] = targets
    output = parse(value)
    adapted = adapt_scenario_to_v31(output.results[0].scenarios[0], packet_context=context())
    assert adapted.status == "COMPATIBLE" and adapted.reason is None
    assert adapted.scenario.targets == tuple(Decimal(target) for target in targets)
    assert adapted.packet == packet_spec_for_v31(context())
    identity = protocol.ExperimentIdentity(adapted.packet, "test-model", "a" * 64, "b" * 64, "c" * 64, "d" * 64)
    first = protocol.Replicate(identity, "r1", "elliott", "D", "VALID", (adapted.scenario,))
    result = protocol.compare_replicates(first, replace(first, replicate_id="r2"))
    assert result.status == "STABLE" and result.errors == ()
    assert result.target_state == ("BOTH_PRESENT" if targets else "NONE")


def test_same_family_elliott_primary_and_alternative_remain_complete():
    value = payload()
    alternate = copy.deepcopy(value["results"][0]["scenarios"][0])
    alternate.update(role="ALTERNATIVE", direction="BEARISH", structure_state="OTHER_COUNT")
    value["results"][0]["scenarios"].append(alternate)
    output = parse(value)
    assert len(output.results[0].scenarios) == 2
    assert output.results[0].scenarios[0].causal_anchors == output.results[0].scenarios[1].causal_anchors


def test_abstention_has_explicit_reason_and_qualitative_uncertainty():
    value = payload()
    value["results"][1].update(state="ABSTAIN", scenarios=[], abstention_reason="Insufficient completed weekly pivots", uncertainty="UNKNOWN")
    assert parse(value).results[1].state == "ABSTAIN"
    value["results"][1]["abstention_reason"] = " "
    with pytest.raises(ContractViolation, match="abstention_requires"):
        parse(value)


@pytest.mark.parametrize("mutation", ["second_primary", "only_alternative", "two_confirmation", "three_conditions",
                                     "wrong_method", "wrong_timeframe", "unknown_anchor", "wrong_anchor_timeframe",
                                     "wrong_packet", "duplicate_target", "duplicate_anchor_role", "no_aliases"])
def test_semantic_and_packet_validation(mutation):
    value = payload()
    first = value["results"][0]["scenarios"][0]
    if mutation == "second_primary":
        value["results"][0]["scenarios"].append(copy.deepcopy(first))
    elif mutation == "only_alternative":
        first["role"] = "ALTERNATIVE"
    elif mutation == "two_confirmation":
        first["conditions"][1]["kind"] = "CONFIRMATION"
    elif mutation == "three_conditions":
        first["conditions"].append(copy.deepcopy(first["conditions"][0]))
    elif mutation == "wrong_method":
        first["method"] = "WYCKOFF"
    elif mutation == "wrong_timeframe":
        first["timeframe"] = "W"
    elif mutation == "unknown_anchor":
        first["causal_anchors"][0]["anchor_id"] = "unknown"
    elif mutation == "wrong_anchor_timeframe":
        first["causal_anchors"][0]["anchor_id"] = "W-0"
    elif mutation == "wrong_packet":
        value["packet_digest"] = "b" * 64
    elif mutation == "duplicate_target":
        first["targets"] = ["110", "110.00"]
    elif mutation == "duplicate_anchor_role":
        first["structure_anchors"].append(copy.deepcopy(first["structure_anchors"][0]))
    elif mutation == "no_aliases":
        first["horizon_bars"] = first.pop("horizon_sessions")
    with pytest.raises(ContractViolation):
        parse(value)


def test_future_confirmation_and_bad_chronology_rejected():
    packet_context = context()
    future = replace(packet_context.anchors[0], confirmed_at=NOW + timedelta(days=1))
    with pytest.raises(ContractViolation, match="future_anchor"):
        parse(packet_context=replace(packet_context, anchors=(future,) + packet_context.anchors[1:]))
    value = payload("wyckoff")
    first = value["results"][0]["scenarios"][0]
    first["causal_anchors"][0]["anchor_id"] = "D-2"
    with pytest.raises(ContractViolation, match="chronology"):
        parse(value, agent="wyckoff")


def test_wyckoff_cannot_join_other_role_cohort_or_claim_verified_volume():
    value = payload("wyckoff")
    value["evaluation_cohort"] = "OTHER_ROLES"
    with pytest.raises(ContractViolation, match="cohort_mismatch"):
        parse(value, agent="wyckoff")
    value = payload("wyckoff")
    value["volume_basis"] = "ABSOLUTE_VERIFIED"
    with pytest.raises(ContractViolation):
        parse(value, agent="wyckoff")


def test_schema_export_forbids_extra_fields_and_numeric_prices():
    schema = role_output_schema()
    assert schema["$id"] == "technical-desk-role-output-v1"
    assert schema["additionalProperties"] is False
    assert all(item.get("additionalProperties") is False for item in schema["$defs"].values())
    assert schema["$defs"]["ConditionOutput"]["properties"]["level"]["type"] == "string"
    assert schema["properties"]["mode"]["const"] == "SHADOW_ONLY"

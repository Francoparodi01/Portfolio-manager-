"""Strict, non-operative role outputs for frozen D/W packets; no model I/O.

This is role-output-v1, not a change to semantic-v3. Empty targets are valid
here and in v3.1. The explicitly legacy v3 adapter still requires targets.
Horizon is always BYMA trading sessions; persistence_bars uses the timeframe.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from fractions import Fraction
import json
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from . import pilot_protocol_v3 as v3
from . import pilot_protocol_v31 as v31


CONTRACT_VERSION = "technical-desk-role-output-v1"
PriceText = Annotated[str, Field(pattern=r"^(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")]
Code = Annotated[str, Field(pattern=r"^[A-Z][A-Z0-9_]{0,63}$")]
Nonempty = Annotated[str, Field(min_length=1)]
PositiveInt = Annotated[int, Field(strict=True, gt=0)]


class ContractViolation(ValueError):
    """The response is not an interpretable, packet-bound shadow output."""


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)


class AnchorRef(StrictModel):
    role: Nonempty
    anchor_id: Nonempty

    @field_validator("role", "anchor_id")
    @classmethod
    def canonical_anchor_text(cls, value):
        if not value.strip() or value != value.strip():
            raise ValueError("anchor_text_must_be_nonempty_without_edge_whitespace")
        return value


class ConditionOutput(StrictModel):
    kind: Literal["CONFIRMATION", "INVALIDATION"]
    field: Literal["CLOSE", "HIGH", "LOW"]
    operator: Literal["GT", "GE", "LT", "LE", "EQ"]
    level: PriceText
    persistence_bars: PositiveInt


class ScenarioOutput(StrictModel):
    method: Literal["ELLIOTT", "WYCKOFF", "STRUCTURE", "MOMENTUM", "SYNTHESIS"]
    timeframe: Literal["D", "W"]
    family: Code
    causal_anchors: tuple[AnchorRef, ...]
    direction: Literal["BULLISH", "BEARISH", "NEUTRAL"]
    role: Literal["PRIMARY", "ALTERNATIVE"]
    structure_state: Code
    structure_anchors: tuple[AnchorRef, ...]
    conditions: Annotated[tuple[ConditionOutput, ...], Field(min_length=2, max_length=2)]
    targets: tuple[PriceText, ...] = ()
    horizon_sessions: PositiveInt
    uncertainty: Literal["LOW", "MODERATE", "HIGH", "UNKNOWN"]
    text: str

    @field_validator("causal_anchors", "structure_anchors", "conditions", "targets", mode="before")
    @classmethod
    def json_arrays(cls, value):
        return tuple(value) if isinstance(value, list) else value

    @model_validator(mode="after")
    def unique_roles_and_conditions(self):
        for anchors in (self.causal_anchors, self.structure_anchors):
            if len({item.role for item in anchors}) != len(anchors):
                raise ValueError("duplicate_anchor_role")
        if {item.kind for item in self.conditions} != {"CONFIRMATION", "INVALIDATION"}:
            raise ValueError("exactly_one_confirmation_and_invalidation_required")
        if len({Decimal(item) for item in self.targets}) != len(self.targets):
            raise ValueError("duplicate_target")
        return self


class TimeframeOutput(StrictModel):
    timeframe: Literal["D", "W"]
    state: Literal["VALID", "ABSTAIN"]
    scenarios: tuple[ScenarioOutput, ...]
    abstention_reason: str | None
    uncertainty: Literal["LOW", "MODERATE", "HIGH", "UNKNOWN"]

    @field_validator("scenarios", mode="before")
    @classmethod
    def json_array(cls, value):
        return tuple(value) if isinstance(value, list) else value

    @model_validator(mode="after")
    def response_state(self):
        if self.state == "ABSTAIN":
            if self.scenarios or not self.abstention_reason or not self.abstention_reason.strip():
                raise ValueError("abstention_requires_reason_and_no_scenarios")
        elif self.abstention_reason is not None or sum(s.role == "PRIMARY" for s in self.scenarios) != 1:
            raise ValueError("valid_response_requires_one_primary_and_no_abstention_reason")
        if any(s.timeframe != self.timeframe for s in self.scenarios):
            raise ValueError("scenario_timeframe_mismatch")
        return self


class RoleOutput(StrictModel):
    contract_version: Literal["technical-desk-role-output-v1"] = CONTRACT_VERSION
    packet_id: Nonempty
    packet_digest: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    agent: Literal["elliott", "wyckoff", "structure", "momentum", "moderator"]
    mode: Literal["SHADOW_ONLY"] = "SHADOW_ONLY"
    affects_analysis: Literal[False] = False
    affects_execution: Literal[False] = False
    horizon_unit: Literal["BYMA_TRADING_SESSIONS"] = "BYMA_TRADING_SESSIONS"
    volume_basis: Literal["RELATIVE_UNVERIFIED", "NOT_USED"]
    evaluation_cohort: Literal["WYCKOFF_RELATIVE_UNVERIFIED", "OTHER_ROLES"]
    results: Annotated[tuple[TimeframeOutput, ...], Field(min_length=2, max_length=2)]

    @field_validator("affects_analysis", "affects_execution", mode="before")
    @classmethod
    def fixed_authority(cls, value):
        if value is not False:
            raise ValueError("authority_flags_are_fixed_false")
        return False

    @field_validator("results", mode="before")
    @classmethod
    def json_array(cls, value):
        return tuple(value) if isinstance(value, list) else value

    @model_validator(mode="after")
    def role_metadata(self):
        if {r.timeframe for r in self.results} != {"D", "W"}:
            raise ValueError("one_daily_and_one_weekly_result_required")
        expected_volume = "RELATIVE_UNVERIFIED" if self.agent == "wyckoff" else "NOT_USED"
        if self.volume_basis != expected_volume or self.evaluation_cohort != v3.evaluation_cohort(self.agent):
            raise ValueError("role_volume_and_evaluation_cohort_mismatch")
        if any(s.method != v3.METHOD_BY_AGENT[self.agent] for r in self.results for s in r.scenarios):
            raise ValueError("scenario_method_role_mismatch")
        return self


@dataclass(frozen=True)
class AnchorContext:
    anchor_id: str
    timeframe: str
    pivot_time: datetime
    confirmed_at: datetime


@dataclass(frozen=True)
class PacketContext:
    packet: v3.PacketSpec
    cutoff: datetime
    anchors: tuple[AnchorContext, ...]


def _aware(value: object) -> bool:
    return isinstance(value, datetime) and value.tzinfo is not None and value.utcoffset() is not None


def _context_anchors(context: PacketContext) -> dict[str, AnchorContext]:
    if not isinstance(context, PacketContext):
        raise ContractViolation("packet_context_required")
    v3._validate_packet(context.packet)
    if not _aware(context.cutoff):
        raise ContractViolation("timezone_aware_cutoff_required")
    result = {}
    for anchor in context.anchors:
        if not isinstance(anchor, AnchorContext) or anchor.anchor_id not in context.packet.anchor_ids:
            raise ContractViolation("unknown_context_anchor")
        if anchor.anchor_id in result or anchor.timeframe not in {"D", "W"}:
            raise ContractViolation("invalid_context_anchor_identity")
        if not _aware(anchor.pivot_time) or not _aware(anchor.confirmed_at):
            raise ContractViolation("timezone_aware_anchor_times_required")
        if not anchor.pivot_time < anchor.confirmed_at <= context.cutoff:
            raise ContractViolation("unconfirmed_or_future_anchor")
        result[anchor.anchor_id] = anchor
    if set(result) != set(context.packet.anchor_ids):
        raise ContractViolation("packet_anchor_context_incomplete")
    return result


def _validate_scenario(scenario: ScenarioOutput, context: PacketContext, anchors: dict) -> None:
    required_roles = v3.CAUSAL_ROLES.get((scenario.method, scenario.family))
    if required_roles is None or {a.role for a in scenario.causal_anchors} != required_roles:
        raise ContractViolation("unsupported_family_or_causal_roles")
    for anchor in scenario.causal_anchors + scenario.structure_anchors:
        if anchor.anchor_id not in anchors or anchors[anchor.anchor_id].timeframe != scenario.timeframe:
            raise ContractViolation("unknown_or_wrong_timeframe_anchor")
    causal = {a.role: anchors[a.anchor_id] for a in scenario.causal_anchors}
    if "range_start" in causal and causal["range_start"].pivot_time >= causal["range_end"].pivot_time:
        raise ContractViolation("range_anchor_chronology_invalid")
    if "origin" in causal and any(anchors[a.anchor_id].pivot_time < causal["origin"].pivot_time
                                  for a in scenario.structure_anchors):
        raise ContractViolation("structure_anchor_precedes_origin")
    for price in tuple(c.level for c in scenario.conditions) + scenario.targets:
        value = Decimal(price)
        if value <= 0 or (Fraction(value) / Fraction(context.packet.tick_size)).denominator != 1:
            raise ContractViolation("positive_price_on_packet_tick_required")


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ContractViolation("duplicate_json_key")
        result[key] = value
    return result


def _nonfinite(value):
    raise ContractViolation("nonfinite_json_number")


def parse_role_output(raw: str, *, expected_agent: str, packet_context: PacketContext) -> RoleOutput:
    """Validate exact JSON plus authoritative packet context, without I/O.

    Default authority fields are generated by code. Supplied values must match
    the constants exactly; omission never grants authority. No field aliases.
    """
    if type(raw) is not str or expected_agent not in v3.AGENTS:
        raise ContractViolation("json_text_and_known_expected_agent_required")
    try:
        payload = json.loads(raw, object_pairs_hook=_unique_object, parse_constant=_nonfinite)
        output = RoleOutput.model_validate(payload)
        if output.agent != expected_agent:
            raise ContractViolation("unexpected_agent")
        packet = packet_context.packet
        if output.packet_id != packet.packet_id or output.packet_digest != packet.digest:
            raise ContractViolation("packet_identity_mismatch")
        anchors = _context_anchors(packet_context)
        for result in output.results:
            for scenario in result.scenarios:
                _validate_scenario(scenario, packet_context, anchors)
        return output
    except (ValueError, TypeError, AttributeError) as exc:
        if isinstance(exc, ContractViolation):
            raise
        raise ContractViolation(str(exc)) from exc


def role_output_schema() -> dict:
    """JSON Schema handles shape; packet bindings/chronology require parser."""
    schema = RoleOutput.model_json_schema()
    schema["$id"] = CONTRACT_VERSION
    schema["description"] = (
        "Five-role SHADOW_ONLY output. Prices are exact decimal strings. "
        "Horizons are BYMA trading sessions, including W scenarios. "
        "Contextual constraints require parse_role_output."
    )
    return schema


@dataclass(frozen=True)
class V3ScenarioAdapter:
    status: str
    reason: str | None
    scenario: v3.Scenario | None


def adapt_scenario_to_v3(scenario: ScenarioOutput, *, packet_context: PacketContext) -> V3ScenarioAdapter:
    """Legacy adapter: do not fabricate targets for the preserved v3 comparator."""
    anchors = _context_anchors(packet_context)
    _validate_scenario(scenario, packet_context, anchors)
    if not scenario.targets:
        return V3ScenarioAdapter("NOT_COMPATIBLE", "V3_REQUIRES_TARGETS_ABSENT_IN_ROLE_OUTPUT", None)
    adapted = v3.Scenario(
        method=scenario.method, timeframe=scenario.timeframe, family=scenario.family,
        causal_anchors=tuple((a.role, a.anchor_id) for a in scenario.causal_anchors),
        direction=scenario.direction, role=scenario.role, structure_state=scenario.structure_state,
        structure_anchors=tuple((a.role, a.anchor_id) for a in scenario.structure_anchors),
        conditions=tuple(v3.Condition(c.kind, c.field, c.operator, Decimal(c.level), c.persistence_bars)
                         for c in scenario.conditions),
        targets=tuple(Decimal(t) for t in scenario.targets), horizon_sessions=scenario.horizon_sessions,
        text=scenario.text,
    )
    v3.scenario_fingerprint(adapted, packet_context.packet)
    return V3ScenarioAdapter("COMPATIBLE", None, adapted)


def packet_spec_for_v31(packet_context: PacketContext) -> v31.PacketSpec:
    """Bind the new version to the same validated, frozen market evidence."""
    _context_anchors(packet_context)
    return v31.PacketSpec(**asdict(packet_context.packet))


@dataclass(frozen=True)
class V31ScenarioAdapter:
    status: str
    reason: str | None
    scenario: v31.Scenario
    packet: v31.PacketSpec


def adapt_scenario_to_v31(scenario: ScenarioOutput, *, packet_context: PacketContext) -> V31ScenarioAdapter:
    """Current adapter: an absent target set is valid and remains absent."""
    anchors = _context_anchors(packet_context)
    _validate_scenario(scenario, packet_context, anchors)
    packet = v31.PacketSpec(**asdict(packet_context.packet))
    adapted = v31.Scenario(
        method=scenario.method, timeframe=scenario.timeframe, family=scenario.family,
        causal_anchors=tuple((a.role, a.anchor_id) for a in scenario.causal_anchors),
        direction=scenario.direction, role=scenario.role, structure_state=scenario.structure_state,
        structure_anchors=tuple((a.role, a.anchor_id) for a in scenario.structure_anchors),
        conditions=tuple(v31.Condition(c.kind, c.field, c.operator, Decimal(c.level), c.persistence_bars)
                         for c in scenario.conditions),
        targets=tuple(Decimal(t) for t in scenario.targets), horizon_sessions=scenario.horizon_sessions,
        text=scenario.text,
    )
    v31.scenario_fingerprint(adapted, packet)
    return V31ScenarioAdapter("COMPATIBLE", None, adapted, packet)

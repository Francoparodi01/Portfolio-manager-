"""V3 primary-only comparison and whole-pilot budget; no I/O or dispatch.

Rates are caller-supplied evidence, never estimates supplied by this module.
An approved reservation is a feasibility result, not spending authorization.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal, localcontext
from fractions import Fraction
import hashlib
import json
import re


PROTOCOL_VERSION = "technical-desk-pilot-v3"
SCENARIO_RULE_VERSION = "technical-desk-semantic-v3"
AGENTS = ("elliott", "wyckoff", "structure", "momentum", "moderator")
METHOD_BY_AGENT = dict(zip(AGENTS, ("ELLIOTT", "WYCKOFF", "STRUCTURE", "MOMENTUM", "SYNTHESIS")))
PACKAGES_REQUIRED = 5
REPLICATES_REQUIRED = 2
RETRY_LIMIT = 1
BUDGET_CAP_USD = Decimal("2")
# Families describe causal setups, not predicted directions or primary ranking.
CAUSAL_ROLES = {
    ("ELLIOTT", "IMPULSE"): frozenset({"origin"}),
    ("ELLIOTT", "CORRECTION"): frozenset({"origin"}),
    ("WYCKOFF", "RANGE"): frozenset({"range_start", "range_end"}),
    ("STRUCTURE", "SWING_SEQUENCE"): frozenset({"origin"}),
    ("STRUCTURE", "RANGE"): frozenset({"range_start", "range_end"}),
    ("MOMENTUM", "DIVERGENCE"): frozenset({"price_start", "indicator_start"}),
    ("MOMENTUM", "TREND"): frozenset({"origin"}),
    ("SYNTHESIS", "CONSENSUS"): frozenset({"origin"}),
}
ACTION_BY_CHANGE = {
    "STABLE": "RETAIN_PAIR_FOR_DIAGNOSTIC_REPORT",
    "TEXT_ONLY": "RETAIN_SEMANTICS_AND_ARCHIVE_BOTH_TEXTS",
    "LEVEL_DISPERSION": "REPORT_CONTINUOUS_TICKS_AND_PACKAGE_ATR_WITHOUT_BINARY_THRESHOLD",
    "STRUCTURE_CHANGE": "QUARANTINE_PAIR_AND_REPORT_STRUCTURE_VARIABILITY",
    "DIRECTION_CHANGE": "QUARANTINE_PAIR_AND_REPORT_UNSTABLE_DIRECTION",
    # Reserved: PRIMARY-to-PRIMARY comparisons cannot independently observe a
    # role change. Alternative contents never enter any comparison calculation.
    "ROLE_CHANGE": "QUARANTINE_PAIR_AND_REPORT_ROLE_VARIABILITY",
    "ANCHOR_CHANGE": "QUARANTINE_PAIR_AND_REPORT_ANCHOR_VARIABILITY",
    "ABSTENTION_CHANGE": "QUARANTINE_PAIR_AND_REPORT_ABSTENTION_VARIABILITY",
    "BOTH_ABSTAIN": "COLLECT_MISSING_EVIDENCE",
    "NOT_COMPARABLE": "MARK_INCOMPLETE_UNLESS_REGISTERED_RETRY_REPAIRS_RESPONSE",
    "INVALID_EXPERIMENT": "INVALIDATE_PAIR_AND_REQUIRE_NEW_REGISTERED_EXPERIMENT",
}


class ProtocolViolation(ValueError):
    """An input cannot be interpreted without inventing missing semantics."""


@dataclass(frozen=True)
class PacketSpec:
    packet_id: str
    digest: str
    instrument_id: str
    currency: str
    tick_size: Decimal
    anchor_ids: tuple[str, ...]
    validated_timeframes: tuple[str, ...]
    quality_status: str
    volume_basis: str = "RELATIVE_UNVERIFIED"
    volume_quality_status: str = "PARTIAL"
    atr_by_timeframe: tuple[tuple[str, Decimal], ...] = ()
    operating_mode: str = "SHADOW_ONLY"


@dataclass(frozen=True)
class ExperimentIdentity:
    """Frozen configuration and market packet for a repeated run.

    prompt_digest hashes static instructions/templates. The moderator's four
    analyst inputs are archived separately per replica by the future ledger;
    its comparison measures end-to-end synthesis stability, not isolated noise.
    """

    packet: PacketSpec
    model_id: str
    model_digest: str
    prompt_digest: str
    rules_digest: str
    parameters_digest: str
    protocol_version: str = PROTOCOL_VERSION


@dataclass(frozen=True)
class Condition:
    kind: str
    field: str
    operator: str
    level: Decimal
    persistence_bars: int


@dataclass(frozen=True)
class Scenario:
    method: str
    timeframe: str
    family: str
    causal_anchors: tuple[tuple[str, str], ...]
    direction: str
    role: str
    structure_state: str
    structure_anchors: tuple[tuple[str, str], ...]
    conditions: tuple[Condition, ...]
    targets: tuple[Decimal, ...]
    horizon_sessions: int
    text: str = ""


@dataclass(frozen=True)
class Replicate:
    identity: ExperimentIdentity
    replicate_id: str
    agent: str
    timeframe: str
    state: str
    scenarios: tuple[Scenario, ...] = ()
    abstention_reason: str = ""
    text: str = ""


@dataclass(frozen=True)
class Change:
    kind: str
    action: str
    matching_key: str | None = None
    before_fingerprint: str | None = None
    after_fingerprint: str | None = None


@dataclass(frozen=True)
class LevelDispersion:
    """Two-replica distances; target ranks pair only at equal cardinality.

    A zero and a one-tick distance have identical binary status. Missing ATR
    or unmatched target sets remain explicit nulls, never substituted values.
    """

    kind: str
    before_ticks: tuple[int, ...]
    after_ticks: tuple[int, ...]
    delta_ticks: tuple[int, ...] | None
    absolute_delta_ticks: tuple[int, ...] | None
    delta_atr: tuple[Decimal, ...] | None
    absolute_delta_atr: tuple[Decimal, ...] | None
    atr: Decimal | None
    atr_reason: str | None
    pairing_rule: str
    unpaired_reason: str | None = None


@dataclass(frozen=True)
class Comparison:
    status: str
    changes: tuple[Change, ...]
    matched_scenarios: int = 0
    added_keys: tuple[str, ...] = ()
    removed_keys: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()
    market_change_claim: bool = False
    evaluation_cohort: str = "UNKNOWN"
    level_dispersion: tuple[LevelDispersion, ...] = ()
    excluded_alternatives: tuple[int, int] = (0, 0)


def _require(test: bool, message: str) -> None:
    if not test:
        raise ProtocolViolation(message)


def _text(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip()) and value == value.strip()


def _decimal(value: object, *, positive: bool) -> Decimal:
    _require(isinstance(value, Decimal), "decimal_required_no_float_coercion")
    _require(value.is_finite(), "nonfinite_decimal")
    _require(value > 0 if positive else value >= 0, "invalid_decimal_sign")
    return value


def _positive_int(value: object) -> bool:
    return type(value) is int and value > 0


def _digest(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _hash(value: object) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _validate_packet(packet: PacketSpec) -> None:
    _require(isinstance(packet, PacketSpec), "packet_missing_or_invalid")
    _require(all(_text(v) for v in (packet.packet_id, packet.instrument_id, packet.currency)), "packet_identity_missing")
    identity_parts = packet.instrument_id.split(":")
    _require(len(identity_parts) == 4
             and all(part and not any(char.isspace() for char in part) for part in identity_parts)
             and packet.instrument_id == ":".join(part.strip().upper() for part in identity_parts),
             "canonical_market_type_ticker_currency_required")
    _require(identity_parts[3] == packet.currency, "instrument_currency_mismatch")
    _require(_digest(packet.digest), "packet_digest_invalid")
    _decimal(packet.tick_size, positive=True)
    _require(isinstance(packet.anchor_ids, tuple) and bool(packet.anchor_ids), "anchors_required")
    _require(all(_text(v) for v in packet.anchor_ids), "anchor_id_invalid")
    _require(len(set(packet.anchor_ids)) == len(packet.anchor_ids), "duplicate_packet_anchor")
    _require(isinstance(packet.validated_timeframes, tuple) and len(packet.validated_timeframes) == 2 and set(packet.validated_timeframes) == {"D", "W"}, "complete_daily_weekly_packet_required")
    _require(packet.quality_status == "READY", "packet_not_ready")
    _require(packet.operating_mode == "SHADOW_ONLY", "shadow_only_required")
    _require(packet.volume_basis == "RELATIVE_UNVERIFIED"
             and packet.volume_quality_status == "PARTIAL", "v3_relative_unverified_volume_contract_required")


def _validate_identity(identity: ExperimentIdentity) -> None:
    _require(isinstance(identity, ExperimentIdentity), "experiment_identity_missing")
    _validate_packet(identity.packet)
    _require(identity.protocol_version == PROTOCOL_VERSION, "unsupported_protocol")
    _require(_text(identity.model_id), "resolved_model_required")
    _require(all(_digest(v) for v in (identity.model_digest, identity.prompt_digest, identity.rules_digest, identity.parameters_digest)), "experiment_digest_invalid")


def _anchors(values: object, packet: PacketSpec) -> tuple[tuple[str, str], ...]:
    _require(isinstance(values, tuple), "anchor_pairs_must_be_tuple")
    _require(all(isinstance(v, tuple) and len(v) == 2 and all(_text(x) for x in v) for v in values), "invalid_anchor_pair")
    _require(len({v[0] for v in values}) == len(values), "duplicate_anchor_role")
    _require(all(v[1] in packet.anchor_ids for v in values), "unknown_packet_anchor")
    return tuple(sorted(values))


def _ticks(value: Decimal, packet: PacketSpec) -> int:
    ratio = Fraction(_decimal(value, positive=True)) / Fraction(packet.tick_size)
    _require(ratio.denominator == 1, "level_not_on_packet_tick")
    return ratio.numerator


def _scenario_parts(scenario: Scenario, packet: PacketSpec) -> dict:
    _validate_packet(packet)
    _require(isinstance(scenario, Scenario), "invalid_scenario")
    _require(scenario.timeframe in {"D", "W"}, "unsupported_timeframe")
    roles = CAUSAL_ROLES.get((scenario.method, scenario.family))
    _require(roles is not None, "unsupported_method_family")
    causal = _anchors(scenario.causal_anchors, packet)
    _require({v[0] for v in causal} == roles, "causal_anchor_roles_mismatch")
    _require(scenario.direction in {"BULLISH", "BEARISH", "NEUTRAL"}, "invalid_direction")
    _require(scenario.role in {"PRIMARY", "ALTERNATIVE"}, "invalid_scenario_role")
    _require(isinstance(scenario.structure_state, str) and re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", scenario.structure_state) is not None, "structure_state_must_be_code")
    structure = _anchors(scenario.structure_anchors, packet)
    _require(_positive_int(scenario.horizon_sessions), "invalid_horizon")
    _require(isinstance(scenario.text, str), "scenario_text_invalid")
    _require(isinstance(scenario.conditions, tuple) and bool(scenario.conditions), "conditions_required")
    conditions = []
    for condition in scenario.conditions:
        _require(isinstance(condition, Condition), "invalid_condition")
        _require(condition.kind in {"CONFIRMATION", "INVALIDATION"}, "invalid_condition_kind")
        _require(condition.field in {"CLOSE", "HIGH", "LOW"}, "invalid_condition_field")
        _require(condition.operator in {"GT", "GE", "LT", "LE", "EQ"}, "invalid_condition_operator")
        _require(_positive_int(condition.persistence_bars), "invalid_condition_persistence")
        conditions.append((condition.kind, condition.field, condition.operator, condition.persistence_bars, _ticks(condition.level, packet)))
    _require({c[0] for c in conditions} == {"CONFIRMATION", "INVALIDATION"}, "confirmation_and_invalidation_required")
    # This version has one predicate of each kind. Compound AND/OR rules need
    # a versioned expression schema; prose cannot supply executable semantics.
    _require(len(conditions) == 2, "exactly_one_condition_per_kind_required")
    _require(len(set(conditions)) == len(conditions), "duplicate_condition")
    _require(isinstance(scenario.targets, tuple) and bool(scenario.targets), "targets_required")
    targets = tuple(sorted(_ticks(v, packet) for v in scenario.targets))
    _require(len(set(targets)) == len(targets), "duplicate_target")
    return {
        "rule_version": SCENARIO_RULE_VERSION,
        "packet": (packet.digest, packet.instrument_id, packet.currency, Fraction(packet.tick_size).numerator, Fraction(packet.tick_size).denominator),
        "key": (packet.instrument_id, packet.currency, scenario.method,
                scenario.timeframe, scenario.family, causal),
        "direction": scenario.direction,
        "role": scenario.role,
        "structure": (scenario.structure_state, structure, scenario.horizon_sessions),
        "conditions": tuple(sorted(conditions)),
        "targets": targets,
    }


def scenario_matching_key(scenario: Scenario, packet: PacketSpec) -> str:
    """Exclude direction/role/levels/text so semantic changes remain visible."""
    parts = _scenario_parts(scenario, packet)
    return _hash((SCENARIO_RULE_VERSION, parts["key"]))


def scenario_fingerprint(scenario: Scenario, packet: PacketSpec) -> str:
    """Fingerprint exact executable semantics; decimal spelling is irrelevant."""
    return _hash(_scenario_parts(scenario, packet))


def _replicate_scenarios(replicate: Replicate) -> dict[str, tuple[Scenario, dict]]:
    _require(_text(replicate.replicate_id), "replicate_id_required")
    _require(replicate.agent in AGENTS and replicate.timeframe in {"D", "W"}, "invalid_comparison_group")
    _require(isinstance(replicate.text, str), "replicate_text_invalid")
    _require(isinstance(replicate.scenarios, tuple), "scenarios_must_be_tuple")
    _require(replicate.state in {"VALID", "ABSTAIN"}, "invalid_or_failed_response")
    if replicate.state == "ABSTAIN":
        _require(not replicate.scenarios and _text(replicate.abstention_reason), "invalid_abstention")
        return {}
    _require(bool(replicate.scenarios) and not replicate.abstention_reason, "valid_response_requires_scenarios")
    mapped = {}
    for scenario in replicate.scenarios:
        parts = _scenario_parts(scenario, replicate.identity.packet)
        _require(scenario.method == METHOD_BY_AGENT[replicate.agent] and scenario.timeframe == replicate.timeframe, "scenario_group_mismatch")
        # Alternatives remain fully archived, including identical causal keys.
        # Only principal scenarios enter matching and numerical dispersion.
        if scenario.role == "ALTERNATIVE":
            continue
        key = _hash((SCENARIO_RULE_VERSION, parts["key"]))
        _require(key not in mapped, "ambiguous_duplicate_matching_key")
        mapped[key] = (scenario, parts)
    _require(sum(s.role == "PRIMARY" for s in replicate.scenarios) == 1, "exactly_one_primary_required")
    return mapped


def _change(kind: str, key=None, before=None, after=None) -> Change:
    return Change(kind, ACTION_BY_CHANGE[kind], key, before, after)


def evaluation_cohort(agent: str) -> str:
    if agent == "wyckoff":
        return "WYCKOFF_RELATIVE_UNVERIFIED"
    return "OTHER_ROLES" if agent in AGENTS else "UNKNOWN"


def replicate_ledger_record(replicate: Replicate) -> dict:
    """Return the complete archive record without writing or dropping alternates.

    The future ledger serializes Decimal/date values without float coercion.
    This function deliberately preserves every field of the original response.
    """
    _require(isinstance(replicate, Replicate), "invalid_response_record")
    return {
        "protocol_version": PROTOCOL_VERSION,
        "evaluation_cohort": evaluation_cohort(replicate.agent),
        "noise_scope": "PRIMARY_ONLY",
        "response": asdict(replicate),
    }


def _packet_atr(packet: PacketSpec, timeframe: str) -> tuple[Decimal | None, str | None]:
    values = packet.atr_by_timeframe
    if not isinstance(values, tuple) or any(not isinstance(v, tuple) or len(v) != 2 for v in values):
        return None, "INVALID_PACKAGE_ATR_SCHEMA"
    matching = [value for name, value in values if name == timeframe]
    if not matching:
        return None, "PACKAGE_ATR_MISSING"
    if len(matching) != 1:
        return None, "AMBIGUOUS_PACKAGE_ATR"
    atr = matching[0]
    if not isinstance(atr, Decimal) or not atr.is_finite() or atr <= 0:
        return None, "PACKAGE_ATR_MUST_BE_FINITE_AND_POSITIVE_DECIMAL"
    return atr, None


def _level_dispersion(old: dict, new: dict, packet: PacketSpec, timeframe: str) -> tuple[LevelDispersion, ...]:
    atr, atr_reason = _packet_atr(packet, timeframe)
    old_conditions = {condition[0]: (condition[-1],) for condition in old["conditions"]}
    new_conditions = {condition[0]: (condition[-1],) for condition in new["conditions"]}
    metrics = []
    for kind in ("CONFIRMATION", "INVALIDATION", "TARGETS"):
        before = old["targets"] if kind == "TARGETS" else old_conditions[kind]
        after = new["targets"] if kind == "TARGETS" else new_conditions[kind]
        pairing = "SORTED_TARGET_RANK_EQUAL_CARDINALITY" if kind == "TARGETS" else "CONDITION_KIND"
        delta = tuple(b - a for a, b in zip(before, after)) if len(before) == len(after) else None
        delta_atr = None
        if delta is not None and atr is not None:
            with localcontext() as context:
                context.prec = 34
                delta_atr = tuple(Decimal(v) * packet.tick_size / atr for v in delta)
        metrics.append(LevelDispersion(
            kind, before, after, delta, tuple(abs(v) for v in delta) if delta is not None else None,
            delta_atr, tuple(abs(v) for v in delta_atr) if delta_atr is not None else None,
            atr, atr_reason, pairing, "TARGET_CARDINALITY_DIFFERS_NO_PAIRING" if delta is None else None,
        ))
    return tuple(metrics)


def compare_replicates(left: Replicate | None, right: Replicate | None) -> Comparison:
    """Classify only PRIMARY structure/direction/role/anchors.

    Numeric levels always have continuous diagnostics, even when all distances
    are zero. STABLE/TEXT_ONLY never assert numerical equality of levels.
    Role is always PRIMARY in this scope: ROLE_CHANGE remains reserved, while
    a newly selected principal is measured by its structure/direction/anchors.
    """
    cohort = evaluation_cohort(left.agent) if isinstance(left, Replicate) else "UNKNOWN"
    try:
        _require(isinstance(left, Replicate) and isinstance(right, Replicate), "missing_replicate")
        _validate_identity(left.identity)
        _validate_identity(right.identity)
        if left.identity != right.identity or left.replicate_id == right.replicate_id:
            return Comparison("INVALID_EXPERIMENT", (_change("INVALID_EXPERIMENT"),), errors=("experiment_changed_or_replicate_reused",), evaluation_cohort=cohort)
        _require((left.agent, left.timeframe) == (right.agent, right.timeframe), "agent_or_timeframe_mismatch")
        before, after = _replicate_scenarios(left), _replicate_scenarios(right)
    except (ProtocolViolation, TypeError, AttributeError) as exc:
        return Comparison("NOT_COMPARABLE", (_change("NOT_COMPARABLE"),), errors=(str(exc),), evaluation_cohort=cohort)
    excluded = tuple(sum(s.role == "ALTERNATIVE" for s in r.scenarios) for r in (left, right))
    context = dict(evaluation_cohort=cohort, excluded_alternatives=excluded)
    if left.state == right.state == "ABSTAIN":
        return Comparison("INSUFFICIENT_EVIDENCE", (_change("BOTH_ABSTAIN"),), **context)
    if left.state != right.state:
        return Comparison("INSUFFICIENT_EVIDENCE", (_change("ABSTENTION_CHANGE"),), **context)
    added, removed = tuple(sorted(after.keys() - before.keys())), tuple(sorted(before.keys() - after.keys()))
    # Exactly one primary on each side makes direct comparison deterministic,
    # including changed causal anchors that change the semantic matching key.
    key, (old_scenario, old) = next(iter(before.items()))
    _, (new_scenario, new) = next(iter(after.items()))
    old_fp, new_fp = _hash(old), _hash(new)
    changes = []
    if old["direction"] != new["direction"]:
        changes.append(_change("DIRECTION_CHANGE", key, old_fp, new_fp))
    if old["key"][-1] != new["key"][-1] or old["structure"][1] != new["structure"][1]:
        changes.append(_change("ANCHOR_CHANGE", key, old_fp, new_fp))
    if (old["key"][4], old["structure"][0], old["structure"][2], tuple(c[:-1] for c in old["conditions"])) != (
            new["key"][4], new["structure"][0], new["structure"][2], tuple(c[:-1] for c in new["conditions"])):
        changes.append(_change("STRUCTURE_CHANGE", key, old_fp, new_fp))
    dispersions = _level_dispersion(old, new, left.identity.packet, left.timeframe)
    context["level_dispersion"] = dispersions
    diagnostic = _change("LEVEL_DISPERSION", key, old_fp, new_fp)
    if changes:
        return Comparison("NOISE_UNSTABLE", tuple(changes) + (diagnostic,), 1, added, removed, **context)
    status = "TEXT_ONLY" if old_scenario.text != new_scenario.text else "STABLE"
    return Comparison(status, (_change(status), diagnostic), 1, added, removed, **context)


@dataclass(frozen=True)
class PricingEvidence:
    model_id: str
    source: str
    verified_at: datetime
    valid_until: datetime
    input_usd_per_million: Decimal
    output_usd_per_million: Decimal
    extra_usd_per_attempt: Decimal
    covers_all_billable_usage: bool
    currency: str = "USD"


@dataclass(frozen=True)
class CallEnvelope:
    agent: str
    max_billable_input_tokens: int
    max_billable_output_tokens: int
    pricing: PricingEvidence
    model_digest: str


@dataclass(frozen=True)
class PilotManifest:
    """The exact sample and per-role model/pricing/token reservation inputs."""

    packets: tuple[PacketSpec, ...]
    envelopes: tuple[CallEnvelope, ...]
    pricing_as_of: datetime


@dataclass(frozen=True)
class PilotBudget:
    status: str
    required_packages: int = PACKAGES_REQUIRED
    replicas_per_package: int = REPLICATES_REQUIRED
    base_calls: int = 50
    maximum_attempts: int = 100
    required_reservation_usd: Decimal | None = None
    budget_usd: Decimal | None = None
    reasons: tuple[str, ...] = ()
    dispatch_authorized: bool = False
    observed_cost_usd: Decimal | None = None
    manifest: PilotManifest | None = None


def _aware(value: object) -> bool:
    return isinstance(value, datetime) and value.tzinfo is not None and value.utcoffset() is not None


def _fraction_decimal(value: Fraction) -> Decimal:
    """Exact conversion for terminating decimal money, independent of context."""
    denominator, twos, fives = value.denominator, 0, 0
    while denominator % 2 == 0:
        denominator //= 2
        twos += 1
    while denominator % 5 == 0:
        denominator //= 5
        fives += 1
    _require(denominator == 1, "nonterminating_money")
    scale = max(twos, fives)
    integer = value.numerator * (2 ** (scale - twos)) * (5 ** (scale - fives))
    return Decimal((int(integer < 0), tuple(int(c) for c in str(abs(integer))), -scale))


def plan_pilot_budget(
    packets: tuple[PacketSpec, ...],
    envelopes: tuple[CallEnvelope, ...],
    *,
    as_of: datetime,
    budget_usd: Decimal = BUDGET_CAP_USD,
    retry_limit: int = RETRY_LIMIT,
    debate_enabled: bool = False,
) -> PilotBudget:
    """Require all 5x2x5 calls plus their retries to fit before any first call.

    Token bounds cover the entire request/response (including reasoning and
    image tokens when token-billed). Other charges require an explicit bound
    in extra_usd_per_attempt. No cache discount is presumed.
    """
    try:
        _require(_aware(as_of), "aware_pricing_cutoff_required")
        _decimal(budget_usd, positive=True)
        _require(budget_usd <= BUDGET_CAP_USD, "budget_exceeds_usd_2")
        _require(type(debate_enabled) is bool and not debate_enabled, "debate_excluded_from_pilot")
        _require(type(retry_limit) is int and retry_limit == RETRY_LIMIT, "one_retry_reserve_required")
        _require(isinstance(packets, tuple) and len(packets) == PACKAGES_REQUIRED, "five_complete_packets_required")
        for packet in packets:
            _validate_packet(packet)
        _require(len({p.packet_id for p in packets}) == PACKAGES_REQUIRED and len({p.digest for p in packets}) == PACKAGES_REQUIRED, "duplicate_pilot_packet")
        _require(len({(p.instrument_id, p.currency) for p in packets}) == PACKAGES_REQUIRED, "five_distinct_instruments_required")
        _require(isinstance(envelopes, tuple) and len(envelopes) == len(AGENTS), "five_role_envelopes_required")
        _require(all(isinstance(e, CallEnvelope) for e in envelopes), "invalid_call_envelope")
        _require({e.agent for e in envelopes} == set(AGENTS), "exactly_one_envelope_per_role_required")
        one_replica = Fraction(0)
        for envelope in envelopes:
            _require(_positive_int(envelope.max_billable_input_tokens) and _positive_int(envelope.max_billable_output_tokens), "positive_billable_token_bounds_required")
            _require(_digest(envelope.model_digest), "resolved_model_digest_required")
            rate = envelope.pricing
            _require(isinstance(rate, PricingEvidence), "pricing_evidence_required")
            _require(_text(rate.model_id) and _text(rate.source), "resolved_model_and_pricing_source_required")
            _require(rate.currency == "USD" and rate.covers_all_billable_usage is True, "complete_usd_pricing_required")
            _require(_aware(rate.verified_at) and _aware(rate.valid_until), "pricing_dates_required")
            _require(rate.verified_at <= as_of <= rate.valid_until, "pricing_not_valid_at_cutoff")
            input_rate = Fraction(_decimal(rate.input_usd_per_million, positive=False))
            output_rate = Fraction(_decimal(rate.output_usd_per_million, positive=False))
            extra = Fraction(_decimal(rate.extra_usd_per_attempt, positive=False))
            one_replica += (envelope.max_billable_input_tokens * input_rate + envelope.max_billable_output_tokens * output_rate) / 1_000_000 + extra
        required = _fraction_decimal(one_replica * PACKAGES_REQUIRED * REPLICATES_REQUIRED * (1 + RETRY_LIMIT))
        if required > budget_usd:
            return PilotBudget("NOT_STARTED", required_reservation_usd=required, budget_usd=budget_usd, reasons=("whole_pilot_including_retries_exceeds_budget",))
        manifest = PilotManifest(packets, envelopes, as_of)
        return PilotBudget("READY", required_reservation_usd=required, budget_usd=budget_usd, manifest=manifest)
    except (ProtocolViolation, TypeError, AttributeError) as exc:
        return PilotBudget("NOT_STARTED", budget_usd=budget_usd if isinstance(budget_usd, Decimal) and budget_usd.is_finite() else None, reasons=(str(exc),))


@dataclass(frozen=True)
class CohortEvaluation:
    evaluation_cohort: str
    stable_pairs: int = 0
    unstable_pairs: int = 0
    insufficient_evidence_pairs: int = 0


@dataclass(frozen=True)
class PilotCompletion:
    status: str
    complete_packets: int = 0
    other_roles: CohortEvaluation = CohortEvaluation("OTHER_ROLES")
    wyckoff: CohortEvaluation = CohortEvaluation("WYCKOFF_RELATIVE_UNVERIFIED")
    reasons: tuple[str, ...] = ()


def assess_pilot_completion(
    packets: tuple[PacketSpec, ...],
    responses: tuple[Replicate, ...],
    *,
    budget: PilotBudget,
    calculated_cost_usd: Decimal | None,
) -> PilotCompletion:
    """Check 100 D/W results from 50 logical calls; never infer missing replies.

    Inputs are the archived final responses of each logical call. Attempts,
    actual usage, billing reconciliation and dispatch limits belong to the
    future ledger/orchestrator; this function cannot attest those by itself.
    """
    try:
        _require(isinstance(budget, PilotBudget) and budget.status == "READY", "whole_pilot_reservation_required")
        _decimal(budget.budget_usd, positive=True)
        _decimal(budget.required_reservation_usd, positive=False)
        _require(budget.required_reservation_usd <= budget.budget_usd <= BUDGET_CAP_USD, "invalid_budget_reservation")
        _require(isinstance(budget.manifest, PilotManifest), "registered_budget_manifest_required")
        manifest = budget.manifest
        _require(packets == manifest.packets, "pilot_packets_differ_from_reserved_manifest")
        reconstructed = plan_pilot_budget(manifest.packets, manifest.envelopes,
                                          as_of=manifest.pricing_as_of, budget_usd=budget.budget_usd)
        _require(reconstructed == budget, "budget_does_not_match_registered_manifest")
        model_by_agent = {e.agent: (e.pricing.model_id, e.model_digest) for e in manifest.envelopes}
        _require(isinstance(packets, tuple) and len(packets) == PACKAGES_REQUIRED, "five_complete_packets_required")
        for packet in packets:
            _validate_packet(packet)
        _require(len({p.packet_id for p in packets}) == PACKAGES_REQUIRED
                 and len({p.digest for p in packets}) == PACKAGES_REQUIRED
                 and len({(p.instrument_id, p.currency) for p in packets}) == PACKAGES_REQUIRED,
                 "five_distinct_packets_required")
        _require(isinstance(responses, tuple), "response_tuple_required")
        expected_packets = {p.packet_id: p for p in packets}
        indexed = {}
        for response in responses:
            _require(isinstance(response, Replicate), "invalid_response_record")
            _validate_identity(response.identity)
            _require(model_by_agent.get(response.agent) == (response.identity.model_id, response.identity.model_digest),
                     "response_model_differs_from_reserved_manifest")
            packet = response.identity.packet
            _require(expected_packets.get(packet.packet_id) == packet, "response_packet_mismatch")
            _replicate_scenarios(response)
            key = (packet.packet_id, response.replicate_id, response.agent, response.timeframe)
            _require(key not in indexed, "duplicate_response_group")
            indexed[key] = response
        replicas = sorted({r.replicate_id for r in responses})
        _require(len(replicas) == REPLICATES_REQUIRED, "exactly_two_registered_replicas_required")
        complete = 0
        cohorts = {name: dict(stable_pairs=0, unstable_pairs=0, insufficient_evidence_pairs=0)
                   for name in ("OTHER_ROLES", "WYCKOFF_RELATIVE_UNVERIFIED")}
        for packet in packets:
            required = {(packet.packet_id, r, a, t) for r in replicas for a in AGENTS for t in ("D", "W")}
            if not required <= indexed.keys():
                continue
            for agent in AGENTS:
                for replica in replicas:
                    daily = indexed[(packet.packet_id, replica, agent, "D")]
                    weekly = indexed[(packet.packet_id, replica, agent, "W")]
                    _require(daily.identity == weekly.identity, "daily_weekly_call_identity_mismatch")
                for timeframe in ("D", "W"):
                    pair = compare_replicates(
                        indexed[(packet.packet_id, replicas[0], agent, timeframe)],
                        indexed[(packet.packet_id, replicas[1], agent, timeframe)])
                    _require(pair.status not in {"NOT_COMPARABLE", "INVALID_EXPERIMENT"}, "invalid_registered_pair")
                    cohort = cohorts[pair.evaluation_cohort]
                    cohort["stable_pairs"] += pair.status in {"STABLE", "TEXT_ONLY"}
                    cohort["unstable_pairs"] += pair.status == "NOISE_UNSTABLE"
                    cohort["insufficient_evidence_pairs"] += pair.status == "INSUFFICIENT_EVIDENCE"
            complete += 1
        counts = dict(complete_packets=complete,
                      other_roles=CohortEvaluation("OTHER_ROLES", **cohorts["OTHER_ROLES"]),
                      wyckoff=CohortEvaluation("WYCKOFF_RELATIVE_UNVERIFIED", **cohorts["WYCKOFF_RELATIVE_UNVERIFIED"]))
        if complete != PACKAGES_REQUIRED:
            return PilotCompletion("INCOMPLETE", **counts, reasons=("five_complete_paired_daily_weekly_packets_required",))
        if calculated_cost_usd is None:
            return PilotCompletion("COST_UNVERIFIED", **counts, reasons=("calculated_usage_cost_missing",))
        _decimal(calculated_cost_usd, positive=False)
        if calculated_cost_usd > budget.required_reservation_usd:
            return PilotCompletion("BUDGET_BREACH", **counts, reasons=("calculated_cost_exceeds_reserved_maximum",))
        return PilotCompletion("COMPLETE", **counts)
    except (ProtocolViolation, TypeError, AttributeError) as exc:
        return PilotCompletion("INCOMPLETE", reasons=(str(exc),))

"""Bitemporal evidence and immutable experiment contracts.

Payloads are canonical JSON strings to avoid shallow-frozen mutable dictionaries.
Decimal quantities/cash retain the evidence's precision; statistical returns are
converted to finite floats only at the reporting boundary.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator


def canonical(value: Any) -> str:
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json")
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False,
                      default=lambda x: x.isoformat() if isinstance(x, datetime) else str(x))


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


class Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    @field_validator("*", mode="after")
    @classmethod
    def utc(cls, value):
        return value.astimezone(timezone.utc) if isinstance(value, datetime) else value


SourceQuality = Literal["POINT_IN_TIME_SAFE", "RECONSTRUCTIBLE", "APPROXIMATE", "CURRENT_STATE_ONLY", "UNSAFE_FOR_REPLAY"]
ReplayMode = Literal["HISTORICAL_POLICY_REPLAY", "CURRENT_POLICY_ON_HISTORICAL_DATA", "RECORDED_PLAN_EVALUATION"]
OutcomeStatus = Literal["MATURE", "PENDING", "UNAVAILABLE", "INVALID", "NOT_APPLICABLE"]


class Evidence(Frozen):
    kind: Literal["PORTFOLIO", "BAR", "UNIVERSE", "MACRO", "FX", "SENTIMENT", "NEWS", "EVENT", "CORPORATE_ACTION", "ACTION_COVERAGE", "FEATURES", "PLAN", "CONFIG", "FILL", "HUMAN_COVERAGE"]
    record_id: str = Field(min_length=1)
    effective_at: AwareDatetime
    available_at: AwareDatetime
    revision_at: AwareDatetime | None = None
    source: str = Field(min_length=1)
    quality: SourceQuality
    owner: int | None = None
    payload_json: str

    @field_validator("payload_json")
    @classmethod
    def normalized(cls, value):
        data = json.loads(value, parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
        if not isinstance(data, dict):
            raise ValueError("evidence payload must be an object")
        return canonical(data)

    @model_validator(mode="after")
    def no_labels_in_decision_evidence(self):
        if self.kind in {"FILL", "HUMAN_COVERAGE"}:
            return self
        def check(value):
            if isinstance(value, dict):
                for key, item in value.items():
                    if key.startswith(("outcome_", "executable_outcome_", "forward_return", "future_return")) or key in {"was_correct", "label_timeout", "closed_at"}:
                        raise ValueError("future outcome labels cannot enter decision evidence")
                    check(item)
            elif isinstance(value, list):
                for item in value:
                    check(item)
        check(self.payload)
        return self

    @property
    def payload(self) -> dict:
        return json.loads(self.payload_json)

    @property
    def content_hash(self) -> str:
        return digest(self)

    def known_at(self, cutoff: datetime) -> bool:
        return self.available_at <= cutoff and (self.revision_at is None or self.revision_at <= cutoff)


class Session(Frozen):
    session_id: str
    open_at: AwareDatetime
    close_at: AwareDatetime

    @model_validator(mode="after")
    def clocks(self):
        if self.close_at <= self.open_at:
            raise ValueError("session must close after opening")
        return self


class Dataset(Frozen):
    records: tuple[Evidence, ...]
    sessions: tuple[Session, ...]
    calendar_version: str

    @model_validator(mode="after")
    def unique(self):
        keys = [(e.kind, e.record_id, e.available_at, e.revision_at) for e in self.records]
        if len(set(keys)) != len(keys):
            raise ValueError("duplicate evidence version")
        if len({s.session_id for s in self.sessions}) != len(self.sessions):
            raise ValueError("duplicate session")
        ordered = sorted(self.sessions, key=lambda s: s.open_at)
        if any(a.close_at >= b.open_at for a, b in zip(ordered, ordered[1:])):
            raise ValueError("sessions overlap")
        return self


class Position(Frozen):
    ticker: str
    quantity: Decimal = Field(ge=0)
    mark_ars: Decimal = Field(gt=0)
    lot_size: Decimal = Field(default=Decimal(1), gt=0)
    sector: str | None = None


class QualityAssessment(Frozen):
    level: Literal["HIGH", "MEDIUM", "LOW", "INVALID"]
    primary_eligible: bool
    components_json: str
    missing_fields: tuple[str, ...] = ()
    reconstruction_assumptions: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()


class HistoricalState(Frozen):
    as_of: AwareDatetime
    owner: int
    state_id: str
    opportunity_id: str
    records: tuple[Evidence, ...]
    positions: tuple[Position, ...]
    cash_ars: Decimal = Field(ge=0)
    capital_base_ars: Decimal = Field(gt=0)
    portfolio_snapshot_id: str
    market_snapshot_id: str
    feature_snapshot_id: str | None
    universe_snapshot_id: str | None
    universe_quality: Literal["EXACT", "RECONSTRUCTED", "APPROXIMATE", "UNKNOWN"]
    input_hash: str
    quality: QualityAssessment


class StrategySpec(Frozen):
    strategy_version: str
    adapter: Literal["quantia_core_v1", "hold_v1", "recorded_plan_v1"]
    available_at: AwareDatetime
    code_version: str
    config_hash: str
    config_available_at: AwareDatetime
    planner_version: str
    optimizer_version: str
    risk_policy_version: str
    model_version: str = "none"
    training_end: AwareDatetime | None = None
    implementation_hash: str


class Order(Frozen):
    ticker: str
    side: Literal["BUY", "SELL"]
    quantity: Decimal = Field(ge=0)
    reference_price: Decimal = Field(ge=0)
    target_amount_ars: Decimal = Field(ge=0)
    executable: bool
    blocked: bool = False
    action: str
    current_weight: float | None = None
    target_weight: float | None = None
    reason: str = ""
    restriction: str | None = None
    priority: int = 0
    funded_by: tuple[str, ...] = ()

    @model_validator(mode="after")
    def executable_price(self):
        if self.executable and not self.blocked and self.quantity > 0 and self.reference_price <= 0:
            raise ValueError("executable order requires a positive reference price")
        return self


class FrozenPlan(Frozen):
    strategy: StrategySpec
    mode: ReplayMode
    state_id: str
    orders: tuple[Order, ...]
    affected_tickers: tuple[str, ...]
    mechanism_json: str
    feasible: bool
    plan_hash: str


class CostModel(Frozen):
    version: str = "decision-lab-ars-v1"
    fee_bps: Decimal = Field(default=Decimal(75), ge=0)
    half_spread_bps: Decimal = Field(default=Decimal(0), ge=0)
    slippage_bps: Decimal = Field(default=Decimal(0), ge=0)
    tax_bps: Decimal = Field(default=Decimal(0), ge=0)
    fx_bps: Decimal = Field(default=Decimal(0), ge=0)
    minimum_trade_ars: Decimal = Field(default=Decimal(0), ge=0)
    reporting_currency: Literal["ARS"] = "ARS"
    cash_convention: Literal["NOMINAL_ARS_NO_INTEREST"] = "NOMINAL_ARS_NO_INTEREST"
    return_basis: Literal["PRICE_ONLY", "TOTAL_RETURN"] = "PRICE_ONLY"
    rounding: Literal["FLOOR_TO_LOT"] = "FLOOR_TO_LOT"


class Episode(Frozen):
    episode_id: str
    state: HistoricalState
    plan: FrozenPlan
    cost_model: CostModel
    experiment_id: str
    split: Literal["TRAIN", "VALIDATION", "HOLDOUT", "EXPLORATORY"]
    quality: QualityAssessment
    alternatives_json: str


class AlternativeOutcome(Frozen):
    episode_id: str
    alternative: str
    horizon: int = Field(gt=0)
    evaluated_as_of: AwareDatetime
    status: OutcomeStatus
    reason: str | None = None
    entry_at: AwareDatetime | None = None
    exit_at: AwareDatetime | None = None
    capital_base_ars: Decimal
    gross_return: float | None = None
    net_return: float | None = None
    cost_drag: float | None = None
    turnover: float | None = None
    ending_nav_ars: Decimal | None = None
    costs_ars: Decimal | None = None
    executed_orders_json: str = "[]"
    ending_positions_json: str = "{}"
    evidence_hashes: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    quality: Literal["HIGH", "MEDIUM", "LOW", "INVALID"]
    primary_eligible: bool = False


class Experiment(Frozen):
    experiment_id: str
    registered_at: AwareDatetime
    split: Literal["TRAIN", "VALIDATION", "HOLDOUT", "EXPLORATORY"] = "EXPLORATORY"
    confirmatory: bool = False
    train_end: AwareDatetime | None = None
    validation_end: AwareDatetime | None = None
    evaluation_start: AwareDatetime | None = None
    evaluation_end: AwareDatetime | None = None
    horizons: tuple[int, ...] = (5, 10, 20, 40)
    bootstrap_resamples: int = Field(default=5000, ge=100, le=100000)
    bootstrap_block_sessions: int = Field(default=20, ge=1)
    seed: int = 42
    confidence: float = Field(default=.95, gt=0, lt=1)
    minimum_n: int = Field(default=30, ge=2)
    family: tuple[str, ...] = ()

    @model_validator(mode="after")
    def chronology(self):
        if not self.horizons or len(set(self.horizons)) != len(self.horizons) or any(h < 1 for h in self.horizons):
            raise ValueError("unique positive horizons required")
        boundaries = [x for x in (self.train_end, self.validation_end, self.evaluation_start, self.evaluation_end) if x is not None]
        if any(a >= b for a, b in zip(boundaries, boundaries[1:])):
            raise ValueError("training, validation and evaluation windows must be strictly ordered")
        if self.confirmatory and (not self.family or not self.evaluation_start or self.registered_at >= self.evaluation_start):
            raise ValueError("confirmation requires a family preregistered before evaluation")
        return self

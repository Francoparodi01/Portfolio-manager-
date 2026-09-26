"""Immutable input contracts. Unknown evidence is never a numeric zero."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, FiniteFloat, field_validator, model_validator


class Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    @field_validator("*", mode="after")
    @classmethod
    def utc_clocks(cls, value):
        return value.astimezone(timezone.utc) if isinstance(value, datetime) else value


def canonical(value):
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json")
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


class CostScenario(Frozen):
    name: str
    bps: FiniteFloat = Field(ge=0)


class CostPolicy(Frozen):
    version: str = "cost-v2"
    scenarios: tuple[CostScenario, ...] = (
        CostScenario(name="ZERO", bps=0), CostScenario(name="CURRENT_VIABILITY", bps=75),
        CostScenario(name="RESEARCH_BASE", bps=150), CostScenario(name="STRESS", bps=250),
        CostScenario(name="SEVERE", bps=400),
    )

    @model_validator(mode="after")
    def unique(self):
        names = [s.name for s in self.scenarios]
        if len(set(names)) != len(names) or not {"RESEARCH_BASE", "STRESS"} <= set(names):
            raise ValueError("unique scenarios including RESEARCH_BASE and STRESS required")
        return self


class GatePolicy(Frozen):
    version: str = "gate-policy-v2"
    reportable: int = Field(default=30, ge=1)
    provisional_effective: int = Field(default=20, ge=1)
    confirmed_episodes: int = Field(default=100, ge=1)
    confirmed_effective: int = Field(default=60, ge=1)
    minimum_coverage: float = Field(default=.9, ge=0, le=1)
    minimum_ic: float = Field(default=.03, ge=0, le=1)
    minimum_ev: float = Field(default=.0025, ge=0)
    max_top1: float = Field(default=.40, ge=0, le=1)
    max_top3: float = Field(default=.75, ge=0, le=1)
    max_drawdown: float = Field(default=.25, ge=0, le=1)
    max_q: float = Field(default=.10, ge=0, le=1)
    stable_periods: int = Field(default=3, ge=1)
    swaps_revalidated: bool = False


class AnalyticsPolicy(Frozen):
    version: str = "analytics-v2"
    experiment_id: str
    evaluated_as_of: AwareDatetime
    metric_policy_version: str = "metric-v2"
    dedup_policy_version: str = "episode-v2"
    costs: CostPolicy = CostPolicy()
    gates: GatePolicy = GatePolicy()
    bootstrap_seed: int = 1729
    bootstrap_resamples: int = Field(default=5000, ge=20)
    bootstrap_block_length: int = Field(default=20, ge=1)
    block_sensitivity: tuple[int, ...] = (5, 10, 20, 40)
    confidence_level: float = Field(default=.95, gt=0, lt=1)
    horizons: tuple[int, ...] = (1, 5, 10, 20, 40)
    primary_hypotheses: tuple[str, ...] = ()
    preregistered_at: AwareDatetime | None = None

    @model_validator(mode="after")
    def valid_policy(self):
        if any(h < 1 for h in (*self.horizons, *self.block_sensitivity)):
            raise ValueError("positive horizons and blocks required")
        if len(set(self.horizons)) != len(self.horizons):
            raise ValueError("duplicate horizons")
        if len(set(self.primary_hypotheses)) != len(self.primary_hypotheses):
            raise ValueError("duplicate hypotheses")
        if self.primary_hypotheses and self.preregistered_at is None:
            raise ValueError("primary family requires preregistered_at")
        return self


class Evidence(Frozen):
    available_at: AwareDatetime
    ingested_at: AwareDatetime
    sealed_at: AwareDatetime
    source_id: str = Field(min_length=1)

    def visible(self, cutoff: datetime):
        return max(self.available_at, self.ingested_at, self.sealed_at) <= cutoff


class RecommendationFact(Evidence):
    recommendation_id: str
    account_id: str
    plan_id: str | None = None
    source_module: Literal["CORE", "RADAR", "SWAP", "MANUAL"]
    cohort: str
    instrument_id: str
    ticker: str
    direction: Literal["BUY", "SELL", "NEUTRAL", "ABSTAIN"]
    score: FiniteFloat | None = None
    reference_notional: FiniteFloat | None = Field(default=None, gt=0)
    decision_as_of: AwareDatetime
    native_horizon: int | None = Field(default=None, ge=1)
    benchmark_id: str | None = None
    price_series_id: str | None = None
    original_instrument_id: str | None = None
    expiry_at: AwareDatetime | None = None
    explicit_close_or_reopen: bool = False
    eligible: bool = True
    ambiguous: bool = False
    ambiguity_reason: str | None = None

    @model_validator(mode="after")
    def valid_rec(self):
        if self.expiry_at and self.expiry_at <= self.decision_as_of:
            raise ValueError("expiry must follow decision")
        if self.ambiguous and not self.ambiguity_reason:
            raise ValueError("ambiguity requires reason")
        if self.source_module == "SWAP" and (not self.original_instrument_id or self.direction != "BUY"):
            raise ValueError("SWAP requires BUY replacement and original instrument")
        return self


class Session(Evidence):
    session_id: str
    open_at: AwareDatetime
    close_at: AwareDatetime

    @model_validator(mode="after")
    def ordered(self):
        if self.close_at <= self.open_at:
            raise ValueError("session close must follow open")
        return self


class Bar(Evidence):
    series_id: str
    instrument_id: str
    session_id: str
    revision: int = Field(ge=0)
    open: FiniteFloat = Field(gt=0)
    close: FiniteFloat = Field(gt=0)
    currency: str
    adjustment_basis: str


class HumanAction(Evidence):
    recommendation_id: str
    instrument_id: str
    decision_as_of: AwareDatetime
    action: Literal["FOLLOW", "IGNORE", "CONTRARY", "MODIFIED", "UNKNOWN"]
    ambiguous: bool = False
    ambiguity_reason: str | None = None


class ExecutionFact(Evidence):
    broker_fill_id: str
    account_id: str
    instrument_id: str
    currency: str
    execution_plan_id: str | None = None
    broker_order_id: str | None = None
    recommendation_id: str | None = None
    quantity: FiniteFloat = Field(gt=0)
    side: Literal["BUY", "SELL"]
    fill_price: FiniteFloat = Field(gt=0)
    reference_price: FiniteFloat | None = Field(default=None, gt=0)
    reference_kind: Literal["DECISION", "EXECUTION_MARKET", "WINDOW", "UNKNOWN"] = "UNKNOWN"
    fees: FiniteFloat | None = Field(default=None, ge=0)
    taxes: FiniteFloat | None = Field(default=None, ge=0)
    other_costs: FiniteFloat | None = Field(default=None, ge=0)
    fill_at: AwareDatetime


class EconomicPeriod(Evidence):
    account_id: str
    currency: str
    start: AwareDatetime
    end: AwareDatetime
    beginning_nav: FiniteFloat
    ending_nav: FiniteFloat
    net_external_flows: FiniteFloat | None = None
    realized_pnl: FiniteFloat | None = None
    unrealized_pnl_change: FiniteFloat | None = None
    fees: FiniteFloat | None = Field(default=None, ge=0)
    taxes: FiniteFloat | None = Field(default=None, ge=0)
    financing: FiniteFloat | None = Field(default=None, ge=0)
    other_costs: FiniteFloat | None = Field(default=None, ge=0)
    average_capital_deployed: FiniteFloat | None = Field(default=None, gt=0)
    coverage_confirmed: bool = False
    evidence_ids: tuple[str, ...] = ()

    @model_validator(mode="after")
    def ordered(self):
        if self.end <= self.start:
            raise ValueError("economic end must follow start")
        return self


class NavPoint(Evidence):
    account_id: str
    currency: str
    at: AwareDatetime
    nav: FiniteFloat = Field(gt=0)
    gross_notional: FiniteFloat = Field(ge=0)
    net_notional: FiniteFloat
    # A curve is admissible only with an explicit sizing and flow convention.
    sizing_policy: str
    kind: Literal["ACTUAL", "BOT_SHADOW", "HUMAN_SHADOW"]
    cohort: str | None = None
    horizon_days: int | None = Field(default=None, ge=1)
    periods_per_year: int = Field(default=252, ge=1)
    risk_free_per_period: FiniteFloat = 0
    mar_per_period: FiniteFloat = 0
    net_external_flow: FiniteFloat | None = None
    flow_timing: Literal["NONE", "PERIOD_END", "SUBPERIOD_TWR", "UNKNOWN"] = "UNKNOWN"
    flow_adjusted_return: FiniteFloat | None = None
    reconciled: bool


class Dataset(Frozen):
    label: str
    recommendations: tuple[RecommendationFact, ...] = ()
    sessions: tuple[Session, ...] = ()
    bars: tuple[Bar, ...] = ()
    human_actions: tuple[HumanAction, ...] = ()
    executions: tuple[ExecutionFact, ...] = ()
    economic_periods: tuple[EconomicPeriod, ...] = ()
    nav_points: tuple[NavPoint, ...] = ()

    def as_of(self, cutoff):
        fields = {}
        event_fields = {"recommendations": "decision_as_of", "human_actions": "decision_as_of",
                        "executions": "fill_at", "economic_periods": "end", "nav_points": "at"}
        for name in type(self).model_fields:
            if name == "label":
                continue
            fields[name] = tuple(row for row in getattr(self, name) if row.visible(cutoff)
                                 and (name not in event_fields or getattr(row, event_fields[name]) <= cutoff))
        return Dataset(label=self.label, **fields)

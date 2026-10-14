"""Typed QEEF contracts extend Decision Lab's immutable evidence primitives."""
from __future__ import annotations

from typing import Literal
from pydantic import AwareDatetime, Field, model_validator
from src.decision_lab.models import Frozen, Order, digest

Status = Literal['VERIFIED', 'PARTIAL', 'INSUFFICIENT_DATA', 'NOT_IDENTIFIABLE', 'INVALID']


class Gate(Frozen):
    status: Status
    reasons: tuple[str, ...] = ()

    @model_validator(mode='after')
    def reason_required(self):
        if self.status != 'VERIFIED' and not self.reasons:
            raise ValueError('non-verified result requires reasons')
        return self


class Condition(Frozen):
    condition_id: str = Field(min_length=1)
    field: str = Field(min_length=1)
    operator: Literal['LT', 'LE', 'GT', 'GE', 'EQ']
    value: float
    unit: str = Field(min_length=1)
    source: str = Field(min_length=1)


class Thesis(Frozen):
    owner: int
    opportunity_id: str = Field(min_length=1)
    thesis_id: str = Field(min_length=1)
    ticker: str = Field(min_length=1)
    strategy_version: str = Field(min_length=1)
    thesis_opened_at: AwareDatetime
    recorded_at: AwareDatetime
    intended_horizon: int = Field(gt=0)
    horizon_unit: Literal['BYMA_SESSIONS', 'CALENDAR_DAYS', 'EVENT']
    invalidation_conditions: tuple[Condition, ...]
    target_conditions: tuple[Condition, ...]
    source_references: tuple[str, ...] = Field(min_length=1)
    evidence_quality: Literal['HIGH', 'LOW', 'UNKNOWN']
    formation_kind: Literal['PROSPECTIVE', 'HISTORICAL_INFERENCE'] = 'PROSPECTIVE'

    @model_validator(mode='after')
    def valid(self):
        if not self.owner or self.recorded_at < self.thesis_opened_at:
            raise ValueError('exact owner and valid recording chronology required')
        if self.formation_kind == 'HISTORICAL_INFERENCE' and self.evidence_quality == 'HIGH':
            raise ValueError('historical inference cannot be HIGH')
        if not self.invalidation_conditions or not self.target_conditions:
            raise ValueError('objective frozen target and invalidation conditions required')
        ids = [c.condition_id for c in self.invalidation_conditions + self.target_conditions]
        if len(set(ids)) != len(ids):
            raise ValueError('condition identifiers must be unique')
        return self

    @property
    def content_hash(self):
        return digest(self)


class ThesisEvent(Frozen):
    owner: int
    thesis_id: str
    thesis_hash: str
    event_id: str
    effective_at: AwareDatetime
    available_at: AwareDatetime
    event_type: Literal['UPDATE', 'TARGET', 'INVALIDATION', 'POSITION_CLOSED', 'REGIME_CHANGE', 'EXPIRED']
    decision_id: str | None = None
    recommendation_id: str | None = None
    plan_id: str | None = None
    execution_id: str | None = None
    condition_id: str | None = None
    observed_value: float | None = None
    observed_unit: str | None = None
    observed_source: str | None = None
    elapsed_sessions: int | None = Field(default=None, ge=0)
    calendar_reference: str | None = None
    source_references: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode='after')
    def chronology(self):
        if self.available_at < self.effective_at:
            raise ValueError('lifecycle event cannot be observed before occurrence')
        return self


class Benchmark(Frozen):
    name: Literal['REBALANCE_BASELINE', 'ELIGIBLE_ALTERNATIVE']
    owner: int
    state_id: str
    defined_at: AwareDatetime
    selection_available_at: AwareDatetime
    policy_version: str = Field(min_length=1)
    policy_hash: str = Field(min_length=1)
    source_references: tuple[str, ...] = Field(min_length=1)
    orders: tuple[Order, ...]
    # An attested feasible set frozen at T, not the ex-post winning ticker.
    eligible_tickers: tuple[str, ...] = Field(min_length=1)

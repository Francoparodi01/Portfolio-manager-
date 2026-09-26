from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class EvidenceMode(str, Enum):
    OBSERVATION = "OBSERVATION"
    PRODUCTION = "PRODUCTION"
    SHADOW = "SHADOW"
    RESEARCH = "RESEARCH"


class EvidenceQuality(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    UNKNOWN = "UNKNOWN"


class TaskSpec(StrictModel):
    intent: str = "general"
    entities: list[str] = Field(default_factory=list)
    objective: str = "answer_user"
    required_evidence: list[str] = Field(default_factory=list)
    inherited_subject: str | None = None
    ambiguity: list[str] = Field(default_factory=list)
    raw_message: str
    # Structured conversational semantics. These fields are produced by the
    # semantic router (or conservative fallback parser) and consumed by policy;
    # they never directly select arbitrary tools or authorize actions.
    lookback_days: int | None = Field(default=None, ge=1, le=730)
    horizon_days: int | None = Field(default=None, ge=1, le=365)
    aggregation: Literal["plan_level", "normalized"] | None = None
    reference: Literal["current_turn", "previous_turn"] = "current_turn"
    routing_source: Literal["semantic", "fallback", "precomputed"] = "fallback"
    routing_confidence: float | None = Field(default=None, ge=0.0, le=1.0)

    @field_validator("entities")
    @classmethod
    def normalize_entities(cls, value: list[str]) -> list[str]:
        return list(dict.fromkeys(str(item).upper().strip() for item in value if str(item).strip()))


class ContextPlan(StrictModel):
    allowed_tools: list[str] = Field(default_factory=list)
    required_tools: list[str] = Field(default_factory=list)
    parallel_groups: list[list[str]] = Field(default_factory=list)
    max_steps: int = Field(default=6, ge=1, le=20)
    max_tool_calls: int = Field(default=8, ge=1, le=30)
    max_retries: int = Field(default=1, ge=0, le=3)
    max_seconds: int = Field(default=240, ge=1, le=1800)
    max_context_chars: int = Field(default=16000, ge=1000, le=100000)
    max_observation_chars: int = Field(default=6000, ge=500, le=100000)


class ConversationState(StrictModel):
    conversation_id: str = Field(default_factory=lambda: str(uuid4()))
    owner_chat_id: int
    active_symbols: list[str] = Field(default_factory=list)
    last_intent: str | None = None
    last_decision_id: str | None = None
    conversation_subject: str | None = None
    evidence_refs: list[str] = Field(default_factory=list)
    recent_user_messages: list[str] = Field(default_factory=list)
    # Compact validated semantics of the previous user turn. Follow-ups inherit
    # from this structure rather than reparsing arbitrary prior prose.
    last_task: dict[str, Any] | None = None
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class EvidenceObject(StrictModel):
    evidence_id: str = Field(default_factory=lambda: str(uuid4()))
    source: str
    tool_name: str
    symbol: str | None = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    payload: dict[str, Any] | str
    quality: EvidenceQuality = EvidenceQuality.UNKNOWN
    mode: EvidenceMode = EvidenceMode.OBSERVATION
    warnings: list[str] = Field(default_factory=list)
    content_sha256: str | None = None
    ok: bool = True
    elapsed_ms: int = 0


class HarnessState(StrictModel):
    run_id: str = Field(default_factory=lambda: str(uuid4()))
    task: str
    status: Literal["PLANNING", "RUNNING", "VERIFYING", "COMPLETE", "PARTIAL", "FAILED"] = "PLANNING"
    completed_steps: list[str] = Field(default_factory=list)
    pending_steps: list[str] = Field(default_factory=list)
    failed_steps: list[str] = Field(default_factory=list)
    tool_calls: int = 0
    retries: int = 0
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    errors: list[str] = Field(default_factory=list)
    evidence_refs: list[str] = Field(default_factory=list)


class VerificationReport(StrictModel):
    passed: bool
    grounded: bool
    numeric_consistency: bool = True
    stale_or_missing_sources: list[str] = Field(default_factory=list)
    failures: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class HarnessResponse(StrictModel):
    run_id: str
    conversation_id: str
    task: TaskSpec
    answer: str
    status: str
    stop_reason: str
    verification: VerificationReport
    evidence: list[EvidenceObject] = Field(default_factory=list)
    tool_calls: int = 0
    model: str
    latency_ms: int = 0
    metadata: dict[str, Any] = Field(default_factory=dict)

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


class EvidenceMode(str, Enum):
    PRODUCTION = "PRODUCTION"
    OBSERVATION = "OBSERVATION"
    SHADOW = "SHADOW"
    RESEARCH = "RESEARCH"


class EvidenceQuality(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    UNKNOWN = "UNKNOWN"
    INSUFFICIENT = "INSUFFICIENT"


class Capability(str, Enum):
    READ = "READ"
    COMPUTE = "COMPUTE"
    WRITE_INTERNAL = "WRITE_INTERNAL"
    FORBIDDEN = "FORBIDDEN"


class TaskSpec(BaseModel):
    """Normalized user goal. It is a planning hint, never economic evidence."""

    raw_message: str = Field(min_length=1, max_length=4000)
    intent: str = "general"
    objective: str = "answer_user"
    entities: list[str] = Field(default_factory=list)
    horizons: list[int] = Field(default_factory=list)
    required_tools: list[str] = Field(default_factory=list)
    optional_tools: list[str] = Field(default_factory=list)
    inherited_subjects: list[str] = Field(default_factory=list)
    inherited_from_run_id: str | None = None
    compound: bool = False
    ambiguous: bool = False
    verification_required: bool = True

    @field_validator("entities")
    @classmethod
    def normalize_entities(cls, values: list[str]) -> list[str]:
        return list(dict.fromkeys(str(value).upper().strip() for value in values if str(value).strip()))

    @field_validator("horizons")
    @classmethod
    def normalize_horizons(cls, values: list[int]) -> list[int]:
        allowed = {5, 10, 20, 40}
        return sorted({int(value) for value in values if int(value) in allowed})


class ConversationState(BaseModel):
    conversation_id: str
    owner_chat_id: int
    active_symbols: list[str] = Field(default_factory=list)
    last_intent: str | None = None
    last_run_id: str | None = None
    last_decision_id: str | None = None
    conversation_subject: str | None = None
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("active_symbols")
    @classmethod
    def normalize_symbols(cls, values: list[str]) -> list[str]:
        return list(dict.fromkeys(str(value).upper().strip() for value in values if str(value).strip()))[:8]


class ExecutionBudget(BaseModel):
    max_steps: int = Field(default=8, ge=1, le=20)
    max_tool_calls: int = Field(default=10, ge=1, le=30)
    max_identical_calls: int = Field(default=1, ge=1, le=3)
    max_retries: int = Field(default=1, ge=0, le=3)
    max_elapsed_seconds: int = Field(default=300, ge=5, le=1800)
    max_context_chars: int = Field(default=18000, ge=1000, le=100000)
    max_evidence_chars: int = Field(default=12000, ge=500, le=100000)


class Evidence(BaseModel):
    source: str
    tool: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    mode: EvidenceMode = EvidenceMode.OBSERVATION
    quality: EvidenceQuality = EvidenceQuality.UNKNOWN
    data: Any = None
    excerpt: str = ""
    warnings: list[str] = Field(default_factory=list)
    freshness_seconds: float | None = Field(default=None, ge=0)
    sha256: str | None = None
    ok: bool = True
    error: str | None = None
    elapsed_ms: int = Field(default=0, ge=0)
    cached: bool = False


class ContextPack(BaseModel):
    conversation: ConversationState
    recent_user_goals: list[str] = Field(default_factory=list)
    selected_evidence: list[Evidence] = Field(default_factory=list)
    selected_chars: int = 0
    pruned_items: int = 0
    budget_chars: int = 18000


class RunPhase(str, Enum):
    CREATED = "CREATED"
    PARSED = "PARSED"
    GATHERING = "GATHERING"
    VERIFYING = "VERIFYING"
    SYNTHESIZING = "SYNTHESIZING"
    COMPLETE = "COMPLETE"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"


class RunState(BaseModel):
    run_id: str
    conversation_id: str
    task: TaskSpec
    phase: RunPhase = RunPhase.CREATED
    completed_steps: list[str] = Field(default_factory=list)
    pending_tools: list[str] = Field(default_factory=list)
    evidence: list[Evidence] = Field(default_factory=list)
    failed_tools: list[str] = Field(default_factory=list)
    tool_calls: int = 0
    retries: int = 0
    transitions: list[str] = Field(default_factory=list)
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def transition(self, phase: RunPhase, note: str | None = None) -> None:
        self.phase = phase
        self.transitions.append(f"{datetime.now(timezone.utc).isoformat()} {phase.value}" + (f" {note}" if note else ""))


class VerificationResult(BaseModel):
    passed: bool
    status: Literal["PASS", "DEGRADED", "FAIL"]
    checks: dict[str, bool] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)
    blocking_errors: list[str] = Field(default_factory=list)


class HarnessResponse(BaseModel):
    run_id: str
    conversation_id: str
    answer: str
    status: str
    task: TaskSpec
    verification: VerificationResult
    evidence: list[Evidence] = Field(default_factory=list)
    state_transitions: list[str] = Field(default_factory=list)
    telemetry: dict[str, Any] = Field(default_factory=dict)

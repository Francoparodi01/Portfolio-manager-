from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


SCHEMA_VERSION = 1


@dataclass(frozen=True)
class TrainingExample:
    """Leakage-safe row used by the post-agentic training harness.

    `features` may only contain information available at decision time.
    Realized outcomes are stored separately as labels/diagnostics.
    """

    run_id: str
    decided_at: datetime
    model: str
    status: str
    stop_reason: str | None
    goal_sha256: str
    tool_sequence: tuple[str, ...]
    tool_count: int
    unique_tool_count: int
    failed_tool_calls: int
    mean_controller_confidence: float | None
    jev_label: str
    jev_score: float | None
    jev_version: str
    decision_log_id: int | None
    action: str | None
    features: dict[str, Any]
    horizon_days: int
    directional_outcome: float
    alpha_vs_benchmark: float | None
    max_adverse_excursion: float | None
    risk_guard_violations: int
    data_quality_ok: bool
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["decided_at"] = self.decided_at.astimezone(timezone.utc).isoformat()
        value["tool_sequence"] = list(self.tool_sequence)
        value["schema_version"] = SCHEMA_VERSION
        return value


@dataclass(frozen=True)
class TrainingManifest:
    created_at: datetime
    schema_version: int
    total_runs: int
    eligible_examples: int
    excluded_examples: int
    exclusion_reasons: dict[str, int]
    train_examples: int
    validation_examples: int
    earliest_decision_at: str | None
    latest_decision_at: str | None
    jev_version_counts: dict[str, int]
    notes: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["created_at"] = self.created_at.astimezone(timezone.utc).isoformat()
        value["notes"] = list(self.notes)
        return value


@dataclass(frozen=True)
class PromotionGateResult:
    eligible_for_review: bool
    checks: dict[str, bool]
    values: dict[str, Any]
    reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "eligible_for_review": self.eligible_for_review,
            "checks": dict(self.checks),
            "values": dict(self.values),
            "reasons": list(self.reasons),
            "auto_promoted": False,
        }

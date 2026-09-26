from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Literal


class AgentError(RuntimeError):
    """Base exception for the Quantia agentic layer."""


class AgentModelError(AgentError):
    """Raised when the model cannot produce a valid control decision."""


class ToolValidationError(AgentError):
    """Raised when a requested tool call does not satisfy its contract."""


def validate_answer(answer: Any) -> str:
    """A scalar/JSON value is never a user-facing analytical explanation."""
    if not isinstance(answer, str) or not answer.strip():
        raise AgentModelError("final answer must be explanatory text")
    answer = answer.strip()
    try:
        json.loads(answer)
    except (ValueError, TypeError):
        pass
    else:
        raise AgentModelError("final answer must explain the goal, not return a scalar or JSON value")
    if not any(char.isalpha() for char in answer):
        raise AgentModelError("final answer cannot be only a score or number")
    return answer[:12000]


@dataclass(frozen=True)
class ToolSpec:
    name: str
    description: str
    input_schema: dict[str, Any]
    read_only: bool = True
    timeout_seconds: float = 600.0

    def prompt_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "input_schema": self.input_schema,
            "read_only": self.read_only,
        }


@dataclass
class ToolObservation:
    tool_name: str
    arguments: dict[str, Any]
    ok: bool
    content: str
    elapsed_ms: int = 0
    cached: bool = False
    error: str | None = None
    content_sha256: str | None = None


ToolHandler = Callable[[dict[str, Any]], Awaitable[ToolObservation]]


@dataclass
class AgentDecision:
    kind: Literal["tool", "final"]
    tool_name: str | None = None
    arguments: dict[str, Any] = field(default_factory=dict)
    answer: str | None = None
    rationale: str = ""
    confidence: float | None = None
    answer_origin: str = "model"
    objective_status: str = "NOT_ASSESSED"

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "AgentDecision":
        kind = str(value.get("kind") or value.get("type") or "").strip().lower()
        if kind not in {"tool", "final"}:
            raise AgentModelError("model output must set kind='tool' or kind='final'")

        rationale = " ".join(str(value.get("rationale") or "").split())[:500]
        confidence = value.get("confidence")
        if confidence is not None:
            try:
                confidence = float(confidence)
                confidence = max(0.0, min(1.0, confidence)) if math.isfinite(confidence) else None
            except (TypeError, ValueError):
                confidence = None

        if kind == "tool":
            tool_name = str(
                value.get("tool") or value.get("tool_name") or ""
            ).strip()
            arguments = value.get("arguments", {})
            if not tool_name:
                raise AgentModelError("tool decision missing tool name")
            if not isinstance(arguments, dict):
                raise AgentModelError("tool arguments must be an object")
            return cls(
                kind="tool",
                tool_name=tool_name,
                arguments=arguments,
                rationale=rationale,
                confidence=confidence,
            )

        answer = validate_answer(value.get("answer"))
        return cls(
            kind="final",
            answer=answer,
            rationale=rationale,
            confidence=confidence,
        )


@dataclass
class AgentTraceStep:
    step_no: int
    decision: AgentDecision
    observation: ToolObservation | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class AgentResult:
    run_id: str
    goal: str
    answer: str
    status: str
    stop_reason: str
    steps: list[AgentTraceStep]
    model: str
    started_at: datetime
    finished_at: datetime
    audit_persisted: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "goal": self.goal,
            "answer": self.answer,
            "status": self.status,
            "stop_reason": self.stop_reason,
            "model": self.model,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "audit_persisted": self.audit_persisted,
            "metadata": self.metadata,
            "objective_status": (self.steps[-1].decision.objective_status
                                 if self.steps and self.steps[-1].decision.kind == "final" else "INSUFFICIENT"),
            "answer_origin": (self.steps[-1].decision.answer_origin
                              if self.steps and self.steps[-1].decision.kind == "final" else None),
            "steps": [
                {
                    "step_no": step.step_no,
                    "decision": {
                        "kind": step.decision.kind,
                        "tool_name": step.decision.tool_name,
                        "arguments": step.decision.arguments,
                        "answer": step.decision.answer,
                        "rationale": step.decision.rationale,
                        "confidence": step.decision.confidence,
                        "answer_origin": step.decision.answer_origin,
                        "objective_status": step.decision.objective_status,
                    },
                    "observation": (
                        {
                            "tool_name": step.observation.tool_name,
                            "arguments": step.observation.arguments,
                            "ok": step.observation.ok,
                            "content": step.observation.content,
                            "elapsed_ms": step.observation.elapsed_ms,
                            "cached": step.observation.cached,
                            "error": step.observation.error,
                            "content_sha256": step.observation.content_sha256,
                        }
                        if step.observation
                        else None
                    ),
                }
                for step in self.steps
            ],
        }


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)

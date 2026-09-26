from __future__ import annotations

import os
import asyncio
import hashlib
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from .contracts import (
    AgentModelError,
    AgentResult,
    AgentTraceStep,
    ToolObservation,
    ToolValidationError,
    validate_answer,
)
from .model import AgentModel
from .persistence import AgentRunStore
from .tools import ToolRegistry, execute_tool, tool_call_key


class AgentOrchestrator:
    """Bounded perception -> model decision -> tool -> observation loop."""

    def __init__(
        self,
        *,
        model: AgentModel,
        registry: ToolRegistry,
        store: AgentRunStore | None = None,
        max_steps: int = 8,
        max_identical_calls: int = 1,
        require_audit: bool = True,
    ) -> None:
        if max_steps < 1 or max_steps > 20:
            raise ValueError("max_steps must be between 1 and 20")
        self.model = model
        self.registry = registry
        self.store = store
        self.max_steps = int(max_steps)
        self.max_identical_calls = max(1, int(max_identical_calls))
        self.require_audit = bool(require_audit)

    @staticmethod
    def _history_payload(steps: list[AgentTraceStep]) -> list[dict[str, Any]]:
        payload: list[dict[str, Any]] = []
        for step in steps:
            decision = {
                "kind": step.decision.kind,
                "tool": step.decision.tool_name,
                "arguments": step.decision.arguments,
                "answer": step.decision.answer,
                "rationale": step.decision.rationale,
                "confidence": step.decision.confidence,
            }
            observation = None
            if step.observation:
                observation = {
                    "tool": step.observation.tool_name,
                    "ok": step.observation.ok,
                    "content": step.observation.content,
                    "error": step.observation.error,
                    "cached": step.observation.cached,
                }
            payload.append({"decision": decision, "observation": observation})
        return payload

    async def _persist_step(
        self,
        run_id: str,
        step: AgentTraceStep,
    ) -> bool:
        if not self.store:
            return False
        observation = step.observation
        await self.store.record_step(
            run_id=run_id,
            step_no=step.step_no,
            decision_kind=step.decision.kind,
            tool_name=step.decision.tool_name,
            tool_arguments=step.decision.arguments,
            rationale=step.decision.rationale,
            confidence=step.decision.confidence,
            observation_ok=observation.ok if observation else None,
            observation=observation.content if observation else None,
            observation_sha256=observation.content_sha256 if observation else None,
            observation_cached=observation.cached if observation else False,
            elapsed_ms=observation.elapsed_ms if observation else 0,
            error=observation.error if observation else None,
        )
        return True

    async def run(
        self,
        *,
        goal: str,
        owner_chat_id: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> AgentResult:
        goal = " ".join(str(goal or "").split())
        if not goal:
            raise ValueError("goal cannot be empty")
        if len(goal) > 4000:
            raise ValueError("goal exceeds 4000 characters")

        run_id = str(uuid4())
        started_at = datetime.now(timezone.utc)
        steps: list[AgentTraceStep] = []
        cache: dict[str, ToolObservation] = {}
        call_counts: dict[str, int] = {}
        audit_persisted = False
        audit_complete = True

        if self.require_audit and not self.store:
            raise RuntimeError("agent audit store is required but not configured")

        if self.store:
            try:
                await self.store.ensure_schema()
                await self.store.start_run(
                    run_id=run_id,
                    owner_chat_id=owner_chat_id,
                    goal=goal,
                    model=self.model.name,
                    max_steps=self.max_steps,
                    started_at=started_at,
                    metadata=metadata or {},
                )
                audit_persisted = True
            except Exception:
                audit_complete = False
                if self.require_audit:
                    raise

        answer = ""
        status = "FAILED"
        stop_reason = "unhandled"

        try:
            for step_no in range(1, self.max_steps + 1):
                decision = await self.model.decide(
                    goal=goal,
                    tools=self.registry.specs(),
                    history=self._history_payload(steps),
                    step_no=step_no,
                    max_steps=self.max_steps,
                    force_final=False,
                )

                if decision.kind == "final":
                    if not any(s.observation and s.observation.ok for s in steps):
                        raise AgentModelError("no successful tool evidence; cannot substantiate a final answer")
                    decision.answer = validate_answer(decision.answer)
                    step = AgentTraceStep(step_no=step_no, decision=decision)
                    steps.append(step)
                    try:
                        audit_persisted = (await self._persist_step(run_id, step)) or audit_persisted
                    except Exception:
                        audit_complete = False
                        if self.require_audit:
                            raise
                    answer = decision.answer or ""
                    status = "COMPLETE"
                    stop_reason = "model_final"
                    break

                try:
                    decision.arguments = self.registry.validate(decision.tool_name or "", decision.arguments)
                except ToolValidationError:
                    pass  # execute_tool emits a persisted validation observation.
                key = tool_call_key(decision.tool_name or "", decision.arguments)
                call_counts[key] = call_counts.get(key, 0) + 1

                if call_counts[key] > self.max_identical_calls:
                    observation = ToolObservation(
                        tool_name=decision.tool_name or "",
                        arguments=decision.arguments,
                        ok=False,
                        content="",
                        cached=False,
                        error="repeated identical tool call blocked by loop guard",
                    )
                elif key in cache:
                    original = cache[key]
                    observation = ToolObservation(
                        tool_name=original.tool_name,
                        arguments=original.arguments,
                        ok=original.ok,
                        content=original.content,
                        elapsed_ms=0,
                        cached=True,
                        error=original.error,
                        content_sha256=original.content_sha256,
                    )
                else:
                    try:
                        observation = await execute_tool(
                            self.registry,
                            name=decision.tool_name or "",
                            arguments=decision.arguments,
                        )
                    except ToolValidationError as exc:
                        observation = ToolObservation(
                            tool_name=decision.tool_name or "",
                            arguments=decision.arguments,
                            ok=False,
                            content="",
                            error=f"validation: {exc}",
                        )
                    cache[key] = observation

                observation.content_sha256 = hashlib.sha256(observation.content.encode("utf-8")).hexdigest()
                step = AgentTraceStep(
                    step_no=step_no,
                    decision=decision,
                    observation=observation,
                )
                steps.append(step)
                try:
                    audit_persisted = (await self._persist_step(run_id, step)) or audit_persisted
                except Exception:
                    audit_complete = False
                    if self.require_audit:
                        raise

            else:
                final_decision = await self.model.decide(
                    goal=goal,
                    tools=self.registry.specs(),
                    history=self._history_payload(steps),
                    step_no=self.max_steps + 1,
                    max_steps=self.max_steps,
                    force_final=True,
                )
                if final_decision.kind != "final":
                    raise AgentModelError("model refused forced finalization")
                if not any(s.observation and s.observation.ok for s in steps):
                    raise AgentModelError("no successful tool evidence at budget exhaustion")
                final_decision.answer = validate_answer(final_decision.answer)
                answer = final_decision.answer or ""
                status = "LIMIT_REACHED"
                stop_reason = "max_steps"
                final_step = AgentTraceStep(
                    step_no=self.max_steps + 1,
                    decision=final_decision,
                )
                steps.append(final_step)
                try:
                    audit_persisted = (await self._persist_step(run_id, final_step)) or audit_persisted
                except Exception:
                    audit_complete = False
                    if self.require_audit:
                        raise

        except asyncio.CancelledError:
            status, stop_reason, answer = "CANCELLED", "cancelled", "Run cancelado."
            raise
        except Exception as exc:
            status = "FAILED"
            stop_reason = f"{type(exc).__name__}"
            answer = (
                "El loop agéntico se detuvo de forma segura antes de completar el objetivo. "
                f"Motivo técnico: {type(exc).__name__}. Consultá la traza de auditoría."
            )
            if isinstance(exc, AgentModelError):
                stop_reason = "model_error"
        finally:
            finished_at = datetime.now(timezone.utc)
            if self.store and audit_persisted:
                try:
                    await self.store.finish_run(
                        run_id=run_id,
                        status=status,
                        stop_reason=stop_reason,
                        final_answer=answer,
                        finished_at=finished_at,
                        metadata_patch={"steps_used": len(steps),
                                        "answer_origin": (steps[-1].decision.answer_origin
                                                          if steps and steps[-1].decision.kind == "final" else None)},
                    )
                except Exception:
                    audit_complete = False
                    if self.require_audit:
                        raise

        return AgentResult(
            run_id=run_id,
            goal=goal,
            answer=answer,
            status=status,
            stop_reason=stop_reason,
            steps=steps,
            model=self.model.name,
            started_at=started_at,
            finished_at=finished_at,
            audit_persisted=audit_persisted and audit_complete,
        )


def default_max_steps() -> int:
    return max(1, min(20, int(os.getenv("QUANTIA_AGENT_MAX_STEPS", "8"))))

from __future__ import annotations

import asyncio
import hashlib
import os
from datetime import datetime, timezone
from typing import Any, TYPE_CHECKING
from uuid import uuid4

from .answer import evidence_decision
from .contracts import (
    AgentDecision,
    AgentModelError,
    AgentResult,
    AgentTraceStep,
    ToolObservation,
    ToolValidationError,
    validate_answer,
)
from .diagnostics import diagnostic_decision, question_plan
from .evidence_gate import (
    INTENT_REQUIREMENTS,
    canonical_sql_is_redundant,
    evaluate_evidence,
    render_portfolio_review,
    successful_payloads,
)
from .model import AgentModel
from .persistence import AgentRunStore
from .tools import ToolRegistry, execute_tool, tool_call_key

if TYPE_CHECKING:
    from .grounded_composer import GroundedAgentComposer, VerifierVerdict


class AgentOrchestrator:
    """Bounded evidence orchestration with deterministic completion contracts.

    Known intents are driven by Python until their canonical evidence contract is
    either satisfied or proven incomplete. Open-ended requests retain the bounded
    LLM controller. A synthesis model never receives authority to request tools.
    """

    def __init__(
        self,
        *,
        model: AgentModel,
        registry: ToolRegistry,
        store: AgentRunStore | None = None,
        composer: GroundedAgentComposer | None = None,
        max_steps: int = 8,
        max_identical_calls: int = 1,
        require_audit: bool = True,
    ) -> None:
        if max_steps < 1 or max_steps > 20:
            raise ValueError("max_steps must be between 1 and 20")
        self.model = model
        self.registry = registry
        self.store = store
        self.composer = composer
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

    @staticmethod
    def _progress(events: list[str], label: str) -> None:
        if label not in events:
            events.append(label)

    def _deterministic_plan(self, goal: str):
        context = getattr(self.model, "conversation_context", None)
        plan = question_plan(goal, context if isinstance(context, list) else None)
        requirement = INTENT_REQUIREMENTS.get(plan.intent)
        available = {spec.name for spec in self.registry.specs()}
        active = bool(requirement and all(name in available for name in requirement.required_tools))
        return plan, requirement, active

    @staticmethod
    def _required_next(requirement, history: list[dict[str, Any]]) -> str | None:
        if requirement is None:
            return None
        attempted = {
            str((item.get("decision") or {}).get("tool") or "")
            for item in history
            if item.get("observation") is not None
        }
        for name in requirement.required_tools:
            if name not in attempted:
                return name
        return None

    def _safe_renderer(
        self,
        *,
        goal: str,
        history: list[dict[str, Any]],
        plan,
        gate,
        composer_issue: str | None = None,
    ) -> AgentDecision:
        if plan.intent == "portfolio_review" and gate.normalized:
            decision = AgentDecision(
                kind="final",
                answer=render_portfolio_review(gate),
                rationale=gate.reason,
                answer_origin="portfolio_renderer_v6_current_run",
                objective_status="EXPLAINED" if gate.complete else "PARTIAL",
            )
        else:
            context = getattr(self.model, "conversation_context", None)
            try:
                decision = diagnostic_decision(
                    goal,
                    history,
                    plan,
                    context if isinstance(context, list) else None,
                )
            except Exception:
                decision = evidence_decision(goal, history)
            decision.answer_origin = "evidence_renderer_v2"
            if gate.complete:
                decision.objective_status = "EXPLAINED"
            elif decision.objective_status == "NOT_ASSESSED":
                decision.objective_status = "PARTIAL" if successful_payloads(history) else "INSUFFICIENT"
        decision.answer = validate_answer(decision.answer)
        reason = gate.reason
        if composer_issue:
            reason += f" Safe renderer used: {composer_issue}."
        decision.rationale = reason[:500]
        return decision

    async def _compose_from_gate(
        self,
        *,
        goal: str,
        history: list[dict[str, Any]],
        plan,
        gate,
        progress_events: list[str],
    ) -> tuple[AgentDecision, bool, VerifierVerdict | None]:
        self._progress(progress_events, "⏳ Elaborando respuesta")
        if self.composer is not None:
            decision, verdict = await self.composer.compose(
                goal=goal,
                intent=plan.intent,
                history=history,
                gate=gate,
            )
            if decision is not None:
                self._progress(progress_events, "✓ Respuesta verificada")
                return decision, False, verdict
            rendered = self._safe_renderer(
                goal=goal,
                history=history,
                plan=plan,
                gate=gate,
                composer_issue="; ".join(verdict.issues) if verdict else "verifier unavailable",
            )
            self._progress(progress_events, "✓ Respuesta verificada")
            return rendered, True, verdict

        rendered = self._safe_renderer(
            goal=goal,
            history=history,
            plan=plan,
            gate=gate,
            composer_issue="composer not configured",
        )
        self._progress(progress_events, "✓ Respuesta verificada")
        return rendered, True, None

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
        progress_events: list[str] = []
        self._progress(progress_events, "✓ Consulta interpretada")

        plan, requirement, deterministic_active = self._deterministic_plan(goal)
        run_metadata = dict(metadata or {})
        run_metadata.update(
            {
                "intent": plan.intent,
                "required_tools": list(requirement.required_tools) if requirement else list(plan.required_tools),
                "deterministic_orchestration": deterministic_active,
            }
        )

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
                    metadata=run_metadata,
                )
                audit_persisted = True
            except Exception:
                audit_complete = False
                if self.require_audit:
                    raise

        answer = ""
        status = "FAILED"
        stop_reason = "unhandled"
        completion_gate = None
        fallback_used = False
        verifier_verdict = None

        try:
            for step_no in range(1, self.max_steps + 1):
                history = self._history_payload(steps)

                if deterministic_active:
                    next_required = self._required_next(requirement, history)
                    if next_required is None:
                        completion_gate = evaluate_evidence(plan.intent, goal, history)
                        if completion_gate.complete:
                            self._progress(progress_events, "✓ Evidencia suficiente")
                        final_decision, fallback_used, verifier_verdict = await self._compose_from_gate(
                            goal=goal,
                            history=history,
                            plan=plan,
                            gate=completion_gate,
                            progress_events=progress_events,
                        )
                        final_step = AgentTraceStep(step_no=step_no, decision=final_decision)
                        steps.append(final_step)
                        try:
                            audit_persisted = (await self._persist_step(run_id, final_step)) or audit_persisted
                        except Exception:
                            audit_complete = False
                            if self.require_audit:
                                raise
                        answer = final_decision.answer or ""
                        status = "COMPLETE"
                        stop_reason = "evidence_complete" if completion_gate.complete else "evidence_incomplete"
                        break
                    decision = AgentDecision(
                        kind="tool",
                        tool_name=next_required,
                        arguments={},
                        rationale=f"Fuente canónica requerida por contrato de evidencia {plan.intent}.",
                    )
                else:
                    decision = await self.model.decide(
                        goal=goal,
                        tools=self.registry.specs(),
                        history=history,
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
                    pass

                if (
                    decision.tool_name == "query_quantia_sql"
                    and canonical_sql_is_redundant(plan.intent, history)
                ):
                    observation = ToolObservation(
                        tool_name="query_quantia_sql",
                        arguments=decision.arguments,
                        ok=False,
                        content="",
                        cached=False,
                        error="redundant SQL blocked: canonical decision evidence already supplies these fields",
                    )
                else:
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

                if observation.ok and observation.tool_name == "get_portfolio_snapshot":
                    self._progress(progress_events, "✓ Cartera cargada")
                if observation.ok and observation.tool_name == "get_decision_evidence":
                    self._progress(progress_events, "✓ Evidencia de decisión obtenida")

                if deterministic_active:
                    current_history = self._history_payload(steps)
                    completion_gate = evaluate_evidence(plan.intent, goal, current_history)
                    required_attempted = self._required_next(requirement, current_history) is None
                    if required_attempted:
                        if completion_gate.complete:
                            self._progress(progress_events, "✓ Evidencia suficiente")
                        final_decision, fallback_used, verifier_verdict = await self._compose_from_gate(
                            goal=goal,
                            history=current_history,
                            plan=plan,
                            gate=completion_gate,
                            progress_events=progress_events,
                        )
                        final_step = AgentTraceStep(step_no=step_no + 1, decision=final_decision)
                        steps.append(final_step)
                        try:
                            audit_persisted = (await self._persist_step(run_id, final_step)) or audit_persisted
                        except Exception:
                            audit_complete = False
                            if self.require_audit:
                                raise
                        answer = final_decision.answer or ""
                        status = "COMPLETE"
                        stop_reason = "evidence_complete" if completion_gate.complete else "evidence_incomplete"
                        break

            else:
                history = self._history_payload(steps)
                completion_gate = evaluate_evidence(plan.intent, goal, history)
                if deterministic_active:
                    if completion_gate.complete:
                        self._progress(progress_events, "✓ Evidencia suficiente")
                    final_decision, fallback_used, verifier_verdict = await self._compose_from_gate(
                        goal=goal,
                        history=history,
                        plan=plan,
                        gate=completion_gate,
                        progress_events=progress_events,
                    )
                    answer = final_decision.answer or ""
                    status = "COMPLETE"
                    stop_reason = "evidence_complete_at_budget" if completion_gate.complete else "evidence_incomplete_at_budget"
                    final_step = AgentTraceStep(step_no=self.max_steps + 1, decision=final_decision)
                    steps.append(final_step)
                    try:
                        audit_persisted = (await self._persist_step(run_id, final_step)) or audit_persisted
                    except Exception:
                        audit_complete = False
                        if self.require_audit:
                            raise
                else:
                    final_decision = await self.model.decide(
                        goal=goal,
                        tools=self.registry.specs(),
                        history=history,
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
            history = self._history_payload(steps)
            executed_tools = [
                str((item.get("decision") or {}).get("tool") or "")
                for item in history
                if item.get("observation") is not None
            ]
            metadata_patch = {
                "steps_used": len(steps),
                "objective_status": (
                    steps[-1].decision.objective_status
                    if steps and steps[-1].decision.kind == "final"
                    else "INSUFFICIENT"
                ),
                "answer_origin": (
                    steps[-1].decision.answer_origin
                    if steps and steps[-1].decision.kind == "final"
                    else None
                ),
                "intent": plan.intent,
                "required_tools": list(requirement.required_tools) if requirement else list(plan.required_tools),
                "executed_tools": executed_tools,
                "completion_gate": (
                    "SATISFIED" if completion_gate and completion_gate.complete
                    else "UNSATISFIED" if completion_gate
                    else "NOT_APPLICABLE"
                ),
                "completion_reason": completion_gate.reason if completion_gate else None,
                "synthesis_model": getattr(self.composer, "synthesis_model", None),
                "verifier_model": getattr(self.composer, "verifier_model", None),
                "verifier_action": getattr(verifier_verdict, "action", None),
                "fallback_used": fallback_used,
                "progress_events": progress_events,
            }
            run_metadata.update(metadata_patch)
            if self.store and audit_persisted:
                try:
                    await self.store.finish_run(
                        run_id=run_id,
                        status=status,
                        stop_reason=stop_reason,
                        final_answer=answer,
                        finished_at=finished_at,
                        metadata_patch=metadata_patch,
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
            metadata=run_metadata,
        )


def default_max_steps() -> int:
    return max(1, min(20, int(os.getenv("QUANTIA_AGENT_MAX_STEPS", "8"))))

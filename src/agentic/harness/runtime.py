from __future__ import annotations

import asyncio
import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any

from src.agentic.contracts import AgentDecision, ToolObservation, ToolValidationError
from src.agentic.diagnostics import decision_lab_arguments
from src.agentic.model import OllamaAgentModel
from src.agentic.persistence import AgentRunStore
from src.agentic.tools import (
    ToolContext,
    ToolRegistry,
    build_default_registry,
    execute_tool,
    tool_call_key,
)
from src.agentic.conversation.session import ConversationSessionStore

from .context import ContextSelector
from .models import ModelRoles
from .permissions import PermissionPolicy
from .schemas import (
    ConversationState,
    EvidenceMode,
    EvidenceObject,
    EvidenceQuality,
    HarnessResponse,
    HarnessState,
    TaskSpec,
)
from .synthesis import GroundedSynthesizer
from .task import TaskParser
from .tools_ext import register_harness_tools
from .verifier import HarnessVerifier


class _RegistryView:
    def __init__(self, base: ToolRegistry, allowed: list[str]) -> None:
        self.base = base
        self.allowed = set(allowed)

    def specs(self):
        return [spec for spec in self.base.specs() if spec.name in self.allowed]

    def get(self, name: str):
        if name not in self.allowed:
            raise ToolValidationError(f"tool not permitted for this task: {name}")
        return self.base.get(name)

    def validate(self, name: str, arguments: dict[str, Any]):
        self.get(name)
        return self.base.validate(name, arguments)


class ConversationalHarness:
    """Task -> context -> bounded tools -> evidence -> verification -> answer.

    Financial computation remains in existing Quantia services/tools. This class
    owns orchestration only and has no broker/trade capability.
    """

    def __init__(
        self,
        *,
        database_url: str,
        owner_chat_id: int,
        repo_root: str,
        legacy_single_owner: bool,
        require_audit: bool | None = None,
    ) -> None:
        self.database_url = database_url
        self.owner_chat_id = int(owner_chat_id)
        self.repo_root = repo_root
        self.legacy_single_owner = bool(legacy_single_owner)
        self.require_audit = (
            os.getenv("QUANTIA_AGENT_REQUIRE_AUDIT", "true").lower() in {"1", "true", "yes", "on"}
            if require_audit is None else bool(require_audit)
        )
        self.parser = TaskParser()
        self.selector = ContextSelector()
        self.permissions = PermissionPolicy()
        self.verifier = HarnessVerifier()
        self.roles = ModelRoles.from_env()

    async def run(self, message: str) -> HarnessResponse:
        started = time.monotonic()
        sessions = ConversationSessionStore(self.owner_chat_id)
        session = await sessions.load()
        raw = " ".join(str(message or "").split())
        if raw.lower() in {"nuevo", "nueva conversación", "nueva conversacion"}:
            session = await sessions.reset()
            raw = "¿Cómo está mi cartera?"
        elif raw.lower().startswith("nuevo "):
            session = await sessions.reset()
            raw = raw[6:].strip()

        task = self.parser.parse(raw, session)
        tool_context = ToolContext(
            database_url=self.database_url,
            owner_chat_id=self.owner_chat_id,
            repo_root=self.repo_root,
            output_limit_chars=int(os.getenv("QUANTIA_AGENT_TOOL_OUTPUT_CHARS", "18000")),
            tool_timeout_seconds=float(os.getenv("QUANTIA_AGENT_TOOL_TIMEOUT_SECONDS", "600")),
            legacy_single_owner=self.legacy_single_owner,
        )
        registry = register_harness_tools(build_default_registry(tool_context), tool_context)
        safe_names = self.permissions.filter(registry, [spec.name for spec in registry.specs()])
        plan = self.selector.select(task, set(safe_names))
        plan.allowed_tools = self.permissions.filter(registry, plan.allowed_tools)
        plan.required_tools = [name for name in plan.required_tools if name in plan.allowed_tools]
        view = _RegistryView(registry, plan.allowed_tools)

        state = HarnessState(task=task.intent, status="RUNNING", pending_steps=list(plan.required_tools))
        store = AgentRunStore(self.database_url) if self.database_url else None
        context_rows = [
            {"goal": text, "run_id": "session", "conversation_id": session.conversation_id}
            for text in session.recent_user_messages[-3:]
        ]
        model = OllamaAgentModel(model=self.roles.reasoning, conversation_context=context_rows)
        synthesizer = GroundedSynthesizer(model=self.roles.synthesis)
        history: list[dict[str, Any]] = []
        evidence: list[EvidenceObject] = []
        calls_seen: set[str] = set()
        step_no = 0
        audit_started = False
        llm_calls = 0
        stop_reason = "objective_satisfied"

        metadata = {
            "trigger": "conversational_harness",
            "conversation_id": session.conversation_id,
            "context_namespace": "conversational-harness-v1",
            "task": task.model_dump(mode="json"),
            "context_plan": plan.model_dump(mode="json"),
            "read_only": True,
            "legacy_single_owner": self.legacy_single_owner,
            "llm_roles": self.roles.__dict__,
        }
        if self.require_audit and store is None:
            raise RuntimeError("harness audit store is required")
        if store:
            try:
                await store.ensure_schema()
                await store.start_run(
                    run_id=state.run_id,
                    owner_chat_id=self.owner_chat_id,
                    goal=task.raw_message,
                    model=self.roles.reasoning,
                    max_steps=plan.max_tool_calls + 2,
                    started_at=state.started_at,
                    metadata=metadata,
                )
                audit_started = True
            except Exception:
                if self.require_audit:
                    raise

        deadline = time.monotonic() + plan.max_seconds
        try:
            # Independent required sources are prefetched concurrently. Optional
            # tools remain under dynamic planner control.
            prefetched: set[str] = set()
            for group in plan.parallel_groups:
                runnable = [name for name in group if name in plan.allowed_tools and self._arguments(name, task) is not None]
                remaining = plan.max_tool_calls - state.tool_calls
                runnable = runnable[:remaining]
                if len(runnable) < 2:
                    continue
                observations = await asyncio.gather(*[
                    self._execute_with_retry(view, name, self._arguments(name, task) or {}, plan.max_retries)
                    for name in runnable
                ])
                for name, observation in zip(runnable, observations):
                    state.tool_calls += 1
                    prefetched.add(name)
                    calls_seen.add(tool_call_key(name, observation.arguments))
                    step_no += 1
                    item = self._to_evidence(observation)
                    evidence.append(item)
                    state.evidence_refs.append(item.evidence_id)
                    self._append_history(history, name, observation, "parallel context prefetch")
                    await self._record(store, state.run_id, step_no, name, observation, "parallel context prefetch")
                    self._mark_state(state, name, observation.ok)
                break

            # Required sources not covered by the parallel group execute once.
            for name in plan.required_tools:
                if name in prefetched or state.tool_calls >= plan.max_tool_calls:
                    continue
                args = self._arguments(name, task)
                if args is None:
                    continue
                observation = await self._execute_with_retry(view, name, args, plan.max_retries)
                state.tool_calls += 1
                calls_seen.add(tool_call_key(name, observation.arguments))
                step_no += 1
                item = self._to_evidence(observation)
                evidence.append(item)
                state.evidence_refs.append(item.evidence_id)
                self._append_history(history, name, observation, "required context")
                await self._record(store, state.run_id, step_no, name, observation, "required context")
                self._mark_state(state, name, observation.ok)

            final_decision: AgentDecision | None = None
            planner_steps = 0
            while state.tool_calls < plan.max_tool_calls and planner_steps < plan.max_steps:
                if time.monotonic() >= deadline:
                    stop_reason = "time_budget"
                    break
                planner_steps += 1
                llm_calls += 1
                try:
                    decision = await model.decide(
                        goal=task.raw_message,
                        tools=view.specs(),
                        history=history,
                        step_no=planner_steps,
                        max_steps=plan.max_steps,
                        force_final=False,
                    )
                except Exception as exc:
                    state.errors.append(f"planner:{type(exc).__name__}")
                    stop_reason = "planner_error"
                    break
                if decision.kind == "final":
                    final_decision = decision
                    break
                name = str(decision.tool_name or "")
                if not self.permissions.allow(registry, name) or name not in plan.allowed_tools:
                    state.errors.append(f"permission_denied:{name}")
                    continue
                try:
                    args = view.validate(name, decision.arguments)
                except Exception as exc:
                    state.errors.append(f"invalid_tool_args:{name}:{type(exc).__name__}")
                    continue
                key = tool_call_key(name, args)
                if key in calls_seen:
                    state.errors.append(f"duplicate_call_blocked:{name}")
                    continue
                calls_seen.add(key)
                observation = await self._execute_with_retry(view, name, args, plan.max_retries)
                state.tool_calls += 1
                step_no += 1
                item = self._to_evidence(observation)
                evidence.append(item)
                state.evidence_refs.append(item.evidence_id)
                self._append_history(history, name, observation, decision.rationale or "dynamic planner")
                await self._record(store, state.run_id, step_no, name, observation, decision.rationale or "dynamic planner")
                self._mark_state(state, name, observation.ok)

            # The current model's forced-final path is deterministic and is our
            # safe fallback if natural synthesis fails verification.
            llm_calls += 1
            try:
                fallback_decision = await model.decide(
                    goal=task.raw_message,
                    tools=view.specs(),
                    history=history,
                    step_no=plan.max_steps + 1,
                    max_steps=plan.max_steps,
                    force_final=True,
                )
                fallback = str(fallback_decision.answer or "").strip()
            except Exception:
                fallback = ""
            if final_decision and final_decision.answer:
                fallback = str(final_decision.answer)
            if not fallback:
                fallback = self._insufficient_answer(state)

            answer = await synthesizer.synthesize(task=task, evidence=evidence, fallback=fallback)
            verification = self.verifier.verify(
                task=task,
                answer=answer,
                evidence=evidence,
                required_tools=plan.required_tools,
            )
            high_stakes_numeric = task.intent in {
                "performance", "decision_history", "decision_lab", "position_analysis", "position_comparison"
            }
            if not verification.passed or (high_stakes_numeric and not verification.numeric_consistency):
                answer = fallback
                verification = self.verifier.verify(
                    task=task,
                    answer=answer,
                    evidence=evidence,
                    required_tools=plan.required_tools,
                )
            if not verification.passed:
                answer = self._insufficient_answer(state)
                state.status = "PARTIAL" if evidence else "FAILED"
            else:
                state.status = "COMPLETE"

            step_no += 1
            if store and audit_started:
                await store.record_step(
                    run_id=state.run_id,
                    step_no=step_no,
                    decision_kind="final",
                    tool_name=None,
                    tool_arguments={},
                    rationale="verified conversational synthesis",
                    confidence=None,
                    observation_ok=None,
                    observation=None,
                    observation_sha256=None,
                    observation_cached=False,
                    elapsed_ms=0,
                    error=None,
                )

            supported_answer_symbols = self._supported_answer_symbols(answer, evidence, task.entities)
            session.last_intent = task.intent
            if task.entities:
                session.active_symbols = task.entities
            elif supported_answer_symbols:
                session.active_symbols = supported_answer_symbols
            if session.active_symbols:
                session.conversation_subject = " vs ".join(session.active_symbols[:2])
            session.evidence_refs = state.evidence_refs[-20:]
            session.recent_user_messages.append(task.raw_message)
            await sessions.save(session)

            finished = datetime.now(timezone.utc)
            if store and audit_started:
                await store.finish_run(
                    run_id=state.run_id,
                    status=state.status,
                    stop_reason=stop_reason,
                    final_answer=answer,
                    finished_at=finished,
                    metadata_patch={
                        "tool_calls": state.tool_calls,
                        "llm_calls": llm_calls,
                        "verification": verification.model_dump(mode="json"),
                        "evidence_refs": state.evidence_refs,
                    },
                )
            return HarnessResponse(
                run_id=state.run_id,
                conversation_id=session.conversation_id,
                task=task,
                answer=answer,
                status=state.status,
                stop_reason=stop_reason,
                verification=verification,
                evidence=evidence,
                tool_calls=state.tool_calls,
                model=self.roles.reasoning,
                latency_ms=int((time.monotonic() - started) * 1000),
                metadata={"llm_calls": llm_calls, "context_plan": plan.model_dump(mode="json")},
            )
        except Exception as exc:
            state.status = "FAILED"
            state.errors.append(type(exc).__name__)
            if store and audit_started:
                try:
                    await store.finish_run(
                        run_id=state.run_id,
                        status="FAILED",
                        stop_reason=type(exc).__name__,
                        final_answer="El harness se detuvo de forma segura.",
                        finished_at=datetime.now(timezone.utc),
                        metadata_patch={"errors": state.errors, "tool_calls": state.tool_calls},
                    )
                except Exception:
                    if self.require_audit:
                        raise
            raise

    async def _execute_with_retry(self, registry: _RegistryView, name: str, arguments: dict[str, Any], max_retries: int) -> ToolObservation:
        observation = await execute_tool(registry, name=name, arguments=arguments)
        retries = 0
        while not observation.ok and retries < max_retries and "validation" not in str(observation.error or "").lower():
            retries += 1
            await asyncio.sleep(0.15 * retries)
            observation = await execute_tool(registry, name=name, arguments=arguments)
        return observation

    @staticmethod
    def _append_history(history: list[dict[str, Any]], name: str, observation: ToolObservation, rationale: str) -> None:
        history.append({
            "decision": {"kind": "tool", "tool": name, "arguments": observation.arguments, "rationale": rationale},
            "observation": {
                "tool": name,
                "ok": observation.ok,
                "content": observation.content,
                "error": observation.error,
                "cached": observation.cached,
            },
        })

    async def _record(self, store: AgentRunStore | None, run_id: str, step_no: int, name: str, observation: ToolObservation, rationale: str) -> None:
        if not store:
            return
        try:
            await store.record_step(
                run_id=run_id,
                step_no=step_no,
                decision_kind="tool",
                tool_name=name,
                tool_arguments=observation.arguments,
                rationale=rationale[:500],
                confidence=None,
                observation_ok=observation.ok,
                observation=observation.content,
                observation_sha256=observation.content_sha256,
                observation_cached=observation.cached,
                elapsed_ms=observation.elapsed_ms,
                error=observation.error,
            )
        except Exception:
            if self.require_audit:
                raise

    def _arguments(self, name: str, task: TaskSpec) -> dict[str, Any] | None:
        if name == "analyze_ticker":
            return {"ticker": task.entities[0]} if len(task.entities) == 1 else None
        if name == "get_meta_policy":
            return {"ticker": task.entities[0]} if task.entities else {}
        if name in {
            "compare_plan_vs_hold", "get_decision_value_added", "get_decision_counterfactuals",
            "get_similar_historical_episodes", "get_replay_evidence_quality", "compare_strategy_versions",
        }:
            return decision_lab_arguments(task.raw_message)
        if name == "scan_opportunities":
            return {"limit": 8}
        if name == "get_decision_ledger":
            match = re.search(r"\b(\d{1,3})\s*(?:dias|días|days)\b", task.raw_message.lower())
            return {"days": max(1, min(365, int(match.group(1))))} if match else {"days": 90}
        return {}

    @staticmethod
    def _mark_state(state: HarnessState, name: str, ok: bool) -> None:
        if name in state.pending_steps:
            state.pending_steps.remove(name)
        target = state.completed_steps if ok else state.failed_steps
        target.append(name)

    @staticmethod
    def _insufficient_answer(state: HarnessState) -> str:
        failed = ", ".join(state.failed_steps) if state.failed_steps else "evidencia requerida"
        return f"No tengo evidencia suficiente para determinarlo con seguridad. Faltó o falló: {failed}."

    def _supported_answer_symbols(
        self,
        answer: str,
        evidence: list[EvidenceObject],
        task_entities: list[str],
    ) -> list[str]:
        candidates = self.parser.extract_entities(answer)
        if not candidates:
            return []
        evidence_text = "\n".join(
            json.dumps(item.payload, ensure_ascii=False, default=str)
            if isinstance(item.payload, dict) else str(item.payload)
            for item in evidence if item.ok
        ).upper()
        task_set = {item.upper() for item in task_entities}
        supported: list[str] = []
        for symbol in candidates:
            if symbol in task_set or re.search(rf"\b{re.escape(symbol)}\b", evidence_text):
                supported.append(symbol)
        return list(dict.fromkeys(supported))[:8]

    @staticmethod
    def _to_evidence(observation: ToolObservation) -> EvidenceObject:
        payload: dict[str, Any] | str
        try:
            parsed = json.loads(observation.content)
            payload = parsed if isinstance(parsed, dict) else observation.content
        except (ValueError, TypeError):
            payload = observation.content
        name = observation.tool_name
        if name == "get_meta_policy":
            source, mode, quality = "economic_meta_policy", EvidenceMode.SHADOW, EvidenceQuality.MEDIUM
        elif name.startswith(("compare_", "get_decision_value", "get_decision_counter", "get_similar_", "get_replay_")):
            source, mode, quality = "decision_lab", EvidenceMode.RESEARCH, EvidenceQuality.MEDIUM
        elif name == "get_portfolio_snapshot":
            source, mode, quality = "portfolio", EvidenceMode.PRODUCTION, EvidenceQuality.HIGH
        elif name == "get_decision_ledger":
            source, mode, quality = "ledger", EvidenceMode.PRODUCTION, EvidenceQuality.HIGH
        elif name.startswith("get_macro"):
            source, mode, quality = "macro", EvidenceMode.OBSERVATION, EvidenceQuality.MEDIUM
        elif name in {"get_decision_evidence", "analyze_portfolio", "analyze_ticker"}:
            source, mode, quality = "quantia_analysis", EvidenceMode.OBSERVATION, EvidenceQuality.MEDIUM
        else:
            source, mode, quality = name, EvidenceMode.OBSERVATION, EvidenceQuality.MEDIUM if observation.ok else EvidenceQuality.LOW

        warnings: list[str] = []
        timestamp = None
        if isinstance(payload, dict):
            raw_quality = payload.get("quality")
            if isinstance(raw_quality, str) and raw_quality.upper() in EvidenceQuality.__members__:
                quality = EvidenceQuality[raw_quality.upper()]
            for key in ("timestamp", "as_of", "scraped_at", "evaluated_at", "fetched_at", "generated_at"):
                value = payload.get(key)
                if value:
                    try:
                        timestamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
                        if timestamp.tzinfo is None:
                            timestamp = timestamp.replace(tzinfo=timezone.utc)
                        break
                    except ValueError:
                        continue
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
            warnings.append("source_timestamp_missing")
        if not observation.ok:
            warnings.append(f"tool_error:{observation.error or 'unknown'}")
            quality = EvidenceQuality.LOW
        symbol = None
        ticker = observation.arguments.get("ticker") if isinstance(observation.arguments, dict) else None
        if ticker:
            symbol = str(ticker).upper()
        return EvidenceObject(
            source=source,
            tool_name=name,
            symbol=symbol,
            timestamp=timestamp,
            payload=payload,
            quality=quality,
            mode=mode,
            warnings=warnings,
            content_sha256=observation.content_sha256,
            ok=observation.ok,
            elapsed_ms=observation.elapsed_ms,
        )

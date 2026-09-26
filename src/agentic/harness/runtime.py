from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from src.agentic.contracts import ToolObservation, ToolValidationError
from src.agentic.model import OllamaAgentModel
from src.agentic.orchestrator import AgentOrchestrator
from src.agentic.persistence import AgentRunStore
from src.agentic.tools import ToolContext, execute_tool, tool_call_key, verify_single_owner
from src.core.config import get_config
from src.core.redis_client import client as redis_client

from .capabilities import ToolPolicy, assert_tool_allowed, build_conversational_registry
from .context import build_context_pack, observation_to_evidence
from .contracts import HarnessResponse, RunPhase, RunState, TaskSpec, VerificationResult
from .conversation import ConversationStateStore, wants_new_conversation
from .settings import HarnessSettings
from .synthesis import synthesize
from .tasking import parse_task
from .verifier import evidence_disclosure, verify_response


@dataclass(frozen=True)
class PlannedCall:
    name: str
    arguments: dict[str, Any]
    rationale: str


class ConversationalHarness:
    """Bounded, audited, read-only conversational harness around Quantia.

    Known tasks use deterministic routing. Unknown tasks may use the existing local
    LLM controller only to select read-only tools. Final prose is reconstructed from
    normalized Quantia evidence and passes deterministic verification.
    """

    def __init__(
        self,
        *,
        database_url: str,
        configured_owner_chat_id: str = "",
        multiuser_enabled: bool = False,
        settings: HarnessSettings | None = None,
        run_store: AgentRunStore | None = None,
    ) -> None:
        self.database_url = database_url
        self.configured_owner_chat_id = str(configured_owner_chat_id or "").strip()
        self.multiuser_enabled = bool(multiuser_enabled)
        self.settings = settings or HarnessSettings.from_env()
        self.run_store = run_store or AgentRunStore(database_url)
        self.conversations = ConversationStateStore(run_store=self.run_store)

    @classmethod
    def from_project(cls) -> "ConversationalHarness":
        cfg = get_config()
        return cls(
            database_url=cfg.database.url,
            configured_owner_chat_id=str(getattr(cfg.scraper, "telegram_chat_id", "") or ""),
            multiuser_enabled=bool(cfg.multiuser_enabled),
        )

    async def _tool_context(self, owner_chat_id: int) -> ToolContext:
        legacy_single_owner = False
        if not self.multiuser_enabled and self.configured_owner_chat_id == str(owner_chat_id):
            legacy_single_owner = await verify_single_owner(self.database_url, owner_chat_id)
        return ToolContext(
            database_url=self.database_url,
            owner_chat_id=owner_chat_id,
            output_limit_chars=int(os.getenv("QUANTIA_AGENT_TOOL_OUTPUT_CHARS", "18000")),
            tool_timeout_seconds=float(os.getenv("QUANTIA_AGENT_TOOL_TIMEOUT_SECONDS", "600")),
            legacy_single_owner=legacy_single_owner,
        )

    @staticmethod
    def _tool_arguments(
        name: str,
        task: TaskSpec,
        *,
        ticker: str | None = None,
    ) -> dict[str, Any]:
        symbol = ticker or (task.entities[0] if len(task.entities) == 1 else None)
        if name == "analyze_ticker":
            return {"ticker": symbol} if symbol else {}
        if name in {
            "compare_plan_vs_hold",
            "get_decision_value_added",
            "get_decision_counterfactuals",
            "get_similar_historical_episodes",
            "compare_strategy_versions",
            "get_replay_evidence_quality",
        }:
            args: dict[str, Any] = {}
            if symbol:
                args["ticker"] = symbol
            if len(task.horizons) == 1:
                args["horizon"] = task.horizons[0]
            return args
        if name == "get_ledger_outcomes":
            args = {"days": 90}
            if symbol:
                args["ticker"] = symbol
            if len(task.horizons) == 1:
                args["horizon"] = task.horizons[0]
            return args
        if name == "get_analytics_v2":
            return {"days": 180}
        if name == "scan_opportunities":
            return {"limit": 8}
        if name == "get_meta_policy_shadow":
            return {"ticker": symbol} if symbol else {}
        return {}

    @staticmethod
    def _dedupe_calls(calls: list[PlannedCall]) -> list[PlannedCall]:
        result: list[PlannedCall] = []
        seen: set[str] = set()
        for call in calls:
            key = tool_call_key(call.name, call.arguments)
            if key not in seen:
                seen.add(key)
                result.append(call)
        return result

    def _required_calls(self, task: TaskSpec) -> list[PlannedCall]:
        calls: list[PlannedCall] = []
        for name in task.required_tools:
            if name == "analyze_ticker" and task.entities:
                for ticker in task.entities[:4]:
                    calls.append(
                        PlannedCall(
                            name,
                            self._tool_arguments(name, task, ticker=ticker),
                            "required ticker evidence",
                        )
                    )
            else:
                calls.append(
                    PlannedCall(
                        name,
                        self._tool_arguments(name, task),
                        "required by normalized task",
                    )
                )
        return self._dedupe_calls(calls)

    def _optional_calls(self, task: TaskSpec) -> list[PlannedCall]:
        text = task.raw_message.lower()
        available = set(task.optional_tools)
        selected: list[PlannedCall] = []

        if task.intent in {"position_analysis", "decision_lab_mechanism"} and task.entities:
            if "analyze_ticker" in available:
                selected.append(
                    PlannedCall(
                        "analyze_ticker",
                        self._tool_arguments("analyze_ticker", task, ticker=task.entities[0]),
                        "targeted instrument evidence",
                    )
                )
            if "compare_plan_vs_hold" in available and any(
                term in text
                for term in (
                    "que hago",
                    "conviene",
                    "vender",
                    "comprar",
                    "reduc",
                    "hold",
                    "por que",
                    "por qué",
                )
            ):
                selected.append(
                    PlannedCall(
                        "compare_plan_vs_hold",
                        self._tool_arguments("compare_plan_vs_hold", task),
                        "economic comparison against HOLD",
                    )
                )
            if "get_decision_value_added" in available and any(
                term in text for term in ("dva", "valor agregado", "value added")
            ):
                selected.append(
                    PlannedCall(
                        "get_decision_value_added",
                        self._tool_arguments("get_decision_value_added", task),
                        "explicit DVA request",
                    )
                )

        if task.intent == "position_comparison" and "analyze_ticker" in available:
            for ticker in task.entities[:4]:
                selected.append(
                    PlannedCall(
                        "analyze_ticker",
                        self._tool_arguments("analyze_ticker", task, ticker=ticker),
                        "targeted comparison evidence",
                    )
                )

        if task.intent == "portfolio_review":
            if "get_macro_context" in available and any(
                term in text for term in ("riesgo", "macro", "mercado", "contexto")
            ):
                selected.append(
                    PlannedCall("get_macro_context", {}, "macro context requested")
                )
            if "get_analytics_v2" in available and any(
                term in text for term in ("performance", "rendimiento", "resultado", "gano", "ganó")
            ):
                selected.append(
                    PlannedCall(
                        "get_analytics_v2",
                        {"days": 180},
                        "economic results requested",
                    )
                )

        if task.intent == "performance" and task.horizons and "get_ledger_outcomes" in available:
            selected.append(
                PlannedCall(
                    "get_ledger_outcomes",
                    self._tool_arguments("get_ledger_outcomes", task),
                    "requested horizon detail",
                )
            )
        return self._dedupe_calls(selected)

    @staticmethod
    def _cache_key(owner_chat_id: int, call: PlannedCall) -> str:
        raw = f"{owner_chat_id}:{tool_call_key(call.name, call.arguments)}"
        return "quantia:harness:tool:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()

    async def _execute_one(
        self,
        *,
        owner_chat_id: int,
        registry,
        policies: dict[str, ToolPolicy],
        call: PlannedCall,
    ) -> tuple[ToolObservation, int]:
        try:
            policy = assert_tool_allowed(call.name, policies)
        except PermissionError as exc:
            return (
                ToolObservation(
                    tool_name=call.name,
                    arguments=call.arguments,
                    ok=False,
                    content="",
                    error=str(exc),
                ),
                1,
            )

        cache_key = self._cache_key(owner_chat_id, call)
        if policy.cache_ttl_seconds > 0:
            try:
                raw = await redis_client.get(cache_key)
                if raw:
                    payload = json.loads(raw)
                    content = str(payload.get("content") or "")
                    sha = payload.get("sha256") or hashlib.sha256(content.encode("utf-8")).hexdigest()
                    return (
                        ToolObservation(
                            tool_name=call.name,
                            arguments=call.arguments,
                            ok=True,
                            content=content,
                            elapsed_ms=0,
                            cached=True,
                            error=None,
                            content_sha256=sha,
                        ),
                        0,
                    )
            except Exception:
                pass

        attempts = 0
        observation: ToolObservation | None = None
        for attempt in range(self.settings.budget.max_retries + 1):
            attempts += 1
            try:
                observation = await execute_tool(
                    registry,
                    name=call.name,
                    arguments=call.arguments,
                )
            except ToolValidationError as exc:
                observation = ToolObservation(
                    tool_name=call.name,
                    arguments=call.arguments,
                    ok=False,
                    content="",
                    error=f"validation: {exc}",
                )
            if observation.ok:
                break
            if (observation.error or "").startswith("validation:"):
                break
            if attempt < self.settings.budget.max_retries:
                await asyncio.sleep(min(0.25 * (attempt + 1), 1.0))

        assert observation is not None
        if observation.ok and policy.cache_ttl_seconds > 0:
            try:
                await redis_client.set(
                    cache_key,
                    json.dumps(
                        {
                            "content": observation.content,
                            "sha256": observation.content_sha256,
                        },
                        ensure_ascii=False,
                    ),
                    ex=policy.cache_ttl_seconds,
                )
            except Exception:
                pass
        return observation, attempts

    async def _execute_wave(
        self,
        *,
        owner_chat_id: int,
        registry,
        policies: dict[str, ToolPolicy],
        calls: list[PlannedCall],
        remaining_calls: int,
    ) -> list[tuple[PlannedCall, ToolObservation, int]]:
        calls = calls[: max(0, remaining_calls)]
        semaphore = asyncio.Semaphore(3)

        async def run(call: PlannedCall):
            async with semaphore:
                observation, attempts = await self._execute_one(
                    owner_chat_id=owner_chat_id,
                    registry=registry,
                    policies=policies,
                    call=call,
                )
                return call, observation, attempts

        if not calls:
            return []
        return list(await asyncio.gather(*(run(call) for call in calls)))

    async def _dynamic_llm_evidence(
        self,
        *,
        task: TaskSpec,
        recent_context: list[dict],
        registry,
    ) -> tuple[list[ToolObservation], int]:
        if not self.settings.enable_llm_fallback_planner:
            return [], 0
        model = OllamaAgentModel(
            model=self.settings.llm.router,
            base_url=self.settings.llm.ollama_url,
            conversation_context=recent_context,
        )
        orchestrator = AgentOrchestrator(
            model=model,
            registry=registry,
            store=None,
            max_steps=min(4, self.settings.budget.max_steps),
            max_identical_calls=self.settings.budget.max_identical_calls,
            require_audit=False,
        )
        result = await orchestrator.run(
            goal=task.raw_message,
            owner_chat_id=None,
            metadata={"nested_planner": True},
        )
        observations = [
            step.observation for step in result.steps if step.observation is not None
        ]
        return observations, len(result.steps)

    async def _start_audit(
        self,
        run_state: RunState,
        owner_chat_id: int,
        metadata: dict[str, Any],
    ) -> bool:
        try:
            await self.run_store.ensure_schema()
            await self.run_store.start_run(
                run_id=run_state.run_id,
                owner_chat_id=owner_chat_id,
                goal=run_state.task.raw_message,
                model="conversational-harness",
                max_steps=self.settings.budget.max_steps,
                started_at=run_state.started_at,
                metadata=metadata,
            )
            return True
        except Exception:
            if self.settings.require_audit:
                raise
            return False

    async def _record_observation(
        self,
        *,
        run_state: RunState,
        step_no: int,
        call: PlannedCall,
        observation: ToolObservation,
    ) -> None:
        try:
            await self.run_store.record_step(
                run_id=run_state.run_id,
                step_no=step_no,
                decision_kind="tool",
                tool_name=call.name,
                tool_arguments=call.arguments,
                rationale=call.rationale,
                confidence=None,
                observation_ok=observation.ok,
                observation=observation.content,
                observation_sha256=observation.content_sha256,
                observation_cached=observation.cached,
                elapsed_ms=observation.elapsed_ms,
                error=observation.error,
            )
        except Exception:
            if self.settings.require_audit:
                raise

    async def _ingest_results(
        self,
        *,
        run_state: RunState,
        policies: dict[str, ToolPolicy],
        results: list[tuple[PlannedCall, ToolObservation, int]],
        step_no: int,
    ) -> int:
        for call, observation, attempts in results:
            step_no += 1
            run_state.tool_calls += 1
            run_state.retries += max(0, attempts - 1)
            evidence = observation_to_evidence(
                observation,
                max_chars=self.settings.budget.max_evidence_chars,
            )
            policy = assert_tool_allowed(call.name, policies)
            evidence.mode = policy.mode
            run_state.evidence.append(evidence)
            run_state.completed_steps.append(call.name)
            if call.name in run_state.pending_tools:
                run_state.pending_tools.remove(call.name)
            if not observation.ok:
                run_state.failed_tools.append(call.name)
            await self._record_observation(
                run_state=run_state,
                step_no=step_no,
                call=call,
                observation=observation,
            )
        return step_no

    async def _finish_audit(
        self,
        *,
        run_state: RunState,
        answer: str,
        verification: VerificationResult,
        telemetry: dict[str, Any],
        audit_started: bool,
    ) -> None:
        if not audit_started:
            return
        try:
            await self.run_store.finish_run(
                run_id=run_state.run_id,
                status=(
                    "COMPLETE"
                    if verification.status == "PASS"
                    else "PARTIAL"
                    if verification.passed
                    else "INSUFFICIENT"
                ),
                stop_reason=(
                    "verified"
                    if verification.status == "PASS"
                    else "verified_degraded"
                    if verification.passed
                    else "verification_failed"
                ),
                final_answer=answer,
                finished_at=datetime.now(timezone.utc),
                metadata_patch={
                    "normalized_task": run_state.task.model_dump(mode="json"),
                    "verification": verification.model_dump(mode="json"),
                    "state_transitions": run_state.transitions,
                    "telemetry": telemetry,
                    "evidence": [
                        {
                            "tool": item.tool,
                            "source": item.source,
                            "mode": item.mode.value,
                            "quality": item.quality.value,
                            "timestamp": item.timestamp.isoformat(),
                            "sha256": item.sha256,
                            "ok": item.ok,
                            "warnings": item.warnings,
                        }
                        for item in run_state.evidence
                    ],
                },
            )
        except Exception:
            if self.settings.require_audit:
                raise

    async def _reset_conversation(self, owner_chat_id: int, message: str) -> HarnessResponse:
        started = time.monotonic()
        conversation = await self.conversations.reset(owner_chat_id)
        task = TaskSpec(
            raw_message=message,
            intent="conversation_reset",
            objective="reset_conversation_state",
            verification_required=False,
        )
        run_state = RunState(
            run_id=str(uuid4()),
            conversation_id=conversation.conversation_id,
            task=task,
        )
        run_state.transition(RunPhase.COMPLETE, "conversation state reset")
        verification = VerificationResult(
            passed=True,
            status="PASS",
            checks={"conversation_reset": True},
        )
        audit_started = await self._start_audit(
            run_state,
            owner_chat_id,
            {
                "trigger": "telegram_conversation",
                "agent_version": "quantia-conversational-harness-v1",
                "conversation_id": conversation.conversation_id,
                "context_namespace": "conversational-harness",
                "control_event": "conversation_reset",
                "read_only": True,
            },
        )
        answer = "Conversación reiniciada. Escribí lo que quieras saber de Quantia."
        telemetry = {
            "total_latency_ms": int((time.monotonic() - started) * 1000),
            "tool_calls": 0,
            "llm_calls": 0,
            "verification_status": "PASS",
        }
        await self._finish_audit(
            run_state=run_state,
            answer=answer,
            verification=verification,
            telemetry=telemetry,
            audit_started=audit_started,
        )
        return HarnessResponse(
            run_id=run_state.run_id,
            conversation_id=conversation.conversation_id,
            answer=answer,
            status="COMPLETE",
            task=task,
            verification=verification,
            evidence=[],
            state_transitions=run_state.transitions,
            telemetry=telemetry,
        )

    def _remaining_budget(self, run_state: RunState, step_no: int) -> int:
        return max(
            0,
            min(
                self.settings.budget.max_tool_calls - run_state.tool_calls,
                self.settings.budget.max_steps - step_no,
            ),
        )

    async def handle(self, *, owner_chat_id: int, message: str) -> HarnessResponse:
        started = time.monotonic()
        if not owner_chat_id:
            raise ValueError("owner_chat_id is required")
        if wants_new_conversation(message):
            return await self._reset_conversation(owner_chat_id, message)

        conversation, recent_context = await self.conversations.load(owner_chat_id)
        task = parse_task(message, conversation, recent_context)
        run_state = RunState(
            run_id=str(uuid4()),
            conversation_id=conversation.conversation_id,
            task=task,
            pending_tools=list(task.required_tools),
        )
        run_state.transition(RunPhase.PARSED, task.intent)

        tool_context = await self._tool_context(owner_chat_id)
        registry, policies = build_conversational_registry(tool_context)
        audit_started = await self._start_audit(
            run_state,
            owner_chat_id,
            {
                "trigger": "telegram_conversation",
                "agent_version": "quantia-conversational-harness-v1",
                "conversation_id": conversation.conversation_id,
                "context_namespace": "conversational-harness",
                "context_run_ids": [str(item.get("run_id")) for item in recent_context],
                "normalized_task": task.model_dump(mode="json"),
                "llm_roles": {
                    "router": self.settings.llm.router,
                    "reasoning": self.settings.llm.reasoning,
                    "synthesis": self.settings.llm.synthesis,
                    "verifier": self.settings.llm.verifier,
                },
                "read_only": True,
                "production_trade_execution": False,
            },
        )

        step_no = 0
        llm_calls = 0
        context_pack = None
        answer = ""
        verification = VerificationResult(
            passed=False,
            status="FAIL",
            checks={},
            blocking_errors=["run did not reach verification"],
        )

        try:
            async with asyncio.timeout(self.settings.budget.max_elapsed_seconds):
                run_state.transition(RunPhase.GATHERING)
                required_calls = self._required_calls(task)
                required_results = await self._execute_wave(
                    owner_chat_id=owner_chat_id,
                    registry=registry,
                    policies=policies,
                    calls=required_calls,
                    remaining_calls=self._remaining_budget(run_state, step_no),
                )
                step_no = await self._ingest_results(
                    run_state=run_state,
                    policies=policies,
                    results=required_results,
                    step_no=step_no,
                )

                if not required_calls and self._remaining_budget(run_state, step_no) > 0:
                    dynamic, dynamic_model_calls = await self._dynamic_llm_evidence(
                        task=task,
                        recent_context=recent_context,
                        registry=registry,
                    )
                    llm_calls += dynamic_model_calls
                    remaining = self._remaining_budget(run_state, step_no)
                    dynamic_results: list[tuple[PlannedCall, ToolObservation, int]] = []
                    for observation in dynamic[:remaining]:
                        call = PlannedCall(
                            observation.tool_name,
                            observation.arguments,
                            "dynamic bounded LLM planner",
                        )
                        try:
                            assert_tool_allowed(call.name, policies)
                        except PermissionError:
                            continue
                        dynamic_results.append((call, observation, 1))
                    step_no = await self._ingest_results(
                        run_state=run_state,
                        policies=policies,
                        results=dynamic_results,
                        step_no=step_no,
                    )

                optional_calls = self._optional_calls(task)
                required_keys = {
                    tool_call_key(call.name, call.arguments) for call in required_calls
                }
                optional_calls = [
                    call
                    for call in optional_calls
                    if tool_call_key(call.name, call.arguments) not in required_keys
                ]
                optional_results = await self._execute_wave(
                    owner_chat_id=owner_chat_id,
                    registry=registry,
                    policies=policies,
                    calls=optional_calls,
                    remaining_calls=self._remaining_budget(run_state, step_no),
                )
                step_no = await self._ingest_results(
                    run_state=run_state,
                    policies=policies,
                    results=optional_results,
                    step_no=step_no,
                )

                context_pack = build_context_pack(
                    state=conversation,
                    task=task,
                    evidence=run_state.evidence,
                    recent_context=recent_context,
                    budget=self.settings.budget,
                )
                run_state.transition(RunPhase.SYNTHESIZING)
                answer = synthesize(run_state)
                disclosure = evidence_disclosure(run_state.evidence)
                if disclosure:
                    answer = answer.rstrip() + "\n\n" + disclosure

                run_state.transition(RunPhase.VERIFYING)
                verification = verify_response(answer=answer, state=run_state)
                if not verification.passed:
                    answer = (
                        "No tengo evidencia suficiente para responderlo con seguridad sin completar huecos. "
                        "La verificación final bloqueó la respuesta porque faltó respaldo verificable."
                    )
                    run_state.transition(RunPhase.DEGRADED, "verification failed closed")
                elif verification.status == "DEGRADED":
                    run_state.transition(RunPhase.DEGRADED, "material source limitation")
                else:
                    run_state.transition(RunPhase.COMPLETE)

        except TimeoutError:
            answer = (
                "No pude completar toda la evidencia dentro del límite de ejecución. "
                "No voy a completar la respuesta con supuestos."
            )
            verification = VerificationResult(
                passed=False,
                status="FAIL",
                checks={"execution_budget": False},
                warnings=["total harness time budget exhausted"],
                blocking_errors=["execution time budget exhausted"],
            )
            run_state.transition(RunPhase.DEGRADED, "time budget exhausted")
        except Exception as exc:
            answer = (
                "La consulta se detuvo de forma segura antes de completar la evidencia. "
                "No se ejecutaron operaciones ni se modificó capital."
            )
            verification = VerificationResult(
                passed=False,
                status="FAIL",
                checks={"runtime": False},
                blocking_errors=[type(exc).__name__],
            )
            run_state.transition(RunPhase.FAILED, type(exc).__name__)

        conversation = self.conversations.apply_task(
            conversation,
            task,
            run_id=run_state.run_id,
        )
        await self.conversations.save(conversation)
        telemetry = {
            "total_latency_ms": int((time.monotonic() - started) * 1000),
            "tool_calls": run_state.tool_calls,
            "failed_tool_calls": len(run_state.failed_tools),
            "retry_count": run_state.retries,
            "llm_calls": llm_calls,
            "llm_tokens_input": None,
            "llm_tokens_output": None,
            "context_chars": context_pack.selected_chars if context_pack else 0,
            "context_pruned_items": context_pack.pruned_items if context_pack else 0,
            "evidence_count": len(run_state.evidence),
            "verification_status": verification.status,
        }
        await self._finish_audit(
            run_state=run_state,
            answer=answer,
            verification=verification,
            telemetry=telemetry,
            audit_started=audit_started,
        )
        return HarnessResponse(
            run_id=run_state.run_id,
            conversation_id=conversation.conversation_id,
            answer=answer,
            status=(
                "COMPLETE"
                if verification.status == "PASS"
                else "PARTIAL"
                if verification.passed
                else "INSUFFICIENT"
            ),
            task=task,
            verification=verification,
            evidence=run_state.evidence,
            state_transitions=run_state.transitions,
            telemetry=telemetry,
        )

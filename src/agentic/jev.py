from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Literal

import requests

from src.core.logger import get_logger

from .contracts import AgentDecision, ToolSpec
from .model import AgentModel

RouterMode = Literal["off", "shadow", "active"]
log = get_logger(__name__)


class JevConfigError(ValueError):
    """Raised when Jev routing configuration is unsafe or incomplete."""


class JevProviderError(RuntimeError):
    """Transient or permanent provider failure."""


class JevResponseError(JevProviderError):
    """Provider returned a response that does not match the expected contract."""


class JevPermanentError(JevProviderError):
    """Non-retryable provider error such as invalid credentials or request schema."""


def router_mode_from_env() -> RouterMode:
    raw = os.getenv("QUANTIA_AGENT_ROUTER", "off").strip().lower()
    if raw not in {"off", "shadow", "active"}:
        raise JevConfigError("QUANTIA_AGENT_ROUTER must be one of: off, shadow, active")
    return raw  # type: ignore[return-value]


@dataclass(frozen=True)
class JevConfig:
    api_key: str
    base_url: str = "https://api.typesafe.ai"
    model: str = "jev-latest"
    timeout_seconds: float = 5.0
    confidence_threshold: float = 0.85
    max_retries: int = 1
    retry_backoff_ms: int = 250
    state_observation_chars: int = 3000

    @classmethod
    def from_env(cls, *, mode: RouterMode | None = None) -> "JevConfig":
        resolved_mode = mode or router_mode_from_env()
        api_key = os.getenv("QUANTIA_JEV_API_KEY", "").strip()
        if resolved_mode != "off" and not api_key:
            raise JevConfigError(
                "QUANTIA_JEV_API_KEY is required when QUANTIA_AGENT_ROUTER is shadow or active"
            )
        timeout_seconds = float(os.getenv("QUANTIA_JEV_TIMEOUT_SECONDS", "5"))
        threshold = float(os.getenv("QUANTIA_JEV_CONFIDENCE_THRESHOLD", "0.85"))
        max_retries = int(os.getenv("QUANTIA_JEV_MAX_RETRIES", "1"))
        backoff_ms = int(os.getenv("QUANTIA_JEV_RETRY_BACKOFF_MS", "250"))
        observation_chars = int(os.getenv("QUANTIA_JEV_STATE_OBSERVATION_CHARS", "3000"))
        if not 0.1 <= timeout_seconds <= 60:
            raise JevConfigError("QUANTIA_JEV_TIMEOUT_SECONDS must be between 0.1 and 60")
        if not 0 <= threshold <= 1:
            raise JevConfigError("QUANTIA_JEV_CONFIDENCE_THRESHOLD must be between 0 and 1")
        if not 0 <= max_retries <= 3:
            raise JevConfigError("QUANTIA_JEV_MAX_RETRIES must be between 0 and 3")
        if not 0 <= backoff_ms <= 5000:
            raise JevConfigError("QUANTIA_JEV_RETRY_BACKOFF_MS must be between 0 and 5000")
        if not 500 <= observation_chars <= 12000:
            raise JevConfigError(
                "QUANTIA_JEV_STATE_OBSERVATION_CHARS must be between 500 and 12000"
            )
        return cls(
            api_key=api_key,
            base_url=os.getenv("QUANTIA_JEV_BASE_URL", "https://api.typesafe.ai").rstrip("/"),
            model=os.getenv("QUANTIA_JEV_MODEL", "jev-latest").strip() or "jev-latest",
            timeout_seconds=timeout_seconds,
            confidence_threshold=threshold,
            max_retries=max_retries,
            retry_backoff_ms=backoff_ms,
            state_observation_chars=observation_chars,
        )


@dataclass
class JevRoutingResult:
    choice: str | None = None
    confidence: float | None = None
    probabilities: dict[str, float] = field(default_factory=dict)
    model: str | None = None
    elapsed_ms: int = 0
    usage: dict[str, int] = field(default_factory=dict)
    error: str | None = None
    attempts: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": "typesafe",
            "model": self.model,
            "choice": self.choice,
            "confidence": self.confidence,
            "probabilities": self.probabilities,
            "elapsed_ms": self.elapsed_ms,
            "usage": self.usage,
            "error": self.error,
            "attempts": self.attempts,
        }


RequestFn = Callable[..., Any]


class JevRouter:
    """Thin, validated client for TypeSafe System One / Jev choice routing."""

    def __init__(self, config: JevConfig, *, request_fn: RequestFn | None = None) -> None:
        self.config = config
        self._request_fn = request_fn or requests.post

    @staticmethod
    def _choice_key(tool_name: str) -> str:
        return f"tool__{tool_name}"

    def _criteria(self, tools: list[ToolSpec]) -> dict[str, str]:
        criteria = {
            self._choice_key(spec.name): (
                f"Use the read-only tool '{spec.name}' next. {spec.description}"
            )
            for spec in tools
        }
        criteria["final"] = (
            "The available evidence is sufficient; stop gathering evidence and synthesize the final answer."
        )
        criteria["delegate_llm"] = (
            "The next step requires nuanced reasoning, dynamic tool arguments, or ambiguity resolution by the controller LLM."
        )
        return criteria

    def _bounded_state(
        self,
        *,
        goal: str,
        tools: list[ToolSpec],
        history: list[dict[str, Any]],
        step_no: int,
        max_steps: int,
    ) -> dict[str, Any]:
        bounded_history: list[dict[str, Any]] = []
        for item in history[-8:]:
            decision = item.get("decision") or {}
            observation = item.get("observation") or None
            compact_observation = None
            if observation:
                compact_observation = {
                    "tool": observation.get("tool"),
                    "ok": observation.get("ok"),
                    "error": observation.get("error"),
                    "cached": observation.get("cached"),
                    "content": str(observation.get("content") or "")[
                        : self.config.state_observation_chars
                    ],
                }
            bounded_history.append(
                {
                    "decision": {
                        "kind": decision.get("kind"),
                        "tool": decision.get("tool"),
                        "arguments": decision.get("arguments") or {},
                        "answer": str(decision.get("answer") or "")[:1000] or None,
                    },
                    "observation": compact_observation,
                }
            )
        return {
            "system": "Quantia bounded read-only analytical agent router",
            "goal": goal,
            "step": step_no,
            "max_steps": max_steps,
            "authority": {
                "read_only": True,
                "can_trade": False,
                "can_change_risk_guards": False,
                "deterministic_financial_engine_authoritative": True,
            },
            "tools": [spec.prompt_dict() for spec in tools],
            "history": bounded_history,
        }

    def _request_sync(self, payload: dict[str, Any]) -> dict[str, Any]:
        response = self._request_fn(
            f"{self.config.base_url}/v1/systemone",
            headers={
                "Authorization": f"Bearer {self.config.api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=self.config.timeout_seconds,
        )
        status_code = int(getattr(response, "status_code", 200))
        if 400 <= status_code < 500 and status_code != 429:
            raise JevPermanentError(f"TypeSafe HTTP {status_code}")
        if status_code >= 400:
            raise JevProviderError(f"TypeSafe HTTP {status_code}")
        data = response.json()
        if not isinstance(data, dict):
            raise JevResponseError("TypeSafe response must be an object")
        return data

    def _parse(self, data: dict[str, Any], criteria: dict[str, str]) -> JevRoutingResult:
        answers = data.get("answers")
        if not isinstance(answers, dict):
            raise JevResponseError("TypeSafe response missing answers")
        answer = answers.get("next_action")
        if not isinstance(answer, dict) or answer.get("type") != "choice":
            raise JevResponseError("TypeSafe next_action must be a choice answer")
        choice = str(answer.get("choice") or "")
        if choice not in criteria:
            raise JevResponseError("TypeSafe returned an unknown routing choice")
        try:
            confidence = float(answer["confidence"])
        except (KeyError, TypeError, ValueError) as exc:
            raise JevResponseError("TypeSafe response has invalid confidence") from exc
        if not 0 <= confidence <= 1:
            raise JevResponseError("TypeSafe confidence is outside [0,1]")
        raw_probabilities = answer.get("probabilities")
        if not isinstance(raw_probabilities, dict):
            raise JevResponseError("TypeSafe response missing probabilities")
        probabilities: dict[str, float] = {}
        for key, value in raw_probabilities.items():
            if key not in criteria:
                continue
            try:
                probability = float(value)
            except (TypeError, ValueError) as exc:
                raise JevResponseError("TypeSafe response has invalid probability") from exc
            if not 0 <= probability <= 1:
                raise JevResponseError("TypeSafe probability is outside [0,1]")
            probabilities[str(key)] = probability
        if choice not in probabilities:
            raise JevResponseError("TypeSafe selected choice has no probability")
        usage = data.get("usage") or {}
        clean_usage: dict[str, int] = {}
        if isinstance(usage, dict):
            for key in ("input_tokens", "output_tokens"):
                try:
                    clean_usage[key] = int(usage.get(key, 0))
                except (TypeError, ValueError):
                    clean_usage[key] = 0
        return JevRoutingResult(
            choice=choice,
            confidence=confidence,
            probabilities=probabilities,
            model=str(data.get("model") or self.config.model),
            usage=clean_usage,
        )

    async def route(
        self,
        *,
        goal: str,
        tools: list[ToolSpec],
        history: list[dict[str, Any]],
        step_no: int,
        max_steps: int,
    ) -> JevRoutingResult:
        criteria = self._criteria(tools)
        payload = {
            "state": self._bounded_state(
                goal=goal,
                tools=tools,
                history=history,
                step_no=step_no,
                max_steps=max_steps,
            ),
            "model": self.config.model,
            "questions": {
                "next_action": {
                    "type": "choice",
                    "instructions": (
                        "Choose the next control action for this bounded read-only agent. "
                        "Prefer the minimum evidence-gathering action needed. Select final only "
                        "when the existing observations are sufficient. Select delegate_llm when "
                        "the step needs dynamic arguments or deeper synthesis."
                    ),
                    "criteria": criteria,
                }
            },
        }
        started = time.monotonic()
        last_error: Exception | None = None
        attempts = self.config.max_retries + 1
        for attempt in range(1, attempts + 1):
            try:
                data = await asyncio.to_thread(self._request_sync, payload)
                result = self._parse(data, criteria)
                result.elapsed_ms = int((time.monotonic() - started) * 1000)
                result.attempts = attempt
                return result
            except JevPermanentError as exc:
                last_error = exc
                break
            except Exception as exc:
                last_error = exc
                if attempt < attempts:
                    await asyncio.sleep((self.config.retry_backoff_ms / 1000.0) * (2 ** (attempt - 1)))
        return JevRoutingResult(
            model=self.config.model,
            elapsed_ms=int((time.monotonic() - started) * 1000),
            error=f"{type(last_error).__name__}: {str(last_error)[:300]}",
            attempts=min(attempts, attempt if 'attempt' in locals() else attempts),
        )


class RoutedAgentModel:
    """Jev System-One router in front of the existing controller LLM."""

    def __init__(
        self,
        *,
        base_model: AgentModel,
        mode: RouterMode,
        router: JevRouter | None = None,
        confidence_threshold: float = 0.85,
    ) -> None:
        if mode not in {"off", "shadow", "active"}:
            raise JevConfigError("router mode must be off, shadow, or active")
        if not 0 <= confidence_threshold <= 1:
            raise JevConfigError("confidence threshold must be between 0 and 1")
        if mode != "off" and router is None:
            raise JevConfigError("Jev router is required in shadow/active mode")
        self.base_model = base_model
        self.mode = mode
        self.router = router
        self.confidence_threshold = confidence_threshold
        self.name = base_model.name

    @staticmethod
    def _attach(
        decision: AgentDecision,
        result: JevRoutingResult | None,
        *,
        mode: RouterMode,
        applied: bool,
        action: str,
        fallback_reason: str | None = None,
    ) -> AgentDecision:
        routing = result.to_dict() if result else {"provider": "typesafe"}
        routing.update(
            {
                "mode": mode,
                "applied": applied,
                "action": action,
                "fallback_reason": fallback_reason,
            }
        )
        decision.routing = routing
        return decision

    async def _delegate(self, **kwargs: Any) -> AgentDecision:
        return await self.base_model.decide(**kwargs)

    async def decide(
        self,
        *,
        goal: str,
        tools: list[ToolSpec],
        history: list[dict[str, Any]],
        step_no: int,
        max_steps: int,
        force_final: bool = False,
    ) -> AgentDecision:
        base_kwargs = {
            "goal": goal,
            "tools": tools,
            "history": history,
            "step_no": step_no,
            "max_steps": max_steps,
            "force_final": force_final,
        }
        if force_final:
            decision = await self._delegate(**base_kwargs)
            return self._attach(
                decision,
                None,
                mode=self.mode,
                applied=False,
                action="delegate_llm",
                fallback_reason="forced_finalization",
            )
        if self.mode == "off":
            return await self._delegate(**base_kwargs)

        assert self.router is not None
        result = await self.router.route(
            goal=goal,
            tools=tools,
            history=history,
            step_no=step_no,
            max_steps=max_steps,
        )
        if result.error:
            log.warning("Jev routing failed; falling back to controller LLM: %s", result.error)
            decision = await self._delegate(**base_kwargs)
            return self._attach(
                decision,
                result,
                mode=self.mode,
                applied=False,
                action="delegate_llm",
                fallback_reason="provider_error",
            )

        if self.mode == "shadow":
            decision = await self._delegate(**base_kwargs)
            return self._attach(
                decision,
                result,
                mode=self.mode,
                applied=False,
                action="shadow_only",
                fallback_reason="shadow_mode",
            )

        confidence = result.confidence if result.confidence is not None else 0.0
        if confidence < self.confidence_threshold:
            decision = await self._delegate(**base_kwargs)
            return self._attach(
                decision,
                result,
                mode=self.mode,
                applied=False,
                action="delegate_llm",
                fallback_reason="low_confidence",
            )

        choice = result.choice or ""
        if choice == "delegate_llm":
            decision = await self._delegate(**base_kwargs)
            return self._attach(decision, result, mode=self.mode, applied=True, action="delegate_llm")
        if choice == "final":
            final_kwargs = dict(base_kwargs)
            final_kwargs["force_final"] = True
            decision = await self._delegate(**final_kwargs)
            return self._attach(decision, result, mode=self.mode, applied=True, action="force_final")
        if choice.startswith("tool__"):
            tool_name = choice[len("tool__") :]
            spec = next((item for item in tools if item.name == tool_name), None)
            if spec is None or not spec.read_only:
                decision = await self._delegate(**base_kwargs)
                return self._attach(
                    decision,
                    result,
                    mode=self.mode,
                    applied=False,
                    action="delegate_llm",
                    fallback_reason="unknown_or_unsafe_tool",
                )
            required = list(spec.input_schema.get("required") or [])
            if required:
                decision = await self._delegate(**base_kwargs)
                return self._attach(
                    decision,
                    result,
                    mode=self.mode,
                    applied=False,
                    action="delegate_llm",
                    fallback_reason="dynamic_arguments_required",
                )
            routing = result.to_dict()
            routing.update(
                {
                    "mode": self.mode,
                    "applied": True,
                    "action": "direct_read_only_tool",
                    "fallback_reason": None,
                }
            )
            log.info(
                "Jev route applied step=%s tool=%s confidence=%.3f",
                step_no,
                tool_name,
                confidence,
            )
            return AgentDecision(
                kind="tool",
                tool_name=tool_name,
                arguments={},
                rationale="Jev selected the next read-only evidence tool.",
                confidence=confidence,
                routing=routing,
            )

        decision = await self._delegate(**base_kwargs)
        return self._attach(
            decision,
            result,
            mode=self.mode,
            applied=False,
            action="delegate_llm",
            fallback_reason="invalid_choice",
        )


def build_routed_agent_model(base_model: AgentModel) -> AgentModel:
    mode = router_mode_from_env()
    if mode == "off":
        return RoutedAgentModel(base_model=base_model, mode="off")
    config = JevConfig.from_env(mode=mode)
    router = JevRouter(config)
    return RoutedAgentModel(
        base_model=base_model,
        mode=mode,
        router=router,
        confidence_threshold=config.confidence_threshold,
    )

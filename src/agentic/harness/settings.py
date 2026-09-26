from __future__ import annotations

import os
from dataclasses import dataclass

from .contracts import ExecutionBudget


@dataclass(frozen=True)
class LLMSettings:
    router: str
    reasoning: str
    synthesis: str
    verifier: str
    ollama_url: str

    @classmethod
    def from_env(cls) -> "LLMSettings":
        fallback = os.getenv("QUANTIA_AGENT_MODEL", "qwen2.5:3b")
        return cls(
            router=os.getenv("QUANTIA_LLM_ROUTER", fallback),
            reasoning=os.getenv("QUANTIA_LLM_REASONING", fallback),
            synthesis=os.getenv("QUANTIA_LLM_SYNTHESIS", fallback),
            verifier=os.getenv("QUANTIA_LLM_VERIFIER", fallback),
            ollama_url=(os.getenv("QUANTIA_AGENT_OLLAMA_URL") or os.getenv("OLLAMA_URL") or "http://host.docker.internal:11434").rstrip("/"),
        )


@dataclass(frozen=True)
class HarnessSettings:
    llm: LLMSettings
    budget: ExecutionBudget
    require_audit: bool
    enable_llm_fallback_planner: bool

    @classmethod
    def from_env(cls) -> "HarnessSettings":
        def flag(name: str, default: bool) -> bool:
            raw = os.getenv(name)
            if raw is None:
                return default
            return raw.strip().lower() in {"1", "true", "yes", "on"}

        budget = ExecutionBudget(
            max_steps=int(os.getenv("QUANTIA_HARNESS_MAX_STEPS", os.getenv("QUANTIA_AGENT_MAX_STEPS", "8"))),
            max_tool_calls=int(os.getenv("QUANTIA_HARNESS_MAX_TOOL_CALLS", "10")),
            max_identical_calls=int(os.getenv("QUANTIA_HARNESS_MAX_IDENTICAL_CALLS", "1")),
            max_retries=int(os.getenv("QUANTIA_HARNESS_MAX_RETRIES", "1")),
            max_elapsed_seconds=int(os.getenv("QUANTIA_HARNESS_MAX_SECONDS", "300")),
            max_context_chars=int(os.getenv("QUANTIA_HARNESS_CONTEXT_CHARS", "18000")),
            max_evidence_chars=int(os.getenv("QUANTIA_HARNESS_EVIDENCE_CHARS", "12000")),
        )
        return cls(
            llm=LLMSettings.from_env(),
            budget=budget,
            require_audit=flag("QUANTIA_AGENT_REQUIRE_AUDIT", True),
            enable_llm_fallback_planner=flag("QUANTIA_HARNESS_LLM_FALLBACK", True),
        )

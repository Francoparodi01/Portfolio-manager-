from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ModelRoles:
    """Single source of truth for LLM role configuration."""

    router: str
    reasoning: str
    synthesis: str
    verifier: str

    @classmethod
    def from_env(cls) -> "ModelRoles":
        default = os.getenv("QUANTIA_AGENT_MODEL", "qwen2.5:3b")
        return cls(
            router=os.getenv("QUANTIA_LLM_ROUTER", default),
            reasoning=os.getenv("QUANTIA_LLM_REASONING", default),
            synthesis=os.getenv("QUANTIA_LLM_SYNTHESIS", default),
            verifier=os.getenv("QUANTIA_LLM_VERIFIER", default),
        )

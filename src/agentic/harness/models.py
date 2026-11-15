from __future__ import annotations

import os
from dataclasses import dataclass
from ..model_transport import default_agent_model


@dataclass(frozen=True)
class ModelRoles:
    """Single source of truth for LLM role configuration."""

    router: str
    reasoning: str
    synthesis: str
    verifier: str

    @classmethod
    def from_env(cls) -> "ModelRoles":
        default = default_agent_model()
        return cls(
            router=os.getenv("QUANTIA_LLM_ROUTER", default),
            reasoning=os.getenv("QUANTIA_LLM_REASONING", default),
            synthesis=os.getenv("QUANTIA_LLM_SYNTHESIS", default),
            verifier=os.getenv("QUANTIA_LLM_VERIFIER", default),
        )

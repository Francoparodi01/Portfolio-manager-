"""Quantia conversational agent harness.

The package orchestrates existing read-only Quantia capabilities. It does not
contain or expose production trade execution.
"""

from .contracts import (
    Capability,
    ContextPack,
    ConversationState,
    Evidence,
    EvidenceMode,
    EvidenceQuality,
    ExecutionBudget,
    HarnessResponse,
    RunPhase,
    RunState,
    TaskSpec,
    VerificationResult,
)
from .runtime import ConversationalHarness
from .settings import HarnessSettings, LLMSettings

__all__ = [
    "Capability",
    "ContextPack",
    "ConversationState",
    "ConversationalHarness",
    "Evidence",
    "EvidenceMode",
    "EvidenceQuality",
    "ExecutionBudget",
    "HarnessResponse",
    "HarnessSettings",
    "LLMSettings",
    "RunPhase",
    "RunState",
    "TaskSpec",
    "VerificationResult",
]

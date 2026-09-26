"""Conversational harness for Quantia.

This layer sits above the existing read-only agentic runtime. It owns task/context/
state/memory boundaries while the Quantia backend remains the financial source of
truth.
"""

from .schemas import (
    ContextPlan,
    ConversationState,
    EvidenceMode,
    EvidenceObject,
    EvidenceQuality,
    HarnessResponse,
    HarnessState,
    TaskSpec,
    VerificationReport,
)
from .task import TaskParser
from .context import ContextSelector
from .permissions import Capability, PermissionPolicy
from .verifier import HarnessVerifier

__all__ = [
    "Capability",
    "ContextPlan",
    "ContextSelector",
    "ConversationState",
    "EvidenceMode",
    "EvidenceObject",
    "EvidenceQuality",
    "HarnessResponse",
    "HarnessState",
    "HarnessVerifier",
    "PermissionPolicy",
    "TaskParser",
    "TaskSpec",
    "VerificationReport",
]

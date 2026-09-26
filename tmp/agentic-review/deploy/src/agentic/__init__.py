"""Bounded agentic orchestration for Quantia.

The agent can choose read-only evidence and analysis tools dynamically. It cannot
execute trades, alter optimizer/planner outputs, bypass risk guards, or persist
financial decisions.
"""

from .contracts import AgentResult, ToolObservation, ToolSpec
from .model import OllamaAgentModel
from .orchestrator import AgentOrchestrator
from .persistence import AgentRunStore
from .tools import ToolContext, ToolRegistry, build_default_registry

__all__ = [
    "AgentOrchestrator",
    "AgentResult",
    "AgentRunStore",
    "OllamaAgentModel",
    "ToolContext",
    "ToolObservation",
    "ToolRegistry",
    "ToolSpec",
    "build_default_registry",
]

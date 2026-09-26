from __future__ import annotations

from enum import Enum


class Capability(str, Enum):
    READ = "READ"
    COMPUTE = "COMPUTE"
    WRITE = "WRITE"
    FORBIDDEN = "FORBIDDEN"


_FORBIDDEN_MARKERS = (
    "execute_order", "place_order", "broker_order", "trade_execution", "target_weight_override",
    "unsafe_db", "secret", "credential", "delete_", "mutate_",
)
_COMPUTE_PREFIXES = ("analyze_", "scan_")


class PermissionPolicy:
    """Capability gate independent from model prompting."""

    def capability_for(self, tool_name: str) -> Capability:
        name = str(tool_name or "").lower()
        if any(marker in name for marker in _FORBIDDEN_MARKERS):
            return Capability.FORBIDDEN
        if name.startswith(_COMPUTE_PREFIXES):
            return Capability.COMPUTE
        if name.startswith(("get_", "compare_")):
            return Capability.READ
        return Capability.FORBIDDEN

    def allow(self, tool_name: str) -> bool:
        return self.capability_for(tool_name) in {Capability.READ, Capability.COMPUTE}

    def filter(self, tool_names: list[str]) -> list[str]:
        return [name for name in tool_names if self.allow(name)]

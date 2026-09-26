from __future__ import annotations

from enum import Enum
from typing import Protocol


class Capability(str, Enum):
    READ = "READ"
    COMPUTE = "COMPUTE"
    WRITE = "WRITE"
    FORBIDDEN = "FORBIDDEN"


class _Spec(Protocol):
    name: str
    capability: str
    read_only: bool


class _Registered(Protocol):
    spec: _Spec


class RegistryLike(Protocol):
    def get(self, name: str) -> _Registered: ...


# Secondary defense only. Primary authorization comes from the registered
# capability metadata below, never from these strings.
_FORBIDDEN_MARKERS = (
    "execute_order", "place_order", "broker_order", "trade_execution", "target_weight_override",
    "unsafe_db", "secret", "credential", "delete_", "mutate_",
)


class PermissionPolicy:
    """Capability gate independent from model prompting and fail-closed.

    The Tool Registry is the source of truth. Missing/unknown capability metadata,
    non-read-only registrations and WRITE/FORBIDDEN capabilities are denied.
    Name/namespace markers remain only as defense in depth.
    """

    @staticmethod
    def _secondary_name_blocked(tool_name: str) -> bool:
        name = str(tool_name or "").lower()
        return any(marker in name for marker in _FORBIDDEN_MARKERS)

    def capability_for(self, registry: RegistryLike, tool_name: str) -> Capability:
        if self._secondary_name_blocked(tool_name):
            return Capability.FORBIDDEN
        try:
            item = registry.get(tool_name)
            spec = item.spec
        except Exception:
            return Capability.FORBIDDEN
        if not bool(getattr(spec, "read_only", False)):
            return Capability.FORBIDDEN
        raw = str(getattr(spec, "capability", "") or "").upper().strip()
        try:
            capability = Capability(raw)
        except ValueError:
            return Capability.FORBIDDEN
        if capability not in {Capability.READ, Capability.COMPUTE}:
            return Capability.FORBIDDEN
        return capability

    def allow(self, registry: RegistryLike, tool_name: str) -> bool:
        return self.capability_for(registry, tool_name) in {Capability.READ, Capability.COMPUTE}

    def filter(self, registry: RegistryLike, tool_names: list[str]) -> list[str]:
        return [name for name in tool_names if self.allow(registry, name)]

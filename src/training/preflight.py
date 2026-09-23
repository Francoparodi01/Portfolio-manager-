from __future__ import annotations

import importlib
import os
from dataclasses import dataclass
from typing import Any


DEFAULT_JEV_MODULE = "src.agentic.jev"


@dataclass(frozen=True)
class PreflightResult:
    ready: bool
    checks: dict[str, bool]
    details: dict[str, Any]
    blockers: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "ready": self.ready,
            "checks": dict(self.checks),
            "details": dict(self.details),
            "blockers": list(self.blockers),
        }


def _has_attribute(module_name: str, attribute: str) -> tuple[bool, str | None]:
    try:
        module = importlib.import_module(module_name)
    except Exception as exc:  # import failure is a fail-closed readiness signal
        return False, f"{module_name}: {exc}"
    return hasattr(module, attribute), None


def run_preflight(*, jev_module: str | None = None) -> PreflightResult:
    """Verify that both upstream implementations are present before training.

    JEV has a deliberately small integration surface:
      - JEV_CAPABILITY_VERSION: non-empty string
      - export_training_assessments(): iterable of assessment mappings

    The training harness does not depend on JEV internals beyond that adapter.
    """

    jev_module = (
        jev_module
        or os.getenv("QUANTIA_JEV_MODULE")
        or DEFAULT_JEV_MODULE
    ).strip()

    checks: dict[str, bool] = {}
    details: dict[str, Any] = {"jev_module": jev_module}
    blockers: list[str] = []

    checks["agent_orchestrator"] , err = _has_attribute(
        "src.agentic.orchestrator", "AgentOrchestrator"
    )
    if err:
        details["agent_orchestrator_error"] = err
    if not checks["agent_orchestrator"]:
        blockers.append("agentic loop missing: AgentOrchestrator not available")

    checks["agent_audit_store"], err = _has_attribute(
        "src.agentic.persistence", "AgentRunStore"
    )
    if err:
        details["agent_audit_store_error"] = err
    if not checks["agent_audit_store"]:
        blockers.append("agentic audit persistence missing: AgentRunStore not available")

    try:
        jev = importlib.import_module(jev_module)
        version = str(getattr(jev, "JEV_CAPABILITY_VERSION", "")).strip()
        exporter = getattr(jev, "export_training_assessments", None)
        checks["jev_version"] = bool(version)
        checks["jev_training_export"] = callable(exporter)
        details["jev_version"] = version or None
    except Exception as exc:
        checks["jev_version"] = False
        checks["jev_training_export"] = False
        details["jev_error"] = str(exc)

    if not checks["jev_version"]:
        blockers.append(
            "JEV missing: expose non-empty JEV_CAPABILITY_VERSION on the configured module"
        )
    if not checks["jev_training_export"]:
        blockers.append(
            "JEV training adapter missing: expose export_training_assessments()"
        )

    ready = all(checks.values())
    return PreflightResult(
        ready=ready,
        checks=checks,
        details=details,
        blockers=tuple(blockers),
    )

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any


class AgentProgressState(str, Enum):
    """User-visible lifecycle states. Never contains model reasoning."""

    RECEIVED = "RECEIVED"
    PLANNING = "PLANNING"
    USING_TOOL = "USING_TOOL"
    ANALYZING = "ANALYZING"
    COMPOSING = "COMPOSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class AgentProgressEvent:
    state: AgentProgressState
    tool_name: str | None = None
    step_no: int | None = None
    created_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "state": self.state.value,
            "tool_name": self.tool_name,
            "step_no": self.step_no,
            "created_at": self.created_at or datetime.now(timezone.utc).isoformat(),
        }

    @classmethod
    def from_mapping(cls, payload: dict[str, Any]) -> "AgentProgressEvent":
        return cls(
            state=AgentProgressState(str(payload["state"])),
            tool_name=(str(payload.get("tool_name")) if payload.get("tool_name") else None),
            step_no=(int(payload["step_no"]) if payload.get("step_no") is not None else None),
            created_at=(str(payload.get("created_at")) if payload.get("created_at") else None),
        )


class JsonlProgressWriter:
    """Append-only process boundary for sanitized progress events."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    async def __call__(self, event: AgentProgressEvent) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(event.to_dict(), ensure_ascii=False) + "\n")
            stream.flush()


def safe_tool_label(tool_name: str | None) -> str:
    """Map an internal tool name to a stable, non-sensitive user label."""

    name = str(tool_name or "").lower()
    if "portfolio" in name or "position" in name:
        return "Consultando cartera"
    if "macro" in name:
        return "Revisando contexto macro"
    if any(token in name for token in ("decision", "performance", "history", "replay", "counterfactual", "ledger")):
        return "Contrastando evidencia histórica"
    if any(token in name for token in ("ticker", "market", "quote", "radar", "signal")):
        return "Consultando mercado y señales"
    if "doc" in name:
        return "Consultando documentación"
    if "sql" in name:
        return "Consultando evidencia estructurada"
    return "Consultando evidencia"


def read_progress_events(path: str | Path) -> list[AgentProgressEvent]:
    source = Path(path)
    if not source.is_file():
        return []
    events: list[AgentProgressEvent] = []
    for raw in source.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            payload = json.loads(raw)
            if isinstance(payload, dict):
                events.append(AgentProgressEvent.from_mapping(payload))
        except (ValueError, KeyError, TypeError, json.JSONDecodeError):
            continue
    return events


def render_progress(events: list[AgentProgressEvent]) -> str:
    """Render high-level activity only; no prompts, arguments or reasoning."""

    if not events:
        events = [AgentProgressEvent(AgentProgressState.RECEIVED)]

    current = events[-1]
    lines = ["🧠 Quantia está analizando…", ""]

    if current.state == AgentProgressState.FAILED:
        return "🧠 Quantia\n\n⚠️ El análisis se detuvo de forma segura."
    if current.state == AgentProgressState.COMPLETED:
        return "🧠 Quantia\n\n✅ Respuesta lista"

    lines.append("✓ Consulta recibida")

    planned = any(event.state in {
        AgentProgressState.PLANNING,
        AgentProgressState.USING_TOOL,
        AgentProgressState.ANALYZING,
        AgentProgressState.COMPOSING,
    } for event in events)
    if planned:
        lines.append("✓ Planificando análisis")
    else:
        lines.append("⏳ Planificando análisis…")
        return "\n".join(lines)

    tool_events = [event for event in events if event.state == AgentProgressState.USING_TOOL]
    completed_tools = {
        (event.step_no, event.tool_name)
        for event in events
        if event.state == AgentProgressState.ANALYZING and event.tool_name
    }
    seen: list[tuple[int | None, str | None]] = []
    for event in tool_events:
        key = (event.step_no, event.tool_name)
        if key in seen:
            continue
        seen.append(key)
        label = safe_tool_label(event.tool_name)
        marker = "✓" if key in completed_tools or current.state == AgentProgressState.COMPOSING else "⏳"
        suffix = "" if marker == "✓" else "…"
        lines.append(f"{marker} {label}{suffix}")

    if current.state == AgentProgressState.ANALYZING:
        lines.append("⏳ Integrando evidencia…")
    elif current.state == AgentProgressState.COMPOSING:
        lines.append("✓ Evidencia integrada")
        lines.append("⏳ Preparando respuesta…")
    elif not tool_events:
        lines.append("⏳ Seleccionando evidencia…")

    return "\n".join(lines[-8:])

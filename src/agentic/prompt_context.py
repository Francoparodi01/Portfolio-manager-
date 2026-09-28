from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class AgentPromptContext:
    text: str
    source_hashes: dict[str, str]


_CONTEXT_FILES = (
    "AGENTS.md",
    "docs/agent/quantia-semantics.md",
    "docs/agent/quantia-data-model.md",
)


def load_agent_prompt_context(root: str | Path, *, max_chars: int = 24000) -> AgentPromptContext:
    base = Path(root)
    chunks: list[str] = []
    hashes: dict[str, str] = {}
    used = 0

    for relative in _CONTEXT_FILES:
        path = base / relative
        if not path.is_file():
            continue
        raw = path.read_text(encoding="utf-8").strip()
        hashes[relative] = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        header = f"\n\n--- BEGIN {relative} ---\n"
        footer = f"\n--- END {relative} ---"
        room = max(0, max_chars - used - len(header) - len(footer))
        if room <= 0:
            break
        body = raw[:room]
        chunk = header + body + footer
        chunks.append(chunk)
        used += len(chunk)

    return AgentPromptContext(text="".join(chunks).strip(), source_hashes=hashes)


__all__ = ["AgentPromptContext", "load_agent_prompt_context"]

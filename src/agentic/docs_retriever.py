from __future__ import annotations

import hashlib
import json
import re
import time
from pathlib import Path
from typing import Any

from .contracts import ToolObservation, ToolSpec
from .tools import ToolContext, ToolRegistry


_TOKEN = re.compile(r"[a-zA-Z0-9_áéíóúñÁÉÍÓÚÑ]{3,}")
_ALLOWED_ROOTS = ("docs",)
_ALLOWED_TOP_LEVEL = {"README.md", "AGENTS.md"}
_MAX_FILE_BYTES = 512_000


def _tokens(text: str) -> set[str]:
    return {token.lower() for token in _TOKEN.findall(str(text or ""))}


def _candidate_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for name in sorted(_ALLOWED_TOP_LEVEL):
        path = root / name
        if path.is_file():
            files.append(path)
    for relative in _ALLOWED_ROOTS:
        base = root / relative
        if not base.is_dir():
            continue
        files.extend(sorted(path for path in base.rglob("*.md") if path.is_file()))
    return files[:500]


def search_project_docs(root: str | Path, query: str, *, max_results: int = 6) -> dict[str, Any]:
    base = Path(root).resolve()
    wanted = _tokens(query)
    if not wanted:
        return {"schema_version": "quantia-doc-search-v1", "query": query, "results": []}

    scored: list[tuple[int, str, str, str]] = []
    for path in _candidate_files(base):
        try:
            if path.stat().st_size > _MAX_FILE_BYTES:
                continue
            raw = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        lower = raw.lower()
        score = sum(lower.count(token) for token in wanted)
        if score <= 0:
            continue
        lines = raw.splitlines()
        best_index = 0
        best_line_score = -1
        for index, line in enumerate(lines):
            line_tokens = _tokens(line)
            line_score = len(wanted & line_tokens)
            if line_score > best_line_score:
                best_line_score = line_score
                best_index = index
        start = max(0, best_index - 3)
        end = min(len(lines), best_index + 8)
        snippet = "\n".join(lines[start:end]).strip()[:2400]
        relative = path.relative_to(base).as_posix()
        digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        scored.append((score, relative, snippet, digest))

    scored.sort(key=lambda item: (-item[0], item[1]))
    results = [
        {
            "path": relative,
            "score": score,
            "snippet": snippet,
            "sha256": digest,
        }
        for score, relative, snippet, digest in scored[: max(1, min(10, int(max_results)))]
    ]
    return {
        "schema_version": "quantia-doc-search-v1",
        "query": query,
        "result_count": len(results),
        "results": results,
    }


_SOURCE_ROOTS = ("scripts", "src/agentic", "src/analysis", "src/collector")
_SOURCE_MAX_FILES = 500


def _candidate_source_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for relative in _SOURCE_ROOTS:
        base = root / relative
        if base.is_dir():
            files.extend(sorted(path for path in base.rglob("*.py") if path.is_file()))
    return files[:_SOURCE_MAX_FILES]


def search_project_source(root: str | Path, query: str, *, max_results: int = 6) -> dict[str, Any]:
    """Search allowlisted checked-in Python source; never reads configuration or secret files."""
    base = Path(root).resolve()
    wanted = _tokens(query)
    if not wanted:
        return {"schema_version": "quantia-source-search-v1", "query": query, "results": []}

    scored: list[tuple[int, str, int, str, str]] = []
    for path in _candidate_source_files(base):
        try:
            if path.stat().st_size > _MAX_FILE_BYTES:
                continue
            raw = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        lower = raw.lower()
        score = sum(lower.count(token) for token in wanted)
        if score <= 0:
            continue
        lines = raw.splitlines()
        best_index = 0
        best_line_score = -1
        for index, line in enumerate(lines):
            line_score = len(wanted & _tokens(line))
            if line_score > best_line_score:
                best_line_score = line_score
                best_index = index
        start = max(0, best_index - 4)
        end = min(len(lines), best_index + 9)
        snippet = "\n".join(lines[start:end]).strip()[:2600]
        relative = path.relative_to(base).as_posix()
        digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        scored.append((score, relative, start + 1, snippet, digest))

    scored.sort(key=lambda item: (-item[0], item[1]))
    results = [
        {"path": relative, "score": score, "start_line": start_line,
         "snippet": snippet, "sha256": digest}
        for score, relative, start_line, snippet, digest
        in scored[: max(1, min(10, int(max_results)))]
    ]
    return {
        "schema_version": "quantia-source-search-v1",
        "query": query,
        "result_count": len(results),
        "results": results,
    }


def register_source_retriever_tool(registry: ToolRegistry, context: ToolContext) -> ToolRegistry:
    async def handler(arguments: dict[str, Any]) -> ToolObservation:
        started = time.monotonic()
        query = str(arguments.get("query") or "")
        max_results = max(1, min(10, int(arguments.get("max_results", 6))))
        try:
            payload = search_project_source(context.root, query, max_results=max_results)
            content = json.dumps(payload, ensure_ascii=False, allow_nan=False)
            if len(content) > context.output_limit_chars:
                content = content[: context.output_limit_chars]
            return ToolObservation(
                tool_name="search_quantia_source",
                arguments={"query": query, "max_results": max_results},
                ok=True,
                content=content,
                elapsed_ms=int((time.monotonic() - started) * 1000),
                content_sha256=hashlib.sha256(content.encode("utf-8")).hexdigest(),
            )
        except Exception as exc:
            return ToolObservation(
                tool_name="search_quantia_source",
                arguments={"query": query, "max_results": max_results},
                ok=False,
                content="",
                elapsed_ms=int((time.monotonic() - started) * 1000),
                error=f"{type(exc).__name__}: {exc}",
            )

    registry.register(
        ToolSpec(
            name="search_quantia_source",
            description=(
                "Search allowlisted checked-in Python source under scripts/, src/agentic/, "
                "src/analysis/ and src/collector/. Returns matching snippets, line numbers and hashes. "
                "Use for questions about what a command or implementation actually does; this searches code, not runtime database rows."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "maxLength": 1000},
                    "max_results": {"type": "integer", "minimum": 1, "maximum": 10, "default": 6},
                },
                "required": ["query"],
                "additionalProperties": False,
            },
            read_only=True,
            timeout_seconds=10,
            capability="READ",
        ),
        handler,
    )
    return registry


def register_docs_retriever_tool(registry: ToolRegistry, context: ToolContext) -> ToolRegistry:
    async def handler(arguments: dict[str, Any]) -> ToolObservation:
        started = time.monotonic()
        query = str(arguments.get("query") or "")
        max_results = max(1, min(10, int(arguments.get("max_results", 6))))
        try:
            payload = search_project_docs(context.root, query, max_results=max_results)
            content = json.dumps(payload, ensure_ascii=False, allow_nan=False)
            if len(content) > context.output_limit_chars:
                content = content[: context.output_limit_chars]
            return ToolObservation(
                tool_name="search_quantia_docs",
                arguments={"query": query, "max_results": max_results},
                ok=True,
                content=content,
                elapsed_ms=int((time.monotonic() - started) * 1000),
                content_sha256=hashlib.sha256(content.encode("utf-8")).hexdigest(),
            )
        except Exception as exc:
            return ToolObservation(
                tool_name="search_quantia_docs",
                arguments={"query": query, "max_results": max_results},
                ok=False,
                content="",
                elapsed_ms=int((time.monotonic() - started) * 1000),
                error=f"{type(exc).__name__}: {exc}",
            )

    registry.register(
        ToolSpec(
            name="search_quantia_docs",
            description=(
                "Search Quantia's checked-in Markdown documentation and return source snippets with file hashes. "
                "Use for architecture, metric definitions, implementation rationale and project behavior that should be grounded in repository docs rather than model memory."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "maxLength": 1000},
                    "max_results": {"type": "integer", "minimum": 1, "maximum": 10, "default": 6},
                },
                "required": ["query"],
                "additionalProperties": False,
            },
            read_only=True,
            timeout_seconds=10,
            capability="READ",
        ),
        handler,
    )
    return registry


__all__ = [
    "register_docs_retriever_tool",
    "register_source_retriever_tool",
    "search_project_docs",
    "search_project_source",
]

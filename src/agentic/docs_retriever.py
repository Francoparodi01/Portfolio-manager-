from __future__ import annotations

import hashlib
import json
import math
import re
import time
import unicodedata
from pathlib import Path
from typing import Any

from .contracts import ToolObservation, ToolSpec
from .tools import ToolContext, ToolRegistry


_TOKEN = re.compile(r"[a-z0-9_]{2,}")
_STOP = set("a al algo ante como con cual cuando de del desde donde el en es esa ese esta este estos ha hace hacia hasta hay la las lo los mas me mi no o para por que se si sin sobre son su sus te tiene un una unos unas y the a an and are as at be by for from in is it of on or that this to with".split())
_ALLOWED_ROOTS = ("docs",)
_ALLOWED_TOP_LEVEL = {"README.md", "AGENTS.md"}
_MAX_FILE_BYTES = 512_000


def _tokens(text: str) -> set[str]:
    normalized = unicodedata.normalize("NFKD", str(text or "").lower())
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    return set(_TOKEN.findall(normalized)) - _STOP


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
    return [path for path in files if path.resolve().is_relative_to(root.resolve())][:500]


def search_project_docs(root: str | Path, query: str, *, max_results: int = 6) -> dict[str, Any]:
    base = Path(root).resolve()
    wanted = _tokens(query)
    if not wanted:
        return {"schema_version": "quantia-doc-search-v1", "query": query, "results": []}

    documents = []
    for path in _candidate_files(base):
        try:
            if path.stat().st_size > _MAX_FILE_BYTES:
                continue
            raw = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        doc_tokens = _tokens(raw)
        if not wanted & doc_tokens:
            continue
        lines = raw.splitlines()
        relative = path.relative_to(base).as_posix()
        title = next((line.lstrip("# ") for line in lines if line.startswith("# ")), relative)
        documents.append((relative, raw, lines, title, doc_tokens))

    # Binary token coverage prevents long documents/repeated words dominating.
    weights = {token: 1 + math.log((len(documents) + 1) / (1 + sum(token in doc[4] for doc in documents))) for token in wanted}

    def coverage(tokens: set[str]) -> float:
        return sum(weights[token] for token in wanted & tokens)

    results = []
    for relative, raw, lines, title, doc_tokens in documents:
        best = None
        heading = title
        for index, line in enumerate(lines):
            if line.startswith("#"):
                heading = line.lstrip("# ")
            if not wanted & _tokens(line):
                continue
            start = max(0, index - 3)
            end = min(len(lines), index + 15)
            snippet = "\n".join(lines[start:end])[:2400]
            score = coverage(_tokens(snippet)) + 0.8 * coverage(_tokens(heading))
            if best is None or score > best[0]:
                best = (score, start, snippet)
        if best is None:
            continue
        score, start, snippet = best
        score += 1.5 * coverage(_tokens(title)) + 0.4 * coverage(_tokens(relative))
        results.append({
            "path": relative,
            "document_title": title,
            "score": round(score, 6),
            "line_start": start + 1,
            "line_end": start + snippet.count("\n") + 1,
            "snippet": snippet,
            "sha256": hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        })
    results.sort(key=lambda item: (-item["score"], item["path"]))
    results = results[:max(1, min(10, int(max_results)))]
    return {
        "schema_version": "quantia-doc-search-v1",
        "retrieval_method": "weighted-passage-coverage-v2",
        "query": query,
        "result_count": len(results),
        "results": results,
    }


def _bounded_json(payload: dict[str, Any], limit: int) -> str:
    """Keep observations parseable even with small tool-output budgets."""
    payload = {**payload, "results": [dict(item) for item in payload.get("results", [])]}
    def encode() -> str:
        return json.dumps(payload, ensure_ascii=False, allow_nan=False)
    while len(encode()) > limit:
        payload["truncated"] = True
        if len(payload["results"]) > 1:
            payload["results"].pop()
        elif payload["results"] and len(payload["results"][0]["snippet"]) > 160:
            item = payload["results"][0]
            item["snippet"] = item["snippet"][:max(160, len(item["snippet"]) // 2)]
            item["line_end"] = item["line_start"] + item["snippet"].count("\n")
        elif payload["results"]:
            payload["results"].pop()
        elif payload.get("query"):
            payload["query"] = ""
        else:
            return json.dumps({"schema_version": "quantia-doc-search-v1", "results": [], "result_count": 0, "truncated": True})
        payload["result_count"] = len(payload["results"])
    return encode()


def register_docs_retriever_tool(registry: ToolRegistry, context: ToolContext) -> ToolRegistry:
    async def handler(arguments: dict[str, Any]) -> ToolObservation:
        started = time.monotonic()
        query = str(arguments.get("query") or "")
        max_results = max(1, min(10, int(arguments.get("max_results", 6))))
        try:
            payload = search_project_docs(context.root, query, max_results=max_results)
            content = _bounded_json(payload, context.output_limit_chars)
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
                "Search Quantia's Markdown documentation and return relevant passages with paths, line ranges and file hashes. "
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


__all__ = ["register_docs_retriever_tool", "search_project_docs"]

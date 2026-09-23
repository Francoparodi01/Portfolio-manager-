from __future__ import annotations

import asyncio
import hashlib
import html
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.analysis.macro import fetch_macro, get_macro_regime
from src.collector.db import PortfolioDatabase

from .contracts import (
    ToolHandler,
    ToolObservation,
    ToolSpec,
    ToolValidationError,
    canonical_json,
)


_TAG_RE = re.compile(r"<[^>]+>")
_TICKER_RE = re.compile(r"^[A-Z0-9][A-Z0-9.\-=]{0,14}$")


@dataclass(frozen=True)
class ToolContext:
    database_url: str
    owner_chat_id: int | None = None
    repo_root: str | None = None
    output_limit_chars: int = 18000
    tool_timeout_seconds: float = 600.0

    @property
    def root(self) -> Path:
        if self.repo_root:
            return Path(self.repo_root)
        return Path(__file__).resolve().parents[2]


@dataclass
class RegisteredTool:
    spec: ToolSpec
    handler: ToolHandler


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: dict[str, RegisteredTool] = {}

    def register(self, spec: ToolSpec, handler: ToolHandler) -> None:
        if not spec.read_only:
            raise ValueError(
                f"unsafe tool rejected: {spec.name}. Quantia agent only accepts read-only tools"
            )
        if spec.name in self._tools:
            raise ValueError(f"duplicate tool: {spec.name}")
        self._tools[spec.name] = RegisteredTool(spec=spec, handler=handler)

    def specs(self) -> list[ToolSpec]:
        return [item.spec for item in self._tools.values()]

    def get(self, name: str) -> RegisteredTool:
        try:
            return self._tools[name]
        except KeyError as exc:
            raise ToolValidationError(f"unknown tool: {name}") from exc


def _clean_text(value: str) -> str:
    unescaped = html.unescape(value or "")
    no_tags = _TAG_RE.sub("", unescaped)
    lines = [line.rstrip() for line in no_tags.replace("\r\n", "\n").splitlines()]
    return "\n".join(line for line in lines if line.strip()).strip()


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    return str(value)


def _validate_schema(schema: dict[str, Any], arguments: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(arguments, dict):
        raise ToolValidationError("tool arguments must be an object")
    if schema.get("type") != "object":
        raise ToolValidationError("unsupported tool schema")

    properties = schema.get("properties") or {}
    required = set(schema.get("required") or [])
    unknown = set(arguments) - set(properties)
    if schema.get("additionalProperties") is False and unknown:
        raise ToolValidationError(f"unknown argument(s): {sorted(unknown)}")
    missing = required - set(arguments)
    if missing:
        raise ToolValidationError(f"missing required argument(s): {sorted(missing)}")

    clean: dict[str, Any] = {}
    for key, value in arguments.items():
        rule = properties.get(key) or {}
        expected = rule.get("type")
        if expected == "integer":
            if isinstance(value, bool):
                raise ToolValidationError(f"{key} must be an integer")
            try:
                value = int(value)
            except (TypeError, ValueError) as exc:
                raise ToolValidationError(f"{key} must be an integer") from exc
            if "minimum" in rule and value < int(rule["minimum"]):
                raise ToolValidationError(f"{key} below minimum")
            if "maximum" in rule and value > int(rule["maximum"]):
                raise ToolValidationError(f"{key} above maximum")
        elif expected == "string":
            value = str(value).strip()
            if not value:
                raise ToolValidationError(f"{key} cannot be empty")
            if "maxLength" in rule and len(value) > int(rule["maxLength"]):
                raise ToolValidationError(f"{key} too long")
        elif expected == "boolean":
            if not isinstance(value, bool):
                raise ToolValidationError(f"{key} must be boolean")
        clean[key] = value
    return clean


async def _run_subprocess(
    context: ToolContext,
    *,
    tool_name: str,
    arguments: dict[str, Any],
    command: list[str],
    timeout_seconds: float | None = None,
) -> ToolObservation:
    started = time.monotonic()
    proc = await asyncio.create_subprocess_exec(
        *command,
        cwd=str(context.root),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=os.environ.copy(),
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(),
            timeout=timeout_seconds or context.tool_timeout_seconds,
        )
    except asyncio.TimeoutError:
        proc.kill()
        await proc.communicate()
        elapsed = int((time.monotonic() - started) * 1000)
        return ToolObservation(
            tool_name=tool_name,
            arguments=arguments,
            ok=False,
            content="",
            elapsed_ms=elapsed,
            error=f"tool timeout after {timeout_seconds or context.tool_timeout_seconds:.0f}s",
        )

    elapsed = int((time.monotonic() - started) * 1000)
    out = _clean_text(stdout.decode("utf-8", errors="replace"))
    err = _clean_text(stderr.decode("utf-8", errors="replace"))
    if len(out) > context.output_limit_chars:
        out = out[: context.output_limit_chars] + "\n[output truncated]"
    if len(err) > 4000:
        err = err[-4000:]

    combined = out
    if proc.returncode != 0 and err:
        combined = (out + "\n\nstderr:\n" + err).strip()

    digest = hashlib.sha256(combined.encode("utf-8")).hexdigest() if combined else None
    return ToolObservation(
        tool_name=tool_name,
        arguments=arguments,
        ok=proc.returncode == 0,
        content=combined,
        elapsed_ms=elapsed,
        error=None if proc.returncode == 0 else f"exit code {proc.returncode}",
        content_sha256=digest,
    )


def build_default_registry(context: ToolContext) -> ToolRegistry:
    registry = ToolRegistry()

    async def portfolio_snapshot(arguments: dict[str, Any]) -> ToolObservation:
        started = time.monotonic()
        db = PortfolioDatabase(context.database_url)
        try:
            await db.connect()
            snapshot = await db.get_latest_snapshot(owner_chat_id=context.owner_chat_id)
            if not snapshot:
                content = json.dumps(
                    {"status": "missing", "message": "No portfolio snapshot available."},
                    ensure_ascii=False,
                )
                return ToolObservation(
                    tool_name="get_portfolio_snapshot",
                    arguments=arguments,
                    ok=False,
                    content=content,
                    elapsed_ms=int((time.monotonic() - started) * 1000),
                    error="portfolio snapshot missing",
                    content_sha256=hashlib.sha256(content.encode()).hexdigest(),
                )

            positions = []
            total = float(snapshot.get("total_value_ars", 0.0) or 0.0)
            for raw in snapshot.get("positions") or []:
                market_value = float(raw.get("market_value", 0.0) or 0.0)
                positions.append(
                    {
                        "ticker": str(raw.get("ticker") or "").upper(),
                        "quantity": raw.get("quantity"),
                        "price": raw.get("price") or raw.get("current_price"),
                        "market_value_ars": market_value,
                        "weight": (market_value / total) if total > 0 else None,
                        "pnl_pct": raw.get("pnl_pct"),
                    }
                )
            positions.sort(key=lambda item: float(item.get("market_value_ars") or 0.0), reverse=True)
            payload = {
                "snapshot_id": snapshot.get("snapshot_id") or snapshot.get("id"),
                "scraped_at": _json_safe(snapshot.get("scraped_at")),
                "cash_ars": snapshot.get("cash_ars"),
                "total_value_ars": snapshot.get("total_value_ars"),
                "confidence_score": snapshot.get("confidence_score"),
                "positions": positions,
            }
            content = json.dumps(_json_safe(payload), ensure_ascii=False)
            return ToolObservation(
                tool_name="get_portfolio_snapshot",
                arguments=arguments,
                ok=True,
                content=content[: context.output_limit_chars],
                elapsed_ms=int((time.monotonic() - started) * 1000),
                content_sha256=hashlib.sha256(content.encode()).hexdigest(),
            )
        except Exception as exc:
            return ToolObservation(
                tool_name="get_portfolio_snapshot",
                arguments=arguments,
                ok=False,
                content="",
                elapsed_ms=int((time.monotonic() - started) * 1000),
                error=f"{type(exc).__name__}: {exc}",
            )
        finally:
            try:
                await db.close()
            except Exception:
                pass

    registry.register(
        ToolSpec(
            name="get_portfolio_snapshot",
            description=(
                "Read the latest persisted portfolio snapshot: positions, cash, total value, "
                "snapshot timestamp and confidence. Use this before reasoning about current exposure."
            ),
            input_schema={
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
            read_only=True,
            timeout_seconds=30,
        ),
        portfolio_snapshot,
    )

    async def macro_context(arguments: dict[str, Any]) -> ToolObservation:
        started = time.monotonic()
        try:
            macro = await asyncio.to_thread(fetch_macro)
            payload = macro.to_dict() if hasattr(macro, "to_dict") else dict(macro.__dict__)
            try:
                payload["regime"] = get_macro_regime(macro)
            except Exception as exc:
                payload["regime_error"] = f"{type(exc).__name__}: {exc}"
            content = json.dumps(_json_safe(payload), ensure_ascii=False)
            return ToolObservation(
                tool_name="get_macro_context",
                arguments=arguments,
                ok=True,
                content=content[: context.output_limit_chars],
                elapsed_ms=int((time.monotonic() - started) * 1000),
                content_sha256=hashlib.sha256(content.encode()).hexdigest(),
            )
        except Exception as exc:
            return ToolObservation(
                tool_name="get_macro_context",
                arguments=arguments,
                ok=False,
                content="",
                elapsed_ms=int((time.monotonic() - started) * 1000),
                error=f"{type(exc).__name__}: {exc}",
            )

    registry.register(
        ToolSpec(
            name="get_macro_context",
            description=(
                "Fetch the current Quantia macro snapshot and regime using the existing macro module. "
                "This is an external read and does not write decisions or orders."
            ),
            input_schema={
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
            read_only=True,
            timeout_seconds=90,
        ),
        macro_context,
    )

    def owner_args() -> list[str]:
        if context.owner_chat_id is None:
            return []
        return ["--owner-chat-id", str(context.owner_chat_id)]

    async def analyze_portfolio(arguments: dict[str, Any]) -> ToolObservation:
        command = [
            sys.executable,
            "scripts/run_analysis.py",
            "--no-telegram",
            "--no-llm",
            "--skip-radar",
            "--no-persist",
            *owner_args(),
        ]
        return await _run_subprocess(
            context,
            tool_name="analyze_portfolio",
            arguments=arguments,
            command=command,
        )

    registry.register(
        ToolSpec(
            name="analyze_portfolio",
            description=(
                "Run Quantia's existing deterministic portfolio analysis + optimizer + execution "
                "planner in NO-PERSIST mode. It may propose a plan, but cannot persist decisions or "
                "execute orders. Use when a full portfolio-level answer is needed."
            ),
            input_schema={
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
            read_only=True,
            timeout_seconds=context.tool_timeout_seconds,
        ),
        analyze_portfolio,
    )

    async def analyze_ticker(arguments: dict[str, Any]) -> ToolObservation:
        ticker = str(arguments["ticker"]).upper().strip()
        if not _TICKER_RE.fullmatch(ticker):
            raise ToolValidationError("invalid ticker format")
        command = [
            sys.executable,
            "scripts/run_analysis.py",
            "--tickers",
            ticker,
            "--no-telegram",
            "--no-llm",
            "--skip-radar",
            "--no-persist",
            *owner_args(),
        ]
        return await _run_subprocess(
            context,
            tool_name="analyze_ticker",
            arguments={"ticker": ticker},
            command=command,
        )

    registry.register(
        ToolSpec(
            name="analyze_ticker",
            description=(
                "Run the existing deterministic Quantia analysis for one ticker in NO-PERSIST mode. "
                "Use for targeted evidence after a ticker becomes relevant."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "ticker": {"type": "string", "maxLength": 15},
                },
                "required": ["ticker"],
                "additionalProperties": False,
            },
            read_only=True,
            timeout_seconds=context.tool_timeout_seconds,
        ),
        analyze_ticker,
    )

    async def scan_opportunities(arguments: dict[str, Any]) -> ToolObservation:
        limit = int(arguments.get("limit", 8))
        command = [
            sys.executable,
            "scripts/run_opportunity.py",
            "--no-telegram",
            "--no-persist",
            "--max",
            str(limit),
            *owner_args(),
        ]
        return await _run_subprocess(
            context,
            tool_name="scan_opportunities",
            arguments={"limit": limit},
            command=command,
        )

    registry.register(
        ToolSpec(
            name="scan_opportunities",
            description=(
                "Run Quantia's existing opportunity/radar pipeline in NO-PERSIST mode and return "
                "the top external candidates. Use only when the goal needs alternatives outside "
                "the current portfolio."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "minimum": 1, "maximum": 12},
                },
                "additionalProperties": False,
            },
            read_only=True,
            timeout_seconds=context.tool_timeout_seconds,
        ),
        scan_opportunities,
    )

    async def performance(arguments: dict[str, Any]) -> ToolObservation:
        command = [
            sys.executable,
            "scripts/run_performance.py",
            "--no-telegram",
            *owner_args(),
        ]
        return await _run_subprocess(
            context,
            tool_name="get_performance",
            arguments=arguments,
            command=command,
        )

    registry.register(
        ToolSpec(
            name="get_performance",
            description=(
                "Read Quantia's current performance report over already-persisted decisions and "
                "outcomes. This does not execute trades."
            ),
            input_schema={
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
            read_only=True,
            timeout_seconds=context.tool_timeout_seconds,
        ),
        performance,
    )

    return registry


async def execute_tool(
    registry: ToolRegistry,
    *,
    name: str,
    arguments: dict[str, Any],
) -> ToolObservation:
    item = registry.get(name)
    clean = _validate_schema(item.spec.input_schema, arguments)
    try:
        return await asyncio.wait_for(
            item.handler(clean),
            timeout=item.spec.timeout_seconds + 5,
        )
    except ToolValidationError:
        raise
    except asyncio.TimeoutError:
        return ToolObservation(
            tool_name=name,
            arguments=clean,
            ok=False,
            content="",
            error=f"tool wrapper timeout after {item.spec.timeout_seconds:.0f}s",
        )
    except Exception as exc:
        return ToolObservation(
            tool_name=name,
            arguments=clean,
            ok=False,
            content="",
            error=f"{type(exc).__name__}: {exc}",
        )


def tool_call_key(name: str, arguments: dict[str, Any]) -> str:
    return f"{name}:{canonical_json(arguments)}"

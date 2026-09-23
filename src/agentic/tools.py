from __future__ import annotations

import asyncio
import hashlib
import html
import json
import math
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import asyncpg

from src.analysis.macro import fetch_macro, get_macro_regime
from src.collector.db import PortfolioDatabase

from .contracts import (
    ToolHandler,
    ToolObservation,
    ToolSpec,
    ToolValidationError,
    canonical_json,
)
from .read_only import connect_read_only, guarded_pool


_TAG_RE = re.compile(r"<[^>]+>")
_TICKER_RE = re.compile(r"^[A-Z0-9][A-Z0-9.\-=]{0,14}$")


@dataclass(frozen=True)
class ToolContext:
    database_url: str
    owner_chat_id: int | None = None
    repo_root: str | None = None
    output_limit_chars: int = 18000
    tool_timeout_seconds: float = 600.0
    legacy_single_owner: bool = False

    def __post_init__(self):
        if not self.owner_chat_id:
            raise ValueError("an explicit account owner is required")
        if not 256 <= self.output_limit_chars <= 100000:
            raise ValueError("output limit must be between 256 and 100000")
        if not math.isfinite(self.tool_timeout_seconds) or not 0 < self.tool_timeout_seconds <= 600:
            raise ValueError("tool timeout must be within (0, 600] seconds")

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

    def validate(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        return _validate_schema(self.get(name).spec.input_schema, arguments)


def read_only_dsn(dsn: str) -> str:
    parts = urlsplit(dsn.replace("postgresql+asyncpg://", "postgresql://"))
    params = dict(parse_qsl(parts.query))
    params.update(default_transaction_read_only="on", statement_timeout="60000")
    return urlunsplit(parts._replace(query=urlencode(params)))


async def verify_single_owner(dsn: str, owner_chat_id: int) -> bool:
    conn = await connect_read_only(read_only_dsn(dsn), command_timeout=60)
    try:
        return not await conn.fetchval("""SELECT EXISTS (
            SELECT 1 FROM portfolio_snapshots WHERE owner_chat_id IS NOT NULL AND owner_chat_id<>$1
            UNION ALL SELECT 1 FROM decision_log WHERE owner_chat_id IS NOT NULL AND owner_chat_id<>$1
            UNION ALL SELECT 1 FROM broker_fills WHERE owner_chat_id IS NOT NULL AND owner_chat_id<>$1
        )""", owner_chat_id)
    finally:
        await conn.close()


def _clean_text(value: str) -> str:
    unescaped = html.unescape(value or "")
    no_tags = _TAG_RE.sub("", unescaped)
    lines = [line.rstrip() for line in no_tags.replace("\r\n", "\n").splitlines()]
    return "\n".join(line for line in lines if line.strip()).strip()


def _json_safe(value: Any) -> Any:
    if isinstance(value, float) and not math.isfinite(value):
        return None
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
            if isinstance(value, bool) or not isinstance(value, int):
                raise ToolValidationError(f"{key} must be an integer")
            if "minimum" in rule and value < int(rule["minimum"]):
                raise ToolValidationError(f"{key} below minimum")
            if "maximum" in rule and value > int(rule["maximum"]):
                raise ToolValidationError(f"{key} above maximum")
        elif expected == "string":
            if not isinstance(value, str):
                raise ToolValidationError(f"{key} must be a string")
            value = value.strip()
            if not value:
                raise ToolValidationError(f"{key} cannot be empty")
            if "maxLength" in rule and len(value) > int(rule["maxLength"]):
                raise ToolValidationError(f"{key} too long")
            if key == "ticker":
                value = value.upper()
                if not _TICKER_RE.fullmatch(value):
                    raise ToolValidationError("invalid ticker format")
        elif expected == "boolean":
            if not isinstance(value, bool):
                raise ToolValidationError(f"{key} must be boolean")
        clean[key] = value
    for key, rule in properties.items():
        if key not in clean and "default" in rule:
            clean[key] = rule["default"]
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
    child_env = os.environ.copy()
    child_env["DATABASE_URL"] = read_only_dsn(context.database_url)
    # Commands are fixed by the registry; no model-supplied shell or environment.
    child_env["PGOPTIONS"] = "-c default_transaction_read_only=on -c statement_timeout=60000"
    proc = await asyncio.create_subprocess_exec(
        command[0], "-m", "src.agentic.read_only_runner", *command[1:],
        cwd=str(context.root),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=child_env,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(),
            timeout=timeout_seconds or context.tool_timeout_seconds,
        )
    except asyncio.TimeoutError:
        if proc.returncode is None:
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
    except asyncio.CancelledError:
        if proc.returncode is None:
            proc.kill()
        await proc.communicate()
        raise

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
        db = PortfolioDatabase(read_only_dsn(context.database_url))
        try:
            db._pool = await guarded_pool(read_only_dsn(context.database_url), min_size=1, max_size=2)
            snapshot = await db.get_latest_snapshot(owner_chat_id=context.owner_chat_id)
            if not snapshot and context.legacy_single_owner:
                async with db._pool.acquire() as conn:
                    row = await conn.fetchrow("""SELECT r.payload FROM raw_snapshots r
                        JOIN portfolio_snapshots p USING (snapshot_id)
                        WHERE p.owner_chat_id IS NULL ORDER BY r.scraped_at DESC LIMIT 1""")
                    snapshot = json.loads(row["payload"]) if row else None
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
            total = snapshot.get("total_value_ars")
            total = float(total) if total is not None else None
            for raw in snapshot.get("positions") or []:
                market_value = raw.get("market_value")
                market_value = float(market_value) if market_value is not None else None
                positions.append(
                    {
                        "ticker": str(raw.get("ticker") or "").upper(),
                        "quantity": raw.get("quantity"),
                        "price": raw.get("price") or raw.get("current_price"),
                        "market_value_ars": market_value,
                        "weight": (market_value / total) if total and total > 0 and market_value is not None else None,
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
            fields = ("sp500", "vix", "ccl", "mep", "riesgo_pais", "reservas", "merval", "wti")
            observed = [key for key in fields if payload.get(key) is not None]
            payload["observed_indicators"] = observed
            payload["missing_indicators"] = [key for key in fields if key not in observed]
            payload["data_status"] = "PARTIAL" if len(observed) < len(fields) else "OBSERVED"
            content = json.dumps(_json_safe(payload), ensure_ascii=False)
            return ToolObservation(
                tool_name="get_macro_context",
                arguments=arguments,
                ok=bool(observed),
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
        if not context.legacy_single_owner:
            raise ToolValidationError("legacy analysis requires a verified single-owner database")
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
        if not context.legacy_single_owner:
            raise ToolValidationError("legacy analysis requires a verified single-owner database")
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
        if not context.legacy_single_owner:
            raise ToolValidationError("legacy radar requires a verified single-owner database")
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
                    "limit": {"type": "integer", "minimum": 1, "maximum": 12, "default": 8},
                },
                "additionalProperties": False,
            },
            read_only=True,
            timeout_seconds=context.tool_timeout_seconds,
        ),
        scan_opportunities,
    )

    async def performance(arguments: dict[str, Any]) -> ToolObservation:
        # run_performance/get_performance_stats_v2 close expired trades and run
        # migrations. This explicit SELECT must never invoke that write path.
        conn = await connect_read_only(read_only_dsn(context.database_url), command_timeout=60)
        try:
            rows = await conn.fetch("""SELECT source, status, metric_scope, COUNT(*) AS n_raw,
                COUNT(COALESCE(executable_outcome_5d,outcome_5d)) AS n_recorded_5d,
                AVG(COALESCE(executable_outcome_5d,outcome_5d)) AS mean_recorded_gross_5d,
                AVG(COALESCE(executable_outcome_10d,outcome_10d)) AS mean_recorded_gross_10d,
                AVG(COALESCE(executable_outcome_20d,outcome_20d)) AS mean_recorded_gross_20d
                FROM decision_log dl
                WHERE (owner_chat_id=$1 OR ($2::boolean AND owner_chat_id IS NULL))
                  AND decided_at BETWEEN NOW()-INTERVAL '90 days' AND NOW()
                  AND outcome_basis LIKE 'canonical_cocos%'
                  AND superseded_by_id IS NULL
                  AND decision IN ('BUY','SELL','SELL_PARTIAL','SELL_FULL')
                  AND NOT EXISTS (SELECT 1 FROM broker_fills bf WHERE bf.decision_log_id=dl.id
                    AND COALESCE(bf.raw_payload,'{}'::jsonb) ? 'superseded_by_real'
                    AND NOT EXISTS (SELECT 1 FROM broker_fills live WHERE live.decision_log_id=dl.id
                      AND NOT (COALESCE(live.raw_payload,'{}'::jsonb) ? 'superseded_by_real')))
                GROUP BY source,status,metric_scope ORDER BY source,status,metric_scope""",
                context.owner_chat_id, context.legacy_single_owner)
        finally:
            await conn.close()
        content = json.dumps(_json_safe({"lookback_days": 90, "cohorts": [dict(r) for r in rows],
            "scope": "LEGACY_RECORDED_GROSS_OUTCOMES_NOT_DEDUPLICATED",
            "limitations": "No es PnL económico, EV neto, comparación pareada ni evidencia de edge. No sumar cohortes."}), ensure_ascii=False)
        return ToolObservation(tool_name="get_performance", arguments=arguments, ok=bool(rows), content=content[:context.output_limit_chars],
                               error=None if rows else "no recorded outcomes for this account")

    registry.register(
        ToolSpec(
            name="get_performance",
            description=(
                "Read account-scoped legacy gross outcomes grouped by source/status/scope. "
                "Rows are not deduplicated: not economic PnL, net EV or proof of edge. No writes."
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
        observation = await asyncio.wait_for(
            item.handler(clean),
            timeout=item.spec.timeout_seconds + 5,
        )
        observation.tool_name = name
        observation.arguments = clean
        observation.content = observation.content[:100000]
        observation.content_sha256 = hashlib.sha256(observation.content.encode("utf-8")).hexdigest()
        return observation
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

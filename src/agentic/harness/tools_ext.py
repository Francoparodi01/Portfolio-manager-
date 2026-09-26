from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any

from src.agentic.contracts import ToolObservation, ToolSpec
from src.agentic.conversation.session import ConversationSessionStore
from src.agentic.read_only import connect_read_only
from src.agentic.tools import ToolContext, ToolRegistry, read_only_dsn
from src.core.redis_client import client as redis_client

from .tools_audits import register_audit_tools


def _observation(
    *,
    tool_name: str,
    arguments: dict[str, Any],
    started: float,
    payload: dict[str, Any],
    limit: int,
    ok: bool = True,
    error: str | None = None,
) -> ToolObservation:
    return ToolObservation(
        tool_name=tool_name,
        arguments=arguments,
        ok=ok,
        content=json.dumps(payload, ensure_ascii=False, default=str)[:limit],
        elapsed_ms=int((time.monotonic() - started) * 1000),
        error=error,
    )


def register_harness_tools(registry: ToolRegistry, context: ToolContext) -> ToolRegistry:
    """Add read-only capabilities that previously lived behind Telegram commands."""

    async def decision_ledger(arguments: dict[str, Any]) -> ToolObservation:
        from src.analysis.bot_counterfactual import fetch_normalized_bot_counterfactual
        from src.analysis.decision_ledger import fetch_decision_ledger, render_decision_ledger

        started = time.monotonic()
        days = int(arguments.get("days", 90))
        conn = await connect_read_only(read_only_dsn(context.database_url), command_timeout=90)
        try:
            data = await fetch_decision_ledger(
                conn,
                days=days,
                match_window_days=2,
                owner_chat_id=context.owner_chat_id,
            )
            normalized_bot = await fetch_normalized_bot_counterfactual(
                conn,
                days=days,
                owner_chat_id=int(context.owner_chat_id or 0),
                legacy_single_owner=bool(context.legacy_single_owner),
            )
            report = render_decision_ledger(data)
        finally:
            await conn.close()
        return _observation(
            tool_name="get_decision_ledger",
            arguments=arguments,
            started=started,
            payload={
                "source": "decision_ledger",
                "mode": "PRODUCTION_OBSERVATION",
                "as_of": datetime.now(timezone.utc).isoformat(),
                "lookback_days": days,
                "normalized_bot_counterfactual": normalized_bot,
                "report": report,
            },
            limit=context.output_limit_chars,
        )

    registry.register(
        ToolSpec(
            name="get_decision_ledger",
            description=(
                "Read Quantia's account-scoped economic Decision Ledger: real execution PnL, "
                "bot-vs-human attribution, radar/swap comparisons, pending marks, and a structured "
                "deduplicated bot counterfactual. Read-only."
            ),
            input_schema={
                "type": "object",
                "properties": {"days": {"type": "integer", "minimum": 1, "maximum": 365, "default": 90}},
                "additionalProperties": False,
            },
            read_only=True,
            timeout_seconds=90,
        ),
        decision_ledger,
    )

    async def run_evidence_provenance(arguments: dict[str, Any]) -> ToolObservation:
        """Return the exact tool/evidence provenance of the preceding completed turn."""
        started = time.monotonic()
        conn = await connect_read_only(read_only_dsn(context.database_url), command_timeout=20)
        try:
            session = await ConversationSessionStore(int(context.owner_chat_id or 0)).load()
            run = await conn.fetchrow(
                """
                SELECT id::text AS run_id, goal, status, started_at, finished_at
                FROM agent_runs
                WHERE owner_chat_id=$1
                  AND status <> 'RUNNING'
                  AND metadata->>'context_namespace'='conversational-harness-v1'
                  AND metadata->>'conversation_id'=$2
                ORDER BY started_at DESC, id DESC
                LIMIT 1
                """,
                int(context.owner_chat_id or 0),
                session.conversation_id,
            )
            if not run:
                return _observation(
                    tool_name="get_run_evidence_provenance",
                    arguments=arguments,
                    started=started,
                    payload={
                        "schema_version": "run-evidence-provenance-v1",
                        "status": "missing",
                        "conversation_id": session.conversation_id,
                        "sources": [],
                        "warnings": ["No previous completed conversational run exists in this conversation."],
                    },
                    limit=context.output_limit_chars,
                )

            step_rows = await conn.fetch(
                """
                SELECT step_no, tool_name, tool_arguments, observation_ok,
                       observation_sha256, observation_cached, elapsed_ms, observation
                FROM agent_steps
                WHERE run_id=$1::uuid
                  AND decision_kind='tool'
                  AND tool_name IS NOT NULL
                ORDER BY step_no
                """,
                run["run_id"],
            )
            sources: list[dict[str, Any]] = []
            for raw in step_rows:
                row = dict(raw)
                payload: dict[str, Any] = {}
                try:
                    parsed = json.loads(str(row.get("observation") or ""))
                    if isinstance(parsed, dict):
                        payload = parsed
                except (TypeError, ValueError):
                    payload = {}
                normalized = payload.get("normalized_bot_counterfactual")
                source = {
                    "step_no": int(row.get("step_no") or 0),
                    "tool": row.get("tool_name"),
                    "arguments": row.get("tool_arguments") or {},
                    "ok": bool(row.get("observation_ok")),
                    "cached": bool(row.get("observation_cached")),
                    "elapsed_ms": int(row.get("elapsed_ms") or 0),
                    "content_sha256": row.get("observation_sha256"),
                    "source": payload.get("source"),
                    "mode": payload.get("mode"),
                    "lookback_days": payload.get("lookback_days"),
                    "scope": payload.get("scope"),
                    "schema_version": payload.get("schema_version"),
                }
                if isinstance(normalized, dict):
                    source["normalized_component"] = {
                        "source": normalized.get("source"),
                        "scope": normalized.get("scope"),
                        "lookback_days": normalized.get("lookback_days"),
                        "raw_plans_total": normalized.get("raw_plans_total"),
                        "episodes_total": normalized.get("episodes_total"),
                        "duplicates_removed": normalized.get("duplicates_removed"),
                        "episode_definition": normalized.get("episode_definition"),
                    }
                sources.append(source)

            return _observation(
                tool_name="get_run_evidence_provenance",
                arguments=arguments,
                started=started,
                payload={
                    "schema_version": "run-evidence-provenance-v1",
                    "status": "observed",
                    "conversation_id": session.conversation_id,
                    "referenced_run_id": run["run_id"],
                    "referenced_goal": run["goal"],
                    "referenced_status": run["status"],
                    "started_at": run["started_at"].isoformat() if run["started_at"] else None,
                    "finished_at": run["finished_at"].isoformat() if run["finished_at"] else None,
                    "sources": sources,
                },
                limit=context.output_limit_chars,
            )
        except Exception as exc:
            return _observation(
                tool_name="get_run_evidence_provenance",
                arguments=arguments,
                started=started,
                payload={
                    "schema_version": "run-evidence-provenance-v1",
                    "status": "error",
                    "sources": [],
                },
                limit=context.output_limit_chars,
                ok=False,
                error=f"{type(exc).__name__}: {exc}",
            )
        finally:
            await conn.close()

    registry.register(
        ToolSpec(
            name="get_run_evidence_provenance",
            description=(
                "Read the exact audited tools and source metadata used by the immediately preceding "
                "completed conversational run in the same conversation. It never substitutes new market data."
            ),
            input_schema={"type": "object", "properties": {}, "additionalProperties": False},
            read_only=True,
            timeout_seconds=20,
        ),
        run_evidence_provenance,
    )

    async def net_decision_report(arguments: dict[str, Any]) -> ToolObservation:
        from src.analysis.net_decision_report import fetch_net_decision_report, render_net_decision_report

        started = time.monotonic()
        days = int(arguments.get("days", 180))
        limit_runs = int(arguments.get("limit_runs", 6))
        conn = await connect_read_only(read_only_dsn(context.database_url), command_timeout=90)
        try:
            data = await fetch_net_decision_report(
                conn,
                days=days,
                owner_chat_id=context.owner_chat_id,
            )
            report = render_net_decision_report(data, limit_runs=limit_runs)
        finally:
            await conn.close()
        return _observation(
            tool_name="get_net_decision_report",
            arguments=arguments,
            started=started,
            payload={
                "source": "net_decision_report",
                "mode": "PRODUCTION_OBSERVATION",
                "as_of": datetime.now(timezone.utc).isoformat(),
                "lookback_days": days,
                "report": report,
            },
            limit=context.output_limit_chars,
        )

    registry.register(
        ToolSpec(
            name="get_net_decision_report",
            description=(
                "Read the owner-scoped net performance report by analysis run and decision. "
                "This is the natural-language replacement for /neto and never sends Telegram output."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "days": {"type": "integer", "minimum": 1, "maximum": 730, "default": 180},
                    "limit_runs": {"type": "integer", "minimum": 1, "maximum": 20, "default": 6},
                },
                "additionalProperties": False,
            },
            read_only=True,
            timeout_seconds=90,
        ),
        net_decision_report,
    )

    async def analytics_v2(arguments: dict[str, Any]) -> ToolObservation:
        from src.analysis.analytics_v2_live import capture, render_telegram, summarize

        started = time.monotonic()
        days = int(arguments.get("days", 180))
        conn = await connect_read_only(read_only_dsn(context.database_url), command_timeout=90)
        try:
            snapshot = await capture(
                conn,
                owner_chat_id=context.owner_chat_id,
                days=days,
                allow_legacy_null=context.legacy_single_owner,
            )
            package = summarize(snapshot)
            report = render_telegram(package)
        finally:
            await conn.close()
        return _observation(
            tool_name="get_analytics_v2",
            arguments=arguments,
            started=started,
            payload={
                "source": "analytics_v2",
                "mode": "OBSERVATION",
                "as_of": datetime.now(timezone.utc).isoformat(),
                "lookback_days": days,
                "report": report,
            },
            limit=context.output_limit_chars,
        )

    registry.register(
        ToolSpec(
            name="get_analytics_v2",
            description=(
                "Read the owner-scoped Analytics v2 observational report using the existing live capture/summarizer. "
                "No archive is written and no attribution/outcome rows are changed."
            ),
            input_schema={
                "type": "object",
                "properties": {"days": {"type": "integer", "minimum": 1, "maximum": 730, "default": 180}},
                "additionalProperties": False,
            },
            read_only=True,
            timeout_seconds=90,
        ),
        analytics_v2,
    )

    async def viability_audit(arguments: dict[str, Any]) -> ToolObservation:
        from src.analysis.viability_audit import (
            ViabilityAuditConfig,
            render_viability_audit,
            run_viability_audit,
        )

        started = time.monotonic()
        days = int(arguments.get("days", 180))
        cost_bps = float(arguments.get("cost_bps", 75))
        min_sample = int(arguments.get("min_sample", 30))
        report_obj = await run_viability_audit(
            ViabilityAuditConfig(
                database_url=read_only_dsn(context.database_url),
                days=days,
                cost_bps=cost_bps,
                min_sample=min_sample,
            )
        )
        report = render_viability_audit(report_obj)
        return _observation(
            tool_name="get_viability_audit",
            arguments=arguments,
            started=started,
            payload={
                "source": "viability_audit",
                "mode": "OBSERVATION",
                "as_of": report_obj.generated_at.isoformat(),
                "lookback_days": report_obj.days,
                "cost_bps": report_obj.cost_bps,
                "min_sample": report_obj.min_sample,
                "verdict": report_obj.verdict,
                "warnings": report_obj.warnings,
                "report": report,
            },
            limit=context.output_limit_chars,
        )

    registry.register(
        ToolSpec(
            name="get_viability_audit",
            description=(
                "Run the existing 5D/10D/20D/40D viability audit in strictly read-only mode. "
                "Unlike the legacy CLI, this tool does not refresh or write plan attributions."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "days": {"type": "integer", "minimum": 1, "maximum": 730, "default": 180},
                    "cost_bps": {"type": "integer", "minimum": 0, "maximum": 1000, "default": 75},
                    "min_sample": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 30},
                },
                "additionalProperties": False,
            },
            read_only=True,
            timeout_seconds=120,
        ),
        viability_audit,
    )

    async def meta_policy(arguments: dict[str, Any]) -> ToolObservation:
        from src.analysis.economic_meta_telegram import render_latest_meta

        started = time.monotonic()
        ticker = str(arguments.get("ticker") or "").upper().strip() or None
        report = render_latest_meta(ticker=ticker)
        return _observation(
            tool_name="get_meta_policy",
            arguments=arguments,
            started=started,
            payload={
                "source": "economic_meta_policy",
                "mode": "SHADOW",
                "capital_effect": "NO",
                "as_of": datetime.now(timezone.utc).isoformat(),
                "ticker": ticker,
                "report": report,
            },
            limit=context.output_limit_chars,
        )

    registry.register(
        ToolSpec(
            name="get_meta_policy",
            description=(
                "Read the latest Economic Meta Policy evidence. It is strictly SHADOW_ONLY and "
                "has no capital effect; never present it as a production policy."
            ),
            input_schema={
                "type": "object",
                "properties": {"ticker": {"type": "string", "maxLength": 15}},
                "additionalProperties": False,
            },
            read_only=True,
            timeout_seconds=15,
        ),
        meta_policy,
    )

    async def system_status(arguments: dict[str, Any]) -> ToolObservation:
        started = time.monotonic()
        payload: dict[str, Any] = {
            "source": "system_status",
            "as_of": datetime.now(timezone.utc).isoformat(),
            "database": "UNKNOWN",
            "redis": "UNKNOWN",
        }
        conn = None
        try:
            conn = await connect_read_only(read_only_dsn(context.database_url), command_timeout=15)
            payload["database"] = "OK"
            payload["database_time"] = (await conn.fetchval("SELECT NOW()")).isoformat()
            if await conn.fetchval("SELECT to_regclass('public.portfolio_snapshots') IS NOT NULL"):
                row = await conn.fetchrow(
                    "SELECT MAX(scraped_at) AS latest, COUNT(*) AS n FROM portfolio_snapshots WHERE owner_chat_id=$1",
                    context.owner_chat_id,
                )
                payload["portfolio_snapshot"] = {
                    "latest": row["latest"].isoformat() if row and row["latest"] else None,
                    "count": int(row["n"] or 0) if row else 0,
                }
        except Exception as exc:
            payload["database"] = "ERROR"
            payload["database_error"] = type(exc).__name__
        finally:
            if conn:
                await conn.close()
        try:
            payload["redis"] = "OK" if await redis_client.ping() else "ERROR"
        except Exception as exc:
            payload["redis"] = "ERROR"
            payload["redis_error"] = type(exc).__name__
        ok = payload["database"] == "OK"
        return _observation(
            tool_name="get_system_status",
            arguments=arguments,
            started=started,
            payload=payload,
            limit=context.output_limit_chars,
            ok=ok,
            error=None if ok else "database unavailable",
        )

    registry.register(
        ToolSpec(
            name="get_system_status",
            description="Read DB/Redis health and the latest owner-scoped portfolio snapshot timestamp. No mutation.",
            input_schema={"type": "object", "properties": {}, "additionalProperties": False},
            read_only=True,
            timeout_seconds=20,
        ),
        system_status,
    )
    return register_audit_tools(registry, context)

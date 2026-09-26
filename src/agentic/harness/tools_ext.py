from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any

from src.agentic.contracts import ToolObservation, ToolSpec
from src.agentic.read_only import connect_read_only
from src.agentic.tools import ToolContext, ToolRegistry, read_only_dsn
from src.core.redis_client import client as redis_client


def register_harness_tools(registry: ToolRegistry, context: ToolContext) -> ToolRegistry:
    """Add read-only capabilities that previously lived behind Telegram commands."""

    async def decision_ledger(arguments: dict[str, Any]) -> ToolObservation:
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
            report = render_decision_ledger(data)
        finally:
            await conn.close()
        payload = {
            "source": "decision_ledger",
            "mode": "PRODUCTION_OBSERVATION",
            "as_of": datetime.now(timezone.utc).isoformat(),
            "lookback_days": days,
            "report": report,
        }
        return ToolObservation(
            tool_name="get_decision_ledger",
            arguments=arguments,
            ok=True,
            content=json.dumps(payload, ensure_ascii=False)[: context.output_limit_chars],
            elapsed_ms=int((time.monotonic() - started) * 1000),
        )

    registry.register(
        ToolSpec(
            name="get_decision_ledger",
            description=(
                "Read Quantia's account-scoped economic Decision Ledger: real execution PnL, "
                "bot-vs-human attribution, radar/swap comparisons and pending marks. Read-only."
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

    async def meta_policy(arguments: dict[str, Any]) -> ToolObservation:
        from src.analysis.economic_meta_telegram import render_latest_meta

        started = time.monotonic()
        ticker = str(arguments.get("ticker") or "").upper().strip() or None
        report = render_latest_meta(ticker=ticker)
        payload = {
            "source": "economic_meta_policy",
            "mode": "SHADOW",
            "capital_effect": "NO",
            "as_of": datetime.now(timezone.utc).isoformat(),
            "ticker": ticker,
            "report": report,
        }
        return ToolObservation(
            tool_name="get_meta_policy",
            arguments=arguments,
            ok=True,
            content=json.dumps(payload, ensure_ascii=False),
            elapsed_ms=int((time.monotonic() - started) * 1000),
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
        return ToolObservation(
            tool_name="get_system_status",
            arguments=arguments,
            ok=ok,
            content=json.dumps(payload, ensure_ascii=False),
            elapsed_ms=int((time.monotonic() - started) * 1000),
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
    return registry

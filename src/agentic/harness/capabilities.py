from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any

import asyncpg

from src.agentic.contracts import ToolObservation, ToolSpec
from src.agentic.tools import ToolContext, ToolRegistry, build_default_registry, read_only_dsn
from src.analysis.analytics_v2_live import capture as analytics_capture
from src.analysis.analytics_v2_live import normalize as analytics_normalize
from src.analysis.analytics_v2_live import summarize as analytics_summarize
from src.core.redis_client import client as redis_client

from .contracts import Capability, EvidenceMode


@dataclass(frozen=True)
class ToolPolicy:
    capability: Capability
    mode: EvidenceMode
    cache_ttl_seconds: int = 0
    material_failure: bool = False


_FORBIDDEN_NAMES = {
    "place_order", "execute_trade", "submit_order", "rebalance_live", "mutate_portfolio",
    "write_decision", "change_policy", "set_threshold",
}


def _json_observation(name: str, arguments: dict[str, Any], payload: dict[str, Any], started: float, *, ok: bool = True, error: str | None = None) -> ToolObservation:
    content = json.dumps(analytics_normalize(payload), ensure_ascii=False, separators=(",", ":"))
    return ToolObservation(
        tool_name=name,
        arguments=arguments,
        ok=ok,
        content=content,
        elapsed_ms=int((time.monotonic() - started) * 1000),
        error=error,
        content_sha256=hashlib.sha256(content.encode("utf-8")).hexdigest(),
    )


async def _connect(dsn: str):
    clean = read_only_dsn(dsn).replace("postgresql+asyncpg://", "postgresql://")
    return await asyncpg.connect(clean, timeout=15, command_timeout=60)


def build_conversational_registry(context: ToolContext) -> tuple[ToolRegistry, dict[str, ToolPolicy]]:
    registry = build_default_registry(context)
    policies: dict[str, ToolPolicy] = {}

    for spec in registry.specs():
        mode = EvidenceMode.RESEARCH if spec.name in {
            "compare_plan_vs_hold", "get_decision_value_added", "get_decision_counterfactuals",
            "get_similar_historical_episodes", "compare_strategy_versions", "get_replay_evidence_quality",
        } else EvidenceMode.OBSERVATION
        capability = Capability.COMPUTE if spec.name in {"analyze_portfolio", "analyze_ticker", "scan_opportunities", "get_decision_evidence"} else Capability.READ
        policies[spec.name] = ToolPolicy(capability=capability, mode=mode, material_failure=spec.name in {"get_portfolio_snapshot", "get_decision_evidence"})

    async def get_analytics_v2(arguments: dict[str, Any]) -> ToolObservation:
        started = time.monotonic()
        days = int(arguments.get("days", 180))
        conn = await _connect(context.database_url)
        try:
            snapshot = await analytics_capture(
                conn,
                owner_chat_id=int(context.owner_chat_id),
                days=days,
                allow_legacy_null=bool(context.legacy_single_owner),
            )
        finally:
            await conn.close()
        package = analytics_summarize(snapshot)
        # Keep the analytical contract intact while compacting raw episode detail.
        payload = {
            "schema_version": "analytics-v2-agent-summary-v1",
            "source": "analytics_v2_live",
            "mode": "OBSERVATION",
            "captured_at": snapshot.get("captured_at"),
            "days": days,
            "economic_pnl": package.get("economic_pnl"),
            "matching": package.get("matching"),
            "swaps": package.get("swaps"),
            "fill_coverage": snapshot.get("fill_coverage"),
            "metrics": package.get("metrics", [])[:32],
            "limitations": [
                "Live Analytics v2 is observational and not canonical point-in-time proof of edge.",
                "Economic net PnL remains unavailable when NAV, external flows and observed costs are not reconciled.",
            ],
        }
        return _json_observation("get_analytics_v2", arguments, payload, started)

    registry.register(
        ToolSpec(
            name="get_analytics_v2",
            description="Read owner-scoped Analytics v2 observational metrics for 5D/10D/20D/40D. It never mutates data and never upgrades observational evidence into a production gate.",
            input_schema={"type": "object", "properties": {"days": {"type": "integer", "minimum": 30, "maximum": 365, "default": 180}}, "additionalProperties": False},
            read_only=True,
            timeout_seconds=90,
        ),
        get_analytics_v2,
    )
    policies["get_analytics_v2"] = ToolPolicy(Capability.READ, EvidenceMode.OBSERVATION, cache_ttl_seconds=300)

    async def get_ledger_outcomes(arguments: dict[str, Any]) -> ToolObservation:
        started = time.monotonic()
        days = int(arguments.get("days", 90))
        ticker = str(arguments.get("ticker") or "").upper().strip() or None
        horizon = arguments.get("horizon")
        conn = await _connect(context.database_url)
        try:
            rows = await conn.fetch(
                """
                SELECT id, decided_at, ticker, decision, source, status, metric_scope, final_score,
                       COALESCE(executable_outcome_5d, outcome_5d) AS outcome_5d,
                       COALESCE(executable_outcome_10d, outcome_10d) AS outcome_10d,
                       COALESCE(executable_outcome_20d, outcome_20d) AS outcome_20d,
                       COALESCE(executable_outcome_40d, outcome_40d) AS outcome_40d,
                       outcome_basis
                FROM decision_log
                WHERE (owner_chat_id=$1 OR ($2::boolean AND owner_chat_id IS NULL))
                  AND decided_at >= NOW() - ($3::text || ' days')::interval
                  AND ($4::text IS NULL OR ticker=$4)
                  AND superseded_by_id IS NULL
                ORDER BY decided_at DESC, id DESC
                LIMIT 250
                """,
                context.owner_chat_id,
                context.legacy_single_owner,
                days,
                ticker,
            )
        finally:
            await conn.close()
        records = [dict(row) for row in rows]
        horizons = [int(horizon)] if horizon in {5, 10, 20, 40} else [5, 10, 20, 40]
        metrics = []
        for h in horizons:
            values = [float(row[f"outcome_{h}d"]) for row in records if row.get(f"outcome_{h}d") is not None]
            metrics.append({
                "horizon": h,
                "n": len(values),
                "mean_recorded_gross_return": (sum(values) / len(values)) if values else None,
                "positive_rate": (sum(value > 0 for value in values) / len(values)) if values else None,
            })
        payload = {
            "schema_version": "ledger-agent-evidence-v1",
            "source": "decision_log",
            "mode": "OBSERVATION",
            "as_of": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
            "days": days,
            "ticker": ticker,
            "metrics": metrics,
            "recent": records[:30],
            "limitations": "Recorded directional outcomes are not reconciled economic PnL and must not be summed across overlapping decisions.",
        }
        return _json_observation("get_ledger_outcomes", arguments, payload, started, ok=bool(records), error=None if records else "no ledger rows")

    registry.register(
        ToolSpec(
            name="get_ledger_outcomes",
            description="Read account-scoped decision ledger outcomes at 5D/10D/20D/40D. Returns recorded gross directional outcomes, not reconciled economic PnL.",
            input_schema={"type": "object", "properties": {
                "days": {"type": "integer", "minimum": 5, "maximum": 365, "default": 90},
                "ticker": {"type": "string", "maxLength": 15},
                "horizon": {"type": "integer", "minimum": 5, "maximum": 40},
            }, "additionalProperties": False},
            read_only=True,
            timeout_seconds=30,
        ),
        get_ledger_outcomes,
    )
    policies["get_ledger_outcomes"] = ToolPolicy(Capability.READ, EvidenceMode.OBSERVATION, cache_ttl_seconds=60)

    async def get_system_status(arguments: dict[str, Any]) -> ToolObservation:
        started = time.monotonic()
        conn = await _connect(context.database_url)
        try:
            db_now = await conn.fetchval("SELECT CURRENT_TIMESTAMP")
            snapshot_at = await conn.fetchval("SELECT MAX(scraped_at) FROM portfolio_snapshots WHERE owner_chat_id=$1 OR ($2::boolean AND owner_chat_id IS NULL)", context.owner_chat_id, context.legacy_single_owner)
            decision_at = await conn.fetchval("SELECT MAX(decided_at) FROM decision_log WHERE owner_chat_id=$1 OR ($2::boolean AND owner_chat_id IS NULL)", context.owner_chat_id, context.legacy_single_owner)
            market_at = await conn.fetchval("SELECT MAX(timestamp) FROM market_prices")
        finally:
            await conn.close()
        redis_ok = False
        try:
            redis_ok = bool(await redis_client.ping())
        except Exception:
            redis_ok = False
        payload = {
            "schema_version": "system-status-agent-v1",
            "source": "quantia_runtime",
            "mode": "OBSERVATION",
            "timestamp": db_now,
            "database": "OK",
            "redis": "OK" if redis_ok else "DEGRADED",
            "latest_portfolio_snapshot": snapshot_at,
            "latest_decision": decision_at,
            "latest_market_price": market_at,
        }
        return _json_observation("get_system_status", arguments, payload, started)

    registry.register(
        ToolSpec(
            name="get_system_status",
            description="Read database/Redis health and freshness timestamps. No mutations or secret exposure.",
            input_schema={"type": "object", "properties": {}, "additionalProperties": False},
            read_only=True,
            timeout_seconds=20,
        ),
        get_system_status,
    )
    policies["get_system_status"] = ToolPolicy(Capability.READ, EvidenceMode.OBSERVATION, cache_ttl_seconds=15)

    for forbidden in _FORBIDDEN_NAMES:
        policies[forbidden] = ToolPolicy(Capability.FORBIDDEN, EvidenceMode.PRODUCTION)

    return registry, policies


def assert_tool_allowed(name: str, policies: dict[str, ToolPolicy]) -> ToolPolicy:
    policy = policies.get(name)
    if policy is None:
        raise PermissionError(f"tool is not in conversational capability registry: {name}")
    if policy.capability == Capability.FORBIDDEN:
        raise PermissionError(f"forbidden capability requested: {name}")
    return policy

from __future__ import annotations

import hashlib
import json
import math
import time
from datetime import datetime, timedelta, timezone
from statistics import mean, median
from typing import Any

import asyncpg

from src.analysis.historical_edge import build_directional_episodes
from src.agentic.contracts import ToolObservation, ToolSpec
from src.agentic.read_only import connect_read_only
from src.agentic.tools import ToolContext, ToolRegistry, read_only_dsn

_ALLOWED_HORIZONS = {5, 10, 20, 40}
_CANONICAL_PREFIX = "canonical_cocos"


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _aware(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


def _profit_factor(values: list[float]) -> float | None:
    gains = sum(value for value in values if value > 0)
    losses = abs(sum(value for value in values if value < 0))
    if losses > 0:
        return gains / losses
    if gains > 0:
        return 100.0
    return None


def _summarize_side(
    episodes: list[dict[str, Any]],
    *,
    side: str,
    horizon: int,
    as_of: datetime,
    cost_bps: float,
) -> dict[str, Any]:
    outcome_key = f"outcome_{horizon}d"
    eligible: list[dict[str, Any]] = []
    for episode in episodes:
        if episode.get("action") != side:
            continue
        anchor = episode.get("anchor") or {}
        outcome = _finite(anchor.get(outcome_key))
        basis = str(anchor.get("outcome_basis") or "").lower().strip()
        filled_at = _aware(anchor.get("outcome_filled_at"))
        decided_at = _aware(anchor.get("decided_at"))
        if outcome is None:
            continue
        if not basis.startswith(_CANONICAL_PREFIX):
            continue
        if filled_at is None or filled_at > as_of:
            continue
        if decided_at is None or decided_at >= as_of:
            continue
        eligible.append(episode)

    gross = [_finite((episode.get("anchor") or {}).get(outcome_key)) for episode in eligible]
    gross_values = [value for value in gross if value is not None]
    drag = max(0.0, float(cost_bps)) / 10_000.0
    net_values = [value - drag for value in gross_values]
    dates = {
        decided.date().isoformat()
        for episode in eligible
        if (decided := _aware((episode.get("anchor") or {}).get("decided_at"))) is not None
    }
    n = len(net_values)
    if n >= 30 and len(dates) >= 12:
        quality = "HIGH"
    elif n >= 20 and len(dates) >= 8:
        quality = "MEDIUM"
    elif n >= 10 and len(dates) >= 5:
        quality = "LOW"
    else:
        quality = "INSUFFICIENT"

    return {
        "side": side,
        "n_episodes": n,
        "n_dates": len(dates),
        "recommendations_in_episodes": sum(
            int(episode.get("recommendation_count") or 1) for episode in eligible
        ),
        "win_rate_net": (sum(value > 0 for value in net_values) / n) if n else None,
        "mean_gross_return": mean(gross_values) if gross_values else None,
        "mean_net_return": mean(net_values) if net_values else None,
        "median_net_return": median(net_values) if net_values else None,
        "profit_factor_net": _profit_factor(net_values),
        "quality": quality,
    }


async def fetch_bot_directional_history(
    conn: Any,
    *,
    owner_chat_id: int,
    days: int = 180,
    horizon: int = 20,
    cost_bps: float = 150.0,
    allow_legacy_null: bool = False,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    bounded_days = max(1, min(int(days), 730))
    selected_horizon = int(horizon)
    if selected_horizon not in _ALLOWED_HORIZONS:
        raise ValueError("horizon must be one of 5, 10, 20 or 40")
    evaluated_at = as_of or datetime.now(timezone.utc)
    cutoff = evaluated_at - timedelta(days=bounded_days)

    # Keep every formal plan row in the window, including rows whose selected
    # horizon is not mature yet. They are needed to preserve run chronology and
    # correctly deduplicate consecutive recommendations into episodes.
    rows = await conn.fetch(
        """
        SELECT
            id,
            run_id::text AS run_id,
            decided_at,
            ticker,
            decision,
            final_score,
            regime,
            outcome_5d,
            outcome_10d,
            outcome_20d,
            outcome_40d,
            outcome_basis,
            outcome_filled_at,
            status,
            metric_scope,
            COALESCE(source, layers->>'source') AS source
        FROM decision_log
        WHERE (owner_chat_id = $1 OR ($4::boolean AND owner_chat_id IS NULL))
          AND decided_at >= $2
          AND decided_at < $3
          AND COALESCE(source, layers->>'source') = 'execution_plan'
          AND COALESCE(metric_scope, 'planner_audit') <> 'debug'
        ORDER BY decided_at, id
        """,
        int(owner_chat_id),
        cutoff,
        evaluated_at,
        bool(allow_legacy_null),
    )
    evidence_rows = [dict(row) for row in rows]

    try:
        hold_rows = await conn.fetch(
            """
            SELECT
                id,
                run_id::text AS run_id,
                observed_at AS decided_at,
                ticker,
                action AS decision,
                final_score,
                regime,
                NULL::double precision AS outcome_5d,
                NULL::double precision AS outcome_10d,
                NULL::double precision AS outcome_20d,
                NULL::double precision AS outcome_40d,
                NULL::text AS outcome_basis,
                NULL::timestamptz AS outcome_filled_at,
                status,
                metric_scope,
                source
            FROM position_hold_observations
            WHERE (owner_chat_id = $1 OR ($4::boolean AND owner_chat_id IS NULL))
              AND observed_at >= $2
              AND observed_at < $3
            ORDER BY observed_at, id
            """,
            int(owner_chat_id),
            cutoff,
            evaluated_at,
            bool(allow_legacy_null),
        )
    except asyncpg.UndefinedTableError:
        hold_rows = []

    evidence_rows.extend(dict(row) for row in hold_rows)
    evidence_rows.sort(
        key=lambda row: (
            _aware(row.get("decided_at")) or datetime.min.replace(tzinfo=timezone.utc),
            str(row.get("id") or ""),
        )
    )
    episodes = build_directional_episodes(evidence_rows)
    sides = {
        side: _summarize_side(
            episodes,
            side=side,
            horizon=selected_horizon,
            as_of=evaluated_at,
            cost_bps=float(cost_bps),
        )
        for side in ("BUY", "SELL")
    }
    return {
        "schema_version": "bot-directional-history-v1",
        "status": "observed",
        "source": "decision_log_formal_plan_episodes",
        "as_of": evaluated_at.isoformat(),
        "lookback_days": bounded_days,
        "horizon_days": selected_horizon,
        "cost_bps": max(0.0, float(cost_bps)),
        "legacy_null_included": bool(allow_legacy_null),
        "episode_definition": (
            "same ticker+direction in consecutive formal runs is one episode; "
            "HOLD/absence/direction change breaks it"
        ),
        "outcome_semantics": "canonical directional return; net subtracts research cost only",
        "realized_account_pnl": False,
        "dva_vs_hold": False,
        "sides": sides,
    }


def register_bot_directional_history_tool(
    registry: ToolRegistry,
    context: ToolContext,
) -> ToolRegistry:
    async def handler(arguments: dict[str, Any]) -> ToolObservation:
        started = time.monotonic()
        days = max(1, min(730, int(arguments.get("days", 180))))
        horizon = int(arguments.get("horizon", 20))
        cost_bps = max(0.0, min(1000.0, float(arguments.get("cost_bps", 150.0))))
        conn = await connect_read_only(read_only_dsn(context.database_url), command_timeout=60)
        try:
            payload = await fetch_bot_directional_history(
                conn,
                owner_chat_id=int(context.owner_chat_id or 0),
                days=days,
                horizon=horizon,
                cost_bps=cost_bps,
                allow_legacy_null=bool(context.legacy_single_owner),
            )
            content = json.dumps(payload, ensure_ascii=False, allow_nan=False)
            return ToolObservation(
                tool_name="get_bot_directional_outcomes",
                arguments={"days": days, "horizon": horizon, "cost_bps": cost_bps},
                ok=True,
                content=content[: context.output_limit_chars],
                elapsed_ms=int((time.monotonic() - started) * 1000),
                content_sha256=hashlib.sha256(content.encode("utf-8")).hexdigest(),
            )
        except Exception as exc:
            return ToolObservation(
                tool_name="get_bot_directional_outcomes",
                arguments={"days": days, "horizon": horizon, "cost_bps": cost_bps},
                ok=False,
                content="",
                elapsed_ms=int((time.monotonic() - started) * 1000),
                error=f"{type(exc).__name__}: {exc}",
            )
        finally:
            await conn.close()

    registry.register(
        ToolSpec(
            name="get_bot_directional_outcomes",
            description=(
                "Read owner-scoped historical formal bot recommendation episodes and compare BUY versus SELL "
                "at one 5D/10D/20D/40D horizon. Returns n, net win rate, gross/net EV, median and profit factor. "
                "Read-only; directional outcomes are not realized account PnL or DVA vs HOLD."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "days": {"type": "integer", "minimum": 1, "maximum": 730, "default": 180},
                    "horizon": {"type": "integer", "minimum": 5, "maximum": 40, "default": 20},
                    "cost_bps": {"type": "integer", "minimum": 0, "maximum": 1000, "default": 150},
                },
                "additionalProperties": False,
            },
            read_only=True,
            timeout_seconds=60,
        ),
        handler,
    )
    return registry


__all__ = ["fetch_bot_directional_history", "register_bot_directional_history_tool"]

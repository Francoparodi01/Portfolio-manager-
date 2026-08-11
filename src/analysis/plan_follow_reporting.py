"""Read-only reporting helpers for persisted plan-follow attribution."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return default if value is None else float(value)
    except (TypeError, ValueError):
        return default


def apply_plan_follow_overlay(
    plans: Iterable[dict[str, Any]],
    links_by_plan: Mapping[int, Mapping[str, Any]],
) -> None:
    """Prefer confirmed persisted attribution over the legacy window matcher."""
    for plan in plans:
        try:
            plan_id = int(plan.get("id"))
        except (TypeError, ValueError):
            continue
        link = links_by_plan.get(plan_id)
        if not link:
            continue

        plan["normalized_attribution_id"] = link.get("attribution_id")
        plan["normalized_temporal_quality"] = link.get("temporal_quality")
        plan["normalized_eligible"] = bool(link.get("eligible_for_viability"))
        is_confirmed = (
            plan["normalized_eligible"]
            and str(link.get("temporal_quality") or "") == "CONFIRMED_SEQUENCE"
        )
        status = str(link.get("follow_status") or "") if is_confirmed else ""
        if not status:
            continue

        plan["normalized_override_status"] = status
        plan["same_amount_ars"] = link.get("matched_amount_ars")
        plan["same_executed_at"] = link.get("executed_at")
        plan["same_executed_at_precision"] = link.get("executed_at_precision")
        plan["same_executed_at_source"] = link.get("executed_at_source")
        plan["match_evidence"] = "plan_execution_attribution"


def summarize_plan_follow_operations(operations: Iterable[Mapping[str, Any]]) -> dict:
    rows = [dict(row) for row in operations]
    eligible = [row for row in rows if bool(row.get("eligible_for_viability"))]
    closed_5d = [row for row in eligible if row.get("outcome_5d") is not None]
    by_status: dict[str, int] = {}
    for row in eligible:
        status = str(row.get("follow_status") or "UNKNOWN")
        by_status[status] = by_status.get(status, 0) + 1

    gross_returns = [_as_float(row.get("outcome_5d")) for row in closed_5d]
    actual_pnl = [
        _as_float(row.get("executed_amount_ars")) * _as_float(row.get("outcome_5d"))
        for row in closed_5d
    ]
    target_pnl = [
        _as_float(row.get("target_amount_ars")) * _as_float(row.get("outcome_5d"))
        for row in closed_5d
    ]
    return {
        "operations": len(rows),
        "eligible": len(eligible),
        "ambiguous": len(rows) - len(eligible),
        "plan_links": sum(int(row.get("plan_link_count") or 0) for row in rows),
        "movement_links": sum(int(row.get("movement_link_count") or 0) for row in rows),
        "closed_5d": len(closed_5d),
        "by_status": by_status,
        "win_rate_5d": (
            sum(value > 0 for value in gross_returns) / len(gross_returns)
            if gross_returns
            else None
        ),
        "avg_return_5d": sum(gross_returns) / len(gross_returns) if gross_returns else None,
        "actual_pnl_5d_ars": sum(actual_pnl) if actual_pnl else None,
        "target_pnl_5d_ars": sum(target_pnl) if target_pnl else None,
    }


async def fetch_plan_follow_reporting_data(
    conn,
    *,
    days: int,
    owner_chat_id: int | None = None,
) -> dict:
    ready = await conn.fetch(
        "SELECT to_regclass('public.plan_execution_attributions') AS relation_name"
    )
    if not ready or ready[0]["relation_name"] is None:
        return {
            "links_by_plan": {},
            "operations": [],
            "linked_movement_ids": set(),
            "summary": summarize_plan_follow_operations([]),
        }

    link_rows = await conn.fetch(
        """
        SELECT
            link.decision_log_id,
            link.attribution_id,
            link.matched_amount_ars,
            link.follow_ratio,
            link.follow_status,
            link.temporal_quality,
            attribution.executed_at,
            attribution.executed_at_precision,
            attribution.executed_at_source,
            attribution.eligible_for_viability
        FROM plan_execution_attribution_plans link
        JOIN plan_execution_attributions attribution
          ON attribution.id = link.attribution_id
        WHERE attribution.plan_decided_at >= NOW() - ($1::int * INTERVAL '1 day')
          AND ($2::bigint IS NULL OR attribution.owner_chat_id = $2)
        """,
        days,
        owner_chat_id,
    )
    operations = await conn.fetch(
        """
        SELECT
            attribution.id AS attribution_id,
            attribution.representative_decision_log_id,
            attribution.ticker,
            attribution.side,
            attribution.plan_decided_at,
            attribution.executed_at,
            attribution.target_amount_ars,
            attribution.executed_amount_ars,
            attribution.follow_ratio,
            attribution.follow_status,
            attribution.temporal_quality,
            attribution.eligible_for_viability,
            COALESCE(dl.executable_outcome_5d, dl.outcome_5d) AS outcome_5d,
            COALESCE(dl.executable_outcome_10d, dl.outcome_10d) AS outcome_10d,
            COALESCE(dl.executable_outcome_20d, dl.outcome_20d) AS outcome_20d,
            (SELECT COUNT(*) FROM plan_execution_attribution_plans p
             WHERE p.attribution_id = attribution.id) AS plan_link_count,
            (SELECT COUNT(*) FROM plan_execution_attribution_movements m
             WHERE m.attribution_id = attribution.id) AS movement_link_count
        FROM plan_execution_attributions attribution
        JOIN decision_log dl
          ON dl.id = attribution.representative_decision_log_id
        WHERE attribution.plan_decided_at >= NOW() - ($1::int * INTERVAL '1 day')
          AND ($2::bigint IS NULL OR attribution.owner_chat_id = $2)
        ORDER BY attribution.executed_at DESC, attribution.id DESC
        """,
        days,
        owner_chat_id,
    )
    movement_rows = await conn.fetch(
        """
        SELECT link.broker_movement_id
        FROM plan_execution_attribution_movements link
        JOIN plan_execution_attributions attribution
          ON attribution.id = link.attribution_id
        WHERE attribution.plan_decided_at >= NOW() - ($1::int * INTERVAL '1 day')
          AND ($2::bigint IS NULL OR attribution.owner_chat_id = $2)
          AND attribution.eligible_for_viability = TRUE
        """,
        days,
        owner_chat_id,
    )
    materialized = [dict(row) for row in operations]
    return {
        "links_by_plan": {int(row["decision_log_id"]): dict(row) for row in link_rows},
        "operations": materialized,
        "linked_movement_ids": {int(row["broker_movement_id"]) for row in movement_rows},
        "summary": summarize_plan_follow_operations(materialized),
    }


__all__ = [
    "apply_plan_follow_overlay",
    "fetch_plan_follow_reporting_data",
    "summarize_plan_follow_operations",
]

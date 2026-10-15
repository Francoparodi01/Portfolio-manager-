"""Read-only, owner-scoped view over the latest frozen Radar Discovery run.

The productive Radar ranking is reused as recorded. No rescoring, hindsight
outcomes, threshold changes or execution hooks are involved.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

STALE_AFTER_SECONDS = 7 * 24 * 60 * 60


def _empty(status: str) -> dict[str, Any]:
    return {
        "status": status,
        "source": "radar_discovery_snapshots",
        "research_only": True,
        "ranking_basis": "frozen_production_rank",
        "captured_at": None,
        "scoring_version": None,
        "stale": None,
        "count": 0,
        "items": [],
    }


async def fetch_top_opportunities(conn: Any, *, owner_chat_id: int) -> dict[str, Any]:
    """Never substitute historical radar_audit rows for absent prospective data."""
    schema_ready = await conn.fetchval(
        """SELECT to_regclass('public.radar_discovery_runs') IS NOT NULL
               AND to_regclass('public.radar_discovery_snapshots') IS NOT NULL"""
    )
    if not schema_ready:
        return _empty("SCHEMA_MISSING")

    run = await conn.fetchrow(
        """SELECT run_id, captured_at, captured_session, scoring_version,
                  universe_count, eligible_count,
                  EXTRACT(EPOCH FROM NOW() - captured_at) AS capture_age_seconds
           FROM radar_discovery_runs
           WHERE owner_chat_id = $1 AND status = 'COMPLETE'
                 AND captured_at <= NOW()
           ORDER BY captured_at DESC, run_id DESC
           LIMIT 1""",
        owner_chat_id,
    )
    if run is None:
        return _empty("NO_CAPTURE")

    candidates = await conn.fetch(
        """SELECT ticker, asset_type, reference_ts, reference_price,
                  price_quality_flag, feature_quality_flag, data_source_mode,
                  radar_status, readiness_state, technical_regime, v3_classification,
                  trigger_price, invalidation_price, target_price, setup_risk_reward,
                  ranking_score, rank_position, trend_component_score,
                  relative_strength_component_score, compression_component_score,
                  setup_component_score, acceptance_reason,
                  metadata->>'why_not_now' AS why_not_now,
                  metadata->>'manual_event_risk' AS manual_event_risk
           FROM radar_discovery_snapshots
           WHERE run_id = $1
             AND radar_eligible = TRUE
             AND screener_passed = TRUE
             AND in_portfolio = FALSE
             AND rank_position IS NOT NULL
             AND ticker NOT IN ('SPY', 'QQQ')
           ORDER BY rank_position ASC, ticker ASC
           LIMIT 10""",
        run["run_id"],
    )
    captured_at = run["captured_at"]
    age = max(0.0, float(run["capture_age_seconds"] or 0))
    items = []
    for candidate in candidates:
        item = dict(candidate)
        for key in ("reference_ts",):
            if isinstance(item.get(key), datetime):
                item[key] = item[key].astimezone(timezone.utc).isoformat()
        items.append(item)
    return {
        "status": "READY",
        "source": "radar_discovery_snapshots",
        "research_only": True,
        "ranking_basis": "frozen_production_rank",
        "captured_at": captured_at.astimezone(timezone.utc).isoformat(),
        "scoring_version": str(run["scoring_version"]),
        "captured_session": str(run["captured_session"]),
        "universe_count": int(run["universe_count"]),
        "eligible_count": int(run["eligible_count"]),
        "capture_age_seconds": age,
        "stale": age > STALE_AFTER_SECONDS,
        "count": len(items),
        "items": items,
    }

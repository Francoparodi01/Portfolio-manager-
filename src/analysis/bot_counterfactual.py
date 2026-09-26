from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


async def fetch_normalized_bot_counterfactual(
    conn,
    *,
    days: int,
    owner_chat_id: int,
    legacy_single_owner: bool = False,
) -> dict[str, Any]:
    """Read a deduplicated hypothetical bot-follow result.

    Episode definition is deliberately deterministic and auditable:
    - formal runs are ordered chronologically;
    - repeated BUY/SELL recommendations for the same ticker across consecutive
      formal runs belong to one episode;
    - a side change or an intervening formal run without the same recommendation
      starts a new episode;
    - episode boundaries use every persisted formal recommendation, even when a
      row is missing notional;
    - within each episode, the first row with a positive persisted notional is
      the representative used for PnL; an episode with no valid notional is
      excluded instead of receiving fabricated capital.

    This is a counterfactual over persisted formal plans. It is not realized
    account PnL and it does not use actual human execution attribution.
    """
    days = max(1, min(365, int(days)))
    row = await conn.fetchrow(
        """
        WITH run_events AS (
            SELECT run_id, MIN(created_at) AS run_at
            FROM execution_plans
            WHERE created_at >= NOW() - ($1::int * INTERVAL '1 day')
              AND run_id IS NOT NULL
              AND source = 'execution_plan'
              AND (owner_chat_id=$2 OR ($3::boolean AND owner_chat_id IS NULL))
            GROUP BY run_id
            UNION ALL
            SELECT run_id, MIN(decided_at) AS run_at
            FROM decision_log
            WHERE decided_at >= NOW() - ($1::int * INTERVAL '1 day')
              AND run_id IS NOT NULL
              AND (owner_chat_id=$2 OR ($3::boolean AND owner_chat_id IS NULL))
              AND COALESCE(source, layers->>'source') = 'execution_plan'
              AND COALESCE(run_intent, 'formal_plan') = 'formal_plan'
            GROUP BY run_id
        ),
        formal_runs AS (
            SELECT
                run_id,
                MIN(run_at) AS run_at,
                DENSE_RANK() OVER (ORDER BY MIN(run_at), run_id) AS run_seq
            FROM run_events
            GROUP BY run_id
        ),
        candidate_plans AS (
            SELECT
                dl.id,
                dl.run_id,
                fr.run_seq,
                dl.decided_at,
                dl.ticker,
                dl.decision,
                COALESCE(
                    NULLIF(ABS(NULLIF(dl.layers->>'amount_ars', '')::numeric), 0),
                    NULLIF(ABS(dl.executed_amount_ars), 0),
                    NULLIF(ABS(dl.theoretical_amount_ars), 0)
                )::double precision AS target_amount_ars,
                COALESCE(dl.executable_outcome_5d, dl.outcome_5d) AS outcome_5d,
                COALESCE(dl.executable_outcome_10d, dl.outcome_10d) AS outcome_10d,
                COALESCE(dl.executable_outcome_20d, dl.outcome_20d) AS outcome_20d
            FROM decision_log dl
            JOIN formal_runs fr ON fr.run_id = dl.run_id
            WHERE dl.decided_at >= NOW() - ($1::int * INTERVAL '1 day')
              AND (dl.owner_chat_id=$2 OR ($3::boolean AND dl.owner_chat_id IS NULL))
              AND COALESCE(dl.source, dl.layers->>'source') = 'execution_plan'
              AND COALESCE(dl.run_intent, 'formal_plan') = 'formal_plan'
              AND COALESCE(dl.metric_scope, 'planner_audit') IN ('planner_audit', 'primary')
              AND dl.status IN ('APPROVED', 'EXECUTED')
              AND dl.decision_type = 'executable'
              AND dl.decision IN ('BUY', 'SELL')
              AND dl.price_at_decision IS NOT NULL
        ),
        ordered AS (
            SELECT
                p.*,
                LAG(p.decision) OVER (
                    PARTITION BY p.ticker ORDER BY p.run_seq, p.decided_at, p.id
                ) AS prev_decision,
                LAG(p.run_seq) OVER (
                    PARTITION BY p.ticker ORDER BY p.run_seq, p.decided_at, p.id
                ) AS prev_run_seq
            FROM candidate_plans p
        ),
        boundaries AS (
            SELECT
                o.*,
                CASE
                    WHEN o.prev_decision IS NULL THEN 1
                    WHEN o.prev_decision <> o.decision THEN 1
                    WHEN o.prev_run_seq IS NULL OR o.run_seq - o.prev_run_seq > 1 THEN 1
                    ELSE 0
                END AS new_episode
            FROM ordered o
        ),
        episode_ids AS (
            SELECT
                b.*,
                SUM(b.new_episode) OVER (
                    PARTITION BY b.ticker ORDER BY b.run_seq, b.decided_at, b.id
                    ROWS UNBOUNDED PRECEDING
                ) AS episode_id
            FROM boundaries b
        ),
        representatives AS (
            SELECT
                e.*,
                ROW_NUMBER() OVER (
                    PARTITION BY e.ticker, e.episode_id
                    ORDER BY
                        CASE WHEN e.target_amount_ars IS NOT NULL AND e.target_amount_ars > 0 THEN 0 ELSE 1 END,
                        e.run_seq,
                        e.decided_at,
                        e.id
                ) AS episode_row
            FROM episode_ids e
        ),
        chosen AS (
            SELECT *
            FROM representatives
            WHERE episode_row = 1
              AND target_amount_ars IS NOT NULL
              AND target_amount_ars > 0
        )
        SELECT
            (SELECT COUNT(*) FROM candidate_plans)::int AS candidate_plans_total,
            (
                SELECT COUNT(*)
                FROM candidate_plans
                WHERE target_amount_ars IS NOT NULL AND target_amount_ars > 0
            )::int AS raw_plans_total,
            (
                SELECT COUNT(*)
                FROM candidate_plans
                WHERE target_amount_ars IS NULL OR target_amount_ars <= 0
            )::int AS excluded_missing_notional,
            COUNT(*)::int AS episodes_total,
            COUNT(outcome_5d)::int AS episodes_closed_5d,
            COUNT(outcome_10d)::int AS episodes_closed_10d,
            COUNT(outcome_20d)::int AS episodes_closed_20d,
            SUM(target_amount_ars * outcome_5d) AS pnl_5d_ars,
            SUM(target_amount_ars * outcome_10d) AS pnl_10d_ars,
            SUM(target_amount_ars * outcome_20d) AS pnl_20d_ars,
            AVG(outcome_5d) FILTER (WHERE outcome_5d IS NOT NULL) AS avg_return_5d,
            AVG(outcome_10d) FILTER (WHERE outcome_10d IS NOT NULL) AS avg_return_10d,
            AVG(outcome_20d) FILTER (WHERE outcome_20d IS NOT NULL) AS avg_return_20d
        FROM chosen
        """,
        days,
        int(owner_chat_id),
        bool(legacy_single_owner),
    )
    values = dict(row or {})
    candidate_total = int(values.get("candidate_plans_total") or 0)
    raw_total = int(values.get("raw_plans_total") or 0)
    excluded_missing_notional = int(values.get("excluded_missing_notional") or 0)
    episodes_total = int(values.get("episodes_total") or 0)
    return {
        "schema_version": "bot-follow-pnl-normalized-v1",
        "source": "decision_log_formal_plan_episodes",
        "mode": "PRODUCTION_OBSERVATION",
        "as_of": datetime.now(timezone.utc).isoformat(),
        "lookback_days": days,
        "candidate_plans_total": candidate_total,
        "raw_plans_total": raw_total,
        "excluded_missing_notional": excluded_missing_notional,
        "episodes_total": episodes_total,
        "duplicates_removed": max(0, raw_total - episodes_total),
        "episodes_closed_5d": int(values.get("episodes_closed_5d") or 0),
        "episodes_closed_10d": int(values.get("episodes_closed_10d") or 0),
        "episodes_closed_20d": int(values.get("episodes_closed_20d") or 0),
        "pnl_5d_ars": _to_float(values.get("pnl_5d_ars")),
        "pnl_10d_ars": _to_float(values.get("pnl_10d_ars")),
        "pnl_20d_ars": _to_float(values.get("pnl_20d_ars")),
        "avg_return_5d": _to_float(values.get("avg_return_5d")),
        "avg_return_10d": _to_float(values.get("avg_return_10d")),
        "avg_return_20d": _to_float(values.get("avg_return_20d")),
        "scope": "FORMAL_PLAN_DIRECTIONAL_GROSS_EPISODE_DEDUPLICATED",
        "episode_definition": (
            "Same ticker + same BUY/SELL across consecutive formal runs is one episode; "
            "a side change or an intervening formal run without that recommendation starts a new episode. "
            "Episode continuity uses all formal recommendations, and the first row with valid positive notional is the representative."
        ),
        "limitations": [
            "Hypothetical bot-follow PnL, not realized account PnL.",
            "Gross directional result before fees/slippage.",
            "Plans without a positive persisted notional are excluded; no fallback capital is invented.",
            "Episode deduplication prevents repeated consecutive recommendations from adding capital repeatedly.",
            "5D/10D/20D are alternative horizons and must not be summed.",
        ],
    }


__all__ = ["fetch_normalized_bot_counterfactual"]

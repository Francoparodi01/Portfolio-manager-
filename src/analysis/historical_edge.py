"""Point-in-time historical pattern matching for Economic Meta shadow research.

This module deliberately stays outside the production decision path. It reads
formal ``decision_log`` evidence, deduplicates repeated same-direction signals
into episodes and estimates whether a new candidate resembles historically
profitable decisions after a conservative cost assumption.

Important semantics:
- ``outcome_20d`` is Quantia's canonical *directional* return. It is not a
  portfolio HOLD counterfactual, so this module never labels it DVA vs HOLD.
- Only canonical outcomes that were already filled by the candidate ``as_of``
  timestamp are eligible. This prevents look-ahead leakage.
- Buckets/backoff are deterministic and preregistered; the code never searches
  arbitrary cuts for the most profitable historical subset.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from math import isfinite
from statistics import mean, median
from typing import Any, Mapping, Sequence


HISTORICAL_EDGE_VERSION = "historical-edge-v1"
PRIMARY_HORIZON_DAYS = 20
DEFAULT_LOOKBACK_DAYS = 365
DEFAULT_RESEARCH_COST_BPS = 150.0
CANONICAL_OUTCOME_PREFIX = "canonical_cocos"
MAX_PROFIT_FACTOR = 100.0

# Fixed, economically interpretable score bands. They intentionally align with
# existing Quantia decision/meta thresholds instead of being fitted ex post.
SCORE_BUCKETS: tuple[tuple[float, str], ...] = (
    (0.08, "LT_008"),
    (0.12, "008_012"),
    (0.18, "012_018"),
    (float("inf"), "GE_018"),
)


@dataclass(frozen=True, slots=True)
class HistoricalEdgeMatch:
    version: str
    horizon_days: int
    action: str
    score_bucket: str
    market_regime: str
    specificity: str
    lookback_days: int
    cost_bps: float
    n_episodes: int
    n_dates: int
    win_rate_net: float | None
    mean_gross_return: float | None
    mean_net_return: float | None
    median_net_return: float | None
    profit_factor_net: float | None
    top1_positive_share: float | None
    top3_positive_share: float | None
    quality: str
    passes_shadow_gate: bool
    gate_reasons: tuple[str, ...]
    outcome_semantics: str = "DIRECTIONAL_RETURN_NET_OF_RESEARCH_COST"
    dva_vs_hold_status: str = "UNAVAILABLE_NOT_A_PORTFOLIO_HOLD_COUNTERFACTUAL"

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["gate_reasons"] = list(self.gate_reasons)
        return payload


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


def _finite(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if isfinite(result) else None


def normalize_action(value: Any) -> str:
    raw = str(value or "").upper().strip()
    if raw in {"SELL", "SELL_FULL", "SELL_PARTIAL", "REDUCE", "TRIM", "EXIT", "CLOSE"}:
        return "SELL"
    if raw in {"BUY", "BUY_FULL", "BUY_PARTIAL", "ADD"}:
        return "BUY"
    if raw in {"HOLD", "WATCH", "NONE", ""}:
        return "HOLD"
    return raw


def score_bucket(score: Any) -> str:
    value = abs(_finite(score) or 0.0)
    for upper, label in SCORE_BUCKETS:
        if value < upper:
            return label
    return "GE_018"


def normalize_regime(value: Any) -> str:
    raw = str(value or "UNKNOWN").upper().strip()
    return raw or "UNKNOWN"


def _is_formal_candidate(row: Mapping[str, Any]) -> bool:
    action = normalize_action(row.get("decision") or row.get("action"))
    status = str(row.get("status") or "").upper().strip()
    if action not in {"BUY", "SELL"}:
        return False
    # Historical Edge learns from formal executable proposals only. Blocked
    # decisions remain available to the separate learning-shadow audit.
    return status in {"APPROVED", "EXECUTED"}


def _eligible_anchor(row: Mapping[str, Any], *, as_of: datetime) -> bool:
    if not _is_formal_candidate(row):
        return False
    outcome = _finite(row.get("outcome_20d"))
    if outcome is None:
        return False
    basis = str(row.get("outcome_basis") or "").lower().strip()
    if not basis.startswith(CANONICAL_OUTCOME_PREFIX):
        return False
    filled_at = _aware(row.get("outcome_filled_at"))
    if filled_at is None or filled_at > as_of:
        return False
    decided_at = _aware(row.get("decided_at") or row.get("as_of"))
    return decided_at is not None and decided_at < as_of


def build_directional_episodes(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Conservatively deduplicate repeated formal signals.

    For each ticker, the first executable BUY/SELL starts an episode. Repeating
    the same direction does not create a new sample. A recorded HOLD/non-formal
    row or a direction change closes/restarts the episode. This intentionally
    under-counts rather than treating correlated repeated recommendations as
    independent wins.
    """
    ordered = sorted(
        (dict(row) for row in rows),
        key=lambda row: (
            _aware(row.get("decided_at") or row.get("as_of"))
            or datetime.min.replace(tzinfo=timezone.utc),
            str(row.get("id") or ""),
        ),
    )
    active: dict[str, dict[str, Any]] = {}
    episodes: list[dict[str, Any]] = []

    for row in ordered:
        ticker = str(row.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        action = normalize_action(row.get("decision") or row.get("action"))
        if action not in {"BUY", "SELL"} or not _is_formal_candidate(row):
            active.pop(ticker, None)
            continue

        previous = active.get(ticker)
        if previous is not None and previous["action"] == action:
            previous["recommendation_count"] += 1
            previous["last_seen_at"] = row.get("decided_at") or row.get("as_of")
            continue

        episode = {
            "ticker": ticker,
            "action": action,
            "anchor": row,
            "recommendation_count": 1,
            "started_at": row.get("decided_at") or row.get("as_of"),
            "last_seen_at": row.get("decided_at") or row.get("as_of"),
        }
        episodes.append(episode)
        active[ticker] = episode

    return episodes


def _positive_concentration(values: Sequence[float]) -> tuple[float | None, float | None]:
    positives = sorted((float(value) for value in values if value > 0), reverse=True)
    total = sum(positives)
    if total <= 0:
        return None, None
    return sum(positives[:1]) / total, sum(positives[:3]) / total


def _profit_factor(values: Sequence[float]) -> float | None:
    positive = sum(value for value in values if value > 0)
    negative = abs(sum(value for value in values if value < 0))
    if negative > 0:
        return min(MAX_PROFIT_FACTOR, positive / negative)
    if positive > 0:
        # Keep JSON/evidence finite. A zero-loss sample is displayed as a capped
        # high PF, never Infinity, because downstream stores and UIs require
        # interoperable JSON numbers.
        return MAX_PROFIT_FACTOR
    return None


def _pool_metrics(
    episodes: Sequence[dict[str, Any]],
    *,
    as_of: datetime,
    cost_bps: float,
) -> dict[str, Any]:
    eligible = [
        episode
        for episode in episodes
        if _eligible_anchor(episode["anchor"], as_of=as_of)
    ]
    gross = [_finite(episode["anchor"].get("outcome_20d")) for episode in eligible]
    gross_values = [value for value in gross if value is not None]
    drag = max(0.0, float(cost_bps)) / 10_000.0
    net_values = [value - drag for value in gross_values]
    dates = {
        decided.date().isoformat()
        for episode in eligible
        if (decided := _aware(
            episode["anchor"].get("decided_at") or episode["anchor"].get("as_of")
        )) is not None
    }
    top1, top3 = _positive_concentration(net_values)
    return {
        "episodes": eligible,
        "n_episodes": len(net_values),
        "n_dates": len(dates),
        "win_rate_net": (
            sum(value > 0 for value in net_values) / len(net_values)
            if net_values
            else None
        ),
        "mean_gross_return": mean(gross_values) if gross_values else None,
        "mean_net_return": mean(net_values) if net_values else None,
        "median_net_return": median(net_values) if net_values else None,
        "profit_factor_net": _profit_factor(net_values),
        "top1_positive_share": top1,
        "top3_positive_share": top3,
    }


def _matches(
    episode: Mapping[str, Any],
    *,
    action: str,
    bucket: str | None,
    regime: str | None,
) -> bool:
    anchor = episode["anchor"]
    if episode["action"] != action:
        return False
    if bucket is not None and score_bucket(anchor.get("final_score")) != bucket:
        return False
    if regime is not None and normalize_regime(anchor.get("regime")) != regime:
        return False
    return True


def _quality_and_gate(metrics: Mapping[str, Any]) -> tuple[str, bool, tuple[str, ...]]:
    n = int(metrics.get("n_episodes") or 0)
    dates = int(metrics.get("n_dates") or 0)
    win = metrics.get("win_rate_net")
    ev = metrics.get("mean_net_return")
    med = metrics.get("median_net_return")
    pf = metrics.get("profit_factor_net")
    top1 = metrics.get("top1_positive_share")
    top3 = metrics.get("top3_positive_share")

    if n < 10 or dates < 5:
        quality = "INSUFFICIENT"
    elif n >= 30 and dates >= 12:
        quality = "HIGH"
    elif n >= 20 and dates >= 8:
        quality = "MEDIUM"
    else:
        quality = "LOW"

    reasons: list[str] = []
    if n < 20:
        reasons.append("HIST_SAMPLE_LT_20_EPISODES")
    if dates < 8:
        reasons.append("HIST_SAMPLE_LT_8_DATES")
    if win is None or float(win) < 0.55:
        reasons.append("HIST_WIN_RATE_LT_55PCT")
    if ev is None or float(ev) < 0.0025:
        reasons.append("HIST_EV_NET_LT_25BPS")
    if med is None or float(med) <= 0:
        reasons.append("HIST_MEDIAN_NET_NOT_POSITIVE")
    if pf is None or float(pf) < 1.10:
        reasons.append("HIST_PROFIT_FACTOR_LT_1_10")
    if top1 is not None and float(top1) > 0.40:
        reasons.append("HIST_TOP1_CONCENTRATION_GT_40PCT")
    if top3 is not None and float(top3) > 0.75:
        reasons.append("HIST_TOP3_CONCENTRATION_GT_75PCT")

    return quality, not reasons, tuple(reasons)


def match_historical_edge(
    rows: Sequence[Mapping[str, Any]],
    *,
    candidate_action: str,
    candidate_score: float,
    candidate_regime: str,
    as_of: datetime,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    cost_bps: float = DEFAULT_RESEARCH_COST_BPS,
) -> HistoricalEdgeMatch:
    action = normalize_action(candidate_action)
    bucket = score_bucket(candidate_score)
    regime = normalize_regime(candidate_regime)
    evaluated_at = _aware(as_of) or datetime.now(timezone.utc)
    bounded_days = max(30, min(int(lookback_days), 730))
    cutoff = evaluated_at - timedelta(days=bounded_days)
    visible_rows = []
    for row in rows:
        decided_at = _aware(row.get("decided_at") or row.get("as_of"))
        if decided_at is not None and cutoff <= decided_at < evaluated_at:
            visible_rows.append(row)
    episodes = build_directional_episodes(visible_rows)

    # Fixed backoff hierarchy. Every selected pool already satisfies the minimum
    # sample used by META-D, so an undersized exact cell may back off to a broader
    # preregistered pool rather than failing merely because it was too specific.
    candidates = [
        ("ACTION_SCORE_REGIME", 20, bucket, regime),
        ("ACTION_SCORE", 20, bucket, None),
        ("ACTION_REGIME", 20, None, regime),
        ("ACTION_ONLY", 30, None, None),
    ]
    selected_name = "ACTION_SCORE_REGIME"
    selected_pool: list[dict[str, Any]] = [
        episode
        for episode in episodes
        if _matches(episode, action=action, bucket=bucket, regime=regime)
    ]
    selected_metrics = _pool_metrics(
        selected_pool, as_of=evaluated_at, cost_bps=cost_bps
    )

    for name, minimum, wanted_bucket, wanted_regime in candidates:
        pool = [
            episode
            for episode in episodes
            if _matches(
                episode,
                action=action,
                bucket=wanted_bucket,
                regime=wanted_regime,
            )
        ]
        metrics = _pool_metrics(pool, as_of=evaluated_at, cost_bps=cost_bps)
        if int(metrics["n_episodes"]) >= minimum:
            selected_name = name
            selected_metrics = metrics
            break

    quality, passes, reasons = _quality_and_gate(selected_metrics)
    return HistoricalEdgeMatch(
        version=HISTORICAL_EDGE_VERSION,
        horizon_days=PRIMARY_HORIZON_DAYS,
        action=action,
        score_bucket=bucket,
        market_regime=regime,
        specificity=selected_name,
        lookback_days=bounded_days,
        cost_bps=max(0.0, float(cost_bps)),
        n_episodes=int(selected_metrics["n_episodes"]),
        n_dates=int(selected_metrics["n_dates"]),
        win_rate_net=selected_metrics["win_rate_net"],
        mean_gross_return=selected_metrics["mean_gross_return"],
        mean_net_return=selected_metrics["mean_net_return"],
        median_net_return=selected_metrics["median_net_return"],
        profit_factor_net=selected_metrics["profit_factor_net"],
        top1_positive_share=selected_metrics["top1_positive_share"],
        top3_positive_share=selected_metrics["top3_positive_share"],
        quality=quality,
        passes_shadow_gate=passes,
        gate_reasons=reasons,
    )


async def load_historical_rows(
    conn: Any,
    *,
    owner_chat_id: int,
    as_of: datetime,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> list[dict[str, Any]]:
    evaluated_at = _aware(as_of) or datetime.now(timezone.utc)
    bounded_days = max(30, min(int(lookback_days), 730))
    cutoff = evaluated_at - timedelta(days=bounded_days)
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
            outcome_20d,
            outcome_basis,
            outcome_filled_at,
            status,
            metric_scope,
            COALESCE(source, layers->>'source') AS source
        FROM decision_log
        WHERE owner_chat_id = $1
          AND decided_at >= $2
          AND decided_at < $3
          AND COALESCE(source, layers->>'source') = 'execution_plan'
          AND COALESCE(metric_scope, 'planner_audit') <> 'debug'
        ORDER BY decided_at, id
        """,
        int(owner_chat_id),
        cutoff,
        evaluated_at,
    )
    return [dict(row) for row in rows]


__all__ = [
    "DEFAULT_LOOKBACK_DAYS",
    "DEFAULT_RESEARCH_COST_BPS",
    "HISTORICAL_EDGE_VERSION",
    "HistoricalEdgeMatch",
    "MAX_PROFIT_FACTOR",
    "build_directional_episodes",
    "load_historical_rows",
    "match_historical_edge",
    "normalize_action",
    "normalize_regime",
    "score_bucket",
]

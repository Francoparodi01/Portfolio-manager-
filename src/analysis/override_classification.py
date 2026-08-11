from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Mapping

from src.core.market_calendar import is_trading_day


PENDING_OPEN = "PENDING_OPEN"
OPPOSITE = "OPPOSITE"
OVERFOLLOWED = "OVERFOLLOWED"
FOLLOWED = "FOLLOWED"
PARTIAL = "PARTIAL"
IGNORED = "IGNORED"
UNKNOWN = "UNKNOWN"
FOLLOWED_PROVISIONAL = "FOLLOWED_PROVISIONAL"
OVERFOLLOWED_PROVISIONAL = "OVERFOLLOWED_PROVISIONAL"
PARTIAL_PROVISIONAL = "PARTIAL_PROVISIONAL"
OPPOSITE_PROVISIONAL = "OPPOSITE_PROVISIONAL"

STATUS_RANK = {
    PENDING_OPEN: 0,
    IGNORED: 1,
    OPPOSITE: 2,
    PARTIAL: 3,
    FOLLOWED: 4,
    OVERFOLLOWED: 5,
    OPPOSITE_PROVISIONAL: 2,
    PARTIAL_PROVISIONAL: 3,
    FOLLOWED_PROVISIONAL: 4,
    OVERFOLLOWED_PROVISIONAL: 5,
}


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def override_target(item: Mapping[str, Any]) -> float:
    return max(as_float(item.get("target_amount_ars")), 1.0)


def override_same_ratio(item: Mapping[str, Any]) -> float:
    amount = as_float(item.get("same_amount_ars"))
    if amount <= 0:
        amount = as_float(item.get("inferred_same_amount_ars"))
    return amount / override_target(item)


def override_opposite_ratio(item: Mapping[str, Any]) -> float:
    amount = as_float(item.get("opposite_amount_ars"))
    if amount <= 0:
        amount = as_float(item.get("inferred_opposite_amount_ars"))
    return amount / override_target(item)


def classify_override(item: Mapping[str, Any]) -> str:
    if (
        item.get("match_basis") == "pending_open_revalidation"
        or item.get("match_start_at") is None
    ):
        return PENDING_OPEN

    same_ratio = override_same_ratio(item)
    opposite_ratio = override_opposite_ratio(item)
    provisional = (
        as_float(item.get("same_amount_ars")) <= 0
        and as_float(item.get("opposite_amount_ars")) <= 0
        and (
            as_float(item.get("inferred_same_amount_ars")) > 0
            or as_float(item.get("inferred_opposite_amount_ars")) > 0
        )
    )
    if same_ratio < 0.15 and opposite_ratio >= 0.15:
        return OPPOSITE_PROVISIONAL if provisional else OPPOSITE
    if same_ratio >= 1.35:
        return OVERFOLLOWED_PROVISIONAL if provisional else OVERFOLLOWED
    if same_ratio >= 0.75:
        return FOLLOWED_PROVISIONAL if provisional else FOLLOWED
    if same_ratio >= 0.15:
        return PARTIAL_PROVISIONAL if provisional else PARTIAL
    return IGNORED


def _as_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _trading_sessions_between(start: date, end: date) -> int:
    if end < start:
        return -1
    sessions = 0
    cursor = start
    while cursor < end:
        cursor += timedelta(days=1)
        if is_trading_day(cursor):
            sessions += 1
    return sessions


def attach_inferred_activity(
    plans: list[dict[str, Any]],
    activity: list[dict[str, Any]],
    *,
    match_window_sessions: int = 2,
) -> list[dict[str, Any]]:
    """Attach stable snapshot deltas as provisional plan-following evidence."""
    for plan in plans:
        if (
            as_float(plan.get("same_amount_ars")) > 0
            or as_float(plan.get("opposite_amount_ars")) > 0
        ):
            continue

        match_start = _as_datetime(plan.get("match_start_at"))
        if match_start is None:
            continue

        ticker = str(plan.get("ticker") or "").upper()
        decision = str(plan.get("decision") or "").upper()
        same_amount = 0.0
        opposite_amount = 0.0
        matched_at: datetime | None = None

        for event in activity:
            if event.get("confirmed_at") or event.get("activity_type") == "CORPORATE_ACTION":
                continue
            if str(event.get("ticker") or "").upper() != ticker:
                continue

            observed_at = _as_datetime(event.get("scraped_at"))
            if observed_at is None or observed_at < match_start:
                continue
            sessions = _trading_sessions_between(match_start.date(), observed_at.date())
            if sessions < 0 or sessions > max(1, int(match_window_sessions)):
                continue

            amount = abs(as_float(event.get("inferred_amount_ars")))
            if amount <= 0:
                continue
            if str(event.get("side") or "").upper() == decision:
                same_amount += amount
            else:
                opposite_amount += amount
            if matched_at is None or observed_at < matched_at:
                matched_at = observed_at

        if same_amount > 0:
            plan["inferred_same_amount_ars"] = same_amount
        if opposite_amount > 0:
            plan["inferred_opposite_amount_ars"] = opposite_amount
        if matched_at is not None:
            plan["inferred_executed_at"] = matched_at.isoformat()
            plan["match_evidence"] = "portfolio_snapshot"

    return plans


def override_delta(status: str, outcome_5d: Any) -> float | None:
    if outcome_5d is None:
        return None
    outcome = as_float(outcome_5d)
    if status in {IGNORED, OPPOSITE}:
        return -outcome
    if status == PARTIAL:
        return -0.5 * outcome
    if status in {FOLLOWED, OVERFOLLOWED}:
        return 0.0
    return None


def dominant_override_status(statuses: list[str]) -> str:
    if not statuses:
        return UNKNOWN
    return max(statuses, key=lambda status: STATUS_RANK.get(status, 0))


__all__ = [
    "FOLLOWED",
    "FOLLOWED_PROVISIONAL",
    "IGNORED",
    "OPPOSITE",
    "OPPOSITE_PROVISIONAL",
    "OVERFOLLOWED",
    "OVERFOLLOWED_PROVISIONAL",
    "PARTIAL",
    "PARTIAL_PROVISIONAL",
    "PENDING_OPEN",
    "STATUS_RANK",
    "UNKNOWN",
    "as_float",
    "attach_inferred_activity",
    "classify_override",
    "dominant_override_status",
    "override_delta",
    "override_opposite_ratio",
    "override_same_ratio",
    "override_target",
]

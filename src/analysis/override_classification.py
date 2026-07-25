from __future__ import annotations

from typing import Any, Mapping


PENDING_OPEN = "PENDING_OPEN"
OPPOSITE = "OPPOSITE"
OVERFOLLOWED = "OVERFOLLOWED"
FOLLOWED = "FOLLOWED"
PARTIAL = "PARTIAL"
IGNORED = "IGNORED"
UNKNOWN = "UNKNOWN"

STATUS_RANK = {
    PENDING_OPEN: 0,
    IGNORED: 1,
    OPPOSITE: 2,
    PARTIAL: 3,
    FOLLOWED: 4,
    OVERFOLLOWED: 5,
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
    return as_float(item.get("same_amount_ars")) / override_target(item)


def override_opposite_ratio(item: Mapping[str, Any]) -> float:
    return as_float(item.get("opposite_amount_ars")) / override_target(item)


def classify_override(item: Mapping[str, Any]) -> str:
    if (
        item.get("match_basis") == "pending_open_revalidation"
        or item.get("match_start_at") is None
    ):
        return PENDING_OPEN

    same_ratio = override_same_ratio(item)
    opposite_ratio = override_opposite_ratio(item)
    if same_ratio < 0.15 and opposite_ratio >= 0.15:
        return OPPOSITE
    if same_ratio >= 1.35:
        return OVERFOLLOWED
    if same_ratio >= 0.75:
        return FOLLOWED
    if same_ratio >= 0.15:
        return PARTIAL
    return IGNORED


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
    "IGNORED",
    "OPPOSITE",
    "OVERFOLLOWED",
    "PARTIAL",
    "PENDING_OPEN",
    "STATUS_RANK",
    "UNKNOWN",
    "as_float",
    "classify_override",
    "dominant_override_status",
    "override_delta",
    "override_opposite_ratio",
    "override_same_ratio",
    "override_target",
]

"""Read-only retrospective grouping of frozen volume-review candidates."""
from __future__ import annotations

from collections import Counter
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation, localcontext

from .packet_data import expected_dates


VERSION = "technical-desk-volume-clusters-v1"
POLICY = {
    "version": VERSION,
    "maximum_consecutive_flag_session_distance": 3,
    "distance_definition": "BYMA_SESSION_INDEX_NEXT_MINUS_PREVIOUS",
    "linkage": "SINGLE_LINK_TRANSITIVE_IN_DATE_ORDER",
    "price_move": "CLOSE_FIRST_FLAG_MINUS_CLOSE_PREVIOUS_BYMA_SESSION",
    "atr_basis": "WILDER_14_D_AT_PREVIOUS_BYMA_SESSION",
    "normalized_move": "SIGNED_PRICE_MOVE_DIVIDED_BY_PREVIOUS_SESSION_ATR",
    "decimal_precision": 34,
    "calendar_missing": "NOT_EVALUABLE_NO_SESSION_INFERENCE",
    "missing_price_or_atr": "NOT_EVALUABLE_NO_IMPUTATION",
    "meaning": "REVIEW_CANDIDATE_CLUSTERS_NOT_CONFIRMED_ADJUSTMENTS",
    "temporal_basis": "RETROSPECTIVE_NOT_CAUSAL_FEATURES",
}


def _day(value):
    if not isinstance(value, str):
        raise ValueError("INVALID_SESSION_DATE")
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError("INVALID_SESSION_DATE")
    return parsed


def _positive(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        result = Decimal(str(value))
    except InvalidOperation:
        return None
    return result if result.is_finite() and result > 0 else None


def _previous_session(day, calendar):
    # Stop at an unknown weekday; never jump over it to a more distant bar.
    minimum = min(item["start"] for item in calendar["verified_ranges"])
    candidate = _day(day) - timedelta(days=1)
    while candidate.isoformat() >= minimum:
        label = candidate.isoformat()
        sessions, unknown = expected_dates(label, label, calendar)
        if unknown:
            return None, "PREVIOUS_SESSION_CALENDAR_UNKNOWN"
        if sessions:
            return label, None
        candidate -= timedelta(days=1)
    return None, "PREVIOUS_SESSION_OUTSIDE_VERIFIED_CALENDAR"


def cluster_volume_flags(flagged_dates, daily_rows, daily_snapshots, calendar, *, policy=None):
    """Group using exchange sessions, then measure each cluster's first-day move.

    Uses only the preceding session's frozen ATR, not a value recomputed with
    the candidate date. Neither grouping nor the volume scan is a causal input.
    """
    policy = POLICY if policy is None else policy
    if policy != POLICY:
        raise ValueError("VOLUME_CLUSTER_POLICY_MISMATCH")
    flags = list(flagged_dates)
    for item in flags:
        _day(item)
    if any(a >= b for a, b in zip(flags, flags[1:])):
        raise ValueError("FLAGS_NOT_STRICTLY_INCREASING")
    base = dict(schema=VERSION, mode="SHADOW_ONLY", affects_analysis=False, affects_execution=False,
                temporal_basis="RETROSPECTIVE", use_in_causal_features=False,
                classification="CANDIDATE_FOR_REVIEW_NOT_ADJUSTMENT", flagged_count=len(flags),
                policy=policy, clusters=[], unclustered_flags=[])
    if not flags:
        return dict(base, status="EVALUATED", cluster_count=0, movement_status_counts={})
    if calendar.get("market") != "BYMA" or calendar.get("weekmask") != [0, 1, 2, 3, 4]:
        return dict(base, status="NOT_EVALUABLE", reason="UNSUPPORTED_CALENDAR", cluster_count=0,
                    unclustered_flags=flags, movement_status_counts={})
    sessions, unknown = expected_dates(flags[0], flags[-1], calendar)
    if unknown or not all(item in sessions for item in flags):
        return dict(base, status="NOT_EVALUABLE", reason="FLAG_INTERVAL_CALENDAR_NOT_VERIFIED",
                    cluster_count=0, unclustered_flags=flags, movement_status_counts={})
    indices = {day: i for i, day in enumerate(sessions)}
    groups = [[flags[0]]]
    for day in flags[1:]:
        if indices[day] - indices[groups[-1][-1]] <= policy["maximum_consecutive_flag_session_distance"]:
            groups[-1].append(day)
        else:
            groups.append([day])
    daily = {}
    for row in daily_rows:
        label = row["session_date"]
        _day(label)
        if label in daily:
            raise ValueError("DUPLICATE_DAILY_SESSION")
        daily[label] = row
    snapshots = {}
    for row in daily_snapshots:
        if row["time"] in snapshots:
            raise ValueError("DUPLICATE_FEATURE_TIME")
        snapshots[row["time"]] = row
    output = []
    for group in groups:
        first = group[0]
        previous, reason = _previous_session(first, calendar)
        record = dict(start=first, end=group[-1], flagged_dates=group, flagged_count=len(group),
                      previous_session=previous, movement_status="NOT_EVALUABLE", movement_reason=reason,
                      close_first=None, close_previous=None, price_move=None, atr_previous=None,
                      signed_move_atr=None, atr_session=previous, atr_snapshot_time=None,
                      atr_snapshot_known_at=None, atr_period=14, atr_method="WILDER",
                      uses_candidate_session_atr=False, use_in_causal_features=False)
        if reason is None:
            current_row, prior_row = daily.get(first), daily.get(previous)
            if current_row is None or prior_row is None:
                record["movement_reason"] = "MISSING_FIRST_OR_PREVIOUS_SESSION_BAR"
            else:
                current_close, prior_close = _positive(current_row.get("close")), _positive(prior_row.get("close"))
                snapshot = snapshots.get(prior_row["bar_start"])
                atr = snapshot.get("indicators", {}).get("atr", {}) if snapshot else {}
                value = _positive(atr.get("value"))
                record.update(close_first=current_close, close_previous=prior_close,
                              atr_snapshot_time=snapshot["time"] if snapshot else None,
                              atr_snapshot_known_at=snapshot.get("known_at") if snapshot else None)
                if current_close is None or prior_close is None:
                    record["movement_reason"] = "MISSING_OR_INVALID_CLOSE"
                elif atr.get("status") != "AVAILABLE" or value is None:
                    record["movement_reason"] = "PREVIOUS_SESSION_ATR_UNAVAILABLE_OR_NONPOSITIVE"
                    record["atr_source_reason"] = atr.get("reason") if snapshot else "MISSING_SNAPSHOT"
                else:
                    with localcontext() as context:
                        context.prec = policy["decimal_precision"]
                        movement = current_close - prior_close
                        normalized = movement / value
                    record.update(movement_status="EVALUATED", movement_reason=None,
                                  price_move=movement, atr_previous=value, signed_move_atr=normalized)
        output.append(record)
    base["clusters"] = output
    return dict(base, status="EVALUATED", cluster_count=len(output),
                movement_status_counts=dict(Counter(row["movement_status"] for row in output)))

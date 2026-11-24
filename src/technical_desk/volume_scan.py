"""Retrospective volume-level diagnostics, isolated from causal features."""
from __future__ import annotations

from collections import Counter
from datetime import date
from decimal import Decimal, InvalidOperation, localcontext
from statistics import median


SCAN_VERSION = "technical-desk-volume-scan-v1"
WINDOW_SESSIONS = 10
JUMP_THRESHOLD = Decimal("3")
DECIMAL_PRECISION = 34


def _volume(value):
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("INVALID_VOLUME")
    try:
        result = Decimal(str(value))
    except InvalidOperation:
        raise ValueError("INVALID_VOLUME") from None
    if not result.is_finite() or result < 0:
        raise ValueError("INVALID_VOLUME")
    return result


def scan_volume(rows, window=WINDOW_SESSIONS, threshold=JUMP_THRESHOLD):
    """Compare ten preceding sessions with ten sessions starting at each date.

    Parameters are fixed for v1: changing them requires a new policy version.
    Marked dates are candidates for review, never inferred adjustment events.
    """
    if type(window) is not int or window != WINDOW_SESSIONS:
        raise ValueError("VOLUME_SCAN_POLICY_MISMATCH")
    try:
        factor = Decimal(str(threshold))
    except InvalidOperation:
        raise ValueError("VOLUME_SCAN_POLICY_MISMATCH") from None
    if not factor.is_finite() or factor != JUMP_THRESHOLD:
        raise ValueError("VOLUME_SCAN_POLICY_MISMATCH")
    dates = []
    for row in rows:
        value = row.get("session_date")
        if not isinstance(value, str):
            raise ValueError("INVALID_SESSION_DATE")
        try:
            parsed = date.fromisoformat(value)
        except ValueError:
            raise ValueError("INVALID_SESSION_DATE") from None
        if parsed.isoformat() != value:
            raise ValueError("INVALID_SESSION_DATE")
        dates.append(value)
    if any(a >= b for a, b in zip(dates, dates[1:])):
        raise ValueError("SESSION_DATES_NOT_STRICTLY_INCREASING")
    values = [_volume(row.get("volume")) for row in rows]
    checks = []
    for index, day in enumerate(dates):
        pre = values[max(0, index-window):index]
        post = values[index:index+window]
        record = dict(session_date=day, pre_sessions=len(pre), post_sessions=len(post),
                      observed_at=dates[min(index+window, len(dates))-1],
                      pre_median=None, post_median=None, post_pre_ratio=None,
                      symmetric_factor=None, level_jump=None, flag=None,
                      use_in_causal_features=False)
        if len(pre) != window or len(post) != window:
            record["status"] = "INSUFFICIENT_WINDOW"
        elif any(value is None for value in pre + post):
            record["status"] = "MISSING_VOLUME"
        else:
            with localcontext() as context:
                context.prec = DECIMAL_PRECISION
                before, after = median(pre), median(post)
            record.update(pre_median=before, post_median=after)
            if before <= 0 or after <= 0:
                record["status"] = "ZERO_MEDIAN_NOT_EVALUABLE"
            else:
                with localcontext() as context:
                    context.prec = DECIMAL_PRECISION
                    ratio = after / before
                    symmetric = max(ratio, before / after)
                    # Cross-multiply to include 1/3 without division rounding.
                    marked = after >= before * factor or after * factor <= before
                record.update(status="EVALUATED", post_pre_ratio=ratio,
                              symmetric_factor=symmetric, level_jump=marked,
                              flag="CANDIDATE_FOR_REVIEW" if marked else None)
        checks.append(record)
    return dict(schema=SCAN_VERSION, mode="SHADOW_ONLY", volume_basis="RELATIVE_UNVERIFIED",
                window_sessions_each_side=window, jump_factor_threshold=factor,
                decimal_precision=DECIMAL_PRECISION,
                pre_window="10_SESSIONS_STRICTLY_BEFORE_CANDIDATE_DATE",
                post_window="10_SESSIONS_STARTING_AT_CANDIDATE_DATE",
                temporal_basis="RETROSPECTIVE", use_in_causal_features=False,
                affects_analysis=False, affects_execution=False,
                action="REVIEW_CANDIDATES_ONLY_NO_AUTOMATIC_RESCALE",
                rows_scanned=len(checks), status_counts=dict(Counter(c["status"] for c in checks)),
                flagged_dates=[c["session_date"] for c in checks if c["level_jump"] is True],
                flagged_count=sum(c["level_jump"] is True for c in checks), checks=checks)

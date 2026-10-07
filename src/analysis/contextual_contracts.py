"""E1 contracts. Unknown provenance stays unknown; economic policy is shadow only."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from hashlib import sha256
import math
from typing import Any

CONTRACT_VERSION = "contextual-e1-v1"
POLICY_VERSION = "authority-shadow-v1"
IDENTITY_FIELDS = ("ticker", "asset_type", "currency", "venue", "interval")


def finite_number(value: Any, low: float | None = None, high: float | None = None) -> bool:
    if value is None or isinstance(value, bool):
        return False
    try:
        number = float(value)
    except (ValueError, TypeError, OverflowError):
        return False
    return math.isfinite(number) and (low is None or number >= low) and (high is None or number <= high)


def frame_quality(frame, *, cutoff=None) -> dict:
    """Inspect without repairing data or claiming a calendar/close we do not know."""
    import numpy as np
    import pandas as pd

    price_reasons, volume_reasons, provenance_reasons = [], [], []
    numeric = frame.reindex(columns=["Open", "High", "Low", "Close"]).apply(
        pd.to_numeric,
        errors="coerce",
    )
    if frame.empty or not np.isfinite(numeric.to_numpy()).all() or (numeric <= 0).any().any():
        price_reasons.append("PRICE_MISSING_NONFINITE_OR_NONPOSITIVE")
    if ((numeric.High < numeric[["Open", "Close", "Low"]].max(axis=1)) |
            (numeric.Low > numeric[["Open", "Close", "High"]].min(axis=1))).any():
        price_reasons.append("IMPOSSIBLE_OHLC")
    if frame.index.has_duplicates:
        price_reasons.append("DUPLICATE_BAR")
    if not frame.index.is_monotonic_increasing:
        price_reasons.append("UNSORTED_BARS")
    volume = pd.to_numeric(frame.get("Volume", pd.Series(index=frame.index, dtype=float)), errors="coerce")
    recent = volume.tail(20)
    valid = np.isfinite(recent) & (recent > 0)
    if len(recent) < 20:
        volume_reasons.append("VOLUME_LOOKBACK_INSUFFICIENT")
    if recent.isna().any():
        volume_reasons.append("VOLUME_MISSING")
    if ((~np.isfinite(recent)) & recent.notna()).any():
        volume_reasons.append("VOLUME_NONFINITE")
    if (recent == 0).any():
        volume_reasons.append("VOLUME_ZERO")
    if (recent < 0).any():
        volume_reasons.append("VOLUME_NEGATIVE")
    identity = dict(frame.attrs.get("series_identity", {}))
    for name in (*IDENTITY_FIELDS, "volume_unit", "calendar", "adjustment_policy"):
        if not identity.get(name):
            provenance_reasons.append(f"UNKNOWN_{name.upper()}")
    if identity.get("asset_type") == "CEDEAR" and not identity.get("depositary_ratio"):
        provenance_reasons.append("UNKNOWN_DEPOSITARY_RATIO")
    if identity.get("volume_unit") not in (None, "units", "shares", "contracts"):
        volume_reasons.append("INCOMPATIBLE_VOLUME_UNIT")
    # Stored timestamps are not evidence that the bar was closed or available.
    for column in ("BarStart", "BarEnd", "AvailableAt", "IsClosed"):
        if column not in frame or frame[column].isna().any():
            provenance_reasons.append(f"UNKNOWN_{column.upper()}")
    if "IsClosed" in frame and (frame.IsClosed == False).any():  # noqa: E712
        provenance_reasons.append("PARTIAL_SESSION")
    cutoff_ts = pd.Timestamp(cutoff) if cutoff is not None else None
    if cutoff_ts is None:
        provenance_reasons.append("UNKNOWN_CUTOFF")
    elif cutoff_ts.tzinfo is None:
        provenance_reasons.append("NAIVE_CUTOFF")
    else:
        for column in ("AvailableAt", "BarEnd"):
            if column in frame:
                dates = pd.to_datetime(frame[column], utc=True, errors="coerce")
                if dates.isna().any():
                    provenance_reasons.append(f"INVALID_{column.upper()}")
                if (dates > cutoff_ts).any():
                    provenance_reasons.append(f"AFTER_CUTOFF_{column.upper()}")
        if isinstance(frame.index, pd.DatetimeIndex) and frame.index.tz is not None and (frame.index > cutoff_ts).any():
            provenance_reasons.append("AFTER_CUTOFF_BAR")
    # Exact missing sessions and staleness require a versioned exchange calendar.
    if not frame.attrs.get("calendar_validation"):
        provenance_reasons.append("GAPS_AND_FRESHNESS_NOT_VERIFIED")
    last = frame.iloc[-1] if len(frame) else None
    def _last_timestamp(column: str) -> str | None:
        if last is None or column not in frame or pd.isna(last[column]):
            return None
        value = pd.Timestamp(last[column])
        return value.isoformat()
    return {
        "version": CONTRACT_VERSION,
        "series_digest": sha256(
            (
                frame.to_json(date_format="iso", orient="split")
                + str(sorted(identity.items()))
            ).encode()
        ).hexdigest(),
        "provider_symbols": list(frame.attrs.get("provider_symbols", [])),
        "selection_policy": frame.attrs.get("selection_policy"),
        "duplicate_rows_resolved": frame.attrs.get("duplicate_rows_resolved", 0),
        "price_status": "INVALID" if price_reasons else "VALID",
        "volume_status": "PARTIAL" if volume_reasons else "VALID",
        "provenance_status": "PARTIAL" if provenance_reasons else "VALID",
        "price_reasons": price_reasons, "volume_reasons": volume_reasons,
        "provenance_reasons": provenance_reasons,
        "volume_quality_20": float(valid.mean()) if len(recent) else None,
        "series_identity": identity,
        "cutoff": cutoff_ts.isoformat() if cutoff_ts is not None else None,
        "bar_count": len(frame),
        "last_bar": str(frame.index[-1]) if len(frame) else None,
        "last_candle_timestamp": str(frame.index[-1]) if len(frame) else None,
        "last_bar_start": _last_timestamp("BarStart"),
        "last_bar_end": _last_timestamp("BarEnd"),
        "last_available_at": _last_timestamp("AvailableAt"),
        "last_scraped_at": _last_timestamp("RetrievedAt"),
        "last_is_closed": (bool(last["IsClosed"])
                           if last is not None and "IsClosed" in frame and pd.notna(last["IsClosed"])
                           else None),
        "volume_ratio_definition": "legacy_current_volume_over_inclusive_sma20",
        "missing_features": ["sma_200"] if len(frame) < 200 else [],
        "volume_provenance": dict(
            frame.attrs.get("volume_source_counts")
            or frame.attrs.get("candle_source_counts", {})
        ),
    }


@dataclass(frozen=True)
class RebalanceAuthorization:
    authorization_id: str
    owner_chat_id: int
    run_id: str
    ticker: str
    reason: str
    max_target_weight: float
    expires_at: datetime
    authority: str = "independent_rebalance_policy"


def evaluate_authority(
    *,
    signal_action: str,
    current_weight: float,
    target_weight: float,
    data_quality: dict,
    asset_view: str = "UNKNOWN",
    action_reason: str | None = None,
    authorization: RebalanceAuthorization | None = None,
    owner_chat_id: int | None = None,
    run_id: str | None = None,
    ticker: str = "",
    cutoff: datetime | None = None,
) -> dict:
    """Shared pure policy. No weights, signal, plan or authorizations are mutated."""
    reasons = []
    intent = "NONE"
    if not all(finite_number(v, 0, 1) for v in (current_weight, target_weight)):
        reasons.append("INVALID_WEIGHT")
    else:
        intent = ("INCREASE" if target_weight > current_weight else
                  "EXIT" if target_weight == 0 < current_weight else
                  "DECREASE" if target_weight < current_weight else "KEEP")
    if intent == "INCREASE":
        if any(data_quality.get(k) != "VALID" for k in ("price_status", "volume_status", "provenance_status")):
            reasons.append("CRITICAL_DATA_UNVERIFIED")
        if signal_action == "HOLD":
            a = authorization
            authorized = bool(a and a.authority == "independent_rebalance_policy" and
                a.authorization_id.strip() and a.reason.strip() and
                finite_number(owner_chat_id, 1) and a.owner_chat_id == owner_chat_id and
                run_id and a.run_id == run_id and a.ticker == ticker and
                finite_number(a.max_target_weight, 0, 1) and target_weight <= a.max_target_weight and
                cutoff and cutoff.tzinfo and a.expires_at.tzinfo and cutoff <= a.expires_at)
            if not authorized:
                reasons.append("HOLD_INCREASE_REQUIRES_INDEPENDENT_REBALANCE")
            else:
                action_reason = "REBALANCE"
        elif signal_action not in ("BUY", "ACCUMULATE"):
            reasons.append("SIGNAL_NOT_ELIGIBLE_FOR_INCREASE")
        else:
            action_reason = action_reason or "SIGNAL_EDGE"
    return {
        "version": POLICY_VERSION, "mode": "SHADOW_ONLY", "affects_execution": False,
        "asset_view": asset_view, "signal_action": signal_action,
        "portfolio_intent": intent, "action_reason": action_reason,
        "execution_status": "SHADOW_ONLY", "eligibility": "BLOCKED" if reasons else "ALLOWED",
        "reason_codes": reasons,
        "authorization": ({**asdict(authorization), "expires_at": authorization.expires_at.isoformat()}
                          if authorization else None),
    }

"""Observed intraday range features for shadow-only evaluation."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, time
from math import isfinite
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo


ART_TZ = ZoneInfo("America/Argentina/Buenos_Aires")
EXPECTED_SLOTS = ("POST_OPEN", "MIDDAY", "PRE_CLOSE", "POST_CLOSE")


@dataclass(frozen=True)
class IntradayRangeShadow:
    first_price: float
    observed_high: float
    observed_low: float
    close_price: float
    first_ts: datetime
    high_ts: datetime
    low_ts: datetime
    close_ts: datetime
    previous_close: float | None
    sample_count: int
    observed_slots: tuple[str, ...]
    slot_coverage: float
    range_pct_vs_previous_close: float | None
    range_pct_from_low: float
    close_location: float | None
    gap_from_previous_close: float | None
    upside_excursion: float
    downside_excursion: float
    state: str
    quality_status: str
    eligible_for_evaluation: bool
    source: str = "market_prices_observed_shadow"

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        for key in ("first_ts", "high_ts", "low_ts", "close_ts"):
            payload[key] = payload[key].isoformat()
        payload["observed_slots"] = list(self.observed_slots)
        return payload


def build_intraday_range_shadow(
    samples: Sequence[Mapping[str, Any]],
    *,
    previous_close: float | None,
) -> IntradayRangeShadow | None:
    clean: list[tuple[datetime, float]] = []
    seen_ts: set[datetime] = set()
    for sample in samples:
        ts = _coerce_datetime(sample.get("ts"))
        price = _positive_float(sample.get("last_price"))
        if ts is None or price is None or ts in seen_ts:
            continue
        seen_ts.add(ts)
        clean.append((ts, price))
    if not clean:
        return None

    clean.sort(key=lambda item: item[0])
    first_ts, first_price = clean[0]
    close_ts, close_price = clean[-1]
    high_ts, observed_high = max(clean, key=lambda item: (item[1], -item[0].timestamp()))
    low_ts, observed_low = min(clean, key=lambda item: (item[1], item[0].timestamp()))
    prev = _positive_float(previous_close)
    observed_slots = tuple(
        slot for slot in EXPECTED_SLOTS if any(_slot_for(ts) == slot for ts, _ in clean)
    )
    slot_coverage = len(observed_slots) / len(EXPECTED_SLOTS)

    spread = observed_high - observed_low
    close_location = (close_price - observed_low) / spread if spread > 0 else None
    range_pct_from_low = spread / observed_low if observed_low > 0 else 0.0
    range_pct_vs_prev = spread / prev if prev else None
    gap = first_price / prev - 1.0 if prev else None
    upside = observed_high / first_price - 1.0
    downside = observed_low / first_price - 1.0

    has_outlier = bool(
        prev and any(abs(price / prev - 1.0) > 0.90 for _, price in clean)
    )
    if has_outlier:
        quality = "OUTLIER"
    elif prev is None:
        quality = "MISSING_PREVIOUS_CLOSE"
    elif slot_coverage < 1.0:
        quality = "PARTIAL_SLOTS"
    else:
        quality = "COMPLETE"

    if spread <= 0:
        state = "FLAT"
    elif close_location is not None and close_location >= 0.80:
        state = "CLOSE_NEAR_HIGH"
    elif close_location is not None and close_location <= 0.20:
        state = "CLOSE_NEAR_LOW"
    else:
        state = "MID_RANGE"

    return IntradayRangeShadow(
        first_price=first_price,
        observed_high=observed_high,
        observed_low=observed_low,
        close_price=close_price,
        first_ts=first_ts,
        high_ts=high_ts,
        low_ts=low_ts,
        close_ts=close_ts,
        previous_close=prev,
        sample_count=len(clean),
        observed_slots=observed_slots,
        slot_coverage=slot_coverage,
        range_pct_vs_previous_close=range_pct_vs_prev,
        range_pct_from_low=range_pct_from_low,
        close_location=close_location,
        gap_from_previous_close=gap,
        upside_excursion=upside,
        downside_excursion=downside,
        state=state,
        quality_status=quality,
        eligible_for_evaluation=quality == "COMPLETE",
    )


def _slot_for(value: datetime) -> str | None:
    local_time = value.astimezone(ART_TZ).time().replace(tzinfo=None)
    if time(10, 20) <= local_time <= time(11, 10):
        return "POST_OPEN"
    if time(11, 30) <= local_time <= time(12, 30):
        return "MIDDAY"
    if time(16, 20) <= local_time < time(17, 0):
        return "PRE_CLOSE"
    if time(17, 0) <= local_time <= time(17, 30):
        return "POST_CLOSE"
    return None


def _coerce_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=ART_TZ)
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=ART_TZ)
    except (TypeError, ValueError):
        return None


def _positive_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if isfinite(parsed) and parsed > 0 else None

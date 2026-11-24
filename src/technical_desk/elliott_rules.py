"""Deterministic candidate checks, without count selection, I/O or authority.

An ordinary impulse has six endpoints for five waves. Compatibility means only
that the explicitly listed rules hold; it is not a forecast or an Elliott label
inferred from the market. Price arithmetic is exact Decimal arithmetic.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Sequence


RULE_VERSION = "elliott-deterministic-rules-v1"
TOLERANCE_TICKS = 1
SCALES = ("D", "W")
RULES_COMPATIBLE = "RULES_COMPATIBLE"
REJECTED = "REJECTED"
INCOMPLETE = "INCOMPLETE"
NOT_EVALUABLE = "NOT_EVALUABLE"


def _decimal(value: Any) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (Decimal, str, int)):
        raise ValueError("EXACT_DECIMAL_REQUIRED")
    try:
        result = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError("INVALID_DECIMAL") from exc
    if not result.is_finite() or result <= 0:
        raise ValueError("POSITIVE_FINITE_PRICE_REQUIRED")
    return result


def _time(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("ISO_TIME_REQUIRED")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("ISO_TIME_REQUIRED") from exc
    # ISO session dates are sufficient for daily/weekly synthetic fixtures.
    if result.tzinfo is None:
        if len(value) != 10:
            raise ValueError("TIMEZONE_REQUIRED_FOR_TIMESTAMP")
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def _rule(name: str, status: str, reason: str, **details: Any) -> dict:
    return dict(rule_id=name, status=status, reason=reason, details=details)


def _result(pattern: str, scale: str, rules: list[dict], tick: Any) -> dict:
    statuses = {rule["status"] for rule in rules}
    status = next((state for state in (NOT_EVALUABLE, REJECTED, INCOMPLETE)
                   if state in statuses), RULES_COMPATIBLE)
    return dict(version=RULE_VERSION, pattern=pattern, scale=scale,
                tick_size=str(tick), tolerance_ticks=TOLERANCE_TICKS,
                status=status, rules=rules, mode="SHADOW_ONLY",
                affects_analysis=False, affects_execution=False,
                tolerance_policy={
                    "pivot_bar_and_segment_extremes": "ABSOLUTE_ONE_TICK",
                    "wave3_shortest": "DIFFERENCE_GREATER_THAN_ONE_TICK",
                    "wave2_origin_and_wave4_overlap": "EXACT_NO_RELAXATION",
                })


def _prepare(pivots: Sequence[Mapping], bars: Sequence[Mapping], *,
             tick_size: Any, scale: str, tolerance_ticks: int,
             required: int) -> tuple[list[dict], list[dict], Decimal, list[dict]]:
    tick = _decimal(tick_size)
    if scale not in SCALES:
        raise ValueError("UNSUPPORTED_SCALE")
    if type(tolerance_ticks) is not int or tolerance_ticks != TOLERANCE_TICKS:
        raise ValueError("FIXED_ONE_TICK_TOLERANCE_REQUIRED")
    if len(pivots) > required:
        raise ValueError("EXPLICIT_CANDIDATE_HAS_TOO_MANY_PIVOTS")
    if not bars:
        raise ValueError("BARS_MISSING")
    times = [_time(bar["time"]) for bar in bars]
    if any(a >= b for a, b in zip(times, times[1:])):
        raise ValueError("BAR_CHRONOLOGY_INVALID")
    knowledge_cutoff = _time(bars[-1].get("closed_at", bars[-1]["time"]))
    prepared = []
    for pivot in pivots:
        index = pivot["index"]
        if type(index) is not int or not 0 <= index < len(bars):
            raise ValueError("PIVOT_INDEX_OUT_OF_RANGE")
        if pivot["kind"] not in ("HIGH", "LOW"):
            raise ValueError("UNKNOWN_PIVOT_KIND")
        if not isinstance(pivot["id"], str) or not pivot["id"]:
            raise ValueError("PIVOT_ID_MISSING")
        if _time(pivot["pivot_time"]) != times[index]:
            raise ValueError("PIVOT_TIME_BAR_MISMATCH")
        item = dict(pivot, price=_decimal(pivot["price"]))
        if pivot.get("status") == "CONFIRMED":
            confirmed = _time(pivot["confirmed_at"])
            if confirmed <= times[index] or confirmed > knowledge_cutoff:
                raise ValueError("CONFIRMATION_NOT_CAUSALLY_AVAILABLE")
        elif pivot.get("status") not in ("CANDIDATE", "OPEN"):
            raise ValueError("UNKNOWN_CONFIRMATION_STATUS")
        prepared.append(item)
    if len({pivot["id"] for pivot in prepared}) != len(prepared):
        raise ValueError("DUPLICATE_PIVOT_ID")
    # Validate OHLC only within the candidate span. Later bars can establish a
    # confirmation timestamp, but never supply an extreme for a wave segment.
    first = min((p["index"] for p in prepared), default=0)
    last = max((p["index"] for p in prepared), default=-1)
    normalized = []
    for index, bar in enumerate(bars):
        if first <= index <= last:
            values = {field: _decimal(bar[field]) for field in ("open", "high", "low", "close")}
            if not (values["low"] <= min(values["open"], values["close"]) <=
                    max(values["open"], values["close"]) <= values["high"]):
                raise ValueError("INVALID_OHLC")
            normalized.append(values)
        else:
            normalized.append({})
    rules = [_rule("endpoint_count", RULES_COMPATIBLE if len(prepared) == required else INCOMPLETE,
                   "COMPLETE_ENDPOINTS" if len(prepared) == required else "MORE_CONFIRMED_ENDPOINTS_REQUIRED",
                   actual=len(prepared), required=required)]
    confirmed = all(p["status"] == "CONFIRMED" for p in prepared)
    rules.append(_rule("confirmed_endpoints", RULES_COMPATIBLE if confirmed else INCOMPLETE,
                       "ALL_ENDPOINTS_CONFIRMED" if confirmed else "OPEN_ENDPOINT_IS_NOT_A_COMPLETED_WAVE"))
    return prepared, normalized, tick, rules


def _geometry(pivots: list[dict], bars: list[dict], tick: Decimal) -> list[dict]:
    if len(pivots) < 2:
        return [_rule("chronology_alternation", INCOMPLETE, "TWO_ENDPOINTS_REQUIRED")]
    ordered = all(a["index"] < b["index"] for a, b in zip(pivots, pivots[1:]))
    alternate = all(a["kind"] != b["kind"] for a, b in zip(pivots, pivots[1:]))
    directional = all((b["price"] > a["price"] if a["kind"] == "LOW"
                       else b["price"] < a["price"]) for a, b in zip(pivots, pivots[1:]))
    valid = ordered and alternate and directional
    rules = [_rule("chronology_alternation", RULES_COMPATIBLE if valid else REJECTED,
                   "ORDERED_ALTERNATING_DIRECTIONAL_ENDPOINTS" if valid else "INVALID_ENDPOINT_GEOMETRY",
                   chronological=ordered, alternating=alternate, directional=directional)]
    if not valid:
        return rules
    for number, pivot in enumerate(pivots):
        actual = bars[pivot["index"]][pivot["kind"].lower()]
        delta = abs(actual - pivot["price"])
        rules.append(_rule(f"endpoint_{number}_bar", RULES_COMPATIBLE if delta <= tick else REJECTED,
                           "MATCHES_BAR_EXTREME_WITHIN_ONE_TICK" if delta <= tick else "PIVOT_NOT_BAR_EXTREME",
                           delta_ticks=str(delta / tick)))
    for number, (left, right) in enumerate(zip(pivots, pivots[1:]), start=1):
        span = bars[left["index"]:right["index"] + 1]
        span_low, span_high = min(b["low"] for b in span), max(b["high"] for b in span)
        bound_low, bound_high = sorted((left["price"], right["price"]))
        fits = span_low >= bound_low - tick and span_high <= bound_high + tick
        rules.append(_rule(f"wave_{number}_segment_extremes", RULES_COMPATIBLE if fits else REJECTED,
                           "ALL_SEGMENT_BARS_WITHIN_ENDPOINTS" if fits else "INTRASEGMENT_EXTREME_OUTSIDE_ENDPOINTS",
                           from_index=left["index"], to_index=right["index"],
                           bar_count=len(span), observed_low=str(span_low), observed_high=str(span_high)))
    return rules


def validate_impulse(pivots: Sequence[Mapping], bars: Sequence[Mapping], *,
                     tick_size: Any, scale: str = "D", tolerance_ticks: int = 1,
                     pattern: str = "ORDINARY_IMPULSE") -> dict:
    """Check one caller-selected ordinary impulse; incomplete counts stay open."""
    if pattern != "ORDINARY_IMPULSE":
        return _result(pattern, scale, [_rule("supported_pattern", NOT_EVALUABLE,
                                             "UNSUPPORTED_PATTERN")], tick_size)
    try:
        points, prices, tick, rules = _prepare(pivots, bars, tick_size=tick_size,
            scale=scale, tolerance_ticks=tolerance_ticks, required=6)
    except (ValueError, TypeError, KeyError) as exc:
        return _result(pattern, scale, [_rule("input_contract", NOT_EVALUABLE, str(exc))], tick_size)
    if any(p["status"] != "CONFIRMED" for p in points):
        rules.append(_rule("completed_wave_rules", INCOMPLETE, "CONFIRMED_ENDPOINTS_REQUIRED"))
        return _result(pattern, scale, rules, tick)
    rules.extend(_geometry(points, prices, tick))
    direction = Decimal(1) if points and points[0]["kind"] == "LOW" else Decimal(-1)
    if len(points) >= 3:
        wave2_bars = prices[points[1]["index"]:points[2]["index"] + 1]
        actual_extreme = (min(bar["low"] for bar in wave2_bars) if direction > 0 else
                          max(bar["high"] for bar in wave2_bars)) if wave2_bars else points[2]["price"]
        origin_not_breached = (direction * (points[2]["price"] - points[0]["price"]) > 0 and
                               direction * (actual_extreme - points[0]["price"]) > 0)
        rules.append(_rule("wave2_less_than_100_percent", RULES_COMPATIBLE if origin_not_breached else REJECTED,
                           "ORIGIN_PRESERVED" if origin_not_breached else "WAVE2_RETRACES_100_PERCENT_OR_MORE",
                           observed_extreme=str(actual_extreme)))
    else:
        rules.append(_rule("wave2_less_than_100_percent", INCOMPLETE, "WAVE2_NOT_COMPLETE"))
    if len(points) >= 5:
        wave4_bars = prices[points[3]["index"]:points[4]["index"] + 1]
        actual_extreme = (min(bar["low"] for bar in wave4_bars) if direction > 0 else
                          max(bar["high"] for bar in wave4_bars)) if wave4_bars else points[4]["price"]
        no_overlap = (direction * (points[4]["price"] - points[1]["price"]) > 0 and
                      direction * (actual_extreme - points[1]["price"]) > 0)
        rules.append(_rule("wave4_no_wave1_overlap", RULES_COMPATIBLE if no_overlap else REJECTED,
                           "WAVE1_TERRITORY_PRESERVED" if no_overlap else "WAVE4_TOUCHES_OR_OVERLAPS_WAVE1",
                           observed_extreme=str(actual_extreme)))
    else:
        rules.append(_rule("wave4_no_wave1_overlap", INCOMPLETE, "WAVE4_NOT_COMPLETE"))
    if len(points) == 6:
        lengths = [abs(points[end]["price"] - points[start]["price"])
                   for start, end in ((0, 1), (2, 3), (4, 5))]
        shortest = lengths[1] + tick < min(lengths[0], lengths[2])
        rules.append(_rule("wave3_not_shortest", REJECTED if shortest else RULES_COMPATIBLE,
                           "WAVE3_STRICTLY_SHORTEST_BEYOND_ONE_TICK" if shortest else "WAVE3_NOT_UNIQUELY_SHORTEST",
                           wave1=str(lengths[0]), wave3=str(lengths[1]), wave5=str(lengths[2])))
    else:
        rules.append(_rule("wave3_not_shortest", INCOMPLETE, "ALL_THREE_MOTIVE_WAVES_MUST_BE_COMPLETE"))
    return _result(pattern, scale, rules, tick)


def validate_abc(pivots: Sequence[Mapping], bars: Sequence[Mapping], *,
                 tick_size: Any, scale: str = "D", tolerance_ticks: int = 1) -> dict:
    """Only chronology, alternation and bar-consistent geometry, no subtype claim."""
    try:
        points, prices, tick, rules = _prepare(pivots, bars, tick_size=tick_size,
            scale=scale, tolerance_ticks=tolerance_ticks, required=4)
    except (ValueError, TypeError, KeyError) as exc:
        return _result("ABC_BASIC", scale, [_rule("input_contract", NOT_EVALUABLE, str(exc))], tick_size)
    if any(p["status"] != "CONFIRMED" for p in points):
        rules.append(_rule("basic_geometry", INCOMPLETE, "CONFIRMED_ENDPOINTS_REQUIRED"))
    else:
        rules.extend(_geometry(points, prices, tick))
    return _result("ABC_BASIC", scale, rules, tick)

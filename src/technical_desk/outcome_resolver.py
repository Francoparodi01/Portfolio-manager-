"""Deterministic, offline resolver of desk scenario outcomes (eval protocol rules, no model I/O).

Implements the evaluation rules of docs/technical-analysis-desk-design.md:
- the first evaluable session starts AFTER the ledger/publication origin; horizons count sessions of the
  instrument's calendar, never calendar days;
- a missing bar is MISSING_BAR: the expiry is never moved to the next observation;
- a bar that touches confirmation/target and invalidation in an order OHLC cannot reveal is
  AMBIGUOUS_SAME_BAR and is never resolved in favour of the thesis;
- targets count only after the confirmation; a target touched earlier is recorded, not scored;
- no intrabar touch is inferred from CLOSE when HIGH/LOW are absent;
- a split/adjustment event inside the window makes the case NOT_COMPARABLE.

Inputs are plain data. Nothing here reads a database, the clock or the network.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Iterable, Optional, Sequence
from zoneinfo import ZoneInfo

RESOLVER_VERSION = "technical-desk-outcome-resolver-v1"
TARGETS_REQUIRE_PRIOR_CONFIRMATION = True      # frozen protocol choice
CONDITIONAL_SCENARIOS_EXPIRE_AT_HORIZON = True  # an unconfirmed scenario expires when its horizon ends

STATUSES = (
    "TARGET_REACHED", "INVALIDATED_BEFORE_CONFIRMATION", "INVALIDATED_AFTER_CONFIRMATION",
    "CONFIRMED_TARGET_NOT_REACHED", "CONFIRMED_NO_TARGETS", "EXPIRED_UNCONFIRMED",
    "AMBIGUOUS_SAME_BAR", "MISSING_BAR", "NOT_COMPARABLE", "NOT_EVALUABLE", "IMMATURE",
)
_OPEN, _INTRABAR, _CLOSE = 0, 1, 2
_ORDER_NAME = {_OPEN: "OPEN", _INTRABAR: "INTRABAR", _CLOSE: "CLOSE"}


# ---------------------------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class Bar:
    """One closed daily session. high/low may be unknown (None); open and close are required."""
    session_date: date
    open: Decimal
    high: Optional[Decimal]
    low: Optional[Decimal]
    close: Decimal

    def __post_init__(self):
        for name in ("open", "close"):
            value = getattr(self, name)
            if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
                raise ValueError(f"BAR_{name.upper()}_MUST_BE_POSITIVE_DECIMAL")
        for name in ("high", "low"):
            value = getattr(self, name)
            if value is not None and (not isinstance(value, Decimal) or not value.is_finite() or value <= 0):
                raise ValueError(f"BAR_{name.upper()}_MUST_BE_POSITIVE_DECIMAL_OR_NONE")
        if self.high is not None and self.high < max(self.open, self.close):
            raise ValueError("INCONSISTENT_OHLC")
        if self.low is not None and self.low > min(self.open, self.close):
            raise ValueError("INCONSISTENT_OHLC")
        if self.high is not None and self.low is not None and self.high < self.low:
            raise ValueError("INCONSISTENT_OHLC")


@dataclass(frozen=True)
class EvalCondition:
    kind: str            # CONFIRMATION | INVALIDATION
    field: str           # CLOSE | HIGH | LOW
    operator: str        # GT | GE | LT | LE | EQ
    level: Decimal
    persistence_bars: int


@dataclass(frozen=True)
class EvalScenario:
    direction: str       # BULLISH | BEARISH | NEUTRAL
    timeframe: str       # D | W
    conditions: tuple[EvalCondition, ...]
    targets: tuple[Decimal, ...]
    horizon_sessions: int


def bars_from_rows(rows: Iterable[dict]) -> list[Bar]:
    """Packet-style rows (session_date, open, high, low, close as str/number/None) to Bars."""
    def dec(value):
        return None if value is None else Decimal(str(value))
    return [Bar(date.fromisoformat(str(row["session_date"])), dec(row["open"]), dec(row.get("high")),
                dec(row.get("low")), dec(row["close"])) for row in rows]


def scenario_from_role_output(scenario: Any) -> EvalScenario:
    """Adapt a validated role_contracts.ScenarioOutput (duck-typed, no import cycle)."""
    return EvalScenario(
        direction=scenario.direction, timeframe=scenario.timeframe,
        conditions=tuple(EvalCondition(c.kind, c.field, c.operator, Decimal(c.level), c.persistence_bars)
                         for c in scenario.conditions),
        targets=tuple(Decimal(t) for t in scenario.targets), horizon_sessions=scenario.horizon_sessions)


def _validate_scenario(scenario: EvalScenario) -> None:
    if scenario.timeframe not in ("D", "W") or scenario.direction not in ("BULLISH", "BEARISH", "NEUTRAL"):
        raise ValueError("INVALID_SCENARIO_TIMEFRAME_OR_DIRECTION")
    if sorted(c.kind for c in scenario.conditions) != ["CONFIRMATION", "INVALIDATION"]:
        raise ValueError("EXACTLY_ONE_CONFIRMATION_AND_ONE_INVALIDATION_REQUIRED")
    for c in scenario.conditions:
        if (c.field not in ("CLOSE", "HIGH", "LOW") or c.operator not in ("GT", "GE", "LT", "LE", "EQ")
                or not isinstance(c.level, Decimal) or not c.level.is_finite() or c.level <= 0
                or type(c.persistence_bars) is not int or c.persistence_bars <= 0):
            raise ValueError("INVALID_CONDITION")
    if any(not isinstance(t, Decimal) or not t.is_finite() or t <= 0 for t in scenario.targets) \
            or len(set(scenario.targets)) != len(scenario.targets):
        raise ValueError("INVALID_TARGETS")
    if type(scenario.horizon_sessions) is not int or scenario.horizon_sessions <= 0:
        raise ValueError("INVALID_HORIZON")


# ---------------------------------------------------------------------------------------------
# Calendar helpers
# ---------------------------------------------------------------------------------------------

def evaluation_sessions(origin: datetime, expected_sessions: Iterable[date], *, tz: str, session_open_local: time) -> list[date]:
    """Calendar sessions whose regular session STARTS strictly after the origin, in order."""
    if not isinstance(origin, datetime) or origin.tzinfo is None or origin.utcoffset() is None:
        raise ValueError("AWARE_ORIGIN_REQUIRED")
    zone = ZoneInfo(tz)
    return [day for day in sorted(set(expected_sessions)) if datetime.combine(day, session_open_local, zone) > origin]


def _week_monday(day: date) -> date:
    return day - timedelta(days=day.weekday())


def _compare(value: Decimal, operator: str, level: Decimal) -> bool:
    return {"GT": value > level, "GE": value >= level, "LT": value < level, "LE": value <= level, "EQ": value == level}[operator]


def _intrabar_capable(field_name: str, operator: str) -> bool:
    """Only a touch (HIGH reaching up, LOW reaching down) happens inside the bar; everything else is known at the close."""
    return (field_name == "HIGH" and operator in ("GT", "GE")) or (field_name == "LOW" and operator in ("LT", "LE"))


# ---------------------------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class Resolution:
    status: str
    final: bool
    reason: Optional[str]
    timeframe: str
    horizon_sessions: int
    first_session: Optional[date] = None
    last_session: Optional[date] = None
    observed_through: Optional[date] = None
    window_fully_observed: bool = False
    provisional_state: Optional[str] = None
    confirmation: Optional[dict] = None
    invalidation: Optional[dict] = None
    targets_reached: tuple[dict, ...] = ()
    targets_touched_before_confirmation: tuple[dict, ...] = ()
    gap_events: tuple[dict, ...] = ()
    ambiguous_events: tuple[dict, ...] = ()
    missing_sessions: tuple[date, ...] = ()
    unexpected_bar_sessions: tuple[date, ...] = ()
    target_note: Optional[str] = None
    resolver_version: str = RESOLVER_VERSION

    def to_payload(self) -> dict:
        def clean(value):
            if isinstance(value, dict):
                return {k: clean(v) for k, v in value.items()}
            if isinstance(value, (tuple, list)):
                return [clean(v) for v in value]
            if isinstance(value, (date, Decimal)):
                return str(value)
            return value
        return clean({name: getattr(self, name) for name in self.__dataclass_fields__})


# ---------------------------------------------------------------------------------------------
# Resolver
# ---------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class _Unit:
    sessions: tuple[date, ...]
    index: int          # 1-based position of the unit's last session inside the horizon window


def _units(scenario: EvalScenario, window: list[date], expected_sessions: Sequence[date]) -> list[_Unit]:
    if scenario.timeframe == "D":
        return [_Unit((day,), number) for number, day in enumerate(window, 1)]
    in_window = set(window)
    by_week: dict[date, list[date]] = {}
    for day in sorted(set(expected_sessions)):
        by_week.setdefault(_week_monday(day), []).append(day)
    units = []
    for monday, days in sorted(by_week.items()):
        if days and all(day in in_window for day in days):          # complete weeks fully inside the window only
            units.append(_Unit(tuple(days), window.index(days[-1]) + 1))
    return units


def resolve_scenario(scenario: EvalScenario, *, origin: datetime, expected_sessions: Sequence[date], bars: Sequence[Bar],
                     data_through: date, tz: str, session_open_local: time, adjustment_events: Sequence[date] = (),
                     horizon_sessions: Optional[int] = None) -> Resolution:
    """Resolve ONE scenario against the bars observed after its origin.

    expected_sessions: the instrument calendar's trading sessions (must reach past the horizon end).
    data_through:      last session whose bar is considered complete; later sessions are unobserved, not missing.
    horizon_sessions:  overrides the scenario's own horizon (e.g. secondary 5/10/20/40 analyses).
    """
    _validate_scenario(scenario)
    horizon = scenario.horizon_sessions if horizon_sessions is None else horizon_sessions
    if type(horizon) is not int or horizon <= 0:
        raise ValueError("INVALID_HORIZON")
    base = dict(timeframe=scenario.timeframe, horizon_sessions=horizon)
    by_day: dict[date, Bar] = {}
    for bar in bars:
        if bar.session_date > data_through:
            continue                                  # never look past the data cut-off
        if bar.session_date in by_day:
            raise ValueError("DUPLICATE_BAR_SESSION")
        by_day[bar.session_date] = bar

    sessions = evaluation_sessions(origin, expected_sessions, tz=tz, session_open_local=session_open_local)
    if len(sessions) < horizon:
        return Resolution(status="NOT_EVALUABLE", final=True, reason="CALENDAR_DOES_NOT_COVER_HORIZON", **base)
    window = sessions[:horizon]
    first, last = window[0], window[-1]
    base.update(first_session=first, last_session=last)
    if any(first <= day <= last for day in adjustment_events):
        return Resolution(status="NOT_COMPARABLE", final=True, reason="ADJUSTMENT_EVENT_IN_WINDOW", **base)
    unexpected = tuple(sorted(day for day in by_day if first <= day <= last and day not in set(window)))
    if unexpected:
        return Resolution(status="NOT_EVALUABLE", final=True, reason="UNEXPECTED_SESSION_LABEL",
                          unexpected_bar_sessions=unexpected, **base)
    units = _units(scenario, window, expected_sessions)
    if not units:
        return Resolution(status="NOT_EVALUABLE", final=True, reason="NO_COMPLETE_WEEKLY_BAR_IN_HORIZON", **base)

    conditions = {c.kind: c for c in scenario.conditions}
    target_note = None
    targets: tuple[Decimal, ...] = scenario.targets
    if targets and scenario.direction == "NEUTRAL":
        targets, target_note = (), "TARGETS_NOT_EVALUATED_FOR_NEUTRAL_DIRECTION"
    target_field = "HIGH" if scenario.direction == "BULLISH" else "LOW"

    state = "PENDING"
    streak = {"CONFIRMATION": 0, "INVALIDATION": 0}
    confirmation = invalidation = None
    reached: list[dict] = []
    before: list[dict] = []
    gaps: list[dict] = []
    ambiguous: list[dict] = []
    missing: list[date] = []
    halt: Optional[tuple[str, str]] = None
    observed_through: Optional[date] = None
    fully_observed = True       # every unit of the horizon window was observed and no terminal event stopped the scan

    for unit in units:
        if unit.sessions[-1] > data_through:
            fully_observed = False
            break
        members = [by_day.get(day) for day in unit.sessions]
        absent = [day for day, bar in zip(unit.sessions, members) if bar is None]
        if absent:
            missing, halt, fully_observed = absent, ("MISSING_BAR", "BAR_MISSING_IN_OBSERVED_SESSION"), False
            break
        highs, lows = [b.high for b in members], [b.low for b in members]
        values = {"CLOSE": members[-1].close,
                  "HIGH": None if any(v is None for v in highs) else max(highs),
                  "LOW": None if any(v is None for v in lows) else min(lows)}
        opening = members[0].open
        needed = {c.field for c in scenario.conditions} | ({target_field} if targets else set())
        if any(values[name] is None for name in needed):
            halt, fully_observed = ("NOT_EVALUABLE", "HIGH_LOW_REQUIRED_BY_CONDITION_OR_TARGET"), False
            break

        events: list[dict] = []
        for kind, cond in conditions.items():
            if kind == "CONFIRMATION" and state != "PENDING":
                continue
            satisfied = _compare(values[cond.field], cond.operator, cond.level)
            streak[kind] = streak[kind] + 1 if satisfied else 0
            if satisfied and streak[kind] == cond.persistence_bars:
                if _intrabar_capable(cond.field, cond.operator):
                    order = _OPEN if _compare(opening, cond.operator, cond.level) else _INTRABAR
                else:
                    order = _CLOSE
                events.append(dict(kind=kind, order=order, field=cond.field, operator=cond.operator, level=cond.level))
        for target in targets:
            if any(item["level"] == target for item in reached):
                continue
            if scenario.direction == "BULLISH":
                touched, at_open = values["HIGH"] >= target, opening >= target
            else:
                touched, at_open = values["LOW"] <= target, opening <= target
            if touched:
                events.append(dict(kind="TARGET", order=_OPEN if at_open else _INTRABAR, field=target_field,
                                   operator="GE" if scenario.direction == "BULLISH" else "LE", level=target))

        stop = False
        for order in sorted({e["order"] for e in events}):
            group = [e for e in events if e["order"] == order]
            kinds = {e["kind"] for e in group}
            order_matters = ("CONFIRMATION" in kinds and len(kinds) > 1) or (
                state == "ACTIVE" and "INVALIDATION" in kinds and "TARGET" in kinds)
            record = lambda e: dict(kind=e["kind"], session=unit.sessions[-1], index=unit.index, field=e["field"],
                                    operator=e["operator"], level=e["level"], order=_ORDER_NAME[e["order"]])
            if order == _OPEN:
                gaps.extend(record(e) for e in group)
            if order_matters:
                ambiguous = [record(e) for e in group]
                stop = True
                break
            for e in sorted(group, key=lambda item: item["kind"] != "INVALIDATION"):
                if e["kind"] == "INVALIDATION":
                    invalidation, stop = record(e), True
                    break
                if e["kind"] == "CONFIRMATION":
                    confirmation, state = record(e), "ACTIVE"
                elif state == "ACTIVE":
                    reached.append(record(e))
                elif not any(item["level"] == e["level"] for item in before):
                    before.append(record(e))
            if stop:
                break
        observed_through = unit.sessions[-1]
        if stop:
            fully_observed = False
            break

    if reached:
        status, reason = "TARGET_REACHED", None
    elif invalidation is not None:
        status = "INVALIDATED_AFTER_CONFIRMATION" if confirmation else "INVALIDATED_BEFORE_CONFIRMATION"
        reason = None
    elif ambiguous:
        status, reason = "AMBIGUOUS_SAME_BAR", "ORDER_OF_TOUCHES_NOT_DETERMINABLE_FROM_OHLC"
    elif halt is not None:
        (status, reason) = halt
    elif fully_observed:
        if state == "PENDING":
            status, reason = "EXPIRED_UNCONFIRMED", None
        else:
            status, reason = ("CONFIRMED_TARGET_NOT_REACHED" if targets else "CONFIRMED_NO_TARGETS"), None
    else:
        status, reason = "IMMATURE", "HORIZON_NOT_COMPLETE_AND_NO_TERMINAL_EVENT"

    return Resolution(status=status, final=status != "IMMATURE", reason=reason, observed_through=observed_through,
                      window_fully_observed=fully_observed,
                      provisional_state=state if status == "IMMATURE" else None,
                      confirmation=confirmation, invalidation=invalidation, targets_reached=tuple(reached),
                      targets_touched_before_confirmation=tuple(before), gap_events=tuple(gaps),
                      ambiguous_events=tuple(ambiguous), missing_sessions=tuple(missing), target_note=target_note, **base)

"""Read-only upcoming earnings context for reports and decision audit layers."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from html import escape
from typing import Any, Iterable, Mapping

from src.core.market_calendar import is_trading_day


PRE_EARNINGS_SHADOW_SESSIONS = 2
DEFAULT_UPCOMING_EARNINGS_DAYS = 45


def _as_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _optional_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed


def _optional_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed


@dataclass(frozen=True)
class UpcomingEarningsEvent:
    observation_key: str
    issuer_id: str
    ticker: str
    event_date: date
    event_time_hint: str
    source: str
    confidence: float
    lifecycle_status: str
    earnings_phase: str = "scheduled"
    fiscal_year: int | None = None
    fiscal_quarter: int | None = None
    fiscal_period_end: date | None = None
    eps_estimate: float | None = None
    reported_eps: float | None = None
    surprise_pct: float | None = None
    source_url: str = ""

    @property
    def fiscal_label(self) -> str:
        if self.fiscal_quarter and self.fiscal_year:
            return f"Q{self.fiscal_quarter} {self.fiscal_year}"
        if self.fiscal_quarter:
            return f"Q{self.fiscal_quarter}"
        if self.fiscal_year:
            return f"FY {self.fiscal_year}"
        return "periodo no informado"


def upcoming_earnings_from_rows(
    rows: Iterable[Mapping[str, Any]],
) -> list[UpcomingEarningsEvent]:
    events: list[UpcomingEarningsEvent] = []
    for row in rows:
        event_date = _as_date(row.get("event_date"))
        ticker = str(row.get("ticker") or "").upper().strip()
        if not event_date or not ticker:
            continue
        raw = row.get("raw_payload")
        payload = dict(raw) if isinstance(raw, Mapping) else {}
        events.append(
            UpcomingEarningsEvent(
                observation_key=str(row.get("observation_key") or ""),
                issuer_id=str(row.get("issuer_id") or "").upper().strip(),
                ticker=ticker,
                event_date=event_date,
                event_time_hint=str(row.get("event_time_hint") or "unknown"),
                source=str(row.get("source") or "unknown").upper().strip(),
                confidence=max(0.0, min(1.0, _optional_float(row.get("confidence")) or 0.0)),
                lifecycle_status=str(row.get("lifecycle_status") or "DISCOVERED").upper(),
                earnings_phase=str(payload.get("earnings_phase") or "scheduled"),
                fiscal_year=_optional_int(
                    row.get("fiscal_year") or payload.get("fiscal_year")
                ),
                fiscal_quarter=_optional_int(
                    row.get("fiscal_quarter") or payload.get("fiscal_quarter")
                ),
                fiscal_period_end=_as_date(
                    row.get("fiscal_period_end") or payload.get("fiscal_period_end")
                ),
                eps_estimate=_optional_float(payload.get("eps_estimate")),
                reported_eps=_optional_float(payload.get("reported_eps")),
                surprise_pct=_optional_float(payload.get("surprise_pct")),
                source_url=str(row.get("source_url") or ""),
            )
        )
    return sorted(events, key=lambda event: (event.event_date, event.ticker))


def trading_sessions_until(event_date: date, *, today: date | None = None) -> int:
    current = today or date.today()
    if event_date <= current:
        return 0
    sessions = 0
    cursor = current + timedelta(days=1)
    while cursor <= event_date:
        if is_trading_day(cursor):
            sessions += 1
        cursor += timedelta(days=1)
    return sessions


def earnings_window_state(
    event: UpcomingEarningsEvent,
    *,
    today: date | None = None,
) -> str:
    if event.earnings_phase == "post_reported":
        return "POST_REPORTED"
    sessions = trading_sessions_until(event.event_date, today=today)
    if event.event_date <= (today or date.today()):
        return "EVENT_DAY"
    if sessions <= PRE_EARNINGS_SHADOW_SESSIONS:
        return "PRE_EARNINGS_WINDOW"
    return "UPCOMING"


def earnings_shadow_layer_for_ticker(
    ticker: str,
    events: Iterable[UpcomingEarningsEvent],
    *,
    today: date | None = None,
) -> dict[str, Any]:
    target = str(ticker or "").upper().strip()
    current = today or date.today()
    candidates = [
        event
        for event in events
        if event.ticker == target
        and event.earnings_phase != "post_reported"
        and event.event_date >= current
        and trading_sessions_until(event.event_date, today=current)
        <= PRE_EARNINGS_SHADOW_SESSIONS
    ]
    if not candidates:
        return {}
    event = min(candidates, key=lambda item: item.event_date)
    sessions = trading_sessions_until(event.event_date, today=current)
    return {
        "active": True,
        "mode": "shadow",
        "state": earnings_window_state(event, today=current),
        "would_block_new_buy": True,
        "decision_changed": False,
        "event_date": event.event_date.isoformat(),
        "event_time_hint": event.event_time_hint,
        "trading_sessions_until": sessions,
        "fiscal_year": event.fiscal_year,
        "fiscal_quarter": event.fiscal_quarter,
        "fiscal_period_end": (
            event.fiscal_period_end.isoformat() if event.fiscal_period_end else None
        ),
        "eps_estimate": event.eps_estimate,
        "source": event.source,
        "confidence": event.confidence,
        "observation_key": event.observation_key,
    }


def _time_label(value: str) -> str:
    return {
        "before_open": "antes de apertura",
        "during_market": "durante la rueda",
        "after_close": "despues del cierre",
    }.get(str(value or "").lower(), "horario no informado")


def render_upcoming_earnings_html(
    events: Iterable[UpcomingEarningsEvent],
    *,
    today: date | None = None,
    compact: bool = False,
    limit: int = 8,
) -> list[str]:
    current = today or date.today()
    selected = list(events)[: max(1, int(limit))]
    if not selected:
        return []
    lines = ["━━━ <b>PROXIMOS BALANCES</b> ━━━"]
    for event in selected:
        sessions = trading_sessions_until(event.event_date, today=current)
        state = earnings_window_state(event, today=current)
        if event.event_date == current:
            distance = "hoy"
        elif sessions == 1:
            distance = "1 rueda"
        else:
            distance = f"{sessions} ruedas"
        warning = "⚠️ " if state in {"EVENT_DAY", "PRE_EARNINGS_WINDOW"} else ""
        eps = (
            f" | EPS est. {event.eps_estimate:.2f}"
            if event.eps_estimate is not None
            else ""
        )
        line = (
            f"{warning}<b>{escape(event.ticker)}</b> "
            f"{event.event_date.strftime('%d/%m')} ({distance}) | "
            f"{escape(_time_label(event.event_time_hint))} | "
            f"{escape(event.fiscal_label)}{eps}"
        )
        if not compact:
            line += f" | {escape(event.source)} conf {event.confidence:.2f}"
        lines.append(line)
    lines.append("Shadow: informa la ventana; no cambia scores ni ordenes.")
    return lines


def render_upcoming_earnings_report(
    events: Iterable[UpcomingEarningsEvent],
    *,
    today: date | None = None,
) -> str:
    selected = list(events)
    if not selected:
        return (
            "<b>PROXIMOS BALANCES</b>\n"
            "No hay presentaciones registradas para la cartera en esta ventana."
        )
    return "\n".join(render_upcoming_earnings_html(selected, today=today, limit=30))

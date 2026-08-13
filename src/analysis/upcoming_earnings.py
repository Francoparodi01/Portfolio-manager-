"""Read-only upcoming earnings context for reports and decision audit layers."""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from html import escape
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from src.core.market_calendar import is_trading_day


PRE_EARNINGS_SHADOW_SESSIONS = 2
DEFAULT_UPCOMING_EARNINGS_DAYS = 45
ART_TZ = ZoneInfo("America/Argentina/Buenos_Aires")


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
    return deduplicate_earnings_events(events)


def _same_earnings_event(
    left: UpcomingEarningsEvent,
    right: UpcomingEarningsEvent,
) -> bool:
    if left.issuer_id != right.issuer_id or abs((left.event_date - right.event_date).days) > 1:
        return False
    if (
        left.fiscal_period_end
        and right.fiscal_period_end
        and left.fiscal_period_end != right.fiscal_period_end
    ):
        return False
    left_period = (left.fiscal_year, left.fiscal_quarter)
    right_period = (right.fiscal_year, right.fiscal_quarter)
    if all(left_period) and all(right_period) and left_period != right_period:
        return False
    return True


def _canonical_event_rank(event: UpcomingEarningsEvent) -> tuple:
    fiscal_detail = sum(
        value is not None
        for value in (event.fiscal_year, event.fiscal_quarter, event.fiscal_period_end)
    )
    known_time = event.event_time_hint in {"before_open", "during_market", "after_close"}
    lifecycle_rank = {"CONFIRMED": 3, "ANNOUNCED": 2, "DISCOVERED": 1}.get(
        event.lifecycle_status,
        0,
    )
    return (
        fiscal_detail,
        int(known_time),
        lifecycle_rank,
        event.confidence,
        -event.event_date.toordinal(),
    )


def deduplicate_earnings_events(
    events: Iterable[UpcomingEarningsEvent],
) -> list[UpcomingEarningsEvent]:
    """Consolidate adjacent cross-source rows that describe one earnings event."""
    groups: list[list[UpcomingEarningsEvent]] = []
    for event in sorted(events, key=lambda item: (item.issuer_id, item.event_date, item.ticker)):
        matching = next(
            (group for group in groups if any(_same_earnings_event(event, item) for item in group)),
            None,
        )
        if matching is None:
            groups.append([event])
        else:
            matching.append(event)

    canonical: list[UpcomingEarningsEvent] = []
    for group in groups:
        selected = max(group, key=_canonical_event_rank)
        reported = next((item for item in group if item.reported_eps is not None), None)
        estimate = next((item.eps_estimate for item in group if item.eps_estimate is not None), None)
        canonical.append(
            replace(
                selected,
                earnings_phase=(
                    "post_reported"
                    if any(item.earnings_phase == "post_reported" for item in group)
                    else selected.earnings_phase
                ),
                eps_estimate=selected.eps_estimate if selected.eps_estimate is not None else estimate,
                reported_eps=reported.reported_eps if reported else selected.reported_eps,
                surprise_pct=reported.surprise_pct if reported else selected.surprise_pct,
            )
        )
    return sorted(canonical, key=lambda event: (event.event_date, event.ticker))


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


def post_earnings_session_date(event: UpcomingEarningsEvent) -> date:
    """Return the first market session able to price the earnings release."""
    session = event.event_date
    if event.event_time_hint != "before_open" or not is_trading_day(session):
        session += timedelta(days=1)
    while not is_trading_day(session):
        session += timedelta(days=1)
    return session


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


def render_recent_exit_earnings_html(
    events: Iterable[UpcomingEarningsEvent],
    *,
    exited_at_by_ticker: Mapping[str, Any],
    today: date | None = None,
    limit: int = 8,
) -> list[str]:
    """Render imminent events for tickers sold recently, without calling them holdings."""
    current = today or date.today()
    selected = list(events)[: max(1, int(limit))]
    if not selected:
        return []
    lines = ["━━━ <b>SALIDAS RECIENTES CON BALANCE</b> ━━━"]
    for event in selected:
        sessions = trading_sessions_until(event.event_date, today=current)
        if event.event_date == current:
            distance = "hoy"
        elif sessions == 1:
            distance = "1 rueda"
        else:
            distance = f"{sessions} ruedas"
        exited_at = exited_at_by_ticker.get(event.ticker)
        exited_date = _as_date(exited_at)
        exit_label = exited_date.strftime("%d/%m") if exited_date else "fecha no informada"
        eps = (
            f" | EPS est. {event.eps_estimate:.2f}"
            if event.eps_estimate is not None
            else ""
        )
        lines.append(
            f"⚪ <b>{escape(event.ticker)}</b> {event.event_date.strftime('%d/%m')} "
            f"({distance}) | {escape(_time_label(event.event_time_hint))} | "
            f"{escape(event.fiscal_label)}{eps} | salida {exit_label}"
        )
    lines.append(
        "Contexto auditable: ya no están en cartera y no cambian scores ni órdenes."
    )
    return lines


def _format_ars(value: Any) -> str:
    try:
        formatted = f"{float(value):,.2f}"
    except (TypeError, ValueError):
        return "sin precio"
    return "$" + formatted.replace(",", "_").replace(".", ",").replace("_", ".")


def render_post_earnings_reactions_html(
    reactions: Iterable[Mapping[str, Any]],
    *,
    limit: int = 8,
) -> list[str]:
    """Render observed post-earnings prices; this is market reaction, not PnL."""
    selected = list(reactions)[: max(1, int(limit))]
    if not selected:
        return []
    lines = ["━━━ <b>RESULTADO TRAS BALANCE</b> ━━━"]
    for row in selected:
        ticker = str(row.get("ticker") or "").upper().strip()
        latest_price = float(row.get("latest_price") or 0.0)
        first_price = float(row.get("first_price") or 0.0)
        previous_close = float(row.get("previous_close") or 0.0)
        reaction_pct = (
            (latest_price / previous_close) - 1.0
            if latest_price > 0 and previous_close > 0
            else None
        )
        opening_pct = (
            (first_price / previous_close) - 1.0
            if first_price > 0 and previous_close > 0
            else None
        )
        icon = "🟢" if reaction_pct is not None and reaction_pct >= 0 else "🔴"
        scope = str(row.get("scope_label") or "cartera actual")
        event_date = _as_date(row.get("event_date"))
        session_date = _as_date(row.get("session_date"))
        observed_at = row.get("observed_at")
        if isinstance(observed_at, datetime):
            if observed_at.tzinfo is not None:
                observed_at = observed_at.astimezone(ART_TZ)
            observed_label = observed_at.strftime("%d/%m %H:%M")
        else:
            observed_label = str(observed_at or "hora no informada")
        reaction_label = f"{reaction_pct:+.2%}" if reaction_pct is not None else "sin base"
        opening_label = f"{opening_pct:+.2%}" if opening_pct is not None else "sin base"
        lines.extend(
            [
                f"{icon} <b>{escape(ticker)}</b> · {escape(scope)}",
                f"Valor tras balance: <b>{_format_ars(latest_price)}</b> ({reaction_label})",
                (
                    f"Apertura observada: {_format_ars(first_price)} ({opening_label}) · "
                    f"cierre previo {_format_ars(previous_close)}"
                ),
                (
                    f"Balance {event_date.strftime('%d/%m') if event_date else '?'} · "
                    f"rueda {session_date.strftime('%d/%m') if session_date else '?'} · "
                    f"muestra {escape(observed_label)}"
                ),
            ]
        )
    lines.append("Reacción de precio observada; no representa PnL ni recomendación.")
    return lines

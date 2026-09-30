"""Compatibility facade strengthening earnings event identity/conflict reporting."""
from __future__ import annotations

from dataclasses import replace
from datetime import date
from html import escape
from typing import Iterable, Mapping, Any

from . import upcoming_earnings_core as _core

for _name in dir(_core):
    if not _name.startswith("__"):
        globals().setdefault(_name, getattr(_core, _name))


def __getattr__(name):
    return getattr(_core, name)


def _same_identity(left, right) -> bool:
    owner_left = left.issuer_id or left.ticker
    owner_right = right.issuer_id or right.ticker
    if owner_left != owner_right:
        return False
    if left.fiscal_period_end and right.fiscal_period_end:
        return left.fiscal_period_end == right.fiscal_period_end
    if left.fiscal_year and left.fiscal_quarter and right.fiscal_year and right.fiscal_quarter:
        return (left.fiscal_year, left.fiscal_quarter) == (right.fiscal_year, right.fiscal_quarter)
    if left.observation_key and right.observation_key and left.observation_key == right.observation_key:
        return True
    # Two quarterly earnings events for the same issuer cannot legitimately be
    # only a few days apart. Treat this as a date conflict, not two certainties.
    return abs((left.event_date - right.event_date).days) <= 14


def deduplicate_earnings_events(events: Iterable[object]) -> list[object]:
    groups: list[list[object]] = []
    for event in sorted(events, key=lambda item: ((item.issuer_id or item.ticker), item.event_date, item.source)):
        group = next((g for g in groups if any(_same_identity(event, prior) for prior in g)), None)
        if group is None:
            groups.append([event])
        else:
            group.append(event)

    canonical = []
    for group in groups:
        selected = max(group, key=_core._canonical_event_rank)
        reported = next((item for item in group if item.reported_eps is not None), None)
        estimate = next((item.eps_estimate for item in group if item.eps_estimate is not None), None)
        dates = tuple(sorted({item.event_date for item in group}))
        sources = tuple(sorted({item.source for item in group}))
        conflict = len(dates) > 1
        canonical.append(replace(
            selected,
            date_conflict=conflict,
            conflicting_dates=dates if conflict else (),
            conflicting_sources=sources if conflict else (),
            earnings_phase=(
                "post_reported" if any(item.earnings_phase == "post_reported" for item in group)
                else selected.earnings_phase
            ),
            eps_estimate=selected.eps_estimate if selected.eps_estimate is not None else estimate,
            reported_eps=reported.reported_eps if reported else selected.reported_eps,
            surprise_pct=reported.surprise_pct if reported else selected.surprise_pct,
        ))
    return sorted(canonical, key=lambda event: (event.event_date, event.ticker))


def upcoming_earnings_from_rows(rows: Iterable[Mapping[str, Any]]) -> list[object]:
    # Let the existing parser normalize source payloads first, then apply the
    # stronger identity contract to the resulting canonical event objects.
    return deduplicate_earnings_events(_core.upcoming_earnings_from_rows(rows))


def render_upcoming_earnings_html(
    events: Iterable[object], *, today: date | None = None,
    compact: bool = False, limit: int = 8,
) -> list[str]:
    current = today or date.today()
    selected = list(events)[: max(1, int(limit))]
    if not selected:
        return []
    lines = ["━━━ <b>PROXIMOS BALANCES</b> ━━━"]
    for event in selected:
        sessions = _core.trading_sessions_until(event.event_date, today=current)
        state = _core.earnings_window_state(event, today=current)
        if event.event_date == current:
            distance = "hoy"
        elif sessions == 1:
            distance = "1 rueda"
        else:
            distance = f"{sessions} ruedas"
        warning = "⚠️ " if state in {"EVENT_DAY", "PRE_EARNINGS_WINDOW"} or event.date_conflict else ""
        eps = f" | EPS est. {event.eps_estimate:.2f}" if event.eps_estimate is not None else ""
        line = (
            f"{warning}<b>{escape(event.ticker)}</b> {event.event_date.strftime('%d/%m')} ({distance}) | "
            f"{escape(_core._time_label(event.event_time_hint))} | {escape(event.fiscal_label)}{eps}"
        )
        if not compact:
            line += f" | {escape(event.source)} conf {event.confidence:.2f}"
        lines.append(line)
        if event.date_conflict:
            dates = " vs ".join(value.strftime("%d/%m") for value in event.conflicting_dates)
            sources = ", ".join(event.conflicting_sources)
            lines.append(
                f"   ⚠️ Conflicto de fecha: {escape(dates)} ({escape(sources)}). "
                "Se usa una fecha canónica por prioridad de fuente documentada, sin ocultar el conflicto."
            )
    lines.append("Shadow: informa la ventana; no cambia scores ni ordenes.")
    return lines


def render_upcoming_earnings_report(events: Iterable[object], *, today: date | None = None) -> str:
    selected = list(events)
    if not selected:
        return "<b>PROXIMOS BALANCES</b>\nNo hay presentaciones registradas para la cartera en esta ventana."
    return "\n".join(render_upcoming_earnings_html(selected, today=today, limit=30))

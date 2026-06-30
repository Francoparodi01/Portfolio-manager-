from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from html import escape
from typing import Any, Iterable
from zoneinfo import ZoneInfo

ART_TZ = ZoneInfo("America/Argentina/Buenos_Aires")

EVENT_TIME_HINTS = {"before_open", "during_market", "after_close", "unknown"}
EVENT_SEVERITIES = {"low", "medium", "high"}
EVENT_POLICIES = {"warn_only", "block_new_buys", "no_action"}
BLOCK_NEW_BUYS = "block_new_buys"

MANUAL_MARKET_EVENTS_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS manual_market_events (
    id              BIGSERIAL PRIMARY KEY,
    event_date      DATE        NOT NULL,
    event_time_hint TEXT        NOT NULL DEFAULT 'unknown',
    ticker          TEXT,
    title           TEXT        NOT NULL,
    impact_scope    TEXT[]      NOT NULL DEFAULT ARRAY[]::TEXT[],
    related_tickers TEXT[]      NOT NULL DEFAULT ARRAY[]::TEXT[],
    severity        TEXT        NOT NULL DEFAULT 'medium',
    active_from     TIMESTAMPTZ NOT NULL,
    active_until    TIMESTAMPTZ NOT NULL,
    action_policy   TEXT        NOT NULL DEFAULT 'warn_only',
    notes           TEXT,
    is_active       BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (event_time_hint IN ('before_open', 'during_market', 'after_close', 'unknown')),
    CHECK (severity IN ('low', 'medium', 'high')),
    CHECK (action_policy IN ('warn_only', 'block_new_buys', 'no_action')),
    CHECK (active_until >= active_from)
);

CREATE INDEX IF NOT EXISTS idx_manual_market_events_active_window
    ON manual_market_events (is_active, active_from, active_until);

CREATE INDEX IF NOT EXISTS idx_manual_market_events_ticker
    ON manual_market_events (ticker);
"""


@dataclass(frozen=True)
class ManualMarketEvent:
    id: int | None
    event_date: date
    event_time_hint: str
    ticker: str
    title: str
    impact_scope: tuple[str, ...]
    related_tickers: tuple[str, ...]
    severity: str
    active_from: datetime
    active_until: datetime
    action_policy: str
    notes: str = ""
    is_active: bool = True

    @property
    def impacted_tickers(self) -> tuple[str, ...]:
        tickers = []
        if self.ticker:
            tickers.append(self.ticker)
        tickers.extend(self.related_tickers)
        return tuple(dict.fromkeys(_norm_ticker(t) for t in tickers if _norm_ticker(t)))

    @property
    def blocks_new_buys(self) -> bool:
        return self.is_active and self.action_policy == BLOCK_NEW_BUYS


def _norm_ticker(value: Any) -> str:
    return str(value or "").upper().strip()


def _norm_token(value: Any) -> str:
    return str(value or "").strip()


def normalize_csv(value: str | Iterable[str] | None, *, ticker: bool = False) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        raw_items = value.split(",")
    else:
        raw_items = list(value)
    out = []
    for item in raw_items:
        token = _norm_ticker(item) if ticker else _norm_token(item)
        if token:
            out.append(token)
    return tuple(dict.fromkeys(out))


def normalize_event_time_hint(value: str | None) -> str:
    hint = str(value or "unknown").lower().strip()
    return hint if hint in EVENT_TIME_HINTS else "unknown"


def normalize_severity(value: str | None) -> str:
    severity = str(value or "medium").lower().strip()
    return severity if severity in EVENT_SEVERITIES else "medium"


def normalize_action_policy(value: str | None) -> str:
    policy = str(value or "warn_only").lower().strip()
    return policy if policy in EVENT_POLICIES else "warn_only"


def default_active_window(event_date: date, event_time_hint: str) -> tuple[datetime, datetime]:
    hint = normalize_event_time_hint(event_time_hint)
    if hint == "before_open":
        active_from_day = event_date - timedelta(days=1)
        active_from = datetime.combine(active_from_day, time(17, 0), tzinfo=ART_TZ)
        active_until = datetime.combine(event_date, time(12, 0), tzinfo=ART_TZ)
    elif hint == "during_market":
        active_from = datetime.combine(event_date, time(0, 0), tzinfo=ART_TZ)
        active_until = datetime.combine(event_date, time(17, 0), tzinfo=ART_TZ)
    elif hint == "after_close":
        active_from = datetime.combine(event_date, time(0, 0), tzinfo=ART_TZ)
        active_until = datetime.combine(event_date + timedelta(days=1), time(12, 0), tzinfo=ART_TZ)
    else:
        active_from = datetime.combine(event_date - timedelta(days=1), time(17, 0), tzinfo=ART_TZ)
        active_until = datetime.combine(event_date + timedelta(days=1), time(12, 0), tzinfo=ART_TZ)
    return active_from, active_until


def manual_market_event_from_row(row: Any) -> ManualMarketEvent:
    get = row.get if hasattr(row, "get") else lambda key, default=None: row[key] if key in row else default
    return ManualMarketEvent(
        id=get("id"),
        event_date=get("event_date"),
        event_time_hint=normalize_event_time_hint(get("event_time_hint")),
        ticker=_norm_ticker(get("ticker")),
        title=str(get("title") or "").strip(),
        impact_scope=normalize_csv(get("impact_scope") or ()),
        related_tickers=normalize_csv(get("related_tickers") or (), ticker=True),
        severity=normalize_severity(get("severity")),
        active_from=get("active_from"),
        active_until=get("active_until"),
        action_policy=normalize_action_policy(get("action_policy")),
        notes=str(get("notes") or "").strip(),
        is_active=bool(get("is_active", True)),
    )


def active_event_risk_by_ticker(events: Iterable[ManualMarketEvent]) -> dict[str, str]:
    risk: dict[str, str] = {}
    for event in events or []:
        if not event.blocks_new_buys:
            continue
        reason = event_block_reason(event)
        for ticker in event.impacted_tickers:
            risk.setdefault(ticker, reason)
    return risk


def event_block_reason(event: ManualMarketEvent) -> str:
    scope = ", ".join(event.impact_scope) if event.impact_scope else "sin scope"
    timing = event.event_time_hint.replace("_", " ")
    return (
        f"EVENT_RISK: {event.title} ({event.event_date.isoformat()} {timing}, "
        f"sev={event.severity}, scope={scope}); policy=block_new_buys"
    )


def manual_event_layers_for_ticker(
    ticker: str,
    events: Iterable[ManualMarketEvent],
) -> dict[str, Any]:
    ticker = _norm_ticker(ticker)
    matched = [
        event
        for event in events or []
        if ticker and ticker in event.impacted_tickers
    ]
    if not matched:
        return {}
    return {
        "active": True,
        "blocked_new_buy": any(event.blocks_new_buys for event in matched),
        "events": [
            {
                "id": event.id,
                "title": event.title,
                "ticker": event.ticker,
                "event_date": event.event_date.isoformat(),
                "event_time_hint": event.event_time_hint,
                "severity": event.severity,
                "action_policy": event.action_policy,
                "impact_scope": list(event.impact_scope),
                "related_tickers": list(event.related_tickers),
            }
            for event in matched
        ],
    }


def serialize_manual_market_events(events: Iterable[ManualMarketEvent]) -> list[dict[str, Any]]:
    return [
        {
            "id": event.id,
            "event_date": event.event_date.isoformat(),
            "event_time_hint": event.event_time_hint,
            "ticker": event.ticker,
            "title": event.title,
            "impact_scope": list(event.impact_scope),
            "related_tickers": list(event.related_tickers),
            "severity": event.severity,
            "active_from": event.active_from.isoformat() if event.active_from else None,
            "active_until": event.active_until.isoformat() if event.active_until else None,
            "action_policy": event.action_policy,
            "notes": event.notes,
        }
        for event in events or []
    ]


def render_manual_market_events_html(
    events: Iterable[ManualMarketEvent],
    *,
    compact: bool = False,
) -> list[str]:
    active = list(events or [])
    if not active:
        return []

    if compact:
        first = active[0]
        impacted = ", ".join(first.impacted_tickers[:6])
        suffix = f" (+{len(active) - 1})" if len(active) > 1 else ""
        return [
            "⚠️ <b>Evento manual activo</b>: "
            f"{escape(first.title)} {escape(first.event_time_hint.replace('_', ' '))} "
            f"| policy <b>{escape(first.action_policy)}</b> | expuestos: {escape(impacted)}{suffix}"
        ]

    lines = ["⚠️ <b>Eventos/catalysts manuales activos</b>"]
    for event in active[:5]:
        impacted = ", ".join(event.impacted_tickers[:8]) or "N/A"
        scope = ", ".join(event.impact_scope) or "N/A"
        policy = (
            "bloquea compras nuevas"
            if event.action_policy == BLOCK_NEW_BUYS
            else event.action_policy.replace("_", " ")
        )
        lines.append(
            f"   • <b>{escape(event.title)}</b> — "
            f"{escape(event.event_date.isoformat())} "
            f"{escape(event.event_time_hint.replace('_', ' '))} | "
            f"sev <b>{escape(event.severity)}</b> | policy: <b>{escape(policy)}</b>"
        )
        lines.append(
            f"     Scope: {escape(scope)} | expuestos: {escape(impacted)}"
        )
        if event.notes:
            lines.append(f"     Nota: {escape(event.notes[:220])}")
    if len(active) > 5:
        lines.append(f"   +{len(active) - 5} evento(s) manual(es) más.")
    return lines

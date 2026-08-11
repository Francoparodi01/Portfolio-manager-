from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
import json
from typing import Any, Iterable, Mapping

import asyncpg


@dataclass(frozen=True, slots=True)
class DecisionTimelineEvent:
    event_id: str
    event_type: str
    ts: datetime
    ticker: str | None
    run_id: str | None
    decision_log_id: int | None
    source: str
    payload: dict[str, Any]
    gaps: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "ts": self.ts.isoformat(),
            "ticker": self.ticker,
            "run_id": self.run_id,
            "decision_log_id": self.decision_log_id,
            "source": self.source,
            "payload": _json_ready(self.payload),
            "gaps": list(self.gaps),
        }


def build_decision_timeline(
    decision_rows: Iterable[Mapping[str, Any]],
    *,
    movement_rows: Iterable[Mapping[str, Any]] | None = None,
    fill_rows: Iterable[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    events: list[DecisionTimelineEvent] = []

    for row in decision_rows:
        events.extend(_events_from_decision(dict(row)))

    for row in movement_rows or []:
        events.append(_event_from_movement(dict(row)))

    for row in fill_rows or []:
        events.append(_event_from_fill(dict(row)))

    events.sort(key=lambda event: (event.ts, event.event_type, event.event_id))
    gaps = sorted({gap for event in events for gap in event.gaps})
    return {
        "events": [event.to_dict() for event in events],
        "summary": {
            "event_count": len(events),
            "tickers": sorted({event.ticker for event in events if event.ticker}),
            "gaps": gaps,
        },
    }


async def fetch_decision_timeline(
    conn: asyncpg.Connection,
    *,
    days: int = 90,
    run_id: str | None = None,
    ticker: str | None = None,
    owner_chat_id: int | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    days = max(1, min(int(days), 365))
    limit = max(1, min(int(limit), 1000))
    ticker_filter = str(ticker or "").upper().strip() or None
    run_filter = str(run_id or "").strip() or None

    decision_rows = await conn.fetch(
        """
        SELECT
            id,
            decided_at,
            ticker,
            decision,
            final_score,
            confidence,
            layers,
            price_at_decision,
            source,
            status,
            decision_type,
            block_reason,
            theoretical_amount_ars,
            executed_amount_ars,
            is_executable,
            was_blocked,
            run_id::text AS run_id,
            run_intent,
            decision_stage,
            metric_scope,
            COALESCE(executable_outcome_5d, outcome_5d) AS outcome_5d,
            COALESCE(executable_outcome_10d, outcome_10d) AS outcome_10d,
            COALESCE(executable_outcome_20d, outcome_20d) AS outcome_20d,
            COALESCE(executable_outcome_40d, outcome_40d) AS outcome_40d,
            outcome_basis,
            is_primary_metric,
            outcome_filled_at
        FROM decision_log
        WHERE decided_at >= NOW() - ($1::int * INTERVAL '1 day')
          AND ($2::text IS NULL OR run_id::text = $2)
          AND ($3::text IS NULL OR ticker = $3)
          AND ($4::bigint IS NULL OR owner_chat_id = $4)
        ORDER BY decided_at DESC, id DESC
        LIMIT $5
        """,
        days,
        run_filter,
        ticker_filter,
        owner_chat_id,
        limit,
    )

    movement_rows = []
    if run_filter is None and owner_chat_id is None:
        movement_rows = await conn.fetch(
            """
            SELECT
                id,
                executed_at,
                ticker,
                movement_type,
                amount,
                quantity,
                price,
                source,
                external_movement_id
            FROM broker_movements
            WHERE executed_at >= NOW() - ($1::int * INTERVAL '1 day')
              AND NOT (COALESCE(raw_payload, '{}'::jsonb) ? 'superseded_by_real')
              AND ($2::text IS NULL OR ticker = $2)
            ORDER BY executed_at DESC, id DESC
            LIMIT $3
            """,
            days,
            ticker_filter,
            limit,
        )

    fill_rows = await conn.fetch(
        """
        SELECT
            bf.id,
            bf.executed_at,
            bf.ticker,
            bf.side,
            bf.quantity,
            bf.avg_fill_price,
            bf.gross_amount_ars,
            bf.fees_ars,
            bf.source,
            bf.external_fill_id,
            bf.decision_log_id,
            dl.run_id::text AS run_id
        FROM broker_fills bf
        LEFT JOIN decision_log dl ON dl.id = bf.decision_log_id
        WHERE bf.executed_at >= NOW() - ($1::int * INTERVAL '1 day')
          AND ($2::text IS NULL OR bf.ticker = $2)
          AND ($3::bigint IS NULL OR bf.owner_chat_id = $3)
          AND ($4::text IS NULL OR dl.run_id::text = $4)
        ORDER BY bf.executed_at DESC, bf.id DESC
        LIMIT $5
        """,
        days,
        ticker_filter,
        owner_chat_id,
        run_filter,
        limit,
    )

    return build_decision_timeline(
        decision_rows,
        movement_rows=movement_rows,
        fill_rows=fill_rows,
    )


def render_decision_timeline(data: Mapping[str, Any]) -> str:
    summary = data.get("summary") or {}
    lines = [
        "Decision Timeline",
        f"events={summary.get('event_count', 0)} tickers={', '.join(summary.get('tickers') or []) or 'none'}",
    ]
    gaps = summary.get("gaps") or []
    if gaps:
        lines.append(f"gaps={', '.join(gaps)}")
    lines.append("")

    for event in data.get("events") or []:
        payload = event.get("payload") or {}
        label = payload.get("decision") or payload.get("side") or payload.get("status") or ""
        amount = payload.get("amount_ars") or payload.get("gross_amount_ars")
        amount_text = f" amount={amount}" if amount is not None else ""
        gap_text = f" gaps={','.join(event.get('gaps') or [])}" if event.get("gaps") else ""
        lines.append(
            f"{event.get('ts')} {event.get('event_type')} "
            f"{event.get('ticker') or '-'} {label}{amount_text}{gap_text}"
        )

    return "\n".join(lines).rstrip()


def _events_from_decision(row: dict[str, Any]) -> list[DecisionTimelineEvent]:
    decision_id = _optional_int(row.get("id"))
    ticker = _text(row.get("ticker"))
    run_id = _text(row.get("run_id"))
    source = _text(row.get("source")) or _layer_source(row.get("layers")) or "decision_log"
    ts = _as_datetime(row.get("decided_at"))
    layers = _layers(row.get("layers"))
    gaps = _decision_gaps(row, layers)
    reason = _decision_reason(row, layers)

    base_payload = {
        "decision": _text(row.get("decision")),
        "status": _text(row.get("status")),
        "decision_type": _text(row.get("decision_type")),
        "reason": reason,
        "final_score": _json_ready(row.get("final_score")),
        "confidence": _json_ready(row.get("confidence")),
        "price_at_decision": _json_ready(row.get("price_at_decision")),
        "is_executable": bool(row.get("is_executable")),
        "was_blocked": bool(row.get("was_blocked")),
        "run_intent": _text(row.get("run_intent")),
        "decision_stage": _text(row.get("decision_stage")),
        "metric_scope": _text(row.get("metric_scope")),
    }

    events = [
        DecisionTimelineEvent(
            event_id=f"decision:{decision_id}",
            event_type="decision_logged",
            ts=ts,
            ticker=ticker,
            run_id=run_id,
            decision_log_id=decision_id,
            source=source,
            payload=base_payload,
            gaps=gaps,
        )
    ]

    if source == "execution_plan" or row.get("theoretical_amount_ars") is not None:
        events.append(
            DecisionTimelineEvent(
                event_id=f"plan:{decision_id}",
                event_type="plan_created",
                ts=ts,
                ticker=ticker,
                run_id=run_id,
                decision_log_id=decision_id,
                source="execution_plan",
                payload={
                    "decision": base_payload["decision"],
                    "status": base_payload["status"],
                    "reason": reason,
                    "theoretical_amount_ars": _json_ready(row.get("theoretical_amount_ars")),
                    "executed_amount_ars": _json_ready(row.get("executed_amount_ars")),
                    "is_executable": base_payload["is_executable"],
                    "was_blocked": base_payload["was_blocked"],
                },
                gaps=sorted(set(gaps + ["missing_order_id"])),
            )
        )

    outcomes = {
        key: _json_ready(row.get(key))
        for key in ("outcome_5d", "outcome_10d", "outcome_20d", "outcome_40d")
        if row.get(key) is not None
    }
    if outcomes:
        outcomes["decision"] = base_payload["decision"]
        outcomes["status"] = base_payload["status"]
        outcomes["reason"] = reason
        outcomes["decided_at"] = ts.isoformat()
        outcomes["executed_amount_ars"] = _json_ready(row.get("executed_amount_ars"))
        outcomes["is_executable"] = base_payload["is_executable"]
        outcomes["was_blocked"] = base_payload["was_blocked"]
        outcomes["metric_scope"] = base_payload["metric_scope"]
        outcomes["outcome_basis"] = _text(row.get("outcome_basis"))
        outcomes["is_primary_metric"] = bool(row.get("is_primary_metric"))
        events.append(
            DecisionTimelineEvent(
                event_id=f"outcome:{decision_id}",
                event_type="outcome_updated",
                ts=_as_datetime(row.get("outcome_filled_at") or row.get("decided_at")),
                ticker=ticker,
                run_id=run_id,
                decision_log_id=decision_id,
                source="decision_log",
                payload=outcomes,
                gaps=[],
            )
        )

    return events


def _event_from_movement(row: dict[str, Any]) -> DecisionTimelineEvent:
    row_id = _optional_int(row.get("id"))
    return DecisionTimelineEvent(
        event_id=f"movement:{row_id}",
        event_type="movement_detected",
        ts=_as_datetime(row.get("executed_at")),
        ticker=_text(row.get("ticker")),
        run_id=None,
        decision_log_id=None,
        source=_text(row.get("source")) or "broker_movements",
        payload={
            "side": _text(row.get("movement_type")),
            "amount_ars": _json_ready(row.get("amount")),
            "quantity": _json_ready(row.get("quantity")),
            "price": _json_ready(row.get("price")),
            "external_movement_id": _text(row.get("external_movement_id")),
        },
        gaps=["missing_decision_link"],
    )


def _event_from_fill(row: dict[str, Any]) -> DecisionTimelineEvent:
    row_id = _optional_int(row.get("id"))
    decision_id = _optional_int(row.get("decision_log_id"))
    gaps = [] if decision_id else ["missing_decision_link"]
    return DecisionTimelineEvent(
        event_id=f"fill:{row_id}",
        event_type="fill_detected",
        ts=_as_datetime(row.get("executed_at")),
        ticker=_text(row.get("ticker")),
        run_id=_text(row.get("run_id")),
        decision_log_id=decision_id,
        source=_text(row.get("source")) or "broker_fills",
        payload={
            "side": _text(row.get("side")),
            "quantity": _json_ready(row.get("quantity")),
            "avg_fill_price": _json_ready(row.get("avg_fill_price")),
            "gross_amount_ars": _json_ready(row.get("gross_amount_ars")),
            "fees_ars": _json_ready(row.get("fees_ars")),
            "external_fill_id": _text(row.get("external_fill_id")),
        },
        gaps=gaps,
    )


def _decision_gaps(row: Mapping[str, Any], layers: Mapping[str, Any]) -> list[str]:
    gaps: list[str] = []
    run_context = layers.get("run_context") if isinstance(layers.get("run_context"), dict) else {}
    feature_snapshot = layers.get("feature_snapshot") if isinstance(layers.get("feature_snapshot"), dict) else {}
    if not row.get("run_id"):
        gaps.append("missing_run_id")
    if not run_context:
        gaps.append("missing_run_context")
    if not (
        feature_snapshot.get("feature_snapshot_id")
        or run_context.get("feature_snapshot_id")
    ):
        gaps.append("missing_feature_snapshot_id")
    if not run_context.get("portfolio_snapshot_id"):
        gaps.append("missing_portfolio_snapshot_id")
    return gaps


def _decision_reason(row: Mapping[str, Any], layers: Mapping[str, Any]) -> str | None:
    return (
        _text(row.get("block_reason"))
        or _text(layers.get("reason"))
        or _text(layers.get("forced_reason"))
    )


def _layers(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _layer_source(value: Any) -> str | None:
    return _text(_layers(value).get("source"))


def _as_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def _text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _optional_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _json_ready(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(v) for v in value]
    return value


__all__ = [
    "DecisionTimelineEvent",
    "build_decision_timeline",
    "fetch_decision_timeline",
    "render_decision_timeline",
]

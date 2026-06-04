from __future__ import annotations

import json
import hashlib
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
import re
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from src.collector.broker_fills import BrokerFill


ART_TZ = ZoneInfo("America/Argentina/Buenos_Aires")


@dataclass(frozen=True)
class BrokerMovement:
    external_movement_id: str
    executed_at: datetime
    movement_type: str
    currency: str
    amount: float | None = None
    quantity: float | None = None
    price: float | None = None
    ticker: str | None = None
    instrument_type: str | None = None
    settlement_date: date | None = None
    description: str | None = None
    detail: str | None = None
    label: str | None = None
    balance: float | None = None
    source: str = "cocos_movements"
    raw_payload: dict[str, Any] | None = None
    executed_at_precision: str = "date_only"
    executed_at_source: str = "cocos_movements.execution_date"


def _parse_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _timestamp_precision(value: Any) -> str:
    if isinstance(value, datetime):
        return "exact"
    if isinstance(value, (int, float)):
        return "exact"
    text = str(value or "").strip()
    if not text:
        return "unknown"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return "date_only"
    if re.fullmatch(r"\d{1,2}[/-]\d{1,2}[/-]\d{4}", text):
        return "date_only"
    return "exact" if re.search(r"\d{1,2}:\d{2}", text) else "date_only"


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=ART_TZ)
    if isinstance(value, (int, float)):
        raw = float(value)
        if raw > 10_000_000_000:
            raw = raw / 1000
        return datetime.fromtimestamp(raw, tz=timezone.utc)

    day = _parse_date(value)
    if day:
        return datetime.combine(day, time.min, tzinfo=ART_TZ)
    text = str(value or "").strip()
    if text:
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=ART_TZ)
        except ValueError:
            pass
    return datetime.now(tz=ART_TZ)


def _timestamp_from_row(
    row: dict[str, Any],
    fields: tuple[str, ...],
    *,
    source: str,
) -> tuple[datetime, str, str]:
    for field in fields:
        value = row.get(field)
        if value in (None, ""):
            continue
        return _parse_datetime(value), _timestamp_precision(value), f"{source}.{field}"
    return datetime.now(tz=ART_TZ), "inferred", f"{source}.scrape_time"


def _parse_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        text = str(value).replace("$", "").replace(".", "").replace(",", ".").strip()
        try:
            return float(text)
        except Exception:
            return None


def _synthetic_ticker_movement_id(row: dict[str, Any], movement_type: str) -> str:
    ticker = str(row.get("instrument_code") or "").upper().strip()
    day = str(row.get("execution_date") or row.get("date") or "")[:10]
    settlement = str(row.get("settlement_date") or "")[:10]
    raw = "|".join(
        [
            day,
            settlement,
            ticker,
            movement_type,
            str(row.get("id_instrument") or ""),
            str(row.get("quantity") or ""),
            str(row.get("price") or ""),
            str(row.get("amount") or ""),
        ]
    )
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
    return f"synthetic:{day}:{ticker}:{movement_type}:{digest}"


def _movement_type(row: dict[str, Any]) -> str:
    raw = str(
        row.get("label")
        or row.get("description")
        or row.get("detail")
        or row.get("operation_type")
        or ""
    ).strip()
    text = raw.lower()
    if "compra" in text:
        return "BUY"
    if "venta" in text:
        return "SELL"
    if "extracci" in text or "retiro" in text:
        return "WITHDRAWAL"
    if "dep" in text or "ingreso" in text:
        return "DEPOSIT"
    if raw:
        return raw.upper()
    return "UNKNOWN"


def _signed_amount(row: dict[str, Any], movement_type: str) -> float | None:
    amount = _parse_float(row.get("quantity"))
    if amount is None:
        amount = _parse_float(row.get("amount") or row.get("total"))
    if amount is None:
        return None
    if movement_type in {"BUY", "WITHDRAWAL"}:
        return -abs(amount)
    if movement_type in {"SELL", "DEPOSIT"}:
        return abs(amount)
    return amount


def _cash_movement_from_row(
    row: dict[str, Any],
    *,
    source: str,
) -> BrokerMovement | None:
    external_id = row.get("id_cash_movement") or row.get("id_ticket") or row.get("id")
    if external_id in (None, ""):
        return None

    movement_type = _movement_type(row)
    ticker = str(row.get("instrument_code") or "").upper().strip() or None
    currency = str(row.get("id_currency") or row.get("currency") or "ARS").upper()

    executed_at, precision, ts_source = _timestamp_from_row(
        row,
        ("execution_date", "timestamp", "created_at", "date"),
        source=source,
    )

    return BrokerMovement(
        external_movement_id=str(external_id),
        executed_at=executed_at,
        movement_type=movement_type,
        currency=currency,
        amount=_signed_amount(row, movement_type),
        ticker=ticker,
        instrument_type=str(row.get("instrument_type") or "").upper().strip() or None,
        settlement_date=_parse_date(row.get("settlement_date")),
        description=str(row.get("description") or "").strip() or None,
        detail=str(row.get("detail") or "").strip() or None,
        label=str(row.get("label") or "").strip() or None,
        balance=_parse_float(row.get("balance")),
        source=source,
        raw_payload=row,
        executed_at_precision=precision,
        executed_at_source=ts_source,
    )


def _ticker_movement_from_row(
    row: dict[str, Any],
    *,
    source: str,
) -> BrokerMovement | None:
    external_id = row.get("id_ticket") or row.get("id_movement") or row.get("id")
    ticker = str(row.get("instrument_code") or "").upper().strip()
    if not ticker:
        return None

    movement_type = _movement_type(row)
    if external_id in (None, ""):
        external_id = _synthetic_ticker_movement_id(row, movement_type)
    quantity = _parse_float(row.get("quantity"))
    amount = _parse_float(row.get("amount"))
    if amount is not None:
        if movement_type == "SELL":
            amount = -abs(amount)
        elif movement_type == "BUY":
            amount = abs(amount)

    executed_at, precision, ts_source = _timestamp_from_row(
        row,
        ("execution_date", "timestamp", "created_at", "date"),
        source=source,
    )

    return BrokerMovement(
        external_movement_id=str(external_id),
        executed_at=executed_at,
        movement_type=movement_type,
        currency=str(row.get("id_currency") or row.get("currency") or "ARS").upper(),
        amount=amount,
        quantity=quantity,
        price=_parse_float(row.get("price")),
        ticker=ticker,
        instrument_type=str(row.get("instrument_type") or "").upper().strip() or None,
        settlement_date=_parse_date(row.get("settlement_date")),
        description=str(row.get("description") or "").strip() or None,
        detail=str(row.get("detail") or "").strip() or None,
        label=str(row.get("label") or "").strip() or None,
        balance=None,
        source=source,
        raw_payload=row,
        executed_at_precision=precision,
        executed_at_source=ts_source,
    )


def _iter_cash_movements(payload: Any) -> Iterable[dict[str, Any]]:
    if isinstance(payload, dict):
        value = payload.get("cashMovements")
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    yield item
        for child in payload.values():
            yield from _iter_cash_movements(child)
    elif isinstance(payload, list):
        for item in payload:
            yield from _iter_cash_movements(item)


def _iter_ticker_movements(payload: Any) -> Iterable[dict[str, Any]]:
    if isinstance(payload, dict):
        value = payload.get("tickerMovements")
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    yield item
        for child in payload.values():
            yield from _iter_ticker_movements(child)
    elif isinstance(payload, list):
        for item in payload:
            yield from _iter_ticker_movements(item)


def broker_movements_from_cocos_payloads(
    payloads: Iterable[Any],
    *,
    source: str = "cocos_movements",
) -> list[BrokerMovement]:
    by_key: dict[tuple[str, str], BrokerMovement] = {}
    for payload in payloads:
        for row in _iter_ticker_movements(payload):
            movement = _ticker_movement_from_row(row, source=source)
            if movement is None:
                continue
            by_key[(movement.source, movement.external_movement_id)] = movement
        for row in _iter_cash_movements(payload):
            movement = _cash_movement_from_row(row, source=source)
            if movement is None:
                continue
            by_key[(movement.source, movement.external_movement_id)] = movement
    return list(by_key.values())


def broker_fills_from_movements(
    movements: Iterable[BrokerMovement],
    *,
    source: str = "cocos_movements",
) -> list[BrokerFill]:
    """Build clean execution fills from Cocos Instrumentos movements."""
    fills: list[BrokerFill] = []
    for movement in movements:
        side = str(movement.movement_type or "").upper().strip()
        if side not in {"BUY", "SELL"}:
            continue
        if not movement.ticker or movement.quantity is None or movement.price is None:
            continue

        quantity = abs(float(movement.quantity))
        price = float(movement.price)
        if quantity <= 0 or price <= 0:
            continue

        gross = (
            abs(float(movement.amount))
            if movement.amount is not None
            else quantity * price
        )
        fills.append(
            BrokerFill(
                external_fill_id=str(movement.external_movement_id),
                executed_at=movement.executed_at,
                ticker=movement.ticker.upper(),
                side=side,
                quantity=quantity,
                avg_fill_price=price,
                gross_amount_ars=gross,
                fees_ars=None,
                source=source,
                raw_payload=movement.raw_payload,
                executed_at_precision=movement.executed_at_precision,
                executed_at_source=movement.executed_at_source,
            )
        )
    return fills


def serialize_raw_payload(payload: dict[str, Any] | None) -> str:
    return json.dumps(payload or {}, ensure_ascii=True, sort_keys=True)

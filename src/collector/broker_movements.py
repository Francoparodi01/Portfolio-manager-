from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Any, Iterable
from zoneinfo import ZoneInfo


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


def _parse_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _parse_datetime(value: Any) -> datetime:
    day = _parse_date(value)
    if day:
        return datetime.combine(day, time.min, tzinfo=ART_TZ)
    return datetime.now(tz=ART_TZ)


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

    return BrokerMovement(
        external_movement_id=str(external_id),
        executed_at=_parse_datetime(
            row.get("execution_date")
            or row.get("timestamp")
            or row.get("created_at")
            or row.get("date")
        ),
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
    )


def _ticker_movement_from_row(
    row: dict[str, Any],
    *,
    source: str,
) -> BrokerMovement | None:
    external_id = row.get("id_ticket") or row.get("id_movement") or row.get("id")
    ticker = str(row.get("instrument_code") or "").upper().strip()
    if external_id in (None, "") or not ticker:
        return None

    movement_type = _movement_type(row)
    quantity = _parse_float(row.get("quantity"))
    amount = _parse_float(row.get("amount"))
    if amount is not None:
        if movement_type == "SELL":
            amount = -abs(amount)
        elif movement_type == "BUY":
            amount = abs(amount)

    return BrokerMovement(
        external_movement_id=str(external_id),
        executed_at=_parse_datetime(row.get("execution_date") or row.get("date")),
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


def serialize_raw_payload(payload: dict[str, Any] | None) -> str:
    return json.dumps(payload or {}, ensure_ascii=True, sort_keys=True)

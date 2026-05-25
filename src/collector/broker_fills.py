from __future__ import annotations

import csv
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo


ART_TZ = ZoneInfo("America/Argentina/Buenos_Aires")


@dataclass(frozen=True)
class BrokerFill:
    external_fill_id: str
    executed_at: datetime
    ticker: str
    side: str
    quantity: float
    avg_fill_price: float
    gross_amount_ars: float | None = None
    fees_ars: float | None = None
    source: str = "manual_import"
    raw_payload: dict[str, Any] | None = None


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=ART_TZ)
    if isinstance(value, (int, float)):
        raw = float(value)
        if raw > 10_000_000_000:
            raw = raw / 1000
        return datetime.fromtimestamp(raw, tz=timezone.utc)

    text = str(value or "").strip()
    if not text:
        raise ValueError("executed_at is required")
    iso = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso)
        return dt if dt.tzinfo else dt.replace(tzinfo=ART_TZ)
    except ValueError:
        pass

    for fmt in (
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%d/%m/%Y",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=ART_TZ)
        except ValueError:
            continue
    raise ValueError(f"invalid executed_at: {text}")


def _parse_float(value: Any, *, required: bool = False) -> float | None:
    if value in (None, ""):
        if required:
            raise ValueError("numeric value is required")
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text:
        if required:
            raise ValueError("numeric value is required")
        return None
    text = re.sub(r"(?i)\b(ars|usd|pesos?)\b", "", text)
    text = text.replace("$", "").replace("%", "").replace("\xa0", " ").strip()
    text = re.sub(r"\s+", "", text)
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        text = text.replace(".", "").replace(",", ".")
    return float(text)


def broker_fill_from_mapping(
    row: dict[str, Any],
    *,
    source: str = "manual_import",
) -> BrokerFill:
    external_fill_id = str(row.get("external_fill_id") or "").strip()
    ticker = str(row.get("ticker") or "").upper().strip()
    side = str(row.get("side") or "").upper().strip()

    if not external_fill_id:
        raise ValueError("external_fill_id is required")
    if not ticker:
        raise ValueError("ticker is required")
    if side not in {"BUY", "SELL"}:
        raise ValueError("side must be BUY or SELL")

    return BrokerFill(
        external_fill_id=external_fill_id,
        executed_at=_parse_datetime(row.get("executed_at")),
        ticker=ticker,
        side=side,
        quantity=float(_parse_float(row.get("quantity"), required=True)),
        avg_fill_price=float(_parse_float(row.get("avg_fill_price"), required=True)),
        gross_amount_ars=_parse_float(row.get("gross_amount_ars")),
        fees_ars=_parse_float(row.get("fees_ars")),
        source=source,
        raw_payload=dict(row),
    )


def broker_fills_from_rows(
    rows: Iterable[dict[str, Any]],
    *,
    source: str = "manual_import",
) -> list[BrokerFill]:
    return [broker_fill_from_mapping(row, source=source) for row in rows]


def load_broker_fills_csv(
    path: str | Path,
    *,
    source: str = "manual_import",
) -> list[BrokerFill]:
    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    return broker_fills_from_rows(rows, source=source)


def serialize_raw_payload(payload: dict[str, Any] | None) -> str:
    return json.dumps(payload or {}, ensure_ascii=True, sort_keys=True)


def _iter_dicts(payload: Any) -> Iterable[dict[str, Any]]:
    if isinstance(payload, dict):
        yield payload
        for value in payload.values():
            yield from _iter_dicts(value)
    elif isinstance(payload, list):
        for item in payload:
            yield from _iter_dicts(item)


def _norm_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def _find_value(obj: Any, names: set[str]) -> Any:
    if isinstance(obj, dict):
        for key, value in obj.items():
            if _norm_key(str(key)) in names and value not in (None, ""):
                return value
        for value in obj.values():
            found = _find_value(value, names)
            if found not in (None, ""):
                return found
    return None


def _text_from_keys(row: dict[str, Any], names: set[str]) -> str:
    values: list[str] = []
    for key, value in row.items():
        if _norm_key(str(key)) in names and value not in (None, ""):
            values.append(str(value))
    return " ".join(values).lower()


def _extract_ticker(row: dict[str, Any]) -> str | None:
    value = _find_value(row, {
        "ticker", "symbol", "simbolo", "especie", "asset", "instrument",
        "security", "securityid", "instrumentid", "instrumentcode", "assetticker",
    })
    if value is None:
        return None
    text = str(value).upper().strip()
    match = re.search(r"\b[A-Z][A-Z0-9.]{1,7}\b", text)
    return match.group(0) if match else None


def _extract_side(row: dict[str, Any]) -> str | None:
    text = _text_from_keys(row, {
        "side", "type", "tipo", "operationtype", "operation", "operacion",
        "action", "orderaction", "ordertype", "label", "description", "detail",
    })
    if any(word in text for word in ("compra", "comprar", "buy", "bought")):
        return "BUY"
    if any(word in text for word in ("venta", "vender", "sell", "sold")):
        return "SELL"
    return None


def _is_executed_fill(row: dict[str, Any]) -> bool:
    status = _text_from_keys(row, {"status", "estado", "state", "orderstatus"})
    if not status:
        return True
    if any(word in status for word in (
        "cancel", "rechaz", "reject", "pend", "open", "abiert", "expir",
        "vencid", "anulad",
    )):
        return False
    return any(word in status for word in (
        "execut", "ejecut", "filled", "fill", "cumpl", "operad", "concert",
        "realiz", "finaliz", "closed", "cerrad",
    ))


def _cocos_external_id(row: dict[str, Any], fallback_payload: dict[str, Any]) -> str:
    value = _find_value(row, {
        "externalfillid", "fillid", "executionid", "id", "orderid",
        "ordenid", "operationid", "operacionid", "transactionid", "idticket",
    })
    if value not in (None, ""):
        return str(value).strip()
    raw = json.dumps(fallback_payload, ensure_ascii=True, sort_keys=True, default=str)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _fill_from_cocos_row(row: dict[str, Any], *, source: str) -> BrokerFill | None:
    if not _is_executed_fill(row):
        return None

    ticker = _extract_ticker(row)
    side = _extract_side(row)
    if not ticker or not side:
        return None

    executed_at_raw = _find_value(row, {
        "executedat", "filledat", "executiondate", "executiondatetime",
        "createdat", "updatedat", "date", "datetime", "fecha", "fechahora",
        "operationdate", "concertationdate",
    })
    quantity_raw = _find_value(row, {
        "quantity", "qty", "cantidad", "nominal", "filledquantity",
        "executedquantity", "shares", "units",
    })
    price_raw = _find_value(row, {
        "avgfillprice", "averageprice", "avgprice", "price", "precio",
        "executionprice", "filledprice", "operatedprice",
    })

    if executed_at_raw is None or quantity_raw is None or price_raw is None:
        return None

    try:
        quantity = _parse_float(quantity_raw, required=True)
        avg_price = _parse_float(price_raw, required=True)
        if not quantity or not avg_price or avg_price <= 0:
            return None
        quantity = abs(quantity)
        gross = _parse_float(_find_value(row, {
            "grossamountars", "grossamount", "amount", "monto", "total",
            "importe", "netamount", "netamountars",
        }))
        fees = _parse_float(_find_value(row, {
            "feesars", "fees", "fee", "commission", "commissions",
            "comision", "derechos", "impuestos",
        }))
        return BrokerFill(
            external_fill_id=_cocos_external_id(row, row),
            executed_at=_parse_datetime(executed_at_raw),
            ticker=ticker,
            side=side,
            quantity=float(quantity),
            avg_fill_price=float(avg_price),
            gross_amount_ars=float(gross) if gross is not None else None,
            fees_ars=float(fees) if fees is not None else None,
            source=source,
            raw_payload=row,
        )
    except Exception:
        return None


def broker_fills_from_cocos_payloads(
    payloads: Iterable[Any],
    *,
    source: str = "cocos_api",
) -> list[BrokerFill]:
    fills_by_key: dict[tuple[str, str], BrokerFill] = {}
    for payload in payloads:
        for row in _iter_dicts(payload):
            fill = _fill_from_cocos_row(row, source=source)
            if fill is None:
                continue
            fills_by_key[(fill.source, fill.external_fill_id)] = fill
    return list(fills_by_key.values())

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


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
    text = str(value or "").strip()
    if not text:
        raise ValueError("executed_at is required")
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def _parse_float(value: Any, *, required: bool = False) -> float | None:
    if value in (None, ""):
        if required:
            raise ValueError("numeric value is required")
        return None
    return float(str(value).replace(",", "."))


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

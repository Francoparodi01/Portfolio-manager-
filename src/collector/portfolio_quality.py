"""Helpers to align portfolio positions with Cocos market data quality."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

ART_TZ = ZoneInfo("America/Argentina/Buenos_Aires")

PRICE_STATUS_FRESH = "FRESH"
PRICE_STATUS_STALE = "STALE"
PRICE_STATUS_MISSING = "MISSING"


def _as_dict(row: Any) -> dict:
    if row is None:
        return {}
    if isinstance(row, dict):
        return dict(row)
    try:
        return dict(row)
    except Exception:
        return {}


def _coerce_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _art_date(value: Any):
    dt = _coerce_datetime(value)
    if dt is None:
        return None
    return dt.astimezone(ART_TZ).date()


def _latest_market_day(latest_market_rows: Iterable[Mapping[str, Any]]) -> object | None:
    dates = [
        _art_date(_as_dict(row).get("ts"))
        for row in latest_market_rows
    ]
    dates = [day for day in dates if day is not None]
    return max(dates) if dates else None


def market_rows_by_ticker(latest_market_rows: Iterable[Mapping[str, Any]]) -> dict[str, dict]:
    rows: dict[str, dict] = {}
    for row in latest_market_rows or []:
        item = _as_dict(row)
        ticker = str(item.get("ticker", "") or "").upper()
        if ticker:
            rows[ticker] = item
    return rows


def enrich_positions_with_market_metadata(
    positions: Iterable[Mapping[str, Any]],
    latest_market_rows: Iterable[Mapping[str, Any]],
) -> list[dict]:
    """Return portfolio positions with Cocos asset type and price freshness metadata.

    A position is operational only if the latest Cocos market price for its ticker
    belongs to the same ART market day as the rest of the latest market universe.
    This prevents old quotes from silently participating in decisions.
    """
    market_rows = list(latest_market_rows or [])
    reference_day = _latest_market_day(market_rows)
    by_ticker = market_rows_by_ticker(market_rows)

    enriched: list[dict] = []
    for raw in positions or []:
        pos = dict(raw)
        ticker = str(pos.get("ticker", "") or "").upper()
        if ticker:
            pos["ticker"] = ticker

        row = by_ticker.get(ticker)
        if not row:
            pos["market_data_status"] = PRICE_STATUS_MISSING
            pos["market_data_reason"] = "sin precio en market_prices"
            pos["is_operable"] = False
            enriched.append(pos)
            continue

        market_asset_type = str(row.get("asset_type", "") or "").upper()
        if market_asset_type:
            pos["asset_type"] = market_asset_type
            pos["asset_type_source"] = "market_prices"

        row_day = _art_date(row.get("ts"))
        row_ts = _coerce_datetime(row.get("ts"))
        if row_ts is not None:
            pos["market_price_ts"] = row_ts.isoformat()

        if row.get("last_price") is not None:
            try:
                pos["market_last_price"] = float(row["last_price"])
            except Exception:
                pass

        if reference_day is None or row_day == reference_day:
            pos["market_data_status"] = PRICE_STATUS_FRESH
            pos["market_data_reason"] = "precio fresco"
            pos["is_operable"] = True
        else:
            pos["market_data_status"] = PRICE_STATUS_STALE
            pos["market_data_reason"] = (
                f"precio desactualizado: {row_day.isoformat() if row_day else 'sin fecha'}"
            )
            pos["is_operable"] = False

        enriched.append(pos)

    return enriched


def is_position_operable(position: Mapping[str, Any]) -> bool:
    status = str(position.get("market_data_status", PRICE_STATUS_FRESH) or "").upper()
    return bool(position.get("is_operable", status == PRICE_STATUS_FRESH))

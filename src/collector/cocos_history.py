from __future__ import annotations

from datetime import datetime, timezone
from src.analysis.contextual_contracts import IDENTITY_FIELDS, finite_number
from urllib.parse import urlparse
from typing import Iterable

from src.collector.data.models import AssetType, Currency, MarketCandle


def parse_history_payload(
    payload: dict,
    *,
    ticker: str,
    long_ticker: str,
    asset_type: AssetType,
    currency: Currency,
    venue: str = "BYMA",
    interval: str = "1d",
) -> list[MarketCandle]:
    """Convierte el payload paralelo de Cocos (`t/o/h/l/c/v`) en velas tipadas."""
    if payload.get("s") != "OK":
        raise ValueError("payload historico de Cocos sin estado OK")

    required = ("t", "o", "h", "l", "c", "v")
    arrays = {key: payload.get(key) or [] for key in required}
    lengths = {key: len(arrays[key]) for key in required}
    if len(set(lengths.values())) != 1:
        raise ValueError(f"payload historico desalineado: {lengths}")

    candles: list[MarketCandle] = []
    for ts, open_, high, low, close, volume in zip(
        arrays["t"],
        arrays["o"],
        arrays["h"],
        arrays["l"],
        arrays["c"],
        arrays["v"],
    ):
        if any(value is None for value in (ts, open_, high, low, close, volume)):
            continue
        try:
            parsed_ts = int(ts)
            parsed_open = float(open_)
            parsed_high = float(high)
            parsed_low = float(low)
            parsed_close = float(close)
            parsed_volume = float(volume)
        except (TypeError, ValueError):
            continue
        candles.append(
            MarketCandle(
                ticker=ticker.upper(),
                long_ticker=long_ticker,
                asset_type=asset_type,
                currency=currency,
                venue=venue,
                interval=interval,
                ts=datetime.fromtimestamp(parsed_ts, tz=timezone.utc),
                open_price=parsed_open,
                high_price=parsed_high,
                low_price=parsed_low,
                close_price=parsed_close,
                volume=parsed_volume,
            )
        )
    return candles


def merge_candle_batches(batches: Iterable[list[MarketCandle]]) -> list[MarketCandle]:
    """Une lotes históricos solapados conservando una vela por timestamp."""
    merged: dict[tuple, MarketCandle] = {}
    for batch in batches:
        for candle in batch:
            key = (candle.ticker, candle.long_ticker, candle.asset_type, candle.currency, candle.venue, candle.interval, candle.ts)
            merged[key] = candle
    return sorted(merged.values(), key=lambda candle: candle.ts)


def asset_type_from_market(market: str) -> AssetType:
    market_name = str(market or "").upper()
    if market_name == "ACCIONES":
        return AssetType.ACCION
    if market_name == "CEDEARS":
        return AssetType.CEDEAR
    raise ValueError(f"market no soportado: {market}")


def long_ticker_from_history_url(url: str) -> str:
    path = urlparse(url).path
    marker = "/api/v1/markets/tickers/"
    if marker not in path or "/historic-data-extended" not in path:
        raise ValueError("url historica de Cocos invalida")
    return path.split(marker, 1)[1].split("/historic-data-extended", 1)[0]


def currency_from_long_ticker(long_ticker: str) -> Currency:
    suffix = str(long_ticker or "").rsplit("-", 1)[-1].upper()
    try:
        return Currency(suffix)
    except ValueError:
        return Currency.ARS


def candles_to_frame(candles):
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError("pandas requerido para convertir velas") from exc

    if not candles:
        frame = pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume", "Source"])
        frame.attrs["candle_sources"] = ()
        frame.attrs["candle_source_counts"] = {}
        frame.attrs["has_reconstructed_candles"] = False
        return frame

    rows = []
    identities = set()
    provider_symbols = set()
    native_symbols = set()
    metadata = {k: set() for k in (
        "volume_unit", "calendar", "calendar_validation",
        "adjustment_policy", "depositary_ratio",
    )}
    for candle in candles:
        if isinstance(candle, dict):
            get = candle.get
        else:
            get = lambda name: getattr(candle, name, None)
        identity = tuple(getattr(get(name), "value", get(name)) for name in IDENTITY_FIELDS)
        identities.add(identity)
        for name, values in metadata.items():
            value = get(name)
            values.add(str(value) if value is not None else None)
        symbol = get("long_ticker")
        if symbol:
            provider_symbols.add(symbol)
            if not str(symbol).startswith(("INTERNAL:", "TV:", "YAHOO:")):
                native_symbols.add(symbol)
        rows.append(
            {
                "ts": get("ts"),
                "Open": float(get("open_price")),
                "High": float(get("high_price")),
                "Low": float(get("low_price")),
                "Close": float(get("close_price")),
                "Volume": float(get("volume")) if get("volume") is not None else None,
                "BarStart": get("bar_start"), "BarEnd": get("bar_end"),
                "AvailableAt": get("available_at"), "RetrievedAt": get("scraped_at"),
                "IsClosed": get("is_closed"),
                "Source": str(get("source") or "UNKNOWN"),
                "ProviderSymbol": get("long_ticker"),
            }
        )

    if len(identities) != 1 or len(native_symbols) > 1:
        raise ValueError("AMBIGUOUS_SERIES_IDENTITY: filter instrument, currency, venue and interval")
    if any(len(values - {None}) > 1 for values in metadata.values()):
        raise ValueError("INCOMPATIBLE_SERIES_UNITS_OR_ADJUSTMENTS")
    series_identity = dict(zip(IDENTITY_FIELDS, next(iter(identities))))
    series_identity.update({k: next(iter(v)) if len(v) == 1 else None for k, v in metadata.items()})
    series_identity["instrument_id"] = ":".join(str(series_identity[k]) for k in ("venue", "asset_type", "ticker", "currency")) if all(series_identity.get(k) for k in ("venue", "asset_type", "ticker", "currency")) else None
    frame = pd.DataFrame(rows)
    frame["ts"] = pd.to_datetime(frame["ts"], utc=True)
    frame["candle_day"] = frame["ts"].dt.date if series_identity.get("interval") in (None, "1d") else frame["ts"]
    frame["source_priority"] = frame["Source"].map(
        {
            "COCOS": 0,
            "TRADINGVIEW_BYMA": 1,
            "internal_snapshot": 2,
        }
    ).fillna(3)
    frame = (
        frame.sort_values(["candle_day", "source_priority", "ts"])
        .drop_duplicates(subset=["candle_day"], keep="first")
        .drop(columns=["candle_day", "source_priority"])
        .set_index("ts")
        .sort_index()
    )
    frame.attrs["series_identity"] = series_identity
    frame.attrs["calendar_validation"] = series_identity.pop("calendar_validation", None)
    frame.attrs["provider_symbols"] = sorted(provider_symbols)
    frame.attrs["duplicate_rows_resolved"] = len(rows) - len(frame)
    frame.attrs["selection_policy"] = "source_priority_v1"
    sources = tuple(sorted(set(frame["Source"])))
    source_counts = {
        str(source): int(count)
        for source, count in frame["Source"].value_counts().sort_index().items()
    }
    frame.attrs["candle_sources"] = sources
    frame.attrs["candle_source_counts"] = source_counts
    frame.attrs["has_reconstructed_candles"] = "internal_snapshot" in sources
    return frame


def overlay_compatible_volume(
    primary_frame,
    volume_frame,
    *,
    source: str = "TRADINGVIEW_BYMA",
    max_close_difference: float = 0.05,
):
    """Fill missing volume without replacing canonical OHLC values.

    The overlay is accepted only for the same local-market session and when the
    two closes agree within the configured tolerance. This rejects adjusted or
    mismatched series while preserving an auditable volume-source summary.
    """
    if primary_frame is None or primary_frame.empty:
        return primary_frame

    result = primary_frame.copy()
    result.attrs = dict(getattr(primary_frame, "attrs", {}) or {})
    primary_sources = result.get("Source")
    volume_source_by_day = {
        index.date(): str(primary_sources.loc[index] if primary_sources is not None else "UNKNOWN")
        for index in result.index
    }
    accepted = 0
    rejected_price_mismatch = 0
    rejected_missing_match = 0

    if volume_frame is not None and not volume_frame.empty:
        left = result.attrs.get("series_identity", {})
        right = volume_frame.attrs.get("series_identity", {})
        compatible = all(left.get(k) and right.get(k) and left[k] == right[k]
                         for k in ("ticker", "asset_type", "currency", "venue", "interval"))
        if not compatible:
            result.attrs["volume_overlay_rejected_identity"] = True
            return result
        volume_by_day = {
            index.date(): row
            for index, row in volume_frame.sort_index().iterrows()
        }
        for index, row in result.iterrows():
            current_volume = float(row.get("Volume") or 0.0)
            if finite_number(current_volume) and current_volume > 0:
                continue
            fallback = volume_by_day.get(index.date())
            if fallback is None:
                rejected_missing_match += 1
                continue
            fallback_volume = float(fallback.get("Volume") or 0.0)
            primary_close = float(row.get("Close") or 0.0)
            fallback_close = float(fallback.get("Close") or 0.0)
            if not all(finite_number(v) and v > 0 for v in (fallback_volume, primary_close, fallback_close)):
                rejected_missing_match += 1
                continue
            close_difference = abs((fallback_close / primary_close) - 1.0)
            if close_difference > float(max_close_difference):
                rejected_price_mismatch += 1
                continue
            result.at[index, "Volume"] = fallback_volume
            volume_source_by_day[index.date()] = source
            accepted += 1

    source_counts: dict[str, int] = {}
    for value in volume_source_by_day.values():
        source_counts[value] = source_counts.get(value, 0) + 1
    result.attrs["volume_source_counts"] = source_counts
    result.attrs["volume_overlay_source"] = source
    result.attrs["volume_overlay_rows"] = accepted
    result.attrs["volume_overlay_max_close_difference"] = float(max_close_difference)
    result.attrs["volume_overlay_rejected_price_mismatch"] = rejected_price_mismatch
    result.attrs["volume_overlay_rejected_missing_match"] = rejected_missing_match
    return result

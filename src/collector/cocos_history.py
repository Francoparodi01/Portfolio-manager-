from __future__ import annotations

from datetime import datetime, timezone
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
    merged: dict[tuple[str, str, datetime], MarketCandle] = {}
    for batch in batches:
        for candle in batch:
            key = (candle.long_ticker, candle.interval, candle.ts)
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
    for candle in candles:
        if isinstance(candle, dict):
            get = candle.get
        else:
            get = lambda name: getattr(candle, name)
        rows.append(
            {
                "ts": get("ts"),
                "Open": float(get("open_price")),
                "High": float(get("high_price")),
                "Low": float(get("low_price")),
                "Close": float(get("close_price")),
                "Volume": float(get("volume")),
                "Source": str(get("source") or "UNKNOWN"),
            }
        )

    frame = pd.DataFrame(rows)
    frame["ts"] = pd.to_datetime(frame["ts"], utc=True)
    frame["candle_day"] = frame["ts"].dt.date
    frame["source_priority"] = frame["Source"].map({"COCOS": 0}).fillna(1)
    frame = (
        frame.sort_values(["candle_day", "source_priority", "ts"])
        .drop_duplicates(subset=["candle_day"], keep="first")
        .drop(columns=["candle_day", "source_priority"])
        .set_index("ts")
        .sort_index()
    )
    sources = tuple(sorted(set(frame["Source"])))
    source_counts = {
        str(source): int(count)
        for source, count in frame["Source"].value_counts().sort_index().items()
    }
    frame.attrs["candle_sources"] = sources
    frame.attrs["candle_source_counts"] = source_counts
    frame.attrs["has_reconstructed_candles"] = "internal_snapshot" in sources
    return frame

from datetime import datetime, timezone

import pytest

from src.collector.cocos_history import merge_candle_batches, parse_history_payload
from src.collector.cocos_history import (
    asset_type_from_market,
    candles_to_frame,
    currency_from_long_ticker,
    long_ticker_from_history_url,
)
from src.collector.data.models import AssetType, Currency


def _payload():
    return {
        "s": "OK",
        "t": [1740355200, 1740441600],
        "o": [7250, 7000],
        "h": [7310, 7140],
        "l": [6970, 6830],
        "c": [7070, 6990],
        "v": [1739731, 1845095],
    }


def test_parse_history_payload_builds_local_candles():
    candles = parse_history_payload(
        _payload(),
        ticker="GGAL",
        long_ticker="GGAL-0002-C-CT-ARS",
        asset_type=AssetType.ACCION,
        currency=Currency.ARS,
    )

    assert len(candles) == 2
    assert candles[0].ticker == "GGAL"
    assert candles[0].asset_type == AssetType.ACCION
    assert candles[0].ts == datetime(2025, 2, 24, tzinfo=timezone.utc)
    assert candles[0].close_price == 7070.0


def test_parse_history_payload_rejects_misaligned_arrays():
    payload = _payload()
    payload["v"] = [1739731]

    with pytest.raises(ValueError, match="desalineado"):
        parse_history_payload(
            payload,
            ticker="T",
            long_ticker="T-0002-C-CT-ARS",
            asset_type=AssetType.CEDEAR,
            currency=Currency.ARS,
        )


def test_merge_candle_batches_deduplicates_overlap():
    batch = parse_history_payload(
        _payload(),
        ticker="T",
        long_ticker="T-0002-C-CT-ARS",
        asset_type=AssetType.CEDEAR,
        currency=Currency.ARS,
    )

    merged = merge_candle_batches([batch, [batch[-1]]])

    assert len(merged) == 2
    assert merged[-1].ts == batch[-1].ts


def test_history_helpers_keep_market_identity_explicit():
    url = (
        "https://api.cocos.capital/api/v1/markets/tickers/"
        "GGAL-0002-C-CT-ARS/historic-data-extended?id_venue=BYMA"
    )

    assert asset_type_from_market("ACCIONES") == AssetType.ACCION
    assert asset_type_from_market("CEDEARS") == AssetType.CEDEAR
    assert long_ticker_from_history_url(url) == "GGAL-0002-C-CT-ARS"
    assert currency_from_long_ticker("GGAL-0002-C-CT-ARS") == Currency.ARS


def test_candles_to_frame_preserves_ohlcv_columns():
    candles = parse_history_payload(
        _payload(),
        ticker="T",
        long_ticker="T-0002-C-CT-ARS",
        asset_type=AssetType.CEDEAR,
        currency=Currency.ARS,
    )

    frame = candles_to_frame(candles)

    assert list(frame.columns) == ["Open", "High", "Low", "Close", "Volume"]
    assert frame.iloc[-1]["Close"] == 6990.0

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd

from src.analysis.radar_setup_shadow import (
    RADAR_SETUP_SHADOW_VERSION,
    build_radar_setup_shadow_universe,
    resolve_setup_event,
)


def _frame(prices, *, volumes=None):
    prices = list(prices)
    volumes = list(volumes) if volumes is not None else [1_000_000.0] * len(prices)
    index = pd.date_range("2025-01-01", periods=len(prices), freq="B", tz="UTC")
    return pd.DataFrame(
        {
            "Open": prices,
            "High": [price * 1.01 for price in prices],
            "Low": [price * 0.99 for price in prices],
            "Close": prices,
            "Volume": volumes,
        },
        index=index,
    )


def test_shadow_components_are_bounded_ranked_and_non_operational():
    sessions = 240
    strong = [100 + index * 0.40 for index in range(sessions)]
    weak = [150 - index * 0.20 for index in range(sessions)]
    flat = [100 + index * 0.05 for index in range(sessions)]
    result = build_radar_setup_shadow_universe(
        tickers=["STRONG", "WEAK", "QQQ", "SPY"],
        history_frames={
            "STRONG": _frame(strong),
            "WEAK": _frame(weak),
            "QQQ": _frame(flat),
            "SPY": _frame(flat),
        },
        asset_types={"STRONG": "CEDEAR", "WEAK": "CEDEAR"},
    )

    assert set(result) == {"STRONG", "WEAK"}
    assert result["STRONG"].relative_strength_component_score > result["WEAK"].relative_strength_component_score
    assert result["STRONG"].discovery_percentile == 1.0
    assert result["WEAK"].discovery_percentile == 0.0
    for measurement in result.values():
        assert measurement.version == RADAR_SETUP_SHADOW_VERSION
        for value in (
            measurement.trend_component_score,
            measurement.relative_strength_component_score,
            measurement.compression_component_score,
            measurement.setup_component_score,
        ):
            assert value is not None
            assert 0.0 <= value <= 25.0
        payload = measurement.to_dict()
        assert payload["affects_radar_ranking"] is False
        assert payload["affects_analysis"] is False
        assert payload["affects_execution"] is False
        assert "cedear_ccl_not_separated" in measurement.warnings


def test_missing_volume_never_earns_compression_points():
    sessions = 240
    prices = [100 + index * 0.02 for index in range(sessions)]
    contracting_volume = [1_000_000.0] * (sessions - 5) + [300_000.0] * 5
    missing_volume = [0.0] * sessions
    result = build_radar_setup_shadow_universe(
        tickers=["VALID", "MISSING", "QQQ", "SPY"],
        history_frames={
            "VALID": _frame(prices, volumes=contracting_volume),
            "MISSING": _frame(prices, volumes=missing_volume),
            "QQQ": _frame(prices),
            "SPY": _frame(prices),
        },
        asset_types={"VALID": "CEDEAR", "MISSING": "CEDEAR"},
    )

    assert result["VALID"].compression_component_score > result["MISSING"].compression_component_score
    assert result["MISSING"].feature_quality_flag == "PARTIAL"
    assert "volume_quality_below_80pct" in result["MISSING"].warnings
    assert "volume_contraction_ratio" not in result["MISSING"].features


def test_setup_event_requires_unambiguous_first_touch():
    reference = datetime(2026, 8, 20, tzinfo=timezone.utc)
    candles = [
        {
            "ts": reference + timedelta(days=1),
            "high_price": 105.0,
            "low_price": 99.0,
            "close_price": 103.0,
            "volume": 2_000_000,
        }
    ]
    triggered = resolve_setup_event(
        reference_ts=reference,
        reference_price=100.0,
        readiness_state="PRE_BREAKOUT",
        trigger_price=104.0,
        invalidation_price=95.0,
        candles=candles,
    )
    assert triggered is not None
    assert triggered["event_status"] == "TRIGGERED_AFTER_DISCOVERY"
    assert triggered["event_price"] == 104.0
    assert triggered["sessions_from_discovery"] == 1

    ambiguous = resolve_setup_event(
        reference_ts=reference,
        reference_price=100.0,
        readiness_state="PRE_BREAKOUT",
        trigger_price=104.0,
        invalidation_price=99.5,
        candles=candles,
    )
    assert ambiguous is not None
    assert ambiguous["event_status"] == "AMBIGUOUS_SAME_SESSION"
    assert ambiguous["event_price"] is None


def test_setup_event_waits_for_full_window_before_expiry():
    reference = datetime(2026, 8, 20, tzinfo=timezone.utc)
    partial = [
        {
            "ts": reference + timedelta(days=index),
            "high_price": 101.0,
            "low_price": 99.0,
            "close_price": 100.0,
        }
        for index in range(1, 10)
    ]
    kwargs = {
        "reference_ts": reference,
        "reference_price": 100.0,
        "readiness_state": "WATCH",
        "trigger_price": 110.0,
        "invalidation_price": 90.0,
    }
    assert resolve_setup_event(candles=partial, **kwargs) is None
    expired = resolve_setup_event(
        candles=partial + [{
            "ts": reference + timedelta(days=10),
            "high_price": 101.0,
            "low_price": 99.0,
            "close_price": 100.0,
        }],
        **kwargs,
    )
    assert expired is not None
    assert expired["event_status"] == "EXPIRED_NO_TRIGGER"

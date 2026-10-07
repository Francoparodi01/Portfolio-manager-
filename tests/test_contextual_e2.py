from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timezone
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from scripts.run_analysis import _layers_payload_for_decision
from src.analysis.contextual_market import (
    ContextIdentityError,
    attach_contextual_shadow,
    breakout_context,
    build_contextual_snapshot,
    market_structure,
    relative_strength,
    render_contextual_diagnostic,
    rvol_context,
    weekly_bars,
)
from src.analysis.execution_planner import (
    PositionSnapshot,
    build_signals_from_synthesis,
    derive_decision_intents,
    reconcile_funding,
)
from src.analysis.synthesis import blend_scores


UTC = timezone.utc


def frame(*, ticker="TEST", currency="ARS", periods=260, slope=.15,
          start="2025-01-02", volume=100.0):
    index = pd.bdate_range(start, periods=periods, tz="UTC") + pd.Timedelta(hours=14)
    steps = np.arange(periods, dtype=float)
    close = 100 + steps * slope + np.sin(steps / 4) * 2
    result = pd.DataFrame({
        "Open": close - .2,
        "High": close + 1.0,
        "Low": close - 1.0,
        "Close": close,
        "Volume": np.full(periods, volume, dtype=float),
        "Source": "CONTROLLED_TEST",
        "ProviderSymbol": f"{ticker}-PROVIDER",
        "BarStart": index,
        "BarEnd": index + pd.Timedelta(hours=6),
        "AvailableAt": index + pd.Timedelta(hours=6, minutes=5),
        "RetrievedAt": index + pd.Timedelta(hours=6, minutes=10),
        "IsClosed": True,
    }, index=index)
    result.attrs["series_identity"] = {
        "ticker": ticker, "asset_type": "CEDEAR", "currency": currency,
        "venue": "BYMA", "interval": "1d", "volume_unit": "units",
        "calendar": "CONTROLLED_BDAYS_V1", "adjustment_policy": "unadjusted",
        "depositary_ratio": "1:1", "instrument_id": f"BYMA:CEDEAR:{ticker}:{currency}",
    }
    result.attrs["calendar_validation"] = "controlled_complete_v1"
    result.attrs["candle_sources"] = ("CONTROLLED_TEST",)
    result.attrs["candle_source_counts"] = {"CONTROLLED_TEST": periods}
    result.attrs["provider_symbols"] = [f"{ticker}-PROVIDER"]
    result.attrs["selection_policy"] = "controlled_unique_v1"
    return result


def cutoff_for(value):
    return value.index[-1] + pd.Timedelta(hours=7)


def test_rvol_uses_previous_equivalent_window_and_excludes_current():
    value = frame(periods=25)
    value.loc[value.index[-1], "Volume"] = 200.0
    context = rvol_context(value)
    assert context["current_volume"] == 200.0
    assert context["expected_volume"] == 100.0
    assert context["rvol"] == 2.0
    assert context["state"] == "EXPANSION"
    assert "previous_20" in context["definition"]


def test_daily_structure_uses_confirmed_higher_highs_and_higher_lows():
    index = pd.date_range("2026-01-01", periods=14, tz="UTC")
    highs = [10, 12, 15, 12, 11, 14, 18, 15, 13, 17, 21, 18, 17, 16]
    lows = [9, 8, 7, 5, 7, 9, 8, 6, 8, 10, 9, 7, 9, 11]
    close = [(high + low) / 2 for high, low in zip(highs, lows)]
    value = pd.DataFrame({"Open": close, "High": highs, "Low": lows,
                          "Close": close, "Volume": 100}, index=index)
    context = market_structure(value)
    assert context["structure"] == "HH_HL"
    assert context["trend"] == "UPTREND"
    assert context["confirmed_swing_highs"][-1]["value"] == 21
    assert context["confirmed_swing_lows"][-1]["value"] == 7


def test_weekly_structure_excludes_incomplete_week_and_is_deterministic():
    value = frame(periods=260)
    cutoff = cutoff_for(value)
    weekly = weekly_bars(value, cutoff)
    assert all(index <= cutoff for index in weekly.index)
    first = market_structure(weekly)
    second = market_structure(weekly.copy())
    assert first == second
    assert first["trend"] == "UPTREND"
    assert first["structure"] == "HH_HL"


def test_relative_strength_multiple_windows_has_no_future_input():
    asset = frame(ticker="ASSET", slope=.25)
    benchmark = frame(ticker="SPY", slope=.08)
    result = relative_strength(asset, benchmark, benchmark_name="SPY")
    assert result["status"] == "VALID"
    assert set(result["windows"]) == {"20", "60", "120"}
    assert all(result["windows"][window]["relative_price_change"] > 0 for window in result["windows"])
    assert all(result["windows"][window]["excess_return"] > 0 for window in result["windows"])


def test_snapshot_is_no_lookahead_when_future_bars_are_present():
    asset = frame(ticker="ASSET", periods=180)
    benchmark = frame(ticker="SPY", periods=180, slope=.08)
    cutoff = cutoff_for(asset)
    baseline = build_contextual_snapshot(asset, cutoff=cutoff, signal_action="BUY",
                                         benchmarks={"SPY": benchmark})
    future_asset = frame(ticker="ASSET", periods=185)
    future_benchmark = frame(ticker="SPY", periods=185, slope=.08)
    replay = build_contextual_snapshot(future_asset, cutoff=cutoff, signal_action="BUY",
                                       benchmarks={"SPY": future_benchmark})
    assert replay.snapshot_id == baseline.snapshot_id
    assert replay.components == baseline.components
    assert replay.input_digests == baseline.input_digests


def test_cutoff_excludes_bar_that_was_not_available_at_decision():
    asset = frame(ticker="ASSET", periods=180)
    benchmark = frame(ticker="SPY", periods=180, slope=.08)
    cutoff = cutoff_for(asset)
    expected_last = asset.index[-2]
    asset.loc[asset.index[-1], "AvailableAt"] = cutoff + pd.Timedelta(minutes=1)
    snapshot = build_contextual_snapshot(asset, cutoff=cutoff, signal_action="HOLD",
                                         benchmarks={"SPY": benchmark})
    assert snapshot.timestamps["last_asset_bar"] == expected_last.isoformat()
    assert snapshot.timestamps["last_available_at"] <= snapshot.cutoff


def test_missing_volume_remains_unknown_and_cannot_confirm_buy():
    asset = frame(ticker="ASSET", periods=180)
    benchmark = frame(ticker="SPY", periods=180, slope=.08)
    asset.loc[asset.index[-1], "Volume"] = np.nan
    snapshot = build_contextual_snapshot(asset, cutoff=cutoff_for(asset), signal_action="BUY",
                                         benchmarks={"SPY": benchmark})
    assert snapshot.components["rvol"] is None
    assert snapshot.components["volume_state"] == "UNKNOWN"
    invalidator = next(item for item in snapshot.invalidators if item["code"] == "VOLUME_CONFIRMATION")
    assert invalidator["status"] == "UNKNOWN"


def test_wrong_instrument_and_incompatible_benchmark_are_rejected_or_unknown():
    asset = frame(ticker="ASSET")
    missing_identity = asset.copy()
    missing_identity.attrs = deepcopy(asset.attrs)
    del missing_identity.attrs["series_identity"]["venue"]
    with pytest.raises(ContextIdentityError, match="MISSING_SERIES_IDENTITY"):
        build_contextual_snapshot(missing_identity, cutoff=cutoff_for(asset), signal_action="HOLD")
    benchmark = frame(ticker="SPY", currency="USD")
    result = relative_strength(asset, benchmark, benchmark_name="SPY")
    assert result["status"] == "UNKNOWN"
    assert result["reason"] == "INCOMPATIBLE_BENCHMARK_CURRENCY"


def test_naive_timestamps_fail_and_snapshot_preserves_auditable_times():
    asset = frame(ticker="ASSET")
    with pytest.raises(ValueError, match="TZ_AWARE_CUTOFF"):
        build_contextual_snapshot(asset, cutoff=datetime(2026, 1, 1), signal_action="HOLD")
    snapshot = build_contextual_snapshot(asset, cutoff=cutoff_for(asset), signal_action="HOLD")
    assert pd.Timestamp(snapshot.timestamps["last_asset_bar"]) <= pd.Timestamp(snapshot.cutoff)
    assert snapshot.sources["provider_symbols"] == ["ASSET-PROVIDER"]
    assert snapshot.identity["instrument_id"] == "BYMA:CEDEAR:ASSET:ARS"


def test_breakout_without_volume_is_a_shadow_fail():
    asset = frame(ticker="ASSET", periods=180)
    benchmark = frame(ticker="SPY", periods=180, slope=.08)
    previous_high = asset.High.iloc[-21:-1].max()
    asset.loc[asset.index[-1], ["Open", "High", "Low", "Close", "Volume"]] = [
        previous_high + .5, previous_high + 2, previous_high, previous_high + 1, 50,
    ]
    volume = rvol_context(asset)
    breakout = breakout_context(asset, volume)
    assert breakout["state"] == "BREAKOUT_WITHOUT_VOLUME"
    snapshot = build_contextual_snapshot(asset, cutoff=cutoff_for(asset), signal_action="BUY",
                                         benchmarks={"SPY": benchmark})
    invalidator = next(item for item in snapshot.invalidators if item["code"] == "BREAKOUT_VOLUME")
    assert invalidator == {
        "code": "BREAKOUT_VOLUME", "status": "FAIL",
        "reason": "BREAKOUT_WITHOUT_VOLUME", "observed": "BREAKOUT_WITHOUT_VOLUME",
    }
    volume_invalidator = next(item for item in snapshot.invalidators if item["code"] == "VOLUME_CONFIRMATION")
    assert volume_invalidator["status"] == "WARN"
    assert snapshot.affects_analysis is False and snapshot.affects_execution is False


def test_snapshot_is_versioned_hashed_and_enters_feature_v3_only_when_attached():
    asset = frame(ticker="ASSET")
    benchmark = frame(ticker="SPY", slope=.08)
    snapshot = build_contextual_snapshot(asset, cutoff=cutoff_for(asset), signal_action="HOLD",
                                         benchmarks={"SPY": benchmark})
    result = blend_scores("ASSET", "HOLD", .1, .5, {"risk_level": "NORMAL"}, -.2, -1.2)
    plain = _layers_payload_for_decision(result)
    assert "contextual_market_shadow" not in plain
    attach_contextual_shadow(result, snapshot)
    layers = _layers_payload_for_decision(result)
    assert layers["contextual_market_shadow"]["snapshot_id"].startswith("context:")
    assert layers["feature_snapshot"]["schema_version"] == "feature_snapshot_v3"
    assert layers["feature_snapshot"]["payload"]["contextual_market_shadow"]["mode"] == "SHADOW_ONLY"


def test_shadow_snapshot_does_not_change_scores_signals_orders_quantities_or_cash():
    asset = frame(ticker="TEST")
    benchmark = frame(ticker="SPY", slope=.08)
    result = blend_scores("TEST", "HOLD", .1, .5, {"risk_level": "NORMAL"}, -.2, -1.2)
    control = deepcopy(result)
    before_signal = (control.final_score, control.decision, control.conviction)
    snapshot = build_contextual_snapshot(asset, cutoff=cutoff_for(asset), signal_action=result.decision,
                                         benchmarks={"SPY": benchmark})
    attach_contextual_shadow(result, snapshot)
    after_signal = (result.final_score, result.decision, result.conviction)
    assert after_signal == before_signal == (.09, "HOLD", .3333)

    report = SimpleNamespace(trades=[SimpleNamespace(ticker="TEST", weight_current=.1, weight_optimal=.2)])
    positions = {"TEST": PositionSnapshot("TEST", 100, 1000, 100000, .1)}
    baseline_decisions = derive_decision_intents(
        report, build_signals_from_synthesis([control]), positions, 1_000_000, "NORMAL",
    )
    shadow_decisions = derive_decision_intents(
        report, build_signals_from_synthesis([result]), positions, 1_000_000, "NORMAL",
    )
    baseline_plan = reconcile_funding(baseline_decisions, positions, 200_000, 1_000_000, "NORMAL")
    shadow_plan = reconcile_funding(shadow_decisions, positions, 200_000, 1_000_000, "NORMAL")
    assert [order.action for order in shadow_plan.buy_orders] == [order.action for order in baseline_plan.buy_orders]
    assert [order.quantity_est for order in shadow_plan.buy_orders] == [order.quantity_est for order in baseline_plan.buy_orders]
    assert shadow_plan.cash_after == baseline_plan.cash_after
    assert asdict(shadow_plan) == asdict(baseline_plan)


def test_contextual_diagnostic_contains_price_volume_and_levels(tmp_path):
    asset = frame(ticker="ASSET")
    benchmark = frame(ticker="SPY", slope=.08)
    snapshot = build_contextual_snapshot(asset, cutoff=cutoff_for(asset), signal_action="HOLD",
                                         benchmarks={"SPY": benchmark})
    output = render_contextual_diagnostic(asset, snapshot, tmp_path / "diagnostic.png")
    assert output.exists()
    assert output.read_bytes().startswith(b"\x89PNG")
    assert output.stat().st_size > 10_000

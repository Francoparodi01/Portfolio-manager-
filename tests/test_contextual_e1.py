from copy import deepcopy
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from src.analysis.contextual_contracts import RebalanceAuthorization, evaluate_authority, frame_quality
from src.analysis.execution_planner import (
    _buy_guard, build_signals_from_synthesis, derive_decision_intents,
    reconcile_funding, PositionSnapshot,
)
from src.analysis.enums import DecisionType
from src.analysis.synthesis import blend_scores
from src.analysis.technical import analyze_ticker_from_frame, compute_indicators
from src.collector.cocos_history import candles_to_frame, overlay_compatible_volume
from scripts.run_analysis import _layers_payload_for_decision


def frame():
    index = pd.date_range("2025-01-01", periods=260, tz="UTC")
    close = 100 + np.arange(260) * .1 + np.sin(np.arange(260))
    f = pd.DataFrame({"Open": close, "High": close+2, "Low": close-2,
                      "Close": close, "Volume": 100., "Source": "COCOS"}, index=index)
    f.attrs["series_identity"] = dict(ticker="TEST", asset_type="CEDEAR", currency="ARS",
        venue="BYMA", interval="1d", volume_unit="units", calendar="fixture-calendar-v1",
        adjustment_policy="fixture-unadjusted", depositary_ratio="10:1")
    f["BarStart"] = index
    f["BarEnd"] = index + pd.Timedelta(hours=8)
    f["AvailableAt"] = index + pd.Timedelta(hours=9)
    f["IsClosed"] = True
    f.attrs["cutoff"] = index[-1] + pd.Timedelta(days=1)
    f.attrs["calendar_validation"] = "synthetic-complete-calendar"
    return f


@pytest.mark.parametrize("score", [None, float("nan"), float("inf"), -float("inf"), 1.1, -1.1])
def test_nonfinite_missing_and_out_of_range_scores_cannot_buy(score):
    assert _buy_guard(score, .1, .2, 100000)[0] == DecisionType.BLOCKED


@pytest.mark.parametrize("value", [0., None, float("nan"), float("inf"), -1.])
def test_invalid_volume_never_becomes_neutral_one_or_stale_last(value):
    f = frame()
    f.loc[f.index[-1], "Volume"] = value
    indicators = compute_indicators(f, "TEST")
    assert indicators.vol_ratio is None
    signal = analyze_ticker_from_frame("TEST", f)
    assert signal.data_quality["volume_status"] == "PARTIAL"
    assert signal.technical_buy_shadow_v3["volume_quality_20"] == .95


def test_valid_inclusive_volume_ratio_preserved_and_price_invalid_rejected():
    f = frame()
    f.loc[f.index[-1], "Volume"] = 200
    assert compute_indicators(f, "TEST").vol_ratio == pytest.approx(200/105)
    f.loc[f.index[-1], "High"] = 1
    assert compute_indicators(f, "TEST") is None
    assert "IMPOSSIBLE_OHLC" in frame_quality(f)["price_reasons"]


def test_temporal_partial_duplicate_and_missing_metadata_are_explicit():
    f = frame()
    assert frame_quality(f, cutoff=f.attrs["cutoff"])["provenance_status"] == "VALID"
    f.loc[f.index[-1], "AvailableAt"] = f.attrs["cutoff"] + pd.Timedelta(days=1)
    f.loc[f.index[-1], "IsClosed"] = False
    q = frame_quality(f, cutoff=f.attrs["cutoff"])
    assert "AFTER_CUTOFF_AVAILABLEAT" in q["provenance_reasons"]
    assert "PARTIAL_SESSION" in q["provenance_reasons"]
    assert "DUPLICATE_BAR" in frame_quality(pd.concat([f, f.tail(1)]))["price_reasons"]
    f.attrs.clear()
    assert "UNKNOWN_CURRENCY" in frame_quality(f)["provenance_reasons"]


def candle(**overrides):
    row = dict(ticker="TEST", long_ticker="TEST-0002-C-CT-ARS", asset_type="CEDEAR",
               currency="ARS", venue="BYMA", interval="1d", ts=datetime(2025, 1, 1, tzinfo=timezone.utc),
               open_price=100, high_price=102, low_price=98, close_price=100, volume=None, source="COCOS")
    return {**row, **overrides}


@pytest.mark.parametrize("change", [dict(currency="USD"), dict(venue="NYSE"),
    dict(asset_type="EQUITY"), dict(ticker="OTHER"), dict(interval="1h"), dict(long_ticker="TEST-0001-C-CT-ARS")])
def test_currency_market_instrument_and_settlement_never_silently_merge(change):
    with pytest.raises(ValueError, match="AMBIGUOUS_SERIES_IDENTITY"):
        candles_to_frame([candle(), candle(**change)])


def test_valid_provider_aliases_preserve_identity_missing_volume_and_provenance():
    f = candles_to_frame([candle(), candle(long_ticker="TV:BYMA:TEST", source="TRADINGVIEW_BYMA")])
    assert f.attrs["series_identity"]["currency"] == "ARS"
    assert len(f.attrs["provider_symbols"]) == 2
    assert f.Volume.isna().all()
    assert f.AvailableAt.isna().all()
    other = candles_to_frame([candle(currency="USD", volume=100)])
    assert overlay_compatible_volume(f, other).attrs["volume_overlay_rejected_identity"]


def policy_args():
    return dict(signal_action="HOLD", current_weight=.1, target_weight=.2,
        data_quality=frame_quality(frame(), cutoff=frame().attrs["cutoff"]),
        owner_chat_id=123, run_id="run-one", ticker="TEST", cutoff=datetime(2026, 1, 1, tzinfo=timezone.utc))


def test_hold_requires_independent_scoped_reasoned_authorization():
    args = policy_args()
    a = RebalanceAuthorization("auth-1", 123, "run-one", "TEST", "Scheduled rebalance", .2,
                               args["cutoff"]+timedelta(days=1))
    assert evaluate_authority(**args)["eligibility"] == "BLOCKED"
    assert evaluate_authority(**args, authorization=a)["eligibility"] == "ALLOWED"
    for bad in (replace(a, owner_chat_id=124), replace(a, run_id="other"), replace(a, reason=" "),
                replace(a, max_target_weight=.15), replace(a, authority="optimizer"),
                replace(a, expires_at=args["cutoff"]-timedelta(days=1))):
        assert evaluate_authority(**args, authorization=bad)["eligibility"] == "BLOCKED"
    assert evaluate_authority(**{**args, "data_quality": {}}, authorization=a)["eligibility"] == "BLOCKED"


def test_favorable_thesis_survives_reduction_by_concentration():
    result = evaluate_authority(**{**policy_args(), "asset_view": "FAVORABLE",
        "current_weight": .4, "target_weight": .2, "action_reason": "CONCENTRATION"})
    assert result["asset_view"] == "FAVORABLE"
    assert result["portfolio_intent"] == "DECREASE"
    assert result["action_reason"] == "CONCENTRATION"


def test_hold_009_03333_fundable_baseline_is_unchanged_by_shadow():
    # Synthetic generic asset: no ticker-specific thresholds.
    synthesis = blend_scores("TEST", "HOLD", .1, .5, {"risk_level": "NORMAL"}, -.2, -1.2)
    assert synthesis.decision == "HOLD"
    assert synthesis.final_score == .09
    assert synthesis.conviction == .3333
    signals = build_signals_from_synthesis([synthesis])
    report = SimpleNamespace(trades=[SimpleNamespace(ticker="TEST", weight_current=.1, weight_optimal=.2)])
    positions = {"TEST": PositionSnapshot("TEST", 100, 1000, 100000, .1)}
    decisions = derive_decision_intents(report, signals, positions, 1000000, "NORMAL")
    assert decisions[0].action == DecisionType.BUY
    assert "HOLD_INCREASE_REQUIRES_INDEPENDENT_REBALANCE" in decisions[0].authority_shadow["reason_codes"]
    baseline = deepcopy(decisions)
    for d in baseline:
        d.authority_shadow = {}
    plan = reconcile_funding(decisions, positions, 200000, 1000000, "NORMAL")
    control = reconcile_funding(baseline, positions, 200000, 1000000, "NORMAL")
    assert plan.buy_orders and plan.cash_after >= 0
    got = asdict(plan)
    for d in got["decisions"]:
        d["authority_shadow"] = {}
    expected = asdict(control)
    got.pop("authority_shadow")
    expected.pop("authority_shadow")
    assert got == expected


def test_quality_and_v3_reach_synthesis_planner_and_versioned_snapshot():
    from src.analysis.synthesis import attach_technical_evidence
    signal = analyze_ticker_from_frame("TEST", frame())
    synthesis = blend_scores("TEST", signal.signal, signal.strength, .1, {}, .1, signal.score_raw)
    attach_technical_evidence(synthesis, signal)
    asset = build_signals_from_synthesis([synthesis])["TEST"]
    layers = _layers_payload_for_decision(synthesis)
    assert asset.data_quality == signal.data_quality == layers["data_quality"]
    assert layers["technical_buy_shadow_v3"] == signal.technical_buy_shadow_v3
    assert layers["feature_snapshot"]["schema_version"] == "feature_snapshot_v3"
    assert layers["feature_snapshot"]["payload"]["data_quality"] == signal.data_quality
    asset.data_quality["price_status"] = "CHANGED"
    assert synthesis.data_quality["price_status"] == "VALID"


def test_snapshot_id_is_identity_not_timestamp_and_nonfinite_snapshot_has_reason():
    from scripts.run_analysis import _portfolio_snapshot_id_from_snapshot
    from src.analysis.feature_snapshot import build_feature_snapshot_from_layers
    assert _portfolio_snapshot_id_from_snapshot({"snapshot_id": "id-1", "scraped_at": "timestamp"}) == "id-1"
    assert _portfolio_snapshot_id_from_snapshot({"scraped_at": "timestamp"}) is None
    snapshot = build_feature_snapshot_from_layers({"technical": {"raw": float("nan")}})
    assert snapshot.payload["technical"]["raw"] is None
    assert snapshot.payload["invalid_values"] == {"features.technical.raw": "NONFINITE"}


@pytest.mark.parametrize("field,value", [("macro_score", float("nan")), ("sentiment_score", float("inf")), ("technical_strength", 1.2)])
def test_synthesis_rejects_invalid_units_before_blending(field, value):
    args = dict(ticker="TEST", technical_signal="HOLD", technical_strength=.1, macro_score=.1,
                risk_position={}, sentiment_score=.1)
    with pytest.raises(ValueError, match="INVALID_SYNTHESIS_INPUT"):
        blend_scores(**{**args, field: value})


def test_future_evidence_is_rejected_and_partial_volume_cannot_confirm():
    f = frame()
    f.loc[f.index[-1], "IsClosed"] = False
    assert compute_indicators(f, "TEST").vol_ratio is None
    f.loc[f.index[-1], "AvailableAt"] = f.attrs["cutoff"] + pd.Timedelta(days=1)
    assert analyze_ticker_from_frame("TEST", f) is None


def test_cash_nonfinite_cannot_pass_arithmetic_tolerances():
    from src.analysis.validators import validate_execution_plan, PlanValidationError
    plan = reconcile_funding([], {}, 100000, 100000, "NORMAL")
    plan.cash_after = float("nan")
    with pytest.raises(PlanValidationError, match="INVALID_FINITE_NONNEGATIVE_ARS:cash_after"):
        validate_execution_plan(plan)


def test_db_identity_check_precedes_limit_and_preserves_cutoff_filters():
    import asyncio
    from contextlib import asynccontextmanager
    from src.collector.db import PortfolioDatabase
    class Conn:
        async def fetch(self, query, *args):
            assert "SELECT DISTINCT ticker, asset_type, currency, venue, interval FROM candidates" in query
            assert "scraped_at <=" in query and "currency =" in query
            assert "scraped_at," in query
            assert args[-1] == 1
            return [{"identity_count": 2}]
    class Pool:
        @asynccontextmanager
        async def acquire(self):
            yield Conn()
    db = PortfolioDatabase("postgresql://unused")
    db._pool = Pool()
    with pytest.raises(ValueError, match="AMBIGUOUS_SERIES_IDENTITY"):
        asyncio.run(db.get_market_candles("TEST", currency="ARS", cutoff=datetime.now(timezone.utc), limit=1))


def test_short_lookback_is_missing_not_zero():
    f = frame().tail(60)
    indicator = compute_indicators(f, "TEST")
    assert indicator.sma_200 is None
    signal = analyze_ticker_from_frame("TEST", f)
    assert signal.data_quality["indicator_values"]["sma_200"] is None
    assert "sma_200" in signal.data_quality["indicator_missing_reasons"]


def test_intraday_conversion_preserves_two_bars_in_same_session():
    first = candle(interval="1h")
    second = candle(interval="1h", ts=first["ts"]+timedelta(hours=1))
    assert len(candles_to_frame([first, second])) == 2


def test_recorded_comparator_rejects_owner_and_run_mismatch():
    from scripts.compare_contextual_e1 import compare
    recorded = {"plan": {"id": "p", "run_id": "r", "owner_chat_id": 123}, "captures": [
        {"payload": {"owner": 124, "plan_id": "p", "signals": [{"ticker": "T"}]}}]}
    with pytest.raises(ValueError, match="CAPTURE_IDENTITY_MISMATCH"):
        compare(recorded)
    recorded["captures"][0]["payload"].update(owner=123, run_id="other")
    with pytest.raises(ValueError, match="CAPTURE_RUN_MISMATCH"):
        compare(recorded)


def test_metadata_known_for_one_row_does_not_fill_unknown_rows():
    f = candles_to_frame([candle(volume_unit="units"), candle(ts=datetime(2025, 1, 2, tzinfo=timezone.utc))])
    assert f.attrs["series_identity"]["volume_unit"] is None


def test_undefined_rsi_is_not_fabricated_oversold_zero():
    f = frame()
    f["Close"] = np.arange(260) + 100.
    f["Open"], f["High"], f["Low"] = f.Close, f.Close+2, f.Close-2
    assert compute_indicators(f, "TEST").rsi_14 is None

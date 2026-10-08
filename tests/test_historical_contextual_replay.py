from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from src.analysis.historical_contextual_replay import (
    DATA_STATUS,
    build_retrospective_context,
    forward_outcome,
    replay_holdings,
    select_market_series,
    summarize_replay,
)


def _market_rows(ticker: str, *, source: str = "TRADINGVIEW_BYMA", days: int = 180):
    rows = []
    for idx, ts in enumerate(pd.bdate_range("2025-10-01", periods=days, tz="UTC")):
        close = 100.0 + idx * 0.4
        rows.append(
            {
                "ticker": ticker,
                "long_ticker": f"BYMA:{ticker}",
                "source": source,
                "ts": ts,
                "open_price": close - 0.2,
                "high_price": close + 1.0,
                "low_price": close - 1.0,
                "close_price": close,
                "volume": 1_000.0 + idx,
                "scraped_at": ts + pd.Timedelta(days=60),
            }
        )
    return rows


def _canonical(observed_date: str, ticker: str = "TEST", *, confidence: str = "HIGH"):
    return {
        "date": observed_date,
        "ticker": ticker,
        "asset_type": "CEDEAR",
        "currency": "ARS",
        "quantity_observed": 10.0,
        "price_observed": 150.0,
        "market_value_observed": 1_500.0,
        "weight_final": 0.5,
        "invested_total_observed_ars": 2_500.0,
        "cash_observed_ars": 500.0,
        "account_total_recalc_ars": 3_000.0,
        "snapshot_id": f"snapshot-{observed_date}-{ticker}",
        "snapshot_scraped_at": f"{observed_date} 20:00:00+00:00",
        "owner_chat_id": None,
        "confidence": confidence,
        "quality_flags": "LEGACY_OWNER_NULL",
    }


def test_series_selection_preserves_one_identity_and_prefers_tradingview():
    rows = _market_rows("TEST", source="COCOS", days=200)
    rows += _market_rows("TEST", source="TRADINGVIEW_BYMA", days=180)

    frame, selection = select_market_series(rows, ticker="TEST", asset_type="CEDEAR")

    assert selection["source"] == "TRADINGVIEW_BYMA"
    assert selection["provider_symbol"] == "BYMA:TEST"
    assert frame is not None
    assert frame.attrs["series_identity"] == {
        "ticker": "TEST",
        "asset_type": "CEDEAR",
        "currency": "ARS",
        "venue": "BYMA",
        "interval": "1d",
        "volume_unit": None,
        "calendar": None,
        "adjustment_policy": None,
        "depositary_ratio": None,
        "instrument_id": "BYMA:CEDEAR:TEST:ARS",
    }


def test_context_cutoff_excludes_future_bars_and_marks_non_pit_inputs():
    asset, _ = select_market_series(
        _market_rows("TEST"), ticker="TEST", asset_type="CEDEAR"
    )
    spy, _ = select_market_series(
        _market_rows("SPY"), ticker="SPY", asset_type="CEDEAR"
    )
    assert asset is not None and spy is not None
    cutoff = asset.index[120] + pd.Timedelta(hours=23)

    context = build_retrospective_context(
        asset,
        spy,
        cutoff=cutoff,
        signal_action="HOLD",
    )

    assert context["timestamps"]["last_asset_bar"] <= cutoff.isoformat()
    assert context["timestamps"]["last_benchmark_bar"] <= cutoff.isoformat()
    assert context["data_status"] == DATA_STATUS
    assert "RETROSPECTIVE_MARKET_HISTORY_NOT_PIT" in context["missingness"]
    assert "INGESTED_AFTER_HISTORICAL_CUTOFF" in context["missingness"]
    assert context["affects_analysis"] is False
    assert context["affects_execution"] is False


def test_forward_outcome_uses_exact_following_sessions_and_sell_avoidance():
    asset, _ = select_market_series(
        _market_rows("TEST", days=20), ticker="TEST", asset_type="CEDEAR"
    )
    assert asset is not None
    sessions = [value.date() for value in asset.index]

    result = forward_outcome(
        asset,
        sessions,
        as_of_session=sessions[2],
        signal="SELL",
        horizon=5,
    )

    expected = asset.iloc[7]["Close"] / asset.iloc[3]["Open"] - 1
    assert result["status"] == "EVALUATED"
    assert result["entry_session"] == sessions[3].isoformat()
    assert result["exit_session"] == sessions[7].isoformat()
    assert result["asset_return"] == pytest.approx(expected)
    assert result["directional_or_hold_return"] == pytest.approx(-expected)


def test_replay_preserves_rows_deduplicates_same_market_session_and_is_neutral():
    market = _market_rows("TEST") + _market_rows("SPY")
    last_session = pd.bdate_range("2025-10-01", periods=180).date[-1]
    friday = last_session.isoformat()
    saturday = (pd.Timestamp(last_session) + pd.Timedelta(days=1)).date().isoformat()
    canonical = [_canonical(friday), _canonical(saturday)]

    replay = replay_holdings(canonical, market)
    summary = summarize_replay(replay)

    assert len(replay["results"]) == 2
    assert replay["results"][0]["metric_eligible"] is True
    assert replay["results"][1]["metric_eligible"] is False
    assert replay["results"][1]["metric_exclusion_reason"] == "DUPLICATE_TICKER_MARKET_SESSION"
    for row in replay["results"]:
        assert row["old_signal"] == row["new_signal"]
        assert row["old_score_raw"] == row["new_score_raw"]
        assert row["signal_changed"] is False
        assert row["score_changed"] is False
    assert summary["non_regression"] == {
        "scores_equal": True,
        "signals_equal": True,
        "decisions_changed": 0,
        "orders_created": 0,
        "quantities_changed": 0,
        "cash_changed": False,
        "affects_analysis": False,
        "affects_execution": False,
    }


def test_non_security_is_preserved_but_excluded():
    row = _canonical("2026-07-01", ticker="USESPECIE")
    row["asset_type"] = "UNKNOWN"
    replay = replay_holdings([row], _market_rows("SPY"))

    assert len(replay["results"]) == 1
    result = replay["results"][0]
    assert result["ticker"] == "USESPECIE"
    assert result["analysis_status"] == "EXCLUDED_NON_SECURITY_LABEL"
    assert result["metric_eligible"] is False


def test_ypfd_split_event_is_preserved_as_corporate_action():
    market = _market_rows("YPFD") + _market_rows("SPY")
    session = pd.bdate_range("2025-10-01", periods=180).date[-1].isoformat()
    canonical = [_canonical(session, ticker="YPFD")]
    events = [
        {
            "event_date": session,
            "ticker": "YPFD",
            "classification": "CORPORATE_ACTION_SPLIT",
            "confidence": "HIGH",
        }
    ]

    replay = replay_holdings(canonical, market, events)

    assert replay["results"][0]["position_event_classification"] == "CORPORATE_ACTION_SPLIT"
    assert replay["results"][0]["position_event_confidence"] == "HIGH"

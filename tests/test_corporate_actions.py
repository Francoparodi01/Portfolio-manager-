import asyncio
from dataclasses import replace
from datetime import datetime, timezone

import pandas as pd
import pytest

from scripts.manage_corporate_action import parse_ratio
from src.analysis.corporate_actions import (
    CorporateActionEffect,
    EvidenceLevel,
    PriceQualityStatus,
    add_market_sessions,
    assess_live_price,
    detect_price_anomaly,
    guard_history_frames,
    matching_effect_for_quantity_transition,
    normalize_candle_rows,
    normalize_frame_for_effects,
    rebase_position_view,
    rebase_reference_price,
)
from src.analysis.enums import DecisionType
from src.analysis.execution_planner import (
    DecisionIntent,
    PositionSnapshot,
    reconcile_funding,
)
from src.analysis.preclose_alerts import build_preclose_alerts
from src.analysis.thesis_shadow import mature_forecast
from src.collector.live_portfolio import (
    build_live_portfolio,
    render_opening_portfolio_report,
    select_portfolio_move_alerts,
)
from src.collector.db import PortfolioDatabase


UTC = timezone.utc


def _effect(
    *,
    ticker: str = "YPFD",
    quantity_factor: float = 10.0,
    price_factor: float = 0.1,
    status: str = "EFFECTIVE",
) -> CorporateActionEffect:
    return CorporateActionEffect(
        event_id=1,
        effect_id=11,
        event_key=f"YPF:{ticker}:SPLIT:2026-08-04",
        issuer_id="YPF",
        event_type="SPLIT",
        lifecycle_status=status,
        effective_at=datetime(2026, 8, 4, 3, 0, tzinfo=UTC),
        expires_at=None,
        source_name="YPF Investors",
        source_url="https://inversores.ypf.com/",
        ingestion_method="MANUAL",
        evidence_level=EvidenceLevel.PRIMARY_OFFICIAL.value,
        detector_score=None,
        instrument_id=f"BYMA:ACCION:{ticker}:ARS",
        ticker=ticker,
        venue="BYMA",
        asset_type="ACCION",
        currency="ARS",
        quantity_factor=quantity_factor,
        price_factor=price_factor,
        cost_basis_factor=price_factor,
    )


def _frame(pre_close: float = 90.0, post_close: float = 9.0) -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "Open": [pre_close, post_close],
            "High": [pre_close, post_close],
            "Low": [pre_close, post_close],
            "Close": [pre_close, post_close],
            "Volume": [100.0, 1000.0],
            "Source": ["COCOS", "COCOS"],
        },
        index=pd.to_datetime(["2026-08-03T20:00:00Z", "2026-08-04T20:00:00Z"]),
    )
    frame.attrs["candle_sources"] = ("COCOS",)
    return frame


def test_detector_supports_non_integer_ratio_and_keeps_it_suspected_without_source():
    flag = detect_price_anomaly(
        ticker="XYZ",
        reference_price=150.0,
        current_price=100.0,
        observed_at=datetime(2026, 8, 4, 20, 0, tzinfo=UTC),
    )

    assert flag is not None
    assert flag.quantity_factor == pytest.approx(1.5)
    assert flag.evidence["ratio_numerator"] == 3
    assert flag.evidence["ratio_denominator"] == 2
    assert flag.evidence_level == "HEURISTIC_ONLY"
    assert flag.resolution_status == "OPEN"


def test_quantity_change_corroborates_but_does_not_impersonate_official_source():
    flag = detect_price_anomaly(
        ticker="YPFD",
        reference_price=90.0,
        current_price=9.0,
        previous_quantity=5.0,
        current_quantity=50.0,
        observed_at=datetime(2026, 8, 4, 20, 0, tzinfo=UTC),
    )

    assert flag is not None
    assert flag.quantity_factor == pytest.approx(10.0)
    assert flag.observed_quantity_factor == pytest.approx(10.0)
    assert flag.evidence_level == "CORROBORATED"
    assert flag.detector_score > 0.9
    assert flag.expires_at > flag.observed_at


def test_detector_prefers_clean_ten_to_one_over_price_noise_fraction():
    flag = detect_price_anomaly(
        ticker="YPFD",
        reference_price=83_000.0,
        current_price=8_080.0,
        observed_at=datetime(2026, 8, 4, 4, 8, tzinfo=UTC),
    )

    assert flag is not None
    assert flag.quantity_factor == pytest.approx(10.0)
    assert flag.evidence["ratio_numerator"] == 10
    assert flag.evidence["ratio_denominator"] == 1


def test_real_crash_that_matches_ratio_is_quarantined_not_confirmed():
    flag = detect_price_anomaly(
        ticker="ILLIQUID",
        reference_price=100.0,
        current_price=20.0,
        previous_quantity=100.0,
        current_quantity=100.0,
        observed_at=datetime(2026, 8, 4, 20, 0, tzinfo=UTC),
    )

    assert flag is not None
    assert flag.quantity_factor == pytest.approx(5.0)
    assert flag.evidence_level == "HEURISTIC_ONLY"
    assert "confirmation is still missing" in flag.reason


def test_earnings_gap_is_not_misclassified_as_unconfirmed_ratio_change():
    frame = _frame(pre_close=3765.0, post_close=4982.5)
    frame.index = pd.to_datetime(["2026-08-05T20:00:00Z", "2026-08-07T20:00:00Z"])
    result = guard_history_frames(
        {"TEAM": frame},
        issuer_events_by_ticker={
            "TEAM": [
                {
                    "event_type": "EARNINGS",
                    "lifecycle_status": "ANNOUNCED",
                    "event_date": datetime(2026, 8, 6, tzinfo=UTC).date(),
                    "source": "YAHOO",
                    "title": "TEAM Q4 2026 Earnings Announcement",
                }
            ]
        },
        observed_at=datetime(2026, 8, 10, 19, 46, tzinfo=UTC),
    )

    assert "TEAM" in result.frames
    assert result.blocked_by_ticker == {}
    assert len(result.flags) == 1
    assert result.flags[0].resolution_status == "DISMISSED"
    assert result.flags[0].action_taken == "DISMISSED_BY_ISSUER_EVENT_CONTEXT"
    assert result.flags[0].evidence["competing_event_type"] == "EARNINGS"


def test_earnings_context_does_not_override_quantity_corroboration():
    frame = _frame(pre_close=90.0, post_close=9.0)
    result = guard_history_frames(
        {"YPFD": frame},
        portfolio_history=[
            {
                "scraped_at": "2026-08-03T20:00:00+00:00",
                "positions": [{"ticker": "YPFD", "quantity": 5.0}],
            },
            {
                "scraped_at": "2026-08-04T20:00:00+00:00",
                "positions": [{"ticker": "YPFD", "quantity": 50.0}],
            },
        ],
        issuer_events_by_ticker={
            "YPFD": [
                {
                    "event_type": "EARNINGS",
                    "lifecycle_status": "ANNOUNCED",
                    "event_date": datetime(2026, 8, 4, tzinfo=UTC).date(),
                }
            ]
        },
        observed_at=datetime(2026, 8, 4, 20, 0, tzinfo=UTC),
    )

    assert "YPFD" not in result.frames
    assert "YPFD" in result.blocked_by_ticker
    assert result.flags[0].evidence_level == "CORROBORATED"


def test_saving_dismissed_flag_closes_only_open_heuristics_for_same_window():
    dismissed = detect_price_anomaly(
        ticker="TEAM",
        reference_price=3765.0,
        current_price=5020.0,
        observed_at=datetime(2026, 8, 7, 20, 0, tzinfo=UTC),
    )
    assert dismissed is not None
    dismissed = replace(
        dismissed,
        resolution_status="DISMISSED",
        action_taken="DISMISSED_BY_ISSUER_EVENT_CONTEXT",
    )

    class _Conn:
        def __init__(self):
            self.calls = []

        async def execute(self, sql, *params):
            self.calls.append((sql, params))
            return "UPDATE 1" if sql.lstrip().startswith("UPDATE") else "INSERT 0 1"

    class _Acquire:
        def __init__(self, conn):
            self.conn = conn

        async def __aenter__(self):
            return self.conn

        async def __aexit__(self, *_args):
            return False

    class _Pool:
        def __init__(self):
            self.conn = _Conn()

        def acquire(self):
            return _Acquire(self.conn)

    db = PortfolioDatabase("postgresql://unused")
    db._pool = _Pool()

    async def _skip_schema(_conn):
        return None

    db._ensure_corporate_actions_schema = _skip_schema
    asyncio.run(db.save_price_quality_flags([dismissed]))

    dismissal_sql, dismissal_params = db._pool.conn.calls[0]
    assert "evidence_level = 'HEURISTIC_ONLY'" in dismissal_sql
    assert "resolution_status = 'OPEN'" in dismissal_sql
    assert dismissal_params[0] == "TEAM"
    assert dismissal_params[2] == "DISMISSED_BY_ISSUER_EVENT_CONTEXT"


def test_reverse_split_is_detected_from_price_and_quantity_factors():
    flag = detect_price_anomaly(
        ticker="REV",
        reference_price=9.0,
        current_price=90.0,
        previous_quantity=100.0,
        current_quantity=10.0,
        observed_at=datetime(2026, 8, 4, 20, 0, tzinfo=UTC),
    )

    assert flag is not None
    assert flag.quantity_factor == pytest.approx(0.1)
    assert flag.expected_price_factor == pytest.approx(10.0)
    assert flag.evidence_level == "CORROBORATED"


def test_suspected_ttl_counts_market_sessions_not_weekdays_only():
    observed_at = datetime(2026, 8, 14, 20, 0, tzinfo=UTC)

    expires_at = add_market_sessions(observed_at, 2)

    assert expires_at.date().isoformat() == "2026-08-19"


def test_snapshot_quantity_jump_is_classified_as_corporate_action():
    effect = matching_effect_for_quantity_transition(
        ticker="YPFD",
        previous_quantity=10.0,
        current_quantity=100.0,
        previous_at=datetime(2026, 8, 3, 20, 0, tzinfo=UTC),
        current_at=datetime(2026, 8, 4, 20, 0, tzinfo=UTC),
        effects=[_effect()],
    )

    assert effect is not None
    assert effect.event_key == _effect().event_key


def test_unrelated_quantity_change_remains_human_activity_candidate():
    effect = matching_effect_for_quantity_transition(
        ticker="YPFD",
        previous_quantity=10.0,
        current_quantity=15.0,
        previous_at=datetime(2026, 8, 3, 20, 0, tzinfo=UTC),
        current_at=datetime(2026, 8, 4, 20, 0, tzinfo=UTC),
        effects=[_effect()],
    )

    assert effect is None


def test_confirmed_split_normalizes_frame_without_mutating_raw_data():
    raw = _frame()
    normalized, applications, blocking_reason = normalize_frame_for_effects(
        "YPFD",
        raw,
        [_effect()],
    )

    assert blocking_reason is None
    assert raw["Close"].iloc[0] == 90.0
    assert normalized["Close"].iloc[0] == pytest.approx(9.0)
    assert normalized["Close"].pct_change().iloc[-1] == pytest.approx(0.0)
    assert normalized["Volume"].iloc[0] == pytest.approx(1000.0)
    assert applications[0].application_status == "APPLIED"
    assert applications[0].invariant_checks["raw_records_mutated"] is False

    second, second_applications, second_block = normalize_frame_for_effects(
        "YPFD",
        normalized,
        [_effect()],
    )
    assert second_block is None
    assert second_applications == []
    assert second["Close"].iloc[0] == pytest.approx(9.0)


def test_confirmed_event_accepts_already_adjusted_source_and_resolves_heuristic():
    result = guard_history_frames(
        {"YPFD": _frame(pre_close=9.0, post_close=9.1)},
        effects=[_effect()],
        observed_at=datetime(2026, 8, 4, 20, 0, tzinfo=UTC),
    )

    assert "YPFD" in result.frames
    assert result.applications[0].application_status == "ALREADY_ADJUSTED"
    assert result.flags[0].resolution_status == "CONFIRMED"
    assert result.flags[0].action_taken == "CONFIRMED_ALREADY_ADJUSTED"


def test_depositary_ratio_change_can_have_identity_market_transform():
    effect = replace(
        _effect(ticker="YPF", quantity_factor=1.0, price_factor=1.0),
        cost_basis_factor=1.0,
        event_type="DEPOSITARY_RATIO_CHANGE",
        depositary_ratio_before="1:1",
        depositary_ratio_after="1:10",
    )
    raw = _frame(pre_close=30.0, post_close=31.0)

    normalized, applications, block = normalize_frame_for_effects("YPF", raw, [effect])

    assert block is None
    assert normalized["Close"].tolist() == raw["Close"].tolist()
    assert applications[0].application_status == "ALREADY_ADJUSTED"
    assert applications[0].invariant_checks["identity_transform"] is True


def test_confirmed_terms_mismatch_blocks_price_use():
    assessment = assess_live_price(
        ticker="YPFD",
        reference_price=90.0,
        current_price=18.0,
        observed_at=datetime(2026, 8, 4, 20, 0, tzinfo=UTC),
        effects=[_effect()],
    )

    assert assessment.status == PriceQualityStatus.DATA_QUALITY_BLOCK.value
    assert assessment.normalized_change is None
    assert assessment.flag is not None
    assert assessment.flag.action_taken == "DATA_QUALITY_QUARANTINE"


def test_heuristic_guard_removes_non_comparable_frame_before_scoring():
    result = guard_history_frames(
        {"YPFD": _frame()},
        observed_at=datetime(2026, 8, 4, 20, 0, tzinfo=UTC),
    )

    assert "YPFD" not in result.frames
    assert "YPFD" in result.blocked_by_ticker
    assert result.flags[0].action_taken == "TEMPORARY_QUARANTINE"


def test_effective_event_blocks_preopen_analysis_until_post_event_candle_exists():
    pre_event = pd.DataFrame(
        {
            "Open": [88.0, 90.0],
            "High": [88.0, 90.0],
            "Low": [88.0, 90.0],
            "Close": [88.0, 90.0],
            "Volume": [100.0, 100.0],
        },
        index=pd.to_datetime(["2026-08-01T20:00:00Z", "2026-08-03T20:00:00Z"]),
    )

    result = guard_history_frames(
        {"YPFD": pre_event},
        effects=[_effect()],
        observed_at=datetime(2026, 8, 4, 4, 8, tzinfo=UTC),
    )

    assert "YPFD" not in result.frames
    assert "no post-event candle" in result.blocked_by_ticker["YPFD"]
    assert result.applications[0].application_status == "PENDING"
    assert result.flags[0].action_taken == "WAIT_POST_EVENT_PRICE"
    assert result.flags[0].expires_at is None


def test_official_event_reconciles_vendor_transition_stamped_one_day_early():
    frame = pd.DataFrame(
        {
            "Open": [81_400.0, 83_000.0, 8_080.0],
            "High": [81_400.0, 83_000.0, 8_080.0],
            "Low": [81_400.0, 83_000.0, 8_080.0],
            "Close": [81_400.0, 83_000.0, 8_080.0],
            "Volume": [100.0, 100.0, 1_000.0],
        },
        index=pd.to_datetime(
            ["2026-07-30T00:00:00Z", "2026-07-31T00:00:00Z", "2026-08-03T00:00:00Z"]
        ),
    )

    result = guard_history_frames(
        {"YPFD": frame},
        effects=[_effect()],
        observed_at=datetime(2026, 8, 4, 4, 8, tzinfo=UTC),
    )

    assert "YPFD" in result.frames
    assert result.blocked_by_ticker == {}
    normalized = result.frames["YPFD"]
    assert normalized["Close"].tolist() == pytest.approx([8_140.0, 8_300.0, 8_080.0])
    assert normalized["Close"].pct_change().iloc[-1] == pytest.approx(-0.026506, abs=1e-6)
    assert result.applications[0].before_state["transition_day"] == "2026-08-03"


def test_heuristic_uses_latest_distinct_portfolio_quantity_across_midnight():
    frame = _frame(pre_close=83_000.0, post_close=8_080.0)
    history = [
        {
            "scraped_at": "2026-08-03T22:35:00+00:00",
            "positions": [{"ticker": "YPFD", "quantity": 5.0}],
        },
        {
            "scraped_at": "2026-08-04T02:25:00+00:00",
            "positions": [{"ticker": "YPFD", "quantity": 50.0}],
        },
        {
            "scraped_at": "2026-08-04T04:07:00+00:00",
            "positions": [{"ticker": "YPFD", "quantity": 50.0}],
        },
    ]

    result = guard_history_frames(
        {"YPFD": frame},
        portfolio_history=history,
        observed_at=datetime(2026, 8, 4, 4, 8, tzinfo=UTC),
    )

    assert "YPFD" in result.blocked_by_ticker
    assert result.flags[0].observed_quantity_factor == pytest.approx(10.0)
    assert result.flags[0].evidence_level == "CORROBORATED"


def test_live_portfolio_rebases_pre_event_snapshot_and_uses_normalized_return():
    live = build_live_portfolio(
        {
            "snapshot_id": "pre-split",
            "scraped_at": "2026-08-03T20:00:00+00:00",
            "cash_ars": 0,
            "positions": [
                {
                    "ticker": "YPFD",
                    "quantity": 10,
                    "avg_cost": 90,
                    "current_price": 90,
                    "market_value": 900,
                }
            ],
        },
        [
            {
                "ticker": "YPFD",
                "asset_type": "ACCION",
                "last_price": 9,
                "previous_close_price": 90,
                "change_pct_1d": -0.9,
                "ts": datetime(2026, 8, 4, 15, 0, tzinfo=UTC),
            }
        ],
        generated_at=datetime(2026, 8, 4, 15, 1, tzinfo=UTC),
        corporate_action_effects=[_effect()],
    )

    position = live["positions"][0]
    assert position["quantity"] == pytest.approx(100.0)
    assert position["avg_cost"] == pytest.approx(9.0)
    assert position["market_value"] == pytest.approx(900.0)
    assert position["raw_change_pct_1d"] == pytest.approx(-0.9)
    assert position["change_pct_1d"] == pytest.approx(0.0)
    assert position["price_quality_status"] == PriceQualityStatus.RECONCILED.value
    assert select_portfolio_move_alerts(live) == []


def test_stale_market_rows_do_not_create_daily_moves_or_alerts():
    opening_at = datetime(2026, 8, 10, 13, 31, tzinfo=UTC)
    live = build_live_portfolio(
        {
            "snapshot_id": "opening",
            "scraped_at": opening_at,
            "cash_ars": 8709,
            "positions": [
                {
                    "ticker": "EFX",
                    "quantity": 13,
                    "current_price": 18030,
                    "market_value": 234390,
                },
                {
                    "ticker": "AXP",
                    "quantity": 7,
                    "current_price": 35800,
                    "market_value": 250600,
                },
            ],
        },
        [
            {
                "ticker": "EFX",
                "last_price": 17710,
                "change_pct_1d": 2.8,
                "ts": datetime(2026, 8, 7, 19, 0, tzinfo=UTC),
            },
            {
                "ticker": "AXP",
                "last_price": 35900,
                "change_pct_1d": -0.4,
                "ts": datetime(2026, 8, 7, 19, 0, tzinfo=UTC),
            },
        ],
        generated_at=opening_at,
    )

    by_ticker = {position["ticker"]: position for position in live["positions"]}
    assert live["price_coverage_count"] == 0
    assert live["day_change_pct"] is None
    assert by_ticker["EFX"]["current_price"] == pytest.approx(18030)
    assert by_ticker["EFX"]["change_pct_1d"] is None
    assert by_ticker["AXP"]["change_pct_1d"] is None
    assert select_portfolio_move_alerts(live) == []

    report = render_opening_portfolio_report(live)
    assert "Movimiento cartera: <b>N/A</b>" in report
    assert "+280.00%" not in report
    assert "-40.00%" not in report


def test_fresh_cocos_percentage_points_are_normalized_without_previous_close():
    opening_at = datetime(2026, 8, 10, 13, 31, tzinfo=UTC)
    live = build_live_portfolio(
        {
            "snapshot_id": "post-open",
            "scraped_at": opening_at,
            "cash_ars": 0,
            "positions": [
                {
                    "ticker": "EFX",
                    "quantity": 13,
                    "current_price": 18030,
                    "market_value": 234390,
                }
            ],
        },
        [
            {
                "ticker": "EFX",
                "last_price": 17710,
                "change_pct_1d": 2.8,
                "ts": datetime(2026, 8, 10, 13, 40, tzinfo=UTC),
            }
        ],
        generated_at=opening_at,
    )

    position = live["positions"][0]
    assert live["price_coverage_count"] == 1
    assert position["price_source"] == "market_prices"
    assert position["change_pct_1d"] == pytest.approx(0.028)
    assert live["day_change_pct"] == pytest.approx(0.028)


def test_partial_coverage_does_not_claim_total_portfolio_return():
    opening_at = datetime(2026, 8, 10, 13, 31, tzinfo=UTC)
    live = build_live_portfolio(
        {
            "snapshot_id": "partial",
            "scraped_at": opening_at,
            "cash_ars": 0,
            "positions": [
                {"ticker": "AMD", "quantity": 1, "current_price": 75000},
                {"ticker": "NVS", "quantity": 1, "current_price": 61000},
            ],
        },
        [
            {
                "ticker": "AMD",
                "last_price": 76000,
                "previous_close_price": 75000,
                "change_pct_1d": 1.3,
                "ts": datetime(2026, 8, 10, 13, 40, tzinfo=UTC),
            },
            {
                "ticker": "NVS",
                "last_price": 60825,
                "change_pct_1d": 0,
                "ts": datetime(2026, 8, 5, 19, 0, tzinfo=UTC),
            },
        ],
        generated_at=opening_at,
    )

    assert live["price_coverage_count"] == 1
    assert live["day_pnl_ars"] == pytest.approx(1000)
    assert live["day_change_pct"] is None
    report = render_opening_portfolio_report(live)
    assert "REVISION: cobertura incompleta" in report
    assert "P&amp;L parcial (1/2): <b>+$1.000 ARS</b>" in report
    assert "no usar el total como rendimiento" in report


def test_position_reconciliation_preserves_value_and_cost_basis():
    original = {
        "ticker": "YPFD",
        "quantity": 10.0,
        "avg_cost": 90.0,
        "current_price": 90.0,
        "market_value": 900.0,
    }

    rebased, applications = rebase_position_view(
        original,
        snapshot_at=datetime(2026, 8, 3, 20, 0, tzinfo=UTC),
        as_of=datetime(2026, 8, 4, 20, 0, tzinfo=UTC),
        effects=[_effect()],
    )

    assert original["quantity"] == 10.0
    assert original["current_price"] == 90.0
    assert rebased["quantity"] == pytest.approx(100.0)
    assert rebased["avg_cost"] == pytest.approx(9.0)
    assert rebased["current_price"] == pytest.approx(9.0)
    assert rebased["market_value"] == pytest.approx(900.0)
    assert applications[0].invariant_checks["cost_basis_invariant"] is True


def test_position_reconciliation_keeps_fresh_price_on_current_basis():
    position = {
        "ticker": "YPFD",
        "quantity": 10.0,
        "avg_cost": 90.0,
        "current_price": 9.0,
        "market_value": 90.0,
        "price_normalized": True,
    }

    rebased, _ = rebase_position_view(
        position,
        snapshot_at=datetime(2026, 8, 3, 20, 0, tzinfo=UTC),
        as_of=datetime(2026, 8, 4, 20, 0, tzinfo=UTC),
        effects=[_effect()],
    )

    assert rebased["quantity"] == pytest.approx(100.0)
    assert rebased["current_price"] == pytest.approx(9.0)
    assert rebased["market_value"] == pytest.approx(900.0)


def test_multiple_position_events_are_applied_and_audited_sequentially():
    first = replace(_effect(), event_id=1, effect_id=11)
    second = replace(
        _effect(quantity_factor=2.0, price_factor=0.5),
        event_id=2,
        effect_id=22,
        event_key="YPF:YPFD:SPLIT:2026-08-05",
        effective_at=datetime(2026, 8, 5, 3, 0, tzinfo=UTC),
    )

    rebased, applications = rebase_position_view(
        {"ticker": "YPFD", "quantity": 10.0, "avg_cost": 90.0, "current_price": 90.0},
        snapshot_at=datetime(2026, 8, 3, 20, 0, tzinfo=UTC),
        as_of=datetime(2026, 8, 5, 20, 0, tzinfo=UTC),
        effects=[first, second],
    )

    assert rebased["quantity"] == pytest.approx(200.0)
    assert rebased["avg_cost"] == pytest.approx(4.5)
    assert rebased["current_price"] == pytest.approx(4.5)
    assert len(applications) == 2
    assert applications[0].after_state["quantity"] == pytest.approx(100.0)
    assert applications[1].before_state["quantity"] == pytest.approx(100.0)


def test_preclose_turns_unconfirmed_ninety_percent_move_into_quality_alert():
    now = datetime(2026, 8, 4, 16, 45, tzinfo=UTC)
    alerts = build_preclose_alerts(
        positions=[{"ticker": "YPFD", "quantity": 10, "market_value": 90}],
        latest_prices=[{"ticker": "YPFD", "last_price": 9, "ts": now}],
        previous_closes={"YPFD": 90},
        total_ars=90,
        now=now,
    )

    assert len(alerts) == 1
    assert alerts[0].alert_type == "PRICE_NOT_COMPARABLE"
    assert alerts[0].change_pct == pytest.approx(-0.9)
    assert "Suspender senal tecnica" in alerts[0].action


def test_planner_blocks_sell_and_buy_for_non_comparable_ticker():
    sell = DecisionIntent(
        ticker="YPFD",
        action=DecisionType.SELL_FULL,
        reason_primary="downtrend",
        reason_secondary=None,
        current_weight=0.5,
        target_weight=0.0,
        delta_weight=-0.5,
        score=-0.9,
        conviction=0.9,
        theoretical_ars=900,
    )
    plan = reconcile_funding(
        decisions=[sell],
        current_positions={
            "YPFD": PositionSnapshot(
                ticker="YPFD",
                quantity=100,
                price=9,
                market_value_ars=900,
                current_weight=0.5,
            )
        },
        cash_before=0,
        portfolio_value_ars=1800,
        gate="NORMAL",
        min_trade_ars=1,
        blocked_trade_tickers={"YPFD": "PRICE_NOT_COMPARABLE"},
    )

    assert plan.sell_orders == []
    assert len(plan.blocked_orders) == 1
    assert plan.blocked_orders[0].block_code == "BLOCKED_CORPORATE_ACTION"
    assert plan.blocked_orders[0].decision_override == "HOLD"


def test_reference_prices_rebase_but_expected_returns_do_not():
    adjusted, factor = rebase_reference_price(
        90.0,
        reference_at=datetime(2026, 8, 3, 20, 0, tzinfo=UTC),
        as_of=datetime(2026, 8, 4, 20, 0, tzinfo=UTC),
        effects=[_effect()],
    )

    assert adjusted == pytest.approx(9.0)
    assert factor == pytest.approx(0.1)


def test_shadow_outcome_across_split_uses_one_price_basis():
    effect = _effect()
    future = normalize_candle_rows(
        [
            {
                "ts": datetime(2026, 8, 4, 20, 0, tzinfo=UTC),
                "open_price": 9.0,
                "high_price": 9.0,
                "low_price": 9.0,
                "close_price": 9.0,
                "volume": 1000.0,
            }
        ],
        [effect],
    )
    adjusted_reference, _ = rebase_reference_price(
        90.0,
        reference_at=datetime(2026, 8, 3, 20, 0, tzinfo=UTC),
        as_of=datetime(2026, 8, 4, 20, 0, tzinfo=UTC),
        effects=[effect],
    )
    outcome = mature_forecast(
        as_of_ts=datetime(2026, 8, 3, 20, 0, tzinfo=UTC),
        reference_price=float(adjusted_reference),
        horizon_sessions=1,
        expected_return=0.0,
        future_candles=future,
    )

    assert outcome is not None
    assert outcome.realized_return == pytest.approx(0.0)
    assert outcome.direction_correct is True


def test_db_outcome_inputs_query_and_reconcile_only_when_candles_jump():
    db = PortfolioDatabase("postgresql://unused")

    async def effects(**_kwargs):
        return [_effect()]

    db.get_corporate_action_effects = effects
    entry, candles, factor = asyncio.run(
        db._corporate_action_adjusted_outcome_inputs(
            ticker="YPFD",
            entry_price=90.0,
            decided_at=datetime(2026, 8, 3, 20, 0, tzinfo=UTC),
            candles=[
                {
                    "ts": datetime(2026, 8, 3, 20, 0, tzinfo=UTC),
                    "close_price": 90.0,
                },
                {
                    "ts": datetime(2026, 8, 4, 20, 0, tzinfo=UTC),
                    "close_price": 9.0,
                },
            ],
            as_of=datetime(2026, 8, 4, 20, 0, tzinfo=UTC),
        )
    )

    assert entry == pytest.approx(9.0)
    assert candles[0]["close_price"] == pytest.approx(9.0)
    assert factor == pytest.approx(0.1)


def test_ratio_parser_uses_new_to_old_quantity_convention():
    factor, numerator, denominator = parse_ratio("10:1")

    assert factor == 10.0
    assert (numerator, denominator) == (10, 1)


def test_ratio_parser_allows_identity_effect_for_depositary_ratio_change():
    factor, numerator, denominator = parse_ratio("1:1")

    assert factor == 1.0
    assert (numerator, denominator) == (1, 1)

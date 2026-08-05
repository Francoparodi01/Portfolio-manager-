from datetime import date

from src.analysis.upcoming_earnings import (
    UpcomingEarningsEvent,
    earnings_shadow_layer_for_ticker,
    earnings_window_state,
    render_upcoming_earnings_report,
    trading_sessions_until,
    upcoming_earnings_from_rows,
)


def _event(
    *,
    ticker: str = "AXP",
    event_date: date = date(2026, 8, 10),
    earnings_phase: str = "scheduled",
) -> UpcomingEarningsEvent:
    return UpcomingEarningsEvent(
        observation_key=f"YAHOO:EARNINGS:{ticker}:{event_date.isoformat()}",
        issuer_id="SEC:0000004962",
        ticker=ticker,
        event_date=event_date,
        event_time_hint="before_open",
        source="YAHOO",
        confidence=0.75,
        lifecycle_status="ANNOUNCED",
        earnings_phase=earnings_phase,
        fiscal_year=2026,
        fiscal_quarter=2,
        fiscal_period_end=date(2026, 6, 30),
        eps_estimate=2.1,
        source_url="https://finance.yahoo.com/quote/AXP/",
    )


def test_upcoming_earnings_maps_fiscal_and_eps_fields_from_persisted_row():
    events = upcoming_earnings_from_rows(
        [
            {
                "observation_key": "YAHOO:EARNINGS:AXP:2026-08-10",
                "issuer_id": "SEC:0000004962",
                "ticker": "axp",
                "event_date": "2026-08-10",
                "event_time_hint": "before_open",
                "source": "yahoo",
                "confidence": "0.75",
                "lifecycle_status": "announced",
                "fiscal_year": 2026,
                "fiscal_quarter": 2,
                "fiscal_period_end": "2026-06-30",
                "source_url": "https://finance.yahoo.com/quote/AXP/",
                "raw_payload": {
                    "earnings_phase": "scheduled",
                    "eps_estimate": 2.1,
                    "reported_eps": None,
                    "surprise_pct": None,
                },
            }
        ]
    )

    assert len(events) == 1
    event = events[0]
    assert event.ticker == "AXP"
    assert event.fiscal_label == "Q2 2026"
    assert event.fiscal_period_end == date(2026, 6, 30)
    assert event.eps_estimate == 2.1


def test_trading_sessions_until_skips_weekend():
    assert trading_sessions_until(date(2026, 8, 10), today=date(2026, 8, 7)) == 1


def test_pre_earnings_shadow_marks_hypothetical_buy_block_without_changing_decision():
    event = _event()

    layer = earnings_shadow_layer_for_ticker(
        "axp",
        [event],
        today=date(2026, 8, 7),
    )

    assert layer["state"] == "PRE_EARNINGS_WINDOW"
    assert layer["trading_sessions_until"] == 1
    assert layer["would_block_new_buy"] is True
    assert layer["decision_changed"] is False
    assert layer["fiscal_quarter"] == 2


def test_pre_earnings_shadow_ignores_distant_and_reported_events():
    assert earnings_shadow_layer_for_ticker(
        "AXP",
        [_event(event_date=date(2026, 8, 14))],
        today=date(2026, 8, 7),
    ) == {}
    assert earnings_shadow_layer_for_ticker(
        "AXP",
        [_event(earnings_phase="post_reported")],
        today=date(2026, 8, 7),
    ) == {}
    assert earnings_window_state(
        _event(earnings_phase="post_reported"),
        today=date(2026, 8, 7),
    ) == "POST_REPORTED"


def test_upcoming_earnings_report_exposes_source_and_shadow_boundary():
    report = render_upcoming_earnings_report(
        [_event()],
        today=date(2026, 8, 7),
    )

    assert "PROXIMOS BALANCES" in report
    assert "<b>AXP</b>" in report
    assert "Q2 2026" in report
    assert "EPS est. 2.10" in report
    assert "YAHOO conf 0.75" in report
    assert "no cambia scores ni ordenes" in report

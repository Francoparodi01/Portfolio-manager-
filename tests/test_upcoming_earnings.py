from datetime import date, datetime, timezone

from src.analysis.upcoming_earnings import (
    UpcomingEarningsEvent,
    earnings_shadow_layer_for_ticker,
    earnings_window_state,
    post_earnings_session_date,
    render_post_earnings_reactions_html,
    render_recent_exit_earnings_html,
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


def test_recent_exit_event_is_labeled_without_claiming_current_holding():
    lines = render_recent_exit_earnings_html(
        [_event(ticker="NU", event_date=date(2026, 8, 13))],
        exited_at_by_ticker={
            "NU": datetime(2026, 8, 11, 16, 45, tzinfo=timezone.utc),
        },
        today=date(2026, 8, 12),
    )
    report = "\n".join(lines)

    assert "SALIDAS RECIENTES CON BALANCE" in report
    assert "<b>NU</b> 13/08" in report
    assert "salida 11/08" in report
    assert "ya no están en cartera" in report


def test_post_earnings_session_uses_same_day_only_for_before_open():
    assert post_earnings_session_date(_event(event_date=date(2026, 8, 13))) == date(2026, 8, 13)
    after_close = UpcomingEarningsEvent(
        **{
            **_event(event_date=date(2026, 8, 13)).__dict__,
            "event_time_hint": "after_close",
        }
    )
    assert post_earnings_session_date(after_close) == date(2026, 8, 14)


def test_post_earnings_reaction_reports_price_not_pnl():
    lines = render_post_earnings_reactions_html(
        [
            {
                "ticker": "NU",
                "event_date": date(2026, 8, 13),
                "session_date": date(2026, 8, 14),
                "previous_close": 10000,
                "first_price": 10800,
                "latest_price": 11000,
                "observed_at": datetime(2026, 8, 14, 14, 0, tzinfo=timezone.utc),
                "scope_label": "salida reciente 11/08",
            }
        ]
    )
    report = "\n".join(lines)

    assert "RESULTADO TRAS BALANCE" in report
    assert "Valor tras balance: <b>$11.000,00</b> (+10.00%)" in report
    assert "Apertura observada: $10.800,00 (+8.00%)" in report
    assert "no representa PnL" in report


def test_adjacent_cross_source_rows_for_same_period_are_one_event():
    events = upcoming_earnings_from_rows(
        [
            {
                "observation_key": "YAHOO:EARNINGS:AR:YPF:2026-08-10",
                "issuer_id": "AR:YPF",
                "ticker": "YPFD",
                "event_date": date(2026, 8, 10),
                "event_time_hint": "after_close",
                "source": "YAHOO",
                "confidence": 0.75,
                "lifecycle_status": "ANNOUNCED",
                "fiscal_year": 2026,
                "fiscal_quarter": 2,
                "raw_payload": {"earnings_phase": "scheduled", "eps_estimate": 2.0},
            },
            {
                "observation_key": "YAHOO:EARNINGS:AR:YPF:2026-08-11",
                "issuer_id": "AR:YPF",
                "ticker": "YPFD",
                "event_date": date(2026, 8, 11),
                "event_time_hint": "unknown",
                "source": "YAHOO",
                "confidence": 0.75,
                "lifecycle_status": "ANNOUNCED",
                "raw_payload": {
                    "earnings_phase": "post_reported",
                    "reported_eps": 3.07,
                    "surprise_pct": 15.4,
                },
            },
        ]
    )

    assert len(events) == 1
    assert events[0].event_date == date(2026, 8, 10)
    assert events[0].fiscal_label == "Q2 2026"
    assert events[0].reported_eps == 3.07

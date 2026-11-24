from datetime import date

from src.core.market_calendar import (
    get_market_closure,
    get_market_special_session,
    is_settlement_day,
    is_trading_day,
    load_market_closures,
    load_market_special_sessions,
    market_closed_reason,
    market_session_note,
)


def test_market_calendar_loads_2026_closures():
    closures = load_market_closures()

    assert len([day for day in closures if day.year == 2026]) == 17
    assert closures[date(2026, 2, 16)].name == "Carnaval"
    assert closures[date(2026, 12, 31)].category == "sin_negociacion_ni_liquidacion"


def test_market_calendar_loads_2026_special_sessions():
    sessions = load_market_special_sessions()

    assert len([day for day in sessions if day.year == 2026]) == 5
    assert sessions[date(2026, 7, 10)].category == "sin_liquidacion_con_negociacion"
    assert sessions[date(2026, 7, 10)].trading is True
    assert sessions[date(2026, 7, 10)].settlement is False


def test_market_calendar_rejects_holidays_and_no_trading_days():
    assert not is_trading_day(date(2026, 2, 16))
    assert not is_trading_day(date(2026, 3, 24))
    assert not is_trading_day(date(2026, 12, 31))
    assert not is_trading_day(date(2026, 6, 20))


def test_market_calendar_accepts_trading_without_settlement_days():
    for day in (date(2026, 7, 10), date(2026, 11, 6), date(2026, 12, 24)):
        assert is_trading_day(day)
        assert not is_settlement_day(day)
        assert market_closed_reason(day) is None
        assert "sin_liquidacion_con_negociacion" in market_session_note(day)
        assert get_market_special_session(day) is not None


def test_market_calendar_accepts_regular_weekday():
    assert is_trading_day(date(2026, 5, 20))
    assert is_settlement_day(date(2026, 5, 20))
    assert get_market_closure(date(2026, 5, 20)) is None
    assert get_market_special_session(date(2026, 5, 20)) is None
    assert market_closed_reason(date(2026, 5, 20)) is None
    assert market_session_note(date(2026, 5, 20)) is None


def test_market_calendar_reports_closure_reason():
    reason = market_closed_reason(date(2026, 11, 23))

    assert "feriado_trasladable" in reason
    assert "Soberania" in reason

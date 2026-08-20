from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from src.analysis.radar_setup_alerts import (
    RADAR_SETUP_ALERT_SCHEMA_SQL,
    RadarSetupAlertStore,
    evaluate_setup_alert_candidate,
    parse_radar_setup_callback,
    radar_setup_alert_keyboard,
    render_radar_setup_alert,
)
from src.collector.notifier import TelegramNotifier
from src.core.telegram_format import validate_telegram_html
from src.scheduler import runner


ART_TZ = ZoneInfo("America/Argentina/Buenos_Aires")


def _candidate_row(now: datetime) -> dict:
    return {
        "snapshot_id": 41,
        "owner_chat_id": 123,
        "ticker": "AMZN",
        "asset_type": "CEDEAR",
        "scoring_version": "radar-v2:test",
        "setup_shadow_version": "radar-setup-shadow-v1",
        "captured_session": now.date() - timedelta(days=1),
        "reference_ts": now - timedelta(days=1),
        "setup_percentile": 0.92,
        "setup_score": 42.0,
        "readiness_state": "PRE_BREAKOUT",
        "trigger_price": 122.8,
        "invalidation_price": 116.5,
        "target_price": 140.0,
        "setup_risk_reward": 2.1,
        "feature_quality_flag": "PARTIAL",
        "setup_warnings": ["cedear_ccl_not_separated"],
        "in_portfolio": False,
        "manual_event_risk": None,
        "market_price_ts": now - timedelta(minutes=2),
        "observed_price": 123.4,
    }


def test_candidate_gate_accepts_fresh_ce_dear_top_quintile_trigger():
    now = datetime(2026, 8, 20, 12, 2, tzinfo=ART_TZ)
    candidate = evaluate_setup_alert_candidate(
        _candidate_row(now),
        observed_at=now,
        run_type="12:00_MARKET",
    )

    assert candidate is not None
    assert candidate.ticker == "AMZN"
    assert candidate.setup_percentile == pytest.approx(0.92)
    assert candidate.extension_pct == pytest.approx(123.4 / 122.8 - 1)
    assert candidate.is_preclose is False


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("asset_type", "ACCION"),
        ("in_portfolio", True),
        ("current_in_portfolio", True),
        ("setup_percentile", 0.79),
        ("setup_risk_reward", 1.99),
        ("observed_price", 121.0),
        ("manual_event_risk", "earnings_window"),
        ("readiness_state", "TRIGGERED"),
    ],
)
def test_candidate_gate_rejects_non_actionable_rows(field, value):
    now = datetime(2026, 8, 20, 12, 2, tzinfo=ART_TZ)
    row = _candidate_row(now)
    row[field] = value

    assert evaluate_setup_alert_candidate(
        row,
        observed_at=now,
        run_type="12:00_MARKET",
    ) is None


def test_candidate_gate_rejects_stale_and_extended_prices():
    now = datetime(2026, 8, 20, 12, 2, tzinfo=ART_TZ)
    stale = _candidate_row(now)
    stale["market_price_ts"] = now - timedelta(minutes=20)
    assert evaluate_setup_alert_candidate(
        stale,
        observed_at=now,
        run_type="12:00_MARKET",
    ) is None

    stale_snapshot = _candidate_row(now)
    stale_snapshot["captured_session"] = now.date() - timedelta(days=8)
    assert evaluate_setup_alert_candidate(
        stale_snapshot,
        observed_at=now,
        run_type="12:00_MARKET",
    ) is None

    extended = _candidate_row(now)
    extended["observed_price"] = 131.0
    assert evaluate_setup_alert_candidate(
        extended,
        observed_at=now,
        run_type="12:00_MARKET",
    ) is None


def test_alert_message_is_explicitly_shadow_and_buttons_are_bounded():
    now = datetime(2026, 8, 20, 16, 42, tzinfo=ART_TZ)
    candidate = evaluate_setup_alert_candidate(
        _candidate_row(now),
        observed_at=now,
        run_type="16:40_MARKET",
    )
    assert candidate is not None
    alert = {"id": 77, **candidate.__dict__, "metadata": {"run_type": candidate.run_type}}

    text = render_radar_setup_alert(alert)
    keyboard = radar_setup_alert_keyboard(77)

    assert "SETUP ACTIVADO · AMZN" in text
    assert "percentil 92" in text
    assert "no genera órdenes automáticas" in text
    assert "no equivale a comprar" in text
    assert "Pre-cierre" in text
    assert validate_telegram_html(text) == (True, [])
    callbacks = [button["callback_data"] for row in keyboard for button in row]
    assert callbacks == ["rs:view:77", "rs:follow:77", "rs:dismiss:77"]
    assert all(len(value.encode("utf-8")) <= 64 for value in callbacks)


def test_callback_parser_and_schema_stay_outside_decision_log():
    assert parse_radar_setup_callback("rs:follow:77") == ("follow", 77)
    assert parse_radar_setup_callback("rs:buy:77") is None
    assert parse_radar_setup_callback("radar:follow:77") is None
    lowered = RADAR_SETUP_ALERT_SCHEMA_SQL.lower()
    assert "create table if not exists radar_setup_alerts" in lowered
    assert "decision_log" not in lowered
    assert "unique (snapshot_id, alert_type)" in lowered
    assert "broker_fill_id" in lowered
    assert "matched_owner_ticker_time" not in lowered


def test_follow_reconciliation_requires_owner_ticker_time_and_buy():
    class _Connection:
        def __init__(self):
            self.calls = []

        async def execute(self, statement, *args):
            self.calls.append((statement, args))
            if "WITH candidate_matches" in statement:
                return "UPDATE 1"
            return "OK"

    class _Acquire:
        def __init__(self, conn):
            self.conn = conn

        async def __aenter__(self):
            return self.conn

        async def __aexit__(self, *_args):
            return False

    class _Pool:
        def __init__(self, conn):
            self.conn = conn

        def acquire(self):
            return _Acquire(self.conn)

    conn = _Connection()
    count = asyncio.run(
        RadarSetupAlertStore(_Pool(conn)).reconcile_followed_fills(
            owner_chat_id=123,
        )
    )

    assert count == 1
    statement, args = next(
        call for call in conn.calls if "WITH candidate_matches" in call[0]
    )
    assert "f.owner_chat_id = a.owner_chat_id" in statement
    assert "UPPER(f.ticker) = UPPER(a.ticker)" in statement
    assert "f.side = 'BUY'" in statement
    assert "f.executed_at >= a.user_action_at" in statement
    assert args == (123, 16)


def test_reservation_revalidates_latest_portfolio_before_alerting():
    class _Transaction:
        async def __aenter__(self):
            return None

        async def __aexit__(self, *_args):
            return False

    class _Connection:
        def __init__(self):
            self.fetch_calls = []

        async def execute(self, *_args):
            return "OK"

        async def fetch(self, statement, *_args):
            self.fetch_calls.append(statement)
            return []

        def transaction(self):
            return _Transaction()

    class _Acquire:
        def __init__(self, conn):
            self.conn = conn

        async def __aenter__(self):
            return self.conn

        async def __aexit__(self, *_args):
            return False

    class _Pool:
        def __init__(self, conn):
            self.conn = conn

        def acquire(self):
            return _Acquire(self.conn)

    conn = _Connection()
    result = asyncio.run(
        RadarSetupAlertStore(_Pool(conn)).reserve_trigger_alerts(
            owner_chat_id=123,
            observed_at=datetime(2026, 8, 20, 12, 2, tzinfo=ART_TZ),
            run_type="12:00_MARKET",
        )
    )

    assert result == []
    candidate_sql = next(sql for sql in conn.fetch_calls if "latest_run AS" in sql)
    assert "latest_portfolio AS" in candidate_sql
    assert "owner_chat_id = $1 OR owner_chat_id IS NULL" in candidate_sql
    assert "current_holdings AS" in candidate_sql
    assert "current_in_portfolio" in candidate_sql


@pytest.mark.parametrize(
    ("requested", "stored", "conflict"),
    [("FOLLOW", "FOLLOW", False), ("DISMISS", "FOLLOW", True)],
)
def test_first_user_action_is_immutable(requested, stored, conflict):
    class _Connection:
        def __init__(self):
            self.fetchrow_calls = []

        async def execute(self, *_args):
            return "OK"

        async def fetchrow(self, statement, *_args):
            self.fetchrow_calls.append(statement)
            if "UPDATE radar_setup_alerts" in statement:
                return None
            return {"id": 77, "owner_chat_id": 123, "user_action": stored}

    class _Acquire:
        def __init__(self, conn):
            self.conn = conn

        async def __aenter__(self):
            return self.conn

        async def __aexit__(self, *_args):
            return False

    class _Pool:
        def __init__(self, conn):
            self.conn = conn

        def acquire(self):
            return _Acquire(self.conn)

    conn = _Connection()
    result = asyncio.run(
        RadarSetupAlertStore(_Pool(conn)).record_user_action(
            77,
            owner_chat_id=123,
            action=requested,
        )
    )

    assert result is not None
    assert result["action_recorded"] is False
    assert result["action_conflict"] is conflict
    update_sql = next(sql for sql in conn.fetchrow_calls if "UPDATE" in sql)
    assert "AND user_action IS NULL" in update_sql


def test_notifier_returns_message_id_with_inline_keyboard(monkeypatch):
    captured = {}

    class _Response:
        status_code = 200
        text = "ok"

        @staticmethod
        def json():
            return {"ok": True, "result": {"message_id": 991}}

    def _post(url, data, timeout):
        captured.update(url=url, data=data, timeout=timeout)
        return _Response()

    monkeypatch.setattr("src.collector.notifier.requests.post", _post)
    notifier = TelegramNotifier("token", "123")
    message_id = notifier.send_with_inline_keyboard(
        "<b>Setup</b>",
        [[{"text": "Seguir", "callback_data": "rs:follow:1"}]],
    )

    assert message_id == 991
    assert captured["data"]["chat_id"] == "123"
    assert "rs:follow:1" in captured["data"]["reply_markup"]


def test_market_refresh_runs_alert_watcher_only_after_success(monkeypatch):
    calls = []

    async def _refresh(**_kwargs):
        return {"ok": True, "market_rows": 249, "acciones": 20, "cedears": 229}

    async def _alerts(*, run_type, observed_at):
        calls.append((run_type, observed_at))
        return {"status": "OK", "reserved": 1, "sent": 1, "failed": 0}

    monkeypatch.setattr(runner, "request_portfolio_refresh", _refresh)
    monkeypatch.setattr(runner, "run_radar_setup_intraday_alerts", _alerts)
    monkeypatch.setattr(runner, "RADAR_INTRADAY_SETUP_ALERTS_ENABLED", True)
    monkeypatch.setattr(runner, "_is_business_day", lambda *_args: True)

    result = asyncio.run(runner.run_market_refresh("12:00_MARKET"))

    assert result["success"] is True
    assert result["radar_setup_alerts"]["sent"] == 1
    assert calls and calls[0][0] == "12:00_MARKET"


def test_market_refresh_does_not_alert_when_refresh_fails(monkeypatch):
    calls = []

    async def _refresh(**_kwargs):
        return {"ok": False, "market_rows": 0, "error": "refresh_failed"}

    async def _alerts(**kwargs):
        calls.append(kwargs)
        return {}

    monkeypatch.setattr(runner, "request_portfolio_refresh", _refresh)
    monkeypatch.setattr(runner, "run_radar_setup_intraday_alerts", _alerts)
    monkeypatch.setattr(runner, "RADAR_INTRADAY_SETUP_ALERTS_ENABLED", True)
    monkeypatch.setattr(runner, "_is_business_day", lambda *_args: True)

    result = asyncio.run(runner.run_market_refresh("10:40_MARKET"))

    assert result["success"] is False
    assert calls == []


def test_market_refresh_keeps_success_when_alert_watcher_fails(monkeypatch):
    async def _refresh(**_kwargs):
        return {"ok": True, "market_rows": 249, "acciones": 20, "cedears": 229}

    async def _alerts(**_kwargs):
        raise RuntimeError("telegram_down")

    monkeypatch.setattr(runner, "request_portfolio_refresh", _refresh)
    monkeypatch.setattr(runner, "run_radar_setup_intraday_alerts", _alerts)
    monkeypatch.setattr(runner, "RADAR_INTRADAY_SETUP_ALERTS_ENABLED", True)
    monkeypatch.setattr(runner, "_is_business_day", lambda *_args: True)

    result = asyncio.run(runner.run_market_refresh("12:00_MARKET"))

    assert result["success"] is True
    assert result["prices"] == 249
    assert result["radar_setup_alerts"] == {
        "status": "ERROR",
        "error": "telegram_down",
    }


@pytest.mark.parametrize(("registered", "expected"), [(True, 123), (False, None)])
def test_fill_owner_requires_registered_bot_user(registered, expected):
    class _Connection:
        async def fetchval(self, statement, chat_id):
            assert "FROM bot_users" in statement
            assert chat_id == 123
            return registered

    class _Acquire:
        async def __aenter__(self):
            return _Connection()

        async def __aexit__(self, *_args):
            return False

    class _Pool:
        def acquire(self):
            return _Acquire()

    class _Db:
        async def get_pool(self):
            return _Pool()

    result = asyncio.run(runner._registered_fill_owner_chat_id(_Db(), "123"))

    assert result == expected

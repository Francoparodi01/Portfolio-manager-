import asyncio
import json
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from src.analysis.preclose_alerts import PrecloseAlert, build_preclose_alerts, render_preclose_alerts
from src.collector.db import PortfolioDatabase


ART_TZ = ZoneInfo("America/Argentina/Buenos_Aires")


def _position(ticker="UPST", market_value=500_000, quantity=10):
    return {
        "ticker": ticker,
        "market_value": market_value,
        "quantity": quantity,
    }


def _price(ticker="UPST", price=96.5, now=None):
    now = now or datetime(2026, 6, 25, 16, 45, tzinfo=ART_TZ)
    return {
        "ticker": ticker,
        "last_price": price,
        "change_pct_1d": None,
        "ts": now,
    }


def test_preclose_sell_watch_for_concentrated_intraday_deterioration():
    now = datetime(2026, 6, 25, 16, 45, tzinfo=ART_TZ)

    alerts = build_preclose_alerts(
        positions=[_position()],
        latest_prices=[_price(price=96.5, now=now)],
        previous_closes={"UPST": 100.0},
        total_ars=1_000_000,
        now=now,
    )

    assert len(alerts) == 1
    assert alerts[0].ticker == "UPST"
    assert alerts[0].alert_type == "PRE_CIERRE_SELL_WATCH"
    assert alerts[0].change_pct == -0.03500000000000003
    assert "posición concentrada" in alerts[0].reason


def test_preclose_buy_watch_when_price_confirms_positive_sentiment():
    now = datetime(2026, 6, 25, 16, 45, tzinfo=ART_TZ)
    sentiment = {
        "MU": SimpleNamespace(
            score=0.22,
            confidence=0.45,
            event_count=3,
            high_impact_count=1,
            top_summary="Earnings beat with strong guidance",
        )
    }

    alerts = build_preclose_alerts(
        positions=[_position("MU", market_value=250_000)],
        latest_prices=[_price("MU", price=103.0, now=now)],
        previous_closes={"MU": 100.0},
        total_ars=1_000_000,
        sentiment_contexts=sentiment,
        now=now,
    )

    assert len(alerts) == 1
    assert alerts[0].alert_type == "PRE_CIERRE_BUY_WATCH"
    assert "sentiment confirmado" in alerts[0].reason
    assert "Earnings beat" in render_preclose_alerts(alerts, slot="16:45")


def test_preclose_no_perseguir_for_extended_intraday_rally():
    now = datetime(2026, 6, 25, 16, 45, tzinfo=ART_TZ)

    alerts = build_preclose_alerts(
        positions=[_position("NVDA", market_value=180_000)],
        latest_prices=[_price("NVDA", price=108.0, now=now)],
        previous_closes={"NVDA": 100.0},
        total_ars=1_000_000,
        now=now,
    )

    assert len(alerts) == 1
    assert alerts[0].alert_type == "NO_PERSEGUIR"
    assert "suba intradía extendida" in alerts[0].reason


def test_preclose_skips_stale_price():
    now = datetime(2026, 6, 25, 16, 45, tzinfo=ART_TZ)
    old_ts = datetime(2026, 6, 25, 15, 50, tzinfo=ART_TZ)

    alerts = build_preclose_alerts(
        positions=[_position()],
        latest_prices=[_price(price=90.0, now=old_ts)],
        previous_closes={"UPST": 100.0},
        total_ars=1_000_000,
        now=now,
        max_price_age_seconds=20 * 60,
    )

    assert alerts == []


def test_save_preclose_alerts_uses_parallel_table_without_decision_log():
    class _Conn:
        def __init__(self):
            self.sql = []
            self.params = []

        async def execute(self, sql, *params):
            self.sql.append(sql)
            self.params.append(params)
            return "INSERT 0 1"

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

    now = datetime(2026, 6, 25, 16, 45, tzinfo=ART_TZ)
    alert = PrecloseAlert(
        ticker="UPST",
        alert_type="PRE_CIERRE_SELL_WATCH",
        severity="HIGH",
        current_price=96.5,
        reference_price=100.0,
        change_pct=-0.035,
        evidence={"sentiment": {"score": -0.2}},
    )
    db = PortfolioDatabase("postgresql://user:pass@localhost/db")
    db._pool = _Pool()

    saved = asyncio.run(db.save_preclose_alerts([alert], alert_ts=now, slot="16:45"))

    assert saved == 1
    assert any("CREATE TABLE IF NOT EXISTS intraday_preclose_alerts" in sql for sql in db._pool.conn.sql)
    insert_sql = db._pool.conn.sql[-1]
    assert "intraday_preclose_alerts" in insert_sql
    assert "decision_log" not in insert_sql
    payload = json.loads(db._pool.conn.params[-1][-1])
    assert payload["sentiment"]["score"] == -0.2

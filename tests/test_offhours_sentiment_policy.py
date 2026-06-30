import sys
import types
import asyncio
from datetime import datetime


redis_module = types.ModuleType("redis")
redis_asyncio_module = types.ModuleType("redis.asyncio")
redis_asyncio_module.from_url = lambda *_args, **_kwargs: object()
redis_module.asyncio = redis_asyncio_module
sys.modules.setdefault("redis", redis_module)
sys.modules.setdefault("redis.asyncio", redis_asyncio_module)

from scripts.run_analysis import ART_TZ, _analysis_run_policy
from src.scheduler import runner


def test_saturday_analysis_is_exploratory_and_never_persists():
    saturday = datetime(2026, 6, 20, 12, 0, tzinfo=ART_TZ)

    no_persist, run_intent, off_market = _analysis_run_policy(
        False,
        "formal_plan",
        saturday,
    )

    assert no_persist is True
    assert run_intent == "exploratory"
    assert off_market is True


def test_business_day_analysis_preserves_requested_policy():
    monday = datetime(2026, 6, 22, 12, 0, tzinfo=ART_TZ)

    assert _analysis_run_policy(False, "formal_plan", monday) == (
        False,
        "formal_plan",
        False,
    )


def test_nuclear_attack_is_an_immediate_offhours_risk_event():
    event = {
        "headline": "Israel launches nuclear attack on Iran",
        "summary": "Major regional escalation",
        "impact": "high",
        "confidence": 0.90,
        "score": -0.95,
    }

    assert runner._is_severe_offhours_sentiment_event(event)


def test_routine_forward_guidance_does_not_match_war_substring():
    event = {
        "headline": "Company raises forward guidance",
        "summary": "Strong demand",
        "impact": "high",
        "confidence": 0.90,
        "score": 0.50,
    }

    assert not runner._is_severe_offhours_sentiment_event(event)


def test_offhours_alert_explicitly_preserves_formal_plan():
    output = runner._render_offhours_sentiment_alert([
        {
            "ticker": None,
            "source": "reuters",
            "score": -0.95,
            "headline": "Nuclear attack triggers emergency response",
        }
    ])

    assert "Sentiment 24/7" in output
    assert "no modifica el último plan formal" in output
    assert "próxima rueda" in output


def test_risk_guard_filters_alerts_to_latest_snapshot_positions():
    class _Conn:
        def __init__(self):
            self.second_params = None

        async def fetch(self, sql, *params):
            if "latest_snapshot" in sql:
                return [{"ticker": "MU"}]
            self.second_params = params
            return [
                {
                    "ticker": "MU",
                    "price_at_decision": 100.0,
                    "stop_loss_pct": None,
                    "stop_loss_price": None,
                    "target_price": None,
                    "last_price": 90.0,
                },
                {
                    "ticker": "NFLX",
                    "price_at_decision": 100.0,
                    "stop_loss_pct": None,
                    "stop_loss_price": None,
                    "target_price": None,
                    "last_price": 70.0,
                },
            ]

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

    pool = _Pool()
    manager = runner.IntradayManager.__new__(runner.IntradayManager)

    alerts = asyncio.run(manager._compute_risk_alerts(pool))

    assert [alert.ticker for alert in alerts] == ["MU"]
    assert pool.conn.second_params == (["MU"],)


def test_scrape_portfolio_retry_reloads_page_before_second_attempt():
    class _Page:
        def __init__(self):
            self.reloads = 0

        async def reload(self, **_kwargs):
            self.reloads += 1

    class _Scraper:
        def __init__(self):
            self._page = _Page()
            self.calls = 0

        async def scrape_portfolio(self):
            self.calls += 1
            if self.calls == 1:
                raise TimeoutError("assetWrapper timeout")
            return types.SimpleNamespace(positions=["MU"])

    scraper = _Scraper()

    snapshot = asyncio.run(
        runner._scrape_portfolio_with_retries(
            scraper,
            "17:02_FULL",
            attempts=2,
            delay_seconds=0,
        )
    )

    assert snapshot.positions == ["MU"]
    assert scraper.calls == 2
    assert scraper._page.reloads == 1

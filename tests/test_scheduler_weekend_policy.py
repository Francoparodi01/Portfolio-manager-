import sys
import types
import asyncio
from datetime import datetime
from pathlib import Path

redis_module = types.ModuleType("redis")
redis_asyncio_module = types.ModuleType("redis.asyncio")
redis_asyncio_module.from_url = lambda *_args, **_kwargs: object()
redis_module.asyncio = redis_asyncio_module
sys.modules.setdefault("redis", redis_module)
sys.modules.setdefault("redis.asyncio", redis_asyncio_module)

from src.scheduler import runner


def test_business_day_cron_uses_weekday_window(monkeypatch):
    captured = {}

    class _FakeCronTrigger:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(runner, "CronTrigger", _FakeCronTrigger, raising=False)

    runner._business_day_cron(hour=17, minute=0)

    assert captured == {
        "day_of_week": "mon-fri",
        "hour": 17,
        "minute": 0,
        "timezone": runner.TIMEZONE,
    }


def test_bot_manual_commands_stay_outside_scheduler_path():
    source = Path("scripts/telegram_bot.py").read_text(encoding="utf-8")

    assert '["scripts/run_analysis.py", "--no-telegram"' in source
    assert '"scripts/run_performance.py"' in source
    assert '"--no-telegram"' in source
    assert '"scripts/run_opportunity.py"' in source
    assert "src.scheduler.runner" not in source


def test_scheduler_has_opening_portfolio_report_before_intraday_start():
    source = Path("src/scheduler/runner.py").read_text(encoding="utf-8")

    assert 'id="opening_portfolio_report"' in source
    assert "run_opening_portfolio_report_then_start_intraday" in source
    assert 'run_opening_portfolio_report("10:31_OPENING_PORTFOLIO")' in source
    assert '_business_day_cron(hour=10, minute=31)' in source
    assert '_business_day_cron(hour=10, minute=32)' not in source


def test_scheduler_runs_main_analysis_after_eod_candles():
    source = Path("src/scheduler/runner.py").read_text(encoding="utf-8")

    assert "async def run_daily_analysis" in source
    assert 'id="daily_analysis"' in source
    assert '_business_day_cron(hour=17, minute=12)' in source
    assert source.index('id="build_daily_candles"') < source.index('id="daily_analysis"')
    assert source.index('id="verify_daily_candles"') < source.index('id="daily_analysis"')


def test_scheduler_captures_auditable_radar_inside_market_hours():
    source = Path("src/scheduler/runner.py").read_text(encoding="utf-8")

    assert "async def run_radar_audit_capture" in source
    assert 'id="radar_audit_capture"' in source
    assert '_business_day_cron(hour=16, minute=50)' in source
    capture = source[source.index("async def run_radar_audit_capture"):]
    capture = capture[:capture.index("async def ", 10)]
    assert '"--no-telegram"' in capture
    assert '"--no-persist"' not in capture


def test_market_window_rejects_argentina_holiday():
    holiday = datetime(2026, 3, 24, 11, 0, tzinfo=runner.ART_TZ)

    assert not runner._is_business_day(holiday)
    assert not runner._is_market_window(holiday)


def test_opening_wrapper_does_not_start_intraday_on_holiday(monkeypatch):
    holiday = datetime(2026, 3, 24, 10, 31, tzinfo=runner.ART_TZ)

    async def _fail_start():
        raise AssertionError("intraday should not start on market holiday")

    monkeypatch.setattr(runner, "_now_art", lambda: holiday)
    monkeypatch.setattr(runner, "start_intraday_loops", _fail_start)

    asyncio.run(runner.run_opening_portfolio_report_then_start_intraday())

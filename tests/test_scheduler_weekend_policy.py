import sys
import types
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
    assert '"scripts/run_performance.py", "--days", "90", "--no-telegram"' in source
    assert '"scripts/run_opportunity.py"' in source
    assert "src.scheduler.runner" not in source

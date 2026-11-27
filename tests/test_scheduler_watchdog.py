import asyncio
from datetime import datetime, timedelta

import pytest

from src.scheduler import watchdog as wd

ART = wd.ART


def _at(hour, minute=0, day=26):
    return datetime(2026, 11, day, hour, minute, tzinfo=ART)


def test_slot_missing_only_after_its_due_time_and_until_its_last_retry():
    artifacts = {"plans": [_at(9, 50)], "market": [_at(10, 41)]}
    names = lambda now: [s.name for s in wd.missing_slots(now, artifacts)]
    assert "analysis_11" not in names(_at(11, 30))  # not due yet
    assert "analysis_11" in names(_at(11, 50))  # yesterday-ish plan before 11:00 does not count
    assert "analysis_11" not in names(_at(12, 45))  # too late to re-run usefully
    assert "market_1040" not in names(_at(11, 50))  # 10:41 tick is there


def test_artifact_after_slot_start_satisfies_it_and_attempted_is_not_retried():
    artifacts = {"plans": [_at(11, 20)]}
    assert "analysis_11" not in [s.name for s in wd.missing_slots(_at(11, 50), artifacts)]
    assert "analysis_13" not in [s.name for s in wd.missing_slots(_at(13, 50), {}, attempted={"analysis_13"})]


class _FakeClock:
    def __init__(self, now):
        self.value = now

    def __call__(self):
        return self.value


def _watchdog(monkeypatch, artifacts_sequence, job):
    calls = iter(artifacts_sequence)

    async def fake_fetch(url, day):
        return next(calls)

    monkeypatch.setattr(wd, "fetch_artifacts", fake_fetch)
    sent = []
    dog = wd.Watchdog(
        database_url="postgresql://unused",
        jobs={"analysis_11": job},
        notify=sent.append,
        now=_FakeClock(_at(11, 50)),
        is_business_day=lambda now: True,
    )
    return dog, sent


def test_watchdog_reruns_a_missed_job_once_and_reports_recovery(monkeypatch):
    ran = []

    async def job():
        ran.append(1)

    before = {"market": [_at(10, 41)], "portfolio": [_at(10, 32)]}
    after = {**before, "plans": [_at(11, 52)]}
    dog, sent = _watchdog(monkeypatch, [before, after, after], job)
    assert asyncio.run(dog.check()) == ["analysis_11:recovered"]
    assert ran == [1] and "recuperado" in sent[-1]
    # Second check of the same day: already attempted, nothing re-run.
    assert asyncio.run(dog.check()) == []
    assert ran == [1]


def test_watchdog_reports_a_job_that_still_leaves_no_trace(monkeypatch):
    async def job():
        raise RuntimeError("refresh_failed")

    empty = {"market": [_at(10, 41)], "portfolio": [_at(10, 32)]}
    dog, sent = _watchdog(monkeypatch, [empty, empty], job)
    assert asyncio.run(dog.check()) == ["analysis_11:failed"]
    assert "sigue faltando" in sent[-1] and "refresh_failed" in sent[-1]


def test_watchdog_does_nothing_on_holidays(monkeypatch):
    dog, sent = _watchdog(monkeypatch, [], lambda: None)
    dog.is_business_day = lambda now: False
    assert asyncio.run(dog.check()) == [] and sent == []


def test_formal_analysis_falls_back_to_a_recent_snapshot(monkeypatch):
    pytest.importorskip("apscheduler")
    import src.scheduler.runner as runner

    sent, cmds = [], []

    class Notifier:
        def __init__(self, *a):
            pass

        def send_raw(self, text):
            sent.append(text)

        def notify_critical_error(self, *a):
            sent.append("critical")

    async def refresh(**kwargs):
        return {"ok": False, "error": "timeout"}

    async def age(url, owner):
        return 3.0

    async def no_sleep(_):
        return None

    class Proc:
        returncode = 0

        async def communicate(self):
            return b"x" * 10, b""

    async def spawn(*cmd, **kwargs):
        cmds.append(cmd)
        return Proc()

    async def verify():
        return None

    cfg = type("C", (), {"scraper": type("S", (), {"telegram_bot_token": "t", "telegram_chat_id": "123"})(),
                         "database": type("D", (), {"url": "postgresql://unused"})()})()
    monkeypatch.setattr(runner, "get_config", lambda: cfg)
    monkeypatch.setattr(runner, "TelegramNotifier", Notifier)
    monkeypatch.setattr(runner, "_is_business_day", lambda *a: True)
    monkeypatch.setattr(runner, "_is_market_window", lambda *a: True)
    monkeypatch.setattr(runner, "request_portfolio_refresh", refresh)
    monkeypatch.setattr(runner, "_latest_snapshot_age_hours", age)
    monkeypatch.setattr(runner.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(runner.asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr(runner, "run_verify_decision_prices", verify)

    asyncio.run(runner.run_daily_analysis(formal=True, notify=False))
    assert cmds, "the analysis must run on the recent snapshot instead of skipping"
    assert any("Corre igual con la cartera de hace 3.0 h" in t for t in sent)

    async def old(url, owner):
        return 72.0

    cmds.clear()
    monkeypatch.setattr(runner, "_latest_snapshot_age_hours", old)
    asyncio.run(runner.run_daily_analysis(formal=True, notify=False))
    assert not cmds and any("no corrió" in t for t in sent)

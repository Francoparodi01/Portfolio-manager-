from pathlib import Path
from datetime import datetime, timezone

from src.core.report_artifacts import _market_bucket, _runner_fingerprint


ROOT = Path(__file__).resolve().parents[1]


def test_main_menu_exposes_ticker_analysis_button_and_command():
    source = (ROOT / "scripts" / "telegram_bot.py").read_text(encoding="utf-8")

    assert '("ticker", "Análisis por ticker")' in source
    assert 'callback_data="ticker_analysis"' in source
    assert '"ticker_analysis": action_ticker_prompt' in source
    assert 'CommandHandler("ticker",' in source
    assert 'CommandHandler("tecnico",' in source
    assert '"scripts/run_ticker_analysis.py"' in source


def test_ticker_button_is_read_only_prompt():
    source = (ROOT / "scripts" / "telegram_bot.py").read_text(encoding="utf-8")
    action_start = source.index("async def action_ticker_prompt")
    action_end = source.index("async def action_ticker_analysis", action_start)
    action = source[action_start:action_end]

    assert "/ticker NVDA" in action
    assert "no guarda decision_log" in action
    assert "no cambia thresholds" in action


def test_ticker_action_sends_multiple_chart_paths():
    source = (ROOT / "scripts" / "telegram_bot.py").read_text(encoding="utf-8")
    action_start = source.index("async def action_ticker_analysis")
    action_end = source.index("async def action_performance", action_start)
    action = source[action_start:action_end]

    assert "_ticker_chart_paths(out, chart_path)" in action
    assert "for index, path in enumerate(chart_paths" in action
    assert "send_photo" in action
    assert "line.startswith(\"[chart]\")" in action


def test_weekly_analysis_uses_report_cache_and_full_analysis_forces_sync():
    source = (ROOT / "scripts" / "telegram_bot.py").read_text(encoding="utf-8")
    sync_start = source.index("async def sync_operational_state")
    sync_end = source.index("def main_keyboard", sync_start)
    sync = source[sync_start:sync_end]
    action_start = source.index("async def action_analysis(")
    action_end = source.index("async def action_analysis_test", action_start)
    action = source[action_start:action_end]
    full_start = source.index("async def action_analysis_full(")
    full_end = source.index("async def action_analysis_debug", full_start)
    full_action = source[full_start:full_end]

    assert "await _has_recent_operational_sync(owner_chat_id)" in sync
    assert "await _mark_operational_sync(owner_chat_id)" in sync
    assert '_load_cached_report("analysis", chat_id)' in action
    assert "sync_operational_state" in action
    assert action.index("sync_operational_state") < action.index("_load_cached_report")
    assert "sync_operational_state(full=True, owner_chat_id=chat_id)" in full_action
    assert "[BOT][ANALYSIS] cache_hit=" in action


def test_historical_reports_do_not_force_cocos_scrape():
    source = (ROOT / "scripts" / "telegram_bot.py").read_text(encoding="utf-8")
    action_names = (
        "action_performance",
        "action_override_audit",
        "action_decision_ledger",
    )

    for name in action_names:
        start = source.index(f"async def {name}(")
        end = source.index("\nasync def ", start + 1)
        action = source[start:end]
        assert "sync_operational_state" not in action


def test_report_fingerprint_uses_fifteen_minute_market_bucket():
    first = datetime(2026, 8, 10, 14, 1, tzinfo=timezone.utc)
    second = datetime(2026, 8, 10, 14, 14, tzinfo=timezone.utc)
    next_bucket = datetime(2026, 8, 10, 14, 15, tzinfo=timezone.utc)

    assert _market_bucket(first) == _market_bucket(second)
    assert _market_bucket(first) != _market_bucket(next_bucket)
    assert _runner_fingerprint("analysis") != "unknown"
    assert _runner_fingerprint("analysis") != _runner_fingerprint("radar")


def test_decisions_shell_does_not_fetch_ingestion_globally():
    shell = (ROOT / "frontend" / "src" / "components" / "layout" / "AppShell.tsx").read_text(encoding="utf-8")
    decisions = (ROOT / "frontend" / "src" / "pages" / "DecisionsPage.tsx").read_text(encoding="utf-8")

    assert "useIngestionQuery" not in shell
    assert 'getString(health.data?.database, "latest_portfolio_at")' in shell
    assert "useAuditTimelineQuery(period, 120)" in decisions


def test_monitor_ingestion_cache_is_mutated_without_replacing_app_state():
    source = (ROOT / "src" / "monitor" / "api.py").read_text(encoding="utf-8")

    assert 'app["ingestion_cache"] = {}' in source
    assert "cache.update(stored_at=now_mono, payload=payload)" in source
    assert 'request.app["ingestion_cache"] =' not in source


def test_history_loads_are_bounded_to_five_concurrent_queries():
    analysis = (ROOT / "scripts" / "run_analysis.py").read_text(encoding="utf-8")
    radar = (ROOT / "scripts" / "run_opportunity.py").read_text(encoding="utf-8")

    assert "semaphore = asyncio.Semaphore(5)" in analysis
    assert "semaphore = asyncio.Semaphore(5)" in radar
    assert "await asyncio.gather" in analysis
    assert "await asyncio.gather" in radar

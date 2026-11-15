from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_main_menu_exposes_shadow_report_button_and_command():
    source = (ROOT / "scripts" / "telegram_bot.py").read_text(encoding="utf-8")

    assert 'InlineKeyboardButton("🔬 Shadow",' in source
    assert 'callback_data="shadow"' in source
    assert '("shadow", "Tesis shadow 5/20/40")' in source
    assert 'CommandHandler("shadow",' in source
    assert '"shadow":         action_shadow' in source


def test_shadow_button_reads_latest_persisted_run_in_telegram_format():
    source = (ROOT / "scripts" / "telegram_bot.py").read_text(encoding="utf-8")
    action_start = source.index("async def action_shadow")
    action_end = source.index("async def action_radar_full", action_start)
    action = source[action_start:action_end]

    assert '"scripts/run_thesis_shadow.py"' in action
    assert '"--latest-report"' in action
    assert '"--telegram-format"' in action
    assert '"--no-persist"' not in action

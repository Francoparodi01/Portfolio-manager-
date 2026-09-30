from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "scripts" / "telegram_conversational.py"


def test_conversational_entrypoint_keeps_agent_and_exposes_shortcut_keyboard():
    text = SOURCE.read_text(encoding="utf-8")
    assert "CallbackQueryHandler" in text
    assert "InlineKeyboardButton" in text
    assert "InlineKeyboardMarkup" in text
    assert 'callback_data="quick:portfolio"' in text
    assert 'callback_data="quick:analysis"' in text
    assert 'callback_data="quick:radar"' in text
    assert 'callback_data="quick:performance"' in text
    assert 'callback_data="quick:analytics"' in text
    assert 'callback_data="quick:meta"' in text
    assert 'callback_data="quick:agent"' in text
    assert 'callback_data="quick:status"' in text


def test_shortcut_buttons_route_through_same_unified_harness():
    text = SOURCE.read_text(encoding="utf-8")
    assert "QUICK_ACTION_PROMPTS" in text
    assert "async def _run_harness_query" in text
    assert "await _run_harness_query(context, chat_id, prompt)" in text
    assert "run_message(" in text
    assert "legacy.action_portfolio" not in text
    assert "legacy.action_analysis" not in text


def test_visible_command_catalog_is_cleared_but_menu_bootstrap_remains_available():
    text = SOURCE.read_text(encoding="utf-8")
    assert "set_my_commands([])" in text
    assert "MenuButtonCommands" not in text
    assert 'CommandHandler("menu", start_handler)' in text


def test_text_gateway_is_self_contained_and_uses_unified_harness():
    text = SOURCE.read_text(encoding="utf-8")
    assert "MessageHandler(filters.TEXT, text_handler)" in text
    assert "await _run_harness_query(context, chat_id, text)" in text
    assert "run_message(" in text


def test_compose_deploys_conversational_harness_as_primary_telegram_surface():
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    override = (ROOT / "docker-compose.override.yml").read_text(encoding="utf-8")

    command = 'command: ["python", "scripts/telegram_conversational.py"]'
    assert command in compose
    assert command in override
    assert 'command: ["python", "scripts/telegram_bot.py"]' not in compose
    assert 'command: ["python", "scripts/telegram_bot_meta.py"]' not in override

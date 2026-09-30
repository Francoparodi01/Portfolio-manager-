from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "scripts" / "telegram_conversational.py"


def test_hybrid_surface_restores_original_menu_and_callbacks():
    text = SOURCE.read_text(encoding="utf-8")
    assert "CallbackQueryHandler" in text
    assert "await legacy.send_menu(context, int(update.effective_chat.id))" in text
    assert "await legacy.callback_handler(update, context)" in text
    assert 'raw_action == "agent_prompt"' in text
    assert 'CallbackQueryHandler(hybrid_callback_handler)' in text


def test_buttons_do_not_translate_into_conversational_prompts():
    text = SOURCE.read_text(encoding="utf-8")
    assert "QUICK_ACTION_PROMPTS" not in text
    assert "quick_keyboard" not in text
    assert 'callback_data="quick:portfolio"' not in text
    assert "await _run_harness_query(context, chat_id, prompt)" not in text
    assert "await legacy.callback_handler(update, context)" in text


def test_agent_text_still_uses_unified_harness():
    text = SOURCE.read_text(encoding="utf-8")
    assert "MessageHandler(filters.TEXT, text_handler)" in text
    assert "await _run_harness_query(context, chat_id, text)" in text
    assert "run_message(" in text
    assert '"agente": arg or "¿Cómo está mi cartera?"' in text


def test_visible_command_catalog_is_cleared_but_original_menu_bootstrap_remains_available():
    text = SOURCE.read_text(encoding="utf-8")
    assert "set_my_commands([])" in text
    assert "MenuButtonCommands" not in text
    assert 'CommandHandler("menu", start_handler)' in text


def test_compose_deploys_conversational_harness_as_primary_telegram_surface():
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    override = (ROOT / "docker-compose.override.yml").read_text(encoding="utf-8")

    command = 'command: ["python", "scripts/telegram_conversational.py"]'
    assert command in compose
    assert command in override
    assert 'command: ["python", "scripts/telegram_bot.py"]' not in compose
    assert 'command: ["python", "scripts/telegram_bot_meta.py"]' not in override

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "scripts" / "telegram_conversational.py"


def test_conversational_entrypoint_has_no_callback_or_keyboard_navigation():
    text = SOURCE.read_text(encoding="utf-8")
    assert "CallbackQueryHandler" not in text
    assert "InlineKeyboard" not in text
    assert "ReplyKeyboard" not in text


def test_visible_command_catalog_is_cleared_in_conversational_surface():
    text = SOURCE.read_text(encoding="utf-8")
    assert "set_my_commands([])" in text
    assert "MenuButtonCommands" not in text


def test_text_gateway_is_self_contained_and_uses_unified_harness():
    text = SOURCE.read_text(encoding="utf-8")
    assert "MessageHandler(filters.TEXT, text_handler)" in text
    assert "run_message(" in text


def test_compose_deploys_conversational_harness_as_primary_telegram_surface():
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    override = (ROOT / "docker-compose.override.yml").read_text(encoding="utf-8")

    command = 'command: ["python", "scripts/telegram_conversational.py"]'
    assert command in compose
    assert command in override
    assert 'command: ["python", "scripts/telegram_bot.py"]' not in compose
    assert 'command: ["python", "scripts/telegram_bot_meta.py"]' not in override

from pathlib import Path


SOURCE = Path(__file__).resolve().parents[1] / "scripts" / "telegram_conversational.py"


def test_conversational_entrypoint_has_no_callback_or_keyboard_navigation():
    text = SOURCE.read_text(encoding="utf-8")
    assert "CallbackQueryHandler" not in text
    assert "InlineKeyboard" not in text
    assert "ReplyKeyboard" not in text


def test_visible_command_catalog_is_cleared():
    text = SOURCE.read_text(encoding="utf-8")
    assert "set_my_commands([])" in text
    assert "MenuButtonCommands" not in text


def test_text_gateway_is_primary_surface():
    text = SOURCE.read_text(encoding="utf-8")
    assert "MessageHandler(filters.TEXT, text_handler)" in text
    assert "run_message(" in text


def test_compose_deploys_conversational_entrypoint():
    compose = (SOURCE.parents[1] / "docker-compose.yml").read_text(encoding="utf-8")
    assert 'command: ["python", "scripts/telegram_conversational.py"]' in compose

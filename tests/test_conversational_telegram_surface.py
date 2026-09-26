from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "scripts" / "telegram_conversational.py"


def test_conversational_entrypoint_has_no_callback_or_keyboard_navigation():
    """The dormant conversational experiment remains isolated if reused later."""
    text = SOURCE.read_text(encoding="utf-8")
    assert "CallbackQueryHandler" not in text
    assert "InlineKeyboard" not in text
    assert "ReplyKeyboard" not in text


def test_visible_command_catalog_is_cleared_in_dormant_conversational_surface():
    text = SOURCE.read_text(encoding="utf-8")
    assert "set_my_commands([])" in text
    assert "MenuButtonCommands" not in text


def test_text_gateway_remains_self_contained_but_not_deployed():
    text = SOURCE.read_text(encoding="utf-8")
    assert "MessageHandler(filters.TEXT, text_handler)" in text
    assert "run_message(" in text


def test_compose_deploys_classic_meta_bot_and_not_conversational_entrypoint():
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    override = (ROOT / "docker-compose.override.yml").read_text(encoding="utf-8")

    assert 'command: ["python", "scripts/telegram_bot.py"]' in compose
    assert 'command: ["python", "scripts/telegram_bot_meta.py"]' in override
    assert "scripts/telegram_conversational.py" not in compose
    assert "scripts/telegram_conversational.py" not in override

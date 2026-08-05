import sys
import types

from scripts import run_analysis


if "telegram" not in sys.modules:
    telegram = types.ModuleType("telegram")
    constants = types.ModuleType("telegram.constants")
    error = types.ModuleType("telegram.error")
    ext = types.ModuleType("telegram.ext")

    class _TelegramDummy:
        def __init__(self, *args, **kwargs):
            self.inline_keyboard = args[0] if args else kwargs.get("inline_keyboard")
            self.callback_data = kwargs.get("callback_data")

    class _FilterDummy:
        def __and__(self, other):
            return self

        def __invert__(self):
            return self

    class _BadRequest(Exception):
        pass

    telegram.InlineKeyboardButton = _TelegramDummy
    telegram.InlineKeyboardMarkup = _TelegramDummy
    telegram.Update = _TelegramDummy
    constants.ParseMode = types.SimpleNamespace(HTML="HTML")
    error.BadRequest = _BadRequest
    ext.Application = _TelegramDummy
    ext.CallbackQueryHandler = _TelegramDummy
    ext.CommandHandler = _TelegramDummy
    ext.ContextTypes = types.SimpleNamespace(DEFAULT_TYPE=object)
    ext.MessageHandler = _TelegramDummy
    ext.filters = types.SimpleNamespace(TEXT=_FilterDummy(), COMMAND=_FilterDummy())

    sys.modules["telegram"] = telegram
    sys.modules["telegram.constants"] = constants
    sys.modules["telegram.error"] = error
    sys.modules["telegram.ext"] = ext

from scripts.telegram_bot import (
    BOT_COMMAND_SPECS,
    CALLBACK_ALIASES,
    main_keyboard,
    split_message,
)


def test_compact_reason_keeps_operational_reason_without_premature_ellipsis():
    reason = (
        "Senal positiva, pero el cash $79,736 no alcanza un nominal minimo "
        "para ejecutar compra operable."
    )

    rendered = run_analysis._compact_reason(reason)

    assert rendered == reason
    assert not rendered.endswith("...")


def test_compact_reason_respects_explicit_limit_for_extreme_text():
    rendered = run_analysis._compact_reason("x" * 260, 80)

    assert len(rendered) <= 80
    assert rendered.endswith("...")


def test_compact_reason_does_not_replace_optimizer_context_with_generic_label():
    reason = "Optimizer sugeria reducir 14.3% a 8.2%, pero una senal neutral no confirma venta."

    rendered = run_analysis._compact_reason(reason)

    assert rendered == reason
    assert "optimizer diverge" not in rendered


def test_split_message_splits_oversized_single_lines():
    chunks = split_message("x" * 250, max_len=80)

    assert len(chunks) > 1
    assert all(len(chunk) <= 80 for chunk in chunks)
    assert "".join(chunks) == "x" * 250


def test_events_command_is_registered_in_telegram_menu():
    assert ("events", "Proximos balances") in BOT_COMMAND_SPECS
    assert CALLBACK_ALIASES["upcoming_events"] == "upcoming_events"


def test_main_keyboard_callbacks_are_all_routable():
    keyboard = main_keyboard()
    callbacks = {
        button.callback_data
        for row in keyboard.inline_keyboard
        for button in row
        if button.callback_data
    }

    assert callbacks
    assert callbacks - CALLBACK_ALIASES.keys() == set()

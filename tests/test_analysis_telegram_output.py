import sys
import types
from datetime import datetime, timezone
from pathlib import Path

from scripts import run_analysis


ROOT = Path(__file__).resolve().parents[1]


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
    compact_radar_report,
    help_text,
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
    assert ("events", "Próximos balances") in BOT_COMMAND_SPECS
    assert CALLBACK_ALIASES["upcoming_events"] == "upcoming_events"


def test_radar_metrics_command_and_callback_are_registered():
    assert ("radar_metricas", "Métricas prospectivas Radar") in BOT_COMMAND_SPECS
    assert CALLBACK_ALIASES["radar_metricas"] == "radar_metrics"


def test_native_command_menu_keeps_only_primary_workflows():
    visible = {command for command, _description in BOT_COMMAND_SPECS}

    assert {
        "menu",
        "help",
        "portfolio",
        "analisis",
        "events",
        "ticker",
        "radar",
        "radar_metricas",
        "mercado",
        "performance",
        "neto",
        "ledger",
        "bot_vs_humano",
        "status",
    } == visible
    assert {
        "analisis_test",
        "analisis_full",
        "analisis_debug",
        "shadow",
        "viability",
        "policy",
        "confianza",
    }.isdisjoint(visible)


def test_help_explains_operational_and_audit_boundaries():
    rendered = help_text()

    assert "No ejecuta órdenes" in rendered
    assert "Score: dirección de la señal, no probabilidad" in rendered
    assert "Radar/shadow/debug: auditoría o exploración" in rendered


def test_upcoming_events_refreshes_sources_before_rendering():
    source = (ROOT / "scripts" / "telegram_bot.py").read_text(encoding="utf-8")
    start = source.index("async def action_upcoming_events")
    end = source.index("async def action_ticker_prompt", start)

    assert '"--refresh"' in source[start:end]


def test_all_menu_callbacks_are_routable():
    from scripts.telegram_bot import audit_keyboard, results_keyboard

    keyboards = [main_keyboard(), results_keyboard(), audit_keyboard()]
    callbacks = {
        button.callback_data
        for keyboard in keyboards
        for row in keyboard.inline_keyboard
        for button in row if button.callback_data
    }

    assert callbacks
    assert callbacks - CALLBACK_ALIASES.keys() == set()


def test_secondary_menu_views_keep_navigation_in_one_message():
    from scripts.telegram_bot import menu_view

    results = menu_view("menu_results")
    audit = menu_view("menu_audit")
    home = menu_view("menu_home")

    assert results is not None and results[0] == "<b>RESULTADOS</b>"
    assert audit is not None and audit[0] == "<b>AUDITORÍA</b>"
    assert home is not None and "<b>QUANTIA</b>" in home[0]


def test_analysis_context_separates_execution_from_detection_time():
    lines = run_analysis._render_analysis_data_context(
        None,
        {
            "ticker": "YPFD",
            "movement_type": "SELL",
            "executed_at": datetime(2026, 8, 11, 3, 0, tzinfo=timezone.utc),
            "created_at": datetime(2026, 8, 12, 13, 32, tzinfo=timezone.utc),
            "executed_at_precision": "date_only",
        },
    )

    rendered = "\n".join(lines)
    assert "operacion 11/08 (hora no informada)" in rendered
    assert "detectado 12/08 10:32" in rendered
    assert "YPFD SELL (12/08 10:32)" not in rendered


def test_compact_radar_labels_idea_separately_from_portfolio_plan(monkeypatch):
    import scripts.telegram_bot as telegram_bot_module

    monkeypatch.setattr(telegram_bot_module, "_is_business_day_now", lambda: True)
    monkeypatch.setattr(telegram_bot_module, "_is_market_hours_now", lambda: True)
    monkeypatch.setattr(telegram_bot_module, "_market_closed_reason_now", lambda: None)
    full_report = """<b>🔭 Radar de oportunidades</b>
🔍 Universo: 10 tickers → 3 ideas rankeadas
✅ Estado operativo: <b>NORMAL</b>
   VIX: 15.0

<b>━━ GDX ━━</b>  🆕
Score: <code>+0.230</code> | Conv: <b>70%</b> | Precio: <b>$14500.00</b>
Edge: 🟢 <code>+0.063</code>
R/R <b>3.3x</b>
💰 Sizing sugerido: <b>9.0%</b> ≈ $210.305 ARS
🔬 Shadow: <b>SHADOW DÉBIL</b> — no perseguir sin pullback
🎯 <b>Lectura radar:</b> Abrir posición
"""

    compact = compact_radar_report(full_report)

    assert "Idea radar: Abrir posición" in compact
    assert "solo /analisis puede convertirlas en un plan de cartera" in compact
    assert "🎯: Abrir posición" not in compact

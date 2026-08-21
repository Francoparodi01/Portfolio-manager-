import asyncio
import sys
import types
from datetime import datetime, timezone
from pathlib import Path

from scripts import run_analysis
from src.collector.portfolio_quality import normalize_positions_with_fresh_market_prices


ROOT = Path(__file__).resolve().parents[1]


if "telegram" not in sys.modules:
    telegram = types.ModuleType("telegram")
    constants = types.ModuleType("telegram.constants")
    error = types.ModuleType("telegram.error")
    ext = types.ModuleType("telegram.ext")

    class _TelegramDummy:
        def __init__(self, *args, **kwargs):
            self.inline_keyboard = args[0] if args else kwargs.get("inline_keyboard")
            self.text = args[0] if args else kwargs.get("text")
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
    _radar_exploratory_keyboard,
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


def test_off_market_analysis_uses_latest_market_day_as_price_reference():
    snapshot = {"scraped_at": "2026-08-21T04:39:00+00:00"}

    assert run_analysis._portfolio_market_reference_at(
        snapshot,
        off_market_context=True,
    ) is None
    assert run_analysis._portfolio_market_reference_at(
        snapshot,
        off_market_context=False,
    ) == snapshot["scraped_at"]


def test_market_rows_use_fresh_tradingview_candle_for_stale_position_price():
    class FakeDb:
        async def get_latest_market_prices(self):
            return [
                {
                    "ticker": "NVDA",
                    "asset_type": "CEDEAR",
                    "last_price": 14850,
                    "ts": datetime(2026, 8, 20, 13, 30, tzinfo=timezone.utc),
                },
                {
                    "ticker": "NVS",
                    "asset_type": "CEDEAR",
                    "last_price": 63350,
                    "ts": datetime(2026, 8, 19, 13, 30, tzinfo=timezone.utc),
                },
            ]

        async def get_market_candles(self, ticker, **kwargs):
            if ticker != "NVS":
                return []
            return [
                {
                    "ticker": "NVS",
                    "asset_type": "CEDEAR",
                    "currency": "ARS",
                    "close_price": 61825,
                    "volume": 337,
                    "ts": datetime(2026, 8, 20, 13, 30, tzinfo=timezone.utc),
                    "source": "TRADINGVIEW_BYMA",
                }
            ]

    rows = asyncio.run(
        run_analysis._market_rows_with_candle_fallback(
            FakeDb(),
            [{"ticker": "NVS", "asset_type": "CEDEAR"}],
        )
    )
    nvs = next(row for row in rows if row["ticker"] == "NVS")

    assert nvs["last_price"] == 61825
    assert nvs["source"] == "TRADINGVIEW_BYMA"


def test_tradingview_fallback_provenance_is_preserved_in_position_value():
    positions = [{"ticker": "NVS", "quantity": 4, "current_price": 63350}]
    market_rows = [
        {
            "ticker": "NVS",
            "asset_type": "CEDEAR",
            "last_price": 61825,
            "ts": datetime(2026, 8, 20, 13, 30, tzinfo=timezone.utc),
            "source": "TRADINGVIEW_BYMA",
        }
    ]

    normalized = normalize_positions_with_fresh_market_prices(
        positions,
        market_rows,
        reference_at=datetime(2026, 8, 20, 20, 5, tzinfo=timezone.utc),
    )

    assert normalized[0]["is_operable"] is True
    assert normalized[0]["current_price"] == 61825
    assert normalized[0]["market_value"] == 247300
    assert normalized[0]["price_source"] == "TRADINGVIEW_BYMA"
    assert normalized[0]["market_value_source"] == "TRADINGVIEW_BYMA"


def test_missing_result_positions_remain_visible_in_compact_analysis():
    results = [types.SimpleNamespace(ticker="NVDA")]
    positions = [
        {"ticker": "NVDA"},
        {"ticker": "NVS", "market_data_reason": "precio desactualizado: 2026-08-19"},
    ]

    missing = run_analysis._positions_missing_from_results(results, positions)

    assert [position["ticker"] for position in missing] == ["NVS"]


def test_compact_report_does_not_present_missing_technicals_as_hold():
    macro = types.SimpleNamespace(
        sp500=7641,
        sp500_chg=-0.9,
        vix=16.0,
        wti=86.5,
        wti_chg=-1.5,
        dxy=98.8,
        tnx=4.70,
        merval=2875950,
        ccl=1585,
        mep=1531,
        riesgo_pais=517,
    )

    rendered = run_analysis._render_compact_report(
        [],
        macro,
        2_301_055,
        1_755,
        None,
        [{"ticker": "NVDA", "market_value": 371_250}],
        off_market_context=True,
    )

    assert "Sin plan: no se pudo evaluar técnicamente la cartera" in rendered
    assert "Cartera no evaluable: 1/1 posiciones" in rendered
    assert "No interpretar esto como una señal HOLD" in rendered
    assert "Mantener y esperar mejor setup" not in rendered
    assert "T=técnico" not in rendered


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


def test_help_lists_all_canonical_user_commands():
    rendered = help_text()

    for command in (
        "portfolio",
        "analisis",
        "analisis_test",
        "analisis_full",
        "analisis_debug",
        "events",
        "ticker NVDA",
        "mercado",
        "radar",
        "radar_full",
        "radar_metricas",
        "shadow AMD",
        "resumen",
        "performance",
        "neto",
        "ledger",
        "bot_vs_humano",
        "viability",
        "confianza",
        "calibracion",
        "regression",
        "policy",
        "status",
        "menu",
        "configuracion",
    ):
        assert f"<code>/{command}</code>" in rendered


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

    assert "Próximo paso: Abrir posición" in compact
    assert "el Radar detecta; /analisis decide si entra al plan" in compact
    assert "Fuente técnica" not in compact


def test_compact_radar_understands_current_watchlist_format(monkeypatch):
    import scripts.telegram_bot as telegram_bot_module

    monkeypatch.setattr(telegram_bot_module, "_is_business_day_now", lambda: False)
    monkeypatch.setattr(telegram_bot_module, "_is_market_hours_now", lambda: False)
    full_report = """<b>🔭 Radar de oportunidades</b>
🔍 Universo: 250 tickers → 128 pasaron screener → 38 ideas rankeadas → top 6 mostradas
✅ Estado operativo: <b>NORMAL</b>
💵 Cash libre: <b>$1.755 ARS</b>
   Mercado cerrado/sin rueda: ideas para revalidar en la próxima apertura.

<b>👁 En vigilancia (3)</b>
<b>A — Casi operables</b>
  <b>LMT</b>: score <code>+0.219</code> | conv. 50% | R/R 1.9x | $45400.00 | edge +0.052
   Fuente técnica: <b>mixed (TRADINGVIEW_BYMA 260)</b>
   ↩️ Reversión: <b>posible rebote alcista</b> (<code>+0.257</code>) | estocástico
   🔬 Shadow: <b>SHADOW DÉBIL</b> — no perseguir
   Compra técnica V3: <b>A</b> · compra primaria · 20d · shadow
   Gráfico: TradingView/BYMA
   └ RS fuerte vs SPY (+8.9% en 20d)
   ⏸ Por qué no entra: <i>convicción 50% &lt; umbral; sin cash ejecutable; requiere funding o swap</i>
   🎯 Revalidar: Revalidar al abrir: Esperar funding o evaluar swap. No ejecutar ahora con mercado cerrado.

  <b>GLOB</b>: score <code>+0.155</code> | conv. 50% | R/R 2.8x | $3490.00 | edge -0.013
   ↩️ Reversión: <b>posible corrección bajista</b> (<code>-0.329</code>)
   Compra técnica V3: <b>REJECTED</b> · no elegible para compra · 20d · shadow
   ⏸ Por qué no entra: <i>no supera a cartera actual (edge -0.013)</i>
   🎯 Revalidar: Revalidar al abrir: Esperar confirmación técnica. No ejecutar ahora con mercado cerrado.

  <b>SHOP</b>: score <code>+0.180</code> | conv. 50% | R/R 0.9x | $2190.00 | edge +0.013
   ↩️ Reversión: <b>posible corrección bajista</b> (<code>-0.471</code>)
   Compra técnica V3: <b>A</b> · compra primaria · 20d · shadow
   └ RS fuerte vs SPY (+23.7% en 20d)
   ⏸ Por qué no entra: <i>R/R 0.9x insuficiente (mín 1.2x)</i>
   🎯 Revalidar: Revalidar al abrir: Esperar pullback hacia soporte. No ejecutar ahora con mercado cerrado.
"""

    compact = compact_radar_report(full_report)

    assert "Radar · próxima apertura" in compact
    assert "250 analizados · 3 ideas detectadas" in compact
    assert "no hay compras habilitadas" in compact
    assert compact.index("<b>LMT</b>") < compact.index("<b>SHOP</b>")
    assert compact.index("<b>SHOP</b>") < compact.index("<b>GLOB</b>")
    assert "<b>No priorizar ahora</b>" in compact
    assert "V3 <b>rechazada</b>" in compact
    assert "convicción todavía baja (50%)" in compact
    assert "no mejora la cartera actual" in compact
    assert "Fuente técnica" not in compact
    assert "Gráfico:" not in compact
    assert "No ejecutar ahora" not in compact
    assert "/radar_full" in compact


def test_exploratory_keyboard_skips_rejected_candidates():
    keyboard = _radar_exploratory_keyboard({
        "candidates": [
            {"id": 1, "ticker": "LMT", "v3_tier": "A", "user_action": None},
            {"id": 2, "ticker": "GLOB", "v3_tier": "REJECTED", "user_action": None},
        ]
    })

    assert keyboard is not None
    assert len(keyboard.inline_keyboard) == 1
    assert keyboard.inline_keyboard[0][0].text == "Seguir LMT · A"
    assert keyboard.inline_keyboard[0][0].callback_data == "re:follow:1"

from __future__ import annotations

import sys
import types


telegram_module = types.ModuleType("telegram")


class _TelegramDummy:
    def __init__(self, *args, **kwargs):
        self.inline_keyboard = args[0] if args else kwargs.get("inline_keyboard")
        self.callback_data = kwargs.get("callback_data")


class _FilterDummy:
    def __and__(self, other):
        return self

    def __invert__(self):
        return self


telegram_module.InlineKeyboardButton = _TelegramDummy
telegram_module.InlineKeyboardMarkup = _TelegramDummy
telegram_module.Update = _TelegramDummy

telegram_constants_module = types.ModuleType("telegram.constants")
telegram_constants_module.ParseMode = types.SimpleNamespace(HTML="HTML")

telegram_error_module = types.ModuleType("telegram.error")
telegram_error_module.BadRequest = Exception

telegram_ext_module = types.ModuleType("telegram.ext")
telegram_ext_module.Application = object
telegram_ext_module.CallbackQueryHandler = object
telegram_ext_module.CommandHandler = object
telegram_ext_module.ContextTypes = types.SimpleNamespace(DEFAULT_TYPE=object)
telegram_ext_module.MessageHandler = object
telegram_ext_module.filters = types.SimpleNamespace(
    TEXT=_FilterDummy(),
    COMMAND=_FilterDummy(),
)

sys.modules.setdefault("telegram", telegram_module)
sys.modules.setdefault("telegram.constants", telegram_constants_module)
sys.modules.setdefault("telegram.error", telegram_error_module)
sys.modules.setdefault("telegram.ext", telegram_ext_module)

from scripts.telegram_bot import _radar_cache_supports_reversion, compact_radar_report


def test_compact_radar_preserves_shadow_context_line():
    full_report = """
<b>🔭 Radar de oportunidades</b>
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔍 Universo: 10 tickers → 1 ideas rankeadas
✅ Estado operativo: <b>NORMAL</b>
   VIX: 18.9

<b>━━ UPST ━━</b>  NUEVA
Score: <code>+0.123</code> | Conv: <b>70%</b> | Precio: <b>$10.06</b>
Fuente técnica: <b>mixed</b>
↩️ Reversión: <b>posible rebote alcista</b> (<code>+0.500</code>) | RSI, Bollinger
📊 Última idea radar 01/07/2026: 5D +4.1% | 10D +5.0% | 20D pendiente | 40D pendiente
🔬 Shadow: <b>SHADOW DÉBIL</b> — Shadow débil: 20r +2.3%, P+ 53%. Momentum actual; no perseguir sin pullback/catalyst.
Compra técnica V3: <b>A</b> · compra primaria · 20d · shadow
🟢 Asimetría <b>BUENA</b> — upside 20.0% | stop -5% | R/R <b>4.0x</b>
💰 Sizing sugerido: <b>5.0%</b> del portfolio ≈ $50.000 ARS
🎯 <b>Acción sugerida:</b> Esperar pullback
"""

    compact = compact_radar_report(full_report, max_items=1)

    assert "SHADOW DÉBIL" in compact
    assert "Compra V3: <b>A</b>" in compact
    assert "compra primaria" in compact
    assert "no perseguir sin pullback/catalyst" in compact
    assert "posible rebote alcista" in compact
    assert "Última idea radar 01/07/2026" in compact
    assert "5D +4.1%" in compact
    assert "Esperar pullback" in compact


def test_legacy_radar_cache_is_rejected_until_it_contains_reversion():
    assert not _radar_cache_supports_reversion("<b>Radar anterior</b>")
    assert not _radar_cache_supports_reversion("↩️ Reversión: sin extremo claro")
    assert _radar_cache_supports_reversion(
        "↩️ Reversión: sin extremo claro\n"
        "Compra técnica V3: <b>A</b> · compra primaria · 20d · shadow"
    )

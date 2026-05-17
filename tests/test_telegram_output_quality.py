import sys
import types
from pathlib import Path
from types import SimpleNamespace


telegram_module = types.ModuleType("telegram")
telegram_module.InlineKeyboardButton = object
telegram_module.InlineKeyboardMarkup = object
telegram_module.Update = object

telegram_constants_module = types.ModuleType("telegram.constants")
telegram_constants_module.ParseMode = SimpleNamespace(HTML="HTML")

telegram_error_module = types.ModuleType("telegram.error")
telegram_error_module.BadRequest = Exception

telegram_ext_module = types.ModuleType("telegram.ext")
telegram_ext_module.Application = object
telegram_ext_module.CallbackQueryHandler = object
telegram_ext_module.CommandHandler = object
telegram_ext_module.ContextTypes = SimpleNamespace(DEFAULT_TYPE=object)

sys.modules.setdefault("telegram", telegram_module)
sys.modules.setdefault("telegram.constants", telegram_constants_module)
sys.modules.setdefault("telegram.error", telegram_error_module)
sys.modules.setdefault("telegram.ext", telegram_ext_module)

from scripts import telegram_bot


def test_compact_radar_parses_header_with_cash_lines():
    report = """
🔭 <b>RADAR DE OPORTUNIDADES</b>
🔍 Universo: 34 tickers → 12 pasaron screener → 6 ideas rankeadas
✅ Gate: <b>NORMAL</b>
💵 Cash libre: <b>$3.027 ARS</b>
   Sin cash ejecutable: nuevas entradas solo via funding o swap.
   VIX: 18.4

<b>━━ MU ━━</b>  🔄 SWAP [fuerte]  Edge: 🟢 <code>+0.104</code> (fuerte)
Score: <code>+0.227</code>
R/R <b>1.3x</b>
⚖️ Compite con: <b>NVDA, AMD, QCOM</b>
🎯 <b>Acción sugerida:</b> Swap vs AMD — reducir antes de comprar
"""

    output = telegram_bot.compact_radar_report(report)

    assert "Universo: 34 tickers → 12 pasaron screener → 6 ideas rankeadas" in output
    assert "Gate: <b>NORMAL</b> | VIX 18.4" in output
    assert "Cash libre" not in output
    assert "Swap vs AMD — reducir antes de comprar (NVDA)" not in output


def test_weekend_freshness_badge_is_not_red_for_expected_gap():
    icon, suffix = telegram_bot._freshness_badge(24 * 60, business_day=False)

    assert icon == "📅"
    assert "esperable sin rueda" in suffix


def test_main_menu_copy_matches_current_data_model():
    source = Path("scripts/telegram_bot.py").read_text(encoding="utf-8")

    assert "Plan de cartera</b> — rotación y acciones sugeridas" in source
    assert "Performance</b> — métricas canónicas y dataset operativo" in source
    assert "Regression</b> — auditoría estadística" in source
    assert "Radar</b> — oportunidades operables del universo" in source

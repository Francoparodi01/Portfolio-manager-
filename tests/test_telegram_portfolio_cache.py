import asyncio
import sys
import types
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


def test_action_portfolio_prefers_live_cache(monkeypatch):
    sent: list[str] = []

    async def _fake_live_cache():
        return {
            "valuation_mode": "live_market_prices",
            "generated_at": "2026-05-18T16:00:00+00:00",
            "cash_ars": 10_000,
            "positions": [
                {
                    "ticker": "NVDA",
                    "quantity": 10,
                    "current_price": 103,
                    "market_value": 1_030,
                }
            ],
        }

    async def _fake_send_text(_context, _chat_id, text, parse_mode=None):
        sent.append(text)

    monkeypatch.setattr(telegram_bot, "get_cached_live_portfolio", _fake_live_cache)
    monkeypatch.setattr(
        telegram_bot,
        "get_config",
        lambda: SimpleNamespace(database=SimpleNamespace(url="unused")),
    )
    monkeypatch.setattr(telegram_bot, "PortfolioDatabase", object)
    monkeypatch.setattr(telegram_bot, "send_text", _fake_send_text)

    asyncio.run(telegram_bot.action_portfolio(SimpleNamespace(), 123))

    assert sent
    assert "Valuación live estimada con market_prices" in sent[0]
    assert "NVDA" in sent[0]

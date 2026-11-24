import asyncio
import sys
import types
from types import SimpleNamespace

from src.core.portfolio_cache import _portfolio_key


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
telegram_ext_module.MessageHandler = object
telegram_ext_module.filters = SimpleNamespace(TEXT=object(), COMMAND=object())

sys.modules.setdefault("telegram", telegram_module)
sys.modules.setdefault("telegram.constants", telegram_constants_module)
sys.modules.setdefault("telegram.error", telegram_error_module)
sys.modules.setdefault("telegram.ext", telegram_ext_module)

from scripts import telegram_bot


def test_action_portfolio_uses_live_cache_only_when_db_has_no_snapshot(monkeypatch):
    sent: list[str] = []

    async def _fake_live_cache(*, owner_chat_id=None):
        assert owner_chat_id == 123
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

    async def _fake_sync(**_kwargs):
        return ""

    class _Db:
        def __init__(self, _dsn):
            pass

        async def connect(self):
            return None

        async def close(self):
            return None

        async def get_latest_snapshot(self, owner_chat_id=None):
            assert owner_chat_id == 123
            return None

    monkeypatch.setattr(telegram_bot, "get_cached_live_portfolio", _fake_live_cache)
    monkeypatch.setattr(
        telegram_bot,
        "get_config",
        lambda: SimpleNamespace(
            database=SimpleNamespace(url="unused"),
            multiuser_enabled=True,
        ),
    )
    monkeypatch.setattr(telegram_bot, "PortfolioDatabase", _Db)
    monkeypatch.setattr(telegram_bot, "sync_operational_state", _fake_sync)
    monkeypatch.setattr(telegram_bot, "send_text", _fake_send_text)

    asyncio.run(telegram_bot.action_portfolio(SimpleNamespace(), 123))

    assert sent
    assert "Valuaci" in sent[0]
    assert "NVDA" in sent[0]


def test_action_portfolio_snapshot_uses_cocos_tenencia_as_primary_total(monkeypatch):
    sent: list[str] = []

    async def _fake_live_cache(*, owner_chat_id=None):
        return None

    class _Db:
        def __init__(self, _dsn):
            pass

        async def connect(self):
            return None

        async def close(self):
            return None

        async def get_latest_snapshot(self, owner_chat_id=None):
            assert owner_chat_id is None
            return {
                "scraped_at": "2026-05-18T20:00:00+00:00",
                "total_value_ars": 2_000,
                "cash_ars": 100,
                "positions": [
                    {
                        "ticker": "NVDA",
                        "quantity": 10,
                        "current_price": 100,
                        "market_value": 1_000,
                    },
                    {
                        "ticker": "AMD",
                        "quantity": 20,
                        "current_price": 50,
                        "market_value": 1_000,
                    },
                ],
            }

    async def _fake_send_text(_context, _chat_id, text, parse_mode=None):
        sent.append(text)

    async def _fake_sync(**_kwargs):
        return ""

    monkeypatch.setattr(telegram_bot, "get_cached_live_portfolio", _fake_live_cache)
    monkeypatch.setattr(
        telegram_bot,
        "get_config",
        lambda: SimpleNamespace(
            database=SimpleNamespace(url="unused"),
            multiuser_enabled=False,
        ),
    )
    monkeypatch.setattr(telegram_bot, "PortfolioDatabase", _Db)
    monkeypatch.setattr(telegram_bot, "sync_operational_state", _fake_sync)
    monkeypatch.setattr(telegram_bot, "send_text", _fake_send_text)

    asyncio.run(telegram_bot.action_portfolio(SimpleNamespace(), 123))

    assert sent
    assert "Tenencia Cocos" in sent[0]
    assert "$2.000" in sent[0]
    assert "Cuenta estimada" in sent[0]
    assert "$2.100" in sent[0]
    assert "Cash disponible" in sent[0]


def test_portfolio_cache_keys_are_user_scoped():
    assert _portfolio_key("cocos:portfolio:snapshot") == "cocos:portfolio:snapshot"
    assert _portfolio_key("cocos:portfolio:snapshot", 123) == "cocos:portfolio:snapshot:123"

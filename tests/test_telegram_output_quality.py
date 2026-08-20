import sys
import types
from pathlib import Path
from types import SimpleNamespace
import asyncio
from src.core.config import ScraperConfig


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


def test_main_menu_is_compact_and_does_not_repeat_button_descriptions():
    text = telegram_bot.menu_text()

    assert "<b>QUANTIA</b>" in text
    assert len(text.splitlines()) <= 5
    assert "último snapshot" not in text
    assert "métricas canónicas" not in text


def test_help_text_is_mobile_compact_and_scope_safe():
    text = telegram_bot.help_text()

    assert len(text.encode("utf-8")) < 1200
    assert len(text.splitlines()) <= 24
    assert "No ejecuta ordenes" in text
    assert "Ejecucion real: solo fills/movimientos confirmados." in text
    assert "Radar/shadow/debug: auditoria o exploracion" in text


def test_status_is_treated_as_fast_action():
    assert "status" in telegram_bot.FAST_ACTIONS


class _Button:
    def __init__(self, text, *, callback_data):
        self.text = text
        self.callback_data = callback_data


class _Markup:
    def __init__(self, rows):
        self.inline_keyboard = rows


def test_main_menu_adds_settings_only_in_multiuser(monkeypatch):
    monkeypatch.setattr(telegram_bot, "InlineKeyboardButton", _Button)
    monkeypatch.setattr(telegram_bot, "InlineKeyboardMarkup", _Markup)

    monkeypatch.setattr(
        telegram_bot,
        "get_config",
        lambda: SimpleNamespace(multiuser_enabled=True),
    )
    enabled_markup = telegram_bot.main_keyboard()
    enabled_labels = [
        button.text
        for row in enabled_markup.inline_keyboard
        for button in row
    ]
    assert "⚙️ Configuración" in enabled_labels

    monkeypatch.setattr(
        telegram_bot,
        "get_config",
        lambda: SimpleNamespace(multiuser_enabled=False),
    )
    monkeypatch.setattr(telegram_bot, "_is_business_day_now", lambda: True)
    monkeypatch.setattr(telegram_bot, "_is_market_hours_now", lambda: True)
    monkeypatch.setattr(telegram_bot, "_market_closed_reason_now", lambda: None)
    disabled_markup = telegram_bot.main_keyboard()
    disabled_labels = [
        button.text
        for row in disabled_markup.inline_keyboard
        for button in row
    ]
    assert "⚙️ Configuración" not in disabled_labels


def test_main_menu_prioritizes_common_actions_and_groups_secondary_views(monkeypatch):
    monkeypatch.setattr(telegram_bot, "InlineKeyboardButton", _Button)
    monkeypatch.setattr(telegram_bot, "InlineKeyboardMarkup", _Markup)
    monkeypatch.setattr(
        telegram_bot,
        "get_config",
        lambda: SimpleNamespace(multiuser_enabled=False),
    )
    monkeypatch.setattr(telegram_bot, "_is_business_day_now", lambda: True)
    monkeypatch.setattr(telegram_bot, "_is_market_hours_now", lambda: True)
    monkeypatch.setattr(telegram_bot, "_market_closed_reason_now", lambda: None)

    rows = telegram_bot.main_keyboard().inline_keyboard
    labels = [[button.text for button in row] for row in rows]

    assert labels == [
        ["💼 Cartera", "🧠 Plan"],
        ["🔎 Ticker", "🔭 Radar"],
        ["📅 Balances", "📊 Resultados"],
        ["🧪 Auditoría", "🩺 Estado"],
    ]


def test_main_menu_exposes_cached_close_analysis_outside_market(monkeypatch):
    monkeypatch.setattr(telegram_bot, "InlineKeyboardButton", _Button)
    monkeypatch.setattr(telegram_bot, "InlineKeyboardMarkup", _Markup)
    monkeypatch.setattr(
        telegram_bot,
        "get_config",
        lambda: SimpleNamespace(multiuser_enabled=False),
    )
    monkeypatch.setattr(telegram_bot, "_is_business_day_now", lambda: True)
    monkeypatch.setattr(telegram_bot, "_is_market_hours_now", lambda: False)

    labels = [
        button.text
        for row in telegram_bot.main_keyboard().inline_keyboard
        for button in row
    ]

    assert "🧠 Cierre" in labels
    assert "🧠 Plan" not in labels


def test_settings_action_reports_unlinked_account(monkeypatch):
    sent: list[str] = []

    class _Cipher:
        @classmethod
        def from_env(cls):
            return object()

    class _Db:
        def __init__(self, _dsn):
            pass

        async def connect(self):
            return None

        async def close(self):
            return None

        async def get_bot_user_credentials(self, *, chat_id, cipher):
            assert chat_id == 123
            assert cipher is not None
            return None

    async def _fake_send_text(_context, _chat_id, text, parse_mode=None):
        sent.append(text)

    monkeypatch.setattr(
        telegram_bot,
        "get_config",
        lambda: SimpleNamespace(
            multiuser_enabled=True,
            database=SimpleNamespace(url="unused"),
        ),
    )
    monkeypatch.setattr(telegram_bot, "CredentialCipher", _Cipher)
    monkeypatch.setattr(telegram_bot, "PortfolioDatabase", _Db)
    monkeypatch.setattr(telegram_bot, "send_text", _fake_send_text)

    context = SimpleNamespace(user_data={})
    asyncio.run(telegram_bot.action_settings(context, 123))

    assert sent
    assert "Cuenta Cocos: <b>sin credenciales vinculadas</b>" in sent[0]
    assert telegram_bot.SETTINGS_STATE_KEY in context.user_data


def test_settings_action_reports_linked_account_without_restarting_setup(monkeypatch):
    sent: list[str] = []

    class _Cipher:
        @classmethod
        def from_env(cls):
            return object()

    class _Db:
        def __init__(self, _dsn):
            pass

        async def connect(self):
            return None

        async def close(self):
            return None

        async def get_bot_user_credentials(self, *, chat_id, cipher):
            assert chat_id == 123
            assert cipher is not None
            return telegram_bot.UserCredentials("franco@example.com", "secreto")

    async def _fake_send_text(_context, _chat_id, text, parse_mode=None):
        sent.append(text)

    monkeypatch.setattr(
        telegram_bot,
        "get_config",
        lambda: SimpleNamespace(
            multiuser_enabled=True,
            database=SimpleNamespace(url="unused"),
        ),
    )
    monkeypatch.setattr(telegram_bot, "CredentialCipher", _Cipher)
    monkeypatch.setattr(telegram_bot, "PortfolioDatabase", _Db)
    monkeypatch.setattr(telegram_bot, "send_text", _fake_send_text)

    context = SimpleNamespace(
        user_data={telegram_bot.SETTINGS_STATE_KEY: telegram_bot.SETTINGS_AWAIT_PASSWORD}
    )
    asyncio.run(telegram_bot.action_settings(context, 123))

    assert sent
    assert "Cuenta Cocos: <b>credenciales vinculadas</b>" in sent[0]
    assert "Tu cuenta ya está vinculada." in sent[0]
    assert "/reconfigurar" in sent[0]
    assert telegram_bot.SETTINGS_STATE_KEY not in context.user_data


def test_settings_flow_saves_credentials_and_clears_state(monkeypatch):
    sent: list[str] = []
    saved = {}
    deleted = []

    class _Message:
        def __init__(self, text):
            self.text = text

        async def delete(self):
            deleted.append(self.text)

    class _Cipher:
        @classmethod
        def from_env(cls):
            return object()

    class _Db:
        def __init__(self, _dsn):
            pass

        async def connect(self):
            return None

        async def close(self):
            return None

        async def upsert_bot_user_credentials(self, **kwargs):
            saved.update(kwargs)

    async def _fake_send_text(_context, _chat_id, text, parse_mode=None):
        sent.append(text)

    async def _fake_send_menu(_context, _chat_id):
        sent.append("MENU")

    async def _fake_start_sync(_context, _chat_id, *, reason):
        assert reason == "Cuenta recién vinculada."
        sent.append("SYNC")
        return True

    monkeypatch.setattr(
        telegram_bot,
        "get_config",
        lambda: SimpleNamespace(
            multiuser_enabled=True,
            database=SimpleNamespace(url="unused"),
        ),
    )
    monkeypatch.setattr(telegram_bot, "CredentialCipher", _Cipher)
    monkeypatch.setattr(telegram_bot, "PortfolioDatabase", _Db)
    monkeypatch.setattr(telegram_bot, "send_text", _fake_send_text)
    monkeypatch.setattr(telegram_bot, "send_menu", _fake_send_menu)
    monkeypatch.setattr(
        telegram_bot,
        "_start_user_portfolio_sync_if_possible",
        _fake_start_sync,
    )

    context = SimpleNamespace(
        user_data={telegram_bot.SETTINGS_STATE_KEY: telegram_bot.SETTINGS_AWAIT_USERNAME}
    )
    user = SimpleNamespace(username="franco", full_name="Franco")

    asyncio.run(
        telegram_bot.settings_text_handler(
            SimpleNamespace(
                message=_Message("franco@example.com"),
                effective_chat=SimpleNamespace(id=123),
                effective_user=user,
            ),
            context,
        )
    )
    assert context.user_data[telegram_bot.SETTINGS_STATE_KEY] == telegram_bot.SETTINGS_AWAIT_PASSWORD

    asyncio.run(
        telegram_bot.settings_text_handler(
            SimpleNamespace(
                message=_Message("secreto"),
                effective_chat=SimpleNamespace(id=123),
                effective_user=user,
            ),
            context,
        )
    )

    assert deleted == ["franco@example.com", "secreto"]
    assert saved["chat_id"] == 123
    assert saved["credentials"].username == "franco@example.com"
    assert saved["credentials"].password == "secreto"
    assert telegram_bot.SETTINGS_STATE_KEY not in context.user_data
    assert sent[-2:] == [
        "✅ Cuenta Cocos vinculada y guardada cifrada.\n"
        "Desde acá ya podemos usar este chat como identidad separada dentro del sandbox multiusuario.",
        "SYNC",
    ]


def test_sync_user_portfolio_uses_private_session_and_owner(monkeypatch):
    sent: list[str] = []
    saved = {}
    cached = {}

    class _Snapshot:
        def __init__(self):
            self.owner_chat_id = None
            self.positions = [object()]

        def to_dict(self):
            return {"owner_chat_id": self.owner_chat_id, "positions": [{}]}

    class _Db:
        def __init__(self, _dsn):
            self.snapshot = None

        async def connect(self):
            return None

        async def close(self):
            return None

        async def save_snapshot(self, snapshot):
            saved["snapshot"] = snapshot

    class _Scraper:
        received_config = None

        def __init__(self, cfg):
            type(self).received_config = cfg

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def login(self):
            return True

        async def scrape_portfolio(self):
            return _Snapshot()

    async def _fake_cache(payload, *, owner_chat_id):
        cached["payload"] = payload
        cached["owner_chat_id"] = owner_chat_id

    async def _fake_send_text(_context, _chat_id, text, parse_mode=None):
        sent.append(text)

    async def _fake_action_portfolio(_context, _chat_id, **_kwargs):
        sent.append("PORTFOLIO")

    async def _fake_send_menu(_context, _chat_id):
        sent.append("MENU")

    monkeypatch.setattr(
        telegram_bot,
        "get_config",
        lambda: SimpleNamespace(
            multiuser_enabled=True,
            database=SimpleNamespace(url="unused"),
            scraper=ScraperConfig(
                telegram_bot_token="token",
                telegram_chat_id="",
                telegram_enabled=False,
            ),
        ),
    )
    monkeypatch.setattr(telegram_bot, "PortfolioDatabase", _Db)
    monkeypatch.setattr(telegram_bot, "CocosCapitalScraper", _Scraper)
    monkeypatch.setattr(telegram_bot, "cache_portfolio_snapshot", _fake_cache)
    monkeypatch.setattr(telegram_bot, "send_text", _fake_send_text)
    monkeypatch.setattr(telegram_bot, "action_portfolio", _fake_action_portfolio)
    monkeypatch.setattr(telegram_bot, "send_menu", _fake_send_menu)

    context = SimpleNamespace(user_data={telegram_bot.PORTFOLIO_SYNC_PENDING_KEY: True})
    asyncio.run(
        telegram_bot._sync_user_portfolio_once(
            context,
            123,
            telegram_bot.UserCredentials("franco@example.com", "secreto"),
        )
    )

    assert _Scraper.received_config.username == "franco@example.com"
    assert _Scraper.received_config.password == "secreto"
    assert _Scraper.received_config.session_file.endswith("cocos_session_123.json")
    assert _Scraper.received_config.telegram_chat_id == "123"
    assert _Scraper.received_config.telegram_mfa_prompt_enabled is True
    assert saved["snapshot"].owner_chat_id == 123
    assert cached["owner_chat_id"] == 123
    assert telegram_bot.PORTFOLIO_SYNC_PENDING_KEY not in context.user_data
    assert sent[-3:] == ["✅ Portfolio inicial sincronizado.", "PORTFOLIO", "MENU"]

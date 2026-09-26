#!/usr/bin/env python3
"""Conversational-only Telegram gateway for Quantia.

Normal users write natural language. Legacy commands are accepted only as hidden
compatibility aliases and are immediately translated into a natural-language goal
that goes through the same harness. No inline/reply keyboards or command menu are
published.

Credential onboarding is an explicit transport sub-flow and never enters the LLM,
agent audit payload, or conversational memory.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

from telegram import Update
from telegram.constants import ChatAction
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.agentic.harness import ConversationalHarness  # noqa: E402
from src.collector.db import PortfolioDatabase  # noqa: E402
from src.core.config import get_config  # noqa: E402
from src.core.credentials import CredentialCipher, UserCredentials  # noqa: E402
from src.core.redis_client import client as redis_client  # noqa: E402


logger = logging.getLogger(__name__)
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )

MAX_MESSAGE_LENGTH = 3900
BOT_HEARTBEAT_KEY = "cocos:bot:last_heartbeat"
ACCOUNT_STATE_KEY = "quantia_account_state"
ACCOUNT_USERNAME_KEY = "quantia_account_username"
ACCOUNT_AWAIT_USERNAME = "await_username"
ACCOUNT_AWAIT_PASSWORD = "await_password"
_CHAT_LOCKS: dict[int, asyncio.Lock] = {}
_HARNESS: ConversationalHarness | None = None


LEGACY_GOALS = {
    "portfolio": "¿Cómo está mi cartera hoy?",
    "analisis": "Analizá mi cartera y explicame el plan operativo actual.",
    "radar": "¿Qué oportunidades hay para mi cartera?",
    "performance": "¿Cuánto ganó Quantia? Separá PnL económico real de métricas observacionales.",
    "neto": "¿Cuál es el resultado económico neto que realmente puede verificarse?",
    "ledger": "¿Qué pasó con las decisiones recientes de Quantia?",
    "bot_vs_humano": "Compará las decisiones del bot y las humanas usando solamente evidencia comparable.",
    "analytics": "Mostrame Analytics v2 y sus limitaciones actuales.",
    "analytics_v2": "Mostrame Analytics v2 y sus limitaciones actuales.",
    "status": "¿Está funcionando Quantia y qué datos están frescos?",
    "mercado": "¿Cómo está el contexto de mercado y macro relevante?",
    "meta": "Explicame el estado actual de Economic Meta Policy y sus filtros A/B/C.",
}


def _plain(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or "").lower())
    return "".join(char for char in normalized if not unicodedata.combining(char))


def _token() -> str:
    token = (
        os.getenv("TELEGRAM_BOT_TOKEN")
        or os.getenv("SCRAPER_TELEGRAM_BOT_TOKEN")
        or os.getenv("TELEGRAM_TOKEN")
    )
    if token:
        return token
    cfg = get_config()
    configured = getattr(cfg.scraper, "telegram_bot_token", None)
    if configured:
        return str(configured)
    raise RuntimeError("Telegram token not configured")


def _ids_from_env(*names: str) -> set[int]:
    values: list[str] = []
    for name in names:
        values.append(os.getenv(name, ""))
    result: set[int] = set()
    for raw in ",".join(values).replace(";", ",").split(","):
        raw = raw.strip()
        if raw.lstrip("-").isdigit():
            result.add(int(raw))
    return result


def _allowed(chat_id: int) -> bool:
    cfg = get_config()
    if bool(cfg.multiuser_enabled):
        return True
    if os.getenv("TELEGRAM_ALLOW_ALL_CHATS", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        return True
    allowed = _ids_from_env(
        "TELEGRAM_CHAT_ID",
        "TELEGRAM_ALLOWED_CHAT_IDS",
        "ADMIN_CHAT_IDS",
    )
    configured = str(getattr(cfg.scraper, "telegram_chat_id", "") or "").strip()
    if configured.lstrip("-").isdigit():
        allowed.add(int(configured))
    return int(chat_id) in allowed if allowed else False


async def _heartbeat() -> None:
    try:
        await redis_client.set(
            BOT_HEARTBEAT_KEY,
            datetime.now(timezone.utc).isoformat(),
            ex=180,
        )
    except Exception:
        logger.debug("Redis heartbeat unavailable", exc_info=True)


def _split_message(text: str, max_len: int = MAX_MESSAGE_LENGTH) -> list[str]:
    text = str(text or "").strip()
    if not text:
        return ["No tengo evidencia suficiente para responder esa consulta."]
    if len(text) <= max_len:
        return [text]
    chunks: list[str] = []
    current = ""
    for paragraph in text.split("\n"):
        piece = paragraph + "\n"
        if len(current) + len(piece) <= max_len:
            current += piece
            continue
        if current.strip():
            chunks.append(current.rstrip())
        while len(piece) > max_len:
            cut = piece.rfind(" ", 0, max_len)
            if cut < max_len // 2:
                cut = max_len
            chunks.append(piece[:cut].rstrip())
            piece = piece[cut:].lstrip()
        current = piece
    if current.strip():
        chunks.append(current.rstrip())
    return chunks


def _legacy_goal(text: str) -> str:
    clean = str(text or "").strip()
    if not clean.startswith("/"):
        return clean
    head, *rest = clean.split(maxsplit=1)
    command = head[1:].split("@", 1)[0].lower()
    args = rest[0].strip() if rest else ""
    if command in {"agente", "agent"} and args:
        return args
    if command == "ticker" and args:
        return f"Analizá {args} y explicame la decisión vigente con su evidencia."
    if command in {"settings", "configuracion", "configuración", "reconfigurar"}:
        return "quiero reconfigurar mi cuenta de Cocos"
    if command in LEGACY_GOALS:
        return LEGACY_GOALS[command] + (f" Contexto adicional: {args}" if args else "")
    return args or command.replace("_", " ")


def _account_setup_requested(text: str) -> bool:
    plain = _plain(text)
    return any(
        phrase in plain
        for phrase in (
            "configurar mi cuenta",
            "configurar cuenta",
            "vincular mi cuenta",
            "vincular cuenta",
            "reconfigurar mi cuenta",
            "reconfigurar cuenta",
            "cambiar credenciales",
            "credenciales de cocos",
            "cuenta de cocos",
        )
    )


def _clear_account_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop(ACCOUNT_STATE_KEY, None)
    context.user_data.pop(ACCOUNT_USERNAME_KEY, None)


async def _credential_status(chat_id: int):
    cfg = get_config()
    cipher = CredentialCipher.from_env()
    db = PortfolioDatabase(cfg.database.url)
    await db.connect()
    try:
        return await db.get_bot_user_credentials(chat_id=chat_id, cipher=cipher)
    finally:
        await db.close()


async def _begin_account_setup(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    force: bool,
) -> bool:
    if not update.effective_chat or not update.message:
        return False
    cfg = get_config()
    if not bool(cfg.multiuser_enabled):
        await update.message.reply_text(
            "Esta instalación usa una cuenta fija; no necesita vinculación por chat."
        )
        return True

    chat_id = int(update.effective_chat.id)
    try:
        current = await _credential_status(chat_id)
    except Exception:
        logger.exception("Could not read credential status chat_id=%s", chat_id)
        await update.message.reply_text(
            "No pude verificar el estado de la cuenta cifrada. No envíes credenciales todavía."
        )
        return True

    if current is not None and not force:
        _clear_account_state(context)
        await update.message.reply_text(
            "Tu cuenta de Cocos ya está vinculada. Si querés reemplazarla, escribí “reconfigurar mi cuenta”."
        )
        return True

    context.user_data[ACCOUNT_STATE_KEY] = ACCOUNT_AWAIT_USERNAME
    context.user_data.pop(ACCOUNT_USERNAME_KEY, None)
    await update.message.reply_text(
        "Enviame el usuario/email de Cocos. Ese mensaje se borra y no se envía al LLM ni al audit del harness."
    )
    return True


async def _handle_account_secret_step(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> bool:
    state = context.user_data.get(ACCOUNT_STATE_KEY)
    if state not in {ACCOUNT_AWAIT_USERNAME, ACCOUNT_AWAIT_PASSWORD}:
        return False
    if not update.effective_chat or not update.message:
        return True

    value = str(update.message.text or "").strip()
    try:
        await update.message.delete()
    except Exception:
        logger.debug("Could not delete sensitive onboarding message", exc_info=True)

    if not value:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="El valor está vacío. Volvé a enviarlo.",
        )
        return True

    if state == ACCOUNT_AWAIT_USERNAME:
        context.user_data[ACCOUNT_USERNAME_KEY] = value
        context.user_data[ACCOUNT_STATE_KEY] = ACCOUNT_AWAIT_PASSWORD
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=(
                "Ahora enviame la contraseña de Cocos. También borraré ese mensaje; "
                "la contraseña no pasa por el LLM ni por la memoria conversacional."
            ),
        )
        return True

    username = str(context.user_data.get(ACCOUNT_USERNAME_KEY) or "").strip()
    password = value
    if not username:
        _clear_account_state(context)
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Se perdió el usuario temporal. Escribí “configurar mi cuenta” para reiniciar de forma segura.",
        )
        return True

    cfg = get_config()
    if not bool(cfg.multiuser_enabled):
        _clear_account_state(context)
        return True

    try:
        cipher = CredentialCipher.from_env()
        db = PortfolioDatabase(cfg.database.url)
        await db.connect()
        try:
            user = update.effective_user
            await db.upsert_bot_user_credentials(
                chat_id=int(update.effective_chat.id),
                credentials=UserCredentials(username=username, password=password),
                cipher=cipher,
                telegram_username=(user.username if user else None),
                display_name=(user.full_name if user else None),
            )
        finally:
            await db.close()
    except Exception:
        logger.exception("Credential onboarding failed chat_id=%s", update.effective_chat.id)
        _clear_account_state(context)
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=(
                "No pude guardar las credenciales de forma cifrada. El flujo se canceló; "
                "escribí “configurar mi cuenta” para volver a intentar."
            ),
        )
        return True

    _clear_account_state(context)
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=(
            "Cuenta de Cocos vinculada y guardada cifrada. Las credenciales no se incorporaron "
            "al contexto del agente. Ya podés seguir hablando con Quantia normalmente."
        ),
    )
    return True


async def _post_init(application: Application) -> None:
    # Removing Telegram's command list is part of the UX cutover. Compatibility
    # handlers remain private implementation details and are not advertised.
    await application.bot.set_my_commands([])
    await _heartbeat()


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message:
        return
    chat_id = int(update.effective_chat.id)
    if not _allowed(chat_id):
        await update.message.reply_text("Chat no autorizado para este bot.")
        return
    await _heartbeat()
    await update.message.reply_text(
        "Soy Quantia. Escribí directamente lo que quieras saber de tu cartera, una decisión, oportunidades, resultados o el estado del sistema."
    )


async def _run_message(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    raw_text: str,
) -> None:
    if not update.effective_chat or not update.message:
        return
    chat_id = int(update.effective_chat.id)
    if not _allowed(chat_id):
        await update.message.reply_text("Chat no autorizado para este bot.")
        return
    await _heartbeat()

    # Sensitive account-linking state is consumed before any harness/LLM call.
    if await _handle_account_secret_step(update, context):
        return

    goal = _legacy_goal(raw_text)
    if _account_setup_requested(goal):
        force = "reconfigurar" in _plain(goal) or "cambiar credenciales" in _plain(goal)
        await _begin_account_setup(update, context, force=force)
        return
    if not goal.strip():
        await update.message.reply_text("Escribí lo que quieras saber de Quantia.")
        return

    harness = _HARNESS
    if harness is None:
        await update.message.reply_text(
            "Quantia no pudo inicializar el harness conversacional."
        )
        return

    lock = _CHAT_LOCKS.setdefault(chat_id, asyncio.Lock())
    async with lock:
        try:
            await context.bot.send_chat_action(
                chat_id=chat_id,
                action=ChatAction.TYPING,
            )
            result = await harness.handle(owner_chat_id=chat_id, message=goal)
            for chunk in _split_message(result.answer):
                await update.message.reply_text(
                    chunk,
                    disable_web_page_preview=True,
                )
        except Exception:
            logger.exception("Conversational harness failed chat_id=%s", chat_id)
            await update.message.reply_text(
                "La consulta se detuvo de forma segura. No se ejecutaron operaciones ni se modificó capital."
            )


async def text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _run_message(
        update,
        context,
        update.message.text if update.message else "",
    )


async def hidden_command_compat(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    await _run_message(
        update,
        context,
        update.message.text if update.message else "",
    )


def build_app() -> Application:
    app = Application.builder().token(_token()).post_init(_post_init).build()
    # /start is a Telegram transport bootstrap, not application navigation.
    app.add_handler(CommandHandler("start", start))
    # Hidden backward compatibility: legacy commands traverse the same harness.
    app.add_handler(MessageHandler(filters.COMMAND, hidden_command_compat))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_message))
    return app


def main() -> None:
    global _HARNESS
    _HARNESS = ConversationalHarness.from_project()
    app = build_app()
    logger.info("Starting Quantia conversational Telegram gateway")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()

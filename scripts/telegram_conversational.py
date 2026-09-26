#!/usr/bin/env python3
"""Quantia Telegram conversational gateway.

User-facing UX is text-only. Legacy handlers remain importable as internal
business adapters, but no keyboard/menu/command catalog is exposed here.
"""
from __future__ import annotations

import asyncio
import logging
import re
import sys
from contextlib import suppress
from html import escape
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

from scripts import telegram_bot as legacy
from src.agentic.conversation.gateway import run_message
from src.analysis.economic_meta_watcher import run_economic_meta_watcher_loop

logger = logging.getLogger(__name__)
_ACTIVE_CHATS: set[int] = set()
HEARTBEAT_TASK_KEY = "conversational_heartbeat_task"
META_TASK_KEY = "conversational_meta_watcher_task"

# Legacy settings code occasionally tries to return to the old inline menu. Keep
# the credential/business logic reusable while making that navigation a no-op.
async def _no_menu(_context, _chat_id: int) -> None:
    return None

legacy.send_menu = _no_menu


def _command_to_language(text: str) -> str:
    """Compatibility only; slash commands are never advertised by the new UI."""
    clean = str(text or "").strip()
    if not clean.startswith("/"):
        return clean
    first, *rest = clean.split(maxsplit=1)
    command = first.split("@", 1)[0].lstrip("/").lower()
    arg = rest[0] if rest else ""
    aliases = {
        "portfolio": "¿Cómo está mi cartera?",
        "analisis": "Analizá mi cartera y las decisiones actuales.",
        "analysis": "Analizá mi cartera y las decisiones actuales.",
        "performance": "¿Cómo vienen los resultados de Quantia?",
        "neto": "¿Cuál fue el resultado económico neto disponible?",
        "ledger": "Mostrame el Decision Ledger y explicame el resultado económico.",
        "analytics": "Explicame la evidencia de Analytics v2.",
        "viability": "Explicame la evidencia de viabilidad disponible.",
        "radar": "Buscame oportunidades relevantes para mi cartera.",
        "mercado": "¿Cómo está el contexto de mercado y macro?",
        "status": "¿Está funcionando bien Quantia?",
        "meta": "Explicame la Economic Meta Policy y dejá claro qué está en shadow.",
        "agente": arg or "¿Cómo está mi cartera?",
        "agent": arg or "¿Cómo está mi cartera?",
    }
    if command in {"ticker", "tecnico", "accion"} and arg:
        return f"Analizá {arg}."
    if command == "shadow" and arg:
        return f"Explicame la evidencia shadow de {arg}."
    return aliases.get(command, clean.lstrip("/"))


def _configuration_intent(text: str) -> bool:
    normalized = re.sub(r"\s+", " ", str(text).lower()).strip()
    return any(phrase in normalized for phrase in (
        "configurar mi cuenta", "configurar cuenta", "cambiar credenciales",
        "reconfigurar mi cuenta", "conectar mi cuenta",
    ))


async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not await legacy.ensure_allowed_chat(update, context):
        return
    await legacy.send_text(
        context,
        int(update.effective_chat.id),
        "<b>QUANTIA</b>\nEscribí lo que quieras saber sobre tu cartera, decisiones, resultados o evidencia.",
    )


async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    if not await legacy.ensure_allowed_chat(update, context):
        return
    chat_id = int(update.effective_chat.id)
    text = _command_to_language(update.message.text or "")

    # Preserve the existing secure credential flow in multiuser mode without
    # surfacing its old menu/buttons.
    if context.user_data.get(legacy.SETTINGS_STATE_KEY):
        await legacy.settings_text_handler(update, context)
        return
    if _configuration_intent(text) and legacy._multiuser_enabled():
        await legacy.action_settings(context, chat_id, force_reconfigure=True)
        return

    if chat_id in _ACTIVE_CHATS:
        await legacy.send_text(context, chat_id, "Ya estoy procesando tu consulta anterior.")
        return
    _ACTIVE_CHATS.add(chat_id)
    try:
        try:
            await context.bot.send_chat_action(chat_id=chat_id, action="typing")
        except Exception:
            pass
        cfg = legacy.get_config()
        configured_raw = str(getattr(cfg.scraper, "telegram_chat_id", "") or "").strip()
        configured_owner = int(configured_raw) if configured_raw.lstrip("-").isdigit() else None

        async def refresh() -> str:
            return await legacy.sync_operational_state(owner_chat_id=chat_id)

        result = await run_message(
            message=text,
            database_url=str(cfg.database.url),
            owner_chat_id=chat_id,
            multiuser_enabled=bool(cfg.multiuser_enabled),
            configured_owner_chat_id=configured_owner,
            repo_root=PROJECT_ROOT,
            refresh_callback=refresh,
        )
        warning = str(result.metadata.get("operational_refresh_warning") or "").strip()
        body = escape(result.answer)
        if warning:
            body += "\n\n<i>Nota de actualización: " + escape(re.sub(r"<[^>]+>", "", warning)) + "</i>"
        await legacy.send_text(context, chat_id, body)
        logger.info(
            "[CHAT] run=%s status=%s tools=%s latency_ms=%s intent=%s",
            result.run_id,
            result.status,
            result.tool_calls,
            result.latency_ms,
            result.task.intent,
        )
    except Exception as exc:
        logger.exception("[CHAT] conversational harness failed")
        await legacy.send_text(
            context,
            chat_id,
            "No pude completar la consulta de forma segura. No se ejecutó ninguna operación. "
            f"Error técnico: <code>{escape(type(exc).__name__)}</code>.",
        )
    finally:
        _ACTIVE_CHATS.discard(chat_id)


async def post_init(app: Application) -> None:
    # Remove Telegram's visible command catalog. /start remains an invisible
    # transport bootstrap only; all normal interaction is natural language.
    try:
        from telegram import MenuButtonDefault
        await app.bot.set_my_commands([])
        await app.bot.set_chat_menu_button(menu_button=MenuButtonDefault())
    except Exception as exc:
        logger.warning("[CHAT] no pude limpiar comandos nativos: %s", exc)

    app.bot_data[HEARTBEAT_TASK_KEY] = asyncio.create_task(
        legacy.bot_heartbeat_loop(app), name="conversational_heartbeat"
    )
    if legacy.get_config is not None:
        try:
            cfg = legacy.get_config()
            app.bot_data[META_TASK_KEY] = asyncio.create_task(
                run_economic_meta_watcher_loop(str(cfg.database.url)),
                name="economic_meta_watcher",
            )
        except Exception:
            logger.exception("[CHAT] meta watcher no pudo iniciar; chat sigue fail-safe")


async def post_shutdown(app: Application) -> None:
    for key in (HEARTBEAT_TASK_KEY, META_TASK_KEY):
        task = app.bot_data.get(key)
        if task:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("[CHAT] Telegram error", exc_info=context.error)
    if isinstance(update, Update) and update.effective_chat:
        try:
            await legacy.send_text(context, int(update.effective_chat.id), "Ocurrió un error interno. No se ejecutó ninguna operación.")
        except Exception:
            pass


def build_app() -> Application:
    app = (
        Application.builder()
        .token(legacy._get_token())
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )
    # /start is the Telegram bootstrap. No other visible command or callback
    # handler is registered; legacy slash input is accepted by the text gateway.
    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(MessageHandler(filters.TEXT, text_handler))
    app.add_error_handler(error_handler)
    return app


def main() -> None:
    logger.info("[CHAT] Iniciando Quantia conversational harness")
    build_app().run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()

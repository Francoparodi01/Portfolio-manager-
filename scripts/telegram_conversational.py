#!/usr/bin/env python3
"""Quantia Telegram conversational gateway with shortcut buttons.

Free-form text always uses the unified conversational harness. Inline buttons are
only shortcuts that translate into bounded natural-language queries for the same
harness, so the UI is richer without reintroducing a second decision path.
"""
# ruff: noqa: E402
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

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from scripts import telegram_bot as legacy
from src.agentic.conversation.gateway import run_message
from src.analysis.economic_meta_watcher import run_economic_meta_watcher_loop

logger = logging.getLogger(__name__)
_ACTIVE_CHATS: set[int] = set()
HEARTBEAT_TASK_KEY = "conversational_heartbeat_task"
META_TASK_KEY = "conversational_meta_watcher_task"

QUICK_ACTION_PROMPTS: dict[str, str] = {
    "portfolio": "¿Cómo está mi cartera?",
    "analysis": "Analizá mi cartera y las decisiones actuales.",
    "radar": "Buscame oportunidades relevantes para mi cartera.",
    "performance": "¿Cómo vienen los resultados de Quantia?",
    "analytics": "Explicame la evidencia de Analytics v2.",
    "meta": "Explicame la Economic Meta Policy y dejá claro qué está en shadow.",
    "status": "¿Está funcionando bien Quantia?",
}


def quick_keyboard() -> InlineKeyboardMarkup:
    """Compact shortcuts; every analytical action still enters the same harness."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 Portfolio", callback_data="quick:portfolio"),
            InlineKeyboardButton("🧠 Análisis", callback_data="quick:analysis"),
        ],
        [
            InlineKeyboardButton("🔭 Radar", callback_data="quick:radar"),
            InlineKeyboardButton("📈 Performance", callback_data="quick:performance"),
        ],
        [
            InlineKeyboardButton("📐 Analytics", callback_data="quick:analytics"),
            InlineKeyboardButton("🧪 Meta", callback_data="quick:meta"),
        ],
        [
            InlineKeyboardButton("🤖 Agente", callback_data="quick:agent"),
            InlineKeyboardButton("⚙️ Status", callback_data="quick:status"),
        ],
    ])


async def _send_quick_menu(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    await context.bot.send_message(
        chat_id=chat_id,
        text=(
            "<b>QUANTIA</b>\n"
            "Elegí un atajo o escribí cualquier pregunta sobre tu cartera, "
            "decisiones, resultados o evidencia."
        ),
        parse_mode="HTML",
        reply_markup=quick_keyboard(),
    )


# Legacy settings/credential flows may return to send_menu. Keep their business
# logic, but make the destination the new hybrid conversational + shortcuts UI.
legacy.send_menu = _send_quick_menu


def _command_to_language(text: str) -> str:
    """Compatibility only; slash commands are accepted but not advertised."""
    clean = str(text or "").strip()
    if not clean.startswith("/"):
        return clean
    first, *rest = clean.split(maxsplit=1)
    command = first.split("@", 1)[0].lstrip("/").lower()
    arg = rest[0] if rest else ""
    aliases = {
        "portfolio": QUICK_ACTION_PROMPTS["portfolio"],
        "analisis": QUICK_ACTION_PROMPTS["analysis"],
        "analysis": QUICK_ACTION_PROMPTS["analysis"],
        "performance": QUICK_ACTION_PROMPTS["performance"],
        "neto": "¿Cuál fue el resultado económico neto disponible?",
        "ledger": "Mostrame el Decision Ledger y explicame el resultado económico.",
        "analytics": QUICK_ACTION_PROMPTS["analytics"],
        "viability": "Explicame la evidencia de viabilidad disponible.",
        "radar": QUICK_ACTION_PROMPTS["radar"],
        "mercado": "¿Cómo está el contexto de mercado y macro?",
        "status": QUICK_ACTION_PROMPTS["status"],
        "meta": QUICK_ACTION_PROMPTS["meta"],
        "agente": arg or QUICK_ACTION_PROMPTS["portfolio"],
        "agent": arg or QUICK_ACTION_PROMPTS["portfolio"],
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
    await _send_quick_menu(context, int(update.effective_chat.id))


async def _run_harness_query(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    text: str,
) -> None:
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


async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    if not await legacy.ensure_allowed_chat(update, context):
        return

    chat_id = int(update.effective_chat.id)
    text = _command_to_language(update.message.text or "")

    # Preserve the existing secure credential flow in multiuser mode.
    if context.user_data.get(legacy.SETTINGS_STATE_KEY):
        await legacy.settings_text_handler(update, context)
        return
    if _configuration_intent(text) and legacy._multiuser_enabled():
        await legacy.action_settings(context, chat_id, force_reconfigure=True)
        return

    await _run_harness_query(context, chat_id, text)


async def quick_action_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None or update.effective_chat is None:
        return
    if not await legacy.ensure_allowed_chat(update, context):
        return

    try:
        await query.answer()
    except Exception:
        pass

    action = str(query.data or "").removeprefix("quick:").strip().lower()
    chat_id = int(update.effective_chat.id)
    if action == "agent":
        await legacy.send_text(
            context,
            chat_id,
            "🤖 Escribime tu pregunta en lenguaje natural. El agente actual sigue activo.",
        )
        return

    prompt = QUICK_ACTION_PROMPTS.get(action)
    if prompt is None:
        await legacy.send_text(context, chat_id, "Atajo no reconocido.")
        return
    await _run_harness_query(context, chat_id, prompt)


async def post_init(app: Application) -> None:
    # Keep the visible slash-command catalog clean; the inline shortcut keyboard
    # is the discoverable UI while free-form text remains the primary interface.
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
            await legacy.send_text(
                context,
                int(update.effective_chat.id),
                "Ocurrió un error interno. No se ejecutó ninguna operación.",
            )
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
    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(CommandHandler("menu", start_handler))
    app.add_handler(CallbackQueryHandler(quick_action_handler, pattern=r"^quick:"))
    app.add_handler(MessageHandler(filters.TEXT, text_handler))
    app.add_error_handler(error_handler)
    return app


def main() -> None:
    logger.info("[CHAT] Iniciando Quantia conversational harness + shortcuts")
    build_app().run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()

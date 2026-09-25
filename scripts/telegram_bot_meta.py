#!/usr/bin/env python3
"""Telegram bot entrypoint with Economic Meta Policy read-only commands."""
from __future__ import annotations

import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from telegram import Update
from telegram.ext import CommandHandler, ContextTypes

from scripts import telegram_bot as base
from src.analysis.economic_meta_telegram import render_latest_meta, render_meta_status

logger = logging.getLogger(__name__)


def _register_command_spec() -> None:
    if not any(name == "meta" for name, _ in base.BOT_COMMAND_SPECS):
        base.BOT_COMMAND_SPECS.insert(1, ("meta", "Economic Meta Policy shadow"))


async def meta_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not await base.ensure_allowed_chat(update, context):
        return

    chat_id = int(update.effective_chat.id)
    args = [str(value).strip() for value in (context.args or []) if str(value).strip()]
    try:
        if args and args[0].lower() in {"status", "estado"}:
            text = render_meta_status()
        else:
            ticker = None
            if args and args[0].lower() not in {"cartera", "portfolio", "all", "todos"}:
                ticker = args[0].upper()
            text = render_latest_meta(ticker=ticker)
        await base.send_text(context, chat_id, text, parse_mode=None)
    except Exception as exc:
        logger.exception("[META][TELEGRAM] read-only command failed")
        await base.send_text(
            context,
            chat_id,
            "🧪 Economic Meta Policy · error\n"
            f"No pude leer la evidencia shadow: {type(exc).__name__}: {exc}\n"
            "Capital effect: NO",
            parse_mode=None,
        )


def build_app():
    _register_command_spec()
    app = base.build_app()
    app.add_handler(CommandHandler("meta", meta_handler))
    return app


def main() -> None:
    logger.info("[BOT] Iniciando Cocos Copilot + Economic Meta Policy read-only")
    build_app().run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()

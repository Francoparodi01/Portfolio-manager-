#!/usr/bin/env python3
"""Telegram bot entrypoint with Economic Meta Policy shadow automation."""
from __future__ import annotations

import asyncio
import logging
import os
import sys
from contextlib import suppress
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from telegram import Update
from telegram.ext import CommandHandler, ContextTypes

from scripts import telegram_bot as base
from src.analysis.economic_meta_report_ingest import ingest_analysis_report
from src.analysis.economic_meta_telegram import render_latest_meta, render_meta_status
from src.analysis.economic_meta_watcher import run_economic_meta_watcher_loop

logger = logging.getLogger(__name__)

_BASE_POST_INIT = base.post_init
_BASE_POST_SHUTDOWN = base.post_shutdown
_BASE_RUN_FIRST_EXISTING_SCRIPT = base.run_first_existing_script
META_WATCHER_TASK_KEY = "economic_meta_watcher_task"


def _register_command_spec() -> None:
    if not any(name == "meta" for name, _ in base.BOT_COMMAND_SPECS):
        base.BOT_COMMAND_SPECS.insert(1, ("meta", "Economic Meta Policy shadow"))


def _env_int(name: str, default: int, minimum: int) -> int:
    try:
        return max(minimum, int(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        return default


def _watcher_state(app) -> str:
    task = app.bot_data.get(META_WATCHER_TASK_KEY)
    if task is None:
        return "NO_INICIADO"
    if task.cancelled():
        return "CANCELADO"
    if task.done():
        return "ERROR" if task.exception() else "FINALIZADO"
    return "RUNNING"


def _contains_analysis_command(candidates: list[list[str]]) -> bool:
    for candidate in candidates or []:
        if not candidate:
            continue
        script = str(candidate[0]).replace("\\", "/")
        if script.endswith("scripts/run_analysis.py"):
            return True
    return False


async def _meta_run_first_existing_script(
    candidates: list[list[str]],
    timeout: int = base.COMMAND_TIMEOUT_SECONDS,
) -> str:
    """Mirror rendered analysis output into shadow without changing the command."""
    report = await _BASE_RUN_FIRST_EXISTING_SCRIPT(candidates, timeout=timeout)
    if not _contains_analysis_command(candidates):
        return report

    try:
        summary = ingest_analysis_report(report)
        if int(summary.get("candidate_count", 0)) > 0:
            logger.info("[META][REPORT] %s", summary)
    except Exception:
        # Presentation-side shadow ingestion must never break the user-facing
        # analysis or any production decision path.
        logger.exception("[META][REPORT] ingest fallo; análisis productivo no afectado")
    return report


async def _meta_post_init(app) -> None:
    await _BASE_POST_INIT(app)
    if base.get_config is None:
        logger.warning("[META][WATCHER] config no disponible; watcher deshabilitado")
        return
    try:
        cfg = base.get_config()
        database_url = str(cfg.database.url)
        poll_seconds = _env_int("ECONOMIC_META_POLL_SECONDS", 15, 5)
        settle_seconds = _env_int("ECONOMIC_META_SETTLE_SECONDS", 45, 10)
        task = asyncio.create_task(
            run_economic_meta_watcher_loop(
                database_url,
                poll_interval_seconds=poll_seconds,
                settle_seconds=settle_seconds,
            ),
            name="economic_meta_watcher",
        )
        app.bot_data[META_WATCHER_TASK_KEY] = task
        logger.info(
            "[META][WATCHER] iniciado poll=%ss settle=%ss SHADOW_ONLY",
            poll_seconds,
            settle_seconds,
        )
    except Exception:
        logger.exception(
            "[META][WATCHER] no pudo iniciar; Telegram y producción continúan sin cambios"
        )


async def _meta_post_shutdown(app) -> None:
    task = app.bot_data.get(META_WATCHER_TASK_KEY)
    if task:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
    await _BASE_POST_SHUTDOWN(app)


async def meta_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not await base.ensure_allowed_chat(update, context):
        return

    chat_id = int(update.effective_chat.id)
    args = [str(value).strip() for value in (context.args or []) if str(value).strip()]
    try:
        if args and args[0].lower() in {"status", "estado"}:
            text = render_meta_status()
            text += f"\nAuto-ingest análisis: {_watcher_state(context.application)}"
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
    # base.build_app and _dispatch_command resolve these module globals at call
    # time. Wrapping them here keeps the user's telegram_bot.py untouched.
    base.post_init = _meta_post_init
    base.post_shutdown = _meta_post_shutdown
    base.run_first_existing_script = _meta_run_first_existing_script
    app = base.build_app()
    app.add_handler(CommandHandler("meta", meta_handler))
    return app


def main() -> None:
    logger.info("[BOT] Iniciando Cocos Copilot + Economic Meta Policy shadow")
    build_app().run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()

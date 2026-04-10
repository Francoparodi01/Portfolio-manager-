"""
scripts/telegram_bot.py
-----------------------
Bot de Telegram para Cocos Copilot.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import sys

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Message, Update
from telegram.ext import (
    Application, CallbackQueryHandler, CommandHandler,
    ContextTypes, MessageHandler, filters,
)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.core.redis_client import client as redis_client

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    level=logging.INFO,
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN  = os.environ["TELEGRAM_BOT_TOKEN"]
ALLOWED_CHAT_ID = int(os.environ["TELEGRAM_CHAT_ID"])

_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
_BASE_DIR    = os.path.dirname(_SCRIPTS_DIR)

_HTML_TAG_RE = re.compile(r"<[^>]+>")


# ── Utilidades ────────────────────────────────────────────────────────────────

def _strip_html(text: str) -> str:
    return _HTML_TAG_RE.sub("", text)


def _main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 Portfolio",       callback_data="portfolio"),
            InlineKeyboardButton("🧠 Análisis",        callback_data="analisis"),
        ],
        [
            InlineKeyboardButton("🔄 Scrape manual",   callback_data="scrape"),
            InlineKeyboardButton("🖥️ Estado",          callback_data="status"),
        ],
        [
            InlineKeyboardButton("⚡ Análisis rápido", callback_data="analisis_rapido"),
            InlineKeyboardButton("🔭 Oportunidades",   callback_data="oportunidades"),
        ],
        [
            InlineKeyboardButton("📈 Performance",     callback_data="performance"),
            InlineKeyboardButton("📅 Resumen semanal", callback_data="resumen_semanal"),
        ],
        [
            InlineKeyboardButton("❓ Ayuda",           callback_data="ayuda"),
        ],
    ])


async def _reply_menu(message: Message, text: str = "¿Qué querés hacer?") -> None:
    await message.reply_text(text, reply_markup=_main_keyboard())


async def _send_html_chunk(message: Message, chunk: str) -> None:
    """Envía un chunk con HTML; si el parser falla, lo reenvía sin tags."""
    try:
        await message.reply_text(chunk, parse_mode="HTML")
    except Exception:
        await message.reply_text(_strip_html(chunk))


def only_allowed(func):
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        chat_id = (
            update.effective_chat.id
            if update.effective_chat
            else (update.callback_query.message.chat.id if update.callback_query else None)
        )
        if chat_id != ALLOWED_CHAT_ID:
            return
        return await func(update, ctx)
    return wrapper


# ── Acciones ──────────────────────────────────────────────────────────────────

async def _action_status(message: Message) -> None:
    msg   = await message.reply_text("🔍 Verificando sistema...")
    lines = ["🖥️ <b>Estado del sistema</b>\n"]

    try:
        pong = await redis_client.ping()
        lines.append("✅ Redis Cloud — conectado" if pong else "❌ Redis — sin respuesta")
    except Exception as e:
        lines.append(f"❌ Redis — error: {e}")

    try:
        from src.core.config import get_config
        from src.collector.db import PortfolioDatabase
        db = PortfolioDatabase(get_config().database.url)
        await db.connect()
        snap = await db.get_latest_snapshot()
        await db.close()
        if snap:
            ts = snap.get("scraped_at", "—")[:16].replace("T", " ")
            n  = len(snap.get("positions", []))
            lines.append(f"✅ DB — último snapshot: {ts} UTC ({n} posiciones)")
        else:
            lines.append("⚠️ DB — conectada pero sin snapshots")
    except Exception as e:
        lines.append(f"❌ DB — {e}")

    await msg.edit_text("\n".join(lines), parse_mode="HTML")
    await _reply_menu(message)


async def _action_portfolio(message: Message) -> None:
    msg = await message.reply_text("📊 Cargando portfolio...")
    try:
        from src.core.config import get_config
        from src.collector.db import PortfolioDatabase
        db = PortfolioDatabase(get_config().database.url)
        await db.connect()
        snap = await db.get_latest_snapshot()
        await db.close()

        if not snap:
            await msg.edit_text("⚠️ Sin snapshots. Corré un scrape primero.")
            await _reply_menu(message)
            return

        ts        = snap.get("scraped_at", "—")[:16].replace("T", " ")
        total     = float(snap.get("total_value_ars", 0) or 0)
        cash      = float(snap.get("cash_ars", 0) or 0)
        positions = snap.get("positions", [])

        lines = [
            f"📊 <b>Portfolio</b> — {ts} UTC\n",
            f"💼 Total: <b>${total:,.0f} ARS</b>",
            f"💵 Cash:  <b>${cash:,.0f} ARS</b>\n",
            "<b>Posiciones:</b>",
        ]
        sorted_pos = sorted(positions, key=lambda p: float(p.get("market_value", 0) or 0), reverse=True)
        total_inv  = sum(float(p.get("market_value", 0) or 0) for p in sorted_pos)

        for p in sorted_pos:
            ticker = p.get("ticker", "?")
            mv     = float(p.get("market_value", 0) or 0)
            qty    = float(p.get("quantity", 0) or 0)
            price  = float(p.get("current_price", 0) or 0)
            pnl_p  = float(p.get("unrealized_pnl_pct", 0) or 0) * 100
            pct    = mv / total_inv * 100 if total_inv > 0 else 0
            icon   = "🟢" if pnl_p >= 0 else "🔴"
            lines.append(
                f"  {icon} <b>{ticker}</b>  {pct:.1f}%  ${mv:,.0f}  ({pnl_p:+.1f}%)\n"
                f"     x{qty:.0f} @ ${price:,.2f}"
            )

        await msg.edit_text("\n".join(lines), parse_mode="HTML")
    except Exception as e:
        logger.error("_action_portfolio: %s", e, exc_info=True)
        await msg.edit_text(f"❌ Error: {e}")
    finally:
        await _reply_menu(message)


async def _run_subprocess(
    message: Message,
    cmd: list[str],
    wait_text: str,
    timeout: int = 300,
) -> None:
    msg = await message.reply_text(wait_text)
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=_BASE_DIR,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        report = stdout.decode("utf-8", errors="replace").strip()

        if proc.returncode != 0 and not report:
            err = stderr.decode("utf-8", errors="replace")[-500:]
            await msg.edit_text(f"❌ Sin output.\n<code>{err}</code>", parse_mode="HTML")
            return

        await msg.delete()
        for i in range(0, max(len(report), 1), 3500):
            await _send_html_chunk(message, report[i : i + 3500])

    except asyncio.TimeoutError:
        await msg.edit_text(f"⏱️ Timeout ({timeout // 60} min). Revisá los logs.")
    except Exception as e:
        logger.error("subprocess error: %s", e, exc_info=True)
        await msg.edit_text(f"❌ Error: {e}")
    finally:
        await _reply_menu(message)


async def _action_analisis(message: Message, *, rapido: bool = False) -> None:
    cmd = [sys.executable, "scripts/run_analysis.py", "--no-telegram"]
    if rapido:
        cmd += ["--no-llm", "--no-sentiment"]
    await _run_subprocess(
        message, cmd,
        "⚡ Modo rápido en curso..." if rapido else "🧠 Análisis completo...\n(2-3 min)",
    )


async def _action_scrape(message: Message) -> None:
    msg = await message.reply_text("🔄 Scrape en curso...")
    try:
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "scripts/run_once.py",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=_BASE_DIR,
        )
        _, stderr = await asyncio.wait_for(proc.communicate(), timeout=180)
        if proc.returncode == 0:
            await msg.delete()
            await _action_portfolio(message)
            return
        err = stderr.decode("utf-8", errors="replace")[-800:]
        await msg.edit_text(f"❌ Scrape falló.\n<code>{err}</code>", parse_mode="HTML")
    except asyncio.TimeoutError:
        await msg.edit_text("⏱️ Timeout (3 min). Revisá los logs.")
    except Exception as e:
        await msg.edit_text(f"❌ Error: {e}")
    finally:
        await _reply_menu(message)


async def _action_oportunidades(message: Message) -> None:
    await _run_subprocess(
        message,
        [sys.executable, "scripts/run_opportunity.py", "--no-telegram"],
        "🔭 Analizando radar de oportunidades...\n(2-3 min)",
    )


async def _action_performance(message: Message) -> None:
    await _run_subprocess(
        message,
        [sys.executable, "scripts/run_performance.py", "--no-telegram"],
        "📈 Calculando performance del sistema...",
        timeout=120,
    )


async def _action_resumen_semanal(message: Message) -> None:
    await _run_subprocess(
        message,
        [sys.executable, "scripts/weekly_summary.py", "--no-telegram"],
        "📅 Generando resumen semanal...",
        timeout=120,
    )


# ── Despachador central ───────────────────────────────────────────────────────

_ACTIONS: dict[str, object] = {
    "status":          _action_status,
    "portfolio":       _action_portfolio,
    "analisis":        lambda m: _action_analisis(m, rapido=False),
    "analisis_rapido": lambda m: _action_analisis(m, rapido=True),
    "scrape":          _action_scrape,
    "oportunidades":   _action_oportunidades,
    "performance":     _action_performance,
    "resumen_semanal": _action_resumen_semanal,
}

_AYUDA_TEXT = (
    "📋 <b>Comandos disponibles</b>\n\n"
    "/start             — menú principal\n"
    "/portfolio         — último snapshot del portfolio\n"
    "/analisis          — pipeline cuantitativo completo\n"
    "/analisis_rapido   — sin LLM ni sentiment (más rápido)\n"
    "/scrape            — scrape manual del portfolio\n"
    "/oportunidades     — radar de nuevas acciones\n"
    "/performance       — win rate, EV y últimas decisiones\n"
    "/resumen_semanal   — comparativa de la semana\n"
    "/status            — estado del sistema\n"
    "/ayuda             — esta lista\n\n"
    "<b>MFA:</b> cuando el sistema pida código, mandá los 6 dígitos."
)


# ── Handlers ──────────────────────────────────────────────────────────────────

@only_allowed
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "👋 <b>Cocos Copilot</b> activo.\n\n¿Qué querés hacer?",
        parse_mode="HTML",
        reply_markup=_main_keyboard(),
    )


@only_allowed
async def cmd_ayuda(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(_AYUDA_TEXT, parse_mode="HTML")


@only_allowed
async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    if query.data == "ayuda":
        await query.message.reply_text(_AYUDA_TEXT, parse_mode="HTML")
        return

    action_fn = _ACTIONS.get(query.data)
    if action_fn:
        await action_fn(query.message)
    else:
        await query.message.reply_text(f"Acción desconocida: {query.data}")


@only_allowed
async def handle_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Handler genérico para todos los comandos de acción."""
    cmd = update.message.text.lstrip("/").split("@")[0]  # quita el @bot si existe
    action_fn = _ACTIONS.get(cmd)
    if action_fn:
        await action_fn(update.message)


@only_allowed
async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    text = (update.message.text or "").strip()

    if re.fullmatch(r"\d{6}", text):
        key = f"mfa:{update.effective_chat.id}"
        await redis_client.lpush(key, text)
        await redis_client.expire(key, 180)
        logger.info("MFA publicado key=%s", key)
        await update.message.reply_text("✅ Código recibido. Intentando login...")
        return

    await update.message.reply_text(
        "No entendí ese mensaje. Usá /start para ver el menú.",
        reply_markup=_main_keyboard(),
    )


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler(["ayuda", "help"], cmd_ayuda))

    # Todos los comandos de acción van al mismo handler genérico
    app.add_handler(CommandHandler(list(_ACTIONS), handle_command))

    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    logger.info("Bot iniciado — esperando mensajes...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
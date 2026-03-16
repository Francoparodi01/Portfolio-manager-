"""
scripts/telegram_bot.py
-----------------------
Bot de Telegram para Cocos Copilot.

Comandos:
  /start      — menú principal con botones
  /status     — estado de los contenedores y Redis
  /portfolio  — último snapshot del portfolio
  /analisis   — lanzar pipeline cuantitativo completo
  /scrape     — scrape manual del portfolio
  /ayuda      — lista de comandos

MFA:
  Cuando el scraper pide el código de 2FA, el bot espera un número
  de 6 dígitos y lo publica en Redis (LPUSH mfa:<chat_id>).
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import subprocess
import sys

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ── Path setup ────────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.redis_client import client as redis_client

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    level=logging.INFO,
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.environ["TELEGRAM_BOT_TOKEN"]
ALLOWED_CHAT_ID  = int(os.environ["TELEGRAM_CHAT_ID"])

# ── Guards ────────────────────────────────────────────────────────────────────

def only_allowed(func):
    """Decorator: ignora mensajes de chats no autorizados."""
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        chat_id = (
            update.effective_chat.id
            if update.effective_chat
            else (update.callback_query.message.chat.id if update.callback_query else None)
        )
        if chat_id != ALLOWED_CHAT_ID:
            logger.warning(f"Chat no autorizado: {chat_id}")
            return
        return await func(update, ctx)
    return wrapper


# ── Menú principal ────────────────────────────────────────────────────────────

def _main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 Portfolio",   callback_data="portfolio"),
            InlineKeyboardButton("🧠 Análisis",    callback_data="analisis"),
        ],
        [
            InlineKeyboardButton("🔄 Scrape manual", callback_data="scrape"),
            InlineKeyboardButton("🖥️ Estado",        callback_data="status"),
        ],
        [
            InlineKeyboardButton("⚡ Análisis rápido", callback_data="analisis_rapido"),
            InlineKeyboardButton("❓ Ayuda",            callback_data="ayuda"),
        ],
    ])


@only_allowed
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "👋 <b>Cocos Copilot</b> activo.\n\n"
        "¿Qué querés hacer?",
        parse_mode="HTML",
        reply_markup=_main_keyboard(),
    )


@only_allowed
async def cmd_ayuda(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "📋 <b>Comandos disponibles</b>\n\n"
        "/start     — menú principal\n"
        "/portfolio — último snapshot del portfolio\n"
        "/analisis  — pipeline cuantitativo completo\n"
        "/scrape    — scrape manual del portfolio\n"
        "/status    — estado del sistema\n"
        "/ayuda     — esta lista\n\n"
        "<b>MFA:</b> cuando el sistema pida código, mandá los 6 dígitos."
    )
    await update.message.reply_text(text, parse_mode="HTML")


# ── /status ───────────────────────────────────────────────────────────────────

@only_allowed
async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    msg = await update.message.reply_text("🔍 Verificando sistema...")

    lines = ["🖥️ <b>Estado del sistema</b>\n"]

    # Redis
    try:
        pong = await redis_client.ping()
        lines.append("✅ Redis Cloud — conectado" if pong else "❌ Redis — sin respuesta")
    except Exception as e:
        lines.append(f"❌ Redis — error: {e}")

    # DB
    try:
        from src.core.config import get_config
        from src.collector.db import PortfolioDatabase
        cfg = get_config()
        db  = PortfolioDatabase(cfg.database.url)
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


# ── /portfolio ────────────────────────────────────────────────────────────────

@only_allowed
async def cmd_portfolio(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    msg = await update.message.reply_text("📊 Cargando portfolio...")

    try:
        from src.core.config import get_config
        from src.collector.db import PortfolioDatabase

        cfg = get_config()
        db  = PortfolioDatabase(cfg.database.url)
        await db.connect()
        snap = await db.get_latest_snapshot()
        await db.close()

        if not snap:
            await msg.edit_text("⚠️ Sin snapshots disponibles. Corré un scrape primero.")
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
            pnl_e  = "🟢" if pnl_p >= 0 else "🔴"
            lines.append(
                f"  {pnl_e} <b>{ticker}</b>  {pct:.1f}%  "
                f"${mv:,.0f}  ({pnl_p:+.1f}%)\n"
                f"     x{qty:.0f} @ ${price:,.2f}"
            )

        await msg.edit_text("\n".join(lines), parse_mode="HTML")

    except Exception as e:
        logger.error(f"cmd_portfolio: {e}", exc_info=True)
        await msg.edit_text(f"❌ Error: {e}")


# ── /analisis ─────────────────────────────────────────────────────────────────

async def _run_analysis(update_or_query, rapido: bool = False) -> None:
    """Lanza el pipeline en background y manda el resultado."""
    is_query = hasattr(update_or_query, "message") and hasattr(update_or_query, "data")
    reply    = update_or_query.message if is_query else update_or_query.message

    msg = await reply.reply_text(
        "⚡ Modo rápido en curso..." if rapido else "🧠 Análisis completo en curso...\n(puede tardar 2-3 min)"
    )

    cmd = ["python", "scripts/run_analysis.py", "--no-telegram"]
    if rapido:
        cmd += ["--no-llm", "--no-sentiment"]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd="/app",
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=300)

        report = stdout.decode("utf-8", errors="replace").strip()
        if not report:
            err = stderr.decode("utf-8", errors="replace")[-500:]
            await msg.edit_text(f"❌ Sin output.\n<code>{err}</code>", parse_mode="HTML")
            return

        # Enviar el reporte (chunkeado si es largo)
        await msg.delete()
        max_len = 3500
        for i in range(0, len(report), max_len):
            await reply.reply_text(report[i:i + max_len], parse_mode="HTML")

    except asyncio.TimeoutError:
        await msg.edit_text("⏱️ Timeout (5 min). Revisá los logs.")
    except Exception as e:
        logger.error(f"run_analysis: {e}", exc_info=True)
        await msg.edit_text(f"❌ Error: {e}")


@only_allowed
async def cmd_analisis(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await _run_analysis(update)


@only_allowed
async def cmd_analisis_rapido(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await _run_analysis(update, rapido=True)


# ── /scrape ───────────────────────────────────────────────────────────────────

@only_allowed
async def cmd_scrape(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    msg = await update.message.reply_text("🔄 Scrape en curso...")

    try:
        proc = await asyncio.create_subprocess_exec(
            "python", "scripts/run_once.py",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd="/app",
        )
        _, stderr = await asyncio.wait_for(proc.communicate(), timeout=180)

        if proc.returncode == 0:
            await msg.edit_text("✅ Scrape completado. Usá /portfolio para ver el resultado.")
        else:
            err = stderr.decode("utf-8", errors="replace")[-800:]
            await msg.edit_text(f"❌ Scrape falló.\n<code>{err}</code>", parse_mode="HTML")

    except asyncio.TimeoutError:
        await msg.edit_text("⏱️ Timeout (3 min). Revisá los logs.")
    except Exception as e:
        await msg.edit_text(f"❌ Error: {e}")


# ── Callback de botones inline ────────────────────────────────────────────────

CALLBACK_MAP = {
    "portfolio":      cmd_portfolio,
    "status":         cmd_status,
    "analisis":       cmd_analisis,
    "analisis_rapido": cmd_analisis_rapido,
    "scrape":         cmd_scrape,
}


@only_allowed
async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    action = query.data

    if action == "ayuda":
        await query.message.reply_text(
            "📋 <b>Comandos</b>\n\n"
            "/start — menú\n/portfolio — portfolio\n"
            "/analisis — análisis completo\n/scrape — scrape manual\n"
            "/status — estado\n/ayuda — ayuda",
            parse_mode="HTML",
        )
        return

    handler = CALLBACK_MAP.get(action)
    if handler:
        # Simular un update con el message del callback
        fake_update = update
        fake_update._effective_message = query.message
        await handler(fake_update, ctx)
    else:
        await query.message.reply_text(f"Acción desconocida: {action}")


# ── Handler de texto — captura MFA ────────────────────────────────────────────

@only_allowed
async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    text    = (update.message.text or "").strip()
    chat_id = update.effective_chat.id

    # Código MFA de 6 dígitos
    if re.fullmatch(r"\d{6}", text):
        key = f"mfa:{chat_id}"
        await redis_client.lpush(key, text)
        await redis_client.expire(key, 180)
        logger.info(f"Código MFA publicado en Redis key={key}: {text}")
        await update.message.reply_text("✅ Código recibido. Intentando login...")
        return

    # Cualquier otro texto
    await update.message.reply_text(
        "No entendí ese mensaje.\n\n"
        "Usá /start para ver el menú, o mandá el código MFA de 6 dígitos si el sistema lo pidió.",
        reply_markup=_main_keyboard(),
    )


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start",           cmd_start))
    app.add_handler(CommandHandler("ayuda",           cmd_ayuda))
    app.add_handler(CommandHandler("help",            cmd_ayuda))
    app.add_handler(CommandHandler("status",          cmd_status))
    app.add_handler(CommandHandler("portfolio",       cmd_portfolio))
    app.add_handler(CommandHandler("analisis",        cmd_analisis))
    app.add_handler(CommandHandler("analisis_rapido", cmd_analisis_rapido))
    app.add_handler(CommandHandler("scrape",          cmd_scrape))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    logger.info("Bot iniciado — esperando mensajes...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()

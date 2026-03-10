"""
scripts/telegram_bot.py — Bot multi-usuario del sistema cuantitativo.

Diseño:
  - Autorización por ALLOWED_CHAT_IDS (lista en .env)
  - Credenciales Cocos por usuario, encriptadas con Fernet
  - Menú de botones interactivo
  - Captura stdout del pipeline → manda como HTML a Telegram
  - Backtests → manda como <pre> (texto plano)
  - Errores → filtra stderr para mostrar solo líneas relevantes

Output handling (run_pipeline):
  - `--no-telegram` se agrega automáticamente a todos los comandos
    (evita que el script interno también mande al bot)
  - stdout capturado → enviado como HTML directo
  - stderr filtrado → solo ERROR / WARNING / Traceback
  - Si stdout vacío → mensaje "completado sin output"
"""
from __future__ import annotations

import asyncio
import logging
import os
import subprocess
import sys
from html import escape
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application, CallbackQueryHandler, CommandHandler, ContextTypes,
    ConversationHandler, MessageHandler, filters,
)

try:
    from src.core.credentials import get_user, set_user, delete_user, is_configured, mask
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from src.core.credentials import get_user, set_user, delete_user, is_configured, mask

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
_raw_ids  = os.environ.get("ALLOWED_CHAT_IDS", os.environ.get("TELEGRAM_CHAT_ID", ""))
ALLOWED   = {int(x.strip()) for x in _raw_ids.split(",") if x.strip().isdigit()}

(ST_IDLE, ST_WAIT_TICKER, ST_SETUP_USER, ST_SETUP_PASS,
 ST_SETUP_MFA, ST_CONFIG_MENU) = range(6)

MAX_MSG_LEN = 3800


# ── Auth ───────────────────────────────────────────────────────────────────────

def authorized(chat_id: int) -> bool:
    return chat_id in ALLOWED

async def deny(update: Update):
    if update.message:
        await update.message.reply_text("⛔ No autorizado.")
    elif update.callback_query:
        await update.callback_query.answer("No autorizado.")


# ── Output processing ──────────────────────────────────────────────────────────

def _filter_stderr(stderr: bytes, max_chars: int = 1200) -> str:
    """
    Filtra stderr: solo muestra líneas de ERROR, WARNING o Traceback.
    Evita inundar al usuario con logs de INFO/DEBUG.
    """
    if not stderr:
        return ""
    lines = []
    for line in stderr.decode("utf-8", errors="replace").splitlines():
        if any(kw in line for kw in ("ERROR", "WARNING", "CRITICAL",
                                      "Traceback", "Exception", "Error:")):
            lines.append(line)
    filtered = "\n".join(lines)
    return filtered[-max_chars:] if len(filtered) > max_chars else filtered


def _chunk_html(text: str, size: int = MAX_MSG_LEN):
    """Divide el reporte HTML en chunks respetando saltos de línea."""
    lines  = text.split("\n")
    chunk  = []
    length = 0
    for line in lines:
        if length + len(line) + 1 > size and chunk:
            yield "\n".join(chunk)
            chunk  = [line]
            length = len(line)
        else:
            chunk.append(line)
            length += len(line) + 1
    if chunk:
        yield "\n".join(chunk)


# ── Keyboards ──────────────────────────────────────────────────────────────────

def main_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    icon = "✅" if is_configured(chat_id) else "⚠️"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Análisis completo",        callback_data="full")],
        [InlineKeyboardButton("📊 Técnico + macro (rápido)", callback_data="quick"),
         InlineKeyboardButton("🔍 Ticker específico",        callback_data="ticker")],
        [InlineKeyboardButton("📈 Backtest",                 callback_data="backtest"),
         InlineKeyboardButton("⚙️ Status",                   callback_data="status")],
        [InlineKeyboardButton(f"🔧 Mi cuenta {icon}",        callback_data="config")],
    ])

def config_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    u  = get_user(chat_id)
    ok = "✅" if u.get("cocos_user") and u.get("cocos_pass") else "❌"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{ok} Usuario y contraseña", callback_data="cfg_cocos")],
        [InlineKeyboardButton("⏱️ MFA timeout",             callback_data="cfg_mfa")],
        [InlineKeyboardButton("📋 Ver mi configuración",    callback_data="cfg_show")],
        [InlineKeyboardButton("🗑️ Borrar mis datos",        callback_data="cfg_delete")],
        [InlineKeyboardButton("◀️ Volver al menú",          callback_data="menu")],
    ])

def backtest_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📈 Backtest estándar (2y)",        callback_data="bt_standard")],
        [InlineKeyboardButton("🔄 Walk-forward (3y, 9 ventanas)", callback_data="bt_walkforward")],
        [InlineKeyboardButton("🌐 Universe (mejores portfolios)",  callback_data="bt_universe")],
        [InlineKeyboardButton("⏱️ Horizonte (quincenal vs mensual)", callback_data="bt_horizon")],
        [InlineKeyboardButton("◀️ Volver al menú",                callback_data="menu")],
    ])

def confirm_delete_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Sí, borrar todo", callback_data="cfg_delete_yes"),
        InlineKeyboardButton("❌ Cancelar",        callback_data="config"),
    ]])


async def send_menu(chat_id: int, context: ContextTypes.DEFAULT_TYPE,
                    text: str = "¿Qué querés hacer?"):
    await context.bot.send_message(chat_id, text, reply_markup=main_keyboard(chat_id))

async def send_config(chat_id: int, context: ContextTypes.DEFAULT_TYPE,
                      text: str = "🔧 <b>Mi cuenta</b>"):
    await context.bot.send_message(chat_id, text, parse_mode="HTML",
                                   reply_markup=config_keyboard(chat_id))


# ── Pipeline runner ────────────────────────────────────────────────────────────

async def run_pipeline(chat_id: int, context: ContextTypes.DEFAULT_TYPE,
                       cmd: list[str], label: str, is_backtest: bool = False):
    """
    Ejecuta un comando del pipeline y envía el resultado al usuario.

    - Agrega --no-telegram automáticamente para evitar doble envío.
    - Captura stdout y lo envía como HTML (análisis) o <pre> (backtest).
    - Filtra stderr para mostrar solo errores relevantes.
    """
    if not is_configured(chat_id):
        await context.bot.send_message(
            chat_id, "⚠️ Primero configurá tu cuenta desde 🔧 Mi cuenta."
        )
        await send_menu(chat_id, context)
        return

    await context.bot.send_message(
        chat_id, f"⏳ Corriendo: <b>{escape(label)}</b>...", parse_mode="HTML"
    )

    # Asegurar --no-telegram (evita doble envío)
    if "--no-telegram" not in cmd:
        cmd = cmd + ["--no-telegram"]

    logger.info(f"[{chat_id}] Iniciando: {label} | cmd={cmd}")

    try:
        u   = get_user(chat_id)
        env = {
            **os.environ,
            "COCOS_USERNAME":       u.get("cocos_user", ""),
            "COCOS_PASSWORD":       u.get("cocos_pass", ""),
            "TELEGRAM_MFA_TIMEOUT": str(u.get("mfa_timeout", 120)),
            "TELEGRAM_CHAT_ID":     str(chat_id),
        }
        proc = await asyncio.create_subprocess_exec(
            *cmd, cwd="/app",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env,
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=660)
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except Exception:
                pass
            await context.bot.send_message(chat_id, "⏱️ Excedió 11 minutos — el proceso fue cancelado.")
            await send_menu(chat_id, context)
            return

        logger.info(f"[{chat_id}] {label} finalizado — returncode={proc.returncode} stdout={len(stdout)}b stderr={len(stderr)}b")

        if proc.returncode != 0:
            err_text = _filter_stderr(stderr) or (stderr or stdout or b"").decode("utf-8", errors="replace")[-800:]
            logger.error(f"[{chat_id}] {label} ERROR: {err_text[:300]}")
            await context.bot.send_message(
                chat_id,
                f"❌ <b>{escape(label)}</b> falló:\n<code>{escape(err_text[-700:])}</code>",
                parse_mode="HTML",
            )
        else:
            out = (stdout or b"").decode("utf-8", errors="replace").strip()
            if not out:
                await context.bot.send_message(chat_id, f"✅ <b>{escape(label)}</b> completado.")
            elif is_backtest:
                # Backtests → texto monoespaciado
                content = out[-3500:]
                await context.bot.send_message(
                    chat_id,
                    f"📈 <b>{escape(label)}</b>\n<pre>{escape(content)}</pre>",
                    parse_mode="HTML",
                )
            else:
                # Análisis → HTML directo (el reporte ya tiene tags <b>, <code>, etc.)
                for chunk in _chunk_html(out):
                    if chunk.strip():
                        try:
                            await context.bot.send_message(
                                chat_id, chunk, parse_mode="HTML",
                                disable_web_page_preview=True,
                            )
                        except Exception as e:
                            # Si falla el parseo HTML, enviar como texto plano
                            logger.warning(f"HTML send failed, fallback to plain: {e}")
                            await context.bot.send_message(
                                chat_id, chunk,
                                disable_web_page_preview=True,
                            )

    except Exception as e:
        await context.bot.send_message(chat_id, f"❌ Error inesperado: {escape(str(e))}", parse_mode="HTML")

    await send_menu(chat_id, context)


# ── Handlers ───────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not authorized(chat_id):
        await deny(update); return ST_IDLE
    if not is_configured(chat_id):
        await context.bot.send_message(
            chat_id,
            "👋 <b>Bienvenido al sistema cuantitativo.</b>\n\n"
            "Para comenzar necesito tu cuenta de Cocos Capital.\n\n"
            "👤 Ingresá tu <b>email de Cocos</b>:", parse_mode="HTML"
        )
        return ST_SETUP_USER
    u    = get_user(chat_id)
    name = u.get("cocos_user", "").split("@")[0]
    await send_menu(chat_id, context, f"👋 Hola <b>{escape(name)}</b>. Sistema listo.")
    return ST_IDLE

async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not authorized(chat_id):
        await deny(update); return ST_IDLE
    await send_menu(chat_id, context)
    return ST_IDLE


async def setup_recv_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    email   = update.message.text.strip()
    if "@" not in email or "." not in email:
        await update.message.reply_text("❌ Ingresá un email válido.")
        return ST_SETUP_USER
    context.user_data["setup_user"] = email
    await update.message.reply_text(
        f"✅ Usuario: <code>{escape(email)}</code>\n\n"
        "🔑 Ahora ingresá tu <b>contraseña</b>:\n"
        "<i>(el mensaje se borrará automáticamente)</i>", parse_mode="HTML"
    )
    return ST_SETUP_PASS

async def setup_recv_pass(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    pwd     = update.message.text.strip()
    try:
        await update.message.delete()
    except Exception:
        pass
    if len(pwd) < 4:
        await context.bot.send_message(chat_id, "❌ Contraseña muy corta.")
        return ST_SETUP_PASS
    set_user(chat_id, {
        "cocos_user": context.user_data.get("setup_user", ""),
        "cocos_pass": pwd, "mfa_timeout": 120,
    })
    await context.bot.send_message(
        chat_id,
        "✅ <b>Contraseña guardada.</b>\n\n"
        "⏱️ ¿Cuántos segundos esperamos el MFA? (60-300, default: <code>120</code>):",
        parse_mode="HTML"
    )
    return ST_SETUP_MFA

async def setup_recv_mfa(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    val     = update.message.text.strip()
    if not val.isdigit() or not (60 <= int(val) <= 300):
        await update.message.reply_text("❌ Ingresá un número entre 60 y 300.")
        return ST_SETUP_MFA
    set_user(chat_id, {"mfa_timeout": int(val)})
    u    = get_user(chat_id)
    name = u.get("cocos_user", "").split("@")[0]
    await update.message.reply_text(
        f"🎉 <b>Todo listo, {escape(name)}!</b>\n\n"
        f"👤 <code>{escape(u.get('cocos_user',''))}</code>\n"
        f"⏱️ MFA: <code>{val}s</code>\n\n"
        "Podés cambiar cualquier dato desde 🔧 Mi cuenta.", parse_mode="HTML"
    )
    await send_menu(chat_id, context)
    return ST_IDLE


async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q       = update.callback_query
    chat_id = q.message.chat_id
    if not authorized(chat_id):
        await q.answer("No autorizado."); return ST_IDLE
    await q.answer()
    key = q.data

    if   key == "menu":   await send_menu(chat_id, context);   return ST_IDLE
    elif key == "config": await send_config(chat_id, context); return ST_CONFIG_MENU
    elif key == "status": await do_status(chat_id, context);   return ST_IDLE

    elif key == "cfg_show":
        await show_my_config(chat_id, context); return ST_CONFIG_MENU
    elif key == "cfg_cocos":
        await q.message.reply_text("👤 Ingresá tu nuevo <b>email de Cocos</b>:", parse_mode="HTML")
        return ST_SETUP_USER
    elif key == "cfg_mfa":
        cur = get_user(chat_id).get("mfa_timeout", 120)
        await q.message.reply_text(
            f"⏱️ MFA actual: <code>{cur}s</code>\n\nIngresá nuevo valor (60-300):",
            parse_mode="HTML"
        )
        return ST_SETUP_MFA
    elif key == "cfg_delete":
        await q.message.reply_text(
            "⚠️ <b>¿Borrar todos tus datos?</b>", parse_mode="HTML",
            reply_markup=confirm_delete_kb()
        )
        return ST_CONFIG_MENU
    elif key == "cfg_delete_yes":
        delete_user(chat_id)
        await q.message.reply_text("🗑️ Datos eliminados. Usá /start para reconfigurar.")
        return ST_IDLE

    elif key == "full":
        await run_pipeline(chat_id, context,
            ["python", "scripts/run_analysis.py"], "Análisis completo")
        return ST_IDLE
    elif key == "quick":
        await run_pipeline(chat_id, context,
            ["python", "scripts/run_analysis.py", "--no-sentiment", "--no-optimizer"],
            "Técnico + macro (rápido)")
        return ST_IDLE
    elif key == "ticker":
        await q.message.reply_text(
            "🔍 Escribí el ticker (ej: <code>AAPL</code>):", parse_mode="HTML"
        )
        return ST_WAIT_TICKER

    elif key == "backtest":
        await context.bot.send_message(
            chat_id, "📊 <b>Backtests disponibles</b>\n\nElegí qué querés correr:",
            parse_mode="HTML", reply_markup=backtest_keyboard()
        )
        return ST_IDLE
    elif key == "bt_standard":
        await run_pipeline(chat_id, context,
            ["python", "scripts/backtest.py", "--years", "2"],
            "Backtest estándar (2y)", is_backtest=True)
        return ST_IDLE
    elif key == "bt_walkforward":
        await run_pipeline(chat_id, context,
            ["python", "scripts/backtest_walkforward.py",
             "--years", "3", "--train", "6", "--test", "3", "--step", "3"],
            "Walk-forward (3y)", is_backtest=True)
        return ST_IDLE
    elif key == "bt_universe":
        await run_pipeline(chat_id, context,
            ["python", "scripts/backtest_universe.py", "--top", "12", "--size", "4", "5"],
            "Universe backtest", is_backtest=True)
        return ST_IDLE
    elif key == "bt_horizon":
        await run_pipeline(chat_id, context,
            ["python", "scripts/backtest_horizon.py", "--years", "2", "--freqs", "2", "4"],
            "Backtest horizonte", is_backtest=True)
        return ST_IDLE

    return ST_IDLE


async def recv_ticker(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    ticker  = update.message.text.strip().upper()
    if not ticker.replace(".", "").replace("-", "").isalnum() or len(ticker) > 8:
        await update.message.reply_text("❌ Ticker inválido.")
        return ST_WAIT_TICKER
    await run_pipeline(chat_id, context,
        ["python", "scripts/run_analysis.py", "--tickers", ticker, "--no-optimizer"],
        f"Análisis de {ticker}")
    return ST_IDLE


async def show_my_config(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    u = get_user(chat_id)
    await context.bot.send_message(
        chat_id,
        "📋 <b>Mi configuración</b>\n\n"
        f"👤 Usuario: <code>{escape(u.get('cocos_user') or 'no configurado')}</code>\n"
        f"🔑 Contraseña: <code>{escape(mask(u.get('cocos_pass', '')))}</code>\n"
        f"⏱️ MFA timeout: <code>{u.get('mfa_timeout', 120)}s</code>",
        parse_mode="HTML"
    )
    await send_config(chat_id, context)


async def do_status(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    lines = ["⚙️ <b>Status del sistema</b>\n"]

    # DB
    try:
        proc = await asyncio.create_subprocess_exec(
            "python", "-c",
            (
                "import asyncio, sys; sys.path.insert(0,'/app');\n"
                "from src.core.config import get_config; from src.collector.db import PortfolioDatabase\n"
                "async def f():\n"
                "    db=PortfolioDatabase(get_config().database.url); await db.connect()\n"
                "    s=await db.get_latest_snapshot(); await db.close()\n"
                "    if s: print(f\"{s.get('total_value_ars',0):,.0f} ARS | {len(s.get('positions',[]))} pos\")\n"
                "    else: print('Sin snapshots')\n"
                "asyncio.run(f())"
            ),
            cwd="/app", stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=15)
        lines.append(f"🗄️ DB: ✅ {out.decode().strip()}")
    except Exception as e:
        lines.append(f"🗄️ DB: ❌ {e}")

    # Ollama
    try:
        import aiohttp
        url = os.environ.get("OLLAMA_URL", "http://host.docker.internal:11434")
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{url}/api/tags", timeout=aiohttp.ClientTimeout(total=5)) as r:
                data  = await r.json()
                names = [m["name"] for m in data.get("models", [])]
                lines.append(f"🤖 Ollama: ✅ {', '.join(names) or 'sin modelos'}")
    except Exception as e:
        lines.append(f"🤖 Ollama: ❌ {e}")

    u = get_user(chat_id)
    lines.append(
        f"\n👤 Tu cuenta: "
        f"{'✅ ' + escape(u.get('cocos_user', '')) if is_configured(chat_id) else '❌ no configurada'}"
    )
    await context.bot.send_message(chat_id, "\n".join(lines), parse_mode="HTML")
    await send_menu(chat_id, context)


async def fallback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not authorized(chat_id):
        await deny(update); return ST_IDLE
    await send_menu(chat_id, context, "Usá los botones 👇")
    return ST_IDLE


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    if not BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN no configurado"); sys.exit(1)
    if not ALLOWED:
        logger.error("ALLOWED_CHAT_IDS vacío"); sys.exit(1)
    logger.info(f"Usuarios autorizados: {ALLOWED}")

    app  = Application.builder().token(BOT_TOKEN).build()
    TEXT = filters.TEXT & ~filters.COMMAND

    conv = ConversationHandler(
        entry_points=[
            CommandHandler("start", cmd_start),
            CommandHandler("menu",  cmd_menu),
            CallbackQueryHandler(on_button),
        ],
        states={
            ST_IDLE:        [CallbackQueryHandler(on_button)],
            ST_WAIT_TICKER: [MessageHandler(TEXT, recv_ticker),
                             CallbackQueryHandler(on_button)],
            ST_SETUP_USER:  [MessageHandler(TEXT, setup_recv_user),
                             CallbackQueryHandler(on_button)],
            ST_SETUP_PASS:  [MessageHandler(TEXT, setup_recv_pass)],
            ST_SETUP_MFA:   [MessageHandler(TEXT, setup_recv_mfa),
                             CallbackQueryHandler(on_button)],
            ST_CONFIG_MENU: [CallbackQueryHandler(on_button)],
        },
        fallbacks=[MessageHandler(filters.ALL, fallback)],
        per_chat=True, allow_reentry=True,
    )
    app.add_handler(conv)
    logger.info("Bot iniciado — esperando mensajes...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
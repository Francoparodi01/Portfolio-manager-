"""
scripts/telegram_bot.py
Bot principal de Telegram — Cocos Copilot.

Objetivo:
  - Menú limpio sin scraping manual público.
  - /admin_scrape restringido por ADMIN_CHAT_IDS.
  - Comandos rápidos y robustos.
  - Ejecución de scripts reales vía subprocess.
  - Mensajes largos divididos en chunks.
  - Timing por comando.

Requisitos env:
  TELEGRAM_BOT_TOKEN o SCRAPER_TELEGRAM_BOT_TOKEN
  TELEGRAM_CHAT_ID   o SCRAPER_TELEGRAM_CHAT_ID
  ADMIN_CHAT_IDS=123456789,987654321

Comandos:
  /start
  /menu
  /portfolio
  /analisis
  /resumen
  /radar
  /performance
  /status
  /admin_scrape
"""

from __future__ import annotations

import asyncio
import logging
import os
import shlex
import sys
import time
from pathlib import Path
from typing import Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest, TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

# Asegura imports desde raíz del proyecto si se ejecuta como script.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from src.core.config import get_config
    from src.core.logger import get_logger
    from src.collector.db import PortfolioDatabase
except Exception:
    get_config = None
    get_logger = None
    PortfolioDatabase = None


# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

logger = get_logger(__name__) if get_logger else logging.getLogger(__name__)

if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

MAX_MESSAGE_LENGTH = 3900
COMMAND_TIMEOUT_SECONDS = 240

ADMIN_CHAT_IDS: set[int] = {
    int(x)
    for x in os.getenv("ADMIN_CHAT_IDS", "").replace(";", ",").split(",")
    if x.strip().isdigit()
}


def _get_token() -> str:
    token = (
        os.getenv("TELEGRAM_BOT_TOKEN")
        or os.getenv("SCRAPER_TELEGRAM_BOT_TOKEN")
        or os.getenv("TELEGRAM_TOKEN")
    )

    if token:
        return token

    if get_config:
        cfg = get_config()
        token = getattr(cfg.scraper, "telegram_bot_token", None)
        if token:
            return token

    raise RuntimeError(
        "No se encontró token de Telegram. Configurá TELEGRAM_BOT_TOKEN "
        "o SCRAPER_TELEGRAM_BOT_TOKEN en .env"
    )


def is_admin(chat_id: int) -> bool:
    if not ADMIN_CHAT_IDS:
        logger.warning("[BOT] ADMIN_CHAT_IDS no configurado — admin bloqueado por defecto")
        return False
    return int(chat_id) in ADMIN_CHAT_IDS


# ─────────────────────────────────────────────────────────────────────────────
# Utils
# ─────────────────────────────────────────────────────────────────────────────

def _money(x: float) -> str:
    try:
        value = float(x)
        sign = "-" if value < 0 else ""
        return f"{sign}${abs(value):,.0f}".replace(",", ".")
    except Exception:
        return "$0"


def split_message(text: str, max_len: int = MAX_MESSAGE_LENGTH) -> list[str]:
    if not text:
        return ["⚠️ Sin contenido para mostrar."]

    if len(text) <= max_len:
        return [text]

    chunks: list[str] = []
    current = ""

    for line in text.splitlines():
        candidate = current + line + "\n"
        if len(candidate) > max_len:
            if current.strip():
                chunks.append(current.rstrip())
            current = line + "\n"
        else:
            current = candidate

    if current.strip():
        chunks.append(current.rstrip())

    return chunks


async def safe_send(
    update_or_context,
    text: str,
    chat_id: Optional[int] = None,
    parse_mode: Optional[str] = ParseMode.HTML,
) -> None:
    """
    Envía texto en chunks.
    Si HTML falla, reintenta como texto plano.
    """
    if hasattr(update_or_context, "bot"):
        bot = update_or_context.bot
    else:
        bot = update_or_context.get_bot()

    if chat_id is None:
        raise ValueError("chat_id requerido")

    for chunk in split_message(text):
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=chunk,
                parse_mode=parse_mode,
                disable_web_page_preview=True,
            )
        except BadRequest as e:
            logger.warning("[BOT] HTML falló, reintentando sin parse_mode: %s", e)
            await bot.send_message(
                chat_id=chat_id,
                text=chunk,
                parse_mode=None,
                disable_web_page_preview=True,
            )


async def edit_or_send(query, text: str, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Para callbacks: edita si puede; si el mensaje es largo, envía aparte.
    """
    chat_id = query.message.chat_id

    if len(text) < 3500:
        try:
            await query.edit_message_text(
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            return
        except BadRequest:
            pass

    await safe_send(context, text, chat_id=chat_id)


def timing(label: str):
    def decorator(fn):
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
            t0 = time.time()
            logger.info("[TIMING] ▶ %s iniciado", label)
            try:
                result = await fn(update, context)
                logger.info("[TIMING] ✓ %s completado en %.2fs", label, time.time() - t0)
                return result
            except Exception as e:
                logger.exception("[TIMING] ✗ %s falló en %.2fs", label, time.time() - t0)
                chat_id = update.effective_chat.id if update.effective_chat else None
                if chat_id:
                    await safe_send(
                        context,
                        f"❌ Error en <b>{label}</b>:\n<code>{str(e)}</code>",
                        chat_id=chat_id,
                    )
        return wrapper
    return decorator


async def run_cmd(
    args: list[str],
    timeout: int = COMMAND_TIMEOUT_SECONDS,
) -> tuple[int, str, str, float]:
    """
    Ejecuta comando en subprocess y devuelve:
      returncode, stdout, stderr, elapsed
    """
    t0 = time.time()
    logger.info("[CMD] Ejecutando: %s", " ".join(shlex.quote(a) for a in args))

    proc = await asyncio.create_subprocess_exec(
        *args,
        cwd=str(PROJECT_ROOT),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    try:
        stdout_b, stderr_b = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        elapsed = time.time() - t0
        return 124, "", f"Timeout luego de {elapsed:.1f}s", elapsed

    elapsed = time.time() - t0
    stdout = stdout_b.decode("utf-8", errors="replace").strip()
    stderr = stderr_b.decode("utf-8", errors="replace").strip()

    logger.info("[CMD] Finalizó rc=%s en %.2fs", proc.returncode, elapsed)
    if stderr:
        logger.warning("[CMD][stderr]\n%s", stderr[-2000:])

    return proc.returncode or 0, stdout, stderr, elapsed


async def run_python_script(
    script: str,
    *extra_args: str,
    timeout: int = COMMAND_TIMEOUT_SECONDS,
) -> str:
    args = [sys.executable, script, *extra_args]
    rc, out, err, elapsed = await run_cmd(args, timeout=timeout)

    if rc != 0:
        return (
            f"❌ <b>Error ejecutando {script}</b>\n"
            f"⏱ {elapsed:.1f}s\n\n"
            f"<b>STDERR</b>\n<code>{err[-3000:] or '—'}</code>\n\n"
            f"<b>STDOUT</b>\n<code>{out[-3000:] or '—'}</code>"
        )

    if not out:
        return f"⚠️ <b>{script}</b> terminó sin output. Tiempo: {elapsed:.1f}s"

    return out


# ─────────────────────────────────────────────────────────────────────────────
# Menú
# ─────────────────────────────────────────────────────────────────────────────

def main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("💼 Portfolio", callback_data="portfolio"),
            InlineKeyboardButton("🧠 Análisis", callback_data="analysis"),
        ],
        [
            InlineKeyboardButton("📅 Resumen semanal", callback_data="weekly_summary"),
            InlineKeyboardButton("📊 Performance", callback_data="performance"),
        ],
        [
            InlineKeyboardButton("🔭 Radar", callback_data="radar"),
            InlineKeyboardButton("🩺 Status", callback_data="status"),
        ],
    ])


def menu_text() -> str:
    return (
        "🤖 <b>Cocos Copilot</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "Sistema cuantitativo personal para seguimiento de cartera.\n\n"
        "Elegí una opción:\n\n"
        "💼 <b>Portfolio</b>: último snapshot.\n"
        "🧠 <b>Análisis</b>: análisis completo/semanal.\n"
        "📅 <b>Resumen semanal</b>: performance semanal corregida.\n"
        "📊 <b>Performance</b>: EV, win rate y outcomes.\n"
        "🔭 <b>Radar</b>: oportunidades externas.\n"
        "🩺 <b>Status</b>: DB y bot.\n\n"
        "⚙️ Scraping manual removido del menú.\n"
        "Usar <code>/admin_scrape</code> solo si sos admin."
    )


# ─────────────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────────────

async def get_latest_portfolio_text() -> str:
    if not get_config or not PortfolioDatabase:
        return "❌ No pude importar config/db del proyecto."

    cfg = get_config()
    db = PortfolioDatabase(cfg.database.url)

    await db.connect()
    try:
        snap = await db.get_latest_snapshot()
    finally:
        await db.close()

    if not snap:
        return "⚠️ Sin snapshots en DB. Ejecutá un scrape primero."

    total = float(snap.get("total_value_ars", 0) or 0)
    cash = float(snap.get("cash_ars", 0) or 0)
    ts = snap.get("scraped_at") or snap.get("timestamp") or "—"
    positions = snap.get("positions") or []

    lines = [
        "💼 <b>PORTFOLIO ACTUAL</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"🕐 Snapshot: <code>{ts}</code>",
        f"💰 Total: <b>{_money(total)} ARS</b>",
        f"💵 Cash:  <b>{_money(cash)} ARS</b>",
        "",
        "<b>POSICIONES</b>",
    ]

    if not positions:
        lines.append("Sin posiciones en el último snapshot.")
        return "\n".join(lines)

    positions_sorted = sorted(
        positions,
        key=lambda p: float(p.get("market_value", 0) or 0),
        reverse=True,
    )

    for p in positions_sorted:
        ticker = str(p.get("ticker", "?")).upper()
        qty = float(p.get("quantity", 0) or 0)
        price = float(p.get("current_price", 0) or 0)
        mv = float(p.get("market_value", 0) or 0)
        weight = float(p.get("weight_in_portfolio", 0) or 0)

        if weight > 1:
            weight = weight / 100

        lines.append(
            f"• <b>{ticker}</b>: {qty:g} × {_money(price)} = "
            f"<b>{_money(mv)}</b> | {weight:.1%}"
        )

    return "\n".join(lines)


async def get_status_text() -> str:
    lines = [
        "🩺 <b>STATUS — Cocos Copilot</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
    ]

    if not get_config or not PortfolioDatabase:
        lines.append("❌ Imports de config/db no disponibles.")
        return "\n".join(lines)

    cfg = get_config()
    db = PortfolioDatabase(cfg.database.url)

    try:
        t0 = time.time()
        await db.connect()
        snap = await db.get_latest_snapshot()
        elapsed = time.time() - t0

        if snap:
            lines.append(f"✅ DB conectada en {elapsed:.2f}s")
            lines.append(f"🕐 Último snapshot: <code>{snap.get('scraped_at') or snap.get('timestamp') or '—'}</code>")
            lines.append(f"💰 Portfolio: <b>{_money(float(snap.get('total_value_ars', 0) or 0))} ARS</b>")
        else:
            lines.append("⚠️ DB conectada, pero sin snapshots.")

    except Exception as e:
        lines.append(f"❌ DB error: <code>{e}</code>")
    finally:
        try:
            await db.close()
        except Exception:
            pass

    lines.append("")
    lines.append("✅ Bot activo")
    lines.append("✅ Menú sin scraping manual")
    lines.append("🔐 /admin_scrape protegido por ADMIN_CHAT_IDS")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Handlers
# ─────────────────────────────────────────────────────────────────────────────

@timing("start")
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        menu_text(),
        parse_mode=ParseMode.HTML,
        reply_markup=main_keyboard(),
        disable_web_page_preview=True,
    )


@timing("menu")
async def menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await start_handler(update, context)


@timing("portfolio")
async def portfolio_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    await safe_send(context, "🔄 Leyendo último portfolio...", chat_id=chat_id)
    text = await get_latest_portfolio_text()
    await safe_send(context, text, chat_id=chat_id)


@timing("weekly_summary")
async def weekly_summary_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    await safe_send(context, "📅 Generando resumen semanal...", chat_id=chat_id)

    report = await run_python_script(
        "scripts/weekly_summary.py",
        "--no-telegram",
        timeout=90,
    )

    await safe_send(context, report, chat_id=chat_id)


@timing("performance")
async def performance_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    await safe_send(context, "📊 Actualizando outcomes y calculando performance...", chat_id=chat_id)

    report = await run_python_script(
        "scripts/run_performance.py",
        "--days",
        "90",
        "--no-telegram",
        timeout=180,
    )

    await safe_send(context, report, chat_id=chat_id)


@timing("analysis")
async def analysis_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id

    await safe_send(
        context,
        "🧠 Generando análisis cuantitativo...\n"
        "Puede tardar. Te aviso cuando termine.",
        chat_id=chat_id,
    )

    # Primero intenta análisis rápido/sin LLM. Si tu script no acepta flags,
    # cae automáticamente al script sin flags.
    rc, out, err, elapsed = await run_cmd(
        [sys.executable, "scripts/run_analysis.py", "--no-llm", "--no-sentiment"],
        timeout=COMMAND_TIMEOUT_SECONDS,
    )

    if rc != 0 and ("unrecognized arguments" in err.lower() or "usage:" in err.lower()):
        rc, out, err, elapsed = await run_cmd(
            [sys.executable, "scripts/run_analysis.py"],
            timeout=COMMAND_TIMEOUT_SECONDS,
        )

    if rc != 0:
        report = (
            f"❌ <b>Error en análisis</b>\n"
            f"⏱ {elapsed:.1f}s\n\n"
            f"<b>STDERR</b>\n<code>{err[-3000:] or '—'}</code>\n\n"
            f"<b>STDOUT</b>\n<code>{out[-3000:] or '—'}</code>"
        )
    else:
        report = out or f"⚠️ Análisis terminado sin output. Tiempo: {elapsed:.1f}s"

    await safe_send(context, report, chat_id=chat_id)


@timing("radar")
async def radar_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    await safe_send(context, "🔭 Generando radar de oportunidades...", chat_id=chat_id)

    candidates = [
        ["scripts/run_opportunities.py", "--no-telegram"],
        ["scripts/run_opportunity_screener.py", "--no-telegram"],
        ["scripts/run_radar.py", "--no-telegram"],
    ]

    last_error = ""

    for cmd in candidates:
        script = cmd[0]
        if not (PROJECT_ROOT / script).exists():
            continue

        rc, out, err, elapsed = await run_cmd(
            [sys.executable, *cmd],
            timeout=COMMAND_TIMEOUT_SECONDS,
        )

        if rc == 0 and out:
            await safe_send(context, out, chat_id=chat_id)
            return

        last_error = (
            f"Script: {script}\n"
            f"rc={rc}\n"
            f"stderr={err[-1500:]}\n"
            f"stdout={out[-1500:]}"
        )

    await safe_send(
        context,
        "⚠️ No encontré/corrí correctamente un script de radar.\n\n"
        f"<code>{last_error or 'No existe run_opportunities/run_radar.'}</code>",
        chat_id=chat_id,
    )


@timing("status")
async def status_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    text = await get_status_text()
    await safe_send(context, text, chat_id=chat_id)


@timing("admin_scrape")
async def admin_scrape_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id

    if not is_admin(chat_id):
        await safe_send(context, "🚫 Comando restringido a administradores.", chat_id=chat_id)
        logger.warning("[BOT] /admin_scrape bloqueado para chat_id=%s", chat_id)
        return

    await safe_send(
        context,
        "⚙️ Iniciando scraping manual en modo admin...",
        chat_id=chat_id,
    )

    candidates = [
        ["scripts/run_once.py", "--full"],
        ["scripts/run_once.py"],
        ["scripts/scrape_once.py"],
    ]

    last_error = ""

    for cmd in candidates:
        script = cmd[0]
        if not (PROJECT_ROOT / script).exists():
            continue

        rc, out, err, elapsed = await run_cmd(
            [sys.executable, *cmd],
            timeout=180,
        )

        if rc == 0:
            msg = (
                f"✅ <b>Scraping completado</b>\n"
                f"⏱ {elapsed:.1f}s\n\n"
                f"<code>{(out or 'Sin output')[-2500:]}</code>"
            )
            await safe_send(context, msg, chat_id=chat_id)
            return

        last_error = (
            f"Script: {script}\n"
            f"rc={rc}\n"
            f"stderr={err[-1500:]}\n"
            f"stdout={out[-1500:]}"
        )

    await safe_send(
        context,
        "❌ No pude ejecutar scraping manual.\n\n"
        f"<code>{last_error or 'No encontré scripts/run_once.py ni scripts/scrape_once.py'}</code>",
        chat_id=chat_id,
    )


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    action = query.data
    chat_id = query.message.chat_id

    # Mensaje corto inmediato para que Telegram no parezca colgado.
    try:
        await query.edit_message_text(
            text=f"🔄 Ejecutando <b>{action}</b>...",
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass

    fake_update = update

    if action == "portfolio":
        text = await get_latest_portfolio_text()
        await safe_send(context, text, chat_id=chat_id)
    elif action == "weekly_summary":
        report = await run_python_script("scripts/weekly_summary.py", "--no-telegram", timeout=90)
        await safe_send(context, report, chat_id=chat_id)
    elif action == "performance":
        report = await run_python_script(
            "scripts/run_performance.py",
            "--days",
            "90",
            "--no-telegram",
            timeout=180,
        )
        await safe_send(context, report, chat_id=chat_id)
    elif action == "analysis":
        await analysis_handler(fake_update, context)
    elif action == "radar":
        await radar_handler(fake_update, context)
    elif action == "status":
        text = await get_status_text()
        await safe_send(context, text, chat_id=chat_id)
    else:
        await safe_send(context, "⚠️ Acción desconocida.", chat_id=chat_id)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("[BOT] Error no capturado", exc_info=context.error)

    try:
        if isinstance(update, Update) and update.effective_chat:
            await safe_send(
                context,
                f"❌ Error interno del bot:\n<code>{context.error}</code>",
                chat_id=update.effective_chat.id,
            )
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def build_app() -> Application:
    token = _get_token()

    app = Application.builder().token(token).build()

    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(CommandHandler("menu", menu_handler))
    app.add_handler(CommandHandler("portfolio", portfolio_handler))
    app.add_handler(CommandHandler("analisis", analysis_handler))
    app.add_handler(CommandHandler("analysis", analysis_handler))
    app.add_handler(CommandHandler("resumen", weekly_summary_handler))
    app.add_handler(CommandHandler("weekly_summary", weekly_summary_handler))
    app.add_handler(CommandHandler("performance", performance_handler))
    app.add_handler(CommandHandler("radar", radar_handler))
    app.add_handler(CommandHandler("status", status_handler))
    app.add_handler(CommandHandler("admin_scrape", admin_scrape_handler))

    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_error_handler(error_handler)

    return app


def main() -> None:
    logger.info("[BOT] Iniciando Cocos Copilot Telegram Bot")
    logger.info("[BOT] Admins configurados: %s", sorted(ADMIN_CHAT_IDS) if ADMIN_CHAT_IDS else "NINGUNO")

    app = build_app()
    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()
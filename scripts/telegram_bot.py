from __future__ import annotations

import asyncio
import logging
import os
import re
import sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

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

ART_TZ = ZoneInfo("America/Argentina/Buenos_Aires")

TELEGRAM_TOKEN  = os.environ["TELEGRAM_BOT_TOKEN"]
ALLOWED_CHAT_ID = int(os.environ["TELEGRAM_CHAT_ID"])

_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
_BASE_DIR    = os.path.dirname(_SCRIPTS_DIR)

_HTML_TAG_RE = re.compile(r"<[^>]+>")


# ── Utilidades ────────────────────────────────────────────────────────────────

def _strip_html(text: str) -> str:
    return _HTML_TAG_RE.sub("", text)


def _fmt_money(x) -> str:
    try:
        return f"${float(x):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "$0,00"


def _fmt_money0(x) -> str:
    try:
        return f"${float(x):,.0f}".replace(",", ".")
    except Exception:
        return "$0"


def _fmt_pct_from_ratio(x) -> str:
    try:
        return f"{float(x) * 100:+.2f}%"
    except Exception:
        return "0,00%"


def _fmt_pct_raw(x) -> str:
    try:
        return f"{float(x):+.2f}%"
    except Exception:
        return "0,00%"


def _main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 Portfolio",       callback_data="portfolio"),
            InlineKeyboardButton("🧠 Análisis",        callback_data="analisis"),
        ],
        [
            InlineKeyboardButton("⚡ Intradía",        callback_data="intradia"),
            InlineKeyboardButton("🛡️ Riesgo",          callback_data="riesgo"),
        ],
        [
            InlineKeyboardButton("🚨 Alertas",         callback_data="alertas"),
            InlineKeyboardButton("🖥️ Estado",          callback_data="status"),
        ],
        [
            InlineKeyboardButton("🔄 Scrape manual",   callback_data="scrape"),
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


# ── DB helpers ────────────────────────────────────────────────────────────────

async def _get_db():
    from src.core.config import get_config
    from src.collector.db import PortfolioDatabase

    db = PortfolioDatabase(get_config().database.url)
    await db.connect()
    return db


async def _load_latest_snapshot():
    db = await _get_db()
    try:
        return await db.get_latest_snapshot()
    finally:
        await db.close()


async def _load_intraday_bundle():
    """
    Devuelve:
      - snapshot actual
      - latest_prices_map por ticker
      - latest buy decision por ticker
      - últimas alertas
    """
    db = await _get_db()
    try:
        snapshot = await db.get_latest_snapshot()
        pool = await db.get_pool()
        async with pool.acquire() as conn:
            latest_prices = await conn.fetch(
                """
                SELECT DISTINCT ON (ticker)
                    ticker,
                    last_price,
                    ts
                FROM market_prices
                ORDER BY ticker, ts DESC
                """
            )

            latest_buys = await conn.fetch(
                """
                SELECT DISTINCT ON (ticker)
                    ticker,
                    decided_at,
                    price_at_decision,
                    stop_loss_pct,
                    stop_loss_price,
                    target_pct,
                    target_price,
                    horizon_days,
                    size_pct,
                    regime,
                    final_score,
                    confidence
                FROM decision_log
                WHERE decision = 'BUY'
                  AND price_at_decision IS NOT NULL
                ORDER BY ticker, decided_at DESC
                """
            )

            recent_alerts = await conn.fetch(
                """
                SELECT
                    ticker,
                    alert_type,
                    severity,
                    created_at,
                    price_at_alert
                FROM stop_alerts
                ORDER BY created_at DESC
                LIMIT 10
                """
            )

        prices_map = {str(r["ticker"]).upper(): dict(r) for r in latest_prices}
        buys_map = {str(r["ticker"]).upper(): dict(r) for r in latest_buys}

        return snapshot, prices_map, buys_map, [dict(r) for r in recent_alerts]
    finally:
        await db.close()


def _compute_risk_state(entry_price: float, current_price: float, stop_price: float) -> str:
    pnl = (current_price / entry_price) - 1.0
    dist_to_stop = (current_price / stop_price) - 1.0 if stop_price else 999.0

    if stop_price and current_price <= stop_price:
        return "STOP_TRIGGERED"
    if stop_price and 0 <= dist_to_stop <= 0.02:
        return "STOP_NEAR"
    if pnl <= -0.06:
        return "CRITICAL"
    if pnl <= -0.04:
        return "WARNING"
    return "OK"


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
        snapshot = await _load_latest_snapshot()
        if snapshot:
            ts = str(snapshot.get("scraped_at", "—"))[:19].replace("T", " ")
            n  = len(snapshot.get("positions", []))
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
        snap = await _load_latest_snapshot()

        if not snap:
            await msg.edit_text("⚠️ Sin snapshots. Corré un scrape primero.")
            await _reply_menu(message)
            return

        ts        = str(snap.get("scraped_at", "—"))[:19].replace("T", " ")
        total     = float(snap.get("total_value_ars", 0) or 0)
        cash      = float(snap.get("cash_ars", 0) or 0)
        positions = snap.get("positions", [])

        lines = [
            f"📊 <b>Portfolio</b> — {ts} UTC\n",
            f"💼 Total: <b>{_fmt_money0(total)} ARS</b>",
            f"💵 Cash:  <b>{_fmt_money0(cash)} ARS</b>\n",
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
                f"  {icon} <b>{ticker}</b>  {pct:.1f}%  {_fmt_money0(mv)}  ({pnl_p:+.1f}%)\n"
                f"     x{qty:.0f} @ {_fmt_money(price)}"
            )

        await msg.edit_text("\n".join(lines), parse_mode="HTML")
    except Exception as e:
        logger.error("_action_portfolio: %s", e, exc_info=True)
        await msg.edit_text(f"❌ Error: {e}")
    finally:
        await _reply_menu(message)


async def _action_intradia(message: Message) -> None:
    msg = await message.reply_text("⚡ Cargando estado intradía...")
    try:
        snapshot, prices_map, buys_map, _ = await _load_intraday_bundle()

        if not snapshot:
            await msg.edit_text("⚠️ Sin snapshot disponible.")
            await _reply_menu(message)
            return

        positions = snapshot.get("positions", []) or []
        if not positions:
            await msg.edit_text("⚠️ No hay posiciones activas en el snapshot.")
            await _reply_menu(message)
            return

        ts = str(snapshot.get("scraped_at", "—"))[:19].replace("T", " ")
        lines = [
            f"⚡ <b>Estado intradía</b>",
            f"🕒 Snapshot base: <b>{ts} UTC</b>",
            "",
        ]

        for p in sorted(positions, key=lambda x: float(x.get("market_value", 0) or 0), reverse=True):
            ticker = str(p.get("ticker", "")).upper()
            if not ticker:
                continue

            px_row = prices_map.get(ticker)
            buy = buys_map.get(ticker)

            if not px_row:
                lines.append(f"• <b>{ticker}</b>: sin precio reciente")
                continue

            last_price = float(px_row["last_price"])
            ts_price = px_row["ts"]

            if buy and buy.get("price_at_decision"):
                entry = float(buy["price_at_decision"])
                pnl = (last_price / entry) - 1.0
                lines.append(
                    f"• <b>{ticker}</b> | last {_fmt_money(last_price)} | "
                    f"entry {_fmt_money(entry)} | pnl <b>{_fmt_pct_from_ratio(pnl)}</b>"
                )
            else:
                lines.append(
                    f"• <b>{ticker}</b> | last {_fmt_money(last_price)} | "
                    f"ts {str(ts_price)[:19].replace('T', ' ')}"
                )

        await msg.edit_text("\n".join(lines), parse_mode="HTML")
    except Exception as e:
        logger.error("_action_intradia: %s", e, exc_info=True)
        await msg.edit_text(f"❌ Error intradía: {e}")
    finally:
        await _reply_menu(message)


async def _action_riesgo(message: Message) -> None:
    msg = await message.reply_text("🛡️ Calculando riesgo actual...")
    try:
        snapshot, prices_map, buys_map, _ = await _load_intraday_bundle()

        if not snapshot:
            await msg.edit_text("⚠️ Sin snapshot disponible.")
            await _reply_menu(message)
            return

        positions = snapshot.get("positions", []) or []
        if not positions:
            await msg.edit_text("⚠️ No hay posiciones activas.")
            await _reply_menu(message)
            return

        lines = ["🛡️ <b>Riesgo actual</b>\n"]

        for p in sorted(positions, key=lambda x: float(x.get("market_value", 0) or 0), reverse=True):
            ticker = str(p.get("ticker", "")).upper()
            if not ticker:
                continue

            buy = buys_map.get(ticker)
            px_row = prices_map.get(ticker)

            if not buy or not px_row or not buy.get("price_at_decision"):
                lines.append(f"• <b>{ticker}</b>: sin BUY rastreable o sin precio reciente")
                continue

            entry = float(buy["price_at_decision"])
            last = float(px_row["last_price"])

            if buy.get("stop_loss_price") is not None:
                stop = float(buy["stop_loss_price"])
            else:
                stop_pct = float(buy.get("stop_loss_pct") or 0.0)
                stop = entry * (1 + stop_pct)

            if buy.get("target_price") is not None:
                target = float(buy["target_price"])
            else:
                target_pct = float(buy.get("target_pct") or 0.0)
                target = entry * (1 + target_pct)

            pnl = (last / entry) - 1.0
            dist_stop = (last / stop) - 1.0 if stop else 0.0
            state = _compute_risk_state(entry, last, stop)

            icon = {
                "OK": "🟢",
                "WARNING": "🟡",
                "CRITICAL": "🔴",
                "STOP_NEAR": "⚠️",
                "STOP_TRIGGERED": "🚨",
            }.get(state, "•")

            lines.append(
                f"{icon} <b>{ticker}</b> — <b>{state}</b>\n"
                f"   entry {_fmt_money(entry)} | last {_fmt_money(last)} | pnl {_fmt_pct_from_ratio(pnl)}\n"
                f"   stop {_fmt_money(stop)} | target {_fmt_money(target)} | dist stop {_fmt_pct_from_ratio(dist_stop)}"
            )

        await msg.edit_text("\n".join(lines), parse_mode="HTML")
    except Exception as e:
        logger.error("_action_riesgo: %s", e, exc_info=True)
        await msg.edit_text(f"❌ Error riesgo: {e}")
    finally:
        await _reply_menu(message)


async def _action_alertas(message: Message) -> None:
    msg = await message.reply_text("🚨 Cargando alertas...")
    try:
        _, _, _, alerts = await _load_intraday_bundle()

        lines = ["🚨 <b>Últimas alertas</b>\n"]

        if not alerts:
            lines.append("No hay alertas registradas.")
        else:
            for a in alerts:
                ticker = a.get("ticker") or "-"
                alert_type = a.get("alert_type") or "-"
                severity = a.get("severity") or "-"
                created_at = a.get("created_at")
                price = a.get("price_at_alert")

                lines.append(
                    f"• <b>{ticker}</b> | {alert_type} | {severity}\n"
                    f"   {created_at} | px {_fmt_money(price)}"
                )

        await msg.edit_text("\n".join(lines), parse_mode="HTML")
    except Exception as e:
        logger.error("_action_alertas: %s", e, exc_info=True)
        await msg.edit_text(f"❌ Error alertas: {e}")
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
            err = stderr.decode("utf-8", errors="replace")[-700:]
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
        message,
        cmd,
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
    "intradia":        _action_intradia,
    "riesgo":          _action_riesgo,
    "alertas":         _action_alertas,
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
    "/intradia          — precios vivos y comparación con entry\n"
    "/riesgo            — estado actual vs stop/target\n"
    "/alertas           — últimas alertas intradía\n"
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
    cmd = update.message.text.lstrip("/").split("@")[0]
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
    app.add_handler(CommandHandler(list(_ACTIONS), handle_command))

    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    logger.info("Bot iniciado — esperando mensajes...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
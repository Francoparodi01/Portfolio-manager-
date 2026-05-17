"""
scripts/telegram_bot.py
Bot Telegram — Cocos Copilot

Menú principal:
  - Portfolio
  - Análisis semanal
  - Resumen semanal
  - Radar
  - Performance
  - Status

Scraping manual:
  - Removido del menú principal
  - Disponible solo con /admin_scrape para ADMIN_CHAT_IDS

Requiere:
  TELEGRAM_BOT_TOKEN o SCRAPER_TELEGRAM_BOT_TOKEN
  ADMIN_CHAT_IDS=123456789
"""

from __future__ import annotations

import asyncio
import logging
import os
import shlex
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

# ─────────────────────────────────────────────────────────────────────────────
# Path raíz del proyecto
# ─────────────────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from src.core.config import get_config
    from src.core.logger import get_logger
    from src.core.portfolio_cache import get_cached_live_portfolio
    from src.collector.db import PortfolioDatabase
except Exception:
    get_config = None
    get_logger = None
    get_cached_live_portfolio = None
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
# Configuración
# ─────────────────────────────────────────────────────────────────────────────

TZ = ZoneInfo("America/Argentina/Buenos_Aires")

MAX_MESSAGE_LENGTH = 3900
COMMAND_TIMEOUT_SECONDS = 300

REGRESSION_MODES = {"optimizer", "execution", "blocked", "signal", "all"}
DEFAULT_REGRESSION_MODE = "optimizer"

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
        logger.warning("[BOT] ADMIN_CHAT_IDS no configurado — admin bloqueado")
        return False
    return int(chat_id) in ADMIN_CHAT_IDS


# ─────────────────────────────────────────────────────────────────────────────
# Helpers de formato
# ─────────────────────────────────────────────────────────────────────────────

def _to_float(value, default: float = 0.0) -> float:
    try:
        return float(value or default)
    except Exception:
        return default


def _money(x: float) -> str:
    """$1.229.700 — formato ARS con punto de miles."""
    try:
        v = float(x)
        sign = "-" if v < 0 else ""
        return f"{sign}${abs(v):,.0f}".replace(",", ".")
    except Exception:
        return "$0"


def _money_signed(x: float) -> str:
    """Con signo explícito: +$5.700 / -$22.080."""
    try:
        v = float(x)
        sign = "+" if v >= 0 else "-"
        return f"{sign}${abs(v):,.0f}".replace(",", ".")
    except Exception:
        return "+$0"


def _pct(x: float) -> str:
    try:
        return f"{float(x) * 100:.1f}%"
    except Exception:
        return "0.0%"


def _pct_signed(x: float) -> str:
    try:
        return f"{float(x) * 100:+.1f}%"
    except Exception:
        return "+0.0%"


def _parse_dt(value) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc).astimezone(TZ)
        return value.astimezone(TZ)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(TZ)
    except Exception:
        return None


def _fmt_dt_art(value) -> str:
    parsed = _parse_dt(value)
    return parsed.strftime("%d/%m/%Y %H:%M ART") if parsed else "—"


def _age_label(value) -> tuple[str, Optional[float]]:
    parsed = _parse_dt(value)
    if not parsed:
        return "desconocido", None
    delta = datetime.now(tz=TZ) - parsed
    minutes = delta.total_seconds() / 60
    if minutes < 1:
        return "recién actualizado", minutes
    if minutes < 60:
        return f"hace {minutes:.0f} min", minutes
    hours = minutes / 60
    if hours < 24:
        return f"hace {hours:.1f} h", minutes
    return f"hace {hours / 24:.1f} días", minutes


def _is_business_day_now() -> bool:
    return datetime.now(tz=TZ).weekday() < 5


def _is_market_hours_now() -> bool:
    now = datetime.now(tz=TZ)
    mins = now.hour * 60 + now.minute
    return 10 * 60 + 30 <= mins < 17 * 60


def _freshness_badge(minutes: Optional[float], *, business_day: bool) -> tuple[str, str]:
    if minutes is None:
        return "⚪", ""
    if not business_day and minutes <= 3 * 24 * 60:
        return "📅", " <i>(esperable sin rueda)</i>"
    if minutes <= 90:
        return "🟢", ""
    if minutes <= 360:
        return "🟡", ""
    return "🔴", ""


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


async def send_text(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    text: str,
    parse_mode: Optional[str] = ParseMode.HTML,
) -> None:
    for chunk in split_message(text):
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=chunk,
                parse_mode=parse_mode,
                disable_web_page_preview=True,
            )
        except BadRequest as e:
            logger.warning("[BOT] Parse HTML falló, reintentando texto plano: %s", e)
            await context.bot.send_message(
                chat_id=chat_id,
                text=chunk,
                parse_mode=None,
                disable_web_page_preview=True,
            )


async def answer_loading(update: Update, text: str) -> None:
    if update.callback_query:
        try:
            await update.callback_query.answer()
        except Exception:
            pass
        try:
            await update.callback_query.edit_message_text(
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            return
        except Exception:
            pass
    if update.effective_chat:
        await update.effective_chat.send_message(
            text=text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Subprocess helpers
# ─────────────────────────────────────────────────────────────────────────────

async def run_cmd(
    args: list[str],
    timeout: int = COMMAND_TIMEOUT_SECONDS,
) -> tuple[int, str, str, float]:
    t0 = time.time()
    logger.info("[CMD] %s", " ".join(shlex.quote(a) for a in args))
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
    MAX_CAPTURE = 20_000
    stdout = stdout_b.decode("utf-8", errors="replace").strip()[-MAX_CAPTURE:]
    stderr = stderr_b.decode("utf-8", errors="replace").strip()[-MAX_CAPTURE:]
    logger.info("[CMD] rc=%s en %.2fs", proc.returncode, elapsed)
    if stderr:
        logger.warning("[CMD][stderr]\n%s", stderr[-2000:])
    return proc.returncode or 0, stdout, stderr, elapsed


async def run_python_script(
    script: str,
    *extra_args: str,
    timeout: int = COMMAND_TIMEOUT_SECONDS,
) -> str:
    script_path = PROJECT_ROOT / script
    if not script_path.exists():
        return f"❌ No existe el script: <code>{script}</code>"
    rc, out, err, elapsed = await run_cmd(
        [sys.executable, script, *extra_args],
        timeout=timeout,
    )
    if rc != 0:
        return (
            f"❌ <b>Error ejecutando {script}</b>  ⏱ {elapsed:.1f}s\n\n"
            f"<b>STDERR</b>\n<code>{err[-3000:] or '—'}</code>\n\n"
            f"<b>STDOUT</b>\n<code>{out[-3000:] or '—'}</code>"
        )
    if not out:
        return f"⚠️ <b>{script}</b> terminó sin output.  ⏱ {elapsed:.1f}s"
    return out


async def run_first_existing_script(
    candidates: list[list[str]],
    timeout: int = COMMAND_TIMEOUT_SECONDS,
) -> str:
    last_error = ""
    for candidate in candidates:
        script, *args = candidate
        if not (PROJECT_ROOT / script).exists():
            continue
        rc, out, err, elapsed = await run_cmd(
            [sys.executable, script, *args],
            timeout=timeout,
        )
        if rc != 0 and ("unrecognized arguments" in err.lower() or "usage:" in err.lower()):
            last_error = err[-1500:]
            continue
        if rc == 0:
            return out or f"⚠️ {script} terminó sin output.  ⏱ {elapsed:.1f}s"
        last_error = f"Script: {script}\nRC: {rc}\nSTDERR:\n{err[-2500:]}\n\nSTDOUT:\n{out[-2500:]}"

    return (
        "❌ No pude ejecutar ningún script candidato.\n\n"
        f"<code>{last_error or 'No se encontraron scripts compatibles.'}</code>"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Menú principal
# ─────────────────────────────────────────────────────────────────────────────

def main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("💼 Portfolio",        callback_data="portfolio"),
            InlineKeyboardButton("🧠 Plan de cartera", callback_data="weekly_analysis"),
        ],
        [
            InlineKeyboardButton("📅 Resumen semanal",  callback_data="weekly_summary"),
            InlineKeyboardButton("📊 Performance",      callback_data="performance"),
        ],
        [
            InlineKeyboardButton("📈 Regression",       callback_data="regression_audit"),
            InlineKeyboardButton("🔭 Radar",            callback_data="radar"),
        ],
        [
            InlineKeyboardButton("🩺 Status",           callback_data="status"),
        ],
    ])


def menu_text() -> str:
    return (
        "🤖 <b>Cocos Copilot</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "💼 <b>Portfolio</b> — último snapshot de la cartera\n"
        "🧠 <b>Plan de cartera</b> — rotación y acciones sugeridas\n"
        "📅 <b>Resumen semanal</b> — performance de la semana\n"
        "📊 <b>Performance</b> — métricas canónicas y dataset operativo\n"
        "📈 <b>Regression</b> — auditoría estadística\n"
        "🔭 <b>Radar</b> — oportunidades operables del universo\n"
        "🩺 <b>Status</b> — estado del sistema y DB\n\n"
        "<b>Regresión:</b>\n"
        "• <code>/regression</code> → optimizer\n"
        "• <code>/regression execution</code>\n"
        "• <code>/regression blocked</code>\n"
        "• <code>/regression signal</code>\n"
        "• <code>/regression all</code>\n\n"
        "<i>Scraping manual: <code>/admin_scrape</code> (solo admin)</i>"
    )


async def send_menu(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    await context.bot.send_message(
        chat_id=chat_id,
        text=menu_text(),
        parse_mode=ParseMode.HTML,
        reply_markup=main_keyboard(),
        disable_web_page_preview=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Acciones de cada sección
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# Portfolio — renderizado de tabla
# ─────────────────────────────────────────────────────────────────────────────

def _render_portfolio_table(positions: list[dict], total_invested: float) -> str:
    """
    Genera tabla monoespaciada para el bloque <code>.

    Para MVP:
    - No muestra flags raros tipo ● / ·.
    - No muestra P&L.
    - El peso se calcula sobre total_invested, no sobre cash ni total cuenta.
    """
    COL_TICKER = 10
    COL_CANT   = 5
    COL_PRECIO = 9
    COL_VALOR  = 10
    COL_PESO   = 6
    SEP        = "  "

    def _ars(v: float) -> str:
        return f"${abs(v):,.0f}".replace(",", ".")

    header = (
        f"{'TICKER':<{COL_TICKER}}{SEP}"
        f"{'CANT':>{COL_CANT}}{SEP}"
        f"{'PRECIO':>{COL_PRECIO}}{SEP}"
        f"{'VALOR':>{COL_VALOR}}{SEP}"
        f"{'PESO':>{COL_PESO}}"
    )

    sep_line = "─" * len(header)
    rows = [header, sep_line]

    for p in positions:
        ticker = str(p.get("ticker", "?")).upper()
        qty    = _to_float(p.get("quantity", 0))
        price  = _to_float(p.get("current_price", 0))
        mv     = _to_float(p.get("market_value", 0))

        weight = mv / total_invested if total_invested > 0 else 0.0

        rows.append(
            f"{ticker:<{COL_TICKER}}{SEP}"
            f"{qty:>{COL_CANT}g}{SEP}"
            f"{_ars(price):>{COL_PRECIO}}{SEP}"
            f"{_ars(mv):>{COL_VALOR}}{SEP}"
            f"{weight:>{COL_PESO}.1%}"
        )

    rows.append(sep_line)
    rows.append(
        f"{'TOTAL':<{COL_TICKER}}{SEP}"
        f"{'':>{COL_CANT}}{SEP}"
        f"{'':>{COL_PRECIO}}{SEP}"
        f"{_ars(total_invested):>{COL_VALOR}}{SEP}"
        f"{'100.0%':>{COL_PESO}}"
    )

    return "\n".join(rows)


# ─────────────────────────────────────────────────────────────────────────────
# Acción: Portfolio
# ─────────────────────────────────────────────────────────────────────────────

async def action_portfolio(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    if not get_config or not PortfolioDatabase:
        await send_text(context, chat_id, "❌ No pude importar config/db del proyecto.")
        return

    cfg = get_config()
    snap = await get_cached_live_portfolio() if get_cached_live_portfolio else None
    valuation_mode = str((snap or {}).get("valuation_mode", "snapshot"))

    if not snap:
        db  = PortfolioDatabase(cfg.database.url)
        await db.connect()
        try:
            snap = await db.get_latest_snapshot()
        finally:
            await db.close()
        valuation_mode = "snapshot"

    if not snap:
        await send_text(
            context,
            chat_id,
            "⚠️ Sin snapshots en DB.\nEjecutá <code>/admin_scrape</code> para actualizar.",
        )
        return

    # ── Valores base ──────────────────────────────────────────────────────────
    #
    # Criterio MVP:
    # - total_invested = suma real de positions.market_value
    # - cash = snap.cash_ars
    # - total_account = total_invested + cash
    #
    # No usamos P&L acá porque el costo base todavía no es confiable.
    # El seguimiento de resultados queda centralizado en /performance.
    #
    cash          = _to_float(snap.get("cash_ars", 0))
    positions_raw = snap.get("positions") or []

    positions = sorted(
        positions_raw,
        key=lambda p: _to_float(p.get("market_value", 0)),
        reverse=True,
    )

    total_invested = sum(
        _to_float(p.get("market_value", 0))
        for p in positions
    )

    total_account = total_invested + cash

    # Timestamp
    ts_raw       = (
        snap.get("generated_at")
        if valuation_mode == "live_market_prices"
        else snap.get("scraped_at") or snap.get("timestamp") or snap.get("created_at")
    )
    age_text, _  = _age_label(ts_raw)
    ts_exact     = _fmt_dt_art(ts_raw)

    # Porcentajes sobre total cuenta
    inv_pct  = total_invested / total_account if total_account > 0 else 0.0
    cash_pct = cash / total_account if total_account > 0 else 0.0

    # Concentración sobre capital invertido
    max_weight = 0.0
    max_ticker = "—"

    for p in positions:
        mv = _to_float(p.get("market_value", 0))
        w  = mv / total_invested if total_invested > 0 else 0.0

        if w > max_weight:
            max_weight = w
            max_ticker = str(p.get("ticker", "?")).upper()

    if max_weight >= 0.35:
        conc_icon = "🔴"
        conc_lbl  = f"alta — {max_ticker} {_pct(max_weight)}"
    elif max_weight >= 0.25:
        conc_icon = "🟡"
        conc_lbl  = f"media — {max_ticker} {_pct(max_weight)}"
    else:
        conc_icon = "🟢"
        conc_lbl  = "normal"

    # ── Construcción del reporte ──────────────────────────────────────────────
    lines = [
        "💼 <b>PORTFOLIO ACTUAL</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"🕐 {ts_exact}  ·  {age_text}",
        "",
        f"💰 Total cuenta    <b>{_money(total_account)}</b>",
        f"📈 Invertido       <b>{_money(total_invested)}</b>  ({_pct(inv_pct)})",
        f"💵 Cash disponible <b>{_money(cash)}</b>  ({_pct(cash_pct)})",
        f"📦 {len(positions)} posiciones  ·  {conc_icon} Concentración {conc_lbl}",
    ]

    if valuation_mode == "live_market_prices":
        lines.append("⚡ Valuación live estimada con market_prices.")

    if not positions:
        lines += [
            "",
            "Sin posiciones en el último snapshot.",
            "",
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            "<i>Portfolio — Cocos Copilot</i>",
        ]
        await send_text(context, chat_id, "\n".join(lines))
        return

    lines += [
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "<b>POSICIONES</b>  <i>(peso sobre invertido)</i>",
        "",
        f"<code>{_render_portfolio_table(positions, total_invested)}</code>",
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "<i>Portfolio — Cocos Copilot</i>",
    ]

    await send_text(context, chat_id, "\n".join(lines))


# ─────────────────────────────────────────────────────────────────────────────
# Acción: Resumen semanal
# ─────────────────────────────────────────────────────────────────────────────

async def action_weekly_summary(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    report = await run_python_script(
        "scripts/weekly_summary.py", "--no-telegram", timeout=120,
    )
    await send_text(context, chat_id, report)


# ─────────────────────────────────────────────────────────────────────────────
# Acción: Análisis semanal
# ─────────────────────────────────────────────────────────────────────────────

async def action_analysis(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    report = await run_first_existing_script(
        [
            ["scripts/run_analysis.py", "--no-telegram", "--no-llm", "--no-sentiment"],
            ["scripts/run_analysis.py", "--no-llm", "--no-sentiment"],
            ["scripts/run_analysis.py", "--no-telegram"],
            ["scripts/run_analysis.py"],
        ],
        timeout=COMMAND_TIMEOUT_SECONDS,
    )
    await send_text(context, chat_id, report)


# ─────────────────────────────────────────────────────────────────────────────
# Acción: Performance
# ─────────────────────────────────────────────────────────────────────────────

async def action_performance(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    report = await run_python_script(
        "scripts/run_performance.py", "--days", "90", "--no-telegram", timeout=240,
    )
    await send_text(context, chat_id, report)


def compact_radar_report(report: str, max_items: int = 6) -> str:
    """
    Compacta el radar largo para Telegram.
    No recalcula nada: extrae bloques principales del texto renderizado.
    """
    import re

    if not report:
        return "⚠️ Radar sin output."

    # Sacar tags HTML simples para parsear más fácil, pero mantener texto legible.
    text = report
    text = re.sub(r"</?b>", "", text)
    text = re.sub(r"</?i>", "", text)
    text = re.sub(r"</?code>", "", text)
    text = text.replace("&gt;", ">").replace("&lt;", "<").replace("&amp;", "&")

    universe_match = re.search(r"^🔍 Universo:\s*(.+)$", text, re.MULTILINE)
    gate_match = re.search(r"^(?:✅|⚠️|🔴|⚪)\s*Gate:\s*(.+)$", text, re.MULTILINE)
    vix_match = re.search(r"^\s*VIX:\s*([0-9.]+)$", text, re.MULTILINE)

    universe = universe_match.group(1).strip() if universe_match else "—"
    gate = gate_match.group(1).strip() if gate_match else "—"
    vix = vix_match.group(1).strip() if vix_match else "—"

    # Detectar bloques por ticker: líneas tipo "━━ KKR ━━ ..."
    ticker_blocks = re.split(r"\n(?=━━\s+[A-Z0-9.-]+\s+━━)", text)

    items = []

    for block in ticker_blocks:
        title = re.search(r"━━\s+([A-Z0-9.-]+)\s+━━\s*(.*)", block)
        if not title:
            continue

        ticker = title.group(1).strip()
        title_tail = title.group(2).strip()

        score = re.search(r"Score:\s*([+-]?\d+\.\d+)", block)
        rr = re.search(r"R/R\s*([0-9.]+)x", block)
        edge = re.search(r"Edge:\s*[🟢🟡🟠🔴]?\s*([+-]?\d+\.\d+)", block)
        sizing_ars = re.search(r"≈\s*\$([0-9.]+)\s*ARS", block)
        action = re.search(r"🎯 Acción sugerida:\s*(.+)", block)
        compete = re.search(r"Compite con:\s*([A-Z0-9.-]+)", block)

        tag = "🆕"
        if "SWAP" in title_tail.upper():
            tag = "🔄"
        elif "VIGILANCIA" in title_tail.upper():
            tag = "👁"

        action_text = action.group(1).strip() if action else "—"

        if compete and "vs " not in action_text.lower():
            action_text += f" ({compete.group(1)})"

        items.append({
            "ticker": ticker,
            "tag": tag,
            "score": score.group(1) if score else "—",
            "rr": rr.group(1) if rr else "—",
            "edge": edge.group(1) if edge else "—",
            "ars": sizing_ars.group(1) if sizing_ars else "—",
            "action": action_text,
        })

    if not items:
        return report

    top = items[:max_items]

    lines = [
        "🔭 <b>RADAR DE OPORTUNIDADES — COMPACTO</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"🔍 Universo: {universe}",
        f"✅ Gate: <b>{gate}</b> | VIX {vix}",
        "",
        "<b>TOP IDEAS</b>",
    ]

    for i, item in enumerate(top, start=1):
        lines.append(
            f"{i}. {item['tag']} <b>{item['ticker']}</b> "
            f"| score <code>{item['score']}</code> "
            f"| edge <code>{item['edge']}</code> "
            f"| R/R {item['rr']}x"
        )

        if item["ars"] != "—":
            lines.append(f"   💰 Sizing aprox: <b>${item['ars']} ARS</b>")

        lines.append(f"   🎯 {item['action']}")

    lines += [
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "<i>Usá consola para ver el radar completo.</i>",
    ]

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Acción: Radar
# ─────────────────────────────────────────────────────────────────────────────

async def action_radar(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    report = await run_first_existing_script(
        [
            [
                "scripts/run_opportunity.py",
                "--no-telegram",
                "--no-sentiment",
                "--period",
                "6mo",
                "--top",
                "6",
                "--min-score",
                "0.10",
            ],
        ],
        timeout=180,
    )

    if not report or not report.strip():
        report = (
            "⚠️ Radar sin output.\n"
            "Runner esperado:\n"
            "<code>scripts/run_opportunity.py --no-telegram --no-sentiment --period 6mo --top 6 --min-score 0.10</code>"
        )
    else:
        report = compact_radar_report(report, max_items=6)

    await send_text(context, chat_id, report)


# ─────────────────────────────────────────────────────────────────────────────
# Acción: Radar completo
# ─────────────────────────────────────────────────────────────────────────────

async def action_radar_full(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    report = await run_first_existing_script(
        [
            [
                "scripts/run_opportunity.py",
                "--no-telegram",
                "--no-sentiment",
                "--period",
                "1y",
                "--max",
                "8",
            ],
        ],
        timeout=COMMAND_TIMEOUT_SECONDS,
    )

    await send_text(context, chat_id, report)


# ─────────────────────────────────────────────────────────────────────────────
# Acción: Auditoría de regresión
# ─────────────────────────────────────────────────────────────────────────────

async def action_regression_audit(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    mode: str = DEFAULT_REGRESSION_MODE,
) -> None:
    mode = str(mode or DEFAULT_REGRESSION_MODE).lower().strip()

    if mode not in REGRESSION_MODES:
        help_text = (
            "⚠️ Modo inválido para regression audit.\n\n"
            "Usá:\n"
            "• <code>/regression optimizer</code>\n"
            "• <code>/regression execution</code>\n"
            "• <code>/regression blocked</code>\n"
            "• <code>/regression signal</code>\n"
            "• <code>/regression all</code>"
        )
        await send_text(context, chat_id, help_text)
        return

    report = await run_python_script(
        "scripts/run_regression_audit.py",
        "--mode",
        mode,
        "--days",
        "180",
        "--target",
        "directional",
        "--compact",
        "--no-telegram",
        timeout=240,
    )
    await send_text(context, chat_id, report)

# ─────────────────────────────────────────────────────────────────────────────
# Acción: Status
# ─────────────────────────────────────────────────────────────────────────────

async def action_status(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    """Estado del sistema: DB, market data, monitor."""
    lines = [
        "🩺 <b>STATUS DEL SISTEMA</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
    ]

    # ── Contexto de mercado ───────────────────────────────────────────────────
    now = datetime.now(tz=TZ)
    business = _is_business_day_now()
    market   = _is_market_hours_now()

    if not business:
        mkt_icon, mkt_label = "📅", "Fin de semana — sin rueda"
    elif market:
        mkt_icon, mkt_label = "🟢", "Rueda abierta"
    else:
        mkt_icon, mkt_label = "🌙", "Día hábil — fuera de rueda"

    lines += [
        f"{mkt_icon} Mercado: <b>{mkt_label}</b>",
        f"🕐 {now.strftime('%d/%m/%Y %H:%M ART')}",
        "",
    ]

    # ── Estado de DB / snapshots ──────────────────────────────────────────────
    if get_config and PortfolioDatabase:
        try:
            cfg = get_config()
            db  = PortfolioDatabase(cfg.database.url)
            await db.connect()
            try:
                snap = await db.get_latest_snapshot()
            finally:
                await db.close()

            if snap:
                ts_raw         = snap.get("scraped_at") or snap.get("timestamp")
                age_text, mins = _age_label(ts_raw)
                ts_exact       = _fmt_dt_art(ts_raw)

                snap_icon, snap_suffix = _freshness_badge(mins, business_day=business)

                total = _to_float(snap.get("total_value_ars", 0))
                n_pos = len(snap.get("positions") or [])
                lines += [
                    f"{snap_icon} Último snapshot: <b>{age_text}</b>{snap_suffix}",
                    f"   {ts_exact}",
                    f"   Portfolio: <b>{_money(total)}</b>  ·  {n_pos} posiciones",
                    "",
                ]
            else:
                lines += ["🔴 No hay snapshots en DB.", ""]

        except Exception as e:
            lines += [f"🔴 Error al leer DB: <code>{e}</code>", ""]
    else:
        lines += ["⚠️ DB no disponible (módulos no importados).", ""]

    # ── Market data ───────────────────────────────────────────────────────────
    try:
        if get_config and PortfolioDatabase:
            cfg  = get_config()
            db   = PortfolioDatabase(cfg.database.url)
            pool = None
            await db.connect()
            try:
                pool = getattr(db, "_pool", None) or getattr(db, "_db_pool", None)
                if pool:
                    async with pool.acquire() as conn:
                        row = await conn.fetchrow(
                            "SELECT MAX(ts) AS latest_ts FROM market_prices"
                        )
                        mkt_ts = row["latest_ts"] if row else None
                    age_text_mkt, mins_mkt = _age_label(mkt_ts)
                    mkt_icon2, mkt_suffix = _freshness_badge(
                        mins_mkt,
                        business_day=business,
                    )
                    lines.append(f"{mkt_icon2} Market data: <b>{age_text_mkt}</b>{mkt_suffix}")
                    lines.append(f"   {_fmt_dt_art(mkt_ts)}")
            finally:
                await db.close()
    except Exception as e:
        lines.append(f"⚠️ No pude leer market_prices: <code>{e}</code>")

    lines += [
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "<i>Status — Cocos Copilot</i>",
    ]

    await send_text(context, chat_id, "\n".join(lines))


# ─────────────────────────────────────────────────────────────────────────────
# Acción: Admin scrape
# ─────────────────────────────────────────────────────────────────────────────

async def action_admin_scrape(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    if not is_admin(chat_id):
        await send_text(context, chat_id, "🚫 Comando restringido a administradores.")
        logger.warning("[BOT] /admin_scrape bloqueado para chat_id=%s", chat_id)
        return
    report = await run_first_existing_script(
        [
            ["scripts/run_once.py", "--full"],
            ["scripts/run_once.py"],
            ["scripts/scrape_once.py"],
        ],
        timeout=240,
    )
    await send_text(context, chat_id, report)


# ─────────────────────────────────────────────────────────────────────────────
# Acción: Admin refresh portfolio
# ─────────────────────────────────────────────────────────────────────────────

async def action_admin_refresh_portfolio(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    if not is_admin(chat_id):
        await send_text(context, chat_id, "🚫 Comando restringido a administradores.")
        logger.warning("[BOT] /admin_refresh_portfolio bloqueado para chat_id=%s", chat_id)
        return
    report = await run_first_existing_script(
        [
            ["scripts/run_once.py"],
            ["scripts/run_once.py", "--full"],
            ["scripts/scrape_once.py"],
        ],
        timeout=240,
    )
    await send_text(context, chat_id, report)
    await action_portfolio(context, chat_id)


# ─────────────────────────────────────────────────────────────────────────────
# Router de callbacks
# ─────────────────────────────────────────────────────────────────────────────

CALLBACK_ALIASES: dict[str, str] = {
    # Portfolio
    "portfolio":        "portfolio",
    "current_portfolio":"portfolio",
    "ver_portfolio":    "portfolio",
    # Análisis
    "weekly_analysis":  "analysis",
    "analysis":         "analysis",
    "analisis":         "analysis",
    "run_analysis":     "analysis",
    "analisis_semanal": "analysis",
    # Resumen semanal
    "weekly_summary":   "weekly_summary",
    "summary":          "weekly_summary",
    "resumen":          "weekly_summary",
    "resumen_semanal":  "weekly_summary",
    # Performance
    "performance":      "performance",
    "perf":             "performance",
    "run_performance":  "performance",
    # Radar
    "radar":            "radar",
    "opportunities":    "radar",
    "opportunity_radar":"radar",
    "oportunidades":    "radar",
    # Regression
    "regression":       "regression_audit",
    "regression_audit": "regression_audit",
    "regression_opt":   "regression_audit",
    # Status
    "status":           "status",
    "health":           "status",
}

ACTION_LOADING_TEXT: dict[str, str] = {
    "portfolio":     "💼 Leyendo último portfolio...",
    "analysis":      "🧠 Generando plan de cartera...",
    "weekly_summary":"📅 Generando resumen semanal...",
    "performance":   "📊 Calculando performance y outcomes...",
    "radar":         "🔭 Generando radar de oportunidades...",
    "radar_full":    "🔭 Generando radar completo...",
    "regression_audit": "📈 Ejecutando auditoría de regresión...",
    "status":        "🩺 Verificando estado del sistema...",
}


async def run_action(action: str, context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    dispatch = {
        "portfolio":      action_portfolio,
        "analysis":       action_analysis,
        "weekly_summary": action_weekly_summary,
        "performance":    action_performance,
        "radar":          action_radar,
        "radar_full": action_radar_full,
        "regression_audit": action_regression_audit,
        "status":         action_status,
    }
    fn = dispatch.get(action)
    if fn:
        await fn(context, chat_id)
    else:
        await send_text(context, chat_id, f"⚠️ Acción desconocida: <code>{action}</code>")


# ─────────────────────────────────────────────────────────────────────────────
# Handlers
# ─────────────────────────────────────────────────────────────────────────────

async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    await update.message.reply_text(
        menu_text(),
        parse_mode=ParseMode.HTML,
        reply_markup=main_keyboard(),
        disable_web_page_preview=True,
    )


async def menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await start_handler(update, context)


async def _dispatch_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    action: str,
) -> None:
    chat_id = update.effective_chat.id
    loading = ACTION_LOADING_TEXT.get(action, "🔄 Procesando...")
    await answer_loading(update, loading)
    t0 = time.time()
    logger.info("[BOT] action=%s chat_id=%s", action, chat_id)
    try:
        await run_action(action, context, chat_id)
        logger.info("[BOT] action=%s OK en %.2fs", action, time.time() - t0)
    except Exception as e:
        logger.exception("[BOT] action=%s falló", action)
        await send_text(
            context, chat_id,
            f"❌ Error en <b>{action}</b>:\n<code>{e}</code>",
        )
    finally:
        await send_menu(context, chat_id)


async def portfolio_handler(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    await _dispatch_command(u, c, "portfolio")

async def analysis_handler(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    await _dispatch_command(u, c, "analysis")

async def weekly_summary_handler(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    await _dispatch_command(u, c, "weekly_summary")

async def performance_handler(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    await _dispatch_command(u, c, "performance")

async def radar_handler(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    await _dispatch_command(u, c, "radar")

async def status_handler(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    await _dispatch_command(u, c, "status")

async def regression_audit_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    chat_id = update.effective_chat.id

    raw_mode = (
        context.args[0].lower().strip()
        if getattr(context, "args", None)
        else DEFAULT_REGRESSION_MODE
    )

    aliases = {
        "opt": "optimizer",
        "optimizer": "optimizer",
        "optim": "optimizer",
        "exec": "execution",
        "execution": "execution",
        "real": "execution",
        "blocked": "blocked",
        "block": "blocked",
        "guards": "blocked",
        "signal": "signal",
        "score": "signal",
        "all": "all",
        "global": "all",
    }

    mode = aliases.get(raw_mode, raw_mode)

    loading = f"📈 Ejecutando auditoría de regresión: <b>{mode}</b>..."
    await answer_loading(update, loading)

    t0 = time.time()
    logger.info("[BOT] regression_audit mode=%s chat_id=%s", mode, chat_id)

    try:
        await action_regression_audit(context, chat_id, mode=mode)
        logger.info(
            "[BOT] regression_audit mode=%s OK en %.2fs",
            mode,
            time.time() - t0,
        )
    except Exception as e:
        logger.exception("[BOT] regression_audit mode=%s falló", mode)
        await send_text(
            context,
            chat_id,
            f"❌ Error en <b>regression {mode}</b>:\n<code>{e}</code>",
        )
    finally:
        await send_menu(context, chat_id)


async def regression_optimizer_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    context.args = ["optimizer"]
    await regression_audit_handler(update, context)


async def regression_execution_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    context.args = ["execution"]
    await regression_audit_handler(update, context)


async def regression_blocked_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    context.args = ["blocked"]
    await regression_audit_handler(update, context)


async def regression_signal_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    context.args = ["signal"]
    await regression_audit_handler(update, context)


async def radar_full_handler(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    await _dispatch_command(u, c, "radar_full")


async def admin_scrape_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    await answer_loading(update, "⚙️ Iniciando scraping en modo admin...")
    await action_admin_scrape(context, chat_id)
    await send_menu(context, chat_id)


async def admin_refresh_portfolio_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    await answer_loading(update, "🔄 Refrescando portfolio en modo admin...")
    await action_admin_refresh_portfolio(context, chat_id)
    await send_menu(context, chat_id)


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return

    raw_action = str(query.data or "").strip()
    action     = CALLBACK_ALIASES.get(raw_action)
    chat_id    = query.message.chat_id if query.message else update.effective_chat.id

    await query.answer()

    if not action:
        await send_text(
            context, chat_id,
            f"⚠️ Botón no reconocido: <code>{raw_action}</code>\n"
            "Abrí /menu para refrescar.",
        )
        logger.warning("[BOT] callback no mapeado: %s", raw_action)
        await send_menu(context, chat_id)
        return

    loading = ACTION_LOADING_TEXT.get(action, "🔄 Procesando...")
    try:
        await query.edit_message_text(
            text=loading,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
    except Exception:
        await send_text(context, chat_id, loading)

    t0 = time.time()
    logger.info("[BOT] callback raw=%s → %s chat_id=%s", raw_action, action, chat_id)
    try:
        await run_action(action, context, chat_id)
        logger.info("[BOT] callback=%s OK en %.2fs", action, time.time() - t0)
    except Exception as e:
        logger.exception("[BOT] callback=%s falló", action)
        await send_text(
            context, chat_id,
            f"❌ Error en <b>{action}</b>:\n<code>{e}</code>",
        )
    finally:
        await send_menu(context, chat_id)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("[BOT] Error no capturado", exc_info=context.error)
    try:
        if isinstance(update, Update) and update.effective_chat:
            await send_text(
                context,
                update.effective_chat.id,
                f"❌ Error interno:\n<code>{context.error}</code>",
            )
            await send_menu(context, update.effective_chat.id)
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# App
# ─────────────────────────────────────────────────────────────────────────────

def build_app() -> Application:
    token = _get_token()
    app   = Application.builder().token(token).build()

    # Comandos principales
    app.add_handler(CommandHandler("start",            start_handler))
    app.add_handler(CommandHandler("menu",             menu_handler))
    app.add_handler(CommandHandler("portfolio",        portfolio_handler))
    app.add_handler(CommandHandler("analisis",         analysis_handler))
    app.add_handler(CommandHandler("analysis",         analysis_handler))
    app.add_handler(CommandHandler("analisis_semanal", analysis_handler))
    app.add_handler(CommandHandler("resumen",          weekly_summary_handler))
    app.add_handler(CommandHandler("weekly_summary",   weekly_summary_handler))
    app.add_handler(CommandHandler("resumen_semanal",  weekly_summary_handler))
    app.add_handler(CommandHandler("performance",      performance_handler))
    app.add_handler(CommandHandler("radar",            radar_handler))
    app.add_handler(CommandHandler("radar_full", radar_full_handler))
    app.add_handler(CommandHandler("regression", regression_audit_handler))
    app.add_handler(CommandHandler("regression_audit", regression_audit_handler))
    app.add_handler(CommandHandler("status",           status_handler))

    # Admin
    app.add_handler(CommandHandler("admin_scrape",              admin_scrape_handler))
    app.add_handler(CommandHandler("admin_refresh_portfolio",   admin_refresh_portfolio_handler))

    # Botones
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_error_handler(error_handler)

    return app


def main() -> None:
    logger.info("[BOT] Iniciando Cocos Copilot")
    logger.info(
        "[BOT] Admins: %s",
        sorted(ADMIN_CHAT_IDS) if ADMIN_CHAT_IDS else "NINGUNO",
    )
    build_app().run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()

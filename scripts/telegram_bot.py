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
  - Configuración (solo multiusuario)

Scraping manual:
  - Removido del menú principal
  - Disponible solo con /admin_scrape para ADMIN_CHAT_IDS

Requiere:
  TELEGRAM_BOT_TOKEN o SCRAPER_TELEGRAM_BOT_TOKEN
  ADMIN_CHAT_IDS=<telegram_chat_id>
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import shlex
import sys
import time
from dataclasses import replace
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
    MessageHandler,
    filters,
)

# ─────────────────────────────────────────────────────────────────────────────
# Path raíz del proyecto
# ─────────────────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.core.telegram_format import html_text, note, validate_telegram_html

try:
    from src.core.config import get_config
    from src.core.credentials import CredentialCipher, UserCredentials
    from src.core.logger import get_logger
    from src.core.market_calendar import is_trading_day, market_closed_reason
    from src.core.portfolio_cache import (
        cache_portfolio_snapshot,
        get_cached_live_portfolio,
    )
    from src.core.portfolio_refresh import request_portfolio_refresh
    from src.core.report_artifacts import (
        load_report_artifact,
        save_report_artifact,
    )
    from src.core.redis_client import client as redis_client
    from src.collector.cocos_scraper import CocosCapitalScraper
    from src.collector.db import PortfolioDatabase
except Exception:
    get_config = None
    CredentialCipher = None
    UserCredentials = None
    get_logger = None
    is_trading_day = None
    market_closed_reason = None
    cache_portfolio_snapshot = None
    get_cached_live_portfolio = None
    request_portfolio_refresh = None
    load_report_artifact = None
    save_report_artifact = None
    redis_client = None
    CocosCapitalScraper = None
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
BOT_HEARTBEAT_KEY = "cocos:bot:last_heartbeat"
OPERATIONAL_SYNC_KEY_PREFIX = "cocos:telegram:operational_sync"
OPERATIONAL_SYNC_TTL_SECONDS = max(
    10,
    int(os.getenv("TELEGRAM_OPERATIONAL_SYNC_TTL_SECONDS", "30")),
)
RADAR_MANUAL_EXPLORATORY_ENABLED = os.getenv(
    "RADAR_MANUAL_EXPLORATORY_ENABLED", "false"
).lower() == "true"

MAX_MESSAGE_LENGTH = 3900
COMMAND_TIMEOUT_SECONDS = 300

REGRESSION_MODES = {"optimizer", "execution", "blocked", "signal", "all"}
DEFAULT_REGRESSION_MODE = "execution"
SETTINGS_STATE_KEY = "settings_state"
SETTINGS_USERNAME_KEY = "settings_username"
SETTINGS_AWAIT_USERNAME = "await_username"
SETTINGS_AWAIT_PASSWORD = "await_password"
PORTFOLIO_SYNC_PENDING_KEY = "portfolio_sync_pending"
NO_AUTO_MENU_ACTIONS = {"settings", "settings_reconfigure"}
FAST_ACTIONS = {"status"}

BOT_COMMAND_SPECS: list[tuple[str, str]] = [
    ("menu", "Abrir panel principal"),
    ("help", "Cómo leer Quantia"),
    ("portfolio", "Cartera actual"),
    ("analisis", "Plan operativo"),
    ("events", "Próximos balances"),
    ("ticker", "Análisis por ticker"),
    ("radar", "Oportunidades experimentales"),
    ("radar_metricas", "Métricas prospectivas Radar"),
    ("mercado", "Mercado y noticias"),
    ("performance", "Resultados operativos"),
    ("neto", "Neto por análisis"),
    ("ledger", "Libro de decisiones"),
    ("bot_vs_humano", "Bot vs decisiones humanas"),
    ("status", "Salud del sistema"),
]

ADMIN_CHAT_IDS: set[int] = {
    int(x)
    for x in os.getenv("ADMIN_CHAT_IDS", "").replace(";", ",").split(",")
    if x.strip().isdigit()
}
TELEGRAM_ALLOWED_CHAT_IDS: set[int] = {
    int(x)
    for x in (
        ",".join(
            [
                os.getenv("TELEGRAM_CHAT_ID", ""),
                os.getenv("TELEGRAM_ALLOWED_CHAT_IDS", ""),
            ]
        )
    )
    .replace(";", ",")
    .split(",")
    if x.strip().isdigit()
}
ALLOW_ALL_CHATS = os.getenv("TELEGRAM_ALLOW_ALL_CHATS", "false").lower() in {
    "1",
    "true",
    "yes",
    "y",
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


def is_allowed_chat(chat_id: int) -> bool:
    if _multiuser_enabled() or ALLOW_ALL_CHATS:
        return True
    allowed = TELEGRAM_ALLOWED_CHAT_IDS | ADMIN_CHAT_IDS
    if not allowed:
        logger.warning("[BOT] No hay chats permitidos configurados; acceso bloqueado")
        return False
    return int(chat_id) in allowed


async def ensure_allowed_chat(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> bool:
    if not update.effective_chat:
        return False
    chat_id = int(update.effective_chat.id)
    if is_allowed_chat(chat_id):
        return True

    logger.warning("[BOT] Chat no autorizado: chat_id=%s", chat_id)
    try:
        if update.callback_query:
            await update.callback_query.answer("Chat no autorizado.", show_alert=True)
        elif update.message:
            await update.message.reply_text("Chat no autorizado para este bot.")
    except Exception:
        pass
    return False


def _multiuser_enabled() -> bool:
    if not get_config:
        return False
    try:
        return bool(getattr(get_config(), "multiuser_enabled", False))
    except Exception:
        logger.exception("[BOT] No pude leer MULTIUSER_ENABLED")
        return False


def _clear_settings_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop(SETTINGS_STATE_KEY, None)
    context.user_data.pop(SETTINGS_USERNAME_KEY, None)


def _user_session_file(chat_id: int) -> str:
    return f"/app/secrets/cocos_session_{int(chat_id)}.json"


def _owner_cli_args(chat_id: int) -> list[str]:
    return ["--owner-chat-id", str(chat_id)] if _multiuser_enabled() else []


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
    now = datetime.now(tz=TZ)
    if is_trading_day is None:
        return now.weekday() < 5
    return is_trading_day(now)


def _market_closed_reason_now() -> Optional[str]:
    now = datetime.now(tz=TZ)
    if market_closed_reason is None:
        return "fin_de_semana" if now.weekday() >= 5 else None
    return market_closed_reason(now)


def _is_market_hours_now() -> bool:
    now = datetime.now(tz=TZ)
    mins = now.hour * 60 + now.minute
    return 10 * 60 + 30 <= mins < 17 * 60


def _report_cache_market_open() -> bool:
    return (
        _is_business_day_now()
        and _is_market_hours_now()
        and _market_closed_reason_now() is None
    )


def _cacheable_report(report: str) -> bool:
    clean = str(report or "").strip()
    return len(clean) >= 80 and not clean.startswith(("❌", "⚠️"))


def _artifact_note(artifact: dict, full_command: str) -> str:
    generated_at = artifact.get("generated_at")
    market_data_at = artifact.get("market_data_at") or artifact.get("candle_data_at")

    def _fmt(value) -> str:
        if not value:
            return "sin fecha"
        if isinstance(value, str):
            try:
                value = datetime.fromisoformat(value)
            except ValueError:
                return value
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            return value.astimezone(TZ).strftime("%d/%m %H:%M")
        return str(value)

    return (
        f"<i>Resultado cacheado: generado {_fmt(generated_at)}; "
        f"datos al {_fmt(market_data_at)}. {full_command} fuerza actualización.</i>\n\n"
    )


async def _load_cached_report(report_type: str, chat_id: int) -> dict | None:
    if load_report_artifact is None or get_config is None:
        return None
    try:
        cfg = get_config()
        return await load_report_artifact(
            cfg.database.url,
            report_type=report_type,
            owner_chat_id=chat_id,
            market_open=_report_cache_market_open(),
        )
    except Exception as exc:
        logger.warning("[BOT][CACHE] read fallo type=%s: %s", report_type, exc)
        return None


async def _save_cached_report(report_type: str, chat_id: int, report: str) -> None:
    if not _cacheable_report(report) or save_report_artifact is None or get_config is None:
        return
    try:
        cfg = get_config()
        await save_report_artifact(
            cfg.database.url,
            report_type=report_type,
            owner_chat_id=chat_id,
            report_text=report,
            metadata={"source": "telegram", "market_open": _report_cache_market_open()},
        )
    except Exception as exc:
        logger.warning("[BOT][CACHE] write fallo type=%s: %s", report_type, exc)


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


def _split_long_line(line: str, max_len: int) -> list[str]:
    if len(line) <= max_len:
        return [line]

    parts: list[str] = []
    remaining = line
    while len(remaining) > max_len:
        cut = remaining.rfind(" ", 0, max_len)
        if cut < max_len // 2:
            cut = max_len
        parts.append(remaining[:cut].rstrip())
        remaining = remaining[cut:].lstrip()
    if remaining:
        parts.append(remaining)
    return parts


def split_message(text: str, max_len: int = MAX_MESSAGE_LENGTH) -> list[str]:
    if not text:
        return ["⚠️ Sin contenido para mostrar."]
    if len(text) <= max_len:
        return [text]
    chunks: list[str] = []
    current = ""
    for line in text.splitlines():
        for part in _split_long_line(line, max_len):
            candidate = current + part + "\n"
            if len(candidate) > max_len:
                if current.strip():
                    chunks.append(current.rstrip())
                current = part + "\n"
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
    reply_markup: Optional[InlineKeyboardMarkup] = None,
) -> None:
    chunks = split_message(text)
    for index, chunk in enumerate(chunks):
        chunk_markup = reply_markup if index == len(chunks) - 1 else None
        try:
            valid_html, errors = validate_telegram_html(chunk)
            if not valid_html:
                logger.warning("[BOT] HTML potencialmente inválido: %s", errors[:3])
            await context.bot.send_message(
                chat_id=chat_id,
                text=chunk,
                parse_mode=parse_mode,
                disable_web_page_preview=True,
                reply_markup=chunk_markup,
            )
        except BadRequest as e:
            logger.warning("[BOT] Parse HTML falló, reintentando texto plano: %s", e)
            await context.bot.send_message(
                chat_id=chat_id,
                text=chunk,
                parse_mode=None,
                disable_web_page_preview=True,
                reply_markup=chunk_markup,
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


async def _delete_incoming_message(update: Update) -> None:
    if not update.message:
        return
    try:
        await update.message.delete()
    except Exception:
        pass


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
            f"⚠️ <b>No pude completar {html_text(script)}</b>\n"
            f"Tiempo: <b>{elapsed:.1f}s</b>\n"
            "Estado: no se guardan decisiones nuevas por este fallo.\n\n"
            "<b>Detalle técnico</b>\n"
            f"<code>{html_text(err[-2200:] or out[-2200:] or 'Sin detalle')}</code>\n\n"
            f"{note('Si se repite, revisar logs del contenedor antes de operar con ese reporte.')}"
        )
    if not out:
        return (
            f"⚠️ <b>{html_text(script)}</b> terminó sin reporte\n"
            f"Tiempo: <b>{elapsed:.1f}s</b>\n"
            "Estado: no hay datos nuevos para mostrar.\n"
            f"{note('No implica compra, venta ni cambio de performance.')}"
        )
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
            return out or (
                f"⚠️ <b>{html_text(script)}</b> terminó sin reporte\n"
                f"Tiempo: <b>{elapsed:.1f}s</b>\n"
                f"{note('No implica compra, venta ni cambio de performance.')}"
            )
        last_error = f"Script: {script}\nRC: {rc}\nSTDERR:\n{err[-2500:]}\n\nSTDOUT:\n{out[-2500:]}"

    return (
        "❌ No pude ejecutar ningún script candidato.\n\n"
        f"<code>{last_error or 'No se encontraron scripts compatibles.'}</code>"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Menú principal
# ─────────────────────────────────────────────────────────────────────────────

def _operational_sync_key(owner_chat_id: int) -> str:
    return f"{OPERATIONAL_SYNC_KEY_PREFIX}:{int(owner_chat_id)}"


async def _has_recent_operational_sync(owner_chat_id: int | None) -> bool:
    if owner_chat_id is None or redis_client is None:
        return False
    try:
        return bool(await redis_client.get(_operational_sync_key(owner_chat_id)))
    except Exception as exc:
        logger.debug("[BOT][SYNC] cache get ignorado: %s", exc)
        return False


async def _mark_operational_sync(owner_chat_id: int | None) -> None:
    if owner_chat_id is None or redis_client is None:
        return
    try:
        await redis_client.set(
            _operational_sync_key(owner_chat_id),
            str(int(time.time())),
            ex=OPERATIONAL_SYNC_TTL_SECONDS,
        )
    except Exception as exc:
        logger.debug("[BOT][SYNC] cache set ignorado: %s", exc)


async def sync_operational_state(
    *,
    full: bool = False,
    owner_chat_id: int | None = None,
    force: bool = False,
) -> str:
    if not force and not full and await _has_recent_operational_sync(owner_chat_id):
        logger.info(
            "[BOT][SYNC] cache hit owner_chat_id=%s ttl=%ss",
            owner_chat_id,
            OPERATIONAL_SYNC_TTL_SECONDS,
        )
        return ""

    if request_portfolio_refresh is None:
        return (
            "<b>Advertencia:</b> el canal de refresco persistente no está disponible. "
            "Muestro la lectura con la DB disponible.\n\n"
        )

    started = time.perf_counter()
    try:
        result = await request_portfolio_refresh(
            requester="telegram",
            owner_chat_id=owner_chat_id,
            include_fills=True,
            include_market=full,
            timeout_seconds=300 if full else 150,
        )
    except Exception as exc:
        result = {"ok": False, "error": "refresh_channel_failed", "detail": str(exc)}
    elapsed = time.perf_counter() - started
    if result.get("ok"):
        await _mark_operational_sync(owner_chat_id)
        logger.info(
            "[BOT][SYNC] persistent refresh ok owner_chat_id=%s full=%s duration_s=%.2f snapshot=%s",
            owner_chat_id,
            full,
            elapsed,
            result.get("snapshot_id"),
        )
        return ""
    detail = str(result.get("error") or result.get("detail") or "sin detalle")
    return (
        "<b>Advertencia:</b> la sesión persistente no pudo refrescar Cocos antes del reporte. "
        "Muestro la lectura con la DB disponible.\n"
        f"<code>{detail}</code>\n\n"
    )


def main_keyboard() -> InlineKeyboardMarkup:
    analysis_label = (
        "🧠 Plan"
        if (
            _is_business_day_now()
            and _is_market_hours_now()
            and _market_closed_reason_now() is None
        )
        else "🧠 Cierre"
    )
    rows = [
        [
            InlineKeyboardButton("💼 Cartera", callback_data="portfolio"),
            InlineKeyboardButton(analysis_label, callback_data="weekly_analysis"),
        ],
        [
            InlineKeyboardButton("🔎 Ticker", callback_data="ticker_analysis"),
            InlineKeyboardButton("🔭 Radar", callback_data="radar"),
        ],
        [
            InlineKeyboardButton("📅 Balances", callback_data="upcoming_events"),
            InlineKeyboardButton("📊 Resultados", callback_data="menu_results"),
        ],
        [
            InlineKeyboardButton("🧪 Auditoría", callback_data="menu_audit"),
            InlineKeyboardButton("🩺 Estado", callback_data="status"),
        ]
    ]
    if _multiuser_enabled():
        rows.append(
            [InlineKeyboardButton("⚙️ Configuración", callback_data="settings")]
        )
    return InlineKeyboardMarkup(rows)


def menu_text() -> str:
    return (
        "<b>QUANTIA</b>\n"
        "Cartera y decisiones\n\n"
        "<i>Seleccioná una vista</i>"
    )


def results_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 Performance", callback_data="performance"),
            InlineKeyboardButton("📅 Semana", callback_data="weekly_summary"),
        ],
        [InlineKeyboardButton("💹 Neto por análisis", callback_data="net_decisions")],
        [
            InlineKeyboardButton("⚖️ Bot vs Franco", callback_data="override_audit"),
            InlineKeyboardButton("🧭 Viabilidad", callback_data="viability"),
        ],
        [InlineKeyboardButton("← Inicio", callback_data="menu_home")],
    ])


def audit_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🧭 Confianza", callback_data="confidence_audit"),
            InlineKeyboardButton("🔬 Shadow", callback_data="shadow"),
        ],
        [
            InlineKeyboardButton("📐 Radar métricas", callback_data="radar_metrics"),
            InlineKeyboardButton("📈 Regresión", callback_data="regression"),
        ],
        [
            InlineKeyboardButton("📒 Decisiones", callback_data="decision_ledger"),
            InlineKeyboardButton("🌳 Flujo", callback_data="policy_tree"),
        ],
        [InlineKeyboardButton("← Inicio", callback_data="menu_home")],
    ])


def menu_view(action: str) -> tuple[str, InlineKeyboardMarkup] | None:
    if action == "menu_home":
        return menu_text(), main_keyboard()
    if action == "menu_results":
        return "<b>RESULTADOS</b>", results_keyboard()
    if action == "menu_audit":
        return "<b>AUDITORÍA</b>", audit_keyboard()
    return None


def help_text() -> str:
    return (
        "<b>Cómo leer Quantia</b>\n"
        "El bot informa y audita. No ejecuta órdenes.\n\n"
        "<b>Cartera y análisis</b>\n"
        "<code>/portfolio</code>: cartera actual y concentración.\n"
        "<code>/analisis</code>: plan operativo compacto y auditable.\n"
        "<code>/analisis_test</code>: simulación compacta sin guardar.\n"
        "<code>/analisis_full</code>: simulación completa sin guardar.\n"
        "<code>/analisis_debug</code>: diagnóstico técnico sin guardar.\n"
        "<code>/events</code>: próximos balances de la cartera.\n"
        "<code>/ticker NVDA</code>: análisis técnico y gráfico por acción.\n"
        "<code>/mercado</code>: contexto de mercado y noticias.\n"
        "\n<b>Radar y shadow</b>\n"
        "<code>/radar</code>: selección compacta de oportunidades experimentales.\n"
        "<code>/radar_full</code>: comparativa legible de todo el top operativo.\n"
        "<code>/radar_metricas</code>: evidencia prospectiva del ranking.\n"
        "<code>/shadow AMD</code>: tesis 5/20/40 por ticker; no ejecuta.\n"
        "\n<b>Resultados</b>\n"
        "<code>/resumen</code>: resumen semanal de la cartera.\n"
        "<code>/performance</code>: resultados reales con fills/movimientos.\n"
        "<code>/neto</code>: neto por corrida y decisión, con CSV completo.\n"
        "<code>/ledger</code>: atribución económica de decisiones.\n"
        "<code>/bot_vs_humano</code>: planes vs movimientos reales.\n"
        "<code>/viability</code>: evidencia y umbrales de viabilidad del bot.\n"
        "\n<b>Auditoría</b>\n"
        "<code>/confianza</code>: cobertura y confiabilidad operativa.\n"
        "<code>/calibracion</code>: calibración entre decisiones y outcomes.\n"
        "<code>/regression</code>: señales y resultados por capa.\n"
        "<code>/policy</code>: recorrido señal → plan → ejecución → outcome.\n"
        "\n<b>Sistema</b>\n"
        "<code>/status</code>: salud de DB, mercado y snapshots.\n\n"
        "<code>/menu</code>: panel principal.\n"
        "<code>/configuracion</code>: conexión del usuario, cuando está habilitada.\n\n"
        "<b>Lectura</b>\n"
        "Score: dirección de la señal, no probabilidad.\n"
        "T/M/S: aporte técnico, macro y sentiment al score.\n"
        "IC: calidad histórica del ranking; una muestra chica pide cautela.\n"
        "EV: retorno observado neto cuando corresponde.\n\n"
        "<b>Alcance</b>\n"
        "Ejecución real: solo fills/movimientos confirmados.\n"
        "Plan: propuesta operativa pendiente de validación.\n"
        "Radar/shadow/debug: auditoría o exploración; no contaminan performance.\n"
        "Los alias y comandos administrativos no se muestran para evitar ruido."
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

def _format_quantity(value: float) -> str:
    try:
        qty = float(value)
        if qty.is_integer():
            return str(int(qty))
        return f"{qty:.4f}".rstrip("0").rstrip(".")
    except Exception:
        return "0"


def _render_portfolio_positions(positions: list[dict], total_invested: float) -> str:
    """
    Genera una lista compacta pensada para Telegram mobile.

    Para MVP:
    - No usa tabla monoespaciada: Telegram mobile corta columnas largas.
    - No muestra P&L.
    - El peso se calcula sobre total_invested, no sobre cash ni total cuenta.
    """
    rows: list[str] = []

    for p in positions:
        ticker = html_text(str(p.get("ticker", "?")).upper())
        qty    = _to_float(p.get("quantity", 0))
        price  = _to_float(p.get("current_price", 0))
        mv     = _to_float(p.get("market_value", 0))
        weight = mv / total_invested if total_invested > 0 else 0.0

        rows.append(f"<b>{ticker}</b> · {_format_quantity(qty)} u · {_money(price)}")
        rows.append(f"{_money(mv)} · {_pct(weight)}")

    rows.append("")
    rows.append(f"<b>TOTAL</b> · {_money(total_invested)} · 100.0%")

    return "\n".join(rows)


# ─────────────────────────────────────────────────────────────────────────────
# Acción: Portfolio
# ─────────────────────────────────────────────────────────────────────────────

async def action_portfolio(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    *,
    refresh: bool = True,
) -> None:
    if not get_config or not PortfolioDatabase:
        await send_text(context, chat_id, "❌ No pude importar config/db del proyecto.")
        return

    cfg = get_config()
    owner_chat_id = chat_id if getattr(cfg, "multiuser_enabled", False) else None
    sync_note = (
        await sync_operational_state(
            full=False,
            owner_chat_id=chat_id,
            force=True,
        )
        if refresh
        else ""
    )
    db = PortfolioDatabase(cfg.database.url)
    await db.connect()
    try:
        snap = await db.get_latest_snapshot(owner_chat_id=owner_chat_id)
    finally:
        await db.close()
    valuation_mode = "snapshot"

    if not snap and get_cached_live_portfolio:
        snap = await get_cached_live_portfolio(owner_chat_id=owner_chat_id)
        valuation_mode = str((snap or {}).get("valuation_mode", "snapshot"))

    if not snap:
        if getattr(cfg, "multiuser_enabled", False):
            started = await _start_user_portfolio_sync_if_possible(
                context,
                chat_id,
                reason="No había snapshot privado todavía.",
            )
            if started:
                return
        await send_text(
            context,
            chat_id,
            sync_note
            + "⚠️ Sin snapshots en DB. La sesión persistente no pudo generar una foto nueva.",
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

    positions_total = sum(
        _to_float(p.get("market_value", 0))
        for p in positions
    )

    reported_total = _to_float(snap.get("total_value_ars", 0))
    reported_invested = _to_float(snap.get("invested_ars", 0))

    if valuation_mode == "live_market_prices":
        total_invested = reported_invested if reported_invested > 0 else positions_total
        total_account = reported_total if reported_total > 0 else total_invested + cash
        total_label = "Total live"
        invested_label = "Invertido live"
    else:
        total_invested = reported_total if reported_total > 0 else positions_total
        total_account = total_invested + cash
        total_label = "Tenencia Cocos"
        invested_label = "Cuenta estimada"

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

    if valuation_mode == "live_market_prices":
        primary_value = total_account
        primary_pct = ""
        secondary_value = total_invested
        secondary_pct = f"  ({_pct(inv_pct)})"
    else:
        primary_value = total_invested
        primary_pct = f"  ({_pct(inv_pct)})"
        secondary_value = total_account
        secondary_pct = ""

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
        f"💰 {total_label} <b>{_money(primary_value)}</b>{primary_pct}",
        f"📈 {invested_label} <b>{_money(secondary_value)}</b>{secondary_pct}",
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
        await send_text(context, chat_id, sync_note + "\n".join(lines))
        return

    lines += [
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "<b>POSICIONES</b>  <i>(peso sobre invertido)</i>",
        "",
        _render_portfolio_positions(positions, total_invested),
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "<i>Portfolio — Cocos Copilot</i>",
    ]

    await send_text(context, chat_id, sync_note + "\n".join(lines))


# ─────────────────────────────────────────────────────────────────────────────
# Acción: Resumen semanal
# ─────────────────────────────────────────────────────────────────────────────

async def action_weekly_summary(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    report = await run_python_script(
        "scripts/weekly_summary.py",
        "--no-telegram",
        *_owner_cli_args(chat_id),
        timeout=120,
    )
    await send_text(context, chat_id, report)


# ─────────────────────────────────────────────────────────────────────────────
# Acción: Análisis semanal
# ─────────────────────────────────────────────────────────────────────────────

async def action_analysis(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    started = time.perf_counter()
    sync_note = await sync_operational_state(
        full=False,
        owner_chat_id=chat_id,
    )
    cached = await _load_cached_report("analysis", chat_id)
    if cached:
        logger.info(
            "[BOT][ANALYSIS] cache_hit=true total_s=%.2f",
            time.perf_counter() - started,
        )
        await send_text(
            context,
            chat_id,
            sync_note
            + _artifact_note(cached, "/analisis_full")
            + str(cached["report_text"]),
        )
        return

    owner_args = _owner_cli_args(chat_id)
    analysis_started = time.perf_counter()
    report = await run_first_existing_script(
        [
            ["scripts/run_analysis.py", "--no-telegram", "--no-llm", "--skip-radar", *owner_args],
            ["scripts/run_analysis.py", "--no-llm", "--skip-radar", *owner_args],
            ["scripts/run_analysis.py", "--no-telegram", "--skip-radar", *owner_args],
            ["scripts/run_analysis.py", "--skip-radar", *owner_args],
        ],
        timeout=COMMAND_TIMEOUT_SECONDS,
    )
    logger.info(
        "[BOT][ANALYSIS] cache_hit=false compute_s=%.2f total_s=%.2f",
        time.perf_counter() - analysis_started,
        time.perf_counter() - started,
    )
    await _save_cached_report("analysis", chat_id, report)
    await send_text(context, chat_id, sync_note + report)


# ─────────────────────────────────────────────────────────────────────────────
# Acción: Performance
# ─────────────────────────────────────────────────────────────────────────────

async def action_analysis_test(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    owner_args = _owner_cli_args(chat_id)
    sync_note = await sync_operational_state(
        full=False,
        owner_chat_id=chat_id,
    )
    report = await run_first_existing_script(
        [
            ["scripts/run_analysis.py", "--no-telegram", "--no-llm", "--skip-radar", "--no-persist", *owner_args],
            ["scripts/run_analysis.py", "--no-llm", "--skip-radar", "--no-persist", *owner_args],
            ["scripts/run_analysis.py", "--no-telegram", "--skip-radar", "--no-persist", *owner_args],
            ["scripts/run_analysis.py", "--skip-radar", "--no-persist", *owner_args],
        ],
        timeout=COMMAND_TIMEOUT_SECONDS,
    )
    await send_text(context, chat_id, sync_note + report)


async def action_analysis_full(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    owner_args = _owner_cli_args(chat_id)
    sync_note = await sync_operational_state(full=True, owner_chat_id=chat_id)
    report = await run_first_existing_script(
        [
            ["scripts/run_analysis.py", "--no-telegram", "--no-llm", "--no-persist", "--run-intent", "exploratory", *owner_args],
            ["scripts/run_analysis.py", "--no-llm", "--no-persist", "--run-intent", "exploratory", *owner_args],
            ["scripts/run_analysis.py", "--no-telegram", "--no-persist", "--run-intent", "exploratory", *owner_args],
            ["scripts/run_analysis.py", "--no-persist", "--run-intent", "exploratory", *owner_args],
        ],
        timeout=COMMAND_TIMEOUT_SECONDS,
    )
    await send_text(context, chat_id, sync_note + report)


async def action_analysis_debug(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    owner_args = _owner_cli_args(chat_id)
    sync_note = await sync_operational_state(full=True, owner_chat_id=chat_id)
    report = await run_first_existing_script(
        [
            ["scripts/run_analysis.py", "--no-telegram", "--no-llm", "--no-persist", "--run-intent", "exploratory", *owner_args],
            ["scripts/run_analysis.py", "--no-llm", "--no-persist", "--run-intent", "exploratory", *owner_args],
        ],
        timeout=COMMAND_TIMEOUT_SECONDS,
    )
    await send_text(context, chat_id, sync_note + report)


async def action_market_context(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    report = await run_python_script(
        "scripts/run_market_context.py",
        "--no-telegram",
        "--score-limit",
        "40",
        "--lookback-hours",
        "12",
        *_owner_cli_args(chat_id),
        timeout=300,
    )
    await send_text(context, chat_id, report)


async def action_upcoming_events(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    report = await run_python_script(
        "scripts/run_upcoming_events.py",
        "--no-telegram",
        "--refresh",
        *_owner_cli_args(chat_id),
        timeout=120,
    )
    await send_text(context, chat_id, report)


async def action_ticker_prompt(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    await send_text(
        context,
        chat_id,
        "<b>Análisis por acción</b>\n"
        "Mandame el ticker con este formato:\n\n"
        "<code>/ticker NVDA</code>\n"
        "<code>/tecnico TSM</code>\n\n"
        "Devuelve señal técnica, contexto de cartera/última decisión si existe, "
        "y gráficos PNG. Es read-only: no guarda decision_log y no cambia thresholds.",
    )


def _ticker_chart_paths(output: str, fallback: Path) -> list[Path]:
    paths: list[Path] = []
    seen: set[str] = set()
    for line in (output or "").splitlines():
        if not line.startswith("[chart]"):
            continue
        raw_path = line.removeprefix("[chart]").strip()
        if not raw_path:
            continue
        path = Path(raw_path)
        key = str(path)
        if key in seen:
            continue
        paths.append(path)
        seen.add(key)
    if not paths and fallback.exists():
        paths.append(fallback)
    return paths[:3]


def _ticker_chart_caption(ticker: str, chart_path: Path, index: int, total: int) -> str:
    chart_name = chart_path.stem.lower()
    detail = "momentum, RSI y MACD" if "momentum" in chart_name else "precio, medias y volumen"
    return (
        f"<b>{html_text(ticker)}</b> - gráfico técnico "
        f"{html_text(detail)} ({index}/{total})"
    )


async def action_ticker_analysis(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    ticker: str,
) -> None:
    clean_ticker = re.sub(r"[^A-Za-z0-9.\-]", "", str(ticker or "")).upper()
    if not clean_ticker:
        await action_ticker_prompt(context, chat_id)
        return

    chart_path = Path("/tmp") / f"cocos_ticker_{chat_id}_{clean_ticker}_{int(time.time())}.png"
    rc, out, err, elapsed = await run_cmd(
        [
            sys.executable,
            "scripts/run_ticker_analysis.py",
            clean_ticker,
            "--no-telegram",
            "--chart-out",
            str(chart_path),
            *_owner_cli_args(chat_id),
        ],
        timeout=120,
    )
    if rc != 0:
        await send_text(
            context,
            chat_id,
            (
                f"<b>No pude completar analisis ticker {html_text(clean_ticker)}</b>\n"
                f"Tiempo: <b>{elapsed:.1f}s</b>\n"
                f"<code>{html_text(err[-2200:] or out[-2200:] or 'Sin detalle')}</code>"
            ),
        )
        return

    chart_paths = _ticker_chart_paths(out, chart_path)
    try:
        total = len(chart_paths)
        for index, path in enumerate(chart_paths, start=1):
            if not path.exists():
                continue
            with path.open("rb") as photo:
                await context.bot.send_photo(
                    chat_id=chat_id,
                    photo=photo,
                    caption=_ticker_chart_caption(clean_ticker, path, index, total),
                    parse_mode=ParseMode.HTML,
                )
    finally:
        for path in chart_paths or [chart_path]:
            try:
                path.unlink(missing_ok=True)
            except Exception:
                pass

    report = "\n".join(
        line for line in (out or "").splitlines() if not line.startswith("[chart]")
    ).strip()
    await send_text(context, chat_id, report or f"Sin reporte para {html_text(clean_ticker)}.")


async def action_performance(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    report = await run_python_script(
        "scripts/run_performance.py",
        "--days",
        "90",
        "--no-telegram",
        *_owner_cli_args(chat_id),
        timeout=240,
    )
    await send_text(context, chat_id, report)


async def action_net_decisions(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    stamp = int(time.time())
    csv_path = Path("/tmp") / f"quantia_neto_decisiones_{chat_id}_{stamp}.csv"
    runs_csv_path = Path("/tmp") / f"quantia_neto_analisis_{chat_id}_{stamp}.csv"
    rc, out, err, elapsed = await run_cmd(
        [
            sys.executable,
            "scripts/run_net_decision_report.py",
            "--days",
            "180",
            "--limit-runs",
            "6",
            "--no-telegram",
            "--csv-out",
            str(csv_path),
            "--runs-csv-out",
            str(runs_csv_path),
            *_owner_cli_args(chat_id),
        ],
        timeout=240,
    )
    if rc != 0:
        await send_text(
            context,
            chat_id,
            (
                "<b>No pude calcular el neto por análisis</b>\n"
                f"Tiempo: <b>{elapsed:.1f}s</b>\n"
                f"<code>{html_text(err[-2200:] or out[-2200:] or 'Sin detalle')}</code>"
            ),
        )
        return

    report = "\n".join(
        line for line in (out or "").splitlines()
        if not line.startswith(("[csv]", "[runs_csv]"))
    ).strip()
    await send_text(context, chat_id, report)
    try:
        if csv_path.exists():
            with csv_path.open("rb") as document:
                await context.bot.send_document(
                    chat_id=chat_id,
                    document=document,
                    filename="quantia_neto_decisiones_180d.csv",
                    caption="Detalle completo: una fila por decisión, con bruto, costo y neto 5/10/20/40r.",
                )
        if runs_csv_path.exists():
            with runs_csv_path.open("rb") as document:
                await context.bot.send_document(
                    chat_id=chat_id,
                    document=document,
                    filename="quantia_neto_analisis_180d.csv",
                    caption="Resumen completo: una fila por corrida de análisis con resultado agregado.",
                )
    finally:
        for path in (csv_path, runs_csv_path):
            try:
                path.unlink(missing_ok=True)
            except Exception:
                pass


async def action_viability(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    chart_path = Path("/tmp") / f"cocos_viability_{chat_id}_{int(time.time())}.png"
    rc, out, err, elapsed = await run_cmd(
        [
            sys.executable,
            "scripts/run_viability_audit.py",
            "--days",
            "180",
            "--no-telegram",
            "--chart-out",
            str(chart_path),
        ],
        timeout=240,
    )
    if rc != 0:
        await send_text(
            context,
            chat_id,
            (
                "⚠️ <b>No pude completar viability audit</b>\n"
                f"Tiempo: <b>{elapsed:.1f}s</b>\n"
                f"<code>{html_text(err[-2200:] or out[-2200:] or 'Sin detalle')}</code>"
            ),
        )
        return

    try:
        if chart_path.exists():
            with chart_path.open("rb") as photo:
                await context.bot.send_photo(
                    chat_id=chat_id,
                    photo=photo,
                    caption="<b>Viability Audit 180d</b>",
                    parse_mode=ParseMode.HTML,
                )
    finally:
        try:
            chart_path.unlink(missing_ok=True)
        except Exception:
            pass

    report = "\n".join(
        line
        for line in (out or "").splitlines()
        if not line.startswith("[chart]")
    ).strip()
    await send_text(context, chat_id, report)


async def action_override_audit(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    report = await run_python_script(
        "scripts/run_override_audit.py",
        "--days",
        "90",
        "--no-telegram",
        *_owner_cli_args(chat_id),
        timeout=240,
    )
    await send_text(context, chat_id, report)


async def action_decision_ledger(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    report = await run_python_script(
        "scripts/run_decision_ledger.py",
        "--days",
        "90",
        "--no-telegram",
        *_owner_cli_args(chat_id),
        timeout=240,
    )
    await send_text(context, chat_id, report)


async def action_policy_tree(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    report = await run_python_script(
        "scripts/run_policy_tree.py",
        "--days",
        "30",
        "--limit",
        "8",
        "--no-telegram",
        *_owner_cli_args(chat_id),
        timeout=120,
    )
    await send_text(context, chat_id, report)


async def action_confidence_audit(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    report = await run_python_script(
        "scripts/run_confidence_audit.py",
        "--days",
        "180",
        "--no-telegram",
        timeout=240,
    )
    await send_text(context, chat_id, report)


async def action_calibration(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    report = await run_python_script(
        "scripts/run_calibration.py",
        "--days",
        "180",
        "--no-telegram",
        *_owner_cli_args(chat_id),
        timeout=240,
    )
    await send_text(context, chat_id, report)


async def action_radar_metrics(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
) -> None:
    report = await run_python_script(
        "scripts/run_radar_metrics.py",
        *_owner_cli_args(chat_id),
        timeout=120,
    )
    await send_text(context, chat_id, report)


def _short_radar_reason(value: str) -> str:
    text = str(value or "").strip().strip(".")
    text = re.sub(r"^Revalidar al abrir:\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(
        r"\. No ejecutar ahora con mercado cerrado\.?$",
        "",
        text,
        flags=re.IGNORECASE,
    )
    replacements = (
        (r"convicción (\d+)% < umbral", r"convicción todavía baja (\1%)"),
        (r"sin cash ejecutable", "no hay efectivo disponible"),
        (r"requiere funding o swap", "requiere fondeo o swap"),
        (r"\bfunding\b", "fondeo"),
        (r"score [+-]?\d+(?:\.\d+)? < umbral operativo", "señal debajo del mínimo operativo"),
        (r"no supera a cartera actual \(edge [^)]+\)", "no mejora la cartera actual"),
    )
    for pattern, replacement in replacements:
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
    return text.strip().strip(".")


def _radar_report_items(text: str) -> list[dict]:
    items: list[dict] = []
    compact_pattern = re.compile(
        r"(?m)^\s{2}(?P<ticker>[A-Z0-9.\-]+):\s+"
        r"score\s+(?P<score>[+-]?\d+(?:\.\d+)?)\s+\|\s+"
        r"conv\.\s+(?P<conviction>\d+)%\s+\|\s+"
        r"R/R\s+(?P<rr>[0-9.]+x|⚠️)\s+\|\s+"
        r"\$(?P<price>[0-9.]+)"
        r"(?:\s+\|\s+edge\s+(?P<edge>[+-]?\d+(?:\.\d+)?))?"
    )
    matches = list(compact_pattern.finditer(text))
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        block = text[match.start():end]
        buy_v3 = re.search(
            r"Compra técnica V3:\s*([^\s·]+)\s*·\s*([^·\n]+)",
            block,
        )
        shadow = re.search(r"🔬 Shadow:\s*([A-ZÁÉÍÓÚÑ ]+)\s*—\s*(.+)", block)
        reversion = re.search(r"↩️ Reversión:\s*(.+)", block)
        prior_radar = re.search(r"📊 Última idea radar\s*(.+)", block)
        why_not = re.search(r"⏸ Por qué no entra:\s*(.+)", block)
        action = re.search(
            r"🎯 (?:Lectura radar|Acción|Revalidar|Revalidación requerida):\s*(.+)",
            block,
        )
        relative_strength = re.search(r"└\s*(RS[^\n]+)", block)
        rr_value = match.group("rr").removesuffix("x")
        items.append({
            "ticker": match.group("ticker"),
            "score": match.group("score"),
            "rr": rr_value,
            "edge": match.group("edge") or "—",
            "conviction": match.group("conviction"),
            "action": _short_radar_reason(action.group(1)) if action else "Revisar al abrir",
            "why_not": _short_radar_reason(why_not.group(1)) if why_not else "",
            "shadow_label": shadow.group(1).strip() if shadow else "",
            "buy_v3_tier": buy_v3.group(1).strip().upper() if buy_v3 else "-",
            "buy_v3_classification": buy_v3.group(2).strip() if buy_v3 else "sin clasificación",
            "reversion": reversion.group(1).strip() if reversion else "",
            "prior_radar": prior_radar.group(1).strip() if prior_radar else "",
            "relative_strength": relative_strength.group(1).strip() if relative_strength else "",
        })
    compact_tickers = {item["ticker"] for item in items}
    ticker_blocks = re.split(r"\n(?=━━\s+[A-Z0-9.-]+\s+━━)", text)
    for block in ticker_blocks:
        title = re.search(r"━━\s+([A-Z0-9.-]+)\s+━━\s*(.*)", block)
        if not title:
            continue
        ticker = title.group(1).strip()
        if ticker in compact_tickers:
            continue
        title_tail = title.group(2).strip()
        score = re.search(r"Score:\s*([+-]?\d+\.\d+)", block)
        rr = re.search(r"R/R\s*([0-9.]+)x", block)
        edge = re.search(r"Edge:\s*[🟢🟡🟠🔴]?\s*([+-]?\d+\.\d+)", block)
        shadow = re.search(r"🔬 Shadow:\s*([A-ZÁÉÍÓÚÑ ]+)\s*—\s*(.+)", block)
        buy_v3 = re.search(
            r"Compra técnica V3:\s*([^\s·<]+)\s*·\s*([^·\n]+)",
            block,
        )
        reversion = re.search(r"↩️ Reversión:\s*(.+)", block)
        prior_radar = re.search(r"📊 Última idea radar\s*(.+)", block)
        action = re.search(
            r"🎯 (?:Lectura radar|Acción sugerida|Revalidación requerida):\s*(.+)",
            block,
        )
        compete = re.search(r"Compite con:\s*([A-Z0-9.-]+)", block)
        action_text = _short_radar_reason(action.group(1)) if action else "Revisar setup"
        if compete and "vs " not in action_text.lower():
            action_text += f" frente a {compete.group(1)}"
        classification = "swap" if "SWAP" in title_tail.upper() else "idea técnica"
        items.append({
            "ticker": ticker,
            "score": score.group(1) if score else "—",
            "rr": rr.group(1) if rr else "—",
            "edge": edge.group(1) if edge else "—",
            "conviction": "—",
            "action": action_text,
            "why_not": "",
            "shadow_label": shadow.group(1).strip() if shadow else "",
            "buy_v3_tier": buy_v3.group(1).strip().upper() if buy_v3 else "-",
            "buy_v3_classification": buy_v3.group(2).strip() if buy_v3 else classification,
            "reversion": reversion.group(1).strip() if reversion else "",
            "prior_radar": prior_radar.group(1).strip() if prior_radar else "",
            "relative_strength": "",
        })
    return items


def compact_radar_report(
    report: str,
    max_items: int = 5,
    *,
    detailed: bool = False,
) -> str:
    """Resume el reporte técnico sin recalcular ranking ni señales."""
    if not report:
        return "⚠️ Radar sin output."

    event_prefix: list[str] = []
    raw_lines = report.splitlines()
    for idx, line in enumerate(raw_lines):
        plain_line = re.sub(r"<[^>]+>", "", line)
        if (
            "Eventos/catalysts manuales activos" in plain_line
            or "Evento manual activo" in plain_line
        ):
            j = idx
            while j < len(raw_lines) and raw_lines[j].strip():
                event_prefix.append(raw_lines[j])
                j += 1
            break

    text = re.sub(r"</?(?:b|i|code)>", "", report)
    text = text.replace("&gt;", ">").replace("&lt;", "<").replace("&amp;", "&")
    items = _radar_report_items(text)
    if not items:
        return report

    universe_match = re.search(r"^🔍 Universo:\s*(.+)$", text, re.MULTILINE)
    gate_match = re.search(
        r"^(?:✅|⚠️|🔴|⚪)\s*(?:Gate|Estado operativo):\s*(.+)$",
        text,
        re.MULTILINE,
    )
    cash_match = re.search(r"^💵 Cash libre:\s*(.+)$", text, re.MULTILINE)
    universe_text = universe_match.group(1).strip() if universe_match else "universo disponible"
    universe_count = re.search(r"(\d+)\s+tickers", universe_text)
    gate = gate_match.group(1).strip() if gate_match else "—"
    cash = cash_match.group(1).strip() if cash_match else "—"
    market_open_now = (
        _is_business_day_now()
        and _is_market_hours_now()
        and _market_closed_reason_now() is None
    )
    market_closed = "Mercado cerrado/sin rueda" in text or not market_open_now
    only_watch = (
        "En vigilancia" in text
        and "Comprable ahora" not in text
        and "Compra habilitada" not in text
        and "Candidato para próxima rueda" not in text
    )

    def _priority(item: dict) -> tuple[int, float]:
        tier_order = {"A": 0, "B": 1, "C": 2, "-": 3, "REJECTED": 4}
        try:
            score_value = float(item["score"])
        except (TypeError, ValueError):
            score_value = -999.0
        return tier_order.get(item["buy_v3_tier"], 3), -score_value

    top = sorted(items, key=_priority)[:max(int(max_items), 1)]
    universe_label = (
        f"{universe_count.group(1)} analizados"
        if universe_count
        else universe_text
    )
    title_mode = "Radar detallado" if detailed else "Radar"
    title = (
        f"🔭 <b>{title_mode} · próxima apertura</b>"
        if market_closed
        else f"🔭 <b>{title_mode} · ahora</b>"
    )
    lines: list[str] = []
    if event_prefix:
        lines.extend(event_prefix)
        lines.append("")
    lines.extend([
        title,
        f"{universe_label} · {len(items)} ideas detectadas · estado <b>{gate}</b>",
        f"Cash disponible: <b>{cash}</b>",
    ])
    if only_watch:
        lines.append("<b>Resultado:</b> no hay compras habilitadas; son ideas para seguimiento.")
    elif market_closed:
        lines.append("<b>Resultado:</b> revalidar precio y liquidez cuando abra el mercado.")
    else:
        lines.append("<b>Resultado:</b> el Radar detecta; /analisis decide si entra al plan.")
    lines.extend([
        "<i>Origen: top del ranking operativo vigente.</i>",
        "<i>Lectura V3 shadow: reordena sólo esta vista; no elige el top ni habilita compras.</i>",
        "<i>V3 A = prioridad técnica. R/R = retorno potencial por unidad de riesgo; no es probabilidad.</i>",
        "",
        "<b>Prioridad técnica V3 dentro del top operativo</b>",
    ])

    rejected_section_started = False
    for index, item in enumerate(top, start=1):
        tier = item["buy_v3_tier"]
        rejected = tier == "REJECTED"
        if rejected and not rejected_section_started:
            lines.extend(["<b>No priorizar ahora</b>", ""])
            rejected_section_started = True
        icon = "⛔" if rejected else "🟡" if tier == "A" else "👁"
        tier_label = "rechazada" if rejected else tier
        classification = item["buy_v3_classification"]
        lines.append(
            f"{index}. {icon} <b>{item['ticker']}</b> · "
            f"V3 <b>{tier_label}</b> ({classification})"
        )
        rr_display = (
            f"{item['rr']}x"
            if item["rr"] not in {"—", "⚠️", "n/d"}
            else "n/d"
        )
        lines.append(
            f"   Score operativo <code>{item['score']}</code> · "
            f"R/R <code>{rr_display}</code>"
        )

        positives: list[str] = []
        concerns: list[str] = []
        if item["relative_strength"]:
            positives.append(item["relative_strength"])
        if item["why_not"]:
            concerns.append(item["why_not"])
        reversion = item["reversion"]
        if "rebote alcista" in reversion.lower():
            positives.append("posible rebote técnico")
        elif "corrección bajista" in reversion.lower():
            concerns.append("posible corrección de corto plazo")
        if item["shadow_label"] and "DÉBIL" in item["shadow_label"].upper():
            concerns.append("confirmación shadow débil")
        if positives and not rejected:
            lines.append(f"   A favor: {' · '.join(positives[:2])}")
        if concerns:
            label = "Motivo" if rejected else "Falta"
            lines.append(f"   {label}: {'; '.join(concerns[:2])}")
        if not rejected:
            lines.append(f"   Próximo paso: {item['action']}")
        lines.append("")

    lines.append(
        "<i>Seguir registra interés; no compra ni altera las métricas oficiales.</i>"
    )
    if detailed:
        lines.extend([
            "<i>Para profundizar una idea: /ticker TICKER.</i>",
            "<i>La evidencia acumulada del ranking está en /radar_metricas.</i>",
        ])
    else:
        lines.append(
            "<i>/radar_full compara todo el top; /ticker profundiza un activo.</i>"
        )
    return "\n".join(lines)


def _radar_cache_supports_reversion(report: str) -> bool:
    text = str(report or "")
    return "↩️ Reversión:" in text and "Compra técnica V3:" in text


async def _persist_radar_exploratory(
    report: str,
    *,
    chat_id: int,
) -> dict | None:
    if (
        not RADAR_MANUAL_EXPLORATORY_ENABLED
        or not get_config
        or not PortfolioDatabase
    ):
        return None
    from src.analysis.radar_discovery import discovery_scoring_version
    from src.analysis.radar_exploratory import (
        RadarExploratoryStore,
        parse_compact_radar_candidates,
    )

    candidates = parse_compact_radar_candidates(report)
    if not candidates:
        logger.warning("[BOT][RADAR] no pude extraer candidatos para auditoría exploratoria")
        return None

    cfg = get_config()
    db = PortfolioDatabase(cfg.database.url)
    await db.connect()
    try:
        store = RadarExploratoryStore(await db.get_pool())
        return await store.save_report(
            owner_chat_id=chat_id,
            report_text=report,
            scoring_version=discovery_scoring_version(
                period="1y",
                min_score=0.10,
                min_rr=0.0,
                operational_top_n=6,
                no_sentiment=False,
            ),
            candidates=candidates,
            metadata={
                "source": "telegram_manual",
                "run_intent": "exploratory",
                "metric_scope": "exploratory",
                "is_primary_metric": False,
                "affects_radar_metrics": False,
                "affects_analysis": False,
                "affects_execution": False,
            },
        )
    finally:
        await db.close()


def _radar_exploratory_keyboard(saved: dict | None) -> InlineKeyboardMarkup | None:
    if not saved:
        return None
    from src.analysis.radar_exploratory import exploratory_callback

    rows = []
    for candidate in saved.get("candidates") or []:
        if candidate.get("user_action"):
            continue
        candidate_id = int(candidate["id"])
        ticker = str(candidate.get("ticker") or "N/D").upper()
        tier = str(candidate.get("v3_tier") or "").upper()
        if tier == "REJECTED":
            continue
        label = f"Seguir {ticker}" + (f" · {tier}" if tier else "")
        rows.append([
            InlineKeyboardButton(
                label,
                callback_data=exploratory_callback("follow", candidate_id),
            ),
            InlineKeyboardButton(
                "Descartar",
                callback_data=exploratory_callback("dismiss", candidate_id),
            ),
        ])
    return InlineKeyboardMarkup(rows) if rows else None


async def _send_manual_radar(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    report: str,
    *,
    prefix: str = "",
) -> None:
    try:
        saved = await _persist_radar_exploratory(report, chat_id=chat_id)
    except Exception:
        saved = None
        logger.exception(
            "[BOT][RADAR] falló la persistencia exploratoria; envío el reporte igual"
        )
    keyboard = _radar_exploratory_keyboard(saved)
    audit_note = ""
    if saved:
        audit_note = (
            "\n\n<i>Seguir registra interés exploratorio antes de operar; "
            "no crea órdenes ni entra en las métricas oficiales del Radar.</i>"
        )
        logger.info(
            "[BOT][RADAR] exploratory_run=%s inserted=%s candidates=%s",
            saved.get("run_id"),
            saved.get("inserted"),
            len(saved.get("candidates") or []),
        )
    await send_text(
        context,
        chat_id,
        prefix + report + audit_note,
        reply_markup=keyboard,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Acción: Radar
# ─────────────────────────────────────────────────────────────────────────────

async def action_radar(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    started = time.perf_counter()
    cached = await _load_cached_report("radar", chat_id)
    if cached and not _radar_cache_supports_reversion(str(cached.get("report_text") or "")):
        logger.info("[BOT][RADAR] cache_schema_miss=reversion_or_buy_v3")
        cached = None
    if cached:
        report = compact_radar_report(str(cached["report_text"]), max_items=5)
        logger.info(
            "[BOT][RADAR] cache_hit=true total_s=%.2f",
            time.perf_counter() - started,
        )
        await _send_manual_radar(
            context,
            chat_id,
            report,
            prefix=_artifact_note(cached, "/radar_full"),
        )
        return

    report = await run_first_existing_script(
        [
            [
                "scripts/run_opportunity.py",
                "--no-telegram",
                "--period",
                "1y",
                "--top",
                "6",
                "--min-score",
                "0.10",
                "--no-persist",
                *_owner_cli_args(chat_id),
            ],
        ],
        timeout=180,
    )

    if not report or not report.strip():
        report = (
            "⚠️ Radar sin output.\n"
            "Runner esperado:\n"
            "<code>scripts/run_opportunity.py --no-telegram --period 1y --top 6 --min-score 0.10 --no-persist</code>"
        )
    else:
        await _save_cached_report("radar", chat_id, report)
        report = compact_radar_report(report, max_items=5)

    logger.info(
        "[BOT][RADAR] cache_hit=false total_s=%.2f",
        time.perf_counter() - started,
    )

    await _send_manual_radar(context, chat_id, report)


async def action_shadow(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    ticker: str | None = None,
) -> None:
    args = [
        "--latest-report",
        "--telegram-format",
        *_owner_cli_args(chat_id),
    ]
    if ticker:
        args.extend(["--tickers", str(ticker).upper()])
    report = await run_python_script(
        "scripts/run_thesis_shadow.py",
        *args,
        timeout=90,
    )
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
                "--period",
                "1y",
                "--max",
                "8",
                "--no-persist",
                *_owner_cli_args(chat_id),
            ],
        ],
        timeout=COMMAND_TIMEOUT_SECONDS,
    )

    await _send_manual_radar(
        context,
        chat_id,
        compact_radar_report(report, max_items=8, detailed=True),
    )


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
    db = None
    if get_config and PortfolioDatabase:
        try:
            cfg = get_config()
            db  = PortfolioDatabase(cfg.database.url)
            await db.connect()
            snap = await db.get_latest_snapshot()

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
        if db:
            pool = None
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
    except Exception as e:
        lines.append(f"⚠️ No pude leer market_prices: <code>{e}</code>")
    finally:
        if db:
            await db.close()

    lines += [
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "<i>Status — Cocos Copilot</i>",
    ]

    await send_text(context, chat_id, "\n".join(lines))


# ─────────────────────────────────────────────────────────────────────────────
# Acción: Configuración
# ─────────────────────────────────────────────────────────────────────────────

async def action_settings(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    *,
    force_reconfigure: bool = False,
) -> None:
    if not _multiuser_enabled():
        await send_text(
            context,
            chat_id,
            "⚙️ <b>CONFIGURACIÓN</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "La configuración multiusuario no está habilitada en este entorno.",
        )
        return

    account_state = "sin credenciales vinculadas"
    key_state = "configurada"
    can_start_setup = True
    has_credentials = False

    if not (PortfolioDatabase and CredentialCipher and UserCredentials and get_config):
        account_state = "estado no disponible"
        key_state = "no disponible"
        can_start_setup = False
    else:
        try:
            cipher = CredentialCipher.from_env()
            cfg = get_config()
            db = PortfolioDatabase(cfg.database.url)
            await db.connect()
            try:
                credentials = await db.get_bot_user_credentials(
                    chat_id=chat_id,
                    cipher=cipher,
                )
            finally:
                await db.close()
            account_state = (
                "credenciales vinculadas"
                if credentials
                else "sin credenciales vinculadas"
            )
            has_credentials = bool(credentials)
        except Exception as exc:
            logger.warning("[BOT] No pude leer configuración de chat_id=%s: %s", chat_id, exc)
            account_state = "estado no disponible"
            key_state = "no disponible"
            can_start_setup = False

    should_start_setup = can_start_setup and (
        force_reconfigure or not has_credentials
    )

    if should_start_setup:
        context.user_data[SETTINGS_STATE_KEY] = SETTINGS_AWAIT_USERNAME
        context.user_data.pop(SETTINGS_USERNAME_KEY, None)
    else:
        _clear_settings_state(context)

    if should_start_setup:
        next_step = (
            "Enviame ahora tu <b>usuario o email de Cocos</b>.\n"
            "Después te voy a pedir la contraseña.\n"
            "Podés cancelar con <code>/cancelar</code>."
        )
    elif has_credentials:
        next_step = (
            "Tu cuenta ya está vinculada.\n"
            "Para reemplazar credenciales, usá <code>/reconfigurar</code>."
        )
    else:
        next_step = "No puedo iniciar el alta hasta corregir la configuración del entorno."

    await send_text(
        context,
        chat_id,
        "⚙️ <b>CONFIGURACIÓN</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 Cuenta Cocos: <b>{account_state}</b>\n"
        f"🔐 Cifrado local: <b>{key_state}</b>\n\n"
        "Este entorno usa cuentas separadas por usuario y no expone secretos en pantalla.\n\n"
        + next_step,
    )


async def action_reconfigure_settings(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
) -> None:
    await action_settings(context, chat_id, force_reconfigure=True)


async def settings_text_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    if not update.message or not update.effective_chat:
        return
    if not await ensure_allowed_chat(update, context):
        return
    if not _multiuser_enabled():
        return

    text = str(update.message.text or "")
    state = context.user_data.get(SETTINGS_STATE_KEY)
    if state not in {SETTINGS_AWAIT_USERNAME, SETTINGS_AWAIT_PASSWORD}:
        if (
            context.user_data.get(PORTFOLIO_SYNC_PENDING_KEY)
            and re.fullmatch(r"\d{6}", text.strip())
            and redis_client is not None
        ):
            await _delete_incoming_message(update)
            try:
                await redis_client.lpush(f"mfa:{update.effective_chat.id}", text.strip())
                await send_text(
                    context,
                    update.effective_chat.id,
                    "🔐 Código MFA recibido. Sigo con la sincronización del portfolio.",
                )
            except Exception as exc:
                logger.warning("[BOT] No pude encolar MFA de chat_id=%s: %s", update.effective_chat.id, exc)
                await send_text(
                    context,
                    update.effective_chat.id,
                    "⚠️ No pude registrar el código MFA. Reintentá en unos segundos.",
                )
        return

    await _delete_incoming_message(update)

    if state == SETTINGS_AWAIT_USERNAME:
        username = text.strip()
        if not username:
            await send_text(
                context,
                update.effective_chat.id,
                "Necesito un usuario o email no vacío. Enviamelo de nuevo o usá <code>/cancelar</code>.",
            )
            return

        context.user_data[SETTINGS_USERNAME_KEY] = username
        context.user_data[SETTINGS_STATE_KEY] = SETTINGS_AWAIT_PASSWORD
        await send_text(
            context,
            update.effective_chat.id,
            "Perfecto. Ahora enviame la <b>contraseña de Cocos</b>.\n"
            "La voy a cifrar antes de guardarla y voy a borrar este mensaje del chat cuando Telegram lo permita.",
        )
        return

    password = text
    if not password:
        await send_text(
            context,
            update.effective_chat.id,
            "La contraseña no puede estar vacía. Enviamela de nuevo o usá <code>/cancelar</code>.",
        )
        return

    username = context.user_data.get(SETTINGS_USERNAME_KEY)
    if not username:
        _clear_settings_state(context)
        await send_text(
            context,
            update.effective_chat.id,
            "Perdí el usuario durante el alta. Abrí <code>/configuracion</code> y lo retomamos desde cero.",
        )
        await send_menu(context, update.effective_chat.id)
        return

    if not (PortfolioDatabase and CredentialCipher and UserCredentials and get_config):
        await send_text(
            context,
            update.effective_chat.id,
            "No puedo guardar credenciales en este entorno ahora mismo.",
        )
        return

    try:
        cipher = CredentialCipher.from_env()
        cfg = get_config()
        db = PortfolioDatabase(cfg.database.url)
        await db.connect()
        try:
            await db.upsert_bot_user_credentials(
                chat_id=update.effective_chat.id,
                credentials=UserCredentials(username=username, password=password),
                cipher=cipher,
                telegram_username=getattr(update.effective_user, "username", None),
                display_name=getattr(update.effective_user, "full_name", None),
            )
        finally:
            await db.close()
    except Exception as exc:
        logger.warning(
            "[BOT] No pude guardar credenciales de chat_id=%s: %s",
            update.effective_chat.id,
            exc,
        )
        await send_text(
            context,
            update.effective_chat.id,
            "No pude guardar la cuenta todavía. Revisá la configuración del sandbox y enviame la contraseña otra vez.",
        )
        return

    _clear_settings_state(context)
    await send_text(
        context,
        update.effective_chat.id,
        "✅ Cuenta Cocos vinculada y guardada cifrada.\n"
        "Desde acá ya podemos usar este chat como identidad separada dentro del sandbox multiusuario.",
    )
    started = await _start_user_portfolio_sync_if_possible(
        context,
        update.effective_chat.id,
        reason="Cuenta recién vinculada.",
    )
    if not started:
        await send_menu(context, update.effective_chat.id)


async def _load_user_credentials(chat_id: int) -> Optional[UserCredentials]:
    if not (PortfolioDatabase and CredentialCipher and get_config):
        return None

    cipher = CredentialCipher.from_env()
    cfg = get_config()
    db = PortfolioDatabase(cfg.database.url)
    await db.connect()
    try:
        return await db.get_bot_user_credentials(chat_id=chat_id, cipher=cipher)
    finally:
        await db.close()


async def _start_user_portfolio_sync_if_possible(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    *,
    reason: str,
) -> bool:
    if not _multiuser_enabled():
        return False
    if context.user_data.get(PORTFOLIO_SYNC_PENDING_KEY):
        await send_text(
            context,
            chat_id,
            "📥 Ya estoy sincronizando tu portfolio inicial.",
        )
        return True

    try:
        credentials = await _load_user_credentials(chat_id)
    except Exception as exc:
        logger.warning("[BOT] No pude leer credenciales para sync chat_id=%s: %s", chat_id, exc)
        credentials = None

    if not credentials:
        await send_text(
            context,
            chat_id,
            "⚠️ Todavía no hay credenciales vinculadas para traer tu portfolio.\n"
            "Abrí <code>/configuracion</code> para cargarlas.",
        )
        return False

    if not getattr(context, "application", None):
        await send_text(
            context,
            chat_id,
            "⚠️ No pude iniciar la sincronización automática en este contexto.",
        )
        return False

    context.user_data[PORTFOLIO_SYNC_PENDING_KEY] = True
    await send_text(
        context,
        chat_id,
        "📥 Voy a traer tu portfolio inicial ahora.\n"
        f"<i>{reason}</i>\n"
        "Esto sí corre aunque sea fin de semana, porque solo consulta tu cartera privada.\n"
        "Si Cocos pide MFA, te voy a pedir el código de 6 dígitos por este chat.",
    )
    context.application.create_task(
        _sync_user_portfolio_once(context, chat_id, credentials)
    )
    return True


async def _sync_user_portfolio_once(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    credentials: UserCredentials,
) -> None:
    try:
        if not (
            get_config
            and PortfolioDatabase
            and CocosCapitalScraper
            and cache_portfolio_snapshot
        ):
            raise RuntimeError("dependencias de scrape no disponibles")

        cfg = get_config()
        user_scraper_cfg = replace(
            cfg.scraper,
            username=credentials.username,
            password=credentials.password,
            telegram_chat_id=str(chat_id),
            telegram_enabled=bool(cfg.scraper.telegram_bot_token),
            telegram_mfa_prompt_enabled=True,
            session_file=_user_session_file(chat_id),
        )

        db = PortfolioDatabase(cfg.database.url)
        await db.connect()
        try:
            async with CocosCapitalScraper(user_scraper_cfg) as scraper:
                await scraper.login()
                snapshot = await scraper.scrape_portfolio()

            snapshot.owner_chat_id = chat_id
            await db.save_snapshot(snapshot)
            await cache_portfolio_snapshot(
                snapshot.to_dict(),
                owner_chat_id=chat_id,
            )
        finally:
            await db.close()

        await send_text(
            context,
            chat_id,
            "✅ Portfolio inicial sincronizado.",
        )
        await action_portfolio(context, chat_id, refresh=False)
    except Exception as exc:
        logger.exception("[BOT] Sync inicial de portfolio falló para chat_id=%s", chat_id)
        await send_text(
            context,
            chat_id,
            "❌ No pude sincronizar tu portfolio inicial.\n"
            f"<code>{exc}</code>",
        )
    finally:
        context.user_data.pop(PORTFOLIO_SYNC_PENDING_KEY, None)
        await send_menu(context, chat_id)


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
    sync_note = await sync_operational_state(
        full=False,
        owner_chat_id=chat_id,
        force=True,
    )
    if sync_note:
        await send_text(context, chat_id, sync_note)
    await action_portfolio(context, chat_id, refresh=False)


# ─────────────────────────────────────────────────────────────────────────────
# Router de callbacks
# ─────────────────────────────────────────────────────────────────────────────

CALLBACK_ALIASES: dict[str, str] = {
    # Navegación
    "menu_home":        "menu_home",
    "menu_results":     "menu_results",
    "menu_audit":       "menu_audit",
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
    "analysis_test":    "analysis_test",
    "analisis_test":    "analysis_test",
    "run_analysis_test":"analysis_test",
    "analysis_full":    "analysis_full",
    "analisis_full":    "analysis_full",
    "run_analysis_full":"analysis_full",
    "analysis_debug":   "analysis_debug",
    "analisis_debug":   "analysis_debug",
    "run_analysis_debug":"analysis_debug",
    "market_context":   "market_context",
    "mercado":          "market_context",
    "noticias":         "market_context",
    "contexto":         "market_context",
    "upcoming_events":  "upcoming_events",
    "ticker_analysis":  "ticker_analysis",
    "ticker":           "ticker_analysis",
    "tecnico":          "ticker_analysis",
    "accion":           "ticker_analysis",
    # Resumen semanal
    "weekly_summary":   "weekly_summary",
    "summary":          "weekly_summary",
    "resumen":          "weekly_summary",
    "resumen_semanal":  "weekly_summary",
    # Performance
    "performance":      "performance",
    "perf":             "performance",
    "run_performance":  "performance",
    "net_decisions":    "net_decisions",
    "neto":             "net_decisions",
    "resultado_neto":   "net_decisions",
    "viability":        "viability",
    "viabilidad":       "viability",
    "bot_only":         "viability",
    # Bot vs Humano
    "override_audit":   "override_audit",
    "overrides":        "override_audit",
    "override":         "override_audit",
    "bot_vs_humano":    "override_audit",
    # Decision Ledger
    "decision_ledger":  "decision_ledger",
    "ledger":           "decision_ledger",
    "atribucion":       "decision_ledger",
    # Policy Tree
    "policy_tree":      "policy_tree",
    "policy":           "policy_tree",
    "decision_tree":    "policy_tree",
    "arbol":            "policy_tree",
    # Confianza operativa
    "confidence":       "confidence_audit",
    "confianza":        "confidence_audit",
    "trust":            "confidence_audit",
    "audit":            "confidence_audit",
    "confidence_audit": "confidence_audit",
    # Decision Calibration Layer
    "calibration":      "calibration",
    "calibracion":      "calibration",
    "dcl":              "calibration",
    # Radar
    "radar":            "radar",
    "opportunities":    "radar",
    "opportunity_radar":"radar",
    "oportunidades":    "radar",
    "radar_metrics":    "radar_metrics",
    "radar_metricas":   "radar_metrics",
    "metricas_radar":   "radar_metrics",
    # Tesis shadow
    "shadow":           "shadow",
    "thesis_shadow":    "shadow",
    "tesis":            "shadow",
    # Regression
    "regression":       "regression_audit",
    "regression_audit": "regression_audit",
    "regression_opt":   "regression_audit",
    # Status
    "status":           "status",
    "health":           "status",
    # Configuración
    "settings":         "settings",
    "config":           "settings",
    "configuracion":    "settings",
}

ACTION_LOADING_TEXT: dict[str, str] = {
    "calibration":   "DCL: auditando decisiones y outcomes...",
    "analysis_test": "Probando analisis sin guardar...",
    "analysis_debug": "Generando diagnostico sin guardar...",
    "market_context": "Revisando mercado y noticias...",
    "upcoming_events": "Leyendo proximos balances...",
    "ticker_analysis": "Preparando analisis por accion...",
    "portfolio":     "💼 Leyendo último portfolio...",
    "analysis":      "🧠 Generando plan de cartera...",
    "weekly_summary":"📅 Generando resumen semanal...",
    "performance":   "📊 Calculando performance y outcomes...",
    "net_decisions": "Calculando neto por corrida y decisión...",
    "viability":     "Auditando bot-only, seguido y manual...",
    "override_audit": "Comparando planes del bot contra movimientos reales...",
    "decision_ledger": "Calculando atribución económica...",
    "policy_tree":   "Construyendo árbol operativo...",
    "confidence_audit": "🧭 Auditando confianza del sistema...",
    "radar":         "🔭 Generando radar de oportunidades...",
    "radar_full":    "🔭 Generando radar completo...",
    "radar_metrics": "📐 Leyendo métricas prospectivas del Radar...",
    "shadow":        "🔬 Leyendo la última tesis shadow...",
    "regression_audit": "📈 Ejecutando auditoría de regresión...",
    "status":        "🩺 Verificando estado del sistema...",
    "settings":      "⚙️ Abriendo configuración...",
    "settings_reconfigure": "⚙️ Preparando reconfiguración...",
}


async def run_action(action: str, context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    dispatch = {
        "portfolio":      action_portfolio,
        "analysis":       action_analysis,
        "analysis_test":  action_analysis_test,
        "analysis_full":  action_analysis_full,
        "analysis_debug": action_analysis_debug,
        "market_context": action_market_context,
        "upcoming_events": action_upcoming_events,
        "ticker_analysis": action_ticker_prompt,
        "weekly_summary": action_weekly_summary,
        "performance":    action_performance,
        "net_decisions":  action_net_decisions,
        "viability":      action_viability,
        "override_audit": action_override_audit,
        "decision_ledger": action_decision_ledger,
        "policy_tree":    action_policy_tree,
        "confidence_audit": action_confidence_audit,
        "calibration":    action_calibration,
        "radar":          action_radar,
        "radar_full": action_radar_full,
        "radar_metrics": action_radar_metrics,
        "shadow":         action_shadow,
        "regression_audit": action_regression_audit,
        "status":         action_status,
        "settings":       action_settings,
        "settings_reconfigure": action_reconfigure_settings,
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
    if not await ensure_allowed_chat(update, context):
        return
    await update.message.reply_text(
        menu_text(),
        parse_mode=ParseMode.HTML,
        reply_markup=main_keyboard(),
        disable_web_page_preview=True,
    )


async def menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await start_handler(update, context)


async def help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_allowed_chat(update, context):
        return
    await update.message.reply_text(
        help_text(),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


async def _dispatch_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    action: str,
) -> None:
    if not await ensure_allowed_chat(update, context):
        return
    chat_id = update.effective_chat.id
    loading = ACTION_LOADING_TEXT.get(action, "🔄 Procesando...")
    if action not in FAST_ACTIONS:
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
        if action not in NO_AUTO_MENU_ACTIONS:
            await send_menu(context, chat_id)


async def portfolio_handler(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    await _dispatch_command(u, c, "portfolio")

async def analysis_handler(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    await _dispatch_command(u, c, "analysis")

async def analysis_test_handler(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    await _dispatch_command(u, c, "analysis_test")

async def analysis_full_handler(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    await _dispatch_command(u, c, "analysis_full")

async def analysis_debug_handler(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    await _dispatch_command(u, c, "analysis_debug")

async def market_context_handler(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    await _dispatch_command(u, c, "market_context")


async def upcoming_events_handler(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    await _dispatch_command(u, c, "upcoming_events")

async def ticker_handler(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    args = list(getattr(c, "args", None) or [])
    if not args:
        await _dispatch_command(u, c, "ticker_analysis")
        return
    if not await ensure_allowed_chat(u, c):
        return
    chat_id = u.effective_chat.id
    ticker = re.sub(r"[^A-Za-z0-9.\-]", "", str(args[0])).upper()
    if not ticker:
        await send_text(c, chat_id, "Uso: <code>/ticker NVDA</code>")
        return
    await answer_loading(u, f"Analizando {html_text(ticker)}...")
    t0 = time.time()
    logger.info("[BOT] action=ticker_analysis ticker=%s chat_id=%s", ticker, chat_id)
    try:
        await action_ticker_analysis(c, chat_id, ticker)
        logger.info("[BOT] action=ticker_analysis OK en %.2fs", time.time() - t0)
    except Exception as e:
        logger.exception("[BOT] action=ticker_analysis fallo")
        await send_text(c, chat_id, f"Error en <b>ticker {html_text(ticker)}</b>:\n<code>{html_text(e)}</code>")
    finally:
        await send_menu(c, chat_id)

async def weekly_summary_handler(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    await _dispatch_command(u, c, "weekly_summary")

async def performance_handler(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    await _dispatch_command(u, c, "performance")


async def net_decisions_handler(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    await _dispatch_command(u, c, "net_decisions")


async def viability_handler(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    await _dispatch_command(u, c, "viability")


async def override_audit_handler(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    await _dispatch_command(u, c, "override_audit")


async def decision_ledger_handler(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    await _dispatch_command(u, c, "decision_ledger")


async def policy_tree_handler(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    await _dispatch_command(u, c, "policy_tree")


async def confidence_handler(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    await _dispatch_command(u, c, "confidence_audit")


async def calibration_handler(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    await _dispatch_command(u, c, "calibration")

async def radar_handler(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    await _dispatch_command(u, c, "radar")

async def radar_metrics_handler(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    await _dispatch_command(u, c, "radar_metrics")

async def shadow_handler(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    args = list(getattr(c, "args", None) or [])
    if not args:
        await _dispatch_command(u, c, "shadow")
        return
    if not await ensure_allowed_chat(u, c):
        return
    chat_id = u.effective_chat.id
    ticker = re.sub(r"[^A-Za-z0-9.\-]", "", str(args[0])).upper()
    if not ticker:
        await send_text(c, chat_id, "Uso: <code>/shadow AMD</code>")
        return
    await answer_loading(u, f"🔬 Leyendo shadow de {ticker}...")
    t0 = time.time()
    logger.info("[BOT] action=shadow_ticker ticker=%s chat_id=%s", ticker, chat_id)
    try:
        await action_shadow(c, chat_id, ticker=ticker)
        logger.info("[BOT] action=shadow_ticker OK en %.2fs", time.time() - t0)
    except Exception as e:
        logger.exception("[BOT] action=shadow_ticker falló")
        await send_text(c, chat_id, f"❌ Error en <b>shadow {ticker}</b>:\n<code>{e}</code>")
    finally:
        await send_menu(c, chat_id)

async def status_handler(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    await _dispatch_command(u, c, "status")

async def settings_handler(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    await _dispatch_command(u, c, "settings")


async def reconfigure_settings_handler(
    u: Update,
    c: ContextTypes.DEFAULT_TYPE,
) -> None:
    await _dispatch_command(u, c, "settings_reconfigure")


async def cancel_settings_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    if not update.effective_chat:
        return
    if not await ensure_allowed_chat(update, context):
        return
    _clear_settings_state(context)
    await send_text(context, update.effective_chat.id, "Configuración cancelada.")
    await send_menu(context, update.effective_chat.id)

async def regression_audit_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    if not await ensure_allowed_chat(update, context):
        return
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
    if not await ensure_allowed_chat(update, context):
        return
    chat_id = update.effective_chat.id
    await answer_loading(update, "⚙️ Iniciando scraping en modo admin...")
    await action_admin_scrape(context, chat_id)
    await send_menu(context, chat_id)


async def admin_refresh_portfolio_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_allowed_chat(update, context):
        return
    chat_id = update.effective_chat.id
    await answer_loading(update, "🔄 Refrescando portfolio en modo admin...")
    await action_admin_refresh_portfolio(context, chat_id)
    await send_menu(context, chat_id)


async def _handle_radar_exploratory_callback(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    raw_action: str,
    chat_id: int,
) -> None:
    query = update.callback_query
    if query is None or not get_config or not PortfolioDatabase:
        return
    from src.analysis.radar_exploratory import (
        RadarExploratoryStore,
        USER_ACTION_DISMISS,
        USER_ACTION_FOLLOW,
        parse_exploratory_callback,
    )

    parsed = parse_exploratory_callback(raw_action)
    if parsed is None:
        await query.answer("Acción exploratoria inválida", show_alert=True)
        return
    action, candidate_id = parsed
    await query.answer("Registrando elección...")

    cfg = get_config()
    db = PortfolioDatabase(cfg.database.url)
    await db.connect()
    try:
        store = RadarExploratoryStore(await db.get_pool())
        candidate = await store.record_user_action(
            candidate_id,
            owner_chat_id=chat_id,
            action=(
                USER_ACTION_FOLLOW
                if action == "follow"
                else USER_ACTION_DISMISS
            ),
        )
    finally:
        await db.close()

    if candidate is None:
        await send_text(context, chat_id, "La idea Radar no existe o no pertenece a este chat.")
        return
    ticker = re.sub(
        r"[^A-Za-z0-9.\-]",
        "",
        str(candidate.get("ticker") or ""),
    ).upper()
    if candidate.get("action_conflict"):
        original = str(candidate.get("user_action") or "").upper()
        original_label = "seguir" if original == USER_ACTION_FOLLOW else "descartar"
        await send_text(
            context,
            chat_id,
            f"<b>{html_text(ticker)}</b>: ya habías elegido {original_label}.\n"
            "La primera respuesta queda congelada para conservar la auditoría.",
        )
        return
    if not candidate.get("action_recorded", True):
        await send_text(
            context,
            chat_id,
            f"<b>{html_text(ticker)}</b>: esa elección ya estaba registrada.",
        )
        return
    if action == "follow":
        await send_text(
            context,
            chat_id,
            f"👁 <b>{html_text(ticker)}</b>: interés Radar registrado.\n"
            "No crea una orden. Una compra posterior podrá vincularse como ejecución seguida, "
            "fuera de las métricas prospectivas oficiales.",
        )
        return
    await send_text(
        context,
        chat_id,
        f"<b>{html_text(ticker)}</b>: idea descartada para tu seguimiento.\n"
        "El reporte original queda conservado como evidencia exploratoria.",
    )


async def _handle_radar_setup_callback(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    raw_action: str,
    chat_id: int,
) -> None:
    query = update.callback_query
    if query is None or not get_config or not PortfolioDatabase:
        return
    from src.analysis.radar_setup_alerts import (
        RadarSetupAlertStore,
        USER_ACTION_DISMISS,
        USER_ACTION_FOLLOW,
        parse_radar_setup_callback,
    )

    parsed = parse_radar_setup_callback(raw_action)
    if parsed is None:
        await query.answer("Acción de Radar inválida", show_alert=True)
        return
    action, alert_id = parsed
    await query.answer("Procesando alerta Radar...")

    cfg = get_config()
    db = PortfolioDatabase(cfg.database.url)
    await db.connect()
    try:
        store = RadarSetupAlertStore(await db.get_pool())
        if action == "view":
            alert = await store.get_alert(alert_id, owner_chat_id=chat_id)
        else:
            alert = await store.record_user_action(
                alert_id,
                owner_chat_id=chat_id,
                action=(
                    USER_ACTION_FOLLOW
                    if action == "follow"
                    else USER_ACTION_DISMISS
                ),
            )
    finally:
        await db.close()

    if alert is None:
        await send_text(
            context,
            chat_id,
            "La alerta no existe o no pertenece a este chat.",
        )
        return

    ticker = re.sub(r"[^A-Za-z0-9.\-]", "", str(alert.get("ticker") or "")).upper()
    if action == "view":
        await send_text(context, chat_id, f"Analizando <b>{html_text(ticker)}</b>...")
        await action_ticker_analysis(context, chat_id, ticker)
        await send_menu(context, chat_id)
        return
    if alert.get("action_conflict"):
        original = str(alert.get("user_action") or "").upper()
        original_label = "seguir" if original == USER_ACTION_FOLLOW else "descartar"
        await send_text(
            context,
            chat_id,
            f"<b>{html_text(ticker)}</b>: ya habías elegido {original_label}.\n"
            "La primera respuesta queda congelada para conservar la auditoría.",
        )
        return
    if not alert.get("action_recorded", True):
        await send_text(
            context,
            chat_id,
            f"<b>{html_text(ticker)}</b>: esa elección ya estaba registrada.",
        )
        return
    if action == "follow":
        await send_text(
            context,
            chat_id,
            f"👁 <b>{html_text(ticker)}</b>: seguimiento shadow registrado.\n"
            "No crea una orden. Si aparece una compra real, se vinculará y auditará por separado.",
        )
        return
    await send_text(
        context,
        chat_id,
        f"<b>{html_text(ticker)}</b>: alerta descartada para tu seguimiento.\n"
        "El outcome teórico del trigger seguirá midiéndose.",
    )


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return

    raw_action = str(query.data or "").strip()
    action     = CALLBACK_ALIASES.get(raw_action)
    chat_id    = query.message.chat_id if query.message else update.effective_chat.id

    if not await ensure_allowed_chat(update, context):
        return

    if raw_action.startswith("re:"):
        try:
            await _handle_radar_exploratory_callback(
                update,
                context,
                raw_action=raw_action,
                chat_id=chat_id,
            )
        except Exception as exc:
            logger.exception("[BOT] callback Radar exploratorio falló: %s", raw_action)
            await send_text(
                context,
                chat_id,
                f"Error procesando la idea Radar:\n<code>{html_text(exc)}</code>",
            )
        return

    if raw_action.startswith("rs:"):
        try:
            await _handle_radar_setup_callback(
                update,
                context,
                raw_action=raw_action,
                chat_id=chat_id,
            )
        except Exception as exc:
            logger.exception("[BOT] callback Radar setup falló: %s", raw_action)
            await send_text(
                context,
                chat_id,
                f"Error procesando la alerta Radar:\n<code>{html_text(exc)}</code>",
            )
        return

    try:
        await query.answer()
    except BadRequest as exc:
        if "query is too old" in str(exc).lower():
            logger.info("[BOT] callback vencido; continúa la acción sin popup: %s", raw_action)
        else:
            raise

    if not action:
        await send_text(
            context, chat_id,
            f"⚠️ Botón no reconocido: <code>{raw_action}</code>\n"
            "Abrí /menu para refrescar.",
        )
        logger.warning("[BOT] callback no mapeado: %s", raw_action)
        await send_menu(context, chat_id)
        return

    view = menu_view(action)
    if view is not None:
        text, keyboard = view
        try:
            await query.edit_message_text(
                text=text,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
                disable_web_page_preview=True,
            )
        except Exception:
            await context.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
                disable_web_page_preview=True,
            )
        return

    loading = ACTION_LOADING_TEXT.get(action, "🔄 Procesando...")
    if action not in FAST_ACTIONS:
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

async def bot_heartbeat_loop(_app: Application) -> None:
    if redis_client is None:
        logger.info("[BOT] Heartbeat Redis deshabilitado")
        return

    while True:
        try:
            await redis_client.set(
                BOT_HEARTBEAT_KEY,
                str(int(datetime.now(tz=timezone.utc).timestamp())),
                ex=90,
            )
        except Exception as exc:
            logger.debug("[BOT] Heartbeat Redis ignorado: %s", exc)
        await asyncio.sleep(30)


async def post_init(app: Application) -> None:
    try:
        from telegram import BotCommand, MenuButtonCommands

        commands = [BotCommand(command, description) for command, description in BOT_COMMAND_SPECS]
        await app.bot.set_my_commands(commands)
        await app.bot.set_chat_menu_button(menu_button=MenuButtonCommands())
        logger.info("[BOT] Menu nativo de Telegram configurado")
    except Exception as exc:
        logger.warning("[BOT] No pude configurar menu nativo de Telegram: %s", exc)

    app.bot_data["heartbeat_task"] = asyncio.create_task(
        bot_heartbeat_loop(app),
        name="bot_heartbeat",
    )


async def post_shutdown(app: Application) -> None:
    task = app.bot_data.get("heartbeat_task")
    if task:
        task.cancel()


def build_app() -> Application:
    token = _get_token()
    app   = (
        Application.builder()
        .token(token)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    # Comandos principales
    app.add_handler(CommandHandler("start",            start_handler))
    app.add_handler(CommandHandler("menu",             menu_handler))
    app.add_handler(CommandHandler("help",             help_handler))
    app.add_handler(CommandHandler("portfolio",        portfolio_handler))
    app.add_handler(CommandHandler("analisis",         analysis_handler))
    app.add_handler(CommandHandler("analysis",         analysis_handler))
    app.add_handler(CommandHandler("analisis_semanal", analysis_handler))
    app.add_handler(CommandHandler("analisis_test",    analysis_test_handler))
    app.add_handler(CommandHandler("analysis_test",    analysis_test_handler))
    app.add_handler(CommandHandler("analysis_full",    analysis_full_handler))
    app.add_handler(CommandHandler("analisis_full",    analysis_full_handler))
    app.add_handler(CommandHandler("analysis_debug",   analysis_debug_handler))
    app.add_handler(CommandHandler("analisis_debug",   analysis_debug_handler))
    app.add_handler(CommandHandler("mercado",          market_context_handler))
    app.add_handler(CommandHandler("market_context",   market_context_handler))
    app.add_handler(CommandHandler("noticias",         market_context_handler))
    app.add_handler(CommandHandler("events",           upcoming_events_handler))
    app.add_handler(CommandHandler("earnings",         upcoming_events_handler))
    app.add_handler(CommandHandler("balances",         upcoming_events_handler))
    app.add_handler(CommandHandler("ticker",           ticker_handler))
    app.add_handler(CommandHandler("tecnico",          ticker_handler))
    app.add_handler(CommandHandler("accion",           ticker_handler))
    app.add_handler(CommandHandler("resumen",          weekly_summary_handler))
    app.add_handler(CommandHandler("weekly_summary",   weekly_summary_handler))
    app.add_handler(CommandHandler("resumen_semanal",  weekly_summary_handler))
    app.add_handler(CommandHandler("performance",      performance_handler))
    app.add_handler(CommandHandler("neto",             net_decisions_handler))
    app.add_handler(CommandHandler("resultado_neto",   net_decisions_handler))
    app.add_handler(CommandHandler("viability",        viability_handler))
    app.add_handler(CommandHandler("viabilidad",       viability_handler))
    app.add_handler(CommandHandler("ledger",           decision_ledger_handler))
    app.add_handler(CommandHandler("decision_ledger",  decision_ledger_handler))
    app.add_handler(CommandHandler("atribucion",       decision_ledger_handler))
    app.add_handler(CommandHandler("policy",           policy_tree_handler))
    app.add_handler(CommandHandler("policy_tree",      policy_tree_handler))
    app.add_handler(CommandHandler("decision_tree",    policy_tree_handler))
    app.add_handler(CommandHandler("arbol",            policy_tree_handler))
    app.add_handler(CommandHandler("override",         override_audit_handler))
    app.add_handler(CommandHandler("overrides",        override_audit_handler))
    app.add_handler(CommandHandler("bot_vs_humano",    override_audit_handler))
    app.add_handler(CommandHandler("confianza",        confidence_handler))
    app.add_handler(CommandHandler("confidence",       confidence_handler))
    app.add_handler(CommandHandler("trust",            confidence_handler))
    app.add_handler(CommandHandler("calibration",      calibration_handler))
    app.add_handler(CommandHandler("calibracion",      calibration_handler))
    app.add_handler(CommandHandler("dcl",              calibration_handler))
    app.add_handler(CommandHandler("radar",            radar_handler))
    app.add_handler(CommandHandler("radar_full", radar_full_handler))
    app.add_handler(CommandHandler("radar_metricas", radar_metrics_handler))
    app.add_handler(CommandHandler("radar_metrics", radar_metrics_handler))
    app.add_handler(CommandHandler("shadow",           shadow_handler))
    app.add_handler(CommandHandler("regression", regression_audit_handler))
    app.add_handler(CommandHandler("regression_audit", regression_audit_handler))
    app.add_handler(CommandHandler("status",           status_handler))
    app.add_handler(CommandHandler("configuracion",    settings_handler))
    app.add_handler(CommandHandler("settings",         settings_handler))
    app.add_handler(CommandHandler("reconfigurar",     reconfigure_settings_handler))
    app.add_handler(CommandHandler("reconfigure",      reconfigure_settings_handler))
    app.add_handler(CommandHandler("cancelar",         cancel_settings_handler))
    app.add_handler(CommandHandler("cancel",           cancel_settings_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, settings_text_handler))

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

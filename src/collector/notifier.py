"""
src/collector/notifier.py
Canal central de notificaciones via Telegram.

FIXES:
- send_raw ahora manda TEXTO PLANO (sin parseo) para evitar 400 por HTML/Markdown.
- chunking para mensajes largos (Telegram limit).
- HTML seguro: escapamos contenido dinámico en <code>...</code>.
- Retornamos bool en sends para trazabilidad real.
"""
from __future__ import annotations

import json
import logging
import tempfile
from html import escape as html_escape
from typing import Optional

import requests

logger = logging.getLogger(__name__)


class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str):
        self._token = bot_token
        self._chat_id = chat_id
        self._base = f"https://api.telegram.org/bot{bot_token}"
        self._enabled = bool(bot_token and chat_id)

        # Telegram hard-limits are ~4096 chars per message; dejo margen.
        self._max_message_len = 3500

    # ---------------------------
    # Low-level send helpers
    # ---------------------------
    def _send(self, text: str, parse_mode: Optional[str] = "HTML") -> bool:
        """
        Envía un mensaje. Si parse_mode=None => texto plano (sin parsing).
        """
        if not self._enabled:
            logger.debug("Telegram deshabilitado (faltan token/chat_id)")
            return False

        try:
            data = {"chat_id": self._chat_id, "text": text, "disable_web_page_preview": True}
            if parse_mode:
                data["parse_mode"] = parse_mode

            r = requests.post(f"{self._base}/sendMessage", data=data, timeout=15)
            ok = (r.status_code == 200)
            if not ok:
                logger.warning(f"Telegram error {r.status_code}: {r.text}")
            return ok
        except Exception as e:
            logger.warning(f"Telegram send error: {e}")
            return False

    def _send_chunked(self, text: str, parse_mode: Optional[str] = None) -> bool:
        """
        Divide en chunks para evitar límites de Telegram.
        Por defecto parse_mode=None para texto plano (lo más robusto).
        """
        if not text:
            return True

        ok_all = True
        max_len = self._max_message_len

        for i in range(0, len(text), max_len):
            chunk = text[i : i + max_len]
            ok = self._send(chunk, parse_mode=parse_mode)
            ok_all = ok_all and ok

        return ok_all

    def _send_document(self, filename: str, content: str, caption: str = "") -> bool:
        if not self._enabled:
            logger.debug("Telegram deshabilitado (faltan token/chat_id)")
            return False

        try:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as f:
                f.write(content)
                tmp_path = f.name

            with open(tmp_path, "rb") as f:
                r = requests.post(
                    f"{self._base}/sendDocument",
                    data={"chat_id": self._chat_id, "caption": caption},
                    files={"document": (filename, f, "application/json")},
                    timeout=45,
                )

            ok = (r.status_code == 200)
            if not ok:
                logger.warning(f"Telegram document error {r.status_code}: {r.text}")
            return ok

        except Exception as e:
            logger.warning(f"Telegram document error: {e}")
            return False

    # ---------------------------
    # Public API (notifications)
    # ---------------------------
    def notify_run_start(self, run_type: str = "SCRAPE") -> bool:
        run_type_safe = html_escape(run_type)
        return self._send(f"<b>INICIO EJECUCION</b>\nTipo: <code>{run_type_safe}</code>", parse_mode="HTML")

    def notify_login_ok(self, with_mfa: bool = False) -> bool:
        mfa_txt = " (con MFA)" if with_mfa else " (sin MFA)"
        # texto simple, HTML ok
        return self._send(f"Login exitoso{mfa_txt}", parse_mode="HTML")

    def notify_login_error(self, error: str) -> bool:
        err_safe = html_escape((error or "")[:500])
        return self._send(f"<b>ERROR DE LOGIN</b>\n<code>{err_safe}</code>", parse_mode="HTML")

    def notify_mfa_required(self, timeout_minutes: int = 2) -> bool:
        return self._send(
            f"<b>CODIGO MFA REQUERIDO</b>\n\n"
            f"Enviar codigo de 6 digitos.\n"
            f"Timeout: <b>{timeout_minutes} minutos</b>",
            parse_mode="HTML",
        )

    def notify_mfa_received(self, code: str) -> bool:
        code_safe = html_escape(code)
        return self._send(f"Codigo <code>{code_safe}</code> recibido. Intentando login...", parse_mode="HTML")

    def notify_mfa_timeout(self) -> bool:
        return self._send("Timeout — no se recibio codigo MFA a tiempo.", parse_mode="HTML")

    def notify_scrape_complete(
        self,
        total_ars: float,
        positions_count: int,
        confidence: float,
        cash_ars: Optional[float] = None,
    ) -> bool:
        cash_line = f"\nCash ARS: <b>${cash_ars:,.0f}</b>" if cash_ars is not None else ""
        return self._send(
            f"<b>SCRAPE COMPLETADO</b>\n\n"
            f"Portfolio total: <b>${total_ars:,.0f} ARS</b>{cash_line}\n"
            f"Posiciones: <b>{positions_count}</b>\n"
            f"Confianza: <b>{confidence:.0%}</b>",
            parse_mode="HTML",
        )

    def notify_critical_error(self, context: str, error: str) -> bool:
        ctx_safe = html_escape((context or "")[:120])
        err_safe = html_escape((error or "")[:500])
        return self._send(
            f"<b>ERROR CRITICO</b>\nContexto: <code>{ctx_safe}</code>\nError: <code>{err_safe}</code>",
            parse_mode="HTML",
        )

    # ---------------------------
    # Reports / attachments
    # ---------------------------
    def send_snapshot_json(self, snapshot_dict: dict) -> bool:
        content = json.dumps(snapshot_dict, indent=2, ensure_ascii=False)
        ts = str(snapshot_dict.get("scraped_at", "snapshot"))[:10]
        return self._send_document(
            filename=f"portfolio_{ts}.json",
            content=content,
            caption=f"Portfolio snapshot {ts}",
        )

    def send_raw(self, text: str) -> bool:
        """
        Para reportes de análisis: SIEMPRE texto plano.
        Evita errores 400 por parse de HTML/Markdown.
        """
        return self._send_chunked(text, parse_mode="HTML")
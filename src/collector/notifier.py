"""
src/collector/notifier.py — Canal central de notificaciones via Telegram.
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
        self._max_message_len = 3500

    def _send(self, text: str, parse_mode: Optional[str] = "HTML") -> bool:
        if not self._enabled:
            logger.debug("Telegram deshabilitado (faltan token/chat_id)")
            return False
        try:
            data = {"chat_id": self._chat_id, "text": text, "disable_web_page_preview": True}
            if parse_mode:
                data["parse_mode"] = parse_mode
            r = requests.post(f"{self._base}/sendMessage", data=data, timeout=15)
            ok = r.status_code == 200
            if not ok:
                logger.warning(f"Telegram error {r.status_code}: {r.text}")
            return ok
        except Exception as e:
            logger.warning(f"Telegram send error: {e}")
            return False

    def _send_chunked(self, text: str, parse_mode: Optional[str] = None) -> bool:
        if not text:
            return True
        ok_all = True
        for i in range(0, len(text), self._max_message_len):
            ok = self._send(text[i: i + self._max_message_len], parse_mode=parse_mode)
            ok_all = ok_all and ok
        return ok_all

    def _send_document(self, filename: str, content: str, caption: str = "") -> bool:
        if not self._enabled:
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
            ok = r.status_code == 200
            if not ok:
                logger.warning(f"Telegram document error {r.status_code}: {r.text}")
            return ok
        except Exception as e:
            logger.warning(f"Telegram document error: {e}")
            return False

    def notify_run_start(self, run_type: str = "SCRAPE") -> bool:
        return self._send(f"<b>INICIO EJECUCION</b>\nTipo: <code>{html_escape(run_type)}</code>")

    def notify_login_ok(self, with_mfa: bool = False) -> bool:
        return self._send(f"Login exitoso{' (con MFA)' if with_mfa else ' (sin MFA)'}")

    def notify_login_error(self, error: str) -> bool:
        return self._send(f"<b>ERROR DE LOGIN</b>\n<code>{html_escape((error or '')[:500])}</code>")

    def notify_mfa_required(self, timeout_minutes: int = 2) -> bool:
        return self._send(
            f"<b>CODIGO MFA REQUERIDO</b>\n\nEnviar codigo de 6 digitos.\n"
            f"Timeout: <b>{timeout_minutes} minutos</b>"
        )

    def notify_mfa_received(self, code: str) -> bool:
        return self._send(f"Codigo <code>{html_escape(code)}</code> recibido. Intentando login...")

    def notify_mfa_timeout(self) -> bool:
        return self._send("Timeout — no se recibio codigo MFA a tiempo.")

    def notify_scrape_complete(self, total_ars: float, positions_count: int,
                                confidence: float, cash_ars: Optional[float] = None) -> bool:
        cash_line = f"\nCash ARS: <b>${cash_ars:,.0f}</b>" if cash_ars is not None else ""
        return self._send(
            f"<b>SCRAPE COMPLETADO</b>\n\n"
            f"Portfolio total: <b>${total_ars:,.0f} ARS</b>{cash_line}\n"
            f"Posiciones: <b>{positions_count}</b>\nConfianza: <b>{confidence:.0%}</b>"
        )

    def notify_critical_error(self, context: str, error: str) -> bool:
        return self._send(
            f"<b>ERROR CRITICO</b>\n"
            f"Contexto: <code>{html_escape((context or '')[:120])}</code>\n"
            f"Error: <code>{html_escape((error or '')[:500])}</code>"
        )

    def send_snapshot_json(self, snapshot_dict: dict) -> bool:
        content = json.dumps(snapshot_dict, indent=2, ensure_ascii=False)
        ts = str(snapshot_dict.get("scraped_at", "snapshot"))[:10]
        return self._send_document(
            filename=f"portfolio_{ts}.json", content=content,
            caption=f"Portfolio snapshot {ts}",
        )

    def send_raw(self, text: str) -> bool:
        """
        Envía el reporte como HTML renderizado.
        Divide por lineas (no por caracteres) para no romper tags HTML.
        Si falla el parseo HTML, reintenta como texto plano.
        """
        if not text:
            return True

        max_len = self._max_message_len
        lines = text.split("\n")
        chunks = []
        current, current_len = [], 0

        for line in lines:
            line_len = len(line) + 1
            if current_len + line_len > max_len and current:
                chunks.append("\n".join(current))
                current, current_len = [line], line_len
            else:
                current.append(line)
                current_len += line_len
        if current:
            chunks.append("\n".join(current))

        ok_all = True
        for chunk in chunks:
            if not chunk.strip():
                continue
            ok = self._send(chunk, parse_mode="HTML")
            if not ok:
                logger.warning("HTML parse falló, reintentando como texto plano")
                ok = self._send(chunk, parse_mode=None)
            ok_all = ok_all and ok
        return ok_all
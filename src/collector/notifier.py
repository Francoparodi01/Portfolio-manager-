"""
src/collector/notifier.py — Canal central de notificaciones via Telegram.
Versión ajustada:
- Silencia notificaciones rutinarias del scraper
- Silencia MFA
- Mantiene activas las críticas / send_raw
- Apertura/cierre de monitoreo se envían desde runner.py
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

    # ── RUTINARIAS SILENCIADAS ─────────────────────────────────────────────

    def notify_run_start(self, run_type: str = "SCRAPE") -> bool:
        logger.info("Telegram silenciado: inicio ejecucion [%s]", run_type)
        return True

    def notify_login_ok(self, with_mfa: bool = False) -> bool:
        logger.info("Telegram silenciado: login ok%s", " con MFA" if with_mfa else "")
        return True

    def notify_scrape_complete(
        self,
        total_ars: float,
        positions_count: int,
        confidence: float,
        cash_ars: Optional[float] = None,
    ) -> bool:
        logger.info(
            "Telegram silenciado: scrape completo total=%s posiciones=%s confianza=%s cash=%s",
            total_ars, positions_count, confidence, cash_ars
        )
        return True

    def send_snapshot_json(self, snapshot_dict: dict) -> bool:
        logger.info("Telegram silenciado: snapshot json no enviado")
        return True

    # ── MFA SILENCIADO ─────────────────────────────────────────────────────

    def notify_mfa_required(self, timeout_minutes: int = 2) -> bool:
        logger.info("Telegram silenciado: MFA requerido (%s min)", timeout_minutes)
        return True

    def notify_mfa_received(self, code: str) -> bool:
        logger.info("Telegram silenciado: MFA recibido")
        return True

    def notify_mfa_timeout(self) -> bool:
        logger.info("Telegram silenciado: MFA timeout")
        return True

    # ── IMPORTANTES / CRÍTICAS ACTIVAS ─────────────────────────────────────

    def notify_login_error(self, error: str) -> bool:
        return self._send(f"<b>ERROR DE LOGIN</b>\n<code>{html_escape((error or '')[:500])}</code>")

    def notify_critical_error(self, context: str, error: str) -> bool:
        return self._send(
            f"<b>ERROR CRITICO</b>\n"
            f"Contexto: <code>{html_escape((context or '')[:120])}</code>\n"
            f"Error: <code>{html_escape((error or '')[:500])}</code>"
        )

    def send_raw(self, text: str) -> bool:
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
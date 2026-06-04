"""src/core/logger.py — Logger centralizado que escribe a stderr."""
from __future__ import annotations
import logging
import re
import sys

_configured = False

SECRET_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\b\d{6,12}:[A-Za-z0-9_-]{20,}\b"), "<telegram_token_redacted>"),
    (re.compile(r"\bbot\d+:[A-Za-z0-9_-]+\b", re.I), "bot***"),
    (re.compile(r"(Authorization:\s*Bearer\s+)[A-Za-z0-9._~+/=-]+", re.I), r"\1***"),
    (re.compile(r"(X-API-Token:\s*)[A-Za-z0-9._~+/=-]+", re.I), r"\1***"),
    (re.compile(r"((?:PASSWORD|PASS|TOKEN|SECRET|API_KEY|APP_ENCRYPTION_KEY)=)[^\s]+", re.I), r"\1***"),
    (re.compile(r"((?:password|token|secret|api_key)=)[^\s&]+", re.I), r"\1***"),
    (re.compile(r"(postgres(?:ql)?(?:\+asyncpg)?://[^:\s]+:)[^@\s]+@", re.I), r"\1***@"),
    (re.compile(r"(redis://[^:\s]+:)[^@\s]+@", re.I), r"\1***@"),
)


def redact_secrets(value: object) -> str:
    text = str(value)
    for pattern, replacement in SECRET_PATTERNS:
        text = pattern.sub(replacement, text)
    return text


class RedactingFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return redact_secrets(super().format(record))


def get_logger(name: str) -> logging.Logger:
    global _configured
    if not _configured:
        # Todo el logging va a stderr para que stdout quede limpio para el bot
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(
            RedactingFormatter("%(asctime)s [%(levelname)s] %(name)s — %(message)s")
        )
        root = logging.getLogger()
        root.handlers.clear()
        root.addHandler(handler)
        root.setLevel(logging.INFO)
        # Silenciar loggers ruidosos de librerías externas
        for noisy in ("yfinance", "urllib3", "asyncio", "httpx", "httpcore"):
            logging.getLogger(noisy).setLevel(logging.WARNING)
        _configured = True
    return logging.getLogger(name)


import functools
import time as _time

def timed(label: str):
    """Decorator que loguea el tiempo de ejecución de una coroutine."""
    def decorator(fn):
        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            t0 = _time.monotonic()
            try:
                result = await fn(*args, **kwargs)
                elapsed = _time.monotonic() - t0
                logging.getLogger(__name__).debug(f"{label} OK ({elapsed:.2f}s)")
                return result
            except Exception as e:
                elapsed = _time.monotonic() - t0
                logging.getLogger(__name__).warning(f"{label} ERROR ({elapsed:.2f}s): {e}")
                raise
        return wrapper
    return decorator

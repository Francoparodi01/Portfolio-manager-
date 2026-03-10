"""src/core/logger.py — Logger centralizado que escribe a stderr."""
from __future__ import annotations
import logging
import sys

_configured = False


def get_logger(name: str) -> logging.Logger:
    global _configured
    if not _configured:
        # Todo el logging va a stderr para que stdout quede limpio para el bot
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s — %(message)s")
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

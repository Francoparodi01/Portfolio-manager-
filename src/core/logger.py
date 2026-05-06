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
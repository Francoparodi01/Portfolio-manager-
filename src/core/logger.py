"""
src/core/logger.py
Logger estructurado con timestamps ISO 8601.
Decorador @timed para medir duración de operaciones críticas.
"""

from __future__ import annotations

import functools
import logging
import sys
import time
from typing import Callable


_FMT = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
_DATE = "%Y-%m-%dT%H:%M:%S"

logging.basicConfig(
    level=logging.INFO,
    format=_FMT,
    datefmt=_DATE,
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def timed(label: str):
    """
    Decorador async que loguea la duración de la función decorada.
    Uso: @timed("scraper.login")
    """
    def decorator(fn: Callable):
        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            log = get_logger(fn.__module__)
            t0 = time.perf_counter()
            try:
                result = await fn(*args, **kwargs)
                elapsed = time.perf_counter() - t0
                log.info(f"[{label}] completado en {elapsed:.2f}s")
                return result
            except Exception as e:
                elapsed = time.perf_counter() - t0
                log.error(f"[{label}] falló en {elapsed:.2f}s — {e}")
                raise
        return wrapper
    return decorator

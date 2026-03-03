"""
__main__.py — Punto de entrada para correr el scheduler.

Uso:
    python -m src.scheduler.runner
    # o desde raiz del proyecto:
    python -m src.scheduler
"""
from src.scheduler.runner import start_scheduler

if __name__ == "__main__":
    start_scheduler()

"""Compatibility exports for the shared raw bot-signal metric contract."""

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analysis.raw_bot_signal_metrics import (  # noqa: F401,E402
    ART,
    CALENDAR,
    CALENDAR_FROM,
    CALENDAR_THROUGH,
    CLOSURES,
    HORIZONS,
    SOURCES,
    compute,
    eligible,
    number,
    sessions_after,
    trading_day,
)

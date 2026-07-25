from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_HOLIDAYS_PATH = PROJECT_ROOT / "config" / "market_holidays_ar.json"


@dataclass(frozen=True)
class MarketClosure:
    date: date
    name: str
    category: str


@dataclass(frozen=True)
class MarketSpecialSession:
    date: date
    name: str
    category: str
    trading: bool
    settlement: bool


def _as_date(value: date | datetime | None) -> date:
    if value is None:
        return datetime.now().date()
    if isinstance(value, datetime):
        return value.date()
    return value


def _as_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "si"}
    return bool(value)


@lru_cache(maxsize=8)
def load_market_closures(path: str | None = None) -> dict[date, MarketClosure]:
    source = Path(path) if path else DEFAULT_HOLIDAYS_PATH
    if not source.exists():
        return {}

    payload: dict[str, Any] = json.loads(source.read_text(encoding="utf-8"))
    closures: dict[date, MarketClosure] = {}
    for item in payload.get("closures", []):
        raw_date = item.get("date")
        if not raw_date:
            continue
        closure_date = date.fromisoformat(str(raw_date))
        closures[closure_date] = MarketClosure(
            date=closure_date,
            name=str(item.get("name") or "Mercado cerrado"),
            category=str(item.get("category") or "closure"),
        )
    return closures


@lru_cache(maxsize=8)
def load_market_special_sessions(path: str | None = None) -> dict[date, MarketSpecialSession]:
    source = Path(path) if path else DEFAULT_HOLIDAYS_PATH
    if not source.exists():
        return {}

    payload: dict[str, Any] = json.loads(source.read_text(encoding="utf-8"))
    sessions: dict[date, MarketSpecialSession] = {}
    for item in payload.get("special_sessions", []):
        raw_date = item.get("date")
        if not raw_date:
            continue
        session_date = date.fromisoformat(str(raw_date))
        sessions[session_date] = MarketSpecialSession(
            date=session_date,
            name=str(item.get("name") or "Jornada especial"),
            category=str(item.get("category") or "special_session"),
            trading=_as_bool(item.get("trading"), True),
            settlement=_as_bool(item.get("settlement"), True),
        )
    return sessions


def get_market_closure(day: date | datetime | None = None) -> MarketClosure | None:
    return load_market_closures().get(_as_date(day))


def get_market_special_session(day: date | datetime | None = None) -> MarketSpecialSession | None:
    return load_market_special_sessions().get(_as_date(day))


def is_trading_day(day: date | datetime | None = None) -> bool:
    target = _as_date(day)
    if target.weekday() >= 5 or target in load_market_closures():
        return False
    special = get_market_special_session(target)
    return True if special is None else special.trading


def is_settlement_day(day: date | datetime | None = None) -> bool:
    target = _as_date(day)
    if not is_trading_day(target):
        return False
    special = get_market_special_session(target)
    return True if special is None else special.settlement


def market_session_note(day: date | datetime | None = None) -> str | None:
    special = get_market_special_session(day)
    if special:
        return f"{special.category}: {special.name}"
    return None


def market_closed_reason(day: date | datetime | None = None) -> str | None:
    target = _as_date(day)
    closure = get_market_closure(target)
    if closure:
        return f"{closure.category}: {closure.name}"
    if target.weekday() >= 5:
        return "fin_de_semana"
    return None

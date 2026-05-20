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


def _as_date(value: date | datetime | None) -> date:
    if value is None:
        return datetime.now().date()
    if isinstance(value, datetime):
        return value.date()
    return value


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


def get_market_closure(day: date | datetime | None = None) -> MarketClosure | None:
    return load_market_closures().get(_as_date(day))


def is_trading_day(day: date | datetime | None = None) -> bool:
    target = _as_date(day)
    return target.weekday() < 5 and target not in load_market_closures()


def market_closed_reason(day: date | datetime | None = None) -> str | None:
    target = _as_date(day)
    closure = get_market_closure(target)
    if closure:
        return f"{closure.category}: {closure.name}"
    if target.weekday() >= 5:
        return "fin_de_semana"
    return None

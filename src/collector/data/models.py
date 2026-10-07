"""src/collector/data/models.py — Modelos de dominio del portfolio."""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class AssetType(str, Enum):
    CEDEAR   = "CEDEAR"
    ACCION   = "ACCION"
    BONO     = "BONO"
    FCI      = "FCI"
    DOLAR    = "DOLAR"
    UNKNOWN  = "UNKNOWN"


class Currency(str, Enum):
    ARS = "ARS"
    USD = "USD"
    MEP = "MEP"


@dataclass
class Position:
    ticker: str
    asset_type: AssetType
    currency: Currency
    quantity: float
    avg_cost: float
    current_price: float
    market_value: float
    unrealized_pnl: float
    unrealized_pnl_pct: float
    weight_in_portfolio: Optional[float] = None
    sector: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "ticker": self.ticker,
            "asset_type": self.asset_type.value,
            "currency": self.currency.value,
            "quantity": float(self.quantity),
            "avg_cost": float(self.avg_cost),
            "current_price": float(self.current_price),
            "market_value": float(self.market_value),
            "unrealized_pnl": float(self.unrealized_pnl),
            "unrealized_pnl_pct": float(self.unrealized_pnl_pct),
            "weight_in_portfolio": float(self.weight_in_portfolio) if self.weight_in_portfolio is not None else None,
            "sector": self.sector,
        }


@dataclass
class PortfolioSnapshot:
    scraped_at: datetime
    positions: list[Position]
    total_value_ars: float
    cash_ars: float
    confidence_score: float
    dom_hash: str
    raw_html_hash: str
    owner_chat_id: Optional[int] = None
    snapshot_id: uuid.UUID = field(default_factory=uuid.uuid4)

    def validate(self) -> list[str]:
        errors = []
        invested = float(self.total_value_ars)
        cash = float(self.cash_ars)
        if self.positions and invested <= 0:
            errors.append("total_value_ars <= 0")
        if not self.positions:
            if cash <= 0:
                errors.append("sin posiciones")
            if invested > 0:
                errors.append("tenencia valorizada positiva sin posiciones")
        return errors

    def to_dict(self) -> dict:
        return {
            "snapshot_id": str(self.snapshot_id),
            "scraped_at": self.scraped_at.isoformat(),
            "total_value_ars": float(self.total_value_ars),
            "cash_ars": float(self.cash_ars),
            "confidence_score": float(self.confidence_score),
            "dom_hash": self.dom_hash,
            "raw_html_hash": self.raw_html_hash,
            "owner_chat_id": self.owner_chat_id,
            "positions": [p.to_dict() for p in self.positions],
        }


@dataclass
class MarketAsset:
    ticker: str
    name: str
    asset_type: AssetType
    currency: Currency
    last_price: float
    change_pct_1d: Optional[float] = None
    volume: Optional[float] = None
    scraped_at: datetime = field(default_factory=utcnow)


@dataclass(frozen=True)
class MarketCandle:
    ticker: str
    long_ticker: str
    asset_type: AssetType
    currency: Currency
    venue: str
    interval: str
    ts: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: Optional[float]
    source: str = "COCOS"
    scraped_at: Optional[datetime] = None
    bar_start: Optional[datetime] = None
    bar_end: Optional[datetime] = None
    available_at: Optional[datetime] = None
    is_closed: Optional[bool] = None
    volume_unit: Optional[str] = None
    calendar: Optional[str] = None
    calendar_validation: Optional[str] = None
    adjustment_policy: Optional[str] = None
    depositary_ratio: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "ticker": self.ticker,
            "long_ticker": self.long_ticker,
            "asset_type": self.asset_type.value,
            "currency": self.currency.value,
            "venue": self.venue,
            "interval": self.interval,
            "ts": self.ts.isoformat(),
            "open_price": float(self.open_price),
            "high_price": float(self.high_price),
            "low_price": float(self.low_price),
            "close_price": float(self.close_price),
            "volume": float(self.volume) if self.volume is not None else None,
            "source": self.source,
            "scraped_at": self.scraped_at.isoformat() if self.scraped_at else None,
            "bar_start": self.bar_start.isoformat() if self.bar_start else None,
            "bar_end": self.bar_end.isoformat() if self.bar_end else None,
            "available_at": self.available_at.isoformat() if self.available_at else None,
            "is_closed": self.is_closed,
            "volume_unit": self.volume_unit,
            "calendar": self.calendar,
            "calendar_validation": self.calendar_validation,
            "adjustment_policy": self.adjustment_policy,
            "depositary_ratio": self.depositary_ratio,
        }

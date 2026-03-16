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
    snapshot_id: uuid.UUID = field(default_factory=uuid.uuid4)

    def validate(self) -> list[str]:
        errors = []
        if float(self.total_value_ars) <= 0:
            errors.append("total_value_ars <= 0")
        if not self.positions:
            errors.append("sin posiciones")
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
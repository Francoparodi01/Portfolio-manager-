"""
src/collector/data/models.py
Modelos de dominio: PortfolioSnapshot, Position, MarketAsset.
Inmutables (frozen dataclasses). Sin dependencias de ORM.
"""
from __future__ import annotations
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Optional


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class AssetType(str, Enum):
    ACCION = "ACCION"
    CEDEAR = "CEDEAR"
    BONO = "BONO"
    FCI = "FCI"
    CASH = "CASH"
    UNKNOWN = "UNKNOWN"


class Currency(str, Enum):
    ARS = "ARS"
    USD = "USD"
    USD_MEP = "USD_MEP"


@dataclass(frozen=True)
class Position:
    ticker: str
    asset_type: AssetType
    currency: Currency
    quantity: Decimal
    avg_cost: Decimal
    current_price: Decimal
    market_value: Decimal
    unrealized_pnl: Decimal
    unrealized_pnl_pct: Decimal
    sector: Optional[str] = None
    weight_in_portfolio: Optional[Decimal] = None

    def __post_init__(self):
        if not self.ticker:
            raise ValueError("ticker no puede ser vacío")
        if self.quantity < 0:
            raise ValueError(f"quantity negativa para {self.ticker}")
        if self.current_price < 0:
            raise ValueError(f"current_price negativo para {self.ticker}")


@dataclass(frozen=True)
class PortfolioSnapshot:
    scraped_at: datetime
    positions: tuple[Position, ...]
    total_value_ars: Decimal
    cash_ars: Decimal
    confidence_score: float
    dom_hash: str = ""
    raw_html_hash: str = ""
    snapshot_id: uuid.UUID = field(default_factory=uuid.uuid4)

    def validate(self) -> list[str]:
        errors = []
        if not self.positions:
            errors.append("Sin posiciones")
        if self.total_value_ars < 0:
            errors.append("total_value_ars negativo")
        if not (0.0 <= self.confidence_score <= 1.0):
            errors.append(f"confidence_score fuera de rango: {self.confidence_score}")
        return errors

    def to_dict(self) -> dict:
        return {
            "snapshot_id": str(self.snapshot_id),
            "scraped_at": self.scraped_at.isoformat(),
            "total_value_ars": str(self.total_value_ars),
            "cash_ars": str(self.cash_ars),
            "confidence_score": self.confidence_score,
            "positions_count": len(self.positions),
            "positions": [
                {
                    "ticker": p.ticker,
                    "asset_type": p.asset_type.value,
                    "currency": p.currency.value,
                    "quantity": str(p.quantity),
                    "avg_cost": str(p.avg_cost),
                    "current_price": str(p.current_price),
                    "market_value": str(p.market_value),
                    "unrealized_pnl": str(p.unrealized_pnl),
                    "unrealized_pnl_pct": str(p.unrealized_pnl_pct),
                    "sector": p.sector,
                    "weight_in_portfolio": str(p.weight_in_portfolio) if p.weight_in_portfolio else None,
                }
                for p in self.positions
            ],
        }


@dataclass(frozen=True)
class MarketAsset:
    ticker: str
    name: str
    asset_type: AssetType
    currency: Currency
    last_price: Decimal
    scraped_at: datetime
    change_pct_1d: Optional[Decimal] = None
    volume: Optional[Decimal] = None

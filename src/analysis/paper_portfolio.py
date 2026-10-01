"""Paper portfolio replica, isolated from live portfolio and decision metrics."""
from __future__ import annotations

import json
import logging
from datetime import datetime
from enum import Enum
from typing import Any

from zoneinfo import ZoneInfo

from src.collector.db import PortfolioDatabase

logger = logging.getLogger(__name__)
ART = ZoneInfo("America/Argentina/Buenos_Aires")

SCHEMA = """
CREATE TABLE IF NOT EXISTS paper_portfolios (
    id BIGSERIAL PRIMARY KEY,
    owner_key BIGINT NOT NULL,
    owner_chat_id BIGINT,
    status TEXT NOT NULL DEFAULT 'active',
    opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    baseline_snapshot_at TIMESTAMPTZ,
    baseline_cash_ars DOUBLE PRECISION NOT NULL DEFAULT 0,
    cash_ars DOUBLE PRECISION NOT NULL DEFAULT 0,
    baseline_positions JSONB NOT NULL DEFAULT '[]'::jsonb,
    positions JSONB NOT NULL DEFAULT '[]'::jsonb,
    last_run_id TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    closed_at TIMESTAMPTZ
);
CREATE UNIQUE INDEX IF NOT EXISTS paper_portfolios_one_active_owner
    ON paper_portfolios(owner_key) WHERE status = 'active';
CREATE TABLE IF NOT EXISTS paper_portfolio_runs (
    id BIGSERIAL PRIMARY KEY,
    portfolio_id BIGINT NOT NULL REFERENCES paper_portfolios(id),
    source_run_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    orders JSONB NOT NULL DEFAULT '[]'::jsonb,
    UNIQUE(portfolio_id, source_run_id)
);
"""

def _owner_key(owner_chat_id: int | None) -> int:
    return int(owner_chat_id or 0)

def _json(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if hasattr(value, "__dataclass_fields__"):
        return {k: _json(v) for k, v in value.__dict__.items()}
    if isinstance(value, dict):
        return {str(k): _json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json(v) for v in value]
    return value

def _clean_positions(raw: list[dict]) -> list[dict]:
    positions = []
    for item in raw or []:
        ticker = str(item.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        row = dict(item)
        row["ticker"] = ticker
        row["quantity"] = float(row.get("quantity") or 0)
        row["current_price"] = float(row.get("current_price") or row.get("price") or 0)
        row["market_value"] = float(row.get("market_value") or row["quantity"] * row["current_price"])
        positions.append(row)
    return positions

async def initialize(database_url: str, owner_chat_id: int | None) -> dict:
    """Copy the newest same-day broker snapshot once; never silently reset an active replica."""
    db = PortfolioDatabase(database_url)
    await db.connect()
    try:
        async with db._pool.acquire() as conn:
            await conn.execute(SCHEMA)
            active = await conn.fetchrow(
                "SELECT id FROM paper_portfolios WHERE owner_key=$1 AND status='active'",
                _owner_key(owner_chat_id),
            )
            if active:
                return {"ok": False, "reason": "active", "portfolio_id": int(active["id"])}
        snapshot = await db.get_latest_snapshot(owner_chat_id=owner_chat_id)
        if not snapshot:
            return {"ok": False, "reason": "missing_snapshot"}
        stamp = snapshot.get("scraped_at") or snapshot.get("timestamp") or snapshot.get("created_at")
        if not stamp:
            return {"ok": False, "reason": "missing_timestamp"}
        if isinstance(stamp, str):
            stamp = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
        local_stamp = stamp.astimezone(ART) if stamp.tzinfo else stamp.replace(tzinfo=ART)
        if local_stamp.date() != datetime.now(ART).date():
            return {"ok": False, "reason": "snapshot_not_today", "snapshot_at": local_stamp.isoformat()}
        positions = _clean_positions(snapshot.get("positions") or [])
        cash = float(snapshot.get("cash_ars") or 0)
        async with db._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO paper_portfolios
                    (owner_key, owner_chat_id, baseline_snapshot_at, baseline_cash_ars,
                     cash_ars, baseline_positions, positions)
                VALUES ($1,$2,$3,$4,$4,$5::jsonb,$5::jsonb)
                RETURNING id, opened_at
                """,
                _owner_key(owner_chat_id), owner_chat_id, local_stamp, cash,
                json.dumps(positions),
            )
        return {"ok": True, "portfolio_id": int(row["id"]), "opened_at": row["opened_at"].isoformat(),
                "position_count": len(positions), "cash_ars": cash}
    finally:
        await db.close()

async def apply_formal_plan(database_url: str, owner_chat_id: int | None, run_id: str, plan: Any) -> bool:
    """Fill the isolated replica at planned quantity/price once per formal analysis run."""
    if not run_id or plan is None:
        return False
    db = PortfolioDatabase(database_url)
    await db.connect()
    try:
        async with db._pool.acquire() as conn:
            await conn.execute(SCHEMA)
            async with conn.transaction():
                portfolio = await conn.fetchrow(
                    "SELECT * FROM paper_portfolios WHERE owner_key=$1 AND status='active' FOR UPDATE",
                    _owner_key(owner_chat_id),
                )
                if not portfolio:
                    return False
                orders = []
                for order in (list(getattr(plan, "sell_orders", [])) + list(getattr(plan, "buy_orders", []))):
                    orders.append(_json(order))
                inserted = await conn.fetchval(
                    """INSERT INTO paper_portfolio_runs(portfolio_id, source_run_id, orders)
                       VALUES($1,$2,$3::jsonb) ON CONFLICT DO NOTHING RETURNING id""",
                    portfolio["id"], run_id, json.dumps(orders),
                )
                if inserted is None:
                    return False
                positions = _clean_positions(json.loads(portfolio["positions"]) if isinstance(portfolio["positions"], str) else portfolio["positions"])
                by_ticker = {p["ticker"]: p for p in positions}
                cash = float(portfolio["cash_ars"])
                for order in orders:
                    ticker = str(order.get("ticker") or "").upper()
                    side = str(order.get("side") or "").upper()
                    amount = max(0.0, float(order.get("amount_ars") or 0))
                    qty = max(0.0, float(order.get("quantity_est") or order.get("planned_qty") or 0))
                    price = max(0.0, float(order.get("reference_price") or 0))
                    if not ticker or not qty:
                        continue
                    pos = by_ticker.get(ticker)
                    if side == "SELL":
                        if not pos:
                            continue
                        sold = min(qty, float(pos["quantity"]))
                        fraction = sold / float(pos["quantity"]) if pos["quantity"] else 0
                        proceeds = amount if sold >= float(pos["quantity"]) else amount * fraction
                        cash += proceeds * 0.9925
                        pos["quantity"] = max(0.0, float(pos["quantity"]) - sold)
                        pos["current_price"] = price or pos["current_price"]
                        pos["market_value"] = pos["quantity"] * pos["current_price"]
                    elif side == "BUY":
                        spend = min(amount, cash / 1.0075)
                        bought = min(qty, spend / price) if price > 0 else qty
                        if bought <= 0:
                            continue
                        cash -= spend * 1.0075
                        if not pos:
                            pos = {"ticker": ticker, "quantity": 0.0, "current_price": price, "market_value": 0.0}
                            by_ticker[ticker] = pos
                        old_qty = float(pos["quantity"])
                        pos["quantity"] = old_qty + bought
                        pos["current_price"] = price or float(pos.get("current_price") or 0)
                        pos["market_value"] = pos["quantity"] * pos["current_price"]
                positions = [p for p in by_ticker.values() if float(p["quantity"]) > 1e-9]
                await conn.execute(
                    """UPDATE paper_portfolios SET positions=$2::jsonb, cash_ars=$3,
                       last_run_id=$4, updated_at=NOW() WHERE id=$1""",
                    portfolio["id"], json.dumps(positions), cash, run_id,
                )
                return True
    finally:
        await db.close()

async def status(database_url: str, owner_chat_id: int | None) -> dict | None:
    db = PortfolioDatabase(database_url)
    await db.connect()
    try:
        async with db._pool.acquire() as conn:
            await conn.execute(SCHEMA)
            row = await conn.fetchrow(
                "SELECT * FROM paper_portfolios WHERE owner_key=$1 AND status='active'",
                _owner_key(owner_chat_id),
            )
            if not row:
                return None
            positions = _clean_positions(json.loads(row["positions"]) if isinstance(row["positions"], str) else row["positions"])
            baseline = _clean_positions(json.loads(row["baseline_positions"]) if isinstance(row["baseline_positions"], str) else row["baseline_positions"])
            tickers = [p["ticker"] for p in positions]
            marks = await conn.fetch(
                """SELECT DISTINCT ON (ticker) ticker, last_price::float AS last_price, ts
                   FROM market_prices
                   WHERE ticker = ANY($1::text[]) AND last_price > 0
                   ORDER BY ticker, ts DESC""",
                tickers,
            ) if tickers else []
        mark_by_ticker = {str(r["ticker"]).upper(): r for r in marks}
        for position in positions:
            mark = mark_by_ticker.get(position["ticker"])
            if mark:
                position["current_price"] = float(mark["last_price"])
                position["market_value"] = position["quantity"] * position["current_price"]
        baseline_nav = float(row["baseline_cash_ars"]) + sum(float(p["market_value"]) for p in baseline)
        nav = float(row["cash_ars"]) + sum(float(p["market_value"]) for p in positions)
        marked_at = max((m["ts"] for m in marks), default=None)
        return {"id": int(row["id"]), "opened_at": row["opened_at"].isoformat(),
                "cash_ars": float(row["cash_ars"]), "positions": positions,
                "nav_ars": nav, "baseline_nav_ars": baseline_nav,
                "pnl_ars": nav-baseline_nav, "pnl_pct": (nav/baseline_nav-1) if baseline_nav else None,
                "last_run_id": row["last_run_id"],
                "marked_at": marked_at.isoformat() if marked_at else None}
    finally:
        await db.close()

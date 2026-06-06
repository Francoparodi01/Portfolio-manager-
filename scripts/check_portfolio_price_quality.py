"""Read-only portfolio price quality check.

Compares the latest portfolio snapshot for a date against same-day
market_prices. It does not write to DB.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

import asyncpg

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.collector.portfolio_quality import (
    normalize_positions_with_fresh_market_prices,
    price_discrepancy_warnings,
)
from src.core.config import get_config


ART = ZoneInfo("America/Argentina/Buenos_Aires")


def _fmt_dt(value) -> str:
    if value is None:
        return "-"
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.astimezone(ART).strftime("%d/%m/%Y %H:%M:%S")
    return str(value)


def _money(value) -> str:
    if value is None:
        return "-"
    return f"${float(value):,.2f}"


def _pct(value) -> str:
    if value is None:
        return "-"
    return f"{float(value) * 100:+.2f}%"


async def _load_snapshot(conn: asyncpg.Connection, target_day: date | None):
    if target_day is None:
        return await conn.fetchrow(
            """
            SELECT snapshot_id, scraped_at, total_value_ars, cash_ars
            FROM portfolio_snapshots
            ORDER BY scraped_at DESC
            LIMIT 1
            """
        )
    return await conn.fetchrow(
        """
        SELECT snapshot_id, scraped_at, total_value_ars, cash_ars
        FROM portfolio_snapshots
        WHERE (scraped_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date = $1
        ORDER BY scraped_at DESC
        LIMIT 1
        """,
        target_day,
    )


async def main() -> int:
    parser = argparse.ArgumentParser(description="Chequeo read-only de precios portfolio vs market_prices")
    parser.add_argument("--date", help="Fecha ART YYYY-MM-DD. Default: ultimo snapshot.")
    parser.add_argument("--threshold", type=float, default=0.05, help="Umbral de discrepancia. Default: 0.05")
    parser.add_argument("--fail-on-warning", action="store_true", help="Exit 2 si hay discrepancias >= threshold")
    args = parser.parse_args()

    target_day = date.fromisoformat(args.date) if args.date else None
    dsn = get_config().database.url.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    try:
        snap = await _load_snapshot(conn, target_day)
        if not snap:
            print("No hay snapshot para la fecha solicitada.")
            return 1

        positions = await conn.fetch(
            """
            SELECT ticker, asset_type, currency, quantity, avg_cost, current_price,
                   market_value, unrealized_pnl, unrealized_pnl_pct, weight_in_portfolio
            FROM positions
            WHERE snapshot_id = $1
            ORDER BY market_value DESC NULLS LAST
            """,
            snap["snapshot_id"],
        )
        tickers = [str(row["ticker"]).upper() for row in positions if row["ticker"]]
        snapshot_day = snap["scraped_at"].astimezone(ART).date()
        market_rows = await conn.fetch(
            """
            SELECT DISTINCT ON (ticker)
                   ticker, asset_type, currency, last_price, change_pct_1d, ts
            FROM market_prices
            WHERE ticker = ANY($1::text[])
              AND (ts AT TIME ZONE 'America/Argentina/Buenos_Aires')::date = $2
            ORDER BY ticker, ts DESC
            """,
            tickers,
            snapshot_day,
        )
    finally:
        await conn.close()

    raw_positions = [dict(row) for row in positions]
    normalized = normalize_positions_with_fresh_market_prices(
        raw_positions,
        [dict(row) for row in market_rows],
        discrepancy_threshold=args.threshold,
    )
    warnings = price_discrepancy_warnings(normalized, threshold=args.threshold)
    snapshot_invested = sum(float(p.get("market_value") or 0) for p in raw_positions)
    normalized_invested = sum(float(p.get("market_value") or 0) for p in normalized)
    cash = float(snap["cash_ars"] or 0)

    print("Portfolio price quality")
    print(f"Snapshot: {_fmt_dt(snap['scraped_at'])}")
    print(f"Positions: {len(raw_positions)} | market coverage: {len(market_rows)}/{len(raw_positions)}")
    print(f"Snapshot invested:        {_money(snapshot_invested)}")
    print(f"Normalized invested:      {_money(normalized_invested)}")
    print(f"Snapshot reported value:  {_money(snap['total_value_ars'])}")
    print(f"Normalized account +cash: {_money(normalized_invested + cash)}")

    if not warnings:
        print(f"OK: sin discrepancias >= {_pct(args.threshold)}")
        return 0

    print(f"Warnings: {len(warnings)} discrepancia(s) >= {_pct(args.threshold)}")
    for item in warnings:
        print(
            "- {ticker}: snapshot {snapshot} vs market {market} | diff {diff}".format(
                ticker=item.get("ticker"),
                snapshot=_money(item.get("snapshot_price")),
                market=_money(item.get("market_price")),
                diff=_pct(item.get("discrepancy_pct")),
            )
        )
    return 2 if args.fail_on_warning else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

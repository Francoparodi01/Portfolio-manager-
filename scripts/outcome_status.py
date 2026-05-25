"""
scripts/outcome_status.py

Read-only diagnostic for decision outcomes.

It explains why performance/outcomes may still be zero:
- decisions that are not mature yet;
- decisions with no entry price;
- rows quarantined as legacy_external;
- rows that are mature and can be updated by update_outcomes.py.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import asyncpg

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.config import get_config


ART = ZoneInfo("America/Argentina/Buenos_Aires")


def _fmt_dt(value) -> str:
    if value is None:
        return "-"
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.astimezone(ART).strftime("%Y-%m-%d %H:%M ART")
    return str(value)


def _fmt_money(value) -> str:
    if value is None:
        return "-"
    return f"{float(value):,.2f}"


def _row(values: list[str], widths: list[int]) -> str:
    return "  " + "  ".join(str(v)[:w].ljust(w) for v, w in zip(values, widths))


async def build_report(days: int, limit: int) -> str:
    cfg = get_config()
    dsn = cfg.database.url.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)

    try:
        totals = await conn.fetchrow(
            """
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE outcome_5d IS NOT NULL) AS closed_5d,
                COUNT(*) FILTER (WHERE outcome_10d IS NOT NULL) AS closed_10d,
                COUNT(*) FILTER (WHERE outcome_20d IS NOT NULL) AS closed_20d,
                COUNT(*) FILTER (
                    WHERE outcome_5d IS NULL
                      AND COALESCE(outcome_basis, '') <> 'legacy_external'
                      AND price_at_decision IS NOT NULL
                      AND price_at_decision > 0
                      AND decision IN ('BUY','SELL')
                      AND decided_at <= NOW() - INTERVAL '5 days'
                ) AS mature_ready,
                COUNT(*) FILTER (
                    WHERE outcome_5d IS NULL
                      AND COALESCE(outcome_basis, '') <> 'legacy_external'
                      AND price_at_decision IS NOT NULL
                      AND price_at_decision > 0
                      AND decision IN ('BUY','SELL')
                      AND decided_at > NOW() - INTERVAL '5 days'
                ) AS not_mature,
                COUNT(*) FILTER (
                    WHERE outcome_5d IS NULL
                      AND COALESCE(outcome_basis, '') <> 'legacy_external'
                      AND (price_at_decision IS NULL OR price_at_decision <= 0)
                      AND decision IN ('BUY','SELL')
                ) AS missing_entry_price,
                COUNT(*) FILTER (
                    WHERE outcome_5d IS NULL
                      AND outcome_basis = 'legacy_external'
                      AND decision IN ('BUY','SELL')
                ) AS legacy_external
            FROM decision_log
            WHERE decided_at >= NOW() - ($1::int * INTERVAL '1 day')
              AND decision IN ('BUY','SELL')
            """,
            days,
        )

        groups = await conn.fetch(
            """
            SELECT
                COALESCE(source, 'NULL') AS source,
                COALESCE(status, 'NULL') AS status,
                decision,
                COALESCE(outcome_basis, 'NULL') AS basis,
                COUNT(*) AS total,
                COUNT(price_at_decision) AS with_entry_price,
                COUNT(outcome_5d) AS closed_5d,
                MIN(decided_at) AS first_at,
                MAX(decided_at) AS last_at
            FROM decision_log
            WHERE decided_at >= NOW() - ($1::int * INTERVAL '1 day')
              AND decision IN ('BUY','SELL')
            GROUP BY source, status, decision, basis
            ORDER BY total DESC
            """,
            days,
        )

        upcoming = await conn.fetch(
            """
            SELECT
                id,
                ticker,
                decision,
                COALESCE(status, '-') AS status,
                COALESCE(source, '-') AS source,
                price_at_decision,
                decided_at,
                decided_at + INTERVAL '5 days' AS due_5d,
                outcome_basis
            FROM decision_log
            WHERE outcome_5d IS NULL
              AND COALESCE(outcome_basis, '') <> 'legacy_external'
              AND price_at_decision IS NOT NULL
              AND price_at_decision > 0
              AND decision IN ('BUY','SELL')
            ORDER BY decided_at ASC
            LIMIT $1
            """,
            limit,
        )
    finally:
        await conn.close()

    lines: list[str] = []
    lines.append("OUTCOME STATUS")
    lines.append("=" * 72)
    lines.append(f"Window: last {days} days")
    lines.append("")
    lines.append("Summary")
    lines.append(f"  Decisions BUY/SELL:        {int(totals['total'] or 0)}")
    lines.append(f"  Closed outcome_5d:         {int(totals['closed_5d'] or 0)}")
    lines.append(f"  Closed outcome_10d:        {int(totals['closed_10d'] or 0)}")
    lines.append(f"  Closed outcome_20d:        {int(totals['closed_20d'] or 0)}")
    lines.append(f"  Mature and update-ready:   {int(totals['mature_ready'] or 0)}")
    lines.append(f"  Not mature yet:            {int(totals['not_mature'] or 0)}")
    lines.append(f"  Missing entry price:       {int(totals['missing_entry_price'] or 0)}")
    lines.append(f"  Legacy external excluded:  {int(totals['legacy_external'] or 0)}")
    lines.append("")

    mature_ready = int(totals["mature_ready"] or 0)
    not_mature = int(totals["not_mature"] or 0)
    missing_entry = int(totals["missing_entry_price"] or 0)
    legacy = int(totals["legacy_external"] or 0)
    if mature_ready:
        lines.append("Reading: there are mature canonical rows. Run scripts/update_outcomes.py.")
    elif not_mature:
        lines.append("Reading: zero outcomes is expected; canonical rows are still maturing.")
    elif missing_entry or legacy:
        lines.append("Reading: zero outcomes is structural; rows either lack entry price or were quarantined as legacy_external.")
    else:
        lines.append("Reading: there are no BUY/SELL rows eligible for outcome calculation.")
    lines.append("")

    if groups:
        widths = [16, 14, 8, 16, 7, 8, 8, 16, 16]
        lines.append("Groups")
        lines.append(_row(["source", "status", "decision", "basis", "total", "entry", "5d", "first", "last"], widths))
        lines.append("  " + "-" * (sum(widths) + 2 * (len(widths) - 1)))
        for row in groups:
            lines.append(
                _row(
                    [
                        row["source"],
                        row["status"],
                        row["decision"],
                        row["basis"],
                        str(row["total"]),
                        str(row["with_entry_price"]),
                        str(row["closed_5d"]),
                        _fmt_dt(row["first_at"]),
                        _fmt_dt(row["last_at"]),
                    ],
                    widths,
                )
            )
        lines.append("")

    if upcoming:
        widths = [5, 8, 6, 12, 14, 14, 19, 19]
        lines.append("Next canonical rows with entry price")
        lines.append(_row(["id", "ticker", "side", "status", "source", "entry", "decided", "due_5d"], widths))
        lines.append("  " + "-" * (sum(widths) + 2 * (len(widths) - 1)))
        for row in upcoming:
            lines.append(
                _row(
                    [
                        str(row["id"]),
                        row["ticker"],
                        row["decision"],
                        row["status"],
                        row["source"],
                        _fmt_money(row["price_at_decision"]),
                        _fmt_dt(row["decided_at"]),
                        _fmt_dt(row["due_5d"]),
                    ],
                    widths,
                )
            )

    return "\n".join(lines)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only outcome maturity diagnostic")
    parser.add_argument("--days", type=int, default=180)
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()
    print(await build_report(days=args.days, limit=args.limit))


if __name__ == "__main__":
    asyncio.run(main())

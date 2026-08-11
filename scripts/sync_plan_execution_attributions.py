"""Rebuild persisted plan-to-movement audit links without changing source data."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

import asyncpg

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.plan_follow_attribution import sync_plan_execution_attributions  # noqa: E402
from src.core.config import get_config  # noqa: E402


async def main() -> None:
    parser = argparse.ArgumentParser(description="Persist normalized followed-plan links")
    parser.add_argument("--days", type=int, default=180)
    parser.add_argument("--match-window-sessions", type=int, default=2)
    args = parser.parse_args()

    cfg = get_config()
    dsn = cfg.database.url.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    try:
        summary = await sync_plan_execution_attributions(
            conn,
            days=args.days,
            match_window_sessions=args.match_window_sessions,
        )
    finally:
        await conn.close()

    print(
        "Plan follow sync: "
        f"plans={summary['plans']} movements={summary['movements']} "
        f"attributions={summary['attributions']} eligible={summary['eligible']} "
        f"ambiguous={summary['ambiguous']}"
    )


if __name__ == "__main__":
    asyncio.run(main())

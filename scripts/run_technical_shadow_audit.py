from __future__ import annotations

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.technical_shadow_audit import (
    build_technical_shadow_audit,
    load_technical_shadow_rows,
    render_technical_shadow_audit,
)
from src.core.config import get_config


async def main(days: int) -> None:
    cfg = get_config()
    rows = await load_technical_shadow_rows(cfg.database.url, days=days)
    print(render_technical_shadow_audit(build_technical_shadow_audit(rows)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Audita technical-shadow-v2 contra baseline")
    parser.add_argument("--days", type=int, default=365)
    args = parser.parse_args()
    asyncio.run(main(args.days))

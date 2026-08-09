from __future__ import annotations

import asyncio
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.core.config import get_config
from src.core.report_artifacts import ensure_report_artifacts_schema


async def main() -> None:
    cfg = get_config()
    await ensure_report_artifacts_schema(cfg.database.url)
    print("telegram_report_artifacts schema OK")


if __name__ == "__main__":
    asyncio.run(main())

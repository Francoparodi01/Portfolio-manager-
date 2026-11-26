"""
/auditoria: verdict, cohorts, bot vs human, score calibration, data health and
alerts in one report, all computed on the same data layer as /viability.

Read-only except for refreshing the derived plan/movement attributions, which
/viability also does. The per-topic commands stay as drill-downs.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

import asyncpg

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.plan_follow_attribution import sync_plan_execution_attributions  # noqa: E402
from src.analysis.rules_freeze import (  # noqa: E402
    check_manifest,
    freeze_note as freeze_note_for,
    load_manifest,
)
from src.analysis.unified_audit import render_unified_audit, run_unified_audit  # noqa: E402
from src.analysis.viability_audit import ViabilityAuditConfig  # noqa: E402
from src.collector.notifier import TelegramNotifier  # noqa: E402
from src.core.config import get_config  # noqa: E402
from src.core.logger import get_logger  # noqa: E402
from src.core.telegram_format import validate_telegram_html  # noqa: E402

logger = get_logger(__name__)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Unified read-only audit")
    parser.add_argument("--days", type=int, default=180)
    parser.add_argument("--cost-bps", type=float, default=75.0)
    parser.add_argument("--min-sample", type=int, default=30)
    parser.add_argument("--benchmark", type=str, default="SPY", help="CEDEAR ARS benchmark ticker; 'none' to skip")
    parser.add_argument("--no-telegram", action="store_true")
    args = parser.parse_args()

    cfg = get_config()
    conn = await asyncpg.connect(cfg.database.url.replace("postgresql+asyncpg://", "postgresql://"))
    try:
        await sync_plan_execution_attributions(conn, days=args.days)
    finally:
        await conn.close()

    frozen_since = freeze_note = None
    manifest = load_manifest()
    if manifest is not None:
        frozen_since = manifest.get("frozen_at")
        freeze_note = freeze_note_for(check_manifest(manifest))

    report = await run_unified_audit(
        ViabilityAuditConfig(
            database_url=cfg.database.url,
            days=args.days,
            cost_bps=args.cost_bps,
            min_sample=args.min_sample,
            benchmark_ticker=None if args.benchmark.lower() == "none" else args.benchmark.upper(),
            frozen_since=frozen_since,
            freeze_note=freeze_note,
        )
    )
    text = render_unified_audit(report)
    validate_telegram_html(text)
    print(text)
    if args.no_telegram:
        return
    try:
        TelegramNotifier(cfg.scraper.telegram_bot_token, cfg.scraper.telegram_chat_id).send_raw(text)
    except Exception as exc:  # noqa: BLE001
        logger.warning("No pude enviar la auditoria unificada a Telegram: %s", exc)


if __name__ == "__main__":
    asyncio.run(main())

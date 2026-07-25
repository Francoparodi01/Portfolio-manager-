"""
Run the read-only 180d viability audit.

The report separates bot-only and manual-only execution, measures
5d/10d/20d/40d independently, and checks positive IC, lower drawdown, and
better net EV after costs. It does not change thresholds or planner behavior.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.viability_audit import (  # noqa: E402
    ViabilityAuditConfig,
    render_viability_chart,
    render_viability_audit,
    run_viability_audit,
)
from src.collector.notifier import TelegramNotifier  # noqa: E402
from src.core.config import get_config  # noqa: E402
from src.core.logger import get_logger  # noqa: E402
from src.core.telegram_format import validate_telegram_html  # noqa: E402


logger = get_logger(__name__)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only viability audit")
    parser.add_argument("--days", type=int, default=180)
    parser.add_argument("--since", type=str, default=None)
    parser.add_argument("--cost-bps", type=float, default=75.0)
    parser.add_argument("--min-sample", type=int, default=30)
    parser.add_argument("--chart-out", type=str, default=None)
    parser.add_argument("--no-telegram", action="store_true")
    args = parser.parse_args()

    cfg = get_config()
    report = await run_viability_audit(
        ViabilityAuditConfig(
            database_url=cfg.database.url,
            days=args.days,
            since=args.since,
            cost_bps=args.cost_bps,
            min_sample=args.min_sample,
        )
    )
    text = render_viability_audit(report)
    chart_path: Path | None = None
    cleanup_chart = False
    if args.chart_out:
        chart_path = render_viability_chart(report, args.chart_out)
    elif not args.no_telegram:
        tmp = tempfile.NamedTemporaryFile(prefix="cocos_viability_", suffix=".png", delete=False)
        tmp.close()
        chart_path = render_viability_chart(report, tmp.name)
        cleanup_chart = True

    validate_telegram_html(text)

    print(text)
    if chart_path:
        print(f"\n[chart] {chart_path}")

    if args.no_telegram:
        return

    try:
        notifier = TelegramNotifier(
            cfg.scraper.telegram_bot_token,
            cfg.scraper.telegram_chat_id,
        )
        if chart_path:
            notifier.send_photo(
                chart_path,
                caption=f"<b>Viability Audit {int(report.days)}d</b>",
            )
        notifier.send_raw(text)
    except Exception as exc:
        logger.warning("No pude enviar viability audit a Telegram: %s", exc)
    finally:
        if cleanup_chart and chart_path:
            try:
                chart_path.unlink(missing_ok=True)
            except Exception:
                pass


if __name__ == "__main__":
    asyncio.run(main())

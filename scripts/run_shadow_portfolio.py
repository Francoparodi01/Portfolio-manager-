"""
/cartera_sombra: the real account cloned at a date, following every real plan
of the bot (plus a sells-only variant), against holding and the benchmark.

Read-only. Default start: the rules freeze date (config/decision_rules_freeze.json).
    python scripts/run_shadow_portfolio.py --desde 2026-07-01 --no-telegram
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.rules_freeze import load_manifest  # noqa: E402
from src.analysis.shadow_portfolio import (  # noqa: E402
    ShadowConfig,
    load_shadow_inputs,
    render_shadow,
    simulate_shadow,
)
from src.collector.notifier import TelegramNotifier  # noqa: E402
from src.core.config import get_config  # noqa: E402
from src.core.logger import get_logger  # noqa: E402
from src.core.telegram_format import validate_telegram_html  # noqa: E402

ART = ZoneInfo("America/Argentina/Buenos_Aires")
logger = get_logger(__name__)


def parse_day(value: str) -> datetime:
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=ART)
        except ValueError:
            continue
    raise SystemExit(f"Fecha inválida: {value} (usá DD/MM/YYYY)")


async def main() -> None:
    parser = argparse.ArgumentParser(description="Cartera sombra (solo lectura)")
    parser.add_argument("--desde", type=str, default=None)
    parser.add_argument("--hasta", type=str, default=None)
    parser.add_argument("--benchmark", type=str, default="SPY")
    parser.add_argument("--no-telegram", action="store_true")
    args = parser.parse_args()

    if args.desde:
        since = parse_day(args.desde)
    else:
        manifest = load_manifest()
        if manifest is None:
            raise SystemExit("Sin fecha de congelamiento: pasá --desde DD/MM/YYYY.")
        since = datetime.fromisoformat(manifest["frozen_at"])
    until = parse_day(args.hasta).replace(hour=23, minute=59) if args.hasta else datetime.now(tz=timezone.utc)

    cfg = get_config()
    inputs = await load_shadow_inputs(
        ShadowConfig(database_url=cfg.database.url, since=since, until=until, benchmark=args.benchmark.upper())
    )
    try:
        report = simulate_shadow(inputs)
        text = render_shadow(report, benchmark=args.benchmark.upper())
    except ValueError as exc:
        text = f"⚠️ <b>Cartera sombra</b>\n{exc}"
    validate_telegram_html(text)
    print(text)
    if args.no_telegram:
        return
    try:
        TelegramNotifier(cfg.scraper.telegram_bot_token, cfg.scraper.telegram_chat_id).send_raw(text)
    except Exception as exc:  # noqa: BLE001
        logger.warning("No pude enviar la cartera sombra a Telegram: %s", exc)


if __name__ == "__main__":
    asyncio.run(main())

import asyncio
from src.core.config import get_config
from src.collector.db import PortfolioDatabase
from src.analysis.pipeline import run_full_quant
from src.collector.notifier import TelegramNotifier


async def main():
    cfg = get_config()

    db = PortfolioDatabase(cfg.database.url)
    notifier = TelegramNotifier(
        cfg.scraper.telegram_bot_token,
        cfg.scraper.telegram_chat_id,
    )

    await db.connect()

    try:
        snapshot = await db.get_latest_snapshot()

        if not snapshot:
            print("No hay snapshot")
            return

        tickers = [p.ticker for p in snapshot.positions]
        print(f"Analizando quant: {tickers}")

        decisions = await run_full_quant(snapshot)

        report = format_quant_report(decisions)

        print(report)
        notifier.send_raw(report)

    finally:
        await db.close()


def format_quant_report(decisions: list[dict]) -> str:
    lines = []
    lines.append("=" * 60)

    for d in decisions:
        lines.append(
            f"[{d['decision']}]  {d['ticker']:6}  "
            f"conf={d['confidence']:.0%}  "
            f"target={d['target_weight']:.2%}"
        )
        if d.get("rationale"):
            lines.append(f"   → {d['rationale'][:120]}")

    lines.append("=" * 60)
    return "\n".join(lines)


if __name__ == "__main__":
    asyncio.run(main())
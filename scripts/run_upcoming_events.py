"""Render upcoming portfolio earnings from persisted issuer observations."""
from __future__ import annotations

import argparse
import asyncio
from datetime import date, datetime, timedelta, timezone
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.upcoming_earnings import (
    DEFAULT_UPCOMING_EARNINGS_DAYS,
    post_earnings_session_date,
    render_post_earnings_reactions_html,
    render_recent_exit_earnings_html,
    render_upcoming_earnings_report,
    upcoming_earnings_from_rows,
)
from src.collector.db import PortfolioDatabase
from src.collector.notifier import TelegramNotifier
from src.core.config import get_config
from src.core.logger import get_logger
from scripts.run_issuer_event_ingestion import run as refresh_issuer_events


logger = get_logger(__name__)


async def run(
    *,
    days: int,
    tickers: list[str],
    owner_chat_id: int | None,
    refresh: bool = False,
    refresh_sources: str = "yahoo,finnhub",
    recent_exit_days: int = 5,
    post_balance_days: int = 5,
) -> str:
    cfg = get_config()
    if refresh:
        try:
            await refresh_issuer_events(
                sources=refresh_sources.split(","),
                dry_run=False,
                timeout_seconds=12.0,
                sec_lookback_days=14,
                calendar_days=max(45, int(days)),
            )
        except Exception as exc:
            logger.warning(
                "No se pudo refrescar issuer events; uso datos persistidos: %s",
                type(exc).__name__,
            )

    db = PortfolioDatabase(cfg.database.url)
    await db.connect()
    try:
        selected_tickers = [str(ticker).upper().strip() for ticker in tickers if str(ticker).strip()]
        if not selected_tickers:
            snapshot = await db.get_latest_snapshot(owner_chat_id=owner_chat_id)
            selected_tickers = sorted({
                str(position.get("ticker") or "").upper().strip()
                for position in (snapshot or {}).get("positions") or []
                if str(position.get("ticker") or "").strip()
            })
        recent_exits = await db.get_recent_portfolio_exits(
            since=datetime.now(timezone.utc) - timedelta(days=max(0, int(recent_exit_days))),
            owner_chat_id=owner_chat_id,
        )
        exited_at_by_ticker = {
            str(row.get("ticker") or "").upper().strip(): row.get("exited_at")
            for row in recent_exits
            if str(row.get("ticker") or "").strip()
        }
        recent_exit_tickers = sorted(set(exited_at_by_ticker) - set(selected_tickers))
        today = date.today()
        rows = await db.get_upcoming_earnings_events(
            from_date=today - timedelta(days=max(7, int(post_balance_days) * 2 + 3)),
            to_date=today + timedelta(days=max(1, int(days))),
            tickers=selected_tickers + recent_exit_tickers,
            limit=100,
        )
        events = upcoming_earnings_from_rows(rows)
        reactions = []
        reacted_keys: set[str] = set()
        for event in events:
            session_date = post_earnings_session_date(event)
            if session_date > today or (today - session_date).days > max(0, int(post_balance_days)):
                continue
            reaction = await db.get_post_event_price_reaction(
                event.ticker,
                session_date=session_date,
            )
            if not reaction:
                continue
            reacted_keys.add(event.observation_key)
            exited_at = exited_at_by_ticker.get(event.ticker)
            exited_date = exited_at.date() if isinstance(exited_at, datetime) else None
            reaction.update(
                {
                    "event_date": event.event_date,
                    "scope_label": (
                        f"salida reciente {exited_date.strftime('%d/%m')}"
                        if event.ticker in recent_exit_tickers and exited_date
                        else "cartera actual"
                    ),
                }
            )
            reactions.append(reaction)
    finally:
        await db.close()
    upcoming_events = [
        event
        for event in events
        if event.observation_key not in reacted_keys and event.event_date >= today
    ]
    current_events = [event for event in upcoming_events if event.ticker in selected_tickers]
    recent_exit_events = [event for event in upcoming_events if event.ticker in recent_exit_tickers]
    report = render_upcoming_earnings_report(current_events, today=today)
    recent_lines = render_recent_exit_earnings_html(
        recent_exit_events,
        exited_at_by_ticker=exited_at_by_ticker,
        today=today,
    )
    reaction_lines = render_post_earnings_reactions_html(reactions)
    sections = [report]
    if recent_lines:
        sections.append("\n".join(recent_lines))
    if reaction_lines:
        sections.append("\n".join(reaction_lines))
    return "\n\n".join(sections)


def main() -> int:
    parser = argparse.ArgumentParser(description="Proximos balances de la cartera")
    parser.add_argument("--days", type=int, default=DEFAULT_UPCOMING_EARNINGS_DAYS)
    parser.add_argument("--tickers", nargs="*", default=[])
    parser.add_argument("--owner-chat-id", type=int, default=None)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument(
        "--refresh-sources",
        default=os.getenv("UPCOMING_EVENTS_REFRESH_SOURCES", "yahoo,finnhub"),
    )
    parser.add_argument(
        "--recent-exit-days",
        type=int,
        default=int(os.getenv("UPCOMING_EVENTS_RECENT_EXIT_DAYS", "5")),
    )
    parser.add_argument(
        "--post-balance-days",
        type=int,
        default=int(os.getenv("UPCOMING_EVENTS_POST_BALANCE_DAYS", "5")),
    )
    parser.add_argument("--no-telegram", action="store_true")
    args = parser.parse_args()
    report = asyncio.run(
        run(
            days=max(1, int(args.days)),
            tickers=args.tickers,
            owner_chat_id=args.owner_chat_id,
            refresh=args.refresh,
            refresh_sources=args.refresh_sources,
            recent_exit_days=max(0, int(args.recent_exit_days)),
            post_balance_days=max(0, int(args.post_balance_days)),
        )
    )
    print(report)
    if not args.no_telegram:
        cfg = get_config()
        TelegramNotifier(
            cfg.scraper.telegram_bot_token,
            cfg.scraper.telegram_chat_id,
        ).send_raw(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

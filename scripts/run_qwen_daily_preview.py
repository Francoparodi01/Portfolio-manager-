"""Read-only daily Qwen preview from the current production DB.

This script only runs SELECT queries, builds a MarketReportPacket, and either
prints it or sends it to a local Qwen/Ollama model for a validated preview. It
does not write to DB, publish to Telegram, restart services, or call scrapers.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.llm_narratives import (
    DEFAULT_MODEL,
    DEFAULT_OLLAMA_URL,
    DEFAULT_TIMEOUT_SECONDS,
    build_market_report_prompt,
    generate_market_narrative_with_ollama,
)
from src.analysis.llm_packet_builder import (
    build_market_report_packet,
    render_market_packet_statement_preview,
    render_market_narrative_preview,
)
from src.collector.live_portfolio import build_live_portfolio
from src.core.config import get_config


ART_TZ = ZoneInfo("America/Argentina/Buenos_Aires")


def _load_env_file(path: str | None) -> None:
    if not path:
        return
    env_path = Path(path)
    if not env_path.exists():
        raise SystemExit(f"Env file not found: {env_path}")
    try:
        from dotenv import load_dotenv

        load_dotenv(env_path, override=False)
        return
    except Exception:
        pass
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _default_live_env_file() -> str | None:
    candidates = [
        Path.cwd() / ".env",
        Path.cwd().parent / "cocos_copilot" / ".env",
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return None


async def _load_latest_snapshot(conn, *, owner_chat_id: int | None = None) -> dict:
    if owner_chat_id is None:
        row = await conn.fetchrow(
            """
            SELECT payload
            FROM raw_snapshots
            ORDER BY scraped_at DESC
            LIMIT 1
            """
        )
    else:
        row = await conn.fetchrow(
            """
            SELECT r.payload
            FROM raw_snapshots r
            JOIN portfolio_snapshots p USING (snapshot_id)
            WHERE p.owner_chat_id = $1
            ORDER BY r.scraped_at DESC
            LIMIT 1
            """,
            int(owner_chat_id),
        )
    if not row:
        raise RuntimeError("No raw_snapshots rows found")
    return json.loads(row["payload"])


async def _load_latest_prices(conn, tickers: list[str]) -> list[dict]:
    clean = sorted({str(ticker or "").upper() for ticker in tickers if str(ticker or "").strip()})
    if not clean:
        return []
    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (ticker)
            ticker, asset_type, currency, last_price, change_pct_1d, ts
        FROM market_prices
        WHERE ticker = ANY($1::text[])
          AND last_price IS NOT NULL
          AND last_price > 0
        ORDER BY ticker, ts DESC
        """,
        clean,
    )
    return [dict(row) for row in rows]


async def _load_previous_closes(conn, tickers: list[str], before_day: date) -> dict[str, float]:
    clean = sorted({str(ticker or "").upper() for ticker in tickers if str(ticker or "").strip()})
    if not clean:
        return {}
    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (ticker)
            ticker, close_price
        FROM market_candles
        WHERE ticker = ANY($1::text[])
          AND close_price IS NOT NULL
          AND close_price > 0
          AND ts::date < $2::date
        ORDER BY ticker, ts DESC
        """,
        clean,
        before_day,
    )
    return {str(row["ticker"]).upper(): float(row["close_price"]) for row in rows}


async def _build_live_portfolio_from_db(
    database_url: str,
    *,
    today: date,
    owner_chat_id: int | None = None,
) -> dict:
    import asyncpg

    dsn = database_url.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    try:
        snapshot = await _load_latest_snapshot(conn, owner_chat_id=owner_chat_id)
        tickers = [
            str(position.get("ticker") or "").upper()
            for position in snapshot.get("positions") or []
            if str(position.get("ticker") or "").strip()
        ]
        latest_prices = await _load_latest_prices(conn, tickers)
        previous_closes = await _load_previous_closes(conn, tickers, today)
    finally:
        await conn.close()

    for row in latest_prices:
        previous_close = previous_closes.get(str(row.get("ticker") or "").upper())
        if previous_close:
            row["previous_close_price"] = previous_close
    return build_live_portfolio(snapshot, latest_prices)


def _print_json(payload: dict) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2))


async def _run(args: argparse.Namespace) -> int:
    _load_env_file(args.env_file)
    database_url = args.database_url or get_config().database.url
    today = date.fromisoformat(args.date) if args.date else datetime.now(ART_TZ).date()

    live_portfolio = await _build_live_portfolio_from_db(
        database_url,
        today=today,
        owner_chat_id=args.owner_chat_id,
    )
    packet = build_market_report_packet(
        live_portfolio,
        run_id=args.run_id,
        as_of=datetime.now(timezone.utc),
    )

    if args.mode == "packet":
        _print_json(packet)
        return 0
    if args.mode == "prompt":
        print(build_market_report_prompt(packet))
        return 0
    if args.mode == "template":
        print(render_market_packet_statement_preview(packet))
        return 0

    narrative = await generate_market_narrative_with_ollama(
        packet,
        model=args.model,
        ollama_url=args.ollama_url,
        timeout_seconds=args.timeout_seconds,
    )
    if args.mode == "json":
        _print_json(narrative.to_dict())
    else:
        print(render_market_narrative_preview(narrative, packet))
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Read-only Qwen daily market preview")
    parser.add_argument(
        "--mode",
        choices=["text", "json", "packet", "prompt", "template"],
        default="text",
        help="Output mode. packet/prompt/template do not call Ollama.",
    )
    parser.add_argument("--env-file", default=_default_live_env_file())
    parser.add_argument("--database-url", default=None)
    parser.add_argument("--date", help="ART date YYYY-MM-DD for previous close lookup. Default: today.")
    parser.add_argument("--run-id", default="manual_qwen_preview")
    parser.add_argument("--owner-chat-id", type=int, default=None)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--ollama-url", default=DEFAULT_OLLAMA_URL)
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    raise SystemExit(asyncio.run(_run(parser.parse_args())))

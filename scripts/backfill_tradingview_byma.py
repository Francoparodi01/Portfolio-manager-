from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import string
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import aiohttp
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)

from src.collector.data.models import AssetType, Currency, MarketCandle
from src.collector.db import PortfolioDatabase
from src.core.config import get_config


SOURCE = "TRADINGVIEW_BYMA"
DEFAULT_BARS = 260
DEFAULT_MIN_ROWS = 60
SYMBOL_ALIASES = {
    "BA.C": "BAC",
    "BRKB": "BRKB",
    "C.I.": "C",
}


@dataclass
class BackfillTarget:
    ticker: str
    asset_type: str
    existing_rows: int


def _session_id(prefix: str) -> str:
    suffix = "".join(random.choice(string.ascii_lowercase) for _ in range(12))
    return f"{prefix}_{suffix}"


def _tv_frame(payload: dict) -> str:
    raw = json.dumps(payload, separators=(",", ":"))
    return f"~m~{len(raw)}~m~{raw}"


def _parse_tv_frames(raw: str) -> list[dict]:
    messages: list[dict] = []
    i = 0
    while i < len(raw):
        if not raw.startswith("~m~", i):
            break
        j = raw.find("~m~", i + 3)
        if j < 0:
            break
        length = int(raw[i + 3 : j])
        start = j + 3
        body = raw[start : start + length]
        i = start + length
        if body == "~h~":
            continue
        try:
            messages.append(json.loads(body))
        except json.JSONDecodeError:
            continue
    return messages


def _tv_symbol(ticker: str) -> str:
    ticker = ticker.upper().strip()
    return f"BYMA:{SYMBOL_ALIASES.get(ticker, ticker)}"


def _long_ticker(ticker: str) -> str:
    return f"TV:BYMA:{_tv_symbol(ticker).split(':', 1)[1]}"


def _asset_type(value: str) -> AssetType:
    try:
        return AssetType(str(value).upper())
    except ValueError:
        return AssetType.UNKNOWN


async def fetch_tv_candles(
    ticker: str,
    asset_type: str,
    *,
    bars: int,
    interval: str = "1D",
    timeout_s: int = 30,
) -> list[MarketCandle]:
    tv_symbol = _tv_symbol(ticker)
    chart_session = _session_id("cs")
    quote_session = _session_id("qs")
    resolved_symbol = "symbol_1"
    url = "wss://data.tradingview.com/socket.io/websocket?from=chart%2F"
    headers = {
        "Origin": "https://www.tradingview.com",
        "User-Agent": "Mozilla/5.0",
    }

    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.ws_connect(url, heartbeat=20, timeout=timeout_s) as ws:
            async def send(method: str, params: list) -> None:
                await ws.send_str(_tv_frame({"m": method, "p": params}))

            await send("set_auth_token", ["unauthorized_user_token"])
            await send("chart_create_session", [chart_session, ""])
            await send("quote_create_session", [quote_session])
            symbol_payload = json.dumps(
                {
                    "symbol": tv_symbol,
                    "adjustment": "splits",
                    "session": "regular",
                },
                separators=(",", ":"),
            )
            await send("resolve_symbol", [chart_session, resolved_symbol, f"={symbol_payload}"])
            await send(
                "create_series",
                [chart_session, "s1", "s1", resolved_symbol, interval, int(bars), ""],
            )

            deadline = asyncio.get_running_loop().time() + timeout_s
            last_error: str | None = None
            while asyncio.get_running_loop().time() < deadline:
                msg = await ws.receive(timeout=5)
                if msg.type != aiohttp.WSMsgType.TEXT:
                    continue
                for item in _parse_tv_frames(msg.data):
                    method = item.get("m")
                    params = item.get("p") or []
                    if method in {"critical_error", "symbol_error"}:
                        last_error = str(params)
                        continue
                    if method != "timescale_update" or len(params) < 2:
                        continue
                    series = (params[1] or {}).get("s1") or {}
                    rows = series.get("s") or []
                    candles = []
                    for row in rows:
                        values = row.get("v") or []
                        if len(values) < 5:
                            continue
                        try:
                            ts, open_, high, low, close = values[:5]
                            volume = values[5] if len(values) > 5 else 0.0
                            candles.append(
                                MarketCandle(
                                    ticker=ticker.upper(),
                                    long_ticker=_long_ticker(ticker),
                                    asset_type=_asset_type(asset_type),
                                    currency=Currency.ARS,
                                    venue="BYMA",
                                    interval="1d",
                                    ts=datetime.fromtimestamp(float(ts), tz=timezone.utc),
                                    open_price=float(open_),
                                    high_price=float(high),
                                    low_price=float(low),
                                    close_price=float(close),
                                    volume=float(volume or 0),
                                    source=SOURCE,
                                )
                            )
                        except Exception:
                            continue
                    return sorted(candles, key=lambda candle: candle.ts)

            raise RuntimeError(last_error or f"timeout TradingView para {tv_symbol}")


async def _targets(
    db: PortfolioDatabase,
    *,
    tickers: Iterable[str],
    asset_type: str,
    min_rows: int,
    all_assets: bool,
) -> list[BackfillTarget]:
    latest = await db.get_cocos_universe_assets()
    wanted = {ticker.upper().strip() for ticker in tickers if ticker.strip()}
    assets = [
        item
        for item in latest
        if (asset_type == "ALL" or str(item.get("asset_type", "")).upper() == asset_type)
        and (not wanted or str(item.get("ticker", "")).upper() in wanted)
    ]

    targets: list[BackfillTarget] = []
    for asset in sorted(assets, key=lambda row: (row.get("asset_type", ""), row.get("ticker", ""))):
        ticker = str(asset["ticker"]).upper()
        atype = str(asset.get("asset_type") or "UNKNOWN").upper()
        rows = await db.get_market_candles(ticker, asset_type=atype, interval="1d", limit=min_rows)
        if all_assets or len(rows) < min_rows:
            targets.append(BackfillTarget(ticker=ticker, asset_type=atype, existing_rows=len(rows)))
    return targets


async def _main() -> None:
    parser = argparse.ArgumentParser(description="Backfill OHLCV desde TradingView/BYMA hacia market_candles")
    parser.add_argument("--tickers", nargs="*", default=[], help="Tickers puntuales. Default: universo Cocos")
    parser.add_argument("--asset-type", default="ALL", choices=["ALL", "ACCION", "CEDEAR"])
    parser.add_argument("--bars", type=int, default=DEFAULT_BARS)
    parser.add_argument("--min-rows", type=int, default=DEFAULT_MIN_ROWS)
    parser.add_argument("--all", action="store_true", help="Backfill incluso si ya tiene min-rows")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, help="Limitar cantidad de targets")
    parser.add_argument("--pause-s", type=float, default=0.8)
    parser.add_argument("--output-dir", type=Path, default=Path("outputs/tradingview_byma"))
    args = parser.parse_args()

    db = PortfolioDatabase(get_config().database.url)
    await db.connect()
    imported = errors = fetched = 0
    try:
        targets = await _targets(
            db,
            tickers=args.tickers,
            asset_type=args.asset_type,
            min_rows=args.min_rows,
            all_assets=args.all,
        )
        if args.limit:
            targets = targets[: args.limit]

        args.output_dir.mkdir(parents=True, exist_ok=True)
        print(
            f"targets={len(targets)} source={SOURCE} bars={args.bars} "
            f"mode={'all' if args.all else f'missing<{args.min_rows}'} dry_run={args.dry_run}"
        )

        for index, target in enumerate(targets, start=1):
            try:
                candles = await fetch_tv_candles(
                    target.ticker,
                    target.asset_type,
                    bars=args.bars,
                )
                fetched += len(candles)
                path = args.output_dir / f"{target.ticker.lower()}_{target.asset_type.lower()}_tv_byma.json"
                payload = [
                    {
                        "ticker": c.ticker,
                        "long_ticker": c.long_ticker,
                        "asset_type": c.asset_type.value,
                        "currency": c.currency.value,
                        "venue": c.venue,
                        "interval": c.interval,
                        "ts": c.ts.isoformat(),
                        "open_price": c.open_price,
                        "high_price": c.high_price,
                        "low_price": c.low_price,
                        "close_price": c.close_price,
                        "volume": c.volume,
                        "source": c.source,
                    }
                    for c in candles
                ]
                path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
                saved = 0 if args.dry_run else await db.save_market_candles(candles)
                imported += saved
                print(
                    f"{index}/{len(targets)} {target.asset_type} {target.ticker}: "
                    f"existing={target.existing_rows} fetched={len(candles)} imported={saved}"
                )
            except Exception as exc:
                errors += 1
                print(f"{index}/{len(targets)} {target.asset_type} {target.ticker}: ERROR {exc}")
            if index < len(targets) and args.pause_s > 0:
                await asyncio.sleep(args.pause_s)
    finally:
        await db.close()

    print(f"done targets={len(targets)} fetched={fetched} imported={imported} errors={errors}")


if __name__ == "__main__":
    asyncio.run(_main())

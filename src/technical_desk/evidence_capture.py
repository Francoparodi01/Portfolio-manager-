"""Public TradingView evidence capture, with no application or database imports.

The response-completed event confirms a finished download, never a closed bar.
Keep incoming frames so normalized decimal values and metadata are inspectable.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import re
from uuid import uuid4

import aiohttp


CAPTURE_VERSION = "technical-desk-public-capture-v1"
ENDPOINT = "wss://data.tradingview.com/socket.io/websocket?from=chart%2F"


def encode(value):
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("NONFINITE_SOURCE_NUMBER")
        return format(value, "f")
    raise TypeError(type(value).__name__)


def canonical(value):
    return json.dumps(value, default=encode, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False).encode("utf-8")


def content_hash(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def frame(method, params):
    body = json.dumps({"m": method, "p": params}, separators=(",", ":"), ensure_ascii=True)
    return f"~m~{len(body)}~m~{body}"


def parse_frames(raw):
    bodies = []
    index = 0
    while index < len(raw):
        if not raw.startswith("~m~", index):
            raise ValueError("UNFRAMED_SOURCE_MESSAGE")
        delimiter = raw.find("~m~", index + 3)
        if delimiter < 0:
            raise ValueError("TRUNCATED_SOURCE_FRAME")
        length = int(raw[index + 3:delimiter])
        start = delimiter + 3
        body = raw[start:start + length]
        if len(body) != length:
            raise ValueError("TRUNCATED_SOURCE_BODY")
        bodies.append(body)
        index = start + length
    return bodies


def normalize_rows(events):
    """Apply complete timescale updates in arrival order; reject malformed facts."""
    bars = {}
    for event in events:
        if event.get("m") != "timescale_update":
            continue
        params = event.get("p") or []
        if len(params) < 2 or not isinstance(params[1], dict):
            raise ValueError("INVALID_TIMESCALE_UPDATE")
        for row in (params[1].get("s1") or {}).get("s", []):
            values = row.get("v")
            if not isinstance(values, list) or len(values) < 5:
                raise ValueError("INVALID_SOURCE_BAR")
            ts = Decimal(str(values[0]))
            if not ts.is_finite() or ts != ts.to_integral_value() or ts <= 0:
                raise ValueError("INVALID_SOURCE_TIMESTAMP")
            prices = [Decimal(str(value)) for value in values[1:5]]
            if not all(value.is_finite() and value > 0 for value in prices):
                raise ValueError("INVALID_OHLC")
            opening, high, low, close = prices
            if high < max(opening, low, close) or low > min(opening, high, close):
                raise ValueError("IMPOSSIBLE_OHLC")
            volume = Decimal(str(values[5])) if len(values) > 5 and values[5] is not None else None
            if volume is not None and (not volume.is_finite() or volume < 0):
                raise ValueError("INVALID_VOLUME")
            bar = dict(bar_start=datetime.fromtimestamp(int(ts), timezone.utc).isoformat(),
                       open=opening, high=high, low=low, close=close, volume=volume,
                       volume_unit=None, close_status="UNCONFIRMED")
            bars[int(ts)] = bar
    ordered = [bars[ts] for ts in sorted(bars)]
    for index, bar in enumerate(ordered[:-1]):
        bar["close_status"] = "CONFIRMED_BY_NEXT_PROVIDER_BAR"
        bar["close_confirmed_at"] = ordered[index + 1]["bar_start"]
    return ordered


async def fetch_public_evidence(ticker: str, *, bars: int = 400, timeout_s: int = 35):
    if not re.fullmatch(r"[A-Z][A-Z0-9.]{0,11}", ticker):
        raise ValueError("CANONICAL_TICKER_REQUIRED")
    if type(bars) is not int or not 2 <= bars <= 1000:
        raise ValueError("BOUNDED_BAR_COUNT_REQUIRED")
    if type(timeout_s) is not int or not 1 <= timeout_s <= 60:
        raise ValueError("BOUNDED_TIMEOUT_REQUIRED")
    symbol = f"BYMA:{ticker}"
    session_id = "cs_" + uuid4().hex[:12]
    started_at = datetime.now(timezone.utc).isoformat()
    request = {"symbol": symbol, "adjustment": "splits", "session": "regular"}
    events, incoming = [], []
    resolved = None
    headers = {"Origin": "https://www.tradingview.com", "User-Agent": "Mozilla/5.0"}
    deadline = asyncio.get_running_loop().time() + timeout_s
    async with aiohttp.ClientSession(headers=headers, timeout=aiohttp.ClientTimeout(total=timeout_s)) as session:
        async with session.ws_connect(ENDPOINT, heartbeat=20, timeout=timeout_s) as socket:
            async def send(method, params):
                await socket.send_str(frame(method, params))

            # Same public access mode as the existing collector; no account credentials.
            await send("set_auth_token", ["unauthorized_user_token"])
            await send("chart_create_session", [session_id, ""])
            await send("resolve_symbol", [session_id, "symbol_1", "=" + json.dumps(request, separators=(",", ":"))])
            await send("create_series", [session_id, "s1", "s1", "symbol_1", "1D", bars, ""])
            completed = False
            while not completed:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    raise TimeoutError("SOURCE_DOWNLOAD_TIMEOUT")
                message = await socket.receive(timeout=remaining)
                if message.type != aiohttp.WSMsgType.TEXT:
                    raise RuntimeError("SOURCE_CONNECTION_CLOSED_OR_NON_TEXT")
                incoming.append(message.data)
                for body in parse_frames(message.data):
                    if body.startswith("~h~"):
                        await socket.send_str(f"~m~{len(body)}~m~{body}")
                        continue
                    event = json.loads(body, parse_float=Decimal)
                    if not isinstance(event, dict):
                        continue  # public connection identifier, retained in raw frames
                    events.append(event)
                    method = event.get("m")
                    params = event.get("p") or []
                    if method in {"critical_error", "symbol_error", "series_error", "protocol_error"}:
                        raise RuntimeError("SOURCE_REJECTED_REQUEST:" + method)
                    if method == "symbol_resolved" and len(params) >= 3:
                        resolved = params[2]
                    if method == "series_completed" and len(params) >= 2 and params[1] == "s1":
                        completed = True
            observed_at = datetime.now(timezone.utc).isoformat()
    normalized = normalize_rows(events)
    if not isinstance(resolved, dict) or not normalized:
        raise ValueError("SOURCE_METADATA_OR_BARS_MISSING")
    return dict(schema=CAPTURE_VERSION, ticker=ticker, requested_symbol=symbol,
                requested_bars=bars, interval="1D", endpoint=ENDPOINT, public_access=True,
                requested_adjustment=request["adjustment"], requested_session=request["session"],
                started_at=started_at, available_at=observed_at, completed_at=observed_at,
                source_metadata=resolved, incoming_frames=incoming, parsed_events=events,
                bars=normalized, download_complete=True,
                original_vintage=False, database_writes=False, models_invoked=False)

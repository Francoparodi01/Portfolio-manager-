"""Authenticated, read-only public disclosure API surface."""
from __future__ import annotations

from datetime import date, datetime, timezone
import re

from aiohttp import web


async def public_disclosures_view(request: web.Request) -> web.Response:
    try:
        limit = max(1, min(int(request.query.get("limit", "120")), 200))
    except ValueError:
        return web.json_response({"ok": False, "error": "invalid limit"}, status=400)
    ticker = request.query.get("ticker", "").strip().upper()
    if ticker and not re.fullmatch(r"[A-Z0-9.]{1,12}", ticker):
        return web.json_response({"ok": False, "error": "invalid ticker"}, status=400)

    state = "NOT_INITIALIZED"
    items = []
    last_capture = None
    async with request.app["pool"].acquire() as conn:
        available = await conn.fetchval(
            "SELECT to_regclass('public.public_disclosure_events') IS NOT NULL"
        )
        if available:
            rows = await conn.fetch(
                """
                SELECT event_key, category, source, ticker, person_name, person_role,
                       direction, occurred_on, filed_on, accepted_at, first_seen_at,
                       shares::double precision AS shares,
                       unit_price_usd::double precision AS unit_price_usd,
                       value_usd::double precision AS value_usd,
                       ownership_type, source_url
                FROM public_disclosure_events
                WHERE ($1::text IS NULL OR ticker = $1)
                ORDER BY filed_on DESC, first_seen_at DESC, event_key
                LIMIT $2
                """,
                ticker or None, limit,
            )
            items = [
                {
                    key: value.isoformat() if isinstance(value, (date, datetime)) else value
                    for key, value in dict(row).items()
                }
                for row in rows
            ]
            last_capture = await conn.fetchval(
                "SELECT MAX(first_seen_at) FROM public_disclosure_events"
            )
            state = "HAS_DATA" if items else "NO_DATA"

    return web.json_response({
        "ok": True, "state": state, "items": items,
        "count_returned": len(items), "limit": limit,
        "last_capture": last_capture.isoformat() if last_capture else None,
        "scope": "SEC non-derivative market-or-private P/S; only collected tickers",
        "sources": {
            "SEC_FORM4": "AVAILABLE" if state != "NOT_INITIALIZED" else "NOT_INITIALIZED",
            "HOUSE_PTR": "NOT_IMPLEMENTED",
            "SENATE_PTR": "NOT_IMPLEMENTED",
        },
        "is_trading_signal": False,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    })

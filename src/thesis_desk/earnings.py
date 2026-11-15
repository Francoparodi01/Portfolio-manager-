"""Dated vendor calendar observations; missing events never imply no earnings risk."""
from __future__ import annotations

import asyncio
from datetime import datetime, time, timedelta

from src.backtesting.thesis_desk import ART, instant
from src.thesis_desk.contracts import digest, dump, now


def fetch_calendar(start, end):
    import yfinance as yf
    calendar = yf.Calendars(start=start, end=end)
    rows, page_hashes = [], set()
    for page in range(60):
        frame = calendar.get_earnings_calendar(limit=100, offset=page * 100, filter_most_active=False, force=True)
        records = []
        for row in frame.reset_index().to_dict(orient="records"):
            stamp = row["Event Start Date"]
            if not hasattr(stamp, "isoformat"):
                raise ValueError("Invalid calendar timestamp")
            records.append(dict(symbol=str(row["Symbol"]), event_at=stamp.isoformat(),
                                event_name=str(row["Event Name"]), timing=str(row["Timing"])))
        page_hash = digest(records)
        if records and page_hash in page_hashes:
            return dict(rows=rows, truncated=True)
        page_hashes.add(page_hash)
        rows.extend(records)
        if len(frame) < 100:
            return dict(rows=rows, truncated=False)
    return dict(rows=rows, truncated=True)


def calendar_evidence(snapshot, catalog, at):
    """Only explicitly listed, future events establish issuer calendar coverage."""
    if at - instant(snapshot["captured_at"]) > timedelta(hours=6) or instant(snapshot["captured_at"]) > at:
        raise ValueError("Stale/future earnings calendar")
    by_symbol = {}
    for item in catalog:
        if item["status"] == "IDENTIFIED" and item["asset_type"] == "CEDEAR":
            symbol = item["underlying"].replace("/", "-").replace(".", "-")
            by_symbol.setdefault(symbol, []).append(item["ticker"])
    blackouts, covered, seen = [], set(), set()
    for row in snapshot["rows"]:
        stamp = instant(row["event_at"])
        # Vendor dates are calendar-day events in New York, even when event time is unknown.
        from zoneinfo import ZoneInfo
        day = stamp.astimezone(ZoneInfo("America/New_York")).date()
        if day < at.astimezone(ART).date() or not snapshot["start"] <= day.isoformat() <= snapshot["end"]:
            continue
        for ticker in by_symbol.get(row["symbol"], []):
            key = (ticker, day.isoformat())
            if key in seen:
                continue
            seen.add(key)
            event = datetime.combine(day, time.min, ART)
            blackouts.append(dict(id=digest([ticker, row["event_at"], snapshot["source_url"]]), ticker=ticker,
                                  start_at=(event - timedelta(days=1)).isoformat(), end_at=(event + timedelta(days=1)).isoformat(),
                                  available_at=snapshot["captured_at"], source_url=snapshot["source_url"],
                                  knowledge_basis="ARCHIVED_AT_TIME"))
            covered.add(ticker)
    return blackouts, sorted(covered)


async def collect_earnings(store, directory, catalog):
    at = now()
    key = "calendar:yahoo-future-v1"
    snapshot = store.cached(key)
    if not snapshot or at - instant(snapshot["captured_at"]) > timedelta(hours=6):
        start, end = at.astimezone(ART).date(), at.astimezone(ART).date() + timedelta(days=110)
        data = await asyncio.to_thread(fetch_calendar, start.isoformat(), end.isoformat())
        snapshot = dict(data, captured_at=now().isoformat(), start=start.isoformat(), end=end.isoformat(),
                        source_url="https://finance.yahoo.com/calendar/earnings?from=" + start.isoformat() + "&to=" + end.isoformat(),
                        representation="YFINANCE_CALENDARS_PARSED_PUBLIC_RESPONSE")
        store.cache(key, snapshot)
    dump(directory / "earnings-calendar.json", snapshot)
    blackouts, covered = calendar_evidence(snapshot, catalog, now())
    return blackouts, covered, dict(source="EARNINGS", status="PARTIAL" if snapshot["truncated"] else "CAPTURED",
                                    checked_at=snapshot["captured_at"], observed_events=len(snapshot["rows"]),
                                    covered_equities=len(covered), source_url=snapshot["source_url"])

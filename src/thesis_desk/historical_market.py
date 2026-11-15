"""Rebuild the PAPER desk's late-session price reference from recorded quotes.

This is an observed quote proxy, not an official exchange closing price. Original
quotes stay unchanged; the aggregation is reconstructed from a current DB capture.
"""
from collections import Counter
from datetime import datetime, time

from src.backtesting.thesis_desk import ART, decimal, instant
from src.core.market_calendar import is_trading_day
from src.thesis_desk.contracts import digest

BASIS = "LAST_RECORDED_QUOTE_16_30_TO_17_NOT_OFFICIAL_CLOSE"


def recorded_daily_references(quotes, captured_at):
    captured = instant(captured_at)
    selected, seen = {}, {}
    skipped = Counter()
    for row in quotes:
        stamp = instant(row["ts"])
        local = stamp.astimezone(ART)
        day, clock = local.date(), local.time().replace(tzinfo=None)
        price = decimal(row["last_price"])
        if stamp > captured or not price.is_finite() or price <= 0 or row["currency"] != "ARS":
            raise ValueError("Invalid recorded historical quote")
        key = (row["ticker"], row["asset_type"], row["currency"], stamp)
        if key in seen and seen[key] != price:
            raise ValueError("Conflicting recorded historical quotes")
        seen[key] = price
        if not is_trading_day(day) or not time(16, 30) <= clock < time(17):
            skipped["OUTSIDE_DEFINED_WINDOW"] += 1
            continue
        end = datetime.combine(day, time(17), ART)
        if end > captured:
            skipped["WINDOW_NOT_CLOSED"] += 1
            continue
        identity = key[:3] + (day,)
        if identity not in selected or stamp > instant(selected[identity]["ts"]):
            selected[identity] = row
    bars = []
    for identity, row in sorted(selected.items()):
        end = datetime.combine(identity[-1], time(17), ART).isoformat()
        bars.append(dict(ticker=row["ticker"], asset_type=row["asset_type"], currency=row["currency"],
            ts=row["ts"], close_price=str(decimal(row["last_price"])), interval="1d", is_closed=True,
            bar_end=end, available_at=end, scraped_at=row["ts"], adjustment_policy="RAW",
            source="DESK_RECORDED_LATE_SESSION_QUOTE", basis=BASIS, source_quote_sha256=digest(row),
            source_table="market_prices", recorded_quote_at=row["ts"], reconstructed_at=captured_at,
            availability_basis="DEFINED_AGGREGATION_WINDOW_END_FROM_RECORDED_QUOTES",
            knowledge_basis="RECONSTRUCTED_FROM_CURRENT_CAPTURE_OF_RECORDED_QUOTES"))
    return bars, dict(basis=BASIS, input_quotes=len(quotes), daily_references=len(bars),
                      symbols=len({b["ticker"] for b in bars}), skipped=dict(skipped),
                      price_vintage="CURRENT_DATABASE_CAPTURE_NOT_ORIGINAL_VINTAGE",
                      official_closes=False, source_rows_modified=False)

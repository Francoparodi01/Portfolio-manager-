"""Canonical, read-only reconstruction of formal bot signal returns.

This is the shared definition for reports that describe hypothetical bot price
returns. It never reads ``outcome_*`` or ``decision_log`` because those are
separate operational views with historical contracts of their own.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, timedelta, timezone
import json
import math
from pathlib import Path
from statistics import mean, median
from typing import Any
from zoneinfo import ZoneInfo

ART = ZoneInfo("America/Argentina/Buenos_Aires")
HORIZONS = (5, 10, 20, 40)
SOURCES = ("COCOS", "TRADINGVIEW_BYMA")
CALENDAR_FROM = date(2026, 1, 1)
CALENDAR_THROUGH = date(2026, 10, 2)
CALENDAR = json.loads((Path(__file__).resolve().parents[2] / "config/market_holidays_ar.json").read_text())
CLOSURES = {date.fromisoformat(item["date"]) for item in CALENDAR["closures"]}
CLOSURES |= {date.fromisoformat(item["date"]) for item in CALENDAR["special_sessions"] if not item["trading"]}


def number(value: Any) -> float | None:
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError, OverflowError):
        return None


def trading_day(day: date) -> bool:
    return day.weekday() < 5 and day not in CLOSURES


def sessions_after(day: date, count: int = 40) -> list[date]:
    result: list[date] = []
    while len(result) < count:
        day += timedelta(days=1)
        if trading_day(day):
            result.append(day)
    return result


def eligible(row: dict[str, Any]) -> bool:
    return (
        row.get("source") == "execution_plan"
        and row.get("feasible") is True
        and row.get("is_executable") is True
        and row.get("was_blocked") is False
        and str(row.get("side", "")).upper() in ("BUY", "SELL")
        and bool(str(row.get("ticker") or "").strip())
    )


def _summarize(values: list[float], total: int, reasons: Counter[str]) -> dict[str, Any]:
    return {
        "n": len(values), "pending": total - len(values),
        "coverage_pct": 100 * len(values) / total if total else None,
        "mean_pct": 100 * mean(values) if values else None,
        "median_pct": 100 * median(values) if values else None,
        "wins": sum(value > 0 for value in values),
        "win_pct": 100 * sum(value > 0 for value in values) / len(values) if values else None,
        "missing_reasons": dict(reasons),
    }


def compute(rows, candles, *, as_of, cost_bps: float = 75.0, events=(), events_available: bool = True):
    """Deduplicate formal signals and evaluate them with exact BYMA sessions.

    Each horizon requires the same complete source/instrument series. Missing
    candles and event uncertainty remain missing; dates never shift forward.
    """
    if number(cost_bps) is None or not 0 <= cost_bps <= 400:
        raise ValueError("cost_bps debe estar entre 0 y 400 y ser finito")
    cutoff = as_of.astimezone(ART).date() - timedelta(days=1)
    while not trading_day(cutoff):
        cutoff -= timedelta(days=1)
    quality: Counter[str] = Counter(raw_intents=len(rows), unique_signals=0, ineligible_intents=0,
                                    same_day_duplicates=0, future_intents=0)
    selected, seen = [], set()
    for row in sorted(rows, key=lambda item: (item["created_at"], item["intent_id"])):
        if row["created_at"] > as_of:
            quality["future_intents"] += 1
            continue
        if not eligible(row):
            quality["ineligible_intents"] += 1
            continue
        key = (row["created_at"].astimezone(ART).date(), str(row["ticker"]).strip().upper(), str(row["side"]).upper())
        if key in seen:
            quality["same_day_duplicates"] += 1
            continue
        seen.add(key)
        selected.append((key, row))
    quality["unique_signals"] = len(selected)

    series: defaultdict[tuple[str, str, str | None], dict[date, tuple[float, float] | None]] = defaultdict(dict)
    conflicts = set()
    for row in candles:
        source = row.get("source")
        if source not in SOURCES or (row.get("currency"), row.get("venue"), row.get("interval")) != ("ARS", "BYMA", "1d"):
            quality["excluded_source_or_basis_candles"] += 1
            continue
        day = row["ts"].astimezone(timezone.utc).date()
        scraped = row.get("scraped_at")
        if (day > cutoff or not trading_day(day) or (scraped and (scraped > as_of or scraped.astimezone(ART).date() < day or
                (scraped.astimezone(ART).date() == day and scraped.astimezone(ART).hour < 18)))):
            quality["incomplete_or_non_session_candles"] += 1
            continue
        opening, closing = number(row["open_price"]), number(row["close_price"])
        value = (opening, closing) if opening and closing and opening > 0 and closing > 0 else None
        if value is None:
            quality["invalid_candles"] += 1
        key = (str(row["ticker"]).strip().upper(), source, row["long_ticker"])
        if day in series[key] and series[key][day] != value:
            conflicts.add((key, day))
        series[key][day] = None if (key, day) in conflicts else value
    quality["conflicting_candle_days"] = len(conflicts)

    results = []
    for (day, ticker, side), row in selected:
        dates = sessions_after(day)
        item = {"date": day.isoformat(), "ticker": ticker, "side": side,
                "plan_id": str(row["plan_id"]), "intent_id": row["intent_id"],
                "entry_date": dates[0].isoformat(), "returns": {}, "details": {}}
        for horizon in HORIZONS:
            key = str(horizon)
            path = dates[:horizon]
            detail: dict[str, Any] = {"status": "immature", "exit_date": path[-1].isoformat()}
            value = None
            if path[-1] <= cutoff:
                if day < CALENDAR_FROM or path[-1] > CALENDAR_THROUGH:
                    detail["status"] = "calendar_unverified"
                elif not events_available:
                    detail["status"] = "corporate_registry_unavailable"
                elif any(event["ticker"].upper() == ticker and path[0] <= event["effective_at"].astimezone(ART).date() <= path[-1]
                         and event["lifecycle_status"] not in ("CANCELLED", "DISMISSED", "SUPERSEDED") for event in events):
                    detail["status"] = "corporate_event"
                else:
                    options = []
                    for source in SOURCES:
                        candidates = [(series_key, prices) for series_key, prices in series.items()
                                      if series_key[0] == ticker and series_key[1] == source and all(prices.get(session) for session in path)]
                        if candidates:
                            options = candidates
                            break
                    if not options:
                        detail["status"] = "missing_or_conflicting_prices"
                    elif len({tuple(prices[session] for session in path) for _, prices in options}) > 1:
                        detail["status"] = "ambiguous_instrument"
                    else:
                        series_key, prices = sorted(options, key=lambda option: option[0])[0]
                        opening, closing = prices[path[0]][0], prices[path[-1]][1]
                        jump = any(abs(prices[session][1] / prices[session][0] - 1) > .30 for session in path)
                        jump |= any(abs(prices[next_day][0] / prices[current_day][1] - 1) > .30
                                    for current_day, next_day in zip(path, path[1:]))
                        if jump:
                            detail["status"] = "unverified_discontinuity"
                        else:
                            gross = (closing / opening - 1) * (1 if side == "BUY" else -1)
                            value = gross - cost_bps / 10_000
                            detail.update(status="evaluated", entry_price=opening, exit_price=closing,
                                          source=series_key[1], long_ticker=series_key[2], gross_pct=gross * 100)
            item["returns"][key] = value
            item["details"][key] = detail
        results.append(item)

    metrics = {}
    for horizon in HORIZONS:
        key = str(horizon)
        values = [signal["returns"][key] for signal in results if signal["returns"][key] is not None]
        reasons = Counter(signal["details"][key]["status"] for signal in results if signal["returns"][key] is None)
        metrics[key] = _summarize(values, len(results), reasons)
    weekly: defaultdict[str, list[float]] = defaultdict(list)
    for signal in results:
        if signal["returns"]["5"] is not None:
            signal_day = date.fromisoformat(signal["date"])
            weekly[(signal_day - timedelta(days=signal_day.weekday())).isoformat()].append(signal["returns"]["5"])
    return {
        "metrics": metrics, "quality": dict(quality), "signals": list(reversed(results)),
        "weekly_5d": [{"week": week, "n": len(values), "mean_pct": 100 * mean(values)} for week, values in sorted(weekly.items())],
        "signal_sides": dict(Counter(signal["side"] for signal in results)),
        "evaluated_price_sources": dict(Counter(detail["source"] for signal in results for detail in signal["details"].values()
                                                  if detail["status"] == "evaluated")),
        "price_cutoff": cutoff.isoformat(), "calendar_verified_through": CALENDAR_THROUGH.isoformat(),
    }


async def verify_legacy_single_owner(conn, owner_id: int) -> bool:
    requested_owner_exists = await conn.fetchval("""SELECT EXISTS (
        SELECT 1 FROM portfolio_snapshots WHERE owner_chat_id=$1
        UNION ALL SELECT 1 FROM decision_log WHERE owner_chat_id=$1
        UNION ALL SELECT 1 FROM broker_fills WHERE owner_chat_id=$1
        UNION ALL SELECT 1 FROM execution_plans WHERE owner_chat_id=$1
    )""", owner_id)
    other_owner_exists = await conn.fetchval("""SELECT EXISTS (
        SELECT 1 FROM portfolio_snapshots WHERE owner_chat_id IS NOT NULL AND owner_chat_id<>$1
        UNION ALL SELECT 1 FROM decision_log WHERE owner_chat_id IS NOT NULL AND owner_chat_id<>$1
        UNION ALL SELECT 1 FROM broker_fills WHERE owner_chat_id IS NOT NULL AND owner_chat_id<>$1
        UNION ALL SELECT 1 FROM execution_plans WHERE owner_chat_id IS NOT NULL AND owner_chat_id<>$1
    )""", owner_id)
    return bool(requested_owner_exists and not other_owner_exists)


async def load_raw_bot_signal_stats(conn, *, owner_chat_id: int, days: int, cost_bps: float = 75.0) -> dict[str, Any]:
    """Load the common raw-signal denominator inside the caller's read-only transaction."""
    if not 1 <= days <= 730:
        raise ValueError("days debe estar entre 1 y 730")
    as_of = await conn.fetchval("SELECT NOW()")
    start = as_of - timedelta(days=days)
    legacy_owner_inferred = await verify_legacy_single_owner(conn, owner_chat_id)
    rows = await conn.fetch("""
        SELECT p.id AS plan_id,p.run_id,p.created_at,p.source,p.feasible,
               i.id AS intent_id,i.ticker,i.side,i.is_executable,i.was_blocked
        FROM execution_plans p JOIN order_intents i ON i.execution_plan_id=p.id
        WHERE (p.owner_chat_id=$1 OR ($4::boolean AND p.owner_chat_id IS NULL))
          AND p.created_at >= $2 AND p.created_at <= $3
        ORDER BY p.created_at,i.id LIMIT 25001
    """, owner_chat_id, start, as_of, legacy_owner_inferred)
    counts = dict(await conn.fetchrow("""
        SELECT count(*) AS raw_intents,
          count(*) FILTER (WHERE p.source='execution_plan' AND p.feasible
            AND i.is_executable AND i.was_blocked IS FALSE AND i.side IN ('BUY','SELL') AND trim(i.ticker)<>'') AS eligible_intents,
          count(DISTINCT ((p.created_at AT TIME ZONE 'America/Argentina/Buenos_Aires')::date,upper(trim(i.ticker)),i.side))
            FILTER (WHERE p.source='execution_plan' AND p.feasible AND i.is_executable
            AND i.was_blocked IS FALSE AND i.side IN ('BUY','SELL') AND trim(i.ticker)<>'') AS unique_signals
        FROM execution_plans p JOIN order_intents i ON i.execution_plan_id=p.id
        WHERE (p.owner_chat_id=$1 OR ($4::boolean AND p.owner_chat_id IS NULL))
          AND p.created_at >= $2 AND p.created_at <= $3
    """, owner_chat_id, start, as_of, legacy_owner_inferred))
    tickers = sorted({str(row["ticker"]).strip().upper() for row in rows if row["source"] == "execution_plan"})
    candles = await conn.fetch("""
        SELECT m.ticker,m.long_ticker,m.source,m.currency,m.venue,m.interval,
               m.ts,m.open_price,m.close_price,m.scraped_at
        FROM market_candles m
        WHERE m.ticker=ANY($1::text[]) AND m.interval='1d' AND m.currency='ARS' AND m.venue='BYMA'
          AND m.ts >= $2 AND m.ts <= $3 AND m.scraped_at <= $3
        ORDER BY m.ticker,m.ts LIMIT 250001
    """, tickers, start - timedelta(days=7), as_of) if tickers else []
    registry_available = bool(await conn.fetchval("SELECT to_regclass('public.corporate_events') IS NOT NULL AND to_regclass('public.corporate_event_instrument_effects') IS NOT NULL"))
    events = await conn.fetch("""
        SELECT e.event_type,e.lifecycle_status,e.effective_at,f.ticker,f.price_factor
        FROM corporate_events e JOIN corporate_event_instrument_effects f ON f.event_id=e.id
        WHERE f.ticker=ANY($1::text[]) AND f.is_active
          AND (f.venue IS NULL OR f.venue='BYMA') AND (f.currency IS NULL OR f.currency='ARS')
          AND e.effective_at >= $2 AND e.effective_at <= $3 LIMIT 10001
    """, tickers, start, as_of) if registry_available and tickers else []
    if len(rows) > 25_000 or len(candles) > 250_000 or len(events) > 10_000:
        raise ValueError("extraccion excede el limite; reducir la ventana")
    result = compute(rows, candles, as_of=as_of, cost_bps=cost_bps, events=events, events_available=registry_available)
    quality = result["quality"]
    reconciled = (quality["raw_intents"] == counts["raw_intents"] and quality["unique_signals"] == counts["unique_signals"]
                  and quality["same_day_duplicates"] == counts["eligible_intents"] - counts["unique_signals"])
    if not reconciled:
        raise RuntimeError("raw count reconciliation failed")
    result.update(as_of=as_of.isoformat(), window_start=start.isoformat(), days=days, cost_bps=cost_bps,
                  raw_candles=len(candles), raw_intents=len(rows), sql_counts=counts, counts_reconciled=True,
                  legacy_owner_inferred=legacy_owner_inferred,
                  owner_scope=("EXPLICIT_PLUS_VERIFIED_LEGACY_NULL" if legacy_owner_inferred else "EXPLICIT_ONLY"),
                  corporate_events=len(events), corporate_registry_available=registry_available,
                  metric_contract="RAW_FORMAL_SIGNAL_BYMA_V1",
                  metric_note="Retorno hipotetico de precio de señales formales; no es PnL realizado ni outcome persistido.")
    return result


def compact_summary(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "contract": result["metric_contract"], "note": result["metric_note"],
        "owner_scope": result["owner_scope"], "legacy_owner_inferred": result["legacy_owner_inferred"],
        "as_of": result["as_of"], "price_cutoff": result["price_cutoff"],
        "days": result["days"], "cost_bps": result["cost_bps"], "raw_intents": result["raw_intents"],
        "unique_signals": result["quality"]["unique_signals"], "counts_reconciled": result["counts_reconciled"],
        "metrics": {str(horizon): result["metrics"][str(horizon)] for horizon in HORIZONS},
    }


__all__ = ["ART", "CALENDAR", "CALENDAR_FROM", "CALENDAR_THROUGH", "CLOSURES", "HORIZONS", "SOURCES",
           "compact_summary", "compute", "eligible", "load_raw_bot_signal_stats", "number", "sessions_after",
           "trading_day", "verify_legacy_single_owner"]

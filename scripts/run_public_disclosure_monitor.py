"""Collect SEC Form 4 open-market insider trades for Quantia's read-only monitor.

Examples:
  SEC_USER_AGENT="Quantia contact@example.com" python scripts/run_public_disclosure_monitor.py --tickers AMD,NVDA,MU
  SEC_USER_AGENT="Quantia contact@example.com" python scripts/run_public_disclosure_monitor.py --portfolio --persist

No orders, decisions, scores, or portfolio weights are touched.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import date, timedelta
import json
import os
import sys
from typing import Any

import asyncpg
import httpx

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.public_disclosures import (
    DISCLOSURES_SCHEMA_SQL,
    parse_sec_form4,
    recent_form4_filings,
)
from src.collector.issuer_event_sources import (
    SEC_SUBMISSIONS_URL,
    SEC_TICKER_DIRECTORY_URL,
    parse_sec_company_directory,
)
from src.core.config import get_config


async def portfolio_tickers() -> list[str]:
    """Read existing canonical issuer mapping, without creating/migrating anything."""
    db = await asyncpg.connect(get_config().database.url.replace("postgresql+asyncpg://", "postgresql://"))
    try:
        if not await db.fetchval("SELECT to_regclass('public.issuer_registry') IS NOT NULL"):
            return []
        rows = await db.fetch("""
            SELECT DISTINCT primary_symbol FROM issuer_registry
            WHERE is_active = TRUE AND source_market = 'US'
              AND sec_cik IS NOT NULL AND primary_symbol IS NOT NULL
            ORDER BY primary_symbol
        """)
        return [str(row["primary_symbol"]).upper() for row in rows]
    finally:
        await db.close()


async def persist_events(events: list[dict[str, Any]]) -> int:
    """Append-only insert: a later fetch must not rewrite first_seen_at/PIT data."""
    if not events:
        return 0
    conn = await asyncpg.connect(get_config().database.url.replace("postgresql+asyncpg://", "postgresql://"))
    try:
        async with conn.transaction():
            await conn.execute(DISCLOSURES_SCHEMA_SQL)
            inserted = 0
            for e in events:
                status = await conn.execute(
                    """
                    INSERT INTO public_disclosure_events (
                        event_key, category, source, ticker, sec_cik, person_name,
                        person_role, direction, occurred_on, filed_on, accepted_at,
                        shares, unit_price_usd, value_usd, ownership_type,
                        source_url, source_hash, raw_payload
                    ) VALUES (
                        $1,$2,$3,$4,$5,$6,$7,$8,$9::date,$10::date,$11::timestamptz,
                        $12::numeric,$13::numeric,$14::numeric,$15,$16,$17,$18::jsonb
                    ) ON CONFLICT (event_key) DO NOTHING
                    """,
                    e["event_key"], e["category"], e["source"], e["ticker"],
                    e["sec_cik"], e["person_name"], e["person_role"], e["direction"],
                    e["occurred_on"], e["filed_on"], e["accepted_at"],
                    e["shares"], e["unit_price_usd"], e["value_usd"],
                    e["ownership_type"], e["source_url"], e["source_hash"],
                    json.dumps(e["raw_payload"], ensure_ascii=False),
                )
                if status == "INSERT 0 1":
                    inserted += 1
            return inserted
    finally:
        await conn.close()


async def collect(
    tickers: list[str], *,
    user_agent: str,
    lookback_days: int,
    max_filings_per_ticker: int,
) -> dict[str, Any]:
    if not user_agent or "@" not in user_agent:
        raise ValueError("SEC_USER_AGENT is required, with a valid contact email")
    if len(tickers) > 50:
        raise ValueError("Maximum 50 tickers per collection")
    since = date.today() - timedelta(days=lookback_days)
    output: dict[str, Any] = {
        "source": "SEC_FORM4", "mode": "read_only", "since_filed": since.isoformat(),
        "tickers_requested": tickers, "tickers_unresolved": [],
        "filings_checked": 0, "filings_failed": 0, "events": [],
        "errors": [],
        "coverage": "open_market_non_derivative_P_S_only",
    }
    async with httpx.AsyncClient(
        headers={"User-Agent": user_agent, "Accept": "application/json,text/xml,*/*"},
        follow_redirects=True, timeout=20.0,
    ) as client:
        directory_response = await client.get(SEC_TICKER_DIRECTORY_URL)
        directory_response.raise_for_status()
        directory = parse_sec_company_directory(directory_response.json())
        await asyncio.sleep(0.2)
        for ticker in tickers:
            company = directory.get(ticker)
            if not company:
                output["tickers_unresolved"].append(ticker)
                continue
            cik = str(company["cik"]).zfill(10)
            try:
                response = await client.get(SEC_SUBMISSIONS_URL.format(cik=cik))
                response.raise_for_status()
                recent = response.json()
                accepted_lookup = dict(zip(
                    recent.get("filings", {}).get("recent", {}).get("accessionNumber", []),
                    recent.get("filings", {}).get("recent", {}).get("acceptanceDateTime", []),
                ))
                filings = recent_form4_filings(
                    recent, cik=cik, since=since, limit=max_filings_per_ticker
                )
                await asyncio.sleep(0.2)
            except (httpx.HTTPError, ValueError, KeyError) as exc:
                output["errors"].append({"ticker": ticker, "stage": "submissions", "type": type(exc).__name__})
                continue
            for filing in filings:
                output["filings_checked"] += 1
                try:
                    response = await client.get(filing["source_url"])
                    response.raise_for_status()
                    accepted = accepted_lookup.get(filing["accession"]) or None
                    # SEC sometimes uses ET-formatted timestamps without a timezone.
                    if accepted and accepted.endswith("Z"):
                        accepted_at = accepted
                    elif accepted:
                        accepted_at = accepted.replace(" ", "T") + "Z"
                    else:
                        accepted_at = None
                    rows = parse_sec_form4(
                        response.text,
                        ticker=ticker, cik=cik,
                        accession=filing["accession"], filed_on=filing["filed_on"],
                        source_url=filing["source_url"], accepted_at=accepted_at,
                    )
                    output["events"].extend(rows)
                except (httpx.HTTPError, ValueError, KeyError, Exception) as exc:
                    # Partial coverage is explicit. One malformed filing is not a green result.
                    output["filings_failed"] += 1
                    output["errors"].append({
                        "ticker": ticker, "accession": filing["accession"],
                        "stage": "ownership_document", "type": type(exc).__name__,
                    })
                await asyncio.sleep(0.2)
    output["event_count"] = len(output["events"])
    output["status"] = "PARTIAL" if output["errors"] or output["tickers_unresolved"] else "COMPLETE_FOR_SCOPE"
    return output


async def main() -> None:
    parser = argparse.ArgumentParser(description="Quantia SEC Form 4 public disclosure monitor")
    parser.add_argument("--tickers", default="", help="Comma-separated US underlying symbols")
    parser.add_argument("--portfolio", action="store_true", help="Include symbols from existing issuer_registry")
    parser.add_argument("--persist", action="store_true", help="Append evidence to PostgreSQL (opt-in)")
    parser.add_argument("--lookback-days", type=int, default=30)
    parser.add_argument("--max-filings-per-ticker", type=int, default=15)
    args = parser.parse_args()
    if not args.tickers and not args.portfolio:
        parser.error("Pass --tickers or --portfolio")
    if not 1 <= args.lookback_days <= 365:
        parser.error("--lookback-days must be 1..365")
    if not 1 <= args.max_filings_per_ticker <= 50:
        parser.error("--max-filings-per-ticker must be 1..50")
    tickers = {value.strip().upper() for value in args.tickers.split(",") if value.strip()}
    if args.portfolio:
        tickers.update(await portfolio_tickers())
    tickers = sorted(tickers)
    if not tickers:
        parser.error("No US tickers available")
    result = await collect(
        tickers,
        user_agent=os.getenv("SEC_USER_AGENT", "").strip(),
        lookback_days=args.lookback_days,
        max_filings_per_ticker=args.max_filings_per_ticker,
    )
    if args.persist:
        result["inserted"] = await persist_events(result["events"])
        result["mode"] = "persist"
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    asyncio.run(main())

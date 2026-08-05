"""Collect issuer-event observations without changing trading decisions.

SEC and CNV work without commercial API keys. FMP splits and Finnhub earnings
are opt-in through FMP_API_KEY and FINNHUB_API_KEY respectively.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import date, timedelta
import json
import os
import sys
from typing import Iterable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.collector.db import PortfolioDatabase
from src.collector.issuer_event_sources import (
    PortfolioInstrumentSeed,
    build_registry_from_portfolio,
    fetch_cnv_relevant_facts,
    fetch_finnhub_earnings,
    fetch_fmp_splits,
    fetch_sec_company_directory,
    fetch_sec_filings,
    issuer_event_http_client,
)
from src.core.config import get_config
from src.core.logger import get_logger


logger = get_logger(__name__)
AVAILABLE_SOURCES = ("sec", "fmp", "finnhub", "cnv")


def _sources(value: str | Iterable[str]) -> tuple[str, ...]:
    values = value.split(",") if isinstance(value, str) else value
    normalized = tuple(dict.fromkeys(
        str(item or "").lower().strip()
        for item in values
        if str(item or "").strip()
    ))
    invalid = sorted(set(normalized) - set(AVAILABLE_SOURCES))
    if invalid:
        raise ValueError(f"unsupported sources: {', '.join(invalid)}")
    return normalized or AVAILABLE_SOURCES


def _dedupe_observations(observations):
    by_key = {}
    for observation in observations:
        by_key[observation.observation_key] = observation
    return list(by_key.values())


async def run(
    *,
    sources: Iterable[str],
    dry_run: bool,
    timeout_seconds: float,
    sec_lookback_days: int,
    calendar_days: int,
) -> dict:
    requested = _sources(sources)
    cfg = get_config()
    db = PortfolioDatabase(cfg.database.url)
    summary = {
        "mode": "dry_run" if dry_run else "persist",
        "sources": {source.upper(): {"status": "not_run", "observations": 0} for source in requested},
        "registry": {"seeds": 0, "issuers": 0, "instruments": 0},
        "observations": {"found": 0, "saved": 0},
    }

    await db.connect()
    try:
        seeds = [
            PortfolioInstrumentSeed(
                ticker=str(row.get("ticker") or ""),
                asset_type=str(row.get("asset_type") or "UNKNOWN"),
                currency=str(row.get("currency") or "ARS"),
                issuer_hint=str(row.get("issuer_hint") or ""),
            )
            for row in await db.get_latest_portfolio_instrument_seeds()
        ]
        summary["registry"]["seeds"] = len(seeds)
        if not seeds:
            return summary

        sec_user_agent = os.getenv("SEC_USER_AGENT", "").strip()
        async with issuer_event_http_client(
            timeout_seconds=timeout_seconds,
            sec_user_agent=sec_user_agent or "CocosCopilotIssuerEvents/1.0",
        ) as client:
            sec_directory = {}
            sec_directory_error = ""
            if sec_user_agent:
                try:
                    sec_directory = await fetch_sec_company_directory(client)
                except Exception as exc:
                    sec_directory_error = type(exc).__name__
                    logger.warning(
                        "SEC ticker directory unavailable; registry will remain unresolved: %s",
                        sec_directory_error,
                    )

            registry_entries, instruments = build_registry_from_portfolio(
                seeds,
                sec_companies=sec_directory,
            )
            summary["registry"]["issuers"] = len(registry_entries)
            summary["registry"]["instruments"] = len(instruments)
            if not dry_run:
                await db.upsert_issuer_registry(registry_entries, instruments)

            today = date.today()
            observations = []
            if "sec" in requested:
                if not sec_user_agent:
                    summary["sources"]["SEC"] = {
                        "status": "skipped_missing_user_agent",
                        "observations": 0,
                    }
                elif sec_directory_error:
                    summary["sources"]["SEC"] = {
                        "status": "failed_directory",
                        "observations": 0,
                        "error": sec_directory_error,
                    }
                else:
                    try:
                        fetched = await fetch_sec_filings(
                            client,
                            registry_entries,
                            since=today - timedelta(days=max(1, int(sec_lookback_days))),
                        )
                        observations.extend(fetched)
                        summary["sources"]["SEC"] = {
                            "status": "ok",
                            "observations": len(fetched),
                        }
                    except Exception as exc:
                        error_type = type(exc).__name__
                        logger.warning("SEC issuer ingestion failed: %s", error_type)
                        summary["sources"]["SEC"] = {
                            "status": "failed",
                            "observations": 0,
                            "error": error_type,
                        }

            if "fmp" in requested:
                api_key = os.getenv("FMP_API_KEY", "").strip()
                if not api_key:
                    summary["sources"]["FMP"] = {"status": "skipped_missing_api_key", "observations": 0}
                else:
                    try:
                        fetched = await fetch_fmp_splits(
                            client,
                            registry_entries,
                            api_key=api_key,
                            from_date=today - timedelta(days=3),
                            to_date=today + timedelta(days=max(1, int(calendar_days))),
                        )
                        observations.extend(fetched)
                        summary["sources"]["FMP"] = {"status": "ok", "observations": len(fetched)}
                    except Exception as exc:
                        error_type = type(exc).__name__
                        logger.warning("FMP split ingestion failed: %s", error_type)
                        summary["sources"]["FMP"] = {
                            "status": "failed",
                            "observations": 0,
                            "error": error_type,
                        }

            if "finnhub" in requested:
                api_key = os.getenv("FINNHUB_API_KEY", "").strip()
                if not api_key:
                    summary["sources"]["FINNHUB"] = {"status": "skipped_missing_api_key", "observations": 0}
                else:
                    try:
                        fetched = await fetch_finnhub_earnings(
                            client,
                            registry_entries,
                            api_key=api_key,
                            from_date=today,
                            to_date=today + timedelta(days=max(1, int(calendar_days))),
                        )
                        observations.extend(fetched)
                        summary["sources"]["FINNHUB"] = {"status": "ok", "observations": len(fetched)}
                    except Exception as exc:
                        error_type = type(exc).__name__
                        logger.warning("Finnhub earnings ingestion failed: %s", error_type)
                        summary["sources"]["FINNHUB"] = {
                            "status": "failed",
                            "observations": 0,
                            "error": error_type,
                        }

            if "cnv" in requested:
                try:
                    fetched = await fetch_cnv_relevant_facts(client, registry_entries)
                    observations.extend(fetched)
                    summary["sources"]["CNV"] = {"status": "ok", "observations": len(fetched)}
                except Exception as exc:
                    logger.warning("CNV relevant facts ingestion failed: %s", exc, exc_info=True)
                    summary["sources"]["CNV"] = {"status": "failed", "observations": 0, "error": str(exc)}

        observations = _dedupe_observations(observations)
        summary["observations"]["found"] = len(observations)
        if not dry_run:
            summary["observations"]["saved"] = await db.save_issuer_event_observations(observations)
    finally:
        await db.close()
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Shadow issuer-event ingestion")
    parser.add_argument("--sources", default=",".join(AVAILABLE_SOURCES))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--timeout-seconds", type=float, default=12.0)
    parser.add_argument("--sec-lookback-days", type=int, default=14)
    parser.add_argument("--calendar-days", type=int, default=45)
    args = parser.parse_args()
    try:
        output = asyncio.run(
            run(
                sources=_sources(args.sources),
                dry_run=args.dry_run,
                timeout_seconds=max(1.0, float(args.timeout_seconds)),
                sec_lookback_days=max(1, int(args.sec_lookback_days)),
                calendar_days=max(1, int(args.calendar_days)),
            )
        )
    except ValueError as exc:
        parser.error(str(exc))
    print(json.dumps(output, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

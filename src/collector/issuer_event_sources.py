"""External issuer-event sources used by the shadow ingestion job.

All functions return observation records only. They do not create corporate
actions, manual-event guards, signals, or execution plans.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date, datetime, timezone
import logging
import re
from typing import Any, Iterable, Mapping
from urllib.parse import urlencode, urljoin
import unicodedata
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup
import httpx

from src.analysis.issuer_events import (
    IssuerEventObservation,
    IssuerInstrument,
    IssuerRegistryEntry,
    confidence_for,
    instrument_id_for,
    normalize_cik,
    normalize_symbol,
)


logger = logging.getLogger(__name__)

ART_TZ = ZoneInfo("America/Argentina/Buenos_Aires")
SEC_TICKER_DIRECTORY_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
FMP_SPLITS_CALENDAR_URL = "https://financialmodelingprep.com/stable/splits-calendar"
FINNHUB_EARNINGS_CALENDAR_URL = "https://finnhub.io/api/v1/calendar/earnings"
CNV_RELEVANT_FACTS_URL = "https://www.cnv.gov.ar/sitioWeb/HechosRelevantes"

SEC_RELEVANT_FORMS = {"8-K", "6-K", "10-K", "10-Q", "20-F", "40-F"}
CNV_SPANISH_MONTHS = {
    "ene": 1,
    "feb": 2,
    "mar": 3,
    "abr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "ago": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dic": 12,
}


@dataclass(frozen=True)
class PortfolioInstrumentSeed:
    ticker: str
    asset_type: str
    currency: str
    issuer_hint: str = ""


def _normalized_text(value: Any) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or ""))
    return "".join(char for char in normalized if not unicodedata.combining(char)).upper()


def _entity_key(value: Any) -> str:
    words = re.findall(r"[A-Z0-9]+", _normalized_text(value))
    suffixes = {
        "SA",
        "S",
        "A",
        "SOCIEDAD",
        "ANONIMA",
        "COMERCIAL",
        "INDUSTRIAL",
        "FINANCIERA",
    }
    while words and words[-1] in suffixes:
        words.pop()
    return " ".join(words)


def _date_from_value(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _datetime_from_value(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_sec_company_directory(payload: Mapping[str, Any]) -> dict[str, dict[str, str]]:
    """Return SEC company data indexed by ticker from both published layouts."""
    parsed: dict[str, dict[str, str]] = {}
    fields = payload.get("fields") if isinstance(payload, Mapping) else None
    rows = payload.get("data") if isinstance(payload, Mapping) else None
    if isinstance(fields, list) and isinstance(rows, list):
        for row in rows:
            if not isinstance(row, list):
                continue
            values = dict(zip(fields, row))
            ticker = normalize_symbol(values.get("ticker"))
            cik = normalize_cik(values.get("cik"))
            if ticker and cik:
                parsed[ticker] = {
                    "cik": cik,
                    "name": str(values.get("name") or ticker).strip(),
                    "exchange": str(values.get("exchange") or "").strip(),
                }
        return parsed

    for row in payload.values() if isinstance(payload, Mapping) else ():
        if not isinstance(row, Mapping):
            continue
        ticker = normalize_symbol(row.get("ticker"))
        cik = normalize_cik(row.get("cik_str") or row.get("cik"))
        if ticker and cik:
            parsed[ticker] = {
                "cik": cik,
                "name": str(row.get("title") or row.get("name") or ticker).strip(),
                "exchange": str(row.get("exchange") or "").strip(),
            }
    return parsed


def build_registry_from_portfolio(
    seeds: Iterable[PortfolioInstrumentSeed],
    *,
    sec_companies: Mapping[str, Mapping[str, Any]] | None = None,
) -> tuple[list[IssuerRegistryEntry], list[IssuerInstrument]]:
    """Map current local instruments to issuer records without assuming a trade venue."""
    entries: dict[str, IssuerRegistryEntry] = {}
    instruments: dict[str, IssuerInstrument] = {}
    companies = {normalize_symbol(key): value for key, value in (sec_companies or {}).items()}

    for seed in seeds:
        ticker = normalize_symbol(seed.ticker)
        if not ticker:
            continue
        asset_type = normalize_symbol(seed.asset_type) or "UNKNOWN"
        currency = normalize_symbol(seed.currency) or "ARS"
        sec_company = companies.get(ticker)
        issuer_hint = normalize_symbol(seed.issuer_hint)

        if sec_company and normalize_cik(sec_company.get("cik")):
            cik = normalize_cik(sec_company.get("cik"))
            issuer_id = f"SEC:{cik}"
            entry = IssuerRegistryEntry(
                issuer_id=issuer_id,
                issuer_name=str(sec_company.get("name") or ticker).strip(),
                source_market="US",
                primary_symbol=ticker,
                sec_cik=cik,
                metadata={"sec_exchange": str(sec_company.get("exchange") or "")},
            ).normalized()
        elif asset_type == "ACCION" or issuer_hint:
            native_issuer = issuer_hint or ticker
            issuer_id = f"AR:{native_issuer}"
            entry = IssuerRegistryEntry(
                issuer_id=issuer_id,
                issuer_name=native_issuer,
                source_market="AR",
                primary_symbol=ticker,
                cnv_entity_name=native_issuer,
                metadata={"registry_basis": "portfolio_or_corporate_hint"},
            ).normalized()
        else:
            issuer_id = f"US:{ticker}"
            entry = IssuerRegistryEntry(
                issuer_id=issuer_id,
                issuer_name=ticker,
                source_market="US",
                primary_symbol=ticker,
                metadata={"registry_basis": "unresolved_cedear_symbol"},
            ).normalized()

        entries[entry.issuer_id] = entry
        instrument = IssuerInstrument(
            issuer_id=entry.issuer_id,
            ticker=ticker,
            instrument_id=instrument_id_for(
                ticker,
                venue="BYMA",
                asset_type=asset_type,
                currency=currency,
            ),
            venue="BYMA",
            asset_type=asset_type,
            currency=currency,
            source_ticker=ticker,
            metadata={"registry_basis": "latest_portfolio_snapshot"},
        ).normalized()
        instruments[instrument.instrument_id] = instrument

    return list(entries.values()), list(instruments.values())


def sec_submission_observations(
    entry: IssuerRegistryEntry,
    payload: Mapping[str, Any],
    *,
    since: date | None = None,
) -> list[IssuerEventObservation]:
    entry = entry.normalized()
    recent = payload.get("filings", {}).get("recent", {}) if isinstance(payload, Mapping) else {}
    if not isinstance(recent, Mapping):
        return []
    accessions = list(recent.get("accessionNumber") or [])
    forms = list(recent.get("form") or [])
    filing_dates = list(recent.get("filingDate") or [])
    accepted_dates = list(recent.get("acceptanceDateTime") or [])
    primary_documents = list(recent.get("primaryDocument") or [])
    report_dates = list(recent.get("reportDate") or [])
    items = list(recent.get("items") or [])
    observations: list[IssuerEventObservation] = []
    cik_without_zeros = str(int(entry.sec_cik)) if entry.sec_cik else ""

    for index, accession in enumerate(accessions):
        form = str(forms[index] if index < len(forms) else "").upper().strip()
        if form not in SEC_RELEVANT_FORMS:
            continue
        accession_text = str(accession or "").strip()
        if not accession_text:
            continue
        document = str(primary_documents[index] if index < len(primary_documents) else "").strip()
        archive = accession_text.replace("-", "")
        source_url = (
            f"https://www.sec.gov/Archives/edgar/data/{cik_without_zeros}/{archive}/{document}"
            if cik_without_zeros and document
            else SEC_SUBMISSIONS_URL.format(cik=entry.sec_cik)
        )
        filed = _date_from_value(filing_dates[index] if index < len(filing_dates) else None)
        if since and filed and filed < since:
            continue
        observations.append(
            IssuerEventObservation(
                observation_key=f"SEC:{entry.sec_cik}:{accession_text}",
                issuer_id=entry.issuer_id,
                ticker=entry.primary_symbol,
                source="SEC",
                event_type="FILING",
                lifecycle_status="DISCOVERED",
                event_date=filed,
                event_time_hint="unknown",
                source_published_at=_datetime_from_value(
                    accepted_dates[index] if index < len(accepted_dates) else None
                ),
                source_url=source_url,
                confidence=confidence_for("primary_official"),
                title=f"{entry.primary_symbol} {form} filing",
                raw_payload={
                    "accession_number": accession_text,
                    "form": form,
                    "filing_date": filed.isoformat() if filed else None,
                    "report_date": str(report_dates[index] if index < len(report_dates) else ""),
                    "items": str(items[index] if index < len(items) else ""),
                    "primary_document": document,
                    "requires_structured_extraction": form in {"8-K", "6-K"},
                    "confidence_basis": "primary_official",
                },
            ).normalized()
        )
    return observations


def fmp_split_observations(
    payload: Any,
    registry_by_symbol: Mapping[str, IssuerRegistryEntry],
    *,
    today: date | None = None,
) -> list[IssuerEventObservation]:
    today = today or date.today()
    rows = payload if isinstance(payload, list) else payload.get("data", []) if isinstance(payload, Mapping) else []
    observations: list[IssuerEventObservation] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        symbol = normalize_symbol(row.get("symbol"))
        entry = registry_by_symbol.get(symbol)
        event_date = _date_from_value(row.get("date"))
        try:
            numerator = float(row.get("numerator"))
            denominator = float(row.get("denominator"))
        except (TypeError, ValueError):
            continue
        if not entry or not event_date or numerator <= 0 or denominator <= 0:
            continue
        factor = numerator / denominator
        event_type = "SPLIT" if factor >= 1 else "REVERSE_SPLIT"
        observations.append(
            IssuerEventObservation(
                observation_key=(
                    f"FMP:{symbol}:{event_date.isoformat()}:{numerator:g}:{denominator:g}"
                ),
                issuer_id=entry.issuer_id,
                ticker=symbol,
                source="FMP",
                event_type=event_type,
                lifecycle_status="ANNOUNCED" if event_date >= today else "DISCOVERED",
                event_date=event_date,
                event_time_hint="unknown",
                source_published_at=None,
                source_url=(
                    f"{FMP_SPLITS_CALENDAR_URL}?"
                    + urlencode({"from": event_date.isoformat(), "to": event_date.isoformat()})
                ),
                confidence=confidence_for("structured_provider"),
                title=f"{symbol} {event_type.lower()} {numerator:g}:{denominator:g}",
                raw_payload={
                    "numerator": numerator,
                    "denominator": denominator,
                    "quantity_factor": factor,
                    "provider_row": dict(row),
                    "confidence_basis": "structured_provider",
                },
            ).normalized()
        )
    return observations


def finnhub_earnings_observations(
    payload: Mapping[str, Any],
    registry_by_symbol: Mapping[str, IssuerRegistryEntry],
) -> list[IssuerEventObservation]:
    rows = payload.get("earningsCalendar") or []
    observations: list[IssuerEventObservation] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        symbol = normalize_symbol(row.get("symbol"))
        entry = registry_by_symbol.get(symbol)
        event_date = _date_from_value(row.get("date"))
        if not entry or not event_date:
            continue
        hour = str(row.get("hour") or "").lower().strip()
        time_hint = {"bmo": "before_open", "amc": "after_close", "dmh": "during_market"}.get(hour, "unknown")
        quarter = str(row.get("quarter") or "")
        year = str(row.get("year") or "")
        observations.append(
            IssuerEventObservation(
                observation_key=f"FINNHUB:EARNINGS:{symbol}:{event_date.isoformat()}:{year}:{quarter}",
                issuer_id=entry.issuer_id,
                ticker=symbol,
                source="FINNHUB",
                event_type="EARNINGS",
                lifecycle_status="ANNOUNCED",
                event_date=event_date,
                event_time_hint=time_hint,
                source_published_at=None,
                source_url=(
                    f"{FINNHUB_EARNINGS_CALENDAR_URL}?"
                    + urlencode({"from": event_date.isoformat(), "to": event_date.isoformat(), "symbol": symbol})
                ),
                confidence=confidence_for("structured_provider"),
                title=f"{symbol} earnings {event_date.isoformat()} ({hour or 'time unknown'})",
                raw_payload={
                    "provider_row": dict(row),
                    "confidence_basis": "structured_provider",
                },
            ).normalized()
        )
    return observations


def _cnv_datetime(value: str) -> datetime | None:
    normalized = _normalized_text(value).lower()
    match = re.search(r"(\d{1,2})\s+([a-z]{3})\.?\s+(\d{4})(?:\s+(\d{1,2}):(\d{2}))?", normalized)
    if not match:
        return None
    month = CNV_SPANISH_MONTHS.get(match.group(2))
    if not month:
        return None
    return datetime(
        int(match.group(3)),
        month,
        int(match.group(1)),
        int(match.group(4) or 0),
        int(match.group(5) or 0),
        tzinfo=ART_TZ,
    ).astimezone(timezone.utc)


def _cnv_event_type(value: str) -> str:
    normalized = _normalized_text(value).lower()
    if any(token in normalized for token in ("consolidacion", "reverse split", "agrupamiento")):
        return "REVERSE_SPLIT"
    if any(token in normalized for token in ("split", "desdoblamiento")):
        return "SPLIT"
    if any(token in normalized for token in ("cambio de ratio", "ratio cedear", "ratio de cedear")):
        return "DEPOSITARY_RATIO_CHANGE"
    if any(token in normalized for token in ("desliste", "delisting", "retiro de cotizacion")):
        return "DELISTING"
    if any(token in normalized for token in ("merger", "fusion", "absorcion")):
        return "MERGER"
    if "dividendo" in normalized:
        return "DIVIDEND"
    if any(token in normalized for token in ("balance", "estados contables", "resultados")):
        return "EARNINGS"
    return "RELEVANT_FACT"


def _cnv_row_matches(
    cells: list[str],
    entries: list[IssuerRegistryEntry],
) -> list[tuple[IssuerRegistryEntry, str]]:
    entity_key = _entity_key(cells[1]) if len(cells) > 1 else ""
    direct = [
        (entry, "issuer_entity")
        for entry in entries
        if entry.cnv_entity_name and _entity_key(entry.cnv_entity_name) == entity_key
    ]

    description = _normalized_text(cells[2] if len(cells) > 2 else "")
    if "CEDEAR" not in description:
        return direct
    description_tokens = set(re.findall(r"[A-Z0-9]+(?:\.[A-Z0-9]+)?", description))
    cedears = [
        (entry, "cedear_ticker")
        for entry in entries
        if entry.source_market == "US"
        and entry.primary_symbol
        and entry.primary_symbol in description_tokens
    ]
    by_issuer = {entry.issuer_id: (entry, basis) for entry, basis in direct + cedears}
    return list(by_issuer.values())


def cnv_relevant_fact_observations(
    content: str,
    registry_entries: Iterable[IssuerRegistryEntry],
) -> list[IssuerEventObservation]:
    """Parse CNV rows conservatively; unknown markup produces no observation."""
    entries = [
        entry.normalized()
        for entry in registry_entries
        if entry.cnv_entity_name or entry.primary_symbol
    ]
    if not entries:
        return []
    soup = BeautifulSoup(content, "html.parser")
    observations: list[IssuerEventObservation] = []
    seen: set[str] = set()
    for row in soup.find_all("tr"):
        cells = [cell.get_text(" ", strip=True) for cell in row.find_all(["td", "th"])]
        row_text = " ".join(cells).strip()
        if len(cells) < 3 or not row_text:
            continue
        matches = _cnv_row_matches(cells, entries)
        if not matches:
            continue
        link = row.find("a", href=True)
        href = str(link.get("href") or "") if link else ""
        source_url = urljoin(CNV_RELEVANT_FACTS_URL, href) if href else CNV_RELEVANT_FACTS_URL
        document_match = re.search(r"(\d{5,})", source_url) or re.search(r"(\d{5,})", row_text)
        document_ref = document_match.group(1) if document_match else ""
        published = _cnv_datetime(row_text)
        key_basis = document_ref or _normalized_text(row_text)
        event_type = _cnv_event_type(row_text)
        for matched, match_basis in matches:
            observation_key = f"CNV:{matched.issuer_id}:{key_basis}"
            if observation_key in seen:
                continue
            seen.add(observation_key)
            observations.append(
                IssuerEventObservation(
                    observation_key=observation_key,
                    issuer_id=matched.issuer_id,
                    ticker=matched.primary_symbol,
                    source="CNV",
                    event_type=event_type,
                    lifecycle_status="DISCOVERED",
                    event_date=published.date() if published else None,
                    event_time_hint="unknown",
                    source_published_at=published,
                    source_url=source_url,
                    confidence=confidence_for(
                        "regulator_issuer_match"
                        if match_basis == "issuer_entity"
                        else "regulator_instrument_match"
                    ),
                    title=row_text[:1000],
                    raw_payload={
                        "document_ref": document_ref,
                        "cells": cells,
                        "match_basis": match_basis,
                        "confidence_basis": (
                            "regulator_issuer_match"
                            if match_basis == "issuer_entity"
                            else "regulator_instrument_match"
                        ),
                        "source_page": CNV_RELEVANT_FACTS_URL,
                    },
                ).normalized()
            )
    return observations


async def fetch_sec_company_directory(client: httpx.AsyncClient) -> dict[str, dict[str, str]]:
    response = await client.get(SEC_TICKER_DIRECTORY_URL)
    response.raise_for_status()
    return parse_sec_company_directory(response.json())


async def fetch_sec_filings(
    client: httpx.AsyncClient,
    registry_entries: Iterable[IssuerRegistryEntry],
    *,
    since: date | None = None,
) -> list[IssuerEventObservation]:
    observations: list[IssuerEventObservation] = []
    for entry in registry_entries:
        normalized = entry.normalized()
        if normalized.source_market != "US" or not normalized.sec_cik:
            continue
        try:
            response = await client.get(SEC_SUBMISSIONS_URL.format(cik=normalized.sec_cik))
            response.raise_for_status()
            observations.extend(
                sec_submission_observations(normalized, response.json(), since=since)
            )
        except httpx.HTTPError as exc:
            logger.warning("SEC submissions unavailable for %s: %s", normalized.primary_symbol, exc)
    return observations


async def fetch_fmp_splits(
    client: httpx.AsyncClient,
    registry_entries: Iterable[IssuerRegistryEntry],
    *,
    api_key: str,
    from_date: date,
    to_date: date,
) -> list[IssuerEventObservation]:
    if not api_key:
        return []
    response = await client.get(
        FMP_SPLITS_CALENDAR_URL,
        params={"from": from_date.isoformat(), "to": to_date.isoformat(), "apikey": api_key},
    )
    response.raise_for_status()
    by_symbol = {
        entry.normalized().primary_symbol: entry.normalized()
        for entry in registry_entries
        if entry.normalized().source_market == "US"
    }
    return fmp_split_observations(response.json(), by_symbol, today=date.today())


async def fetch_finnhub_earnings(
    client: httpx.AsyncClient,
    registry_entries: Iterable[IssuerRegistryEntry],
    *,
    api_key: str,
    from_date: date,
    to_date: date,
) -> list[IssuerEventObservation]:
    if not api_key:
        return []
    observations: list[IssuerEventObservation] = []
    entries = {
        entry.normalized().primary_symbol: entry.normalized()
        for entry in registry_entries
        if entry.normalized().source_market == "US" and entry.normalized().primary_symbol
    }
    semaphore = asyncio.Semaphore(4)

    async def fetch_one(symbol: str) -> list[IssuerEventObservation]:
        async with semaphore:
            response = await client.get(
                FINNHUB_EARNINGS_CALENDAR_URL,
                params={
                    "from": from_date.isoformat(),
                    "to": to_date.isoformat(),
                    "symbol": symbol,
                    "token": api_key,
                },
            )
            response.raise_for_status()
            return finnhub_earnings_observations(response.json(), {symbol: entries[symbol]})

    results = await asyncio.gather(
        *(fetch_one(symbol) for symbol in entries),
        return_exceptions=True,
    )
    for symbol, result in zip(entries, results):
        if isinstance(result, Exception):
            logger.warning(
                "Finnhub earnings unavailable for %s: %s",
                symbol,
                type(result).__name__,
            )
            continue
        observations.extend(result)
    return observations


async def fetch_cnv_relevant_facts(
    client: httpx.AsyncClient,
    registry_entries: Iterable[IssuerRegistryEntry],
) -> list[IssuerEventObservation]:
    response = await client.get(CNV_RELEVANT_FACTS_URL)
    response.raise_for_status()
    return cnv_relevant_fact_observations(response.text, registry_entries)


def issuer_event_http_client(*, timeout_seconds: float, sec_user_agent: str) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=httpx.Timeout(timeout_seconds),
        follow_redirects=True,
        headers={
            "User-Agent": sec_user_agent,
            "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
        },
    )

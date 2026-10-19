"""Official public transaction disclosures, kept separate from Quantia signals.

SEC Form 4 records are *filings*, not a real-time trading tape.
Only non-derivative open-market P/S transactions are included in v1.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation
import hashlib
import re
from typing import Any, Mapping
import xml.etree.ElementTree as ET


DISCLOSURES_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS public_disclosure_events (
    event_key TEXT PRIMARY KEY,
    category TEXT NOT NULL CHECK (category IN ('INSIDER', 'CONGRESS')),
    source TEXT NOT NULL,
    ticker TEXT NOT NULL,
    sec_cik TEXT,
    person_name TEXT NOT NULL,
    person_role TEXT,
    direction TEXT NOT NULL CHECK (direction IN ('BUY', 'SELL')),
    occurred_on DATE NOT NULL,
    filed_on DATE NOT NULL,
    accepted_at TIMESTAMPTZ,
    shares NUMERIC,
    unit_price_usd NUMERIC,
    value_usd NUMERIC,
    ownership_type TEXT,
    source_url TEXT NOT NULL,
    source_hash TEXT NOT NULL,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_public_disclosure_filed
    ON public_disclosure_events (filed_on DESC, event_key);
CREATE INDEX IF NOT EXISTS idx_public_disclosure_ticker
    ON public_disclosure_events (ticker, filed_on DESC);
"""


def _decimal_text(value: str | None) -> str | None:
    if not value:
        return None
    try:
        number = Decimal(value.replace(",", "").strip())
    except (InvalidOperation, AttributeError):
        return None
    if not number.is_finite() or number < 0:
        return None
    return str(number)


def _text(element: ET.Element, path: str) -> str:
    return (element.findtext(path) or "").strip()


def recent_form4_filings(
    submissions: Mapping[str, Any],
    *,
    cik: str,
    since: date,
    limit: int = 15,
) -> list[dict[str, str]]:
    """Get the actual SEC filing references. No historical/publish-date inference."""
    recent = submissions.get("filings", {}).get("recent", {})
    keys = ("form", "accessionNumber", "filingDate", "primaryDocument")
    arrays = [recent.get(key, []) for key in keys]
    if not all(isinstance(part, list) for part in arrays):
        return []
    found: list[dict[str, str]] = []
    for form, accession, filed, document in zip(*arrays):
        if form not in {"4", "4/A"}:
            continue
        try:
            filing_date = date.fromisoformat(str(filed))
        except ValueError:
            continue
        if filing_date < since:
            continue
        accession = str(accession)
        # Only the ownership XML is supported; do not parse rendered HTML.
        filename = str(document).rsplit("/", 1)[-1]
        if not re.fullmatch(r"[A-Za-z0-9_.-]+\.xml", filename, flags=re.IGNORECASE):
            continue
        if not re.fullmatch(r"\d{10}-\d{2}-\d{6}", accession):
            continue
        url = (
            f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/"
            f"{accession.replace('-', '')}/{filename}"
        )
        found.append({
            "accession": accession, "filed_on": filing_date.isoformat(),
            "source_url": url, "form": str(form),
        })
        if len(found) >= limit:
            break
    return found


def parse_sec_form4(
    xml: str,
    *,
    ticker: str,
    cik: str,
    accession: str,
    filed_on: str,
    source_url: str,
    accepted_at: str | None = None,
) -> list[dict[str, Any]]:
    """Parse open-market common/non-derivative P and S, not M/F/G/awards/options."""
    if len(xml) > 3_000_000:
        raise ValueError("SEC ownership XML exceeds size limit")
    root = ET.fromstring(xml)
    # SEC filings sometimes include default XML namespaces.
    for element in root.iter():
        if isinstance(element.tag, str):
            element.tag = element.tag.split("}", 1)[-1]
    if root.tag != "ownershipDocument":
        raise ValueError("not an SEC ownershipDocument")

    names = [
        _text(owner, "reportingOwnerId/rptOwnerName")
        for owner in root.findall("./reportingOwner")
    ]
    names = [n for n in names if n]
    if not names:
        return []
    person = "; ".join(dict.fromkeys(names))
    relationship = root.find("./reportingOwner/reportingOwnerRelationship")
    roles = []
    if relationship is not None:
        if _text(relationship, "isDirector") == "1":
            roles.append("Director")
        if _text(relationship, "isOfficer") == "1":
            roles.append(_text(relationship, "officerTitle") or "Officer")
        if _text(relationship, "isTenPercentOwner") == "1":
            roles.append("10% Owner")
    role = ", ".join(roles) or "Reporting owner"

    digest = hashlib.sha256(xml.encode("utf-8")).hexdigest()
    results: list[dict[str, Any]] = []
    for index, transaction in enumerate(root.findall("./nonDerivativeTable/nonDerivativeTransaction")):
        code = _text(transaction, "transactionCoding/transactionCode").upper()
        disposition = _text(
            transaction, "transactionAmounts/transactionAcquiredDisposedCode/value"
        ).upper()
        if code not in {"P", "S"} or disposition != ("A" if code == "P" else "D"):
            continue
        day = _text(transaction, "transactionDate/value")
        try:
            date.fromisoformat(day)
        except ValueError:
            continue
        shares = _decimal_text(_text(transaction, "transactionAmounts/transactionShares/value"))
        price = _decimal_text(_text(transaction, "transactionAmounts/transactionPricePerShare/value"))
        if shares is None:
            continue
        value = str(Decimal(shares) * Decimal(price)) if price is not None else None
        results.append({
            "event_key": f"SEC_FORM4:{cik}:{accession}:{index}",
            "category": "INSIDER", "source": "SEC_FORM4",
            "ticker": ticker.upper(), "sec_cik": cik,
            "person_name": person, "person_role": role,
            "direction": "BUY" if code == "P" else "SELL",
            "occurred_on": day, "filed_on": filed_on, "accepted_at": accepted_at,
            "shares": shares, "unit_price_usd": price, "value_usd": value,
            "ownership_type": _text(transaction, "ownershipNature/directOrIndirectOwnership/value") or None,
            "source_url": source_url, "source_hash": digest,
            "raw_payload": {
                "accession": accession, "transaction_index": index,
                "transaction_code": code, "disposition_code": disposition,
                "security_title": _text(transaction, "securityTitle/value"),
                "footnote_ids": [
                    f.get("id") for f in transaction.findall(".//footnoteId") if f.get("id")
                ],
                "reporting_owners": names,
            },
        })
    return results

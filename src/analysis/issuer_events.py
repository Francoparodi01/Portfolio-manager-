"""Auditable issuer-event discovery contracts.

This layer records observations from issuer and exchange sources before they
become corporate actions or trading-event guards. It is intentionally shadow
only: observations never modify scores, plans, or instrument price bases.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
import hashlib
import json
from typing import Any, Mapping


ISSUER_SOURCE_MARKETS = {"US", "AR", "OTHER"}
ISSUER_EVENT_SOURCES = {"SEC", "FMP", "FINNHUB", "CNV"}
ISSUER_EVENT_TYPES = {
    "FILING",
    "EARNINGS",
    "SPLIT",
    "REVERSE_SPLIT",
    "DEPOSITARY_RATIO_CHANGE",
    "DIVIDEND",
    "MERGER",
    "DELISTING",
    "RELEVANT_FACT",
}
ISSUER_EVENT_STATUSES = {
    "DISCOVERED",
    "ANNOUNCED",
    "CONFIRMED",
    "CANCELLED",
    "DISMISSED",
}
EVENT_TIME_HINTS = {"before_open", "during_market", "after_close", "unknown"}
OBSERVATION_CONFIDENCE = {
    "primary_official": 1.0,
    "regulator_issuer_match": 0.95,
    "regulator_instrument_match": 0.85,
    "structured_provider": 0.75,
}

# DISCOVERED records raw evidence, ANNOUNCED adds a dated provider claim, and
# CONFIRMED requires later corroboration. This shadow layer never marks events
# effective; effective transformations belong to corporate_events.
ISSUER_EVENT_STATUS_MEANINGS = {
    "DISCOVERED": "raw source evidence captured",
    "ANNOUNCED": "source declares a future or current event",
    "CONFIRMED": "observation corroborated by a later stage",
    "CANCELLED": "source reports cancellation",
    "DISMISSED": "observation rejected after review",
}


ISSUER_EVENTS_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS issuer_registry (
    issuer_id         TEXT PRIMARY KEY,
    issuer_name       TEXT NOT NULL,
    source_market     TEXT NOT NULL,
    primary_symbol    TEXT,
    sec_cik           TEXT,
    cnv_entity_name   TEXT,
    metadata          JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (source_market IN ('US', 'AR', 'OTHER'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_issuer_registry_sec_cik
    ON issuer_registry (sec_cik)
    WHERE sec_cik IS NOT NULL;

CREATE TABLE IF NOT EXISTS issuer_instruments (
    id                BIGSERIAL PRIMARY KEY,
    issuer_id         TEXT NOT NULL REFERENCES issuer_registry(issuer_id) ON DELETE CASCADE,
    ticker            TEXT NOT NULL,
    instrument_id     TEXT NOT NULL UNIQUE,
    venue             TEXT,
    asset_type        TEXT,
    currency          TEXT,
    source_ticker     TEXT,
    metadata          JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (ticker, venue, currency)
);

CREATE INDEX IF NOT EXISTS idx_issuer_instruments_active
    ON issuer_instruments (issuer_id, is_active, ticker);

CREATE TABLE IF NOT EXISTS issuer_event_observations (
    id                  BIGSERIAL PRIMARY KEY,
    observation_key     TEXT NOT NULL UNIQUE,
    issuer_id           TEXT NOT NULL REFERENCES issuer_registry(issuer_id) ON DELETE CASCADE,
    ticker              TEXT,
    source              TEXT NOT NULL,
    event_type          TEXT NOT NULL,
    lifecycle_status    TEXT NOT NULL,
    event_date          DATE,
    event_time_hint     TEXT NOT NULL DEFAULT 'unknown',
    source_published_at TIMESTAMPTZ,
    source_url          TEXT NOT NULL,
    source_hash         TEXT NOT NULL,
    confidence          FLOAT NOT NULL,
    actionable          BOOLEAN NOT NULL DEFAULT FALSE,
    title               TEXT NOT NULL,
    raw_payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (source IN ('SEC', 'FMP', 'FINNHUB', 'CNV')),
    CHECK (event_type IN (
        'FILING', 'EARNINGS', 'SPLIT', 'REVERSE_SPLIT',
        'DEPOSITARY_RATIO_CHANGE', 'DIVIDEND', 'MERGER',
        'DELISTING', 'RELEVANT_FACT'
    )),
    CHECK (lifecycle_status IN ('DISCOVERED', 'ANNOUNCED', 'CONFIRMED', 'CANCELLED', 'DISMISSED')),
    CHECK (event_time_hint IN ('before_open', 'during_market', 'after_close', 'unknown')),
    CHECK (confidence >= 0 AND confidence <= 1)
);

CREATE INDEX IF NOT EXISTS idx_issuer_event_observations_lookup
    ON issuer_event_observations (issuer_id, event_date DESC, source, event_type);

CREATE INDEX IF NOT EXISTS idx_issuer_event_observations_ticker
    ON issuer_event_observations (ticker, created_at DESC);
"""


def normalize_symbol(value: Any) -> str:
    return str(value or "").upper().strip()


def normalize_cik(value: Any) -> str:
    digits = "".join(char for char in str(value or "") if char.isdigit())
    return digits.zfill(10) if digits else ""


def normalize_source_market(value: Any) -> str:
    candidate = str(value or "OTHER").upper().strip()
    return candidate if candidate in ISSUER_SOURCE_MARKETS else "OTHER"


def normalize_event_source(value: Any) -> str:
    candidate = str(value or "").upper().strip()
    if candidate not in ISSUER_EVENT_SOURCES:
        raise ValueError(f"unsupported issuer event source: {candidate or '<empty>'}")
    return candidate


def normalize_event_type(value: Any) -> str:
    candidate = str(value or "FILING").upper().strip()
    return candidate if candidate in ISSUER_EVENT_TYPES else "FILING"


def normalize_event_status(value: Any) -> str:
    candidate = str(value or "DISCOVERED").upper().strip()
    return candidate if candidate in ISSUER_EVENT_STATUSES else "DISCOVERED"


def normalize_time_hint(value: Any) -> str:
    candidate = str(value or "unknown").lower().strip()
    return candidate if candidate in EVENT_TIME_HINTS else "unknown"


def payload_hash(payload: Mapping[str, Any] | None) -> str:
    encoded = json.dumps(
        dict(payload or {}),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def confidence_for(basis: str) -> float:
    normalized = str(basis or "").lower().strip()
    if normalized not in OBSERVATION_CONFIDENCE:
        raise ValueError(f"unsupported confidence basis: {basis or '<empty>'}")
    return OBSERVATION_CONFIDENCE[normalized]


def instrument_id_for(
    ticker: str,
    *,
    venue: str,
    asset_type: str,
    currency: str,
) -> str:
    return ":".join(
        (
            normalize_symbol(venue) or "UNKNOWN",
            normalize_symbol(asset_type) or "UNKNOWN",
            normalize_symbol(ticker),
            normalize_symbol(currency) or "UNKNOWN",
        )
    )


@dataclass(frozen=True)
class IssuerRegistryEntry:
    issuer_id: str
    issuer_name: str
    source_market: str
    primary_symbol: str = ""
    sec_cik: str = ""
    cnv_entity_name: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not str(self.issuer_id or "").strip():
            raise ValueError("issuer_id is required")
        if not str(self.issuer_name or "").strip():
            raise ValueError("issuer_name is required")

    def normalized(self) -> "IssuerRegistryEntry":
        return IssuerRegistryEntry(
            issuer_id=str(self.issuer_id).upper().strip(),
            issuer_name=str(self.issuer_name).strip(),
            source_market=normalize_source_market(self.source_market),
            primary_symbol=normalize_symbol(self.primary_symbol),
            sec_cik=normalize_cik(self.sec_cik),
            cnv_entity_name=str(self.cnv_entity_name or "").strip(),
            metadata=dict(self.metadata or {}),
        )


@dataclass(frozen=True)
class IssuerInstrument:
    issuer_id: str
    ticker: str
    instrument_id: str
    venue: str
    asset_type: str
    currency: str
    source_ticker: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def normalized(self) -> "IssuerInstrument":
        ticker = normalize_symbol(self.ticker)
        venue = normalize_symbol(self.venue)
        asset_type = normalize_symbol(self.asset_type)
        currency = normalize_symbol(self.currency)
        return IssuerInstrument(
            issuer_id=str(self.issuer_id).upper().strip(),
            ticker=ticker,
            instrument_id=str(self.instrument_id or instrument_id_for(
                ticker,
                venue=venue,
                asset_type=asset_type,
                currency=currency,
            )).upper().strip(),
            venue=venue,
            asset_type=asset_type,
            currency=currency,
            source_ticker=normalize_symbol(self.source_ticker) or ticker,
            metadata=dict(self.metadata or {}),
        )


@dataclass(frozen=True)
class IssuerEventObservation:
    observation_key: str
    issuer_id: str
    ticker: str
    source: str
    event_type: str
    lifecycle_status: str
    event_date: date | None
    event_time_hint: str
    source_published_at: datetime | None
    source_url: str
    confidence: float
    title: str
    raw_payload: dict[str, Any] = field(default_factory=dict)
    actionable: bool = False

    @property
    def source_hash(self) -> str:
        return payload_hash(self.raw_payload)

    def normalized(self) -> "IssuerEventObservation":
        key = str(self.observation_key or "").strip()
        if not key:
            raise ValueError("observation_key is required")
        url = str(self.source_url or "").strip()
        if not url:
            raise ValueError("source_url is required")
        title = str(self.title or "").strip()
        if not title:
            raise ValueError("title is required")
        return IssuerEventObservation(
            observation_key=key,
            issuer_id=str(self.issuer_id).upper().strip(),
            ticker=normalize_symbol(self.ticker),
            source=normalize_event_source(self.source),
            event_type=normalize_event_type(self.event_type),
            lifecycle_status=normalize_event_status(self.lifecycle_status),
            event_date=self.event_date,
            event_time_hint=normalize_time_hint(self.event_time_hint),
            source_published_at=self.source_published_at,
            source_url=url,
            confidence=max(0.0, min(1.0, float(self.confidence))),
            title=title[:1000],
            raw_payload=dict(self.raw_payload or {}),
            actionable=bool(self.actionable),
        )

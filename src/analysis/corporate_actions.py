"""Corporate-action detection, normalization and audit contracts.

Raw market and portfolio records remain immutable. This module builds a
comparable operational view and records why a price was normalized or blocked.
It does not change model scores or trading thresholds.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from enum import Enum
from fractions import Fraction
import hashlib
import json
import math
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from src.core.market_calendar import is_trading_day


ART_TZ = ZoneInfo("America/Argentina/Buenos_Aires")
DETECTOR_VERSION = "corporate_action_v1"
DEFAULT_SUSPECTED_SESSIONS = 2
MIN_ANOMALY_RETURN = 0.30
RATIO_RELATIVE_TOLERANCE = 0.08
OFFICIAL_RATIO_RELATIVE_TOLERANCE = 0.12


class CorporateEventType(str, Enum):
    SPLIT = "SPLIT"
    REVERSE_SPLIT = "REVERSE_SPLIT"
    DEPOSITARY_RATIO_CHANGE = "DEPOSITARY_RATIO_CHANGE"
    DIVIDEND = "DIVIDEND"
    SPIN_OFF = "SPIN_OFF"
    TICKER_CHANGE = "TICKER_CHANGE"
    OTHER = "OTHER"


class CorporateEventStatus(str, Enum):
    SUSPECTED = "SUSPECTED"
    ANNOUNCED = "ANNOUNCED"
    CONFIRMED = "CONFIRMED"
    EFFECTIVE = "EFFECTIVE"
    CANCELLED = "CANCELLED"
    DISMISSED = "DISMISSED"
    SUPERSEDED = "SUPERSEDED"


class CorporateApplicationStatus(str, Enum):
    PENDING = "PENDING"
    APPLYING = "APPLYING"
    APPLIED = "APPLIED"
    ALREADY_ADJUSTED = "ALREADY_ADJUSTED"
    FAILED = "FAILED"
    ROLLED_BACK = "ROLLED_BACK"


class EvidenceLevel(str, Enum):
    PRIMARY_OFFICIAL = "PRIMARY_OFFICIAL"
    STRUCTURED_SECONDARY = "STRUCTURED_SECONDARY"
    CORROBORATED = "CORROBORATED"
    HEURISTIC_ONLY = "HEURISTIC_ONLY"


class IngestionMethod(str, Enum):
    MANUAL = "MANUAL"
    AUTOMATED = "AUTOMATED"
    DETECTOR = "DETECTOR"


class PriceQualityStatus(str, Enum):
    COMPARABLE = "COMPARABLE"
    RECONCILED = "RECONCILED"
    PRICE_NOT_COMPARABLE = "PRICE_NOT_COMPARABLE"
    DATA_QUALITY_BLOCK = "DATA_QUALITY_BLOCK"


CORPORATE_ACTIONS_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS corporate_events (
    id                  BIGSERIAL PRIMARY KEY,
    event_key           TEXT NOT NULL UNIQUE,
    issuer_id           TEXT NOT NULL,
    event_type          TEXT NOT NULL,
    lifecycle_status    TEXT NOT NULL,
    announced_at        TIMESTAMPTZ,
    effective_at        TIMESTAMPTZ NOT NULL,
    expires_at          TIMESTAMPTZ,
    source_name         TEXT,
    source_url          TEXT,
    source_published_at TIMESTAMPTZ,
    source_hash         TEXT,
    ingestion_method    TEXT NOT NULL,
    evidence_level      TEXT NOT NULL,
    detector_score      FLOAT,
    detector_version    TEXT,
    raw_payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
    supersedes_event_id BIGINT REFERENCES corporate_events(id) ON DELETE SET NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (event_type IN (
        'SPLIT', 'REVERSE_SPLIT', 'DEPOSITARY_RATIO_CHANGE',
        'DIVIDEND', 'SPIN_OFF', 'TICKER_CHANGE', 'OTHER'
    )),
    CHECK (lifecycle_status IN (
        'SUSPECTED', 'ANNOUNCED', 'CONFIRMED', 'EFFECTIVE',
        'CANCELLED', 'DISMISSED', 'SUPERSEDED'
    )),
    CHECK (ingestion_method IN ('MANUAL', 'AUTOMATED', 'DETECTOR')),
    CHECK (evidence_level IN (
        'PRIMARY_OFFICIAL', 'STRUCTURED_SECONDARY',
        'CORROBORATED', 'HEURISTIC_ONLY'
    )),
    CHECK (detector_score IS NULL OR (detector_score >= 0 AND detector_score <= 1))
);

CREATE TABLE IF NOT EXISTS corporate_event_instrument_effects (
    id                      BIGSERIAL PRIMARY KEY,
    event_id                BIGINT NOT NULL REFERENCES corporate_events(id) ON DELETE CASCADE,
    instrument_id           TEXT NOT NULL,
    ticker                  TEXT NOT NULL,
    venue                   TEXT,
    asset_type              TEXT,
    currency                TEXT,
    quantity_factor         NUMERIC(24,12) NOT NULL,
    price_factor            NUMERIC(24,12) NOT NULL,
    cost_basis_factor       NUMERIC(24,12) NOT NULL,
    depositary_ratio_before TEXT,
    depositary_ratio_after  TEXT,
    metadata                JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active               BOOLEAN NOT NULL DEFAULT TRUE,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (event_id, instrument_id),
    CHECK (quantity_factor > 0),
    CHECK (price_factor > 0),
    CHECK (cost_basis_factor > 0)
);

CREATE TABLE IF NOT EXISTS price_quality_flags (
    id                       BIGSERIAL PRIMARY KEY,
    event_id                 BIGINT REFERENCES corporate_events(id) ON DELETE SET NULL,
    instrument_effect_id     BIGINT REFERENCES corporate_event_instrument_effects(id) ON DELETE SET NULL,
    ticker                   TEXT NOT NULL,
    observed_at              TIMESTAMPTZ NOT NULL,
    expires_at               TIMESTAMPTZ,
    flag_type                TEXT NOT NULL,
    resolution_status        TEXT NOT NULL DEFAULT 'OPEN',
    observed_reference_price FLOAT,
    observed_current_price   FLOAT,
    observed_return          FLOAT,
    expected_price_factor    FLOAT,
    observed_quantity_factor FLOAT,
    quantity_factor          FLOAT,
    evidence_level           TEXT NOT NULL,
    detector_score           FLOAT,
    detector_version         TEXT NOT NULL,
    action_taken             TEXT NOT NULL,
    reason                   TEXT NOT NULL,
    evidence                 JSONB NOT NULL DEFAULT '{}'::jsonb,
    idempotency_key          TEXT NOT NULL UNIQUE,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (resolution_status IN ('OPEN', 'CONFIRMED', 'DISMISSED', 'EXPIRED')),
    CHECK (flag_type IN ('PRICE_NOT_COMPARABLE', 'DATA_QUALITY_BLOCK')),
    CHECK (detector_score IS NULL OR (detector_score >= 0 AND detector_score <= 1))
);

CREATE TABLE IF NOT EXISTS corporate_event_applications (
    id                   BIGSERIAL PRIMARY KEY,
    event_id             BIGINT NOT NULL REFERENCES corporate_events(id) ON DELETE CASCADE,
    instrument_effect_id BIGINT NOT NULL REFERENCES corporate_event_instrument_effects(id) ON DELETE CASCADE,
    owner_chat_id        BIGINT,
    component            TEXT NOT NULL,
    application_status   TEXT NOT NULL,
    adjustment_version   TEXT NOT NULL,
    idempotency_key      TEXT NOT NULL UNIQUE,
    before_state         JSONB NOT NULL DEFAULT '{}'::jsonb,
    after_state          JSONB NOT NULL DEFAULT '{}'::jsonb,
    invariant_checks     JSONB NOT NULL DEFAULT '{}'::jsonb,
    error                TEXT,
    applied_at           TIMESTAMPTZ,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (application_status IN (
        'PENDING', 'APPLYING', 'APPLIED', 'ALREADY_ADJUSTED',
        'FAILED', 'ROLLED_BACK'
    ))
);

CREATE INDEX IF NOT EXISTS idx_corporate_events_effective
    ON corporate_events (effective_at DESC, lifecycle_status);

CREATE INDEX IF NOT EXISTS idx_corporate_effects_ticker
    ON corporate_event_instrument_effects (ticker, is_active);

CREATE INDEX IF NOT EXISTS idx_price_quality_flags_active
    ON price_quality_flags (ticker, resolution_status, expires_at);

CREATE INDEX IF NOT EXISTS idx_corporate_applications_event
    ON corporate_event_applications (event_id, instrument_effect_id, component);
"""


@dataclass(frozen=True)
class CorporateActionEffect:
    event_id: int
    effect_id: int
    event_key: str
    issuer_id: str
    event_type: str
    lifecycle_status: str
    effective_at: datetime
    expires_at: datetime | None
    source_name: str
    source_url: str
    ingestion_method: str
    evidence_level: str
    detector_score: float | None
    instrument_id: str
    ticker: str
    venue: str
    asset_type: str
    currency: str
    quantity_factor: float
    price_factor: float
    cost_basis_factor: float
    depositary_ratio_before: str = ""
    depositary_ratio_after: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def is_official(self) -> bool:
        return self.evidence_level == EvidenceLevel.PRIMARY_OFFICIAL.value

    @property
    def is_effective(self) -> bool:
        return self.lifecycle_status == CorporateEventStatus.EFFECTIVE.value


@dataclass(frozen=True)
class PriceQualityFlag:
    ticker: str
    observed_at: datetime
    expires_at: datetime | None
    flag_type: str
    resolution_status: str
    observed_reference_price: float | None
    observed_current_price: float | None
    observed_return: float | None
    expected_price_factor: float | None
    observed_quantity_factor: float | None
    quantity_factor: float | None
    evidence_level: str
    detector_score: float | None
    action_taken: str
    reason: str
    evidence: dict[str, Any] = field(default_factory=dict)
    event_id: int | None = None
    instrument_effect_id: int | None = None
    detector_version: str = DETECTOR_VERSION
    idempotency_key: str = ""

    @property
    def blocks_price_use(self) -> bool:
        if self.resolution_status != "OPEN":
            return False
        if self.expires_at is None:
            return True
        return self.expires_at > datetime.now(timezone.utc)

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "instrument_effect_id": self.instrument_effect_id,
            "ticker": self.ticker,
            "observed_at": self.observed_at.isoformat(),
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "flag_type": self.flag_type,
            "resolution_status": self.resolution_status,
            "observed_reference_price": self.observed_reference_price,
            "observed_current_price": self.observed_current_price,
            "observed_return": self.observed_return,
            "expected_price_factor": self.expected_price_factor,
            "observed_quantity_factor": self.observed_quantity_factor,
            "quantity_factor": self.quantity_factor,
            "evidence_level": self.evidence_level,
            "detector_score": self.detector_score,
            "detector_version": self.detector_version,
            "action_taken": self.action_taken,
            "reason": self.reason,
            "evidence": self.evidence,
            "idempotency_key": self.idempotency_key,
        }


@dataclass(frozen=True)
class CorporateActionApplication:
    event_id: int
    instrument_effect_id: int
    component: str
    application_status: str
    idempotency_key: str
    before_state: dict[str, Any]
    after_state: dict[str, Any]
    invariant_checks: dict[str, Any]
    error: str | None = None
    owner_chat_id: int | None = None
    adjustment_version: str = DETECTOR_VERSION


@dataclass(frozen=True)
class FrameGuardResult:
    frames: dict[str, Any]
    blocked_by_ticker: dict[str, str]
    flags: tuple[PriceQualityFlag, ...]
    applications: tuple[CorporateActionApplication, ...]


@dataclass(frozen=True)
class LivePriceAssessment:
    ticker: str
    status: str
    raw_change: float | None
    normalized_change: float | None
    normalized_reference_price: float | None
    flag: PriceQualityFlag | None
    effect: CorporateActionEffect | None


def _as_utc(value: datetime | date | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, time(0, 0), tzinfo=ART_TZ)
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ART_TZ)
    return parsed.astimezone(timezone.utc)


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def _relative_error(observed: float, expected: float) -> float:
    if expected == 0:
        return float("inf")
    return abs((observed / expected) - 1.0)


def _stable_key(*parts: Any) -> str:
    payload = "|".join(str(part or "") for part in parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def add_market_sessions(value: datetime, sessions: int) -> datetime:
    current = value
    remaining = max(0, int(sessions))
    while remaining:
        current += timedelta(days=1)
        if is_trading_day(current.astimezone(ART_TZ).date()):
            remaining -= 1
    return current


def instrument_id_for(
    ticker: str,
    *,
    venue: str = "BYMA",
    asset_type: str = "UNKNOWN",
    currency: str = "ARS",
) -> str:
    return ":".join(
        (
            str(venue or "UNKNOWN").upper().strip(),
            str(asset_type or "UNKNOWN").upper().strip(),
            str(ticker or "").upper().strip(),
            str(currency or "UNKNOWN").upper().strip(),
        )
    )


def corporate_action_effect_from_row(row: Mapping[str, Any]) -> CorporateActionEffect:
    metadata = row.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except ValueError:
            metadata = {"raw": metadata}
    return CorporateActionEffect(
        event_id=int(row["event_id"]),
        effect_id=int(row["effect_id"]),
        event_key=str(row.get("event_key") or ""),
        issuer_id=str(row.get("issuer_id") or ""),
        event_type=str(row.get("event_type") or "OTHER").upper(),
        lifecycle_status=str(row.get("lifecycle_status") or "SUSPECTED").upper(),
        effective_at=_as_utc(row.get("effective_at")) or datetime.now(timezone.utc),
        expires_at=_as_utc(row.get("expires_at")),
        source_name=str(row.get("source_name") or ""),
        source_url=str(row.get("source_url") or ""),
        ingestion_method=str(row.get("ingestion_method") or "MANUAL").upper(),
        evidence_level=str(row.get("evidence_level") or "HEURISTIC_ONLY").upper(),
        detector_score=_safe_float(row.get("detector_score")),
        instrument_id=str(row.get("instrument_id") or ""),
        ticker=str(row.get("ticker") or "").upper(),
        venue=str(row.get("venue") or ""),
        asset_type=str(row.get("asset_type") or ""),
        currency=str(row.get("currency") or ""),
        quantity_factor=float(row.get("quantity_factor") or 1.0),
        price_factor=float(row.get("price_factor") or 1.0),
        cost_basis_factor=float(row.get("cost_basis_factor") or 1.0),
        depositary_ratio_before=str(row.get("depositary_ratio_before") or ""),
        depositary_ratio_after=str(row.get("depositary_ratio_after") or ""),
        metadata=dict(metadata),
    )


def price_quality_flag_from_mapping(row: Mapping[str, Any]) -> PriceQualityFlag:
    evidence = row.get("evidence") or {}
    if isinstance(evidence, str):
        try:
            evidence = json.loads(evidence)
        except ValueError:
            evidence = {"raw": evidence}
    observed_at = _as_utc(row.get("observed_at")) or datetime.now(timezone.utc)
    return PriceQualityFlag(
        event_id=(int(row["event_id"]) if row.get("event_id") is not None else None),
        instrument_effect_id=(
            int(row["instrument_effect_id"])
            if row.get("instrument_effect_id") is not None
            else None
        ),
        ticker=str(row.get("ticker") or "").upper(),
        observed_at=observed_at,
        expires_at=_as_utc(row.get("expires_at")),
        flag_type=str(row.get("flag_type") or PriceQualityStatus.PRICE_NOT_COMPARABLE.value),
        resolution_status=str(row.get("resolution_status") or "OPEN").upper(),
        observed_reference_price=_safe_float(row.get("observed_reference_price")),
        observed_current_price=_safe_float(row.get("observed_current_price")),
        observed_return=_safe_float(row.get("observed_return")),
        expected_price_factor=_safe_float(row.get("expected_price_factor")),
        observed_quantity_factor=_safe_float(row.get("observed_quantity_factor")),
        quantity_factor=_safe_float(row.get("quantity_factor")),
        evidence_level=str(row.get("evidence_level") or EvidenceLevel.HEURISTIC_ONLY.value),
        detector_score=_safe_float(row.get("detector_score")),
        detector_version=str(row.get("detector_version") or DETECTOR_VERSION),
        action_taken=str(row.get("action_taken") or "TEMPORARY_QUARANTINE"),
        reason=str(row.get("reason") or "PRICE_NOT_COMPARABLE"),
        evidence=dict(evidence),
        idempotency_key=str(row.get("idempotency_key") or _stable_key(
            DETECTOR_VERSION,
            row.get("ticker"),
            observed_at.isoformat(),
            row.get("flag_type"),
        )),
    )


def corporate_action_application_from_mapping(
    row: Mapping[str, Any],
) -> CorporateActionApplication:
    return CorporateActionApplication(
        event_id=int(row["event_id"]),
        instrument_effect_id=int(row["instrument_effect_id"]),
        owner_chat_id=(int(row["owner_chat_id"]) if row.get("owner_chat_id") is not None else None),
        component=str(row.get("component") or "UNKNOWN"),
        application_status=str(row.get("application_status") or "PENDING").upper(),
        adjustment_version=str(row.get("adjustment_version") or DETECTOR_VERSION),
        idempotency_key=str(row.get("idempotency_key") or ""),
        before_state=dict(row.get("before_state") or {}),
        after_state=dict(row.get("after_state") or {}),
        invariant_checks=dict(row.get("invariant_checks") or {}),
        error=str(row.get("error") or "") or None,
    )


def effects_by_ticker(
    effects: Iterable[CorporateActionEffect],
) -> dict[str, list[CorporateActionEffect]]:
    grouped: dict[str, list[CorporateActionEffect]] = {}
    for effect in effects or []:
        grouped.setdefault(effect.ticker.upper(), []).append(effect)
    for ticker in grouped:
        grouped[ticker].sort(key=lambda item: (item.effective_at, item.effect_id))
    return grouped


def matching_effect_for_quantity_transition(
    *,
    ticker: str,
    previous_quantity: float | None,
    current_quantity: float | None,
    previous_at: datetime,
    current_at: datetime,
    effects: Sequence[CorporateActionEffect],
) -> CorporateActionEffect | None:
    previous = _safe_float(previous_quantity)
    current = _safe_float(current_quantity)
    previous_utc = _as_utc(previous_at)
    current_utc = _as_utc(current_at)
    if (
        previous is None
        or current is None
        or previous <= 0
        or current <= 0
        or previous_utc is None
        or current_utc is None
    ):
        return None
    observed_factor = current / previous
    candidates = [
        effect
        for effect in effects or ()
        if effect.ticker.upper() == str(ticker or "").upper()
        and effect.lifecycle_status
        in {CorporateEventStatus.CONFIRMED.value, CorporateEventStatus.EFFECTIVE.value}
        and previous_utc < effect.effective_at <= current_utc
        and _relative_error(observed_factor, effect.quantity_factor)
        <= RATIO_RELATIVE_TOLERANCE
    ]
    if not candidates:
        return None
    return sorted(candidates, key=lambda item: (item.effective_at, item.effect_id))[-1]


def _candidate_quantity_factor(observed_price_factor: float) -> tuple[float, int, int, float] | None:
    if observed_price_factor <= 0:
        return None
    observed_quantity_factor = 1.0 / observed_price_factor
    if observed_quantity_factor < 0.05 or observed_quantity_factor > 20.0:
        return None
    candidates: dict[tuple[int, int], tuple[Fraction, float]] = {}
    for denominator in range(1, 21):
        numerator = max(1, round(observed_quantity_factor * denominator))
        fraction = Fraction(numerator, denominator)
        candidate = float(fraction)
        if candidate <= 0 or fraction.numerator == fraction.denominator:
            continue
        error = _relative_error(observed_quantity_factor, candidate)
        if error <= RATIO_RELATIVE_TOLERANCE:
            candidates[(fraction.numerator, fraction.denominator)] = (fraction, error)
    if not candidates:
        return None
    fraction, error = min(
        candidates.values(),
        key=lambda item: (
            item[0].numerator + item[0].denominator,
            max(item[0].numerator, item[0].denominator),
            item[1],
        ),
    )
    candidate = float(fraction)
    return candidate, fraction.numerator, fraction.denominator, error


def detect_price_anomaly(
    *,
    ticker: str,
    reference_price: float,
    current_price: float,
    observed_at: datetime,
    previous_quantity: float | None = None,
    current_quantity: float | None = None,
    suspected_sessions: int = DEFAULT_SUSPECTED_SESSIONS,
) -> PriceQualityFlag | None:
    reference = _safe_float(reference_price)
    current = _safe_float(current_price)
    if reference is None or current is None or reference <= 0 or current <= 0:
        return None

    observed_price_factor = current / reference
    observed_return = observed_price_factor - 1.0
    if abs(observed_return) < MIN_ANOMALY_RETURN:
        return None

    candidate = _candidate_quantity_factor(observed_price_factor)
    if candidate is None:
        return None
    quantity_factor, ratio_numerator, ratio_denominator, ratio_error = candidate
    if ratio_error > RATIO_RELATIVE_TOLERANCE:
        return None

    observed_quantity_factor = None
    quantity_error = None
    previous_qty = _safe_float(previous_quantity)
    current_qty = _safe_float(current_quantity)
    if previous_qty is not None and current_qty is not None and previous_qty > 0:
        observed_quantity_factor = current_qty / previous_qty
        quantity_error = _relative_error(observed_quantity_factor, quantity_factor)

    ratio_score = max(0.0, 1.0 - ratio_error / RATIO_RELATIVE_TOLERANCE)
    quantity_matches = quantity_error is not None and quantity_error <= RATIO_RELATIVE_TOLERANCE
    detector_score = min(0.99, 0.45 + (0.30 * ratio_score) + (0.24 if quantity_matches else 0.0))
    evidence_level = (
        EvidenceLevel.CORROBORATED.value
        if quantity_matches
        else EvidenceLevel.HEURISTIC_ONLY.value
    )
    expires_at = add_market_sessions(observed_at, suspected_sessions)
    event_label = "split" if quantity_factor > 1 else "reverse split"
    reason = (
        f"SUSPECTED_CORPORATE_ACTION: price factor {observed_price_factor:.6f} "
        f"matches {ratio_numerator}:{ratio_denominator} {event_label}"
    )
    if quantity_matches:
        reason += f" and quantity factor {observed_quantity_factor:.6f} corroborates it"
    else:
        reason += "; quantity/source confirmation is still missing"

    return PriceQualityFlag(
        ticker=str(ticker or "").upper(),
        observed_at=observed_at,
        expires_at=expires_at,
        flag_type=PriceQualityStatus.PRICE_NOT_COMPARABLE.value,
        resolution_status="OPEN",
        observed_reference_price=reference,
        observed_current_price=current,
        observed_return=observed_return,
        expected_price_factor=1.0 / quantity_factor,
        observed_quantity_factor=observed_quantity_factor,
        quantity_factor=quantity_factor,
        evidence_level=evidence_level,
        detector_score=round(detector_score, 6),
        action_taken="TEMPORARY_QUARANTINE",
        reason=reason,
        evidence={
            "ratio_numerator": ratio_numerator,
            "ratio_denominator": ratio_denominator,
            "ratio_relative_error": ratio_error,
            "quantity_relative_error": quantity_error,
            "confirmation_required": True,
        },
        idempotency_key=_stable_key(
            DETECTOR_VERSION,
            str(ticker or "").upper(),
            observed_at.astimezone(ART_TZ).date().isoformat(),
            f"{reference:.8f}",
            f"{current:.8f}",
        ),
    )


def _matching_effect_for_observation(
    effects: Sequence[CorporateActionEffect],
    observed_at: datetime,
) -> CorporateActionEffect | None:
    observed_day = observed_at.astimezone(ART_TZ).date()
    candidates = [
        effect
        for effect in effects or []
        if effect.lifecycle_status
        in {CorporateEventStatus.CONFIRMED.value, CorporateEventStatus.EFFECTIVE.value}
        and abs((observed_day - effect.effective_at.astimezone(ART_TZ).date()).days) <= 3
    ]
    return candidates[-1] if candidates else None


def assess_live_price(
    *,
    ticker: str,
    reference_price: float | None,
    current_price: float | None,
    observed_at: datetime,
    effects: Sequence[CorporateActionEffect] = (),
    previous_quantity: float | None = None,
    current_quantity: float | None = None,
) -> LivePriceAssessment:
    reference = _safe_float(reference_price)
    current = _safe_float(current_price)
    if reference is None or current is None or reference <= 0 or current <= 0:
        return LivePriceAssessment(
            ticker=str(ticker or "").upper(),
            status=PriceQualityStatus.DATA_QUALITY_BLOCK.value,
            raw_change=None,
            normalized_change=None,
            normalized_reference_price=None,
            flag=None,
            effect=None,
        )

    raw_factor = current / reference
    raw_change = raw_factor - 1.0
    effect = _matching_effect_for_observation(effects, observed_at)
    if effect is not None:
        expected = effect.price_factor
        normalized_reference = reference * expected
        normalized_change = (current / normalized_reference) - 1.0
        ratio_error = _relative_error(raw_factor, expected)
        if ratio_error <= OFFICIAL_RATIO_RELATIVE_TOLERANCE:
            flag = PriceQualityFlag(
                ticker=str(ticker or "").upper(),
                observed_at=observed_at,
                expires_at=None,
                flag_type=PriceQualityStatus.PRICE_NOT_COMPARABLE.value,
                resolution_status="CONFIRMED",
                observed_reference_price=reference,
                observed_current_price=current,
                observed_return=raw_change,
                expected_price_factor=expected,
                observed_quantity_factor=None,
                quantity_factor=effect.quantity_factor,
                evidence_level=effect.evidence_level,
                detector_score=effect.detector_score,
                action_taken="NORMALIZED_INTRADAY_PRICE",
                reason=(
                    f"CONFIRMED_CORPORATE_ACTION: raw return {raw_change:+.2%} is not "
                    f"comparable; normalized return {normalized_change:+.2%}"
                ),
                evidence={
                    "source_name": effect.source_name,
                    "source_url": effect.source_url,
                    "event_key": effect.event_key,
                    "ratio_relative_error": ratio_error,
                },
                event_id=effect.event_id,
                instrument_effect_id=effect.effect_id,
                idempotency_key=_stable_key(
                    DETECTOR_VERSION,
                    "official",
                    effect.effect_id,
                    observed_at.astimezone(ART_TZ).date().isoformat(),
                ),
            )
            return LivePriceAssessment(
                ticker=str(ticker or "").upper(),
                status=PriceQualityStatus.RECONCILED.value,
                raw_change=raw_change,
                normalized_change=normalized_change,
                normalized_reference_price=normalized_reference,
                flag=flag,
                effect=effect,
            )

        if abs(raw_change) < MIN_ANOMALY_RETURN:
            return LivePriceAssessment(
                ticker=str(ticker or "").upper(),
                status=PriceQualityStatus.COMPARABLE.value,
                raw_change=raw_change,
                normalized_change=raw_change,
                normalized_reference_price=reference,
                flag=None,
                effect=effect,
            )

        mismatch_flag = PriceQualityFlag(
            ticker=str(ticker or "").upper(),
            observed_at=observed_at,
            expires_at=add_market_sessions(observed_at, DEFAULT_SUSPECTED_SESSIONS),
            flag_type=PriceQualityStatus.DATA_QUALITY_BLOCK.value,
            resolution_status="OPEN",
            observed_reference_price=reference,
            observed_current_price=current,
            observed_return=raw_change,
            expected_price_factor=expected,
            observed_quantity_factor=None,
            quantity_factor=effect.quantity_factor,
            evidence_level=effect.evidence_level,
            detector_score=effect.detector_score,
            action_taken="DATA_QUALITY_QUARANTINE",
            reason=(
                f"DATA_QUALITY_BLOCK: confirmed event expects price factor {expected:.6f}, "
                f"observed {raw_factor:.6f}"
            ),
            evidence={"ratio_relative_error": ratio_error, "event_key": effect.event_key},
            event_id=effect.event_id,
            instrument_effect_id=effect.effect_id,
            idempotency_key=_stable_key(
                DETECTOR_VERSION,
                "mismatch",
                effect.effect_id,
                observed_at.astimezone(ART_TZ).date().isoformat(),
            ),
        )
        return LivePriceAssessment(
            ticker=str(ticker or "").upper(),
            status=PriceQualityStatus.DATA_QUALITY_BLOCK.value,
            raw_change=raw_change,
            normalized_change=None,
            normalized_reference_price=None,
            flag=mismatch_flag,
            effect=effect,
        )

    flag = detect_price_anomaly(
        ticker=ticker,
        reference_price=reference,
        current_price=current,
        observed_at=observed_at,
        previous_quantity=previous_quantity,
        current_quantity=current_quantity,
    )
    if flag is not None:
        return LivePriceAssessment(
            ticker=str(ticker or "").upper(),
            status=PriceQualityStatus.PRICE_NOT_COMPARABLE.value,
            raw_change=raw_change,
            normalized_change=None,
            normalized_reference_price=None,
            flag=flag,
            effect=None,
        )
    return LivePriceAssessment(
        ticker=str(ticker or "").upper(),
        status=PriceQualityStatus.COMPARABLE.value,
        raw_change=raw_change,
        normalized_change=raw_change,
        normalized_reference_price=reference,
        flag=None,
        effect=None,
    )


def _frame_date(value: Any) -> date:
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    parsed = _as_utc(value)
    if parsed is None:
        raise ValueError(f"invalid frame date: {value!r}")
    # Daily market_candles use the UTC date as their canonical session label.
    return parsed.date()


def normalize_frame_for_effects(
    ticker: str,
    frame: Any,
    effects: Sequence[CorporateActionEffect],
    *,
    observed_at: datetime | None = None,
) -> tuple[Any, list[CorporateActionApplication], str | None]:
    if frame is None or len(frame) < 2 or not effects:
        return frame, [], None

    normalized = frame.copy(deep=True)
    normalized.attrs = dict(getattr(frame, "attrs", {}))
    applied_effect_ids = set(normalized.attrs.get("corporate_action_effect_ids", ()))
    applications: list[CorporateActionApplication] = []
    blocking_reason = None
    observed_utc = _as_utc(observed_at)

    for effect in sorted(effects, key=lambda item: (item.effective_at, item.effect_id)):
        if effect.lifecycle_status not in {
            CorporateEventStatus.CONFIRMED.value,
            CorporateEventStatus.EFFECTIVE.value,
        }:
            continue
        if effect.effect_id in applied_effect_ids:
            continue
        effective_day = effect.effective_at.astimezone(ART_TZ).date()
        frame_days = [_frame_date(value) for value in normalized.index]
        last_frame_day = max(frame_days)
        if (
            abs(effect.quantity_factor - 1.0) <= 1e-12
            and abs(effect.price_factor - 1.0) <= 1e-12
            and abs(effect.cost_basis_factor - 1.0) <= 1e-12
        ):
            if (
                effective_day > last_frame_day
                and (observed_utc is None or effect.effective_at > observed_utc)
            ):
                continue
            applications.append(
                CorporateActionApplication(
                    event_id=effect.event_id,
                    instrument_effect_id=effect.effect_id,
                    component="MARKET_CANDLES_FRAME",
                    application_status=CorporateApplicationStatus.ALREADY_ADJUSTED.value,
                    idempotency_key=_stable_key(
                        DETECTOR_VERSION,
                        effect.effect_id,
                        "MARKET_CANDLES_FRAME",
                        effect.effective_at.astimezone(ART_TZ).date().isoformat(),
                    ),
                    before_state={"identity_transform": True},
                    after_state={"rows_adjusted": 0},
                    invariant_checks={
                        "identity_transform": True,
                        "raw_records_mutated": False,
                    },
                )
            )
            applied_effect_ids.add(effect.effect_id)
            continue

        transition_candidates: list[tuple[int, int, float]] = []
        for idx in range(1, len(normalized)):
            if abs((frame_days[idx] - effective_day).days) > 3:
                continue
            previous_close = float(normalized.iloc[idx - 1]["Close"])
            current_close = float(normalized.iloc[idx]["Close"])
            observed = current_close / previous_close if previous_close > 0 else 0.0
            error = _relative_error(observed, effect.price_factor)
            if error <= OFFICIAL_RATIO_RELATIVE_TOLERANCE:
                transition_candidates.append(
                    (abs((frame_days[idx] - effective_day).days), idx, error)
                )

        transition_position = None
        if transition_candidates and (
            observed_utc is None or effect.effective_at <= observed_utc
        ):
            transition_position = min(transition_candidates)[1]

        if transition_position is None and effective_day > last_frame_day:
            if observed_utc is not None and effect.effective_at <= observed_utc:
                blocking_reason = (
                    f"PRICE_NOT_COMPARABLE: {ticker.upper()} event {effect.event_key} "
                    "is effective but no post-event candle is available"
                )
                applications.append(
                    CorporateActionApplication(
                        event_id=effect.event_id,
                        instrument_effect_id=effect.effect_id,
                        component="MARKET_CANDLES_FRAME",
                        application_status=CorporateApplicationStatus.PENDING.value,
                        idempotency_key=_stable_key(
                            DETECTOR_VERSION,
                            effect.effect_id,
                            "MARKET_CANDLES_FRAME_PENDING",
                            effective_day.isoformat(),
                        ),
                        before_state={
                            "ticker": ticker.upper(),
                            "last_frame_day": last_frame_day.isoformat(),
                            "effective_day": effective_day.isoformat(),
                        },
                        after_state={"rows_adjusted": 0},
                        invariant_checks={
                            "post_event_candle_available": False,
                            "raw_records_mutated": False,
                        },
                        error=blocking_reason,
                    )
                )
                break
            continue

        post_positions = [idx for idx, day in enumerate(frame_days) if day >= effective_day]
        if not post_positions or post_positions[0] == 0:
            if transition_position is None:
                continue

        post_position = transition_position or post_positions[0]
        pre_position = post_position - 1
        transition_day = frame_days[post_position]
        pre_close = float(normalized.iloc[pre_position]["Close"])
        post_close = float(normalized.iloc[post_position]["Close"])
        observed_factor = post_close / pre_close if pre_close > 0 else 0.0
        ratio_error = _relative_error(observed_factor, effect.price_factor)
        before_state = {
            "ticker": ticker.upper(),
            "effective_day": effective_day.isoformat(),
            "transition_day": transition_day.isoformat(),
            "pre_close": pre_close,
            "post_close": post_close,
            "observed_price_factor": observed_factor,
            "expected_price_factor": effect.price_factor,
        }

        if ratio_error <= OFFICIAL_RATIO_RELATIVE_TOLERANCE:
            pre_mask = [day < transition_day for day in frame_days]
            for column in ("Open", "High", "Low", "Close"):
                normalized.loc[pre_mask, column] = (
                    normalized.loc[pre_mask, column].astype(float) * effect.price_factor
                )
            if "Volume" in normalized.columns:
                normalized.loc[pre_mask, "Volume"] = (
                    normalized.loc[pre_mask, "Volume"].astype(float) * effect.quantity_factor
                )
            status = CorporateApplicationStatus.APPLIED.value
            after_pre_close = float(normalized.iloc[pre_position]["Close"])
            normalized_gap = (post_close / after_pre_close) - 1.0 if after_pre_close > 0 else None
            invariant_checks = {
                "normalized_gap": normalized_gap,
                "price_factor_match": True,
                "raw_records_mutated": False,
            }
            after_state = {
                "normalized_pre_close": after_pre_close,
                "post_close": post_close,
                "rows_adjusted": int(sum(pre_mask)),
            }
        elif abs(observed_factor - 1.0) < MIN_ANOMALY_RETURN:
            status = CorporateApplicationStatus.ALREADY_ADJUSTED.value
            invariant_checks = {
                "normalized_gap": observed_factor - 1.0,
                "price_factor_match": False,
                "already_adjusted": True,
                "raw_records_mutated": False,
            }
            after_state = {"rows_adjusted": 0, "observed_price_factor": observed_factor}
        else:
            status = CorporateApplicationStatus.FAILED.value
            invariant_checks = {
                "price_factor_match": False,
                "ratio_relative_error": ratio_error,
                "raw_records_mutated": False,
            }
            after_state = {"rows_adjusted": 0}
            blocking_reason = (
                f"DATA_QUALITY_BLOCK: {ticker.upper()} event {effect.event_key} expects "
                f"factor {effect.price_factor:.6f}, observed {observed_factor:.6f}"
            )

        applications.append(
            CorporateActionApplication(
                event_id=effect.event_id,
                instrument_effect_id=effect.effect_id,
                component="MARKET_CANDLES_FRAME",
                application_status=status,
                idempotency_key=_stable_key(
                    DETECTOR_VERSION,
                    effect.effect_id,
                    "MARKET_CANDLES_FRAME",
                    effective_day.isoformat(),
                ),
                before_state=before_state,
                after_state=after_state,
                invariant_checks=invariant_checks,
                error=blocking_reason if status == CorporateApplicationStatus.FAILED.value else None,
            )
        )
        if status in {
            CorporateApplicationStatus.APPLIED.value,
            CorporateApplicationStatus.ALREADY_ADJUSTED.value,
        }:
            applied_effect_ids.add(effect.effect_id)

    normalized.attrs["corporate_action_effect_ids"] = tuple(sorted(applied_effect_ids))
    normalized.attrs["price_basis"] = (
        "corporate_action_adjusted" if applied_effect_ids else normalized.attrs.get("price_basis", "raw")
    )
    return normalized, applications, blocking_reason


def _latest_quantity_pair(
    history: Sequence[Mapping[str, Any]],
    ticker: str,
) -> tuple[float | None, float | None]:
    quantities: list[tuple[datetime | None, float]] = []
    for snapshot in history or []:
        snapshot_at = _as_utc(snapshot.get("scraped_at"))
        for position in snapshot.get("positions") or []:
            if str(position.get("ticker") or "").upper() != ticker.upper():
                continue
            quantity = _safe_float(position.get("quantity"))
            if quantity is not None and quantity > 0:
                quantities.append((snapshot_at, quantity))
    if not quantities:
        return None, None
    quantities.sort(key=lambda item: item[0] or datetime.min.replace(tzinfo=timezone.utc))
    current = quantities[-1][1]
    current_day = quantities[-1][0].astimezone(ART_TZ).date() if quantities[-1][0] else None
    previous = None
    for _, quantity in reversed(quantities[:-1]):
        if _relative_error(quantity, current) > 0.000001:
            return quantity, current
    for snapshot_at, quantity in reversed(quantities[:-1]):
        snapshot_day = snapshot_at.astimezone(ART_TZ).date() if snapshot_at else None
        if current_day is None or snapshot_day != current_day:
            previous = quantity
            break
    return previous, current


def guard_history_frames(
    frames: Mapping[str, Any],
    *,
    effects: Sequence[CorporateActionEffect] = (),
    portfolio_history: Sequence[Mapping[str, Any]] = (),
    observed_at: datetime | None = None,
) -> FrameGuardResult:
    observed_at = observed_at or datetime.now(timezone.utc)
    grouped_effects = effects_by_ticker(effects)
    output: dict[str, Any] = {}
    blocked: dict[str, str] = {}
    flags: list[PriceQualityFlag] = []
    applications: list[CorporateActionApplication] = []

    for raw_ticker, frame in frames.items():
        ticker = str(raw_ticker or "").upper()
        normalized, ticker_applications, blocking_reason = normalize_frame_for_effects(
            ticker,
            frame,
            grouped_effects.get(ticker, ()),
            observed_at=observed_at,
        )
        applications.extend(ticker_applications)
        ticker_effect_by_id = {
            effect.effect_id: effect for effect in grouped_effects.get(ticker, ())
        }
        for application in ticker_applications:
            if application.application_status not in {
                CorporateApplicationStatus.APPLIED.value,
                CorporateApplicationStatus.ALREADY_ADJUSTED.value,
            }:
                continue
            if application.invariant_checks.get("identity_transform"):
                continue
            effect = ticker_effect_by_id.get(application.instrument_effect_id)
            if effect is None:
                continue
            raw_reference = _safe_float(application.before_state.get("pre_close"))
            raw_current = _safe_float(application.before_state.get("post_close"))
            raw_return = (
                (raw_current / raw_reference) - 1.0
                if raw_reference and raw_current
                else None
            )
            flags.append(
                PriceQualityFlag(
                    event_id=effect.event_id,
                    instrument_effect_id=effect.effect_id,
                    ticker=ticker,
                    observed_at=effect.effective_at,
                    expires_at=None,
                    flag_type=PriceQualityStatus.PRICE_NOT_COMPARABLE.value,
                    resolution_status="CONFIRMED",
                    observed_reference_price=raw_reference,
                    observed_current_price=raw_current,
                    observed_return=raw_return,
                    expected_price_factor=effect.price_factor,
                    observed_quantity_factor=None,
                    quantity_factor=effect.quantity_factor,
                    evidence_level=effect.evidence_level,
                    detector_score=effect.detector_score,
                    action_taken=(
                        "NORMALIZED_MARKET_CANDLES"
                        if application.application_status
                        == CorporateApplicationStatus.APPLIED.value
                        else "CONFIRMED_ALREADY_ADJUSTED"
                    ),
                    reason=(
                        f"CONFIRMED_CORPORATE_ACTION: {effect.event_key}; raw price "
                        "discontinuity normalized before scoring"
                    ),
                    evidence={
                        "source_name": effect.source_name,
                        "source_url": effect.source_url,
                        "application_idempotency_key": application.idempotency_key,
                    },
                    idempotency_key=_stable_key(
                        DETECTOR_VERSION,
                        "confirmed-frame",
                        effect.effect_id,
                        effect.effective_at.astimezone(ART_TZ).date().isoformat(),
                    ),
                )
            )
        if blocking_reason:
            blocked[ticker] = blocking_reason
            for application in ticker_applications:
                if application.application_status != CorporateApplicationStatus.PENDING.value:
                    continue
                effect = ticker_effect_by_id.get(application.instrument_effect_id)
                if effect is None:
                    continue
                flags.append(
                    PriceQualityFlag(
                        event_id=effect.event_id,
                        instrument_effect_id=effect.effect_id,
                        ticker=ticker,
                        observed_at=observed_at,
                        expires_at=None,
                        flag_type=PriceQualityStatus.PRICE_NOT_COMPARABLE.value,
                        resolution_status="OPEN",
                        observed_reference_price=None,
                        observed_current_price=None,
                        observed_return=None,
                        expected_price_factor=effect.price_factor,
                        observed_quantity_factor=None,
                        quantity_factor=effect.quantity_factor,
                        evidence_level=effect.evidence_level,
                        detector_score=effect.detector_score,
                        action_taken="WAIT_POST_EVENT_PRICE",
                        reason=blocking_reason,
                        evidence={
                            "source_name": effect.source_name,
                            "source_url": effect.source_url,
                            "event_key": effect.event_key,
                            "last_frame_day": application.before_state.get("last_frame_day"),
                        },
                        idempotency_key=_stable_key(
                            DETECTOR_VERSION,
                            "pending-post-event-price",
                            effect.effect_id,
                            effect.effective_at.astimezone(ART_TZ).date().isoformat(),
                        ),
                    )
                )
            continue

        if normalized is None or len(normalized) < 2:
            output[ticker] = normalized
            continue

        previous_close = float(normalized["Close"].iloc[-2])
        current_close = float(normalized["Close"].iloc[-1])
        previous_quantity, current_quantity = _latest_quantity_pair(portfolio_history, ticker)
        flag = detect_price_anomaly(
            ticker=ticker,
            reference_price=previous_close,
            current_price=current_close,
            observed_at=observed_at,
            previous_quantity=previous_quantity,
            current_quantity=current_quantity,
        )
        if flag is not None:
            flags.append(flag)
            blocked[ticker] = flag.reason
            continue
        output[ticker] = normalized

    return FrameGuardResult(
        frames=output,
        blocked_by_ticker=blocked,
        flags=tuple(flags),
        applications=tuple(applications),
    )


def rebase_reference_price(
    price: float | None,
    *,
    reference_at: datetime,
    as_of: datetime,
    effects: Sequence[CorporateActionEffect],
) -> tuple[float | None, float]:
    value = _safe_float(price)
    if value is None or value <= 0:
        return value, 1.0
    reference_utc = _as_utc(reference_at)
    as_of_utc = _as_utc(as_of)
    if reference_utc is None or as_of_utc is None:
        return value, 1.0
    factor = 1.0
    for effect in sorted(effects or (), key=lambda item: (item.effective_at, item.effect_id)):
        if effect.lifecycle_status not in {
            CorporateEventStatus.CONFIRMED.value,
            CorporateEventStatus.EFFECTIVE.value,
        }:
            continue
        if reference_utc < effect.effective_at <= as_of_utc:
            factor *= effect.price_factor
    return value * factor, factor


def normalize_candle_rows(
    rows: Sequence[Mapping[str, Any]],
    effects: Sequence[CorporateActionEffect],
) -> list[dict[str, Any]]:
    normalized = [dict(row) for row in rows or []]
    if not normalized or not effects:
        return normalized
    for effect in sorted(effects, key=lambda item: (item.effective_at, item.effect_id)):
        if effect.lifecycle_status not in {
            CorporateEventStatus.CONFIRMED.value,
            CorporateEventStatus.EFFECTIVE.value,
        }:
            continue
        if (
            abs(effect.quantity_factor - 1.0) <= 1e-12
            and abs(effect.price_factor - 1.0) <= 1e-12
            and abs(effect.cost_basis_factor - 1.0) <= 1e-12
        ):
            continue
        effective_day = effect.effective_at.astimezone(ART_TZ).date()
        row_days = [
            (_as_utc(row.get("ts")) or datetime.min.replace(tzinfo=timezone.utc)).date()
            for row in normalized
        ]
        transition_candidates: list[tuple[int, int]] = []
        for index in range(1, len(normalized)):
            if abs((row_days[index] - effective_day).days) > 3:
                continue
            pre_close = _safe_float(normalized[index - 1].get("close_price"))
            post_close = _safe_float(normalized[index].get("close_price"))
            if pre_close is None or post_close is None or pre_close <= 0:
                continue
            if (
                _relative_error(post_close / pre_close, effect.price_factor)
                <= OFFICIAL_RATIO_RELATIVE_TOLERANCE
            ):
                transition_candidates.append(
                    (abs((row_days[index] - effective_day).days), index)
                )
        if transition_candidates:
            post_position = min(transition_candidates)[1]
        else:
            post_positions = [
                index for index, row_day in enumerate(row_days) if row_day >= effective_day
            ]
            if not post_positions or post_positions[0] == 0:
                continue
            post_position = post_positions[0]
        pre_close = _safe_float(normalized[post_position - 1].get("close_price"))
        post_close = _safe_float(normalized[post_position].get("close_price"))
        if pre_close is None or post_close is None or pre_close <= 0:
            continue
        observed_factor = post_close / pre_close
        if _relative_error(observed_factor, effect.price_factor) > OFFICIAL_RATIO_RELATIVE_TOLERANCE:
            continue
        transition_day = row_days[post_position]
        for row in normalized:
            row_ts = _as_utc(row.get("ts"))
            if row_ts is None or row_ts.date() >= transition_day:
                continue
            for key in ("open_price", "high_price", "low_price", "close_price"):
                value = _safe_float(row.get(key))
                if value is not None:
                    row[key] = value * effect.price_factor
            volume = _safe_float(row.get("volume"))
            if volume is not None:
                row["volume"] = volume * effect.quantity_factor
    return normalized


def rebase_position_view(
    position: Mapping[str, Any],
    *,
    snapshot_at: datetime,
    as_of: datetime,
    effects: Sequence[CorporateActionEffect],
) -> tuple[dict[str, Any], list[CorporateActionApplication]]:
    rebased = dict(position)
    applicable = [
        effect
        for effect in sorted(effects or (), key=lambda item: (item.effective_at, item.effect_id))
        if effect.lifecycle_status
        in {CorporateEventStatus.CONFIRMED.value, CorporateEventStatus.EFFECTIVE.value}
        if _as_utc(snapshot_at) < effect.effective_at <= _as_utc(as_of)
    ]
    if not applicable:
        return rebased, []

    applications: list[CorporateActionApplication] = []
    price_is_current_basis = bool(rebased.get("price_normalized"))
    for effect in applicable:
        before_state = {
            "quantity": _safe_float(rebased.get("quantity")),
            "avg_cost": _safe_float(rebased.get("avg_cost")),
            "current_price": _safe_float(rebased.get("current_price")),
            "market_value": _safe_float(rebased.get("market_value")),
        }
        if before_state["quantity"] is not None:
            rebased["quantity"] = before_state["quantity"] * effect.quantity_factor
        if before_state["avg_cost"] is not None:
            rebased["avg_cost"] = before_state["avg_cost"] * effect.cost_basis_factor
        if before_state["current_price"] is not None and not price_is_current_basis:
            rebased["current_price"] = before_state["current_price"] * effect.price_factor
        if rebased.get("quantity") is not None and rebased.get("current_price") is not None:
            rebased["market_value"] = (
                float(rebased["quantity"]) * float(rebased["current_price"])
            )

        after_state = {
            "quantity": _safe_float(rebased.get("quantity")),
            "avg_cost": _safe_float(rebased.get("avg_cost")),
            "current_price": _safe_float(rebased.get("current_price")),
            "market_value": _safe_float(rebased.get("market_value")),
        }
        before_quantity = _safe_float(before_state.get("quantity"))
        before_cost = _safe_float(before_state.get("avg_cost"))
        before_basis = (
            before_quantity * before_cost
            if before_quantity is not None and before_cost is not None
            else None
        )
        after_quantity = _safe_float(after_state.get("quantity"))
        after_cost = _safe_float(after_state.get("avg_cost"))
        after_basis = (
            after_quantity * after_cost
            if after_quantity is not None and after_cost is not None
            else None
        )
        invariant_checks = {
            "raw_snapshot_mutated": False,
            "price_was_current_basis": price_is_current_basis,
            "total_cost_basis_before": before_basis,
            "total_cost_basis_after": after_basis,
            "cost_basis_invariant": (
                before_basis is None
                or after_basis is None
                or _relative_error(after_basis, before_basis) <= 0.001
            ),
        }
        applications.append(
            CorporateActionApplication(
                event_id=effect.event_id,
                instrument_effect_id=effect.effect_id,
                component="PORTFOLIO_POSITION_VIEW",
                application_status=CorporateApplicationStatus.APPLIED.value,
                idempotency_key=_stable_key(
                    DETECTOR_VERSION,
                    effect.effect_id,
                    "PORTFOLIO_POSITION_VIEW",
                    str(position.get("ticker") or "").upper(),
                    _as_utc(snapshot_at).isoformat(),
                ),
                before_state=before_state,
                after_state=after_state,
                invariant_checks=invariant_checks,
            )
        )

    quantity_factor = math.prod(effect.quantity_factor for effect in applicable)
    price_factor = math.prod(effect.price_factor for effect in applicable)
    rebased["corporate_action_rebased"] = True
    rebased["corporate_action_quantity_factor"] = quantity_factor
    rebased["corporate_action_price_factor"] = price_factor

    quantity = _safe_float(rebased.get("quantity"))
    avg_cost = _safe_float(rebased.get("avg_cost"))
    current_price = _safe_float(rebased.get("current_price"))
    if quantity is not None and avg_cost is not None:
        cost_basis = quantity * avg_cost
        rebased["unrealized_pnl"] = float(rebased.get("market_value") or 0.0) - cost_basis
        if current_price is not None and avg_cost > 0:
            rebased["unrealized_pnl_pct"] = (current_price / avg_cost) - 1.0

    return rebased, applications


def corporate_action_layers(
    ticker: str,
    *,
    effects: Sequence[CorporateActionEffect] = (),
    flag: PriceQualityFlag | None = None,
) -> dict[str, Any]:
    ticker_effects = [effect for effect in effects or [] if effect.ticker == ticker.upper()]
    payload: dict[str, Any] = {
        "active": bool(ticker_effects or flag),
        "price_comparable": flag is None or not flag.blocks_price_use,
        "effects": [
            {
                "event_id": effect.event_id,
                "effect_id": effect.effect_id,
                "event_key": effect.event_key,
                "event_type": effect.event_type,
                "lifecycle_status": effect.lifecycle_status,
                "effective_at": effect.effective_at.isoformat(),
                "quantity_factor": effect.quantity_factor,
                "price_factor": effect.price_factor,
                "source_name": effect.source_name,
                "source_url": effect.source_url,
                "evidence_level": effect.evidence_level,
            }
            for effect in ticker_effects
        ],
    }
    if flag is not None:
        payload["price_quality_flag"] = flag.to_dict()
    return payload

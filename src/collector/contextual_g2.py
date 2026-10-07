"""Persistence contracts for the G2 real point-in-time capture.

The operational ``market_candles`` table remains the productive source.  G2
observations are append-only evidence and contextual snapshots are SHADOW_ONLY.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence
from uuid import UUID, NAMESPACE_URL, uuid5

from src.collector.data.models import AssetType, Currency, MarketCandle


MIGRATION_PATH = (
    Path(__file__).resolve().parents[2]
    / "migrations"
    / "20261007_contextual_g2.sql"
)
G3_MIGRATION_PATH = (
    Path(__file__).resolve().parents[2]
    / "migrations"
    / "20261008_contextual_g3.sql"
)


def migration_sql() -> str:
    return MIGRATION_PATH.read_text(encoding="utf-8")


async def ensure_contextual_g2_schema(conn: Any) -> None:
    await conn.execute(migration_sql())


def g3_migration_sql() -> str:
    return G3_MIGRATION_PATH.read_text(encoding="utf-8")


async def ensure_contextual_g3_schema(conn: Any) -> None:
    await ensure_contextual_g2_schema(conn)
    await conn.execute(g3_migration_sql())


def instrument_id(*, market: str, asset_type: str, ticker: str, currency: str) -> str:
    values = [market, asset_type, ticker, currency]
    if any(not str(value or "").strip() for value in values):
        raise ValueError("instrument identity requires market, asset_type, ticker and currency")
    return ":".join(str(value).upper().strip() for value in values)


def _finite(value: Any, *, positive: bool = False, nonnegative: bool = False) -> bool:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return False
    if not math.isfinite(number):
        return False
    if positive and number <= 0:
        return False
    if nonnegative and number < 0:
        return False
    return True


@dataclass(frozen=True)
class CandleObservation:
    observation_id: str
    owner_chat_id: int
    ingestion_run_id: str
    instrument_id: str
    ticker: str
    provider_symbol: str
    asset_type: str
    market: str
    currency: str
    interval: str
    candle_timestamp: datetime
    bar_start: datetime
    bar_end: datetime | None
    available_at: datetime
    scraped_at: datetime
    is_closed: bool | None
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float | None
    price_unit: str | None
    volume_unit: str | None
    source: str
    provenance: dict[str, Any]
    quality: dict[str, Any]
    missingness: list[str]
    code_version: str
    observation_digest: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        for key in (
            "candle_timestamp", "bar_start", "bar_end", "available_at", "scraped_at"
        ):
            value = payload[key]
            payload[key] = value.isoformat() if value is not None else None
        return payload


def _json_value(value: Any, default: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default
    return value if value is not None else default


def _utc_iso(value: Any) -> str | None:
    if value is None:
        return None
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(
        str(value).replace("Z", "+00:00")
    )
    if parsed.tzinfo is None:
        raise ValueError("observation digest requires timezone-aware timestamps")
    return parsed.astimezone(timezone.utc).isoformat()


def _decimal_text(value: Any) -> str | None:
    if value is None:
        return None
    number = Decimal(str(value))
    if not number.is_finite():
        raise ValueError("observation digest rejects nonfinite numbers")
    number = number.quantize(Decimal("0.00000001"))
    normalized = format(number.normalize(), "f")
    return "0" if normalized in {"-0", "-0.0"} else normalized


def observation_digest(value: CandleObservation | Mapping[str, Any]) -> str:
    """Hash every persisted market fact except DB identity and insertion time."""
    get = value.get if isinstance(value, Mapping) else lambda name: getattr(value, name)
    payload = {
        "schema": "market-observation-digest-v1",
        "bar_identity": {
            "instrument_id": str(get("instrument_id")),
            "market": str(get("market")),
            "provider_symbol": str(get("provider_symbol")),
            "interval": str(get("interval")),
            "bar_start": _utc_iso(get("bar_start")),
        },
        "ticker": str(get("ticker")),
        "asset_type": str(get("asset_type")),
        "currency": str(get("currency")),
        "candle_timestamp": _utc_iso(get("candle_timestamp")),
        "bar_end": _utc_iso(get("bar_end")),
        "available_at": _utc_iso(get("available_at")),
        "scraped_at": _utc_iso(get("scraped_at")),
        "is_closed": get("is_closed"),
        "ohlcv": {
            "open": _decimal_text(get("open_price")),
            "high": _decimal_text(get("high_price")),
            "low": _decimal_text(get("low_price")),
            "close": _decimal_text(get("close_price")),
            "volume": _decimal_text(get("volume")),
        },
        "price_unit": get("price_unit"),
        "volume_unit": get("volume_unit"),
        "source": str(get("source")),
        "provenance": _json_value(get("provenance"), {}),
        "quality": _json_value(get("quality"), {}),
        "missingness": _json_value(get("missingness"), []),
        "code_version": str(get("code_version")),
    }
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False,
    )
    return sha256(encoded.encode("utf-8")).hexdigest()


def observations_from_provider_sequence(
    candles: Sequence[MarketCandle],
    *,
    owner_chat_id: int,
    ingestion_run_id: str,
    provider_symbol: str,
    scraped_at: datetime,
    code_version: str,
    price_unit: str | None,
    volume_unit: str | None = None,
    source_contract: str = "tradingview_websocket_timescale_v1",
) -> list[CandleObservation]:
    """Version a provider sequence without inventing historical availability.

    ``available_at`` is the actual first observation time of this capture.  A
    bar is marked closed only when the provider sequence contains a later bar;
    the later bar start is retained as conservative close confirmation.  The
    last bar therefore remains open/unknown instead of receiving an inferred
    exchange close.
    """
    if scraped_at.tzinfo is None:
        raise ValueError("scraped_at requires timezone")
    if int(owner_chat_id) <= 0:
        raise ValueError("owner_chat_id must be positive")
    UUID(str(ingestion_run_id))
    if not str(provider_symbol or "").strip():
        raise ValueError("provider_symbol is required")
    if not str(code_version or "").strip():
        raise ValueError("code_version is required")

    ordered = sorted(candles, key=lambda item: item.ts)
    if not ordered:
        return []
    first = ordered[0]
    identity = instrument_id(
        market=first.venue,
        asset_type=first.asset_type.value,
        ticker=first.ticker,
        currency=first.currency.value,
    )
    result: list[CandleObservation] = []
    for index, candle in enumerate(ordered):
        current_identity = instrument_id(
            market=candle.venue,
            asset_type=candle.asset_type.value,
            ticker=candle.ticker,
            currency=candle.currency.value,
        )
        if current_identity != identity or candle.interval != first.interval:
            raise ValueError("AMBIGUOUS_PROVIDER_SEQUENCE_IDENTITY")
        if candle.ts.tzinfo is None:
            raise ValueError("candle timestamp requires timezone")
        values = (candle.open_price, candle.high_price, candle.low_price, candle.close_price)
        if not all(_finite(value, positive=True) for value in values):
            raise ValueError("INVALID_OHLC_NONFINITE_OR_NONPOSITIVE")
        if candle.high_price < max(candle.open_price, candle.low_price, candle.close_price):
            raise ValueError("INVALID_OHLC_HIGH")
        if candle.low_price > min(candle.open_price, candle.high_price, candle.close_price):
            raise ValueError("INVALID_OHLC_LOW")
        if candle.volume is not None and not _finite(candle.volume, nonnegative=True):
            raise ValueError("INVALID_VOLUME")

        next_start = ordered[index + 1].ts if index + 1 < len(ordered) else None
        is_closed = True if next_start is not None else False
        missingness: list[str] = []
        if next_start is None:
            missingness.append("BAR_END_UNCONFIRMED_NO_LATER_PROVIDER_BAR")
        if candle.volume is None:
            missingness.append("VOLUME_MISSING")
        elif candle.volume == 0:
            missingness.append("VOLUME_ZERO")
        if not volume_unit:
            missingness.append("VOLUME_UNIT_UNKNOWN")
        missingness.extend(["EXCHANGE_CALENDAR_VERSION_UNKNOWN", "DEPOSITARY_RATIO_UNKNOWN"])
        quality = {
            "schema": "contextual-g2-candle-quality-v1",
            "price_status": "VALID",
            "volume_status": (
                "UNKNOWN" if candle.volume is None else "PARTIAL" if candle.volume == 0 or not volume_unit else "VALID"
            ),
            "temporal_status": "VALID" if is_closed else "PARTIAL",
            "identity_status": "VALID",
            "missingness_count": len(missingness),
        }
        provenance = {
            "schema": "contextual-g2-provider-provenance-v1",
            "provider_symbol": provider_symbol,
            "source_contract": source_contract,
            "session": "regular",
            "adjustment_policy": "splits",
            "availability_semantics": "first_observed_by_quantia",
            "close_semantics": (
                "confirmed_by_next_provider_bar_start" if is_closed else "not_confirmed"
            ),
        }
        item = CandleObservation(
            observation_id="",
            owner_chat_id=int(owner_chat_id),
            ingestion_run_id=str(ingestion_run_id),
            instrument_id=identity,
            ticker=candle.ticker.upper(),
            provider_symbol=provider_symbol,
            asset_type=candle.asset_type.value,
            market=candle.venue,
            currency=candle.currency.value,
            interval=candle.interval,
            candle_timestamp=candle.ts,
            bar_start=candle.ts,
            bar_end=next_start,
            available_at=scraped_at,
            scraped_at=scraped_at,
            is_closed=is_closed,
            open_price=float(candle.open_price),
            high_price=float(candle.high_price),
            low_price=float(candle.low_price),
            close_price=float(candle.close_price),
            volume=float(candle.volume) if candle.volume is not None else None,
            price_unit=price_unit,
            volume_unit=volume_unit,
            source=candle.source,
            provenance=provenance,
            quality=quality,
            missingness=missingness,
            code_version=code_version,
        )
        digest = observation_digest(item)
        key = "|".join(
            [
                "quantia-market-observation-v2",
                str(owner_chat_id),
                str(ingestion_run_id),
                identity,
                candle.venue,
                provider_symbol,
                candle.interval,
                str(_utc_iso(candle.ts)),
                str(_utc_iso(scraped_at)),
                candle.source,
                digest,
            ]
        )
        result.append(replace(
            item,
            observation_id=str(uuid5(NAMESPACE_URL, key)),
            observation_digest=digest,
        ))
    return result


async def persist_candle_observations(conn: Any, observations: Sequence[CandleObservation]) -> int:
    saved = 0
    async with conn.transaction():
        for item in observations:
            expected_digest = observation_digest(item)
            if item.observation_digest != expected_digest:
                raise ValueError("OBSERVATION_DIGEST_MISMATCH_BEFORE_INSERT")
            inserted_id = await conn.fetchval(
                """
                INSERT INTO market_candle_observations(
                    observation_id, owner_chat_id, ingestion_run_id, instrument_id,
                    ticker, provider_symbol, asset_type, market, currency, interval,
                    candle_timestamp, bar_start, bar_end, available_at, scraped_at,
                    is_closed, open_price, high_price, low_price, close_price, volume,
                    price_unit, volume_unit, source, provenance, quality, missingness,
                    code_version, observation_digest
                ) VALUES(
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,
                    $17,$18,$19,$20,$21,$22,$23,$24,$25::jsonb,$26::jsonb,$27::jsonb,$28,$29
                )
                ON CONFLICT (observation_id) DO NOTHING
                RETURNING observation_id
                """,
                UUID(item.observation_id), item.owner_chat_id, UUID(item.ingestion_run_id),
                item.instrument_id, item.ticker, item.provider_symbol, item.asset_type,
                item.market, item.currency, item.interval, item.candle_timestamp,
                item.bar_start, item.bar_end, item.available_at, item.scraped_at,
                item.is_closed, Decimal(str(item.open_price)), Decimal(str(item.high_price)),
                Decimal(str(item.low_price)), Decimal(str(item.close_price)),
                Decimal(str(item.volume)) if item.volume is not None else None,
                item.price_unit, item.volume_unit, item.source,
                json.dumps(item.provenance, sort_keys=True),
                json.dumps(item.quality, sort_keys=True),
                json.dumps(item.missingness), item.code_version,
                item.observation_digest,
            )
            if inserted_id is not None:
                saved += 1
                continue
            stored_digest = await conn.fetchval(
                "SELECT observation_digest FROM market_candle_observations WHERE observation_id=$1",
                UUID(item.observation_id),
            )
            if stored_digest != item.observation_digest:
                raise RuntimeError("OBSERVATION_ID_COLLISION_OR_DIGEST_MISMATCH")
    return saved


async def read_candle_observations(
    conn: Any,
    *,
    owner_chat_id: int,
    ingestion_run_id: str,
    ticker: str,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    if cutoff.tzinfo is None:
        raise ValueError("cutoff requires timezone")
    rows = await conn.fetch(
        """
        SELECT * FROM market_candle_observations
        WHERE owner_chat_id = $1
          AND ingestion_run_id = $2
          AND ticker = UPPER($3)
          AND candle_timestamp <= $4
          AND available_at <= $4
          AND scraped_at <= $4
        ORDER BY candle_timestamp, observation_id
        """,
        owner_chat_id, UUID(str(ingestion_run_id)), ticker, cutoff,
    )
    return [dict(row) for row in rows]


def bar_identity(value: CandleObservation | Mapping[str, Any]) -> dict[str, str]:
    get = value.get if isinstance(value, Mapping) else lambda name: getattr(value, name)
    return {
        "instrument_id": str(get("instrument_id")),
        "market": str(get("market")),
        "provider_symbol": str(get("provider_symbol")),
        "interval": str(get("interval")),
        "bar_start": str(_utc_iso(get("bar_start"))),
    }


async def record_capture_event(
    conn: Any,
    *,
    capture_id: str,
    owner_chat_id: int,
    status: str,
    occurred_at: datetime,
    code_version: str,
    reason_code: str | None = None,
    details: Mapping[str, Any] | None = None,
) -> str:
    status = str(status).upper().strip()
    if status not in {"STARTED", "COMPLETE", "ABORTED", "FAILED"}:
        raise ValueError("invalid market evidence capture status")
    if occurred_at.tzinfo is None:
        raise ValueError("capture event occurred_at requires timezone")
    if status in {"ABORTED", "FAILED"} and not str(reason_code or "").strip():
        raise ValueError("aborted/failed capture requires reason_code")
    UUID(str(capture_id))
    event_key = "|".join([
        "quantia-market-capture-event-v1", str(owner_chat_id), str(capture_id),
        status, occurred_at.astimezone(timezone.utc).isoformat(),
        str(reason_code or ""),
    ])
    event_id = str(uuid5(NAMESPACE_URL, event_key))
    payload = dict(details or {})
    inserted = await conn.fetchval(
        """
        INSERT INTO market_evidence_capture_events(
            event_id,capture_id,owner_chat_id,status,occurred_at,reason_code,
            details,code_version
        ) VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8)
        ON CONFLICT DO NOTHING
        RETURNING event_id
        """,
        UUID(event_id), UUID(str(capture_id)), int(owner_chat_id), status,
        occurred_at, reason_code, json.dumps(payload, sort_keys=True), code_version,
    )
    if inserted is None:
        row = await conn.fetchrow(
            """SELECT event_id,owner_chat_id,occurred_at,reason_code,details,code_version
               FROM market_evidence_capture_events
               WHERE capture_id=$1 AND status=$2""",
            UUID(str(capture_id)), status,
        )
        if row is None:
            raise RuntimeError("CAPTURE_EVENT_CONFLICT_WITHOUT_EXISTING_STATE")
        same = (
            int(row["owner_chat_id"]) == int(owner_chat_id)
            and row["occurred_at"] == occurred_at
            and row["reason_code"] == reason_code
            and _json_value(row["details"], {}) == payload
            and str(row["code_version"]) == str(code_version)
        )
        if not same:
            raise RuntimeError("CAPTURE_EVENT_IDEMPOTENCY_MISMATCH")
        return str(row["event_id"])
    return str(inserted)


async def read_capture_state(conn: Any, capture_id: str) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        "SELECT * FROM market_evidence_capture_state WHERE capture_id=$1",
        UUID(str(capture_id)),
    )
    return dict(row) if row else None


async def read_market_evidence_as_of(
    conn: Any,
    *,
    owner_chat_id: int,
    instrument_id_value: str,
    market: str,
    provider_symbol: str,
    interval: str,
    source: str,
    cutoff: datetime,
    allow_legacy_without_lifecycle: bool = False,
) -> list[dict[str, Any]]:
    """Return the latest closed observation per bar known at ``cutoff``.

    Selection is fail-closed on lifecycle by default.  Legacy G2 rows may be
    read only when the caller explicitly opts into their missing lifecycle.
    Every stored digest is recomputed before rows are returned.
    """
    if cutoff.tzinfo is None:
        raise ValueError("cutoff requires timezone")
    rows = await conn.fetch(
        """
        WITH eligible AS (
            SELECT o.*, state.status AS lifecycle_status,
                   ROW_NUMBER() OVER (
                       PARTITION BY o.instrument_id,o.market,o.provider_symbol,
                                    o.interval,o.bar_start
                       ORDER BY o.available_at DESC,o.scraped_at DESC,
                                o.created_at DESC,o.observation_id DESC
                   ) AS version_rank
            FROM market_candle_observations o
            LEFT JOIN LATERAL (
                SELECT event.status
                FROM market_evidence_capture_events event
                WHERE event.capture_id=o.ingestion_run_id
                  AND event.occurred_at <= $7
                  AND event.created_at <= $7
                ORDER BY event.occurred_at DESC,event.created_at DESC,event.event_id DESC
                LIMIT 1
            ) state ON TRUE
            WHERE o.owner_chat_id=$1
              AND o.instrument_id=$2
              AND o.market=$3
              AND o.provider_symbol=$4
              AND o.interval=$5
              AND o.source=$6
              AND o.candle_timestamp <= $7
              AND o.bar_start <= $7
              AND o.bar_end IS NOT NULL
              AND o.bar_end <= $7
              AND o.available_at <= $7
              AND o.scraped_at <= $7
              AND o.created_at <= $7
              AND o.is_closed IS TRUE
              AND (
                    state.status='COMPLETE'
                    OR ($8::boolean AND state.status IS NULL)
              )
        )
        SELECT * FROM eligible WHERE version_rank=1
        ORDER BY bar_start,observation_id
        """,
        int(owner_chat_id), instrument_id_value, market, provider_symbol,
        interval, source, cutoff, bool(allow_legacy_without_lifecycle),
    )
    result: list[dict[str, Any]] = []
    for raw in rows:
        item = dict(raw)
        item.pop("version_rank", None)
        item["provenance"] = _json_value(item.get("provenance"), {})
        item["quality"] = _json_value(item.get("quality"), {})
        item["missingness"] = _json_value(item.get("missingness"), [])
        computed = observation_digest(item)
        stored = item.get("observation_digest")
        if stored is not None and str(stored) != computed:
            raise RuntimeError(
                f"OBSERVATION_DIGEST_INTEGRITY_FAILURE:{item['observation_id']}"
            )
        item["computed_observation_digest"] = computed
        item["digest_status"] = "VERIFIED" if stored else "LEGACY_UNSTORED"
        item["bar_identity"] = bar_identity(item)
        result.append(item)
    return result


def observation_rows_to_candles(rows: Sequence[Mapping[str, Any]]) -> list[MarketCandle]:
    def json_object(value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        return {}

    return [
        MarketCandle(
            ticker=str(row["ticker"]),
            long_ticker=str(row["provider_symbol"]),
            asset_type=AssetType(str(row["asset_type"])),
            currency=Currency(str(row["currency"])),
            venue=str(row["market"]),
            interval=str(row["interval"]),
            ts=row["candle_timestamp"],
            open_price=float(row["open_price"]),
            high_price=float(row["high_price"]),
            low_price=float(row["low_price"]),
            close_price=float(row["close_price"]),
            volume=float(row["volume"]) if row.get("volume") is not None else None,
            source=str(row["source"]),
            scraped_at=row["scraped_at"],
            bar_start=row["bar_start"],
            bar_end=row.get("bar_end"),
            available_at=row["available_at"],
            is_closed=row.get("is_closed"),
            volume_unit=row.get("volume_unit"),
            calendar=json_object(row.get("provenance")).get("calendar"),
            calendar_validation=json_object(row.get("provenance")).get("calendar_validation"),
            adjustment_policy=json_object(row.get("provenance")).get("adjustment_policy"),
            depositary_ratio=json_object(row.get("provenance")).get("depositary_ratio"),
        )
        for row in rows
    ]


async def persist_contextual_snapshot(
    conn: Any,
    *,
    snapshot_id: str,
    owner_chat_id: int,
    run_id: str,
    plan_id: str,
    portfolio_snapshot_id: str,
    instrument_id_value: str,
    signal: str,
    conviction: float,
    cutoff: datetime,
    feature_snapshot_v3: Mapping[str, Any],
    contextual_snapshot: Mapping[str, Any],
    input_hashes: Mapping[str, Any],
    productive_baseline: Mapping[str, Any],
    productive_shadow: Mapping[str, Any],
    non_regression: Mapping[str, Any],
    code_version: str,
    candle_inputs: Mapping[str, Sequence[str]],
) -> None:
    if cutoff.tzinfo is None:
        raise ValueError("cutoff requires timezone")
    async with conn.transaction():
        inserted_snapshot = await conn.fetchval(
            """
            INSERT INTO contextual_market_snapshots(
                snapshot_id, owner_chat_id, run_id, plan_id, portfolio_snapshot_id,
                instrument_id, signal, conviction, cutoff, feature_snapshot_v3,
                contextual_snapshot, input_hashes, productive_baseline,
                productive_shadow, non_regression, code_version,
                mode, affects_analysis, affects_execution
            ) VALUES(
                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb,$12::jsonb,
                $13::jsonb,$14::jsonb,$15::jsonb,$16,'SHADOW_ONLY',FALSE,FALSE
            )
            ON CONFLICT (snapshot_id) DO NOTHING
            RETURNING snapshot_id
            """,
            snapshot_id, owner_chat_id, UUID(str(run_id)), UUID(str(plan_id)),
            UUID(str(portfolio_snapshot_id)), instrument_id_value, signal,
            float(conviction), cutoff,
            json.dumps(dict(feature_snapshot_v3), sort_keys=True),
            json.dumps(dict(contextual_snapshot), sort_keys=True),
            json.dumps(dict(input_hashes), sort_keys=True),
            json.dumps(dict(productive_baseline), sort_keys=True),
            json.dumps(dict(productive_shadow), sort_keys=True),
            json.dumps(dict(non_regression), sort_keys=True), code_version,
        )
        if inserted_snapshot is None:
            same_snapshot = await conn.fetchval(
                """
                SELECT EXISTS(
                    SELECT 1 FROM contextual_market_snapshots
                    WHERE snapshot_id=$1
                      AND owner_chat_id=$2
                      AND run_id=$3
                      AND plan_id=$4
                      AND portfolio_snapshot_id=$5
                      AND instrument_id=$6
                      AND signal=$7
                      AND conviction=$8
                      AND cutoff=$9
                      AND feature_snapshot_v3=$10::jsonb
                      AND contextual_snapshot=$11::jsonb
                      AND input_hashes=$12::jsonb
                      AND productive_baseline=$13::jsonb
                      AND productive_shadow=$14::jsonb
                      AND non_regression=$15::jsonb
                      AND code_version=$16
                      AND mode='SHADOW_ONLY'
                      AND affects_analysis=FALSE
                      AND affects_execution=FALSE
                )
                """,
                snapshot_id, owner_chat_id, UUID(str(run_id)), UUID(str(plan_id)),
                UUID(str(portfolio_snapshot_id)), instrument_id_value, signal,
                float(conviction), cutoff,
                json.dumps(dict(feature_snapshot_v3), sort_keys=True),
                json.dumps(dict(contextual_snapshot), sort_keys=True),
                json.dumps(dict(input_hashes), sort_keys=True),
                json.dumps(dict(productive_baseline), sort_keys=True),
                json.dumps(dict(productive_shadow), sort_keys=True),
                json.dumps(dict(non_regression), sort_keys=True), code_version,
            )
            if not same_snapshot:
                raise ValueError("CONTEXTUAL_SNAPSHOT_ID_COLLISION")
        roles = {
            "asset": "ASSET",
            "general_benchmark": "GENERAL_BENCHMARK",
            "sector_benchmark": "SECTOR_BENCHMARK",
        }
        for role, ids in candle_inputs.items():
            db_role = roles[role]
            for ordinal, observation_id in enumerate(ids):
                inserted_link = await conn.fetchval(
                    """
                    INSERT INTO contextual_snapshot_candles(
                        snapshot_id, observation_id, input_role, ordinal
                    ) VALUES($1,$2,$3,$4)
                    ON CONFLICT (snapshot_id, input_role, ordinal) DO NOTHING
                    RETURNING observation_id
                    """,
                    snapshot_id, UUID(str(observation_id)), db_role, ordinal,
                )
                if inserted_link is None:
                    linked_observation = await conn.fetchval(
                        """
                        SELECT observation_id
                        FROM contextual_snapshot_candles
                        WHERE snapshot_id=$1 AND input_role=$2 AND ordinal=$3
                        """,
                        snapshot_id, db_role, ordinal,
                    )
                    if linked_observation != UUID(str(observation_id)):
                        raise ValueError("CONTEXTUAL_SNAPSHOT_INPUT_COLLISION")


async def read_contextual_snapshot(conn: Any, snapshot_id: str) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        "SELECT * FROM contextual_market_snapshots WHERE snapshot_id=$1",
        snapshot_id,
    )
    if row is None:
        return None
    links = await conn.fetch(
        """
        SELECT c.input_role, c.ordinal, o.*
        FROM contextual_snapshot_candles c
        JOIN market_candle_observations o USING(observation_id)
        WHERE c.snapshot_id=$1
        ORDER BY c.input_role, c.ordinal
        """,
        snapshot_id,
    )
    payload = dict(row)
    payload["candles"] = [dict(item) for item in links]
    return payload


__all__ = [
    "CandleObservation",
    "bar_identity",
    "ensure_contextual_g2_schema",
    "ensure_contextual_g3_schema",
    "g3_migration_sql",
    "instrument_id",
    "migration_sql",
    "observation_digest",
    "observation_rows_to_candles",
    "observations_from_provider_sequence",
    "persist_candle_observations",
    "persist_contextual_snapshot",
    "read_capture_state",
    "read_candle_observations",
    "read_contextual_snapshot",
    "read_market_evidence_as_of",
    "record_capture_event",
]

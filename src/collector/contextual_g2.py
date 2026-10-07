"""Persistence contracts for the G2 real point-in-time capture.

The operational ``market_candles`` table remains the productive source.  G2
observations are append-only evidence and contextual snapshots are SHADOW_ONLY.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
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


def migration_sql() -> str:
    return MIGRATION_PATH.read_text(encoding="utf-8")


async def ensure_contextual_g2_schema(conn: Any) -> None:
    await conn.execute(migration_sql())


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

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        for key in (
            "candle_timestamp", "bar_start", "bar_end", "available_at", "scraped_at"
        ):
            value = payload[key]
            payload[key] = value.isoformat() if value is not None else None
        return payload


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
        key = "|".join(
            [
                "quantia-contextual-g2",
                str(ingestion_run_id),
                identity,
                candle.interval,
                candle.ts.isoformat(),
                scraped_at.isoformat(),
                candle.source,
            ]
        )
        result.append(CandleObservation(
            observation_id=str(uuid5(NAMESPACE_URL, key)),
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
        ))
    return result


async def persist_candle_observations(conn: Any, observations: Sequence[CandleObservation]) -> int:
    saved = 0
    async with conn.transaction():
        for item in observations:
            status = await conn.execute(
                """
                INSERT INTO market_candle_observations(
                    observation_id, owner_chat_id, ingestion_run_id, instrument_id,
                    ticker, provider_symbol, asset_type, market, currency, interval,
                    candle_timestamp, bar_start, bar_end, available_at, scraped_at,
                    is_closed, open_price, high_price, low_price, close_price, volume,
                    price_unit, volume_unit, source, provenance, quality, missingness,
                    code_version
                ) VALUES(
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,
                    $17,$18,$19,$20,$21,$22,$23,$24,$25::jsonb,$26::jsonb,$27::jsonb,$28
                )
                ON CONFLICT (observation_id) DO NOTHING
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
            )
            saved += status == "INSERT 0 1"
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
        await conn.execute(
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
        roles = {
            "asset": "ASSET",
            "general_benchmark": "GENERAL_BENCHMARK",
            "sector_benchmark": "SECTOR_BENCHMARK",
        }
        for role, ids in candle_inputs.items():
            db_role = roles[role]
            for ordinal, observation_id in enumerate(ids):
                await conn.execute(
                    """
                    INSERT INTO contextual_snapshot_candles(
                        snapshot_id, observation_id, input_role, ordinal
                    ) VALUES($1,$2,$3,$4)
                    ON CONFLICT (snapshot_id, input_role, ordinal) DO NOTHING
                    """,
                    snapshot_id, UUID(str(observation_id)), db_role, ordinal,
                )


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
    "ensure_contextual_g2_schema",
    "instrument_id",
    "migration_sql",
    "observation_rows_to_candles",
    "observations_from_provider_sequence",
    "persist_candle_observations",
    "persist_contextual_snapshot",
    "read_candle_observations",
    "read_contextual_snapshot",
]

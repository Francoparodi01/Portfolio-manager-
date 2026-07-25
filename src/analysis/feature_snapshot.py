from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from hashlib import sha256
import json
from typing import Any, Mapping


FEATURE_SNAPSHOT_SCHEMA_VERSION = "feature_snapshot_v1"
FEATURE_HASH_LENGTH = 16

FEATURE_KEYS = (
    "technical",
    "macro",
    "sentiment",
    "risk",
    "sentiment_active",
    "sentiment_context",
    "final_score",
    "decision_from_synthesis",
    "confidence",
    "technical_data_source_mode",
    "technical_has_reconstructed_candles",
    "technical_candle_sources",
    "technical_candle_source_counts",
    "trend_shadow",
    "reversion_shadow",
)


@dataclass(frozen=True, slots=True)
class FeatureSnapshot:
    feature_snapshot_id: str
    schema_version: str
    payload: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "feature_snapshot_id": self.feature_snapshot_id,
            "schema_version": self.schema_version,
            "payload": self.payload,
        }


def build_feature_snapshot_from_layers(
    layers_payload: Mapping[str, Any],
    *,
    schema_version: str = FEATURE_SNAPSHOT_SCHEMA_VERSION,
) -> FeatureSnapshot:
    payload = {
        key: _canonicalize(layers_payload[key])
        for key in FEATURE_KEYS
        if key in layers_payload
    }
    encoded = _canonical_json(
        {
            "schema_version": schema_version,
            "payload": payload,
        }
    )
    digest = sha256(encoded.encode("utf-8")).hexdigest()[:FEATURE_HASH_LENGTH]
    return FeatureSnapshot(
        feature_snapshot_id=f"features:{digest}",
        schema_version=schema_version,
        payload=payload,
    )


def _canonical_json(value: Any) -> str:
    return json.dumps(
        _canonicalize(value),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def _canonicalize(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(k): _canonicalize(v) for k, v in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_canonicalize(item) for item in value]
    return value


__all__ = [
    "FEATURE_SNAPSHOT_SCHEMA_VERSION",
    "FeatureSnapshot",
    "build_feature_snapshot_from_layers",
]

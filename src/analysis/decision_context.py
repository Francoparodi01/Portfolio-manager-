from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from hashlib import sha256
import json
from typing import Any, TypedDict


DEFAULT_STRATEGY_ID = "quantia_core"
UNKNOWN_VERSION = "unknown"
NO_MODEL_VERSION = "none"
DEFAULT_HASH_LENGTH = 12


class DecisionRunContextDict(TypedDict):
    run_id: str
    strategy_id: str
    strategy_version: str
    planner_version: str
    optimizer_version: str
    model_version: str
    prompt_version: str
    decided_at: str
    market_snapshot_id: str | None
    portfolio_snapshot_id: str | None
    feature_snapshot_id: str | None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class DecisionRunContext:
    run_id: str
    strategy_id: str = DEFAULT_STRATEGY_ID
    strategy_version: str = UNKNOWN_VERSION
    planner_version: str = UNKNOWN_VERSION
    optimizer_version: str = UNKNOWN_VERSION
    model_version: str = NO_MODEL_VERSION
    prompt_version: str = NO_MODEL_VERSION
    decided_at: datetime = field(default_factory=_utc_now)
    market_snapshot_id: str | None = None
    portfolio_snapshot_id: str | None = None
    feature_snapshot_id: str | None = None

    def __post_init__(self) -> None:
        _require_non_empty("run_id", self.run_id)
        _require_non_empty("strategy_id", self.strategy_id)
        _require_non_empty("strategy_version", self.strategy_version)
        _require_non_empty("planner_version", self.planner_version)
        _require_non_empty("optimizer_version", self.optimizer_version)
        _require_non_empty("model_version", self.model_version)
        _require_non_empty("prompt_version", self.prompt_version)
        if self.decided_at.tzinfo is None:
            object.__setattr__(self, "decided_at", self.decided_at.replace(tzinfo=timezone.utc))

    def to_dict(self) -> DecisionRunContextDict:
        return {
            "run_id": self.run_id,
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "planner_version": self.planner_version,
            "optimizer_version": self.optimizer_version,
            "model_version": self.model_version,
            "prompt_version": self.prompt_version,
            "decided_at": self.decided_at.isoformat(),
            "market_snapshot_id": self.market_snapshot_id,
            "portfolio_snapshot_id": self.portfolio_snapshot_id,
            "feature_snapshot_id": self.feature_snapshot_id,
        }


def build_decision_run_context(
    run_id: str,
    *,
    strategy_id: str | None = None,
    strategy_version: str | None = None,
    planner_version: str | None = None,
    optimizer_version: str | None = None,
    model_version: str | None = None,
    prompt_version: str | None = None,
    decided_at: datetime | None = None,
    market_snapshot_id: str | None = None,
    portfolio_snapshot_id: str | None = None,
    feature_snapshot_id: str | None = None,
    strategy_config: Mapping[str, Any] | str | bytes | None = None,
) -> DecisionRunContext:
    return DecisionRunContext(
        run_id=run_id,
        strategy_id=_default_text(strategy_id, DEFAULT_STRATEGY_ID),
        strategy_version=_version_or_config_hash(strategy_version, strategy_config),
        planner_version=_default_text(planner_version, UNKNOWN_VERSION),
        optimizer_version=_default_text(optimizer_version, UNKNOWN_VERSION),
        model_version=_default_text(model_version, NO_MODEL_VERSION),
        prompt_version=_default_text(prompt_version, NO_MODEL_VERSION),
        decided_at=decided_at or _utc_now(),
        market_snapshot_id=market_snapshot_id,
        portfolio_snapshot_id=portfolio_snapshot_id,
        feature_snapshot_id=feature_snapshot_id,
    )


def short_config_hash(
    config: Mapping[str, Any] | str | bytes,
    *,
    length: int = DEFAULT_HASH_LENGTH,
) -> str:
    if length <= 0:
        raise ValueError("hash length must be positive")
    if isinstance(config, bytes):
        payload = config
    elif isinstance(config, str):
        payload = config.encode("utf-8")
    else:
        payload = json.dumps(
            config,
            default=str,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    return sha256(payload).hexdigest()[:length]


def _version_or_config_hash(
    explicit_version: str | None,
    strategy_config: Mapping[str, Any] | str | bytes | None,
) -> str:
    if explicit_version is not None and explicit_version.strip():
        return explicit_version
    if strategy_config is not None:
        return short_config_hash(strategy_config)
    return UNKNOWN_VERSION


def _default_text(value: str | None, default: str) -> str:
    if value is None:
        return default
    stripped = value.strip()
    return stripped or default


def _require_non_empty(field_name: str, value: str) -> None:
    if not value.strip():
        raise ValueError(f"{field_name} must be non-empty")


__all__ = [
    "DEFAULT_STRATEGY_ID",
    "DecisionRunContext",
    "DecisionRunContextDict",
    "NO_MODEL_VERSION",
    "UNKNOWN_VERSION",
    "build_decision_run_context",
    "short_config_hash",
]

from __future__ import annotations

import hashlib
from collections import Counter, defaultdict
from datetime import datetime, timezone
from math import isfinite
from typing import Any, Iterable, Mapping, Sequence

from .contracts import TrainingExample


LEAKAGE_TOKENS = (
    "outcome",
    "realized",
    "future",
    "forward_return",
    "pnl_after",
    "return_5d",
    "return_10d",
    "return_20d",
    "target_hit_after",
    "stop_hit_after",
)


def _run_id(row: Mapping[str, Any]) -> str:
    return str(row.get("run_id") or row.get("id") or "").strip()


def _parse_dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value or "").strip()
        if not text:
            raise ValueError("missing timestamp")
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _safe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if isfinite(result) else None


def _safe_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _hash_goal(goal: Any) -> str:
    return hashlib.sha256(str(goal or "").encode("utf-8")).hexdigest()


def _validate_pre_decision_features(features: Mapping[str, Any]) -> dict[str, Any]:
    clean: dict[str, Any] = {}
    for raw_key, value in features.items():
        key = str(raw_key).strip()
        lowered = key.lower()
        if any(token in lowered for token in LEAKAGE_TOKENS):
            raise ValueError(f"potential look-ahead feature rejected: {key}")
        if isinstance(value, (str, int, float, bool)) or value is None:
            clean[key] = value
    return clean


def build_training_dataset(
    *,
    agent_runs: Sequence[Mapping[str, Any]],
    agent_steps: Sequence[Mapping[str, Any]],
    jev_assessments: Sequence[Mapping[str, Any]],
    outcomes: Sequence[Mapping[str, Any]],
    require_outcome: bool = True,
) -> tuple[list[TrainingExample], dict[str, int]]:
    """Join agent trace + JEV + matured ledger outcome into training rows.

    Outcomes are labels only. Any pre-decision features provided by JEV are
    screened for obvious future/outcome leakage before they enter `features`.
    """

    steps_by_run: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in agent_steps:
        rid = _run_id(row)
        if rid:
            steps_by_run[rid].append(row)

    jev_by_run = {_run_id(row): row for row in jev_assessments if _run_id(row)}
    outcome_by_run = {_run_id(row): row for row in outcomes if _run_id(row)}
    outcome_by_decision = {
        int(row["decision_log_id"]): row
        for row in outcomes
        if _safe_int(row.get("decision_log_id")) is not None
    }

    exclusions: Counter[str] = Counter()
    examples: list[TrainingExample] = []

    for run in agent_runs:
        rid = _run_id(run)
        if not rid:
            exclusions["missing_run_id"] += 1
            continue

        jev = jev_by_run.get(rid)
        if jev is None:
            exclusions["missing_jev"] += 1
            continue
        if not bool(jev.get("eligible", True)):
            exclusions["jev_ineligible"] += 1
            continue
        if not bool(jev.get("evidence_complete", True)):
            exclusions["incomplete_evidence"] += 1
            continue

        decision_log_id = _safe_int(jev.get("decision_log_id"))
        outcome = outcome_by_run.get(rid)
        if outcome is None and decision_log_id is not None:
            outcome = outcome_by_decision.get(decision_log_id)

        if outcome is None:
            if require_outcome:
                exclusions["missing_outcome"] += 1
                continue
            outcome = {}

        if require_outcome and not bool(outcome.get("matured", True)):
            exclusions["immature_outcome"] += 1
            continue
        if not bool(outcome.get("data_quality_ok", True)):
            exclusions["outcome_data_quality"] += 1
            continue

        directional_outcome = _safe_float(outcome.get("directional_outcome"))
        horizon_days = _safe_int(outcome.get("horizon_days"))
        if require_outcome and (directional_outcome is None or horizon_days is None):
            exclusions["invalid_outcome"] += 1
            continue
        directional_outcome = directional_outcome if directional_outcome is not None else 0.0
        horizon_days = horizon_days if horizon_days is not None else 0

        try:
            decided_at = _parse_dt(run.get("started_at") or run.get("decided_at"))
            pre_features = _validate_pre_decision_features(
                jev.get("pre_decision_features") or {}
            )
        except (TypeError, ValueError):
            exclusions["invalid_or_leaky_features"] += 1
            continue

        steps = sorted(
            steps_by_run.get(rid, []),
            key=lambda row: int(row.get("step_no") or 0),
        )
        tool_sequence = tuple(
            str(row.get("tool_name") or "").strip()
            for row in steps
            if str(row.get("tool_name") or "").strip()
        )
        failed_tool_calls = sum(
            1
            for row in steps
            if row.get("observation_ok") is False
        )
        confidences = [
            value
            for value in (_safe_float(row.get("confidence")) for row in steps)
            if value is not None
        ]
        mean_confidence = (
            sum(confidences) / len(confidences) if confidences else None
        )

        process_features: dict[str, Any] = {
            "tool_count": len(tool_sequence),
            "unique_tool_count": len(set(tool_sequence)),
            "failed_tool_calls": failed_tool_calls,
            "controller_mean_confidence": mean_confidence,
            "agent_status": str(run.get("status") or ""),
            "stop_reason": str(run.get("stop_reason") or ""),
            **pre_features,
        }

        examples.append(
            TrainingExample(
                run_id=rid,
                decided_at=decided_at,
                model=str(run.get("model") or "unknown"),
                status=str(run.get("status") or ""),
                stop_reason=(
                    str(run.get("stop_reason")) if run.get("stop_reason") is not None else None
                ),
                goal_sha256=_hash_goal(run.get("goal")),
                tool_sequence=tool_sequence,
                tool_count=len(tool_sequence),
                unique_tool_count=len(set(tool_sequence)),
                failed_tool_calls=failed_tool_calls,
                mean_controller_confidence=mean_confidence,
                jev_label=str(jev.get("label") or "UNLABELED"),
                jev_score=_safe_float(jev.get("score")),
                jev_version=str(jev.get("jev_version") or jev.get("version") or "unknown"),
                decision_log_id=decision_log_id,
                action=(str(jev.get("action")) if jev.get("action") is not None else None),
                features=process_features,
                horizon_days=horizon_days,
                directional_outcome=directional_outcome,
                alpha_vs_benchmark=_safe_float(outcome.get("alpha_vs_benchmark")),
                max_adverse_excursion=_safe_float(outcome.get("max_adverse_excursion")),
                risk_guard_violations=int(jev.get("risk_guard_violations") or 0),
                data_quality_ok=True,
                metadata={
                    "outcome_basis": outcome.get("outcome_basis"),
                    "source": outcome.get("source"),
                },
            )
        )

    examples.sort(key=lambda row: (row.decided_at, row.run_id))
    return examples, dict(sorted(exclusions.items()))


def temporal_split(
    examples: Sequence[TrainingExample],
    *,
    validation_fraction: float = 0.20,
    min_validation: int = 1,
) -> tuple[list[TrainingExample], list[TrainingExample]]:
    """Deterministic chronological split. Never randomly mixes future into train."""

    ordered = sorted(examples, key=lambda row: (row.decided_at, row.run_id))
    if len(ordered) < 2:
        return list(ordered), []
    fraction = max(0.05, min(0.50, float(validation_fraction)))
    validation_size = max(min_validation, int(round(len(ordered) * fraction)))
    validation_size = min(validation_size, len(ordered) - 1)
    split_at = len(ordered) - validation_size
    return list(ordered[:split_at]), list(ordered[split_at:])

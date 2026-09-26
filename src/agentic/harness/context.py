from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from src.agentic.contracts import ToolObservation

from .contracts import (
    ContextPack,
    ConversationState,
    Evidence,
    EvidenceMode,
    EvidenceQuality,
    ExecutionBudget,
    TaskSpec,
)


_RESEARCH_TOOLS = {
    "compare_plan_vs_hold",
    "get_decision_value_added",
    "get_decision_counterfactuals",
    "get_similar_historical_episodes",
    "compare_strategy_versions",
    "get_replay_evidence_quality",
}
_SHADOW_PREFIXES = ("meta_", "shadow_")


def _mode(tool: str, payload: Any) -> EvidenceMode:
    if isinstance(payload, dict):
        raw = str(payload.get("mode") or payload.get("policy_mode") or "").upper()
        if raw in EvidenceMode.__members__:
            return EvidenceMode(raw)
        if raw == "SHADOW_ONLY":
            return EvidenceMode.SHADOW
    if tool in _RESEARCH_TOOLS:
        return EvidenceMode.RESEARCH
    if tool.startswith(_SHADOW_PREFIXES):
        return EvidenceMode.SHADOW
    return EvidenceMode.OBSERVATION


def _quality(payload: Any) -> EvidenceQuality:
    if not isinstance(payload, dict):
        return EvidenceQuality.UNKNOWN
    candidates = [payload.get("quality"), payload.get("data_quality"), payload.get("reconstruction_quality")]
    for candidate in candidates:
        if isinstance(candidate, list) and candidate:
            candidate = candidate[0]
        if isinstance(candidate, dict):
            candidate = candidate.get("grade") or candidate.get("quality")
        raw = str(candidate or "").upper()
        if raw in EvidenceQuality.__members__:
            return EvidenceQuality(raw)
    status = str(payload.get("status") or "").upper()
    if status in {"INSUFFICIENT", "MISSING", "UNAVAILABLE"}:
        return EvidenceQuality.INSUFFICIENT
    return EvidenceQuality.UNKNOWN


def _timestamp(payload: Any) -> datetime:
    if isinstance(payload, dict):
        for key in (
            "source_timestamp", "evaluated_at", "fetched_at", "scraped_at", "as_of",
            "cutoff_at", "generated_at", "timestamp",
        ):
            value = payload.get(key)
            if not value:
                continue
            try:
                parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed.astimezone(timezone.utc)
            except ValueError:
                continue
    return datetime.now(timezone.utc)


def _warnings(payload: Any, observation: ToolObservation) -> list[str]:
    warnings: list[str] = []
    if observation.error:
        warnings.append(observation.error)
    if isinstance(payload, dict):
        for key in ("warnings", "limitations"):
            value = payload.get(key)
            if isinstance(value, list):
                warnings.extend(str(item)[:500] for item in value if item)
            elif value:
                warnings.append(str(value)[:1000])
        status = str(payload.get("status") or "").upper()
        if status in {"PARTIAL", "INSUFFICIENT", "MISSING", "UNAVAILABLE"}:
            warnings.append(f"source status: {status}")
        if payload.get("truncated_details"):
            warnings.append("source details were compacted by the tool boundary")
    return list(dict.fromkeys(warnings))[:10]


def observation_to_evidence(observation: ToolObservation, *, max_chars: int = 12000) -> Evidence:
    raw = observation.content or ""
    payload: Any
    try:
        payload = json.loads(raw) if raw else None
    except (TypeError, ValueError):
        payload = None

    timestamp = _timestamp(payload)
    age = max(0.0, (datetime.now(timezone.utc) - timestamp).total_seconds())
    excerpt = raw[:max_chars]
    if len(raw) > max_chars:
        excerpt += "\n[compacted by harness]"

    source = observation.tool_name
    if isinstance(payload, dict):
        source = str(payload.get("source") or payload.get("schema_version") or source)

    return Evidence(
        source=source,
        tool=observation.tool_name,
        timestamp=timestamp,
        mode=_mode(observation.tool_name, payload),
        quality=_quality(payload),
        data=payload,
        excerpt=excerpt,
        warnings=_warnings(payload, observation),
        freshness_seconds=age,
        sha256=observation.content_sha256,
        ok=observation.ok,
        error=observation.error,
        elapsed_ms=observation.elapsed_ms,
        cached=observation.cached,
    )


def _relevance_score(evidence: Evidence, task: TaskSpec) -> int:
    score = 0
    if evidence.tool in task.required_tools:
        score += 100
    if evidence.tool in task.optional_tools:
        score += 50
    text = evidence.excerpt.upper()
    score += 10 * sum(1 for symbol in task.entities if symbol in text)
    if evidence.ok:
        score += 5
    if evidence.quality in {EvidenceQuality.HIGH, EvidenceQuality.MEDIUM}:
        score += 3
    return score


def build_context_pack(
    *,
    state: ConversationState,
    task: TaskSpec,
    evidence: list[Evidence],
    recent_context: list[dict] | None,
    budget: ExecutionBudget,
) -> ContextPack:
    recent_goals = [str(item.get("goal") or "")[:1000] for item in (recent_context or []) if item.get("goal")][-3:]
    ordered = sorted(evidence, key=lambda item: _relevance_score(item, task), reverse=True)
    selected: list[Evidence] = []
    chars = sum(len(item) for item in recent_goals)
    pruned = 0

    for item in ordered:
        remaining = budget.max_context_chars - chars
        if remaining <= 0:
            pruned += 1
            continue
        copy = item.model_copy(deep=True)
        if len(copy.excerpt) > remaining:
            if remaining < 300:
                pruned += 1
                continue
            copy.excerpt = copy.excerpt[:remaining] + "\n[context pruned]"
        selected.append(copy)
        chars += len(copy.excerpt)

    return ContextPack(
        conversation=state,
        recent_user_goals=recent_goals,
        selected_evidence=selected,
        selected_chars=chars,
        pruned_items=pruned,
        budget_chars=budget.max_context_chars,
    )

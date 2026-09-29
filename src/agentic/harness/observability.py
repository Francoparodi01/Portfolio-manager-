from __future__ import annotations

import json
import math
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from src.agentic.read_only import connect_read_only

from .schemas import ClaimStatus, HarnessObservabilitySummary


def _dsn_for_asyncpg(dsn: str) -> str:
    if dsn.startswith("postgresql+asyncpg://"):
        return "postgresql://" + dsn[len("postgresql+asyncpg://") :]
    return dsn


def _row_value(row: Any, key: str, default: Any = None) -> Any:
    try:
        return row[key]
    except (KeyError, IndexError, TypeError):
        return default


def _metadata(row: Any) -> dict[str, Any]:
    raw = _row_value(row, "metadata", {})
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except (TypeError, ValueError):
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _finite_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 4)


def _p95(values: list[float]) -> int | None:
    if not values:
        return None
    ordered = sorted(max(0.0, value) for value in values)
    index = max(0, math.ceil(0.95 * len(ordered)) - 1)
    return int(round(ordered[index]))


def _verification(metadata: dict[str, Any]) -> dict[str, Any] | None:
    value = metadata.get("verification")
    return value if isinstance(value, dict) else None


def _claim_counts(verification: dict[str, Any]) -> Counter[str]:
    result: Counter[str] = Counter()
    explicit = verification.get("claim_status_counts")
    if isinstance(explicit, dict):
        for status in ClaimStatus:
            value = explicit.get(status.value)
            try:
                count = int(value)
            except (TypeError, ValueError):
                count = 0
            if count > 0:
                result[status.value] += count
        if result:
            return result

    claims = verification.get("claim_results")
    if isinstance(claims, list):
        for claim in claims:
            if not isinstance(claim, dict):
                continue
            status = str(claim.get("status") or "").upper().strip()
            if status in {item.value for item in ClaimStatus}:
                result[status] += 1
    return result


def summarize_run_rows(
    rows: Iterable[Any],
    *,
    window_days: int = 7,
) -> HarnessObservabilitySummary:
    """Aggregate safe run telemetry without interpreting financial evidence."""
    days = max(1, min(365, int(window_days)))
    items = list(rows)
    status_counts: Counter[str] = Counter()
    stop_reason_counts: Counter[str] = Counter()
    latencies: list[float] = []
    tool_calls: list[float] = []
    llm_calls: list[float] = []
    verification_passed: list[bool] = []
    numeric_consistency: list[bool] = []
    claim_coverages: list[float] = []
    claim_status_counts: Counter[str] = Counter({status.value: 0 for status in ClaimStatus})

    for row in items:
        status = str(_row_value(row, "status", "UNKNOWN") or "UNKNOWN").upper().strip()
        stop_reason = str(_row_value(row, "stop_reason", "unknown") or "unknown").strip()
        status_counts[status] += 1
        stop_reason_counts[stop_reason] += 1

        metadata = _metadata(row)
        for key, target in (
            ("latency_ms", latencies),
            ("tool_calls", tool_calls),
            ("llm_calls", llm_calls),
        ):
            value = _finite_float(metadata.get(key))
            if value is not None and value >= 0:
                target.append(value)

        verification = _verification(metadata)
        if verification is None:
            continue

        passed = verification.get("passed")
        if isinstance(passed, bool):
            verification_passed.append(passed)

        numeric = verification.get("numeric_consistency")
        if isinstance(numeric, bool):
            numeric_consistency.append(numeric)

        coverage = _finite_float(verification.get("required_claim_coverage"))
        if coverage is not None:
            claim_coverages.append(max(0.0, min(1.0, coverage)))

        claim_status_counts.update(_claim_counts(verification))

    runs_total = len(items)
    complete = int(status_counts.get("COMPLETE", 0))
    completion_rate = complete / runs_total if runs_total else 0.0

    return HarnessObservabilitySummary(
        window_days=days,
        runs_total=runs_total,
        status_counts=dict(status_counts),
        stop_reason_counts=dict(stop_reason_counts),
        completion_rate=round(completion_rate, 4),
        verification_pass_rate=(
            round(sum(verification_passed) / len(verification_passed), 4)
            if verification_passed else None
        ),
        numeric_consistency_rate=(
            round(sum(numeric_consistency) / len(numeric_consistency), 4)
            if numeric_consistency else None
        ),
        avg_required_claim_coverage=_mean(claim_coverages),
        claim_status_counts={status.value: int(claim_status_counts[status.value]) for status in ClaimStatus},
        avg_latency_ms=_mean(latencies),
        p95_latency_ms=_p95(latencies),
        avg_tool_calls=_mean(tool_calls),
        avg_llm_calls=_mean(llm_calls),
    )


async def load_observability_summary(
    database_url: str,
    owner_chat_id: int,
    *,
    window_days: int = 7,
    namespace: str = "conversational-harness-v1",
) -> HarnessObservabilitySummary:
    """Read recent harness runs owner-scoped and aggregate operational telemetry."""
    if not owner_chat_id:
        raise ValueError("observability requires an explicit owner")
    days = max(1, min(365, int(window_days)))
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    conn = await connect_read_only(_dsn_for_asyncpg(database_url), command_timeout=30)
    try:
        rows = await conn.fetch(
            """
            SELECT status, stop_reason, metadata
            FROM agent_runs
            WHERE owner_chat_id=$1
              AND started_at >= $2
              AND metadata->>'context_namespace'=$3
            ORDER BY started_at DESC
            """,
            owner_chat_id,
            cutoff,
            namespace,
        )
        return summarize_run_rows(rows, window_days=days)
    finally:
        await conn.close()


__all__ = ["load_observability_summary", "summarize_run_rows"]

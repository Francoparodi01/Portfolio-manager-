"""Read-only comparison of technical-shadow-v2 against the current baseline."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg


HORIZONS = (5, 10, 20, 40)


def _float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def recover_raw_return(directional_return: Any, parent_decision: str) -> float | None:
    value = _float(directional_return)
    if value is None:
        return None
    if str(parent_decision or "").upper().startswith("SELL"):
        if value <= -1.0:
            return None
        return (1.0 / (1.0 + value)) - 1.0
    return value


def directional_return(raw_return: Any, score: Any) -> float | None:
    raw = _float(raw_return)
    direction_score = _float(score)
    if raw is None or direction_score is None or direction_score == 0.0:
        return None
    if direction_score > 0.0:
        return raw
    if raw <= -1.0:
        return None
    return (1.0 / (1.0 + raw)) - 1.0


@dataclass(frozen=True, slots=True)
class TechnicalShadowMetric:
    horizon: int
    baseline_n: int
    baseline_win_rate: float | None
    baseline_mean: float | None
    shadow_n: int
    shadow_win_rate: float | None
    shadow_mean: float | None


@dataclass(frozen=True, slots=True)
class TechnicalShadowAudit:
    generated_at: datetime
    rows_loaded: int
    episodes: int
    first_observed_at: datetime | None
    last_observed_at: datetime | None
    metrics: tuple[TechnicalShadowMetric, ...]


def build_technical_shadow_audit(rows: list[dict[str, Any]]) -> TechnicalShadowAudit:
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in sorted(rows, key=lambda item: (item.get("decided_at"), item.get("id", 0))):
        observed = row.get("decided_at")
        day = observed.date().isoformat() if hasattr(observed, "date") else str(observed)[:10]
        key = (str(row.get("ticker") or "").upper(), day)
        deduped.setdefault(key, row)

    episodes = list(deduped.values())
    metrics: list[TechnicalShadowMetric] = []
    for horizon in HORIZONS:
        baseline_values: list[float] = []
        shadow_values: list[float] = []
        outcome_key = f"outcome_{horizon}d"
        for row in episodes:
            raw = recover_raw_return(row.get(outcome_key), str(row.get("decision") or ""))
            baseline = directional_return(raw, row.get("baseline_score_raw"))
            shadow = directional_return(raw, row.get("shadow_score"))
            if baseline is not None:
                baseline_values.append(baseline)
            if shadow is not None:
                shadow_values.append(shadow)

        metrics.append(
            TechnicalShadowMetric(
                horizon=horizon,
                baseline_n=len(baseline_values),
                baseline_win_rate=(
                    sum(value > 0 for value in baseline_values) / len(baseline_values)
                    if baseline_values else None
                ),
                baseline_mean=(sum(baseline_values) / len(baseline_values) if baseline_values else None),
                shadow_n=len(shadow_values),
                shadow_win_rate=(
                    sum(value > 0 for value in shadow_values) / len(shadow_values)
                    if shadow_values else None
                ),
                shadow_mean=(sum(shadow_values) / len(shadow_values) if shadow_values else None),
            )
        )

    timestamps = [row.get("decided_at") for row in episodes if row.get("decided_at")]
    return TechnicalShadowAudit(
        generated_at=datetime.now(timezone.utc),
        rows_loaded=len(rows),
        episodes=len(episodes),
        first_observed_at=min(timestamps) if timestamps else None,
        last_observed_at=max(timestamps) if timestamps else None,
        metrics=tuple(metrics),
    )


async def load_technical_shadow_rows(
    database_url: str,
    *,
    days: int = 365,
) -> list[dict[str, Any]]:
    dsn = database_url.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    try:
        rows = await conn.fetch(
            """
            SELECT
                id,
                decided_at,
                ticker,
                decision,
                layers->'technical_shadow_v2'->>'version' AS version,
                (layers->'technical_shadow_v2'->>'baseline_score_raw')::float AS baseline_score_raw,
                (layers->'technical_shadow_v2'->>'score')::float AS shadow_score,
                COALESCE(executable_outcome_5d, outcome_5d) AS outcome_5d,
                COALESCE(executable_outcome_10d, outcome_10d) AS outcome_10d,
                COALESCE(executable_outcome_20d, outcome_20d) AS outcome_20d,
                COALESCE(executable_outcome_40d, outcome_40d) AS outcome_40d
            FROM decision_log
            WHERE decided_at >= NOW() - ($1::int * INTERVAL '1 day')
              AND outcome_basis = 'canonical_cocos'
              AND layers->'technical_shadow_v2'->>'version' = 'technical-shadow-v2'
              AND layers->'technical_shadow_v2'->>'baseline_score_raw' IS NOT NULL
              AND layers->'technical_shadow_v2'->>'score' IS NOT NULL
            ORDER BY decided_at, id
            """,
            max(1, int(days)),
        )
        return [dict(row) for row in rows]
    finally:
        await conn.close()


def render_technical_shadow_audit(report: TechnicalShadowAudit) -> str:
    lines = [
        "TECHNICAL SHADOW V2 - AUDITORIA READ-ONLY",
        f"Filas: {report.rows_loaded} | episodios ticker/dia: {report.episodes}",
    ]
    if not report.episodes:
        return "\n".join(lines + ["Sin episodios maduros todavia."])
    lines.append("Hz | baseline n/win/avg | shadow-v2 n/win/avg")
    for metric in report.metrics:
        def pct(value: float | None) -> str:
            return "pend." if value is None else f"{value:+.1%}"

        lines.append(
            f"{metric.horizon:>2}d | "
            f"{metric.baseline_n}/{pct(metric.baseline_win_rate)}/{pct(metric.baseline_mean)} | "
            f"{metric.shadow_n}/{pct(metric.shadow_win_rate)}/{pct(metric.shadow_mean)}"
        )
    lines.append("No modifica scoring, thresholds, planes ni ordenes.")
    return "\n".join(lines)


__all__ = [
    "TechnicalShadowAudit",
    "TechnicalShadowMetric",
    "build_technical_shadow_audit",
    "directional_return",
    "load_technical_shadow_rows",
    "recover_raw_return",
    "render_technical_shadow_audit",
]

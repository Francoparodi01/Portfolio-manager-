from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from src.collector.broker_fills import BrokerFill


@dataclass(frozen=True)
class ExecutionCandidate:
    id: int
    ticker: str
    decision: str
    decided_at: datetime
    status: str
    theoretical_amount_ars: float | None = None


def choose_execution_candidate(
    fill: BrokerFill,
    candidates: list[ExecutionCandidate],
    *,
    max_age: timedelta = timedelta(days=3),
) -> ExecutionCandidate | None:
    eligible: list[ExecutionCandidate] = []

    for candidate in candidates:
        age = fill.executed_at - candidate.decided_at
        if candidate.ticker.upper() != fill.ticker.upper():
            continue
        if candidate.decision.upper() != fill.side.upper():
            continue
        if candidate.status.upper() != "APPROVED":
            continue
        if age < timedelta(0) or age > max_age:
            continue
        eligible.append(candidate)

    if not eligible:
        return None

    fill_amount = (
        float(fill.gross_amount_ars)
        if fill.gross_amount_ars is not None
        else float(fill.quantity) * float(fill.avg_fill_price)
    )

    def _rank(candidate: ExecutionCandidate) -> tuple[float, float, int]:
        age_seconds = abs((fill.executed_at - candidate.decided_at).total_seconds())
        amount_gap = (
            abs(float(candidate.theoretical_amount_ars) - fill_amount)
            if candidate.theoretical_amount_ars is not None
            else float("inf")
        )
        return age_seconds, amount_gap, candidate.id

    return min(eligible, key=_rank)

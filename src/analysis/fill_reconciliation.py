from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from src.collector.broker_fills import BrokerFill

ART = ZoneInfo("America/Argentina/Buenos_Aires")


@dataclass(frozen=True)
class ExecutionCandidate:
    id: int
    ticker: str
    decision: str
    decided_at: datetime
    status: str
    theoretical_amount_ars: float | None = None


def _local_date(value: datetime):
    if value.tzinfo is None:
        return value.date()
    return value.astimezone(ART).date()


def _decision_matches_fill(decision: str, side: str) -> bool:
    decision_norm = str(decision or "").upper()
    side_norm = str(side or "").upper()
    if side_norm == "BUY":
        return decision_norm == "BUY"
    if side_norm == "SELL":
        return decision_norm in {"SELL", "SELL_PARTIAL", "SELL_FULL"}
    return False


def _age_for_match(fill: BrokerFill, candidate: ExecutionCandidate) -> timedelta | None:
    age = fill.executed_at - candidate.decided_at
    if age >= timedelta(0):
        return age

    # Cocos movements often provide operation date without a reliable intraday
    # timestamp. Same local calendar day is eligible only for plans emitted
    # before close. EOD plans cannot be matched to fills from earlier that day.
    decision_local = candidate.decided_at.astimezone(ART) if candidate.decided_at.tzinfo else candidate.decided_at
    same_local_day = (
        fill.executed_at.date() == candidate.decided_at.date()
        or _local_date(fill.executed_at) == decision_local.date()
    )
    if same_local_day and decision_local.time() < time(17, 0):
        return timedelta(0)
    return None


def choose_execution_candidate(
    fill: BrokerFill,
    candidates: list[ExecutionCandidate],
    *,
    max_age: timedelta = timedelta(days=3),
) -> ExecutionCandidate | None:
    eligible: list[ExecutionCandidate] = []

    for candidate in candidates:
        if candidate.ticker.upper() != fill.ticker.upper():
            continue
        if not _decision_matches_fill(candidate.decision, fill.side):
            continue
        if candidate.status.upper() != "APPROVED":
            continue
        age = _age_for_match(fill, candidate)
        if age is None or age > max_age:
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
        age = _age_for_match(fill, candidate) or timedelta.max
        age_seconds = abs(age.total_seconds())
        amount_gap = (
            abs(abs(float(candidate.theoretical_amount_ars)) - abs(fill_amount))
            if candidate.theoretical_amount_ars is not None
            else float("inf")
        )
        return age_seconds, amount_gap, candidate.id

    return min(eligible, key=_rank)

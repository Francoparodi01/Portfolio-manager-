from __future__ import annotations

from collections import Counter
from dataclasses import dataclass

from src.analysis.dcl.outcome_loader import EnrichedDecision, OutcomeLoader
from src.analysis.dcl.sample_safety import SampleSafety
from src.analysis.dcl.statistical_auditor import AuditResult, StatisticalAuditor


@dataclass(frozen=True)
class CalibrationReport:
    decisions: list[EnrichedDecision]
    safety: SampleSafety
    audit: AuditResult
    status: str
    lookback_days: int
    quality_counts: dict[str, int]
    quality_mode: str = "relaxed"
    owner_chat_id: int | None = None


async def run_calibration_cycle(
    database_url: str,
    *,
    days: int = 180,
    owner_chat_id: int | None = None,
    quality_mode: str = "relaxed",
    min_n: int = 20,
    dry_run: bool = True,
) -> CalibrationReport:
    """Run DCL v0.

    dry_run is kept explicit for the future proposal workflow. This version
    never writes config or calibration proposals.
    """
    decisions = await OutcomeLoader(database_url).load(
        days=days,
        owner_chat_id=owner_chat_id,
        quality_mode=quality_mode,
    )
    safety = SampleSafety.evaluate(decisions)
    quality_counts = dict(Counter(d.data_quality or "unknown" for d in decisions))
    audit = StatisticalAuditor().run(decisions, subset_label="all")

    if safety.n_auditable < min_n:
        status = "INSUFFICIENT_DATA"
    elif audit.is_statistically_significant:
        status = "AUDIT_READY"
    else:
        status = "DESCRIPTIVE_ONLY"

    return CalibrationReport(
        decisions=decisions,
        safety=safety,
        audit=audit,
        status=status,
        lookback_days=days,
        quality_counts=quality_counts,
        quality_mode=quality_mode,
        owner_chat_id=owner_chat_id,
    )

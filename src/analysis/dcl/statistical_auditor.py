from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

from src.analysis.audit_inference import evidence, mean_interval, rank_ic_interval
from src.analysis.dcl.outcome_loader import EnrichedDecision


@dataclass(frozen=True)
class AuditResult:
    subset_label: str
    n_decisions: int
    n_auditable: int
    ic_5d: Optional[float]
    ic_10d: Optional[float]
    ic_20d: Optional[float]
    ic_tstat: Optional[float]
    win_rate: Optional[float]
    win_rate_ci_95: tuple[Optional[float], Optional[float]]
    ev_mean: Optional[float]
    ev_bootstrap_ci: tuple[Optional[float], Optional[float]]
    is_statistically_significant: bool
    confidence_level: str
    warning_flags: list[str] = field(default_factory=list)
    # 95% date-block interval of the primary-horizon rank IC.
    ic_ci_95: tuple[Optional[float], Optional[float]] = (None, None)


def _spearman(xs: list[float], ys: list[float]) -> Optional[float]:
    if len(xs) < 3 or len(ys) < 3:
        return None
    xr = pd.Series(xs).rank(method="average").to_numpy(dtype=float)
    yr = pd.Series(ys).rank(method="average").to_numpy(dtype=float)
    if np.nanstd(xr) == 0 or np.nanstd(yr) == 0:
        return None
    return float(np.corrcoef(xr, yr)[0, 1])


def _wilson_ci(wins: int, n: int, z: float = 1.96) -> tuple[Optional[float], Optional[float]]:
    if n <= 0:
        return None, None
    p = wins / n
    denom = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    margin = z * math.sqrt((p * (1 - p) / n) + (z * z / (4 * n * n))) / denom
    return max(0.0, center - margin), min(1.0, center + margin)


def _bootstrap_mean_ci(values: list[float], n_iter: int = 1000) -> tuple[Optional[float], Optional[float]]:
    if not values:
        return None, None
    if len(values) == 1:
        return values[0], values[0]
    rng = np.random.default_rng(42)
    arr = np.asarray(values, dtype=float)
    samples = rng.choice(arr, size=(n_iter, len(arr)), replace=True).mean(axis=1)
    low, high = np.percentile(samples, [2.5, 97.5])
    return float(low), float(high)


def _outcome(decision: EnrichedDecision, horizon: str) -> Optional[float]:
    return {
        "5d": decision.outcome_5d,
        "10d": decision.outcome_10d,
        "20d": decision.outcome_20d,
    }.get(horizon)


def _score(decision: EnrichedDecision) -> float:
    # Outcomes are directional: sells must be scored in their own direction.
    return decision.aligned_score if decision.aligned_score is not None else decision.final_score


class StatisticalAuditor:
    def run(
        self,
        decisions: list[EnrichedDecision],
        *,
        subset_label: str = "all",
        primary_horizon: str = "5d",
    ) -> AuditResult:
        auditable = [d for d in decisions if d.is_auditable]
        n_decisions = len(decisions)
        n_auditable = len(auditable)
        warning_flags: list[str] = []

        if n_auditable < 20:
            warning_flags.append("INSUFFICIENT_SAMPLE")

        ic_by_horizon: dict[str, Optional[float]] = {}
        for horizon in ("5d", "10d", "20d"):
            rows = [
                (_score(d), _outcome(d, horizon))
                for d in auditable
                if _outcome(d, horizon) is not None
            ]
            ic_by_horizon[horizon] = (
                _spearman([r[0] for r in rows], [float(r[1]) for r in rows])
                if rows
                else None
            )

        primary = [d for d in auditable if _outcome(d, primary_horizon) is not None]
        primary_values = [float(_outcome(d, primary_horizon)) for d in primary]
        primary_ic = ic_by_horizon.get(primary_horizon)
        ic_tstat = None
        if primary_ic is not None and len(primary_values) > 1:
            ic_tstat = primary_ic / (1 / math.sqrt(len(primary_values)))

        wins = sum(1 for value in primary_values if value > 0)
        n = len(primary_values)
        win_rate = wins / n if n else None
        win_ci = _wilson_ci(wins, n)
        ev_mean = float(np.mean(primary_values)) if primary_values else None

        dates = [d.decision_date for d in primary]
        dated = bool(primary) and all(dates)
        if dated:
            # Overlapping outcomes of nearby decisions are not independent rows.
            ic_result = rank_ic_interval(dates, [_score(d) for d in primary], primary_values, primary_horizon)
            ev_result = mean_interval(dates, primary_values, primary_horizon)
            ic_ci = (ic_result.get("lower"), ic_result.get("upper"))
            ev_ci = (ev_result.get("lower"), ev_result.get("upper"))
            ic_evidence = evidence(ic_result)
        else:
            warning_flags.append("NO_DECISION_DATES_INDEPENDENT_ROWS_ASSUMED")
            ic_ci = (None, None)
            ev_ci = _bootstrap_mean_ci(primary_values)
            ic_evidence = None if ic_tstat is None else (True if ic_tstat > 1.96 else False if ic_tstat < -1.96 else None)

        # Only a positive IC is evidence for the score; a significantly negative IC is a red flag.
        significant = ic_evidence is True
        if ic_evidence is False:
            warning_flags.append("IC_SIGNIFICANTLY_NEGATIVE")
        elif not significant:
            warning_flags.append("NOT_STATISTICALLY_SIGNIFICANT")

        if significant and n_auditable >= 50:
            confidence = "HIGH"
        elif significant and n_auditable >= 30:
            confidence = "MEDIUM"
        else:
            confidence = "LOW"

        return AuditResult(
            subset_label=subset_label,
            n_decisions=n_decisions,
            n_auditable=n_auditable,
            ic_5d=ic_by_horizon["5d"],
            ic_10d=ic_by_horizon["10d"],
            ic_20d=ic_by_horizon["20d"],
            ic_tstat=ic_tstat,
            win_rate=win_rate,
            win_rate_ci_95=win_ci,
            ev_mean=ev_mean,
            ev_bootstrap_ci=ev_ci,
            is_statistically_significant=significant,
            confidence_level=confidence,
            warning_flags=warning_flags,
            ic_ci_95=ic_ci,
        )

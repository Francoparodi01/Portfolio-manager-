"""Shared inference for the audit reports (/viability, /regression, /dcl).

Outcomes of decisions taken on nearby dates overlap in time and share the same
market moves, so they are not independent observations. Every interval here is
a date-block bootstrap over decision dates (reusing
``src.analysis.date_block_statistics``): rows of the same date are resampled
together and consecutive dates travel in blocks roughly as long as the outcome
horizon. A gate is only "passed" when the whole interval is on the right side
of zero; an interval that crosses zero is "not conclusive", never a pass.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any, Optional
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from src.analysis.date_block_statistics import date_draws, interval, spearman


ART = ZoneInfo("America/Argentina/Buenos_Aires")
SELL_ACTIONS = ("SELL", "SELL_PARTIAL", "SELL_FULL")
NON_CANONICAL_OUTCOME_BASES = ("legacy_external",)


@dataclass(frozen=True)
class InferencePolicy:
    bootstrap_resamples: int = 2000
    bootstrap_seed: int = 20261125
    confidence_level: float = 0.95


DEFAULT_POLICY = InferencePolicy()


def horizon_block_length(horizon: Any) -> int:
    """Block length in distinct decision dates for a calendar-day horizon.

    Outcomes use calendar days; decisions happen on trading days (~5 of 7), so a
    5d outcome overlaps with decisions of about the next 4 trading dates.
    """
    match = re.search(r"\d+", str(horizon))
    days = int(match.group()) if match else 5
    return max(1, int(round(days * 5 / 7)))


def decision_dates(values: Any) -> pd.Series:
    """Local (ART) calendar date of each decision as 'YYYY-MM-DD'; missing -> None."""
    series = pd.Series(values)
    parsed = pd.to_datetime(series, utc=True, errors="coerce")
    labels = parsed.dt.tz_convert(ART).dt.strftime("%Y-%m-%d")
    return labels.where(parsed.notna(), None)


def aligned_scores(scores: Any, decisions: Any) -> pd.Series:
    """Score in the direction of the decision.

    Outcomes are stored directional (a SELL wins when the price falls) while a
    SELL is taken on a negative score. Correlating the raw score with a
    directional outcome mixes signs; the score has to be flipped for sells.
    """
    score = pd.to_numeric(pd.Series(scores), errors="coerce")
    side = pd.Series(decisions, index=score.index).astype("string").str.upper().str.strip()
    return score.where(~side.isin(SELL_ACTIONS), -score)


def canonical_outcome_mask(frame: pd.DataFrame) -> pd.Series:
    """Rows whose outcome basis is canonical (or not recorded, for old rows)."""
    if "outcome_basis" not in frame.columns:
        return pd.Series(True, index=frame.index)
    basis = frame["outcome_basis"].astype("string").fillna("").str.strip().str.lower()
    return ~basis.isin(NON_CANONICAL_OUTCOME_BASES)


def _clean(dates: Any, *columns: Any) -> tuple[np.ndarray, list[np.ndarray]]:
    frame = pd.DataFrame({"date": pd.Series(dates).reset_index(drop=True)})
    for i, column in enumerate(columns):
        frame[f"c{i}"] = pd.to_numeric(pd.Series(column).reset_index(drop=True), errors="coerce")
    frame = frame.replace([np.inf, -np.inf], np.nan).dropna()
    return frame["date"].astype(str).to_numpy(), [frame[f"c{i}"].to_numpy(dtype=float) for i in range(len(columns))]


def mean_interval(dates: Any, values: Any, horizon: Any, policy: InferencePolicy = DEFAULT_POLICY) -> dict:
    labels, (vals,) = _clean(dates, values)
    return interval(labels, vals, policy, horizon_block_length(horizon))


def rank_ic_interval(
    dates: Any,
    scores: Any,
    values: Any,
    horizon: Any,
    policy: InferencePolicy = DEFAULT_POLICY,
) -> dict:
    labels, (score, vals) = _clean(dates, scores, values)
    if len(vals) < 3:
        return {"estimate": None, "lower": None, "upper": None, "p_value": None,
                "n_dates": len(set(labels)), "reason_code": "INSUFFICIENT_ROWS"}
    return interval(labels, vals, policy, horizon_block_length(horizon), scores=score)


def difference_interval(
    dates_a: Any,
    values_a: Any,
    dates_b: Any,
    values_b: Any,
    horizon: Any,
    policy: InferencePolicy = DEFAULT_POLICY,
) -> dict:
    """Mean(a) - mean(b), resampling the union of decision dates in blocks."""
    labels_a, (a,) = _clean(dates_a, values_a)
    labels_b, (b,) = _clean(dates_b, values_b)
    block = horizon_block_length(horizon)
    labels = np.unique(np.concatenate([labels_a, labels_b])) if len(labels_a) + len(labels_b) else np.array([])
    result = {
        "estimate": float(a.mean() - b.mean()) if len(a) and len(b) else None,
        "lower": None, "upper": None, "p_value": None,
        "block_length": block, "n_dates": len(labels), "reason_code": None,
    }
    if result["estimate"] is None or len(labels) < 2 * block:
        result["reason_code"] = "INSUFFICIENT_DATE_BLOCKS_OR_VARIATION"
        return result
    index = {label: i for i, label in enumerate(labels)}
    codes_a = np.array([index[x] for x in labels_a], dtype=int)
    codes_b = np.array([index[x] for x in labels_b], dtype=int)
    sum_a = np.bincount(codes_a, weights=a, minlength=len(labels))
    cnt_a = np.bincount(codes_a, minlength=len(labels))
    sum_b = np.bincount(codes_b, weights=b, minlength=len(labels))
    cnt_b = np.bincount(codes_b, minlength=len(labels))
    draws = []
    for sampled in date_draws(len(labels), block, policy.bootstrap_resamples, policy.bootstrap_seed):
        na, nb = cnt_a[sampled].sum(), cnt_b[sampled].sum()
        if na and nb:
            draws.append(sum_a[sampled].sum() / na - sum_b[sampled].sum() / nb)
    if len(draws) < 0.9 * policy.bootstrap_resamples:
        result["reason_code"] = "INSUFFICIENT_VALID_RESAMPLES"
        return result
    draws = np.asarray(draws)
    alpha = (1 - policy.confidence_level) / 2
    result["lower"], result["upper"] = map(float, np.quantile(draws, [alpha, 1 - alpha]))
    estimate = result["estimate"]
    result["p_value"] = float((1 + np.sum(draws - estimate >= estimate)) / (len(draws) + 1))
    return result


def evidence(result: Optional[dict]) -> Optional[bool]:
    """True: interval above zero. False: interval below zero. None: crosses zero or unknown."""
    if not result:
        return None
    lower, upper = result.get("lower"), result.get("upper")
    if lower is None or upper is None:
        return None
    if lower > 0:
        return True
    if upper < 0:
        return False
    return None


def rank_ic(scores: Any, values: Any) -> Optional[float]:
    score = pd.to_numeric(pd.Series(scores).reset_index(drop=True), errors="coerce")
    vals = pd.to_numeric(pd.Series(values).reset_index(drop=True), errors="coerce")
    data = pd.concat([score, vals], axis=1).replace([np.inf, -np.inf], np.nan).dropna()
    if len(data) < 5:
        return None
    value = spearman(data.iloc[:, 0].to_numpy(), data.iloc[:, 1].to_numpy())
    return value if value is not None and math.isfinite(value) else None


def fmt_interval(result: Optional[dict], fmt) -> str:
    """'[a; b] al 95%'. Not called "IC95" on purpose: IC already means the score correlation."""
    if not result or result.get("lower") is None or result.get("upper") is None:
        return "rango 95% n/d"
    return f"[{fmt(result['lower'])}; {fmt(result['upper'])}] al 95%"

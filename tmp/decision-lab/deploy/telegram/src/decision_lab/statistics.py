"""Paired account-level DVA inference; exploratory segments never create rules."""

from collections import defaultdict, Counter
from types import SimpleNamespace

import numpy as np

# Extracted without changing semantics from the existing Analytics v2 helper.
from src.analysis.date_block_statistics import interval, benjamini_hochberg
from .models import digest

GROUP = (
    "owner",
    "strategy_version",
    "strategy_hash",
    "mode",
    "experiment_id",
    "split",
    "cost_model_hash",
    "return_basis",
    "horizon",
)
SEGMENTS = (
    "decision_type",
    "score_bucket",
    "conviction_bucket",
    "vix_regime",
    "macro_regime",
    "argentina_regime",
    "sector",
    "ticker",
    "planner_guard",
    "risk_regime",
    "radar_status",
    "market_trend",
    "volatility_regime",
)


def summarize(rows, experiment, *, primary=True):
    """One paired delta per account opportunity. Not a trade-weighted metric."""
    candidates = [
        r
        for r in rows
        if r.get("dva") is not None
        and r["status"] == "MATURE"
        and (r.get("primary_eligible") or not primary)
        and r.get("quality") != "INVALID"
    ]
    unique = {}
    for row in candidates:
        key = row["opportunity_id"]
        if key in unique and unique[key] != row:
            raise ValueError("conflicting paired outcomes for one opportunity")
        unique[key] = row
    accepted = sorted(unique.values(), key=lambda r: (r["as_of"], r["episode_id"]))
    values = np.asarray([r["dva"] for r in accepted], dtype=float)
    if not np.isfinite(values).all():
        raise ValueError("nonfinite DVA")
    # Greedy disjoint account windows. A conservative reporting proxy, never
    # an assertion of independence across tickers or market regimes.
    effective, end = 0, None
    for r in sorted(accepted, key=lambda r: (r["exit_at"], r["entry_at"])):
        if end is None or r["entry_at"] > end:
            effective += 1
            end = r["exit_at"]
    block = (
        max(experiment.bootstrap_block_sessions, int(rows[0]["horizon"]))
        if rows
        else experiment.bootstrap_block_sessions
    )
    policy = SimpleNamespace(
        bootstrap_resamples=experiment.bootstrap_resamples,
        bootstrap_seed=experiment.seed,
        confidence_level=experiment.confidence,
    )
    ci = {
        "estimate": float(values.mean()) if len(values) else None,
        "lower": None,
        "upper": None,
        "p_value": None,
        "reason_code": (
            "INSUFFICIENT_PRIMARY_SAMPLE"
            if primary
            else "EXPLORATORY_QUALITY_NO_INFERENCE"
        ),
        "seed": experiment.seed,
        "resamples": experiment.bootstrap_resamples,
        "block_length": block,
    }
    sensitivity = []
    if primary and len(values) >= experiment.minimum_n and effective >= 2:
        dates = [r["as_of"][:10] for r in accepted]
        ci = interval(dates, values, policy, block)
        sensitivity = [interval(dates, values, policy, b) for b in (5, 10, 20, 40)]
    interpretation = "INSUFFICIENT"
    if len(values):
        interpretation = "DESCRIPTIVE_ONLY" if not primary else "INCONCLUSIVE"
        if ci.get("lower") is not None:
            interpretation = (
                "FAVORABLE_INTERVAL"
                if ci["lower"] > 0
                else "UNFAVORABLE_INTERVAL" if ci["upper"] < 0 else "INCONCLUSIVE"
            )
    positive = np.sort(values[values > 0])[::-1]
    denom = float(positive.sum())
    result = {
        "population": "PRIMARY" if primary else "EXPLORATORY_ALL_ADMISSIBLE",
        "n_raw": len(rows),
        "n": len(values),
        "n_dates": len({r["as_of"][:10] for r in accepted}),
        "n_effective": effective,
        "n_effective_method": "GREEDY_NONOVERLAPPING_ACCOUNT_WINDOWS_PROXY",
        "status_counts": dict(sorted(Counter(r["status"] for r in rows).items())),
        "quality_counts": dict(sorted(Counter(r["quality"] for r in rows).items())),
        "mean": float(values.mean()) if len(values) else None,
        "median": float(np.median(values)) if len(values) else None,
        "std": float(values.std(ddof=1)) if len(values) > 1 else None,
        "win_rate_vs_hold": float((values > 0).mean()) if len(values) else None,
        "quantiles": (
            dict(
                zip(
                    ("p10", "p25", "p75", "p90"),
                    map(float, np.quantile(values, [0.1, 0.25, 0.75, 0.9])),
                )
            )
            if len(values)
            else {}
        ),
        "plan_mean": (
            float(np.mean([r["plan_return"] for r in accepted])) if accepted else None
        ),
        "hold_mean": (
            float(np.mean([r["hold_return"] for r in accepted])) if accepted else None
        ),
        "cost_drag_mean": (
            float(np.mean([r["cost_drag"] for r in accepted])) if accepted else None
        ),
        "turnover_mean": (
            float(np.mean([r["turnover"] for r in accepted])) if accepted else None
        ),
        "top1_positive_share": float(positive[:1].sum() / denom) if denom else None,
        "top3_positive_share": float(positive[:3].sum() / denom) if denom else None,
        "dva_without_top1": (
            float(np.sort(values)[:-1].mean()) if len(values) > 1 else None
        ),
        "ci": ci,
        "block_sensitivity": sensitivity,
        "interpretation": interpretation,
        "sharpe": None,
        "drawdown": None,
        "risk_reason": "OVERLAPPING_ACCOUNT_EPISODES_ARE_NOT_A_CONTINUOUS_EQUITY_CURVE",
    }
    return result


def metrics(rows, experiment, *, include_segments=True):
    grouped = defaultdict(list)
    for r in rows:
        grouped[tuple(r[k] for k in GROUP)].append(r)
    result = []
    for key, cohort in sorted(grouped.items()):
        meta = dict(zip(GROUP, key))
        for primary in (True, False):
            summary = {
                **meta,
                "segment": "ALL",
                **summarize(cohort, experiment, primary=primary),
            }
            summary["hypothesis_id"] = digest([meta, "DVA_MEAN_POSITIVE"])
            result.append(summary)
        if include_segments:
            for field in SEGMENTS:
                values = sorted(
                    {str(r.get("segments", {}).get(field, "UNKNOWN")) for r in cohort}
                )
                for value in values:
                    if value == "UNKNOWN":
                        continue
                    subset = [
                        r
                        for r in cohort
                        if str(r.get("segments", {}).get(field, "UNKNOWN")) == value
                    ]
                    result.append(
                        {
                            **meta,
                            "segment": field + "=" + value,
                            "multiple_testing": "EXPLORATORY_NOT_A_RULE",
                            **summarize(subset, experiment, primary=False),
                        }
                    )
        if include_segments:
            for ticker in sorted({t for r in cohort for t in r.get("tickers", [])}):
                subset = [r for r in cohort if ticker in r.get("tickers", [])]
                for primary in (True, False):
                    result.append(
                        {
                            **meta,
                            "segment": "contains_ticker=" + ticker,
                            "multiple_testing": "EXPLORATORY_ACCOUNT_EPISODES_CONTAINING_TICKER_NOT_TICKER_ATTRIBUTION",
                            **summarize(subset, experiment, primary=primary),
                        }
                    )
    registered = {h: None for h in experiment.family}
    for r in result:
        if (
            r.get("hypothesis_id") in registered
            and r["population"] == "PRIMARY"
            and experiment.confirmatory
        ):
            registered[r["hypothesis_id"]] = r["ci"].get("p_value")
    q = benjamini_hochberg(registered)
    for r in result:
        h = r.get("hypothesis_id")
        r.update(
            hypothesis_family=list(experiment.family),
            raw_p=r["ci"].get("p_value"),
            q_value=(
                q.get(h)
                if experiment.confirmatory and r["population"] == "PRIMARY"
                else None
            ),
            family_size=len(registered),
            confirmatory=bool(
                experiment.confirmatory
                and h in registered
                and r["population"] == "PRIMARY"
            ),
        )
    return result


def compare_versions(rows, version_a, version_b, experiment):
    """Inner exact match; rejected/missing pairs are counted, not zero filled."""
    keys = (
        "owner",
        "opportunity_id",
        "state_id",
        "mode",
        "split",
        "horizon",
        "cost_model_hash",
        "return_basis",
        "entry_at",
        "exit_at",
    )
    populations = {version_a: {}, version_b: {}}
    for row in rows:
        if row["strategy_version"] in populations and row["status"] == "MATURE":
            key = tuple(row[k] for k in keys)
            target = populations[row["strategy_version"]]
            if key in target and target[key] != row:
                raise ValueError("ambiguous strategy version pair")
            target[key] = row
    a, b = populations[version_a], populations[version_b]
    paired = []
    for key in sorted(a.keys() & b.keys()):
        left, right = a[key], b[key]
        if (
            left["capital_base_ars"] != right["capital_base_ars"]
            or left["hold_return"] != right["hold_return"]
        ):
            raise ValueError("strategy comparison requires common capital and HOLD")
        paired.append(
            {
                **right,
                "dva": right["plan_return"] - left["plan_return"],
                "primary_eligible": left["primary_eligible"]
                and right["primary_eligible"],
                "quality": (
                    "HIGH"
                    if left["primary_eligible"] and right["primary_eligible"]
                    else "LOW"
                ),
                "strategy_version": version_b + " minus " + version_a,
                "plan_return": right["plan_return"],
                "hold_return": left["plan_return"],
            }
        )
    return {
        "version_a": version_a,
        "version_b": version_b,
        "n_unmatched_a": len(a.keys() - b.keys()),
        "n_unmatched_b": len(b.keys() - a.keys()),
        "n_pairs": len(paired),
        "metrics": metrics(paired, experiment, include_segments=False),
        "pairs": paired,
    }

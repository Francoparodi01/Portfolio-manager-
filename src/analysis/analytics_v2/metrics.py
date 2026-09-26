"""Descriptive metrics on mature episodes, never on repeated plans."""
import math

import numpy as np
import pandas as pd

from .bootstrap import interval, spearman


def distribution(values):
    a = np.asarray(values, dtype=float)
    if not len(a):
        return {k: None for k in ("ev_net", "median_net", "p10", "p25", "p75", "p90", "std", "win_rate",
                                  "avg_winner", "avg_loser", "payoff_ratio", "profit_factor_unit_notional",
                                  "top1_positive", "top3_positive", "top5_positive", "ev_without_top1",
                                  "ev_without_top3", "trimmed_ev_5pct", "skew")}
    if not np.isfinite(a).all():
        raise ValueError("nonfinite returns")
    wins, losses = a[a > 0], a[a < 0]
    positive = np.sort(wins)[::-1]
    denom = float(positive.sum())
    ordered = np.sort(a)
    trim = math.floor(.05 * len(a))
    std = float(a.std(ddof=1)) if len(a) > 1 else None
    result = {"ev_net": float(a.mean()), "median_net": float(np.median(a)),
              "std": std, "win_rate": float((a > 0).mean()),
              "avg_winner": float(wins.mean()) if len(wins) else None,
              "avg_loser": float(losses.mean()) if len(losses) else None,
              "payoff_ratio": float(wins.mean() / abs(losses.mean())) if len(wins) and len(losses) else None,
              "profit_factor_unit_notional": float(wins.sum() / abs(losses.sum())) if len(losses) else None,
              "ev_without_top1": float(ordered[:-1].mean()) if len(a) > 1 else None,
              "ev_without_top3": float(ordered[:-3].mean()) if len(a) > 3 else None,
              "trimmed_ev_5pct": float(ordered[trim:len(a)-trim].mean()),
              "skew": float(np.mean(((a - a.mean()) / a.std()) ** 3)) if a.std() > 0 else None}
    result.update({f"top{k}_positive": float(positive[:k].sum() / denom) if denom else None for k in (1, 3, 5)})
    result.update(dict(zip(("p10", "p25", "p75", "p90"), map(float, np.quantile(a, [.1, .25, .75, .9])))))
    return result


def dependence(frame, horizon, block_length):
    if frame.empty:
        return [], 0, 0
    clusters, count = {}, 0
    for _, group in frame.groupby("instrument_id", sort=True):
        end = None
        for index, start, finish in group.sort_values(["entry_at", "episode_id"])[["entry_at", "maturity_at"]].itertuples():
            if end is None or start > end:
                count += 1
            end = max(end or finish, finish)
            clusters[index] = count
    # Conservative temporal-block proxy; not an IID effective sample estimate.
    dates = frame.decision_as_of.str[:10].nunique()
    effective = min(count, dates // max(horizon, block_length))
    return [clusters[i] for i in frame.index], count, effective


def cohort_metrics(outcomes, policy, *, inference_enabled=True):
    if not outcomes:
        return [], [], [], []
    df = pd.DataFrame(outcomes)
    metrics, inference, concentrations, calibrations = [], [], [], []
    keys = ["account_id", "source_module", "cohort", "horizon_days", "cost_scenario"]
    for key, all_rows in df.groupby(keys, sort=True):
        meta = dict(zip(keys, key))
        mature = all_rows[all_rows.outcome_status.eq("MATURE")].copy()
        value_col = "net_alpha" if meta["source_module"] == "SWAP" else "net_return"
        mature = mature[mature[value_col].notna()].copy()
        a = mature[value_col].to_numpy(dtype=float)
        clusters, count, effective = dependence(mature, meta["horizon_days"], policy.bootstrap_block_length)
        mature["cluster"] = clusters
        status_counts = all_rows.outcome_status.value_counts()
        eligible_count = len(all_rows) - int(status_counts.get("EXCLUDED_POLICY", 0))
        stats = distribution(a)
        row = {**meta, **stats, "n_raw": int(mature.recommendation_count.sum()),
               "n_recommendations": int(all_rows.recommendation_count.sum()), "n_episodes": len(mature),
               "n_effective": effective, "cluster_count": count,
               "n_dates": int(mature.decision_as_of.str[:10].nunique()),
               "maturity_coverage": len(mature) / eligible_count if eligible_count else 0,
               "gross_ev": float(mature.gross_alpha.mean() if meta["source_module"] == "SWAP" else mature.gross_return.mean()) if len(mature) else None,
               "gross_median": float(mature.gross_return.median()) if len(mature) else None,
               "metric_unit": "swap_incremental_alpha" if meta["source_module"] == "SWAP" else "equal_opportunity_return",
               "risk_reason": "NO_COMPARABLE_SIZED_EQUITY_CURVE", "max_drawdown": None,
               "sharpe": None, "sortino": None, "volatility": None,
               "cost_bps": float(all_rows.cost_bps.iloc[0])}
        for status in ("MATURE", "PENDING", "UNAVAILABLE", "AMBIGUOUS", "EXCLUDED_POLICY"):
            row[f"n_{status.lower()}"] = int(status_counts.get(status, 0))
        row["break_even_cost_bps"] = row["gross_ev"] * 10000 if row["gross_ev"] is not None else None
        weights = mature.reference_notional
        row["capital_weighted_cf_return"] = float(np.average(a, weights=weights)) if len(mature) and weights.notna().all() else None
        row["capital_weighted_reason"] = None if row["capital_weighted_cf_return"] is not None else "REFERENCE_NOTIONAL_UNAVAILABLE"
        valid_ic = mature.dropna(subset=["score", "gross_alpha"])
        row["ic"] = spearman(valid_ic.score, valid_ic.gross_alpha)
        row["ic_n"] = len(valid_ic)
        alpha = mature.gross_alpha.dropna()
        row["mean_alpha"] = float(alpha.mean()) if len(alpha) else None
        row["median_alpha"] = float(alpha.median()) if len(alpha) else None
        row["positive_alpha_fraction"] = float((alpha > 0).mean()) if len(alpha) else None
        row["alpha_n"] = len(alpha)
        daily_ics = [v for _, group in valid_ic.groupby(valid_ic.decision_as_of.str[:10])
                     if (v := spearman(group.score, group.gross_alpha)) is not None]
        row["cross_sectional_ic_mean"] = float(np.mean(daily_ics)) if daily_ics else None
        row["cross_sectional_ic_median"] = float(np.median(daily_ics)) if daily_ics else None
        row["cross_sectional_ic_dates"] = len(daily_ics)
        monthly = mature.groupby(mature.decision_as_of.str[:7])[value_col].mean()
        row["positive_periods"] = int((monthly > 0).sum())
        row["total_periods"] = len(monthly)
        ticker_wins = mature.assign(positive=mature[value_col].clip(lower=0)).groupby("instrument_id").positive.sum().sort_values(ascending=False)
        denom = float(ticker_wins.sum())
        row["top1_ticker"] = float(ticker_wins.iloc[:1].sum() / denom) if denom else None
        row["top3_ticker"] = float(ticker_wins.iloc[:3].sum() / denom) if denom else None
        group_sums = mature.groupby("cluster")[value_col].agg(["sum", "count"])
        loo = [(a.sum() - r.sum) / (len(a) - r.count) for r in group_sums.itertuples() if r.count < len(a)]
        row["leave_one_cluster_out_worst_ev"] = float(min(loo)) if loo else None
        row["average_episode_duration_days"] = float(((pd.to_datetime(mature.maturity_at) - pd.to_datetime(mature.entry_at)).dt.total_seconds() / 86400).mean()) if len(mature) else None
        if len(valid_ic):
            bins = np.minimum(4, (valid_ic.score.rank(method="average", pct=True) * 5).astype(int))
            for bucket, group in valid_ic.groupby(bins):
                calibrations.append({**meta, "score_bucket": int(bucket), "n": len(group), "mean_score": float(group.score.mean()), "mean_alpha": float(group.gross_alpha.mean())})
        if meta["cost_scenario"] == "RESEARCH_BASE" and inference_enabled:
            for block in sorted(set((*policy.block_sensitivity, policy.bootstrap_block_length))):
                for name, sample, column, scores in (("ev", mature, value_col, None), ("ic", valid_ic, "gross_alpha", valid_ic.score)):
                    ci = interval(sample.decision_as_of.str[:10], sample[column], policy, block, scores=scores)
                    inference.append({**meta, "metric": name, **ci})
                    if block == policy.bootstrap_block_length:
                        row.update({f"{name}_{k}": ci[k] for k in ("lower", "upper", "p_value")})
            primary = [i for i in inference if all(i[k] == meta[k] for k in keys) and i["metric"] == "ev"]
            row["inference_stable"] = bool(primary) and all(i["lower"] is not None and i["lower"] > 0 for i in primary)
            available_signs = {i["lower"] > 0 for i in primary if i["lower"] is not None}
            row["inference_stability_status"] = "INFERENCE_UNSTABLE" if len(available_signs) > 1 else "STABLE_POSITIVE" if row["inference_stable"] else "INSUFFICIENT_OR_NONPOSITIVE_BLOCK_EVIDENCE"
        for item in mature.sort_values([value_col, "episode_id"], ascending=[False, True]).to_dict("records"):
            concentrations.append({**meta, "episode_id": item["episode_id"], "ticker": item["ticker"],
                                   "original_instrument_id": item["original_instrument_id"],
                                   "contribution_unit_notional": item[value_col], "cluster": item["cluster"]})
        metrics.append(row)
    return metrics, inference, concentrations, calibrations


def equity_metrics(returns, *, periods_per_year=252, risk_free_per_period=0.0, mar_per_period=0.0, hac_lags=5):
    r = np.asarray(returns, dtype=float)
    if len(r) < 2 or not np.isfinite(r).all() or np.any(r <= -1):
        return {"reason_code": "INVALID_OR_INCOMPLETE_EQUITY_RETURNS"}
    equity = np.r_[1., np.cumprod(1 + r)]
    dd = float(np.min(equity / np.maximum.accumulate(equity) - 1))
    excess = r - risk_free_per_period
    centered = excess - excess.mean()
    lags = min(hac_lags, len(r) - 1)
    variance = float(np.dot(centered, centered) / len(r))
    for lag in range(1, lags + 1):
        variance += 2 * (1 - lag / (lags + 1)) * float(np.dot(centered[lag:], centered[:-lag]) / len(r))
    downside = float(np.sqrt(np.mean(np.minimum(r - mar_per_period, 0) ** 2)))
    return {"max_drawdown": dd, "sharpe_hac": float(excess.mean() / np.sqrt(variance) * np.sqrt(periods_per_year)) if variance > 0 else None,
            "sortino": float((r.mean() - mar_per_period) / downside * np.sqrt(periods_per_year)) if downside else None,
            "volatility": float(r.std(ddof=1) * np.sqrt(periods_per_year)), "periods_per_year": periods_per_year,
            "risk_free_per_period": risk_free_per_period, "mar_per_period": mar_per_period, "hac_lags": lags}

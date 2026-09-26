"""Governance outputs only. Nothing here enables an order or changes a strategy."""
from .bootstrap import benjamini_hochberg


def hypothesis(row, metric):
    return "/".join(map(str, (row["account_id"], row["source_module"], row["cohort"], row["horizon_days"], metric)))


def build_gates(metrics, policy):
    family = {name: None for name in policy.primary_hypotheses}
    base = [r for r in metrics if r["cost_scenario"] == "RESEARCH_BASE"]
    for row in base:
        for metric in ("ev", "ic"):
            key = hypothesis(row, metric)
            if key in family:
                family[key] = row.get(f"{metric}_p_value")
    q = benjamini_hochberg(family)
    hypotheses = [{"hypothesis": k, "hypothesis_family": policy.experiment_id, "raw_p": family[k], "q_value": q[k], "family_size": len(family)} for k in sorted(family)]
    results = []
    cfg = policy.gates
    for row in base:
        reasons, status = [], "OBSERVE"
        stress = next(r for r in metrics if r["cost_scenario"] == "STRESS" and all(r[k] == row[k] for k in ("account_id", "source_module", "cohort", "horizon_days")))
        swap = row["source_module"] == "SWAP"
        sample = row["n_raw"] >= cfg.reportable and row["n_episodes"] >= cfg.reportable and row["n_effective"] >= cfg.provisional_effective and row["maturity_coverage"] >= cfg.minimum_coverage
        concentration = row["top1_positive"] is not None and row["top1_positive"] <= cfg.max_top1 and row["top3_positive"] <= cfg.max_top3
        positive = row["ev_net"] is not None and row["ev_net"] > 0 and (swap or row["ic"] is not None and row["ic"] > 0)
        risk_ok = row.get("shadow_max_drawdown") is None or abs(min(0, row["shadow_max_drawdown"])) <= cfg.max_drawdown
        data_ok = row["n_unavailable"] == 0 and row["n_ambiguous"] == 0
        if swap and not cfg.swaps_revalidated:
            status, reasons = "DISABLED_SHADOW", ["SWAP_REVALIDATION_REQUIRED"]
        elif not sample:
            status, reasons = "IMMATURE", ["SAMPLE_OR_MATURITY_GATE"]
        elif not data_ok:
            status, reasons = "OBSERVE", ["DATA_QUALITY_GATE"]
        elif not positive or not concentration or not risk_ok:
            status, reasons = "FAIL", ["EV_IC_CONCENTRATION_OR_DRAWDOWN_GATE"]
        else:
            status = "PROVISIONAL_GUARDED"
            checks = {
                "CONFIRMED_SAMPLE": row["n_episodes"] >= cfg.confirmed_episodes and row["n_effective"] >= cfg.confirmed_effective,
                "EV_CI": row["ev_net"] >= cfg.minimum_ev and row.get("ev_lower") is not None and row["ev_lower"] > 0,
                "IC_CI": swap or row["ic"] >= cfg.minimum_ic and row.get("ic_lower") is not None and row["ic_lower"] > 0,
                "STRESS": stress["ev_net"] is not None and stress["ev_net"] > 0,
                "INFERENCE_STABLE": row.get("inference_stable", False),
                "SHADOW_DRAWDOWN": row.get("shadow_max_drawdown") is None or abs(min(0, row["shadow_max_drawdown"])) <= cfg.max_drawdown,
                "TEMPORAL_STABILITY": row["positive_periods"] >= cfg.stable_periods and row["positive_periods"] == row["total_periods"],
                "PREREGISTERED_BH": all(q.get(hypothesis(row, m)) is not None and q[hypothesis(row, m)] <= cfg.max_q for m in (("ev",) if swap else ("ev", "ic"))),
            }
            reasons = [name for name, passed in checks.items() if not passed]
            if not reasons:
                status = "CONFIRMED"
        eligible = (status == "CONFIRMED" and row.get("shadow_max_drawdown") is not None
                    and (row["source_module"] != "RADAR" or row["cohort"] == "OPERABLE"))
        results.append({k: row[k] for k in ("account_id", "source_module", "cohort", "horizon_days")} | {
            "status": status, "reasons": "|".join(reasons), "gate_policy_version": cfg.version,
            "hypothesis_family": policy.experiment_id if family else None,
            "ev_q": q.get(hypothesis(row, "ev")), "ic_q": q.get(hypothesis(row, "ic")),
            "statistical_evidence_confirmed": status == "CONFIRMED",
            "capital_authority": False, "authority_eligible_for_review": eligible,
            "authority_reason": "AUDIT_ONLY" if eligible else "AUDIT_ONLY_NO_CONFIRMED_COMPARABLE_EQUITY_GATE",
        })
    return results, hypotheses

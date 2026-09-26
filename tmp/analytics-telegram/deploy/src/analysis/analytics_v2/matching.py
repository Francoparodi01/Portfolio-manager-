"""Exact opportunity matching; observational decisions, no causal claims."""
from .bootstrap import interval
from .episodes import unique_rows
from datetime import datetime
from .models import digest


def execution_decomposition(dataset, out):
    """Only a fully identified long round-trip; no guessing across episodes."""
    missing = {"actual_execution_return": None, "timing_delta": None, "sizing_delta": None,
               "execution_delta": None, "cost_delta": None, "reference_normalized_actual_return": None,
               "economic_trade_id": None, "execution_reason": "IDENTIFIED_LONG_ROUND_TRIP_UNAVAILABLE"}
    if out["outcome_status"] != "MATURE" or out["direction"] != "BUY":
        return missing
    fills = sorted([f for f in unique_rows(dataset.executions, lambda f: f.broker_fill_id)
                    if f.recommendation_id == out["recommendation_id"] and f.account_id == out["account_id"]
                    and f.instrument_id == out["instrument_id"]
                    and datetime.fromisoformat(out["entry_at"]) <= f.fill_at <= datetime.fromisoformat(out["maturity_at"])], key=lambda f: (f.fill_at, f.broker_fill_id))
    buys, sells = [f for f in fills if f.side == "BUY"], [f for f in fills if f.side == "SELL"]
    if not buys or not sells or max(f.fill_at for f in buys) >= min(f.fill_at for f in sells):
        return missing
    qty = sum(f.quantity for f in buys)
    if abs(qty - sum(f.quantity for f in sells)) > 1e-8 or len({f.currency for f in fills}) != 1:
        return missing
    if any(f.reference_price is None or f.reference_kind != "EXECUTION_MARKET" or
           any(v is None for v in (f.fees, f.taxes, f.other_costs)) for f in fills):
        return {**missing, "execution_reason": "FILL_COSTS_OR_CONTEMPORANEOUS_REFERENCE_UNAVAILABLE"}
    buy_notional = sum(f.quantity * f.fill_price for f in buys)
    gross_profit = sum(f.quantity * f.fill_price for f in sells) - buy_notional
    reference_profit = sum(f.quantity * f.reference_price for f in sells) - sum(f.quantity * f.reference_price for f in buys)
    costs = sum(f.fees + f.taxes + f.other_costs for f in fills)
    reference_capital = qty * out["entry_reference_price"]
    return {**missing, "actual_execution_return": (gross_profit - costs) / buy_notional,
            "reference_normalized_actual_return": (gross_profit - costs) / reference_capital,
            "execution_delta": (gross_profit - reference_profit) / reference_capital,
            "timing_delta": reference_profit / reference_capital - out["gross_return"],
            "cost_delta": out["cost_bps"] / 10000 - costs / reference_capital,
            "economic_trade_id": digest([f.broker_fill_id for f in fills]), "execution_reason": None}


def match_actions(dataset, outcomes, policy):
    actions = unique_rows(dataset.human_actions, lambda a: a.recommendation_id)
    actions = {a.recommendation_id: a for a in actions}
    rows = []
    for out in outcomes:
        if out["source_module"] != "CORE":
            continue
        action = actions.get(out["recommendation_id"])
        reason = None
        if action is None:
            reason = "HUMAN_ACTION_UNAVAILABLE"
        elif action.ambiguous:
            reason = action.ambiguity_reason or "AMBIGUOUS_ACTION"
        elif action.instrument_id != out["instrument_id"] or action.decision_as_of.isoformat() != out["decision_as_of"]:
            reason = "INFORMATION_SET_MISMATCH"
        elif action.action in {"MODIFIED", "UNKNOWN"}:
            reason = "MODIFIED_OR_UNKNOWN_ACTION_NOT_IDENTIFIABLE"
        elif out["outcome_status"] != "MATURE":
            reason = out["reason_code"] or out["outcome_status"]
        human = None
        if reason is None:
            cost = out["cost_bps"] / 10000
            human = {"FOLLOW": out["net_return"], "IGNORE": 0., "CONTRARY": -out["gross_return"] - cost}[action.action]
        bot = out["net_return"] if reason is None else None
        rows.append({"match_id": digest([out["episode_id"], out["horizon_days"], out["cost_scenario"], "match-v2"]),
                     "episode_id": out["episode_id"], "account_id": out["account_id"], "cohort": out["cohort"],
                     "horizon_days": out["horizon_days"], "cost_scenario": out["cost_scenario"],
                     "decision_as_of": out["decision_as_of"], "entry_at": out["entry_at"], "maturity_at": out["maturity_at"],
                     "bot_action": out["direction"], "human_action": action.action if action else "UNKNOWN",
                     "comparable": reason is None, "ambiguous": bool(action and action.ambiguous), "reason_code": reason,
                     "bot_cf_return": bot, "human_cf_return": human,
                     "paired_delta_bot_minus_human": bot - human if reason is None else None,
                     "selection_delta_human_minus_bot": human - bot if reason is None else None,
                     "delta_scope": "local_decision_delta" if action and action.action == "IGNORE" else "same_window_unit_notional",
                     **execution_decomposition(dataset, out),
                     "input_hash": digest([out["input_hash"], digest(action) if action else None])})
    return rows


def paired_inference(matches, policy):
    groups = {}
    for row in matches:
        if row["comparable"] and row["cost_scenario"] == "RESEARCH_BASE":
            groups.setdefault((row["account_id"], row["cohort"], row["horizon_days"]), []).append(row)
    results = []
    for (account, cohort, horizon), rows in sorted(groups.items()):
        for block in sorted(set((*policy.block_sensitivity, policy.bootstrap_block_length))):
            ci = interval([r["decision_as_of"][:10] for r in rows], [r["paired_delta_bot_minus_human"] for r in rows], policy, block)
            results.append({"account_id": account, "cohort": cohort, "horizon_days": horizon,
                            "metric": "paired_bot_minus_human", "n_pairs": len(rows), **ci})
    return results

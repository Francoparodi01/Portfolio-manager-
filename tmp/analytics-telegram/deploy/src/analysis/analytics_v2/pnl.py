"""NAV reconciliation and observed execution shortfall in separate estimands."""
from .episodes import unique_rows
from .metrics import equity_metrics
from .models import digest


def execution_facts(dataset, links):
    lookup = {r["recommendation_id"]: r["episode_id"] for r in links}
    rows = []
    for fill in sorted(unique_rows(dataset.executions, lambda f: f.broker_fill_id), key=lambda f: (f.fill_at, f.broker_fill_id)):
        side = 1 if fill.side == "BUY" else -1
        rows.append({**fill.model_dump(mode="json"), "episode_id": lookup.get(fill.recommendation_id),
                     "link_reason": None if fill.recommendation_id in lookup else "EPISODE_LINK_UNAVAILABLE",
                     "execution_shortfall": -side * fill.quantity * (fill.fill_price - fill.reference_price) if fill.reference_price is not None else None,
                     "shortfall_reason": None if fill.reference_price is not None else "REFERENCE_PRICE_UNAVAILABLE",
                     "source_row_hash": digest(fill)})
    return rows


def economic_pnl(dataset):
    fills = unique_rows(dataset.executions, lambda f: f.broker_fill_id)
    navs = unique_rows(dataset.nav_points, lambda p: (p.account_id, p.kind, p.at))
    result, curves = [], []
    for period in unique_rows(dataset.economic_periods, lambda p: (p.account_id, p.start, p.end)):
        selected = [f for f in fills if f.account_id == period.account_id and period.start < f.fill_at <= period.end]
        points = sorted([p for p in navs if p.account_id == period.account_id and p.kind == "ACTUAL"
                         and period.start <= p.at <= period.end], key=lambda p: p.at)
        reasons = []
        components = (period.realized_pnl, period.unrealized_pnl_change, period.fees, period.taxes, period.financing, period.other_costs)
        if period.net_external_flows is None or any(v is None for v in components):
            reasons.append("MISSING_FLOWS_OR_COMPONENTS")
        if not period.coverage_confirmed or not period.evidence_ids:
            reasons.append("COVERAGE_NOT_ATTESTED")
        if any(f.currency != period.currency for f in selected) or any(p.currency != period.currency for p in points):
            reasons.append("CURRENCY_MISMATCH")
        if not points or points[0].at != period.start or points[-1].at != period.end or not all(p.reconciled for p in points):
            reasons.append("MISSING_RECONCILED_NAV_BOUNDARIES")
        elif abs(points[0].nav - period.beginning_nav) > .01 or abs(points[-1].nav - period.ending_nav) > .01:
            reasons.append("NAV_BOUNDARY_MISMATCH")
        for component in ("fees", "taxes", "other_costs"):
            values = [getattr(f, component) for f in selected]
            if any(v is None for v in values):
                reasons.append(f"MISSING_OBSERVED_{component.upper()}")
            elif getattr(period, component) is not None and abs(sum(values) - getattr(period, component)) > .01:
                reasons.append(f"FILL_{component.upper()}_MISMATCH")
        economic = period.ending_nav - period.beginning_nav - period.net_external_flows if period.net_external_flows is not None else None
        reconciled = components[0] + components[1] - sum(components[2:]) if all(v is not None for v in components) else None
        residual = economic - reconciled if economic is not None and reconciled is not None else None
        if residual is not None and abs(residual) > .01:
            reasons.append("PNL_RECONCILIATION_FAILED")
        complete = not reasons
        shortfalls = [- (1 if f.side == "BUY" else -1) * f.quantity * (f.fill_price - f.reference_price)
                      for f in selected if f.reference_price is not None]
        intervals = [(a, (b.at - a.at).total_seconds()) for a, b in zip(points, points[1:])]
        seconds = sum(duration for _, duration in intervals)
        average_nav = sum(p.nav * duration for p, duration in intervals) / seconds if seconds else None
        result.append({**period.model_dump(mode="json"), "status": "COMPLETE" if complete else "INCOMPLETE",
                       "reason_code": "|".join(reasons) or None,
                       "economic_pnl_net": economic if complete else None, "reconciliation_total": reconciled,
                       "reconciliation_residual": residual, "nav_delta_less_reported_flows": economic,
                       "return_on_beginning_nav": economic / period.beginning_nav if complete and period.net_external_flows == 0 and period.beginning_nav > 0 else None,
                       "return_on_average_capital_deployed": economic / period.average_capital_deployed if complete and period.average_capital_deployed else None,
                       "execution_shortfall": sum(shortfalls) if len(shortfalls) == len(selected) else None,
                       "shortfall_coverage": len(shortfalls) / len(selected) if selected else 1,
                       "turnover_one_way": sum(f.quantity * f.fill_price for f in selected) / average_nav if complete and average_nav else None,
                       "average_gross_exposure": sum(p.gross_notional / p.nav * duration for p, duration in intervals) / seconds if complete and seconds else None,
                       "max_gross_exposure": max(p.gross_notional / p.nav for p in points) if complete and points else None,
                       "average_net_exposure": sum(p.net_notional / p.nav * duration for p, duration in intervals) / seconds if complete and seconds else None,
                       "exposure_weighting": "elapsed_time_between_observed_nav_points", "input_hash": digest([digest(period), [digest(f) for f in selected], [digest(p) for p in points]])})
    groups = {}
    for point in navs:
        groups.setdefault((point.account_id, point.kind, point.sizing_policy, point.currency, point.cohort or "", point.horizon_days or 0), []).append(point)
    for key, points in sorted(groups.items()):
        points.sort(key=lambda p: p.at)
        returns = [p.flow_adjusted_return for p in points[1:]]
        valid = all(p.reconciled for p in points) and all(r is not None for r in returns)
        conventions = {(p.periods_per_year, p.risk_free_per_period, p.mar_per_period) for p in points}
        valid = valid and len(conventions) == 1
        for previous, current in zip(points, points[1:]):
            if current.flow_timing not in {"NONE", "PERIOD_END"} or current.net_external_flow is None:
                valid = False
            elif current.flow_adjusted_return is None or abs((current.nav-current.net_external_flow)/previous.nav-1-current.flow_adjusted_return) > 1e-9:
                valid = False
            elif current.flow_timing == "NONE" and current.net_external_flow != 0:
                valid = False
        p = points[0]
        risk = equity_metrics(returns, periods_per_year=p.periods_per_year, risk_free_per_period=p.risk_free_per_period, mar_per_period=p.mar_per_period) if valid else {"reason_code": "FLOW_ADJUSTED_NAV_RETURNS_UNAVAILABLE"}
        if valid:
            import math
            risk["time_weighted_return"] = math.prod(1 + r for r in returns) - 1
        for point in points:
            curves.append({**point.model_dump(mode="json"), **risk})
    if not result:
        result.append({"status": "INCOMPLETE", "reason_code": "ECONOMIC_NAV_FLOWS_COSTS_NOT_PROVIDED", "economic_pnl_net": None})
    return result, curves

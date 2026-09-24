"""Frozen alternatives and common-capital next-session portfolio accounting."""

from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal, ROUND_FLOOR

from .models import (
    AlternativeOutcome,
    CostModel,
    Episode,
    Experiment,
    FrozenPlan,
    HistoricalState,
    Order,
    canonical,
    digest,
)
from .state import EvidenceIndex, InsufficientEvidence


ZERO = Decimal(0)
ONE = Decimal(1)


def _d(value):
    return Decimal(str(value))


def _orders_json(orders):
    return [o.model_dump(mode="json") for o in orders]


def freeze_episode(
    state: HistoricalState, plan: FrozenPlan, costs: CostModel, experiment: Experiment
) -> Episode:
    if plan.state_id != state.state_id:
        raise ValueError("plan/state mismatch")
    if experiment.evaluation_start and state.as_of < experiment.evaluation_start:
        raise ValueError("episode before evaluation window")
    if experiment.evaluation_end and state.as_of > experiment.evaluation_end:
        raise ValueError("episode after evaluation window")
    if experiment.train_end and state.as_of <= experiment.train_end:
        raise ValueError("evaluation overlaps training")
    if (
        experiment.confirmatory
        and plan.strategy.available_at > experiment.registered_at
    ):
        raise ValueError("strategy not frozen before preregistration")
    if costs.fx_bps:
        raise ValueError(
            "ARS-only simulator has no FX conversion leg; nonzero FX costs require an explicit FX execution model"
        )
    alternatives = [{"name": "HOLD", "orders": [], "fraction": "0", "reason": None}]
    executable = tuple(
        o
        for o in plan.orders
        if o.executable and not o.blocked and o.quantity is not None and o.quantity > 0
    )
    for name, fraction in [
        ("PLAN", "1"),
        ("PARTIAL_25", "0.25"),
        ("PARTIAL_50", "0.5"),
        ("PARTIAL_75", "0.75"),
    ]:
        alternatives.append(
            {
                "name": name,
                "orders": _orders_json(executable),
                "fraction": fraction,
                "reason": None if plan.feasible else "FROZEN_PLAN_INFEASIBLE",
            }
        )
    cash_orders = tuple(
        Order(
            ticker=p.ticker,
            side="SELL",
            quantity=p.quantity,
            reference_price=p.mark_ars,
            target_amount_ars=p.quantity * p.mark_ars,
            executable=True,
            action="SELL_FULL",
        )
        for p in state.positions
        if p.ticker in plan.affected_tickers and p.quantity > 0
    )
    alternatives.append(
        {
            "name": "CASH",
            "orders": _orders_json(cash_orders),
            "fraction": "1",
            "reason": None,
        }
    )
    rotations = [o for o in executable if o.side == "BUY" and o.funded_by]
    rotation_tickers = {ticker for o in rotations for ticker in o.funded_by}
    rotation_orders = [
        o
        for o in executable
        if o in rotations or (o.side == "SELL" and o.ticker in rotation_tickers)
    ]
    alternatives.append(
        {
            "name": "ROTATE",
            "orders": _orders_json(rotation_orders),
            "fraction": "1",
            "reason": None if rotations else "NO_EXPLICIT_FROZEN_ROTATION",
        }
    )
    universe = next((e.payload for e in state.records if e.kind == "UNIVERSE"), {})
    candidates = [
        r
        for r in universe.get("instruments", [])
        if r.get("enabled")
        and r.get("operable")
        and r.get("candidate_score") is not None
        and r.get("reference_price_ars") is not None
        and r["ticker"] not in plan.affected_tickers
    ]
    best_reason = "NO_PIT_ELIGIBLE_RANKED_CANDIDATE"
    best_orders = []
    if (
        state.universe_quality in {"EXACT", "RECONSTRUCTED"}
        and candidates
        and cash_orders
    ):
        candidate = sorted(
            candidates, key=lambda r: (-float(r["candidate_score"]), r["ticker"])
        )[0]
        capital = sum((o.target_amount_ars for o in cash_orders), ZERO)
        friction = (
            costs.fee_bps + costs.half_spread_bps + costs.slippage_bps + costs.tax_bps
        ) / 10000
        price = _d(candidate["reference_price_ars"])
        lot = _d(candidate.get("lot_size", 1))
        qty = (
            capital * (1 - friction) / (price * (1 + friction)) / lot
        ).to_integral_value(rounding=ROUND_FLOOR) * lot
        if qty > 0:
            best_orders = [
                *cash_orders,
                Order(
                    ticker=candidate["ticker"],
                    side="BUY",
                    quantity=qty,
                    reference_price=price,
                    target_amount_ars=qty * price,
                    executable=True,
                    action="BUY",
                ),
            ]
            best_reason = None
    alternatives.append(
        {
            "name": "BEST_AVAILABLE_CANDIDATE",
            "orders": _orders_json(best_orders),
            "fraction": "1",
            "reason": best_reason,
        }
    )
    # Actual human fills are observed after T. Never put them in the frozen input
    # or substitute a subset of conveniently linked trades for account coverage.
    alternatives.append(
        {
            "name": "REAL_HUMAN_EXECUTION",
            "orders": [],
            "fraction": "1",
            "reason": "HUMAN_COVERAGE_AND_ATTRIBUTION_REQUIRED",
        }
    )
    payload = {
        "state": state.state_id,
        "plan": plan.plan_hash,
        "costs": costs.model_dump(mode="json"),
        "experiment": experiment.experiment_id,
        "split": experiment.split,
        "alternatives": alternatives,
    }
    return Episode(
        episode_id=digest(payload),
        state=state,
        plan=plan,
        cost_model=costs,
        experiment_id=experiment.experiment_id,
        split=experiment.split,
        quality=state.quality,
        alternatives_json=canonical(alternatives),
    )


def _price(index, bars, ticker, session, field, warnings, hashes):
    record = bars.get((ticker, session.session_id))
    if record is None:
        raise InsufficientEvidence(
            f"MISSING_{field.upper()}:{ticker}:{session.session_id}"
        )
    p = index.payload(record)
    if p.get("price_mode") == "CURRENTLY_ADJUSTED_HISTORY":
        raise InsufficientEvidence("CURRENTLY_ADJUSTED_HISTORY_NOT_SHARE_COMPARABLE")
    if p.get("price_mode") == "POINT_IN_TIME_ADJUSTED":
        raise InsufficientEvidence(
            "ADJUSTED_PRICE_REQUIRES_EXPLICIT_COMMON_SHARE_BASIS"
        )
    if p.get("price_mode") not in {"RAW_AS_TRADED", "POINT_IN_TIME_ADJUSTED"}:
        warnings.add("UNVERIFIED_PRICE_ADJUSTMENT_BASIS")
    if record.quality != "POINT_IN_TIME_SAFE":
        warnings.add("APPROXIMATE_OUTCOME_PRICE")
    value = p.get(field)
    if value is None or _d(value) <= 0:
        raise InsufficientEvidence(f"INVALID_{field.upper()}:{ticker}")
    hashes.add(index.hashes[id(record)])
    return _d(value)


def evaluate_episode(
    episode: Episode,
    index: EvidenceIndex,
    *,
    evaluated_as_of: datetime,
    horizons=(5, 10, 20, 40),
) -> list[AlternativeOutcome]:
    if evaluated_as_of.tzinfo is None or evaluated_as_of < episode.state.as_of:
        raise ValueError("evaluation cutoff must be aware and at/after decision")
    alternatives = json.loads(episode.alternatives_json)
    future = [s for s in index.ordered_sessions if s.open_at > episode.state.as_of]
    all_tickers = {p.ticker for p in episode.state.positions} | {
        o["ticker"] for a in alternatives for o in a["orders"]
    }
    bars = index.bars(evaluated_as_of, all_tickers, owner=episode.state.owner)
    results = []
    for horizon in horizons:
        window = future[:horizon]
        for alt in alternatives:
            fields = dict(
                episode_id=episode.episode_id,
                alternative=alt["name"],
                horizon=horizon,
                evaluated_as_of=evaluated_as_of,
                capital_base_ars=episode.state.capital_base_ars,
                quality=episode.quality.level,
                entry_at=window[0].open_at if window else None,
                exit_at=window[-1].close_at if len(window) == horizon else None,
            )
            if alt["reason"]:
                status = (
                    "UNAVAILABLE"
                    if alt["reason"] == "FROZEN_PLAN_INFEASIBLE"
                    else "NOT_APPLICABLE"
                )
                results.append(
                    AlternativeOutcome(**fields, status=status, reason=alt["reason"])
                )
                continue
            if episode.quality.level == "INVALID":
                results.append(
                    AlternativeOutcome(
                        **fields, status="INVALID", reason="INVALID_EPISODE"
                    )
                )
                continue
            if len(window) < horizon:
                results.append(
                    AlternativeOutcome(
                        **fields, status="UNAVAILABLE", reason="CALENDAR_WINDOW_MISSING"
                    )
                )
                continue
            if window[-1].close_at > evaluated_as_of:
                results.append(
                    AlternativeOutcome(
                        **fields, status="PENDING", reason="HORIZON_NOT_CLOSED"
                    )
                )
                continue
            try:
                values = _simulate(episode, alt, index, bars, window, evaluated_as_of)
                if values["warnings"]:
                    fields["quality"] = "LOW"
                results.append(AlternativeOutcome(**fields, status="MATURE", **values))
            except InsufficientEvidence as exc:
                results.append(
                    AlternativeOutcome(**fields, status="UNAVAILABLE", reason=str(exc))
                )
    return results


def _simulate(episode, alt, index, bars, window, evaluated_as_of):
    state, costs = episode.state, episode.cost_model
    quantities = {p.ticker: p.quantity for p in state.positions}
    lots = {p.ticker: p.lot_size for p in state.positions}
    for record in state.records:
        if record.kind == "UNIVERSE":
            lots.update(
                {
                    r["ticker"]: _d(r.get("lot_size", 1))
                    for r in record.payload.get("instruments", [])
                }
            )
    net_cash = gross_cash = state.cash_ars
    paid = notional = ZERO
    executed = []
    warnings = set()
    hashes = set()
    orders = [Order.model_validate(o) for o in alt["orders"]]
    tickers = set(quantities) | {o.ticker for o in orders}
    actions = [
        e
        for e in index.visible("CORPORATE_ACTION", evaluated_as_of, state.owner)
        if state.as_of < e.effective_at <= window[-1].close_at
        and e.payload.get("ticker") in tickers
    ]
    actions = list(
        {
            e.record_id: e
            for e in sorted(
                actions, key=lambda e: (e.revision_at or e.available_at, e.available_at)
            )
        }.values()
    )
    actions.sort(key=lambda e: (e.effective_at, e.record_id))
    coverage = [
        e
        for e in index.visible("ACTION_COVERAGE", evaluated_as_of, state.owner)
        if e.payload.get("complete") is True
    ]
    for ticker in tickers:
        covered = next(
            (
                e
                for e in coverage
                if e.payload.get("ticker") == ticker
                and datetime.fromisoformat(e.payload["from"]) <= state.as_of
                and datetime.fromisoformat(e.payload["to"]) >= window[-1].close_at
            ),
            None,
        )
        if covered:
            if covered.quality != "POINT_IN_TIME_SAFE":
                warnings.add("CORPORATE_ACTION_COVERAGE_NOT_EXACT")
            hashes.add(index.hashes[id(covered)])
        elif costs.return_basis == "TOTAL_RETURN":
            raise InsufficientEvidence(f"TOTAL_RETURN_ACTION_COVERAGE_MISSING:{ticker}")
        else:
            warnings.add("CORPORATE_ACTION_COVERAGE_UNVERIFIED")
    factors = {t: ONE for t in tickers}
    applied = set()

    def apply_actions(up_to):
        nonlocal net_cash, gross_cash
        for event in actions:
            if event.record_id in applied or event.effective_at > up_to:
                continue
            if event.quality in {"UNSAFE_FOR_REPLAY", "CURRENT_STATE_ONLY"}:
                raise InsufficientEvidence("UNSAFE_CORPORATE_ACTION")
            if event.quality != "POINT_IN_TIME_SAFE":
                warnings.add("CORPORATE_ACTION_NOT_EXACT")
            p = event.payload
            ticker = p["ticker"]
            if p["type"] in {"SPLIT", "RATIO_CHANGE"}:
                factor = _d(p["quantity_factor"])
                if factor <= 0:
                    raise InsufficientEvidence("INVALID_CORPORATE_ACTION_FACTOR")
                quantities[ticker] = quantities.get(ticker, ZERO) * factor
                factors[ticker] *= factor
                # Raw share accounting cannot also consume split-adjusted prices.
                if any(
                    index.payload(bars[ticker, s.session_id]).get("price_mode")
                    == "POINT_IN_TIME_ADJUSTED"
                    for s in (window[0], window[-1])
                    if (ticker, s.session_id) in bars
                ):
                    raise InsufficientEvidence(
                        "ADJUSTED_PRICE_PLUS_QUANTITY_REBASE_WOULD_DOUBLE_COUNT"
                    )
            elif p["type"] == "DIVIDEND":
                if costs.return_basis == "TOTAL_RETURN":
                    if p.get("cash_per_share_ars") is None:
                        raise InsufficientEvidence("DIVIDEND_AMOUNT_OR_FX_UNKNOWN")
                    amount = quantities.get(ticker, ZERO) * _d(p["cash_per_share_ars"])
                    net_cash += amount
                    gross_cash += amount
                else:
                    warnings.add("DIVIDENDS_EXCLUDED_PRICE_ONLY_RETURN")
            else:
                raise InsufficientEvidence("UNSUPPORTED_CORPORATE_ACTION")
            applied.add(event.record_id)
            hashes.add(index.hashes[id(event)])

    apply_actions(window[0].open_at)
    for order in sorted(orders, key=lambda o: (o.side != "SELL", o.priority, o.ticker)):
        fraction = _d(alt["fraction"])
        requested = order.quantity * fraction * factors.get(order.ticker, ONE)
        if order.ticker not in lots:
            raise InsufficientEvidence(f"MINIMUM_LOT_UNKNOWN:{order.ticker}")
        lot = lots[order.ticker]
        qty = (requested / lot).to_integral_value(rounding=ROUND_FLOOR) * lot
        if qty != requested:
            warnings.add("PARTIAL_OR_PLAN_QUANTITY_ROUNDED_TO_LOT")
        if qty == 0:
            executed.append(
                {
                    "ticker": order.ticker,
                    "side": order.side,
                    "requested_quantity": str(requested),
                    "quantity": "0",
                    "reason": "BELOW_LOT",
                }
            )
            continue
        reference = _price(
            index, bars, order.ticker, window[0], "open", warnings, hashes
        )
        gross = qty * reference
        if gross < costs.minimum_trade_ars:
            raise InsufficientEvidence("BELOW_COMMON_MINIMUM_NOTIONAL")
        impact = (costs.half_spread_bps + costs.slippage_bps) / 10000
        fill_price = reference * (ONE + impact if order.side == "BUY" else ONE - impact)
        if fill_price <= 0:
            raise InsufficientEvidence("COST_SCENARIO_INVALID_FILL_PRICE")
        fill_notional = qty * fill_price
        fee = fill_notional * (costs.fee_bps + costs.tax_bps) / 10000
        if fee >= fill_notional:
            raise InsufficientEvidence("COSTS_EXCEED_NOTIONAL")
        if order.side == "SELL":
            if qty > quantities.get(order.ticker, ZERO):
                raise InsufficientEvidence("FROZEN_SELL_EXCEEDS_HOLDINGS")
            quantities[order.ticker] -= qty
            net_cash += fill_notional - fee
            gross_cash += gross
        else:
            if fill_notional + fee > net_cash + Decimal("0.00000001"):
                raise InsufficientEvidence("FROZEN_PLAN_CASH_SHORTFALL_AT_NEXT_OPEN")
            quantities[order.ticker] = quantities.get(order.ticker, ZERO) + qty
            net_cash -= fill_notional + fee
            gross_cash -= gross
        paid += abs(fill_notional - gross) + fee
        notional += gross
        executed.append(
            {
                "ticker": order.ticker,
                "side": order.side,
                "requested_quantity": str(requested),
                "quantity": str(qty),
                "reference_price": str(reference),
                "fill_price": str(fill_price),
                "fees_taxes_ars": str(fee),
            }
        )
    apply_actions(window[-1].close_at)
    marked = ZERO
    for ticker, quantity in sorted(quantities.items()):
        if quantity:
            marked += quantity * _price(
                index, bars, ticker, window[-1], "close", warnings, hashes
            )
    gross_nav = gross_cash + marked
    net_nav = net_cash + marked
    capital = state.capital_base_ars
    return {
        "gross_return": float(gross_nav / capital - ONE),
        "net_return": float(net_nav / capital - ONE),
        "cost_drag": float((gross_nav - net_nav) / capital),
        "costs_ars": paid,
        "turnover": float(notional / capital),
        "ending_nav_ars": net_nav,
        "executed_orders_json": canonical(executed),
        "ending_positions_json": canonical(
            {t: str(q) for t, q in sorted(quantities.items())}
        ),
        "evidence_hashes": tuple(sorted(hashes)),
        "warnings": tuple(sorted(warnings)),
        "primary_eligible": episode.quality.primary_eligible and not warnings,
    }


def comparisons(episode: Episode, outcomes: list[AlternativeOutcome]) -> list[dict]:
    result = []
    for horizon in sorted({o.horizon for o in outcomes}):
        rows = {o.alternative: o for o in outcomes if o.horizon == horizon}
        plan, hold = rows.get("PLAN"), rows.get("HOLD")
        mature = {k: v for k, v in rows.items() if v.status == "MATURE"}
        paired = (
            plan is not None
            and hold is not None
            and plan.status == hold.status == "MATURE"
        )
        row = {
            "episode_id": episode.episode_id,
            "opportunity_id": episode.state.opportunity_id,
            "state_id": episode.state.state_id,
            "as_of": episode.state.as_of.isoformat(),
            "owner": episode.state.owner,
            "horizon": horizon,
            "strategy_version": episode.plan.strategy.strategy_version,
            "strategy_hash": digest(episode.plan.strategy),
            "mode": episode.plan.mode,
            "experiment_id": episode.experiment_id,
            "split": episode.split,
            "cost_model_version": episode.cost_model.version,
            "cost_model_hash": digest(episode.cost_model),
            "return_basis": episode.cost_model.return_basis,
            "quality": (
                "LOW"
                if paired and (plan.quality == "LOW" or hold.quality == "LOW")
                else episode.quality.level
            ),
            "status": "MATURE" if paired else "INSUFFICIENT",
            "n_episode": 1,
            "primary_eligible": bool(
                paired and plan.primary_eligible and hold.primary_eligible
            ),
            "plan_return": (
                plan.net_return if plan and plan.status == "MATURE" else None
            ),
            "hold_return": (
                hold.net_return if hold and hold.status == "MATURE" else None
            ),
            "dva": plan.net_return - hold.net_return if paired else None,
            "entry_at": plan.entry_at.isoformat() if plan and plan.entry_at else None,
            "exit_at": plan.exit_at.isoformat() if plan and plan.exit_at else None,
            "regret": (
                max(o.net_return for o in mature.values()) - plan.net_return
                if plan and "PLAN" in mature
                else None
            ),
            "regret_scope": "EX_POST_BEST_OF_EVALUABLE_FROZEN_ALTERNATIVES_ONLY",
            "alternative_coverage": sorted(mature),
            "capital_base_ars": str(episode.state.capital_base_ars),
            "cost_drag": plan.cost_drag if plan else None,
            "turnover": plan.turnover if plan else None,
        }
        for name in ("CASH", "ROTATE", "REAL_HUMAN_EXECUTION"):
            row["plan_minus_" + name.lower()] = (
                plan.net_return - mature[name].net_return
                if "PLAN" in mature and name in mature
                else None
            )
        mechanism = json.loads(episode.plan.mechanism_json)
        row["segments"] = dict(mechanism.get("segments", {}))
        actions = {
            o.action for o in episode.plan.orders if o.executable and not o.blocked
        }
        row["segments"]["decision_type"] = (
            next(iter(actions)) if len(actions) == 1 else "MIXED" if actions else "HOLD"
        )
        if len(episode.plan.affected_tickers) == 1:
            row["segments"]["ticker"] = episode.plan.affected_tickers[0]
        row["tickers"] = list(episode.plan.affected_tickers)
        row["reasons"] = {k: v.reason for k, v in rows.items() if v.reason}
        result.append(row)
    return result

"""Point-in-time next-session windows, with explicit missingness."""
from .episodes import unique_rows
from .models import digest


def cost_return(gross, direction, bps):
    if bps < 0:
        raise ValueError("negative cost")
    if direction == "ABSTAIN" or gross is None:
        return None
    if direction == "NEUTRAL":
        return 0.0
    return gross - bps / 10000


def build_outcomes(dataset, episodes, policy):
    recs = {r.recommendation_id: r for r in dataset.recommendations}
    sessions = sorted(unique_rows(dataset.sessions, lambda s: s.session_id), key=lambda s: s.open_at)
    bars = unique_rows(dataset.bars, lambda b: (b.series_id, b.instrument_id, b.session_id, b.revision))
    selected = {}
    for bar in sorted(bars, key=lambda b: b.revision):
        selected[bar.series_id, bar.instrument_id, bar.session_id] = bar

    def prices(instrument, window, series):
        if series is None:
            return None, "PRICE_SERIES_UNSPECIFIED"
        values = [selected.get((series, instrument, s.session_id)) for s in window]
        if any(b is None for b in values):
            return None, "MISSING_BAR"
        if len({(b.currency, b.adjustment_basis) for b in values}) != 1:
            return None, "INCONSISTENT_PRICE_BASIS"
        if any(b.available_at < s.close_at for b, s in zip(values, window)):
            return None, "BAR_AVAILABLE_BEFORE_CLOSE"
        return values, None

    output = []
    for ep in episodes:
        rec = recs[ep["anchor_id"]]
        future = [s for s in sessions if s.open_at > rec.decision_as_of]
        for h in policy.horizons:
            window = future[:h]
            status, reason = "MATURE", None
            maturity = window[-1].close_at if len(window) == h else None
            if rec.ambiguous:
                status, reason = "AMBIGUOUS", rec.ambiguity_reason
            elif not rec.eligible or rec.direction in {"NEUTRAL", "ABSTAIN"}:
                status, reason = "EXCLUDED_POLICY", "INELIGIBLE_OR_NON_DIRECTIONAL"
            elif maturity is None:
                status, reason = "UNAVAILABLE", "CALENDAR_INCOMPLETE"
            elif maturity > policy.evaluated_as_of:
                status, reason = "PENDING", "FUTURE_WINDOW"
            vals, original, benchmark = None, None, None
            if status == "MATURE":
                vals, reason = prices(rec.instrument_id, window, rec.price_series_id)
                if vals is None:
                    status = "UNAVAILABLE"
                if rec.source_module == "SWAP":
                    original, original_reason = prices(rec.original_instrument_id, window, rec.price_series_id)
                    if original is None:
                        status, reason = "UNAVAILABLE", original_reason
            if status == "MATURE" and rec.benchmark_id:
                benchmark, _ = prices(rec.benchmark_id, window, rec.price_series_id)
                if benchmark and benchmark[0].currency != vals[0].currency:
                    benchmark = None
            gross = benchmark_return = alpha = original_return = None
            if status == "MATURE":
                sign = 1 if rec.direction == "BUY" else -1
                gross = sign * (vals[-1].close / vals[0].open - 1)
                if benchmark:
                    benchmark_return = sign * (benchmark[-1].close / benchmark[0].open - 1)
                    alpha = gross - benchmark_return
                if original:
                    if original[0].currency != vals[0].currency:
                        status, reason, gross, alpha, benchmark_return = "UNAVAILABLE", "SWAP_CURRENCY_MISMATCH", None, None, None
                    else:
                        original_return = original[-1].close / original[0].open - 1
                        alpha = gross - original_return
            evidence = [digest(b) for seq in (vals, benchmark, original) if seq for b in seq]
            for scenario in policy.costs.scenarios:
                net = cost_return(gross, rec.direction, scenario.bps)
                row = {"episode_id": ep["decision_episode_id"], "recommendation_id": rec.recommendation_id,
                       "account_id": rec.account_id, "source_module": rec.source_module, "cohort": rec.cohort,
                       "ticker": rec.ticker, "instrument_id": rec.instrument_id,
                       "original_instrument_id": rec.original_instrument_id, "direction": rec.direction,
                       "score": rec.score, "decision_as_of": rec.decision_as_of.isoformat(),
                       "reference_notional": rec.reference_notional,
                       "entry_at": window[0].open_at.isoformat() if window else None,
                       "maturity_at": maturity.isoformat() if maturity else None,
                       "horizon_days": h, "outcome_status": status, "reason_code": reason,
                       "recommendation_count": ep["recommendation_count"],
                       "entry_reference_price": vals[0].open if status == "MATURE" else None,
                       "exit_reference_price": vals[-1].close if status == "MATURE" else None,
                       "gross_return": gross, "benchmark_return": benchmark_return, "gross_alpha": alpha,
                       "original_return": original_return,
                       "cost_scenario": scenario.name, "cost_bps": scenario.bps, "net_return": net,
                       "net_alpha": alpha - scenario.bps / 10000 if alpha is not None and status == "MATURE" else None,
                       "ranking_reason": None if alpha is not None else "BENCHMARK_UNAVAILABLE",
                       "cost_convention": "incremental_replacement_round_trip" if original else "directional_round_trip",
                       "evaluated_as_of": policy.evaluated_as_of.isoformat(),
                       "input_hash": digest([ep["source_hash"], evidence, [digest(s) for s in window]])}
                output.append(row)
    return output

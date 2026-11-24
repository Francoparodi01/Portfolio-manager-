"""Preregistered geometrical null for Elliott; no I/O or decision authority."""
from __future__ import annotations

import hashlib
import math
import random
import statistics
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_EVEN

from src.technical_desk.features import _extrema, _moment, _pivot, number
from src.technical_desk.elliott_rules import validate_impulse


VERSION = "technical-desk-elliott-control-v1"
STATES = ("RULES_COMPATIBLE", "REJECTED", "INCOMPLETE", "NOT_EVALUABLE")
MASTER_SEED = 2026111601
REPLICATES = 1000
PROCEDURE = {
    "version": VERSION,
    "master_seed": MASTER_SEED,
    "replicates_per_packet_timeframe": REPLICATES,
    "detector": "UNCHANGED_CLOSED_BAR_FRACTAL_2_LEFT_2_RIGHT",
    "candidate": "ALL_CONSECUTIVE_SIX_ENDPOINT_WINDOWS_ON_FINAL_ACTIVE_ALTERNATING_CHAIN",
    "calibration": "SAMPLE_STDEV_DDOF1_LOG_CLOSE_RETURNS_OF_SAME_FROZEN_PACKET_TIMEFRAME",
    "drift_log_returns": "0",
    "initial_open": "FIRST_OBSERVED_CLOSE",
    "first_close": "FIRST_OBSERVED_CLOSE_NO_RETURN",
    "subsequent_open": "PREVIOUS_UNROUNDED_SIMULATED_CLOSE",
    "subsequent_close": "OPEN_TIMES_EXP_CENTERED_AND_RESCALED_GAUSSIAN_LOG_RETURN",
    "return_normalization": "N_MINUS_1_NORMAL_DRAWS_CENTERED_TO_ZERO_AND_RESCALED_TO_OBSERVED_SAMPLE_SIGMA_BEFORE_TICK_ROUNDING_FLOAT64_PRECISION",
    "high": "MAX_OPEN_CLOSE_TIMES_EXP_SIGMA_TIMES_ABS_INDEPENDENT_STANDARD_NORMAL_DIVIDED_BY_4",
    "low": "MIN_OPEN_CLOSE_TIMES_EXP_MINUS_SIGMA_TIMES_ABS_INDEPENDENT_STANDARD_NORMAL_DIVIDED_BY_4",
    "rounding": "OPEN_CLOSE_NEAREST_TICK_HALF_EVEN_HIGH_CEILING_LOW_FLOOR_MINIMUM_ONE_TICK",
    "seed_derivation": "SHA256_ASCII_MASTER_SEED_COLON_TICKER_COLON_TIMEFRAME_COLON_ZERO_BASED_REPLICA_AS_BIG_ENDIAN_INT",
    "rng": "PYTHON_RANDOM_RANDOM_GAUSS_0_1_ALL_RETURN_DRAWS_FIRST_THEN_UPPER_AND_LOWER_WICK_FOR_EACH_BAR",
    "nonoverlap_all": "GREEDY_CHRONOLOGICAL_ALL_CANDIDATES_ACCEPT_ONLY_START_BAR_GREATER_THAN_PREVIOUS_END_BAR",
    "nonoverlap_compatible": "SAME_GREEDY_RULE_FILTERING_RULES_COMPATIBLE_FIRST",
    "primary_statistic": "RULES_COMPATIBLE_COUNT_DIVIDED_BY_ALL_SIX_ENDPOINT_CANDIDATES",
    "reference_rate": "POOLED_COMPATIBLE_COUNT_DIVIDED_BY_POOLED_CANDIDATE_COUNT",
    "upper_empirical_p": "(1+NUMBER_ELIGIBLE_WALKS_WITH_RATE_GE_OBSERVED)/(1+NUMBER_WALKS_WITH_NONZERO_CANDIDATES)",
    "nonoverlap_statistic": "NONOVERLAPPING_COMPATIBLE_COUNT_PER_FIXED_SERIES_LENGTH",
    "multiplicity": "TEN_DESCRIPTIVE_COMPARISONS_UNADJUSTED_NO_SIGNIFICANCE_DECISION",
    "inference": "IN_SAMPLE_GEOMETRIC_FREQUENCY_REFERENCE_NOT_PREDICTIVE_VALIDATION",
    "control_limitations": [
        "Sigma estimated from the same history; no out-of-sample calibration",
        "Gaussian increments conditioned on sample mean/variance omit autocorrelation, clustering, jumps and regimes",
        "Centering fixes terminal unrounded close to initial close (log-price bridge); this is a conditional null, not unrestricted random walks",
        "Independent fixed-scale wicks are not fitted intraday market geometry",
        "Tick rounding changes realized volatility slightly; sigma matches before rounding",
        "Weekly walks are generated independently, not aggregated from daily simulated walks",
        "Disjoint price intervals do not establish statistical independence",
        "No returns after patterns, costs, profitability or Elliott predictive edge are tested",
    ],
}
SECONDARY_SCALE = {
    "version": "technical-desk-elliott-scale-5x5-v1", "left_bars": 5, "right_bars": 5,
    "scope": "OBSERVED_FROZEN_PACKETS_ONLY_NO_ADDITIONAL_RANDOM_WALKS",
    "authorized": "EXPLICIT_USER_APPROVAL_FOR_ELLIOTT_5_PLUS_5",
    "same_validator": "UNCHANGED_ELLIOTT_DETERMINISTIC_RULES_V1",
    "same_candidate": PROCEDURE["candidate"],
    "comparison": "DESCRIPTIVE_SCALE_SENSITIVITY_NO_WINNING_SCALE_SELECTION",
}


def normalize_bars(rows):
    bars = []
    for row in rows:
        bar = dict(row, **{field: number(row[field]) for field in ("open", "high", "low", "close")})
        started, closed = _moment(bar["time"]), _moment(bar["closed_at"])
        if (bar["low"] <= 0 or bar["low"] > min(bar["open"], bar["close"]) or
                bar["high"] < max(bar["open"], bar["close"]) or started >= closed):
            raise ValueError("INVALID_CLOSED_OHLC_BAR")
        if bars and (_moment(bars[-1]["time"]) >= started or _moment(bars[-1]["closed_at"]) > closed):
            raise ValueError("BARS_MUST_BE_CHRONOLOGICAL")
        bars.append(bar)
    return bars


def extract_pivots(rows, *, timeframe, policy, pivot_window=2):
    """Use the feature detector's exact primitives, omitting unrelated indicators."""
    if timeframe not in ("D", "W") or pivot_window not in (2,5) or (policy["pivots"]["left_bars"], policy["pivots"]["right_bars"]) != (pivot_window,pivot_window):
        raise ValueError("ONLY_REGISTERED_2_PLUS_2_OR_5_PLUS_5_DETECTOR_SUPPORTED")
    bars = normalize_bars(rows)
    confirmed, chain, ambiguous = [], [], 0
    for known_index in range(2*pivot_window, len(bars)):
        index = known_index - pivot_window
        kinds = _extrema(bars, index, pivot_window, known_index)
        if len(kinds) == 2:
            ambiguous += 1
        elif kinds:
            pivot = _pivot(bars, index, kinds[0], timeframe, known_index)
            confirmed.append(pivot)
            if not chain or chain[-1]["kind"] != pivot["kind"]:
                chain.append(pivot)
            elif (pivot["price"] > chain[-1]["price"] if pivot["kind"] == "HIGH" else
                  pivot["price"] < chain[-1]["price"]):
                chain[-1] = pivot
    return dict(bars=bars, confirmed_pivots=confirmed, active_alternating_pivots=chain,
                ambiguous_extrema_count=ambiguous)


def summarize_candidates(candidates):
    counts = {state: 0 for state in STATES}
    nonoverlap_counts = {state: 0 for state in STATES}
    selected_all, selected_compatible = [], []
    last_all = last_compatible = -1
    for candidate in candidates:
        counts[candidate["status"]] += 1
        if candidate["start_index"] > last_all:
            selected_all.append(candidate["candidate_index"])
            nonoverlap_counts[candidate["status"]] += 1
            last_all = candidate["end_index"]
        if candidate["status"] == "RULES_COMPATIBLE" and candidate["start_index"] > last_compatible:
            selected_compatible.append(candidate["candidate_index"])
            last_compatible = candidate["end_index"]
    return dict(candidate_count=len(candidates), status_counts=counts,
                nonoverlap_all=dict(count=len(selected_all), status_counts=nonoverlap_counts,
                                    candidate_indices=selected_all),
                nonoverlap_compatible=dict(count=len(selected_compatible), candidate_indices=selected_compatible))


def evaluate_series(rows, *, tick_size, timeframe, policy, include_candidates=False, pivot_window=2):
    extracted = extract_pivots(rows, timeframe=timeframe, policy=policy,pivot_window=pivot_window)
    bars, chain = extracted["bars"], extracted["active_alternating_pivots"]
    candidates = []
    for offset in range(max(0, len(chain) - 5)):
        pivots = chain[offset:offset + 6]
        first, last = pivots[0]["index"], pivots[-1]["index"]
        # Rebase only the exact candidate/confirmation span. This does not change
        # any OHLC supplied to a rule or the causal confirmation cutoff.
        known = max(p["confirmed_index"] for p in pivots)
        local_pivots = [dict(p, index=p["index"] - first) for p in pivots]
        result = validate_impulse(local_pivots, bars[first:known + 1], tick_size=tick_size, scale=timeframe)
        record = dict(candidate_index=offset, start_index=first, end_index=last,
                      start_time=bars[first]["time"], end_time=bars[last]["time"],
                      status=result["status"], pivot_ids=[p["id"] for p in pivots])
        if include_candidates:
            record["rules"] = result["rules"]
        candidates.append(record)
    summary = summarize_candidates(candidates)
    summary.update(bar_count=len(bars), confirmed_pivot_count=len(extracted["confirmed_pivots"]),
                   active_alternating_pivot_count=len(chain), ambiguous_extrema_count=extracted["ambiguous_extrema_count"])
    if include_candidates:
        summary["candidates"] = candidates
    return summary


def calibrate(rows):
    closes = [float(number(bar["close"])) for bar in rows]
    if len(closes) < 3 or any(value <= 0 or not math.isfinite(value) for value in closes):
        raise ValueError("AT_LEAST_THREE_POSITIVE_CLOSES_REQUIRED")
    returns = [math.log(b / a) for a, b in zip(closes, closes[1:])]
    return dict(bar_count=len(rows), log_return_count=len(returns), ddof=1,
                log_return_sigma=repr(statistics.stdev(returns)), initial_price=str(number(rows[0]["close"])))


def replica_seed(ticker, timeframe, replica):
    payload = f"{MASTER_SEED}:{ticker}:{timeframe}:{replica}".encode("ascii")
    return int.from_bytes(hashlib.sha256(payload).digest(), "big")


def random_walk(rows, *, calibration, tick_size, seed, return_diagnostics=False):
    """Return deterministic OHLC geometry for the frozen null specification."""
    tick, sigma = number(tick_size), float(calibration["log_return_sigma"])
    if tick <= 0 or sigma < 0 or not math.isfinite(sigma):
        raise ValueError("INVALID_CONTROL_CALIBRATION")
    rng, previous, output = random.Random(seed), float(calibration["initial_price"]), []
    normals = [rng.gauss(0,1) for _ in range(len(rows)-1)]
    mean, stdev = statistics.fmean(normals), statistics.stdev(normals)
    increments = [(value-mean)*sigma/stdev for value in normals]
    def rounded(value, mode):
        return max(tick, (Decimal(repr(value)) / tick).to_integral_value(rounding=mode) * tick)
    for index, source in enumerate(rows):
        opened = previous
        closed = opened if index == 0 else opened * math.exp(increments[index-1])
        high = max(opened, closed) * math.exp(sigma * abs(rng.gauss(0, 1)) / 4)
        low = min(opened, closed) * math.exp(-sigma * abs(rng.gauss(0, 1)) / 4)
        output.append(dict(time=source["time"], closed_at=source["closed_at"],
            open=rounded(opened, ROUND_HALF_EVEN), close=rounded(closed, ROUND_HALF_EVEN),
            high=rounded(high, ROUND_CEILING), low=rounded(low, ROUND_FLOOR)))
        previous = closed
    if not return_diagnostics:
        return output
    realized = float(calibrate(output)["log_return_sigma"])
    return dict(bars=output,volatility=dict(target_sigma=sigma,
        pre_tick_sigma=statistics.stdev(increments),pre_tick_mean=statistics.fmean(increments),
        realized_sigma=realized,absolute_relative_error=abs(realized/sigma-1) if sigma else (0 if realized==0 else None)))


def compare_control(observed, simulations):
    total = sum(row["candidate_count"] for row in simulations)
    compatible = sum(row["status_counts"]["RULES_COMPATIBLE"] for row in simulations)
    observed_n = observed["candidate_count"]
    observed_c = observed["status_counts"]["RULES_COMPATIBLE"]
    eligible = [row for row in simulations if row["candidate_count"] > 0]
    exceed = sum(row["status_counts"]["RULES_COMPATIBLE"] * observed_n >= observed_c * row["candidate_count"]
                 for row in eligible) if observed_n else None
    nonoverlap_observed = observed["nonoverlap_compatible"]["count"]
    nonoverlap_control = sum(row["nonoverlap_compatible"]["count"] for row in simulations)
    nonoverlap_exceed = sum(row["nonoverlap_compatible"]["count"] >= nonoverlap_observed for row in simulations)
    return dict(
        observed=dict(compatible=observed_c, candidates=observed_n,
                      rate=observed_c / observed_n if observed_n else None),
        control=dict(compatible=compatible, candidates=total, pooled_rate=compatible / total if total else None,
                     walks=len(simulations), eligible_walks=len(eligible), zero_candidate_walks=len(simulations)-len(eligible)),
        rate_ratio=(observed_c * total / (observed_n * compatible)) if observed_n and compatible else None,
        rate_ratio_status="AVAILABLE" if observed_n and compatible else "ZERO_OR_MISSING_DENOMINATOR",
        upper_empirical_p=dict(value=(1+exceed)/(1+len(eligible)) if exceed is not None and eligible else None,
                               exceedances=exceed, eligible_walks=len(eligible), correction="ADD_ONE"),
        nonoverlap_compatible=dict(observed_count=nonoverlap_observed,
            control_total=nonoverlap_control, control_mean=nonoverlap_control/len(simulations) if simulations else None,
            count_ratio=nonoverlap_observed*len(simulations)/nonoverlap_control if nonoverlap_control else None,
            upper_empirical_p=(1+nonoverlap_exceed)/(1+len(simulations)) if simulations else None,
            exceedances=nonoverlap_exceed, walks=len(simulations)),
        predictive_validity="NOT_EVALUATED")

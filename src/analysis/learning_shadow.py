"""Pure rules for the isolated blocked-decision learning audit."""
from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import isfinite
from typing import Any, Mapping
from zoneinfo import ZoneInfo


POLICY_VERSION = "learning-shadow-v2"
SCHEMA_VERSION = 2
CANONICAL_OUTCOME_BASIS = "canonical_cocos"
DEFAULT_MATERIAL_RETURN_BPS = 75
OUTCOME_GRACE_DAYS = 4
MAX_ABS_DIRECTIONAL_OUTCOME = 5.0
BENCHMARK_TICKER = "SPY"
MEDIUM_ADVERSE_EXCURSION = -0.06
HIGH_ADVERSE_EXCURSION = -0.12
MIN_PATH_SESSIONS = 2
MIN_RULE_SAMPLE = 20
ART_TZ = ZoneInfo("America/Argentina/Buenos_Aires")

HORIZON_MAP = {
    5: 5,
    10: 5,
    20: 20,
    40: 40,
}

PLANNER_BLOCKED = "PLANNER_BLOCKED"
RADAR_BLOCKED = "RADAR_BLOCKED"
RADAR_DEBUG = "RADAR_DEBUG"
OTHER_BLOCKED = "OTHER_BLOCKED"
CASE_POPULATIONS = (PLANNER_BLOCKED, RADAR_BLOCKED, RADAR_DEBUG, OTHER_BLOCKED)

PENDING = "PENDING"
MISSING_OUTCOME = "MISSING_OUTCOME"
EXCLUDED_BASIS = "EXCLUDED_BASIS"
EXCLUDED_OUTLIER = "EXCLUDED_OUTLIER"
POTENTIAL_FALSE_NEGATIVE = "POTENTIAL_FALSE_NEGATIVE"
POSITIVE_BELOW_THRESHOLD = "POSITIVE_BELOW_THRESHOLD"
NON_POSITIVE_COUNTERFACTUAL = "NON_POSITIVE_COUNTERFACTUAL"

CLEAN_MISSED_OPPORTUNITY = "CLEAN_MISSED_OPPORTUNITY"
RISKY_COUNTERFACTUAL_WIN = "RISKY_COUNTERFACTUAL_WIN"
MARKET_DRIVEN_WIN = "MARKET_DRIVEN_WIN"
UNCONTROLLED_COUNTERFACTUAL_WIN = "UNCONTROLLED_COUNTERFACTUAL_WIN"
NO_MATERIAL_UPSIDE = "NO_MATERIAL_UPSIDE"
INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"

MATURE_CLASSIFICATIONS = {
    POTENTIAL_FALSE_NEGATIVE,
    POSITIVE_BELOW_THRESHOLD,
    NON_POSITIVE_COUNTERFACTUAL,
}
EXCLUDED_CLASSIFICATIONS = {EXCLUDED_BASIS, EXCLUDED_OUTLIER}


def _normalized(value: Any) -> str:
    text = str(value or "").strip().lower()
    return "".join(
        char
        for char in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(char)
    )


def classify_population(row: Mapping[str, Any]) -> str:
    source = _normalized(row.get("source"))
    scope = _normalized(row.get("metric_scope"))
    if source == "execution_plan" and scope == "blocked_audit":
        return PLANNER_BLOCKED
    if source == "radar" and scope == "radar_audit":
        return RADAR_BLOCKED
    if source == "radar" and scope == "debug":
        return RADAR_DEBUG
    return OTHER_BLOCKED


def classify_block_reason(reason: str | None, code: str | None = None) -> str:
    text = _normalized(f"{code or ''} {reason or ''}")
    if any(token in text for token in ("nominal", "sizing", "min_trade", "monto minimo")):
        return "MIN_TRADE_OR_NOMINAL"
    if "delta" in text and any(token in text for token in ("umbral", "insuficiente", "min")):
        return "MIN_WEIGHT_DELTA"
    if any(token in text for token in ("funding", "cash", "compra pendiente", "sin fondos")):
        return "FUNDING"
    if any(token in text for token in (
        "score_guard", "score guard", "score", "senal insuficiente", "compra en watch",
    )):
        return "SCORE_GUARD"
    if any(token in text for token in ("r/r", "riesgo/retorno", "asimetria", "upside", "target demasiado")):
        return "RISK_REWARD"
    if any(token in text for token in ("earnings", "evento", "catalyst", "balance")):
        return "EVENT_RISK"
    if any(token in text for token in ("precio no comparable", "corporate", "anomalia", "price basis")):
        return "PRICE_INTEGRITY"
    if any(token in text for token in ("risk gate", "gate blocked", "gate cautious", "vix", "riesgo")):
        return "RISK_GATE"
    return "OTHER"


def classify_counterfactual(
    *,
    directional_outcome: float | None,
    outcome_basis: str | None,
    decided_at: datetime,
    horizon_days: int,
    as_of: datetime,
    material_return_bps: int = DEFAULT_MATERIAL_RETURN_BPS,
) -> str:
    """Classify the counterfactual result without declaring the guard wrong."""
    if outcome_basis and outcome_basis != CANONICAL_OUTCOME_BASIS:
        return EXCLUDED_BASIS

    if directional_outcome is None:
        due_at = decided_at + timedelta(days=int(horizon_days) + OUTCOME_GRACE_DAYS)
        return MISSING_OUTCOME if as_of >= due_at else PENDING

    if outcome_basis != CANONICAL_OUTCOME_BASIS:
        return EXCLUDED_BASIS

    outcome = float(directional_outcome)
    if not isfinite(outcome) or abs(outcome) > MAX_ABS_DIRECTIONAL_OUTCOME:
        return EXCLUDED_OUTLIER

    material_threshold = float(material_return_bps) / 10_000.0
    if outcome >= material_threshold:
        return POTENTIAL_FALSE_NEGATIVE
    if outcome > 0:
        return POSITIVE_BELOW_THRESHOLD
    return NON_POSITIVE_COUNTERFACTUAL


def classify_path_risk(
    *,
    mae: float | None,
    mfe: float | None,
    path_max_gap: float | None = None,
) -> str:
    if mae is None:
        return "PENDING"
    mae_value = float(mae)
    mfe_value = float(mfe) if mfe is not None else None
    if (
        not isfinite(mae_value)
        or mae_value > 0
        or (mfe_value is not None and mfe_value < 0)
        or (path_max_gap is not None and float(path_max_gap) >= 0.35)
        or mae_value < -1.0
        or (mfe_value is not None and mfe_value > 5.0)
    ):
        return "OUTLIER"
    if mae_value <= HIGH_ADVERSE_EXCURSION:
        return "HIGH"
    if mae_value <= MEDIUM_ADVERSE_EXCURSION:
        return "MEDIUM"
    return "OK"


def classify_review_label(
    *,
    classification: str,
    path_sessions: int,
    path_risk: str,
    benchmark_outcome: float | None,
    alpha_vs_benchmark: float | None,
) -> str:
    if classification != POTENTIAL_FALSE_NEGATIVE:
        if classification in MATURE_CLASSIFICATIONS:
            return NO_MATERIAL_UPSIDE
        return INSUFFICIENT_EVIDENCE
    if path_sessions < MIN_PATH_SESSIONS or path_risk in {"PENDING", "OUTLIER"}:
        return INSUFFICIENT_EVIDENCE
    if path_risk in {"MEDIUM", "HIGH"}:
        return RISKY_COUNTERFACTUAL_WIN
    if benchmark_outcome is None or alpha_vs_benchmark is None:
        return UNCONTROLLED_COUNTERFACTUAL_WIN
    if alpha_vs_benchmark <= 0:
        return MARKET_DRIVEN_WIN
    return CLEAN_MISSED_OPPORTUNITY


def shadow_supports_direction(*, decision: str, expected_return: float | None) -> bool | None:
    if expected_return is None:
        return None
    if decision.upper() == "BUY":
        return float(expected_return) > 0
    if decision.upper() == "SELL":
        return float(expected_return) < 0
    return None


@dataclass(frozen=True)
class LearningShadowCase:
    owner_chat_id: int
    decision_log_id: int
    ticker: str
    decision: str
    decided_at: datetime
    horizon_days: int
    shadow_horizon_sessions: int
    case_population: str
    block_reason: str | None
    block_code: str | None
    block_category: str
    outcome_basis: str | None
    outcome_source: str | None
    directional_outcome: float | None
    material_return_bps: int
    classification: str
    audit_entry_price: float | None
    audit_start_day: Any
    path_sessions: int
    path_max_gap: float | None
    mae: float | None
    mfe: float | None
    path_risk: str
    benchmark_ticker: str
    benchmark_outcome: float | None
    alpha_vs_benchmark: float | None
    control_decision_log_id: int | None
    control_status: str | None
    control_outcome: float | None
    control_match_type: str | None
    control_distance: float | None
    delta_vs_control: float | None
    review_label: str
    shadow_forecast_id: int | None
    shadow_as_of_ts: datetime | None
    shadow_expected_return: float | None
    shadow_probability_up: float | None
    shadow_action: str | None
    shadow_direction_correct: bool | None
    shadow_supports_direction: bool | None
    metadata: dict[str, Any]

    @classmethod
    def from_mapping(
        cls,
        row: Mapping[str, Any],
        *,
        as_of: datetime | None = None,
        material_return_bps: int = DEFAULT_MATERIAL_RETURN_BPS,
    ) -> "LearningShadowCase":
        evaluated_at = as_of or datetime.now(timezone.utc)
        executable_outcome = row.get("executable_outcome")
        nominal_outcome = row.get("nominal_outcome")
        if executable_outcome is not None:
            directional_outcome = float(executable_outcome)
            outcome_source = "next_executable"
        elif nominal_outcome is not None:
            directional_outcome = float(nominal_outcome)
            outcome_source = "decision_price"
        else:
            directional_outcome = None
            outcome_source = None

        decided_at = row["decided_at"]
        if decided_at.tzinfo is None:
            decided_at = decided_at.replace(tzinfo=timezone.utc)
        expected_return = row.get("shadow_expected_return")
        decision = str(row["decision"]).upper()
        horizon_days = int(row["horizon_days"])
        block_code = str(row["block_code"]) if row.get("block_code") else None
        block_reason = str(row["block_reason"]) if row.get("block_reason") else None
        classification = classify_counterfactual(
            directional_outcome=directional_outcome,
            outcome_basis=row.get("outcome_basis"),
            decided_at=decided_at,
            horizon_days=horizon_days,
            as_of=evaluated_at,
            material_return_bps=material_return_bps,
        )
        mae = float(row["mae"]) if row.get("mae") is not None else None
        mfe = float(row["mfe"]) if row.get("mfe") is not None else None
        path_sessions = int(row.get("path_sessions") or 0)
        path_max_gap = (
            float(row["path_max_gap"])
            if row.get("path_max_gap") is not None
            else None
        )
        path_risk = classify_path_risk(
            mae=mae,
            mfe=mfe,
            path_max_gap=path_max_gap,
        )
        benchmark_outcome = (
            float(row["benchmark_outcome"])
            if row.get("benchmark_outcome") is not None
            else None
        )
        alpha_vs_benchmark = (
            directional_outcome - benchmark_outcome
            if directional_outcome is not None and benchmark_outcome is not None
            else None
        )
        control_outcome = (
            float(row["control_outcome"])
            if row.get("control_outcome") is not None
            else None
        )
        delta_vs_control = (
            directional_outcome - control_outcome
            if directional_outcome is not None and control_outcome is not None
            else None
        )
        review_label = classify_review_label(
            classification=classification,
            path_sessions=path_sessions,
            path_risk=path_risk,
            benchmark_outcome=benchmark_outcome,
            alpha_vs_benchmark=alpha_vs_benchmark,
        )

        metadata = {
            "source": row.get("source"),
            "status": row.get("status"),
            "metric_scope": row.get("metric_scope"),
            "run_intent": row.get("run_intent"),
            "final_score": row.get("final_score"),
            "regime": row.get("regime"),
            "delta_weight": row.get("delta_weight"),
            "price_at_decision": row.get("price_at_decision"),
            "next_executable_at": row.get("next_executable_at"),
            "match_rule": "latest_forecast_at_or_before_decision",
            "control_rule": "retrospective_nearest_execution_plan_same_side_45d",
            "candle_rule": "one_daily_candle_by_existing_source_priority",
            "horizon_note": "decision and path horizons use calendar days; shadow uses market sessions",
        }
        return cls(
            owner_chat_id=int(row.get("owner_chat_id") or 0),
            decision_log_id=int(row["decision_log_id"]),
            ticker=str(row["ticker"]).upper(),
            decision=decision,
            decided_at=decided_at,
            horizon_days=horizon_days,
            shadow_horizon_sessions=int(row["shadow_horizon_sessions"]),
            case_population=classify_population(row),
            block_reason=block_reason,
            block_code=block_code,
            block_category=classify_block_reason(block_reason, block_code),
            outcome_basis=(str(row["outcome_basis"]) if row.get("outcome_basis") else None),
            outcome_source=outcome_source,
            directional_outcome=directional_outcome,
            material_return_bps=int(material_return_bps),
            classification=classification,
            audit_entry_price=(
                float(row["audit_entry_price"])
                if row.get("audit_entry_price") is not None
                else None
            ),
            audit_start_day=row.get("audit_start_day"),
            path_sessions=path_sessions,
            path_max_gap=path_max_gap,
            mae=mae,
            mfe=mfe,
            path_risk=path_risk,
            benchmark_ticker=BENCHMARK_TICKER,
            benchmark_outcome=benchmark_outcome,
            alpha_vs_benchmark=alpha_vs_benchmark,
            control_decision_log_id=(
                int(row["control_decision_log_id"])
                if row.get("control_decision_log_id") is not None
                else None
            ),
            control_status=(str(row["control_status"]) if row.get("control_status") else None),
            control_outcome=control_outcome,
            control_match_type=(
                str(row["control_match_type"])
                if row.get("control_match_type")
                else None
            ),
            control_distance=(
                float(row["control_distance"])
                if row.get("control_distance") is not None
                else None
            ),
            delta_vs_control=delta_vs_control,
            review_label=review_label,
            shadow_forecast_id=(
                int(row["shadow_forecast_id"])
                if row.get("shadow_forecast_id") is not None
                else None
            ),
            shadow_as_of_ts=row.get("shadow_as_of_ts"),
            shadow_expected_return=(float(expected_return) if expected_return is not None else None),
            shadow_probability_up=(
                float(row["shadow_probability_up"])
                if row.get("shadow_probability_up") is not None
                else None
            ),
            shadow_action=(str(row["shadow_action"]) if row.get("shadow_action") else None),
            shadow_direction_correct=(
                bool(row["shadow_direction_correct"])
                if row.get("shadow_direction_correct") is not None
                else None
            ),
            shadow_supports_direction=shadow_supports_direction(
                decision=decision,
                expected_return=(float(expected_return) if expected_return is not None else None),
            ),
            metadata=metadata,
        )


def _mean(values: list[float | None]) -> float | None:
    clean = [float(value) for value in values if value is not None]
    return sum(clean) / len(clean) if clean else None


def _metric_row(
    cases: list[LearningShadowCase],
    *,
    population: str,
    horizon_days: int,
) -> dict[str, Any]:
    cohort = [
        case
        for case in cases
        if case.case_population == population and case.horizon_days == horizon_days
    ]
    matured = [case for case in cohort if case.classification in MATURE_CLASSIFICATIONS]
    potential = [case for case in matured if case.classification == POTENTIAL_FALSE_NEGATIVE]
    linked = [case for case in cohort if case.shadow_forecast_id is not None]
    aligned = [case for case in linked if case.shadow_supports_direction is True]
    benchmark_linked = [case for case in matured if case.benchmark_outcome is not None]
    control_linked = [case for case in matured if case.control_outcome is not None]
    unique_control_cases = len({
        case.control_decision_log_id
        for case in control_linked
        if case.control_decision_log_id is not None
    })
    clean_misses = [case for case in matured if case.review_label == CLEAN_MISSED_OPPORTUNITY]
    risky_wins = [case for case in matured if case.review_label == RISKY_COUNTERFACTUAL_WIN]
    market_wins = [case for case in matured if case.review_label == MARKET_DRIVEN_WIN]
    uncontrolled_wins = [
        case for case in matured
        if case.review_label == UNCONTROLLED_COUNTERFACTUAL_WIN
    ]
    insufficient_potential_wins = [
        case for case in potential if case.review_label == INSUFFICIENT_EVIDENCE
    ]
    return {
        "case_population": population,
        "horizon_days": horizon_days,
        "shadow_horizon_sessions": HORIZON_MAP[horizon_days],
        "total_cases": len(cohort),
        "matured_cases": len(matured),
        "potential_false_negatives": len(potential),
        "positive_below_threshold": sum(
            case.classification == POSITIVE_BELOW_THRESHOLD for case in matured
        ),
        "non_positive_cases": sum(
            case.classification == NON_POSITIVE_COUNTERFACTUAL for case in matured
        ),
        "pending_cases": sum(case.classification == PENDING for case in cohort),
        "missing_outcome_cases": sum(
            case.classification == MISSING_OUTCOME for case in cohort
        ),
        "excluded_cases": sum(
            case.classification in EXCLUDED_CLASSIFICATIONS for case in cohort
        ),
        "shadow_linked_cases": len(linked),
        "shadow_aligned_cases": len(aligned),
        "benchmark_linked_cases": len(benchmark_linked),
        "control_linked_cases": len(control_linked),
        "unique_control_cases": unique_control_cases,
        "control_reuse_ratio": (
            len(control_linked) / unique_control_cases if unique_control_cases else None
        ),
        "clean_missed_opportunities": len(clean_misses),
        "risky_counterfactual_wins": len(risky_wins),
        "market_driven_wins": len(market_wins),
        "uncontrolled_counterfactual_wins": len(uncontrolled_wins),
        "insufficient_potential_wins": len(insufficient_potential_wins),
        "shadow_coverage_rate": len(linked) / len(cohort) if cohort else None,
        "shadow_alignment_rate": len(aligned) / len(linked) if linked else None,
        "benchmark_coverage_rate": len(benchmark_linked) / len(matured) if matured else None,
        "control_coverage_rate": len(control_linked) / len(matured) if matured else None,
        "potential_false_negative_rate": len(potential) / len(matured) if matured else None,
        "clean_miss_rate": len(clean_misses) / len(matured) if matured else None,
        "mean_directional_outcome": _mean([case.directional_outcome for case in matured]),
        "mean_false_negative_outcome": _mean(
            [case.directional_outcome for case in potential]
        ),
        "mean_mae": _mean([case.mae for case in matured]),
        "mean_mfe": _mean([case.mfe for case in matured]),
        "mean_alpha_vs_benchmark": _mean(
            [case.alpha_vs_benchmark for case in benchmark_linked]
        ),
        "mean_delta_vs_control": _mean(
            [case.delta_vs_control for case in control_linked]
        ),
    }


def build_metric_rows(cases: list[LearningShadowCase]) -> list[dict[str, Any]]:
    return [
        _metric_row(cases, population=population, horizon_days=horizon_days)
        for population in CASE_POPULATIONS
        for horizon_days in HORIZON_MAP
    ]


def build_cohort_metric_rows(cases: list[LearningShadowCase]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, int, Any], list[LearningShadowCase]] = {}
    for case in cases:
        local_day = case.decided_at.astimezone(ART_TZ).date()
        cohort_date = local_day - timedelta(days=local_day.weekday())
        groups.setdefault(
            (case.case_population, case.horizon_days, cohort_date), []
        ).append(case)

    rows: list[dict[str, Any]] = []
    for (population, horizon_days, cohort_date), cohort in sorted(groups.items()):
        metric = _metric_row(
            cohort,
            population=population,
            horizon_days=horizon_days,
        )
        rows.append({"cohort_date": cohort_date, **metric})
    return rows


def build_rule_candidates(cases: list[LearningShadowCase]) -> list[dict[str, Any]]:
    groups: dict[str, list[LearningShadowCase]] = {}
    for case in cases:
        if (
            case.case_population == PLANNER_BLOCKED
            and case.horizon_days == 5
            and case.classification in MATURE_CLASSIFICATIONS
        ):
            groups.setdefault(case.block_category, []).append(case)

    candidate_types = {
        "SCORE_GUARD": "SHADOW_THRESHOLD_REVIEW",
        "FUNDING": "DELAYED_RECHECK",
        "MIN_WEIGHT_DELTA": "SHADOW_THRESHOLD_REVIEW",
        "MIN_TRADE_OR_NOMINAL": "SHADOW_THRESHOLD_REVIEW",
    }
    rows: list[dict[str, Any]] = []
    for category, cohort in sorted(groups.items()):
        clean = [case for case in cohort if case.review_label == CLEAN_MISSED_OPPORTUNITY]
        if len(cohort) < MIN_RULE_SAMPLE or len(clean) < 5:
            continue
        candidate_type = candidate_types.get(category, "EVIDENCE_REVIEW")
        rows.append({
            "block_category": category,
            "horizon_days": 5,
            "candidate_type": candidate_type,
            "sample_size": len(cohort),
            "clean_miss_count": len(clean),
            "clean_miss_rate": len(clean) / len(cohort),
            "risky_win_count": sum(
                case.review_label == RISKY_COUNTERFACTUAL_WIN for case in cohort
            ),
            "market_driven_count": sum(
                case.review_label == MARKET_DRIVEN_WIN for case in cohort
            ),
            "mean_alpha_vs_benchmark": _mean(
                [case.alpha_vs_benchmark for case in cohort]
            ),
            "evidence_start": min(case.decided_at for case in cohort),
            "evidence_end": max(case.decided_at for case in cohort),
            "rationale": (
                f"{len(clean)} of {len(cohort)} mature blocked cases were clean, "
                "benchmark-adjusted counterfactual wins. Review in shadow only."
            ),
            "proposed_rule": {
                "mode": "shadow_only",
                "action": candidate_type,
                "block_category": category,
                "review_after_days": [2, 5],
                "live_threshold_change": False,
                "minimum_future_sample": max(30, len(cohort)),
                "requires_human_approval": True,
            },
        })
    return rows

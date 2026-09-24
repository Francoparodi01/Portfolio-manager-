"""Economic Meta Policy v1.

Research-only meta-labeling layer for Quantia.

The current Decision Engine remains the sole source of candidate actions. This
module can only classify each candidate as ALLOW_SHADOW or REJECT_TO_HOLD for
prospective research. It must never mutate a production recommendation, sizing
decision, broker instruction, or execution path.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from math import isfinite
from typing import Any, Mapping
from uuid import uuid4


ECONOMIC_META_POLICY_VERSION = "economic-meta-policy-v1"
PRIMARY_HORIZON_DAYS = 20
RESEARCH_HORIZONS_DAYS = (5, 10, 20, 40)


class MetaDecision(str, Enum):
    ALLOW_SHADOW = "ALLOW_SHADOW"
    REJECT_TO_HOLD = "REJECT_TO_HOLD"


@dataclass(frozen=True, slots=True)
class CandidateDecision:
    ticker: str
    candidate_action: str
    candidate_score: float
    as_of: datetime
    estimated_cost_bps: float = 0.0
    portfolio_turnover: float = 0.0
    market_regime: str = "UNKNOWN"
    expected_edge_vs_hold_bps: float | None = None
    edge_uncertainty_bps: float | None = None
    opportunity_id: str | None = None

    @property
    def abs_score(self) -> float:
        return abs(float(self.candidate_score))


@dataclass(frozen=True, slots=True)
class MetaPolicyConfig:
    name: str
    version: str
    buy_min_abs_score: float
    sell_min_abs_score: float
    hold_passthrough: bool = True
    max_estimated_cost_bps: float = 250.0
    max_portfolio_turnover: float = 0.35
    require_edge_evidence: bool = False
    min_edge_buffer_bps: float = 0.0
    blocked_regimes_for_buy: tuple[str, ...] = ()
    blocked_regimes_for_sell: tuple[str, ...] = ()
    primary_horizon_days: int = PRIMARY_HORIZON_DAYS
    calibration_status: str = "PREREGISTERED_SHADOW_UNVALIDATED"
    affects_production_recommendation: bool = False
    affects_sizing: bool = False
    affects_execution: bool = False
    capital_effect: bool = False

    def validate(self) -> None:
        if self.primary_horizon_days != PRIMARY_HORIZON_DAYS:
            raise ValueError("Economic Meta Policy v1 primary horizon is frozen at 20D")
        if min(self.buy_min_abs_score, self.sell_min_abs_score) < 0:
            raise ValueError("score thresholds must be non-negative")
        if self.max_estimated_cost_bps < 0:
            raise ValueError("max_estimated_cost_bps must be non-negative")
        if not 0 <= self.max_portfolio_turnover <= 1:
            raise ValueError("max_portfolio_turnover must be between 0 and 1")
        if any(
            (
                self.affects_production_recommendation,
                self.affects_sizing,
                self.affects_execution,
                self.capital_effect,
            )
        ):
            raise ValueError("economic meta policy v1 is SHADOW ONLY")


@dataclass(frozen=True, slots=True)
class MetaDecisionRecord:
    run_id: str
    as_of: str
    ticker: str
    candidate_action: str
    candidate_score: float
    abs_score: float
    policy_version: str
    policy_name: str
    decision: str
    rejection_reason: str | None
    expected_horizon_days: int
    estimated_cost_bps: float
    portfolio_turnover: float
    market_regime: str
    expected_edge_vs_hold_bps: float | None
    edge_uncertainty_bps: float | None
    opportunity_id: str | None
    research_horizons_days: tuple[int, ...] = RESEARCH_HORIZONS_DAYS
    evidence_status: str = "EDGE_NOT_PROVIDED"
    shadow_only: bool = True
    affects_production_recommendation: bool = False
    affects_sizing: bool = False
    affects_execution: bool = False
    capital_effect: bool = False
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["research_horizons_days"] = list(self.research_horizons_days)
        payload["metadata"] = dict(self.metadata)
        return payload


def preregistered_policies() -> dict[str, MetaPolicyConfig]:
    """Return frozen shadow challengers.

    Thresholds are heuristic preregistration values, not fitted optima. They
    must not be interpreted as production thresholds until prospective PIT
    evidence clears the promotion gates.
    """
    policies = {
        "META-A": MetaPolicyConfig(
            name="META-A",
            version=f"{ECONOMIC_META_POLICY_VERSION}:meta-a",
            buy_min_abs_score=0.10,
            sell_min_abs_score=0.10,
        ),
        "META-B": MetaPolicyConfig(
            name="META-B",
            version=f"{ECONOMIC_META_POLICY_VERSION}:meta-b",
            buy_min_abs_score=0.15,
            sell_min_abs_score=0.10,
        ),
        "META-C": MetaPolicyConfig(
            name="META-C",
            version=f"{ECONOMIC_META_POLICY_VERSION}:meta-c",
            buy_min_abs_score=0.20,
            sell_min_abs_score=0.12,
            require_edge_evidence=True,
            min_edge_buffer_bps=25.0,
        ),
    }
    for policy in policies.values():
        policy.validate()
    return policies


def evaluate_candidate(
    candidate: CandidateDecision | Mapping[str, Any],
    policy: MetaPolicyConfig,
    *,
    run_id: str | None = None,
) -> MetaDecisionRecord:
    """Evaluate a candidate in shadow without mutating the candidate itself."""
    policy.validate()
    item = _coerce_candidate(candidate)
    action = item.candidate_action.upper().strip()
    regime = item.market_regime.upper().strip() or "UNKNOWN"
    reasons: list[str] = []

    if action not in {"BUY", "SELL", "REDUCE", "HOLD"}:
        reasons.append("UNSUPPORTED_ACTION")
    if not item.ticker:
        reasons.append("MISSING_TICKER")
    if not _finite(item.candidate_score):
        reasons.append("INVALID_SCORE")
    if not _finite(item.estimated_cost_bps) or item.estimated_cost_bps < 0:
        reasons.append("INVALID_COST")
    if (
        not _finite(item.portfolio_turnover)
        or item.portfolio_turnover < 0
        or item.portfolio_turnover > 1
    ):
        reasons.append("INVALID_TURNOVER")

    if not reasons:
        if action == "HOLD":
            if policy.hold_passthrough:
                return _record(
                    item,
                    policy,
                    MetaDecision.REJECT_TO_HOLD,
                    "SOURCE_ALREADY_HOLD",
                    run_id=run_id,
                    evidence_status=_evidence_status(item),
                )
            reasons.append("HOLD_NOT_PASSTHROUGH")
        else:
            normalized_action = "SELL" if action == "REDUCE" else action
            threshold = (
                policy.buy_min_abs_score
                if normalized_action == "BUY"
                else policy.sell_min_abs_score
            )
            if item.abs_score < threshold:
                reasons.append("SCORE_BELOW_PREREGISTERED_GATE")
            blocked = (
                policy.blocked_regimes_for_buy
                if normalized_action == "BUY"
                else policy.blocked_regimes_for_sell
            )
            if regime in {value.upper() for value in blocked}:
                reasons.append("REGIME_BLOCKED")
            if item.estimated_cost_bps > policy.max_estimated_cost_bps:
                reasons.append("COST_ABOVE_SHADOW_GATE")
            if item.portfolio_turnover > policy.max_portfolio_turnover:
                reasons.append("TURNOVER_ABOVE_SHADOW_GATE")

            edge_status = _evidence_status(item)
            if policy.require_edge_evidence:
                if item.expected_edge_vs_hold_bps is None:
                    reasons.append("EDGE_EVIDENCE_REQUIRED")
                else:
                    uncertainty = max(0.0, float(item.edge_uncertainty_bps or 0.0))
                    required = (
                        float(item.estimated_cost_bps)
                        + uncertainty
                        + float(policy.min_edge_buffer_bps)
                    )
                    if float(item.expected_edge_vs_hold_bps) <= required:
                        reasons.append("EDGE_DOES_NOT_CLEAR_COST_UNCERTAINTY_BUFFER")
            elif item.expected_edge_vs_hold_bps is not None:
                uncertainty = max(0.0, float(item.edge_uncertainty_bps or 0.0))
                required = float(item.estimated_cost_bps) + uncertainty
                if float(item.expected_edge_vs_hold_bps) <= required:
                    reasons.append("EDGE_DOES_NOT_CLEAR_COST_UNCERTAINTY")

            if not reasons:
                return _record(
                    item,
                    policy,
                    MetaDecision.ALLOW_SHADOW,
                    None,
                    run_id=run_id,
                    evidence_status=edge_status,
                )

    return _record(
        item,
        policy,
        MetaDecision.REJECT_TO_HOLD,
        "|".join(dict.fromkeys(reasons)) if reasons else "FAIL_CLOSED",
        run_id=run_id,
        evidence_status=_evidence_status(item),
    )


def evaluate_all_preregistered(
    candidate: CandidateDecision | Mapping[str, Any],
    *,
    run_id: str | None = None,
) -> list[MetaDecisionRecord]:
    shared_run_id = run_id or str(uuid4())
    return [
        evaluate_candidate(candidate, policy, run_id=shared_run_id)
        for policy in preregistered_policies().values()
    ]


def assert_shadow_only(record: MetaDecisionRecord) -> None:
    """Hard guard for any caller before persisting or presenting the record."""
    if not record.shadow_only:
        raise RuntimeError("economic meta policy record lost SHADOW_ONLY flag")
    if any(
        (
            record.affects_production_recommendation,
            record.affects_sizing,
            record.affects_execution,
            record.capital_effect,
        )
    ):
        raise RuntimeError("economic meta policy attempted capital effect")


def _record(
    item: CandidateDecision,
    policy: MetaPolicyConfig,
    decision: MetaDecision,
    reason: str | None,
    *,
    run_id: str | None,
    evidence_status: str,
) -> MetaDecisionRecord:
    record = MetaDecisionRecord(
        run_id=run_id or str(uuid4()),
        as_of=_utc_iso(item.as_of),
        ticker=item.ticker.upper().strip(),
        candidate_action=item.candidate_action.upper().strip(),
        candidate_score=round(float(item.candidate_score), 8),
        abs_score=round(item.abs_score, 8),
        policy_version=policy.version,
        policy_name=policy.name,
        decision=decision.value,
        rejection_reason=reason,
        expected_horizon_days=policy.primary_horizon_days,
        estimated_cost_bps=round(float(item.estimated_cost_bps), 4),
        portfolio_turnover=round(float(item.portfolio_turnover), 8),
        market_regime=item.market_regime.upper().strip() or "UNKNOWN",
        expected_edge_vs_hold_bps=(
            round(float(item.expected_edge_vs_hold_bps), 4)
            if item.expected_edge_vs_hold_bps is not None
            else None
        ),
        edge_uncertainty_bps=(
            round(float(item.edge_uncertainty_bps), 4)
            if item.edge_uncertainty_bps is not None
            else None
        ),
        opportunity_id=item.opportunity_id,
        evidence_status=evidence_status,
        metadata={
            "calibration_status": policy.calibration_status,
            "policy_thresholds": {
                "buy_min_abs_score": policy.buy_min_abs_score,
                "sell_min_abs_score": policy.sell_min_abs_score,
                "max_estimated_cost_bps": policy.max_estimated_cost_bps,
                "max_portfolio_turnover": policy.max_portfolio_turnover,
                "require_edge_evidence": policy.require_edge_evidence,
                "min_edge_buffer_bps": policy.min_edge_buffer_bps,
            },
        },
    )
    assert_shadow_only(record)
    return record


def _coerce_candidate(value: CandidateDecision | Mapping[str, Any]) -> CandidateDecision:
    if isinstance(value, CandidateDecision):
        return value
    raw = dict(value)
    as_of = raw.get("as_of")
    if isinstance(as_of, str):
        as_of = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
    if not isinstance(as_of, datetime):
        as_of = datetime.now(timezone.utc)
    return CandidateDecision(
        ticker=str(raw.get("ticker") or ""),
        candidate_action=str(raw.get("candidate_action") or raw.get("action") or ""),
        candidate_score=float(raw.get("candidate_score", raw.get("final_score", 0.0))),
        as_of=as_of,
        estimated_cost_bps=float(raw.get("estimated_cost_bps") or 0.0),
        portfolio_turnover=float(raw.get("portfolio_turnover") or 0.0),
        market_regime=str(raw.get("market_regime") or "UNKNOWN"),
        expected_edge_vs_hold_bps=(
            float(raw["expected_edge_vs_hold_bps"])
            if raw.get("expected_edge_vs_hold_bps") is not None
            else None
        ),
        edge_uncertainty_bps=(
            float(raw["edge_uncertainty_bps"])
            if raw.get("edge_uncertainty_bps") is not None
            else None
        ),
        opportunity_id=(
            str(raw["opportunity_id"]) if raw.get("opportunity_id") is not None else None
        ),
    )


def _evidence_status(item: CandidateDecision) -> str:
    if item.expected_edge_vs_hold_bps is None:
        return "EDGE_NOT_PROVIDED"
    if item.edge_uncertainty_bps is None:
        return "EDGE_WITHOUT_UNCERTAINTY"
    return "EDGE_AND_UNCERTAINTY_PROVIDED"


def _finite(value: Any) -> bool:
    try:
        return isfinite(float(value))
    except (TypeError, ValueError):
        return False


def _utc_iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


__all__ = [
    "CandidateDecision",
    "ECONOMIC_META_POLICY_VERSION",
    "MetaDecision",
    "MetaDecisionRecord",
    "MetaPolicyConfig",
    "PRIMARY_HORIZON_DAYS",
    "RESEARCH_HORIZONS_DAYS",
    "assert_shadow_only",
    "evaluate_all_preregistered",
    "evaluate_candidate",
    "preregistered_policies",
]

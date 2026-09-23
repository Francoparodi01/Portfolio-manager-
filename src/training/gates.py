from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .contracts import PromotionGateResult


@dataclass(frozen=True)
class PromotionGateConfig:
    min_total_samples: int = 30
    min_holdout_samples: int = 10
    min_jev_score_delta: float = 0.0
    min_net_ev_delta: float = 0.0
    max_drawdown_increase: float = 0.0
    max_risk_guard_violations: int = 0
    min_data_quality_pass_rate: float = 0.95


def _number(row: Mapping[str, Any], key: str) -> float | None:
    value = row.get(key)
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def evaluate_promotion_gate(
    *,
    champion: Mapping[str, Any],
    challenger: Mapping[str, Any],
    total_samples: int,
    holdout_samples: int,
    temporal_holdout: bool,
    config: PromotionGateConfig | None = None,
) -> PromotionGateResult:
    """Return eligibility for human review, never automatic production promotion."""

    cfg = config or PromotionGateConfig()
    champion_jev = _number(champion, "jev_mean_score")
    challenger_jev = _number(challenger, "jev_mean_score")
    champion_ev = _number(champion, "net_ev")
    challenger_ev = _number(challenger, "net_ev")
    champion_dd = _number(champion, "max_drawdown")
    challenger_dd = _number(challenger, "max_drawdown")
    dq_rate = _number(challenger, "data_quality_pass_rate")
    violations = int(challenger.get("risk_guard_violations") or 0)

    checks: dict[str, bool] = {
        "total_samples": total_samples >= cfg.min_total_samples,
        "holdout_samples": holdout_samples >= cfg.min_holdout_samples,
        "temporal_holdout": bool(temporal_holdout),
        "risk_guard_violations": violations <= cfg.max_risk_guard_violations,
        "data_quality": dq_rate is not None and dq_rate >= cfg.min_data_quality_pass_rate,
        "jev_non_degradation": (
            champion_jev is not None
            and challenger_jev is not None
            and challenger_jev - champion_jev >= cfg.min_jev_score_delta
        ),
        "net_ev_non_degradation": (
            champion_ev is not None
            and challenger_ev is not None
            and challenger_ev - champion_ev >= cfg.min_net_ev_delta
        ),
        "drawdown_non_degradation": (
            champion_dd is not None
            and challenger_dd is not None
            and challenger_dd - champion_dd <= cfg.max_drawdown_increase
        ),
    }

    reasons = tuple(name for name, passed in checks.items() if not passed)
    values = {
        "total_samples": total_samples,
        "holdout_samples": holdout_samples,
        "champion_jev_mean_score": champion_jev,
        "challenger_jev_mean_score": challenger_jev,
        "champion_net_ev": champion_ev,
        "challenger_net_ev": challenger_ev,
        "champion_max_drawdown": champion_dd,
        "challenger_max_drawdown": challenger_dd,
        "challenger_risk_guard_violations": violations,
        "challenger_data_quality_pass_rate": dq_rate,
    }
    return PromotionGateResult(
        eligible_for_review=all(checks.values()),
        checks=checks,
        values=values,
        reasons=reasons,
    )

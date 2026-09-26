"""Prospective evaluator for Economic Meta Policy v1 shadow challengers."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from statistics import mean, median
from typing import Any, Iterable, Mapping, Sequence


PRIMARY_HORIZON_DAYS = 20
RESEARCH_HORIZONS_DAYS = (5, 10, 20, 40)


@dataclass(frozen=True, slots=True)
class MetaOutcome:
    policy_name: str
    policy_version: str
    opportunity_id: str
    horizon_days: int
    entry_at: datetime
    exit_at: datetime
    meta_return_net: float
    hold_return_net: float
    current_return_net: float
    turnover: float = 0.0
    cost_drag: float = 0.0
    pnl_net: float | None = None

    @property
    def dva_vs_hold(self) -> float:
        return float(self.meta_return_net) - float(self.hold_return_net)

    @property
    def delta_vs_current(self) -> float:
        return float(self.meta_return_net) - float(self.current_return_net)


@dataclass(frozen=True, slots=True)
class MetaPolicyMetrics:
    policy_name: str
    policy_version: str
    horizon_days: int
    n: int
    n_effective: int
    mean_net_return: float | None
    median_net_return: float | None
    mean_dva_vs_hold: float | None
    mean_delta_vs_current: float | None
    pnl_net_total: float | None
    win_rate_net: float | None
    win_rate_vs_hold: float | None
    profit_factor: float | None
    max_drawdown_proxy: float | None
    mean_turnover: float | None
    mean_cost_drag: float | None
    tail_loss_p10: float | None
    top1_positive_share: float | None
    top3_positive_share: float | None
    interpretation: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def evaluate_policy(
    rows: Iterable[MetaOutcome | Mapping[str, Any]],
    *,
    horizon_days: int,
) -> MetaPolicyMetrics:
    normalized = [_coerce(row) for row in rows]
    selected = [row for row in normalized if row.horizon_days == int(horizon_days)]
    if not selected:
        return MetaPolicyMetrics(
            policy_name="UNKNOWN",
            policy_version="UNKNOWN",
            horizon_days=int(horizon_days),
            n=0,
            n_effective=0,
            mean_net_return=None,
            median_net_return=None,
            mean_dva_vs_hold=None,
            mean_delta_vs_current=None,
            pnl_net_total=None,
            win_rate_net=None,
            win_rate_vs_hold=None,
            profit_factor=None,
            max_drawdown_proxy=None,
            mean_turnover=None,
            mean_cost_drag=None,
            tail_loss_p10=None,
            top1_positive_share=None,
            top3_positive_share=None,
            interpretation="INSUFFICIENT",
        )

    policy_names = {row.policy_name for row in selected}
    versions = {row.policy_version for row in selected}
    if len(policy_names) != 1 or len(versions) != 1:
        raise ValueError("evaluate_policy requires one policy/version at a time")

    returns = [float(row.meta_return_net) for row in selected]
    dvas = [row.dva_vs_hold for row in selected]
    deltas = [row.delta_vs_current for row in selected]
    pnls = [float(row.pnl_net) for row in selected if row.pnl_net is not None]
    positives = [value for value in returns if value > 0]
    negatives = [value for value in returns if value < 0]
    positive_total = sum(positives)
    negative_total = abs(sum(negatives))
    profit_factor = (
        positive_total / negative_total
        if negative_total > 0
        else (float("inf") if positive_total > 0 else None)
    )
    top1, top3 = _positive_concentration(returns)

    return MetaPolicyMetrics(
        policy_name=next(iter(policy_names)),
        policy_version=next(iter(versions)),
        horizon_days=int(horizon_days),
        n=len(selected),
        n_effective=_effective_n(selected),
        mean_net_return=mean(returns),
        median_net_return=median(returns),
        mean_dva_vs_hold=mean(dvas),
        mean_delta_vs_current=mean(deltas),
        pnl_net_total=sum(pnls) if pnls else None,
        win_rate_net=sum(value > 0 for value in returns) / len(returns),
        win_rate_vs_hold=sum(value > 0 for value in dvas) / len(dvas),
        profit_factor=profit_factor,
        max_drawdown_proxy=_max_drawdown_proxy(selected),
        mean_turnover=mean(float(row.turnover) for row in selected),
        mean_cost_drag=mean(float(row.cost_drag) for row in selected),
        tail_loss_p10=_quantile(returns, 0.10),
        top1_positive_share=top1,
        top3_positive_share=top3,
        interpretation=("PRIMARY_20D" if horizon_days == PRIMARY_HORIZON_DAYS else "SECONDARY"),
    )


def evaluate_all_horizons(
    rows: Iterable[MetaOutcome | Mapping[str, Any]],
) -> dict[int, MetaPolicyMetrics]:
    cached = [_coerce(row) for row in rows]
    return {
        horizon: evaluate_policy(cached, horizon_days=horizon)
        for horizon in RESEARCH_HORIZONS_DAYS
    }


def promotion_snapshot(
    challenger: Sequence[MetaPolicyMetrics],
    *,
    min_effective_n: int = 20,
    max_drawdown_deterioration: float = 0.02,
    max_tail_loss_deterioration: float = 0.02,
    max_cost_drag_deterioration: float = 0.0025,
) -> dict[str, Any]:
    """Return a fail-closed promotion summary; v1 never auto-enables capital."""
    primary = [item for item in challenger if item.horizon_days == PRIMARY_HORIZON_DAYS]
    if len(primary) != 1:
        return {"promotable": False, "reason": "PRIMARY_20D_METRIC_REQUIRED"}
    item = primary[0]
    if item.n_effective < int(min_effective_n):
        return {
            "promotable": False,
            "reason": "INSUFFICIENT_EFFECTIVE_SAMPLE",
            "n_effective": item.n_effective,
        }
    if item.mean_dva_vs_hold is None or item.mean_dva_vs_hold <= 0:
        return {"promotable": False, "reason": "DVA_20D_NOT_POSITIVE"}
    if item.mean_delta_vs_current is None or item.mean_delta_vs_current <= 0:
        return {"promotable": False, "reason": "CURRENT_20D_NOT_BEATEN"}
    return {
        "promotable": False,
        "reason": "MANUAL_RISK_REVIEW_REQUIRED",
        "risk_limits": {
            "max_drawdown_deterioration": max_drawdown_deterioration,
            "max_tail_loss_deterioration": max_tail_loss_deterioration,
            "max_cost_drag_deterioration": max_cost_drag_deterioration,
        },
        "note": "v1 never auto-promotes or enables capital",
    }


def _coerce(value: MetaOutcome | Mapping[str, Any]) -> MetaOutcome:
    if isinstance(value, MetaOutcome):
        return value
    raw = dict(value)
    return MetaOutcome(
        policy_name=str(raw["policy_name"]),
        policy_version=str(raw["policy_version"]),
        opportunity_id=str(raw["opportunity_id"]),
        horizon_days=int(raw["horizon_days"]),
        entry_at=_dt(raw["entry_at"]),
        exit_at=_dt(raw["exit_at"]),
        meta_return_net=float(raw["meta_return_net"]),
        hold_return_net=float(raw["hold_return_net"]),
        current_return_net=float(raw["current_return_net"]),
        turnover=float(raw.get("turnover") or 0.0),
        cost_drag=float(raw.get("cost_drag") or 0.0),
        pnl_net=(float(raw["pnl_net"]) if raw.get("pnl_net") is not None else None),
    )


def _dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _effective_n(rows: Sequence[MetaOutcome]) -> int:
    """Greedy count of non-overlapping windows, matching Decision Lab's spirit."""
    ordered = sorted(rows, key=lambda row: (row.entry_at, row.exit_at, row.opportunity_id))
    count = 0
    last_exit: datetime | None = None
    for row in ordered:
        if last_exit is None or row.entry_at >= last_exit:
            count += 1
            last_exit = row.exit_at
    return count


def _max_drawdown_proxy(rows: Sequence[MetaOutcome]) -> float | None:
    """Sequential-return research proxy; never label as executed NAV drawdown."""
    if not rows:
        return None
    equity = 1.0
    peak = 1.0
    max_dd = 0.0
    for row in sorted(rows, key=lambda item: (item.entry_at, item.opportunity_id)):
        equity *= 1.0 + float(row.meta_return_net)
        peak = max(peak, equity)
        if peak > 0:
            max_dd = max(max_dd, (peak - equity) / peak)
    return max_dd


def _positive_concentration(values: Sequence[float]) -> tuple[float | None, float | None]:
    positives = sorted((value for value in values if value > 0), reverse=True)
    total = sum(positives)
    if total <= 0:
        return None, None
    return sum(positives[:1]) / total, sum(positives[:3]) / total


def _quantile(values: Sequence[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * float(q)
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


__all__ = [
    "MetaOutcome",
    "MetaPolicyMetrics",
    "PRIMARY_HORIZON_DAYS",
    "RESEARCH_HORIZONS_DAYS",
    "evaluate_all_horizons",
    "evaluate_policy",
    "promotion_snapshot",
]
